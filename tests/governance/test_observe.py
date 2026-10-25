"""Drift, trạng thái dataset, Airflow, serving và tác động lineage."""

from __future__ import annotations

import unittest

from governance.drift import classify_schema
from governance.inventory import datasets
from governance.observe import (
    batch_freshness,
    classify_dataset,
    cleanup_candidates,
    descendants,
    impact_for,
    summarize_airflow,
    summarize_serving,
)


def _dataset(dataset_id: str):
    return next(item for item in datasets() if item.id == dataset_id)


class DriftTests(unittest.TestCase):
    def test_known_dt_column_needs_review_and_is_not_breaking(self):
        result = classify_schema(
            [("rating_id", "BIGINT NOT NULL"), ("event_date", "DATE NOT NULL")],
            [
                {"name": "rating_id", "type": "long", "required": True},
                {"name": "event_date", "type": "date", "required": True},
                {"name": "dt", "type": "date", "required": False},
            ],
        )
        extra = next(item for item in result["findings"] if item["column"] == "dt")
        self.assertEqual(extra["kind"], "extra")
        self.assertEqual(extra["classification"], "manual_review")
        self.assertEqual(result["classification"], "manual_review")

    def test_missing_column_is_breaking(self):
        result = classify_schema(
            [("movie_id", "BIGINT NOT NULL")],
            [],
        )
        self.assertEqual(result["classification"], "breaking")
        self.assertEqual(result["findings"][0]["kind"], "missing")

    def test_incompatible_type_is_breaking(self):
        result = classify_schema(
            [("movie_id", "BIGINT")],
            [{"name": "movie_id", "type": "string", "required": False}],
        )
        self.assertEqual(result["classification"], "breaking")

    def test_decimal_stored_as_double_needs_review(self):
        result = classify_schema(
            [("rating_val", "DECIMAL(2, 1) NOT NULL")],
            [{"name": "rating_val", "type": "double", "required": False}],
        )
        kinds = {item["kind"] for item in result["findings"]}
        self.assertIn("type", kinds)
        self.assertIn("nullability", kinds)
        self.assertEqual(result["classification"], "manual_review")

    def test_stricter_nullability_stays_compatible(self):
        result = classify_schema(
            [("title", "STRING")],
            [{"name": "title", "type": "string", "required": True}],
        )
        self.assertEqual(result["classification"], "compatible")


class StatusTests(unittest.TestCase):
    def test_disabled_streaming_table_is_not_a_failure(self):
        status = classify_dataset(
            _dataset("iceberg.silver.stg_playback_windows"),
            {"catalog": "absent", "streaming_enabled": False},
        )
        self.assertEqual(status["status"], "expected_absent")
        self.assertFalse(status["failure"])
        impact = impact_for(status["id"], status["status"], "match")
        self.assertEqual(impact["effect"], "disabled_path")
        self.assertIn("clickhouse.movie_windows", impact["downstream"])
        self.assertIn("api.movies.trending", impact["downstream"])

    def test_empty_feature_table_has_no_downstream(self):
        status = classify_dataset(
            _dataset("iceberg.gold.feat_movie_engagement_stats_daily"),
            {"catalog": "present", "snapshot_id": -1, "total_records": None},
        )
        self.assertEqual(status["status"], "declared_empty")
        self.assertFalse(status["failure"])
        self.assertEqual(descendants(status["id"]), [])
        self.assertEqual(impact_for(status["id"], status["status"], "match")["effect"], "none")

    def test_snapshot_disagrees_with_marker_is_stale(self):
        status = classify_dataset(
            _dataset("iceberg.gold.obt_movie_engagement"),
            {
                "catalog": "present",
                "snapshot_id": 22,
                "total_records": 10,
                "marker_snapshot_id": 11,
            },
        )
        self.assertEqual(status["status"], "populated_stale")
        self.assertTrue(status["failure"])
        impact = impact_for(status["id"], status["status"], "match")
        self.assertEqual(impact["effect"], "downstream_review")
        self.assertIn("clickhouse.movie_engagement", impact["downstream"])
        self.assertNotIn("clickhouse.movie_windows", impact["downstream"])

    def test_old_event_time_is_stale_without_calling_the_snapshot_a_success(self):
        status = classify_dataset(
            _dataset("clickhouse.movie_windows"),
            {
                "catalog": "present",
                "snapshot_id": 5,
                "total_records": 2,
                "event_age_seconds": 10_000,
                "stale_after_seconds": 180,
            },
        )
        self.assertEqual(status["status"], "populated_stale")
        self.assertTrue(status["failure"])


class PipelineTests(unittest.TestCase):
    def test_airflow_failure_is_not_hidden_by_a_later_integration_success(self):
        summary = summarize_airflow(
            [
                {"run_id": "new", "state": "success", "production": False, "tasks": [{"task_id": "sync_clickhouse", "state": "success"}]},
                {
                    "run_id": "prod-fail",
                    "state": "failed",
                    "production": True,
                    "tasks": [{"task_id": "spark_raw_to_bronze", "state": "failed"}],
                },
            ]
        )
        self.assertEqual(summary["latest_state"], "success")
        self.assertFalse(summary["latest_is_production"])
        self.assertEqual(summary["production_run_state"], "failed")
        self.assertEqual(summary["latest_production_failed_tasks"], ["spark_raw_to_bronze"])
        self.assertFalse(summary["production_success_implies_current_data"])

    def test_serving_mismatch_is_sync_failure(self):
        result = summarize_serving(
            {
                "rows": 10,
                "engagement_versions": 2,
                "rating_versions": 1,
                "same_batch": False,
                "production_tasks": ["sync_clickhouse"],
            }
        )
        self.assertEqual(result["status"], "sync_failed")

    def test_populated_gold_without_snapshot_pins_is_not_aligned(self):
        statuses = [
            {"id": "iceberg.gold.dim_movies", "status": "populated"},
            {"id": "iceberg.gold.fact_ratings", "status": "populated"},
            {"id": "iceberg.gold.obt_movie_engagement", "status": "populated"},
        ]
        pins = {"dim_movies": 1, "fact_ratings": 1, "obt_movie_engagement": 1}
        self.assertEqual(batch_freshness(statuses, True, {}), "published_without_snapshot_pins")
        self.assertEqual(batch_freshness(statuses, True, pins), "aligned_with_marker")
        stale = [dict(item) for item in statuses]
        stale[2] = {**stale[2], "status": "populated_stale"}
        self.assertEqual(batch_freshness(stale, True, pins), "stale_vs_marker")
        self.assertEqual(batch_freshness(statuses, False, pins), "marker_unread")

    def test_production_publish_without_sync_task_is_not_called_success(self):
        result = summarize_serving(
            {
                "rows": 0,
                "engagement_versions": 0,
                "rating_versions": 0,
                "production_tasks": ["publish_success"],
            }
        )
        self.assertEqual(result["status"], "not_run")

    def test_clickhouse_error_is_sync_failure(self):
        result = summarize_serving({"error": "ClickHouse không tới được", "rows": None, "production_tasks": ["sync_clickhouse"]})
        self.assertEqual(result["status"], "sync_failed")


class HygieneTests(unittest.TestCase):
    def test_integration_tables_inside_silver_are_candidates_without_deletion(self):
        rows = cleanup_candidates(
            [
                {"namespace": "silver", "name": "it_orphan_windows", "referenced": False, "snapshot_id": 1},
                {"namespace": "silver", "name": "it_p2_windows", "referenced": True, "snapshot_id": 2},
                {"namespace": "it_af_ok", "name": "stg_users", "referenced": False},
                {"namespace": "silver", "name": "stg_ratings", "referenced": True},
            ]
        )
        self.assertEqual([item["name"] for item in rows], ["it_orphan_windows", "it_p2_windows"])
        self.assertEqual(rows[0]["disposition"], "cleanup_candidate")
        self.assertEqual(rows[1]["disposition"], "keep_referenced")

    def test_dt_drift_on_ratings_reaches_batch_serving_not_windows(self):
        downstream = descendants("iceberg.silver.stg_ratings")
        self.assertIn("iceberg.gold.fact_ratings", downstream)
        self.assertIn("api.movies.top_rated", downstream)
        self.assertIn("dashboard.analytics", downstream)
        self.assertNotIn("clickhouse.movie_windows", downstream)
        self.assertIn("iceberg.gold.feat_movie_engagement_stats_daily", downstream)


if __name__ == "__main__":
    unittest.main()
