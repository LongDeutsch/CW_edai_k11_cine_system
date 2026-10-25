"""Phân loại finding 5C. Không đụng Iceberg, marker hay ClickHouse."""

from __future__ import annotations

import unittest

from governance.inventory import datasets
from governance.triage import (
    FINDINGS,
    LEGACY_MARKER_KEYS,
    classify_marker,
    cleanup_action,
    comparison_ignores_extra_physical_columns,
    current_serving_tables,
    drift_disposition,
    lineage_review,
)


class MarkerTests(unittest.TestCase):
    def test_current_dag_pins_three_tables_and_legacy_payload_is_rejected(self):
        required = current_serving_tables()
        self.assertEqual(required, ("dim_movies", "fact_ratings", "obt_movie_engagement"))
        legacy = {key: True for key in LEGACY_MARKER_KEYS}
        result = classify_marker(legacy, required)
        self.assertEqual(result["generation"], "legacy_without_pins")
        self.assertEqual(result["sync"], "reject")
        self.assertFalse(result["backfill_ids"])
        self.assertNotIn("gold_snapshots", result)

    def test_incomplete_pins_are_not_filled(self):
        result = classify_marker({"gold_snapshots": {"dim_movies": 1}}, ("dim_movies", "fact_ratings", "obt_movie_engagement"))
        self.assertEqual(result["generation"], "incomplete_pins")
        self.assertEqual(result["missing"], ["fact_ratings", "obt_movie_engagement"])
        self.assertFalse(result["backfill_ids"])
        self.assertNotIn(1, result.values())


class DriftTests(unittest.TestCase):
    def test_extra_dt_is_historical_and_not_a_named_reader_break(self):
        found = drift_disposition("extra", "dt")
        self.assertEqual(found["disposition"], "historical_column")
        self.assertFalse(found["contract_violation"])
        self.assertFalse(found["named_readers_affected"])
        self.assertFalse(found["change_in_5c"])

    def test_double_rating_is_a_sticky_contract_gap_without_a_5c_migration(self):
        found = drift_disposition("type", "rating_val")
        self.assertEqual(found["disposition"], "sticky_decimal_contract")
        self.assertTrue(found["contract_violation"])
        self.assertFalse(found["named_readers_affected"])
        self.assertFalse(found["change_in_5c"])

    def test_nullability_is_a_storage_gap_not_a_measured_value_violation(self):
        found = drift_disposition("nullability", "movie_id")
        self.assertEqual(found["disposition"], "storage_constraint_gap")
        self.assertFalse(found["contract_violation"])
        self.assertFalse(found["change_in_5c"])

    def test_timestamp_without_zone_stays_semantic_review(self):
        found = drift_disposition("type", "rating_ts")
        self.assertEqual(found["disposition"], "sticky_timestamp_semantics")
        self.assertFalse(found["change_in_5c"])

    def test_missing_column_would_break_named_readers(self):
        found = drift_disposition("missing", "movie_id")
        self.assertEqual(found["disposition"], "contract_break")
        self.assertTrue(found["named_readers_affected"])

    def test_same_rows_compares_only_the_new_frame_columns(self):
        self.assertTrue(comparison_ignores_extra_physical_columns())


class LineageTests(unittest.TestCase):
    def test_fact_watch_feature_and_streaming_gaps_match_source_behavior(self):
        review = lineage_review()
        self.assertEqual(review["fact_watch_downstream"], [])
        self.assertEqual(review["feature_downstream"]["iceberg.gold.feat_movie_engagement_stats_daily"], [])
        self.assertEqual(review["feature_status"]["iceberg.gold.feat_user_movie_stats_daily"], "verified")
        self.assertEqual(review["playback_status"], "writer_gated")
        self.assertIn("clickhouse.movie_windows", review["playback_downstream"])
        self.assertEqual(review["heartbeats_downstream"], [])
        self.assertNotIn("iceberg.gold.obt_movie_engagement", review["fact_watch_downstream"])

    def test_marker_freshness_says_sync_rejects_a_marker_without_pins(self):
        marker = next(item for item in datasets() if item.id == "minio.orchestration_marker")
        self.assertIn("gold_snapshots", marker.freshness)
        self.assertIn("từ chối", marker.freshness)


class RegisterTests(unittest.TestCase):
    def test_register_covers_the_triage_and_forbids_production_mutation(self):
        ids = [item["id"] for item in FINDINGS]
        self.assertEqual(ids, ["F-07", "F-05", "F-01", "F-02", "F-04", "F-06", "F-03", "F-08", "F-09", "F-10", "F-11"])
        self.assertTrue(all(item["change_required"] is False for item in FINDINGS))
        self.assertEqual(cleanup_action("cleanup_candidate"), "future_drop_after_dry_run")
        self.assertEqual(cleanup_action("keep_referenced"), "keep_until_test_leaves_silver")


if __name__ == "__main__":
    unittest.main()
