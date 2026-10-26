"""Chiếu data_flow DP1–DP3. Không cần GMS đang chạy."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from governance.datahub.emit import (
    PIPELINES,
    assertion_urn,
    build_proposals,
    dataset_urn,
    emit_pipeline,
    flow_urn,
    job_urn,
    pipeline_edges,
    qualified_dataset,
)
from governance.inventory import datasets
from governance.lineage_kind import KINDS


def _names(bodies: list[dict]) -> list[str]:
    return [item["proposal"]["aspectName"] for item in bodies]


def _urns(bodies: list[dict]) -> list[str]:
    return [item["proposal"]["entityUrn"] for item in bodies]


class DataHubProjectionTests(unittest.TestCase):
    def test_production_urns_match_the_rubric_shape(self):
        raw_users = next(item for item in datasets() if item.id == "iceberg.bronze.raw_users")
        platform, name = qualified_dataset(raw_users, None)
        self.assertEqual(platform, "iceberg")
        self.assertEqual(
            dataset_urn(platform, name),
            "urn:li:dataset:(urn:li:dataPlatform:iceberg,cinestream.bronze.raw_users,PROD)",
        )
        self.assertEqual(
            flow_urn("cinestream_dp1_raw_to_bronze"),
            "urn:li:dataFlow:(airflow,cinestream_dp1_raw_to_bronze,prod)",
        )
        self.assertEqual(
            job_urn("cinestream_dp1_raw_to_bronze", "spark_raw_to_bronze"),
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,cinestream_dp1_raw_to_bronze,prod),spark_raw_to_bronze)",
        )

    def test_only_data_flow_edges_of_each_job_are_projected(self):
        self.assertEqual(KINDS, ("data_flow", "control_dependency", "observability", "api_consumption"))
        dp1 = pipeline_edges(PIPELINES["cinestream_dp1_raw_to_bronze"])
        self.assertTrue(dp1)
        self.assertTrue(all(edge.kind == "data_flow" for edge in dp1))
        self.assertTrue(any(edge.downstream == "iceberg.bronze.raw_users" for edge in dp1))
        self.assertFalse(any(edge.downstream == "iceberg.bronze.quality_results" for edge in dp1))
        self.assertFalse(any("orchestration_marker" in edge.downstream for edge in dp1))
        dp2 = pipeline_edges(PIPELINES["cinestream_dp2_bronze_to_gold"])
        self.assertTrue(any(edge.downstream == "iceberg.gold.fact_ratings" for edge in dp2))
        self.assertFalse(any(edge.evidence[0].startswith("serving/") for edge in dp2))
        dp3 = pipeline_edges(PIPELINES["cinestream_dp3_offline_features"])
        self.assertTrue(any(edge.downstream == "iceberg.gold.feat_user_movie_stats_daily" for edge in dp3))
        self.assertTrue(all("build_offline_features.py" in edge.evidence[0] for edge in dp3))

    def test_it_namespace_does_not_reuse_production_dataset_names(self):
        namespaces = {"bronze": "it_af_dp", "silver": "it_af_dp", "gold": "it_af_dp"}
        bodies = build_proposals(
            PIPELINES["cinestream_dp1_raw_to_bronze"],
            {"ready": True, "datasets": [], "violations": []},
            namespaces=namespaces,
            run_id="manual-it",
            now_ms=1,
        )
        joined = json.dumps(bodies)
        self.assertIn("cinestream.it_af_dp.raw_users", joined)
        self.assertNotIn("cinestream.bronze.raw_users", joined)
        self.assertIn("it_af_dp.external-source-lake/users", joined)
        self.assertNotIn("urn:li:dataset:(urn:li:dataPlatform:s3,external-source-lake/users,PROD)", joined)

    def test_assertions_follow_the_validate_dict(self):
        passed = build_proposals(
            PIPELINES["cinestream_dp1_raw_to_bronze"],
            {"ready": True, "datasets": [{"dataset": "raw_users", "error_rate": 0.0}], "violations": []},
            run_id="run-ok",
            threshold=0.05,
            now_ms=10,
        )
        failed = build_proposals(
            PIPELINES["cinestream_dp3_offline_features"],
            {"ready": False, "user_rows": 3, "user_distinct": 2, "movie_rows": 4, "movie_distinct": 4},
            run_id="run-bad",
            now_ms=11,
        )
        self.assertIn("SUCCESS", _result_types(passed))
        blob = json.dumps(failed)
        self.assertIn("user_rows_distinct", blob)
        self.assertIn("FAILURE", blob)
        self.assertIn("SUCCESS", blob)
        self.assertIn(
            assertion_urn(
                "cinestream_dp3_offline_features",
                "movie_rows_distinct",
                "cinestream.gold.feat_movie_engagement_stats_daily",
            ),
            blob,
        )
        gold = build_proposals(
            PIPELINES["cinestream_dp2_bronze_to_gold"],
            {
                "ready": True,
                "counts": {
                    "stg_ratings": 5,
                    "fact_ratings": 5,
                    "stg_watch_sessions": 4,
                    "fact_watch_sessions": 4,
                    "obt_movie_engagement": 4,
                },
                "orphan_user_sk": 0,
                "orphan_movie_sk": 1,
            },
            run_id="run-gold",
            now_ms=12,
        )
        gold_blob = json.dumps(gold)
        self.assertIn("silver_gold_match", gold_blob)
        self.assertIn("no_orphan_key", gold_blob)
        self.assertIn("FAILURE", gold_blob)

    def test_emit_posts_every_proposal_and_does_not_touch_storage(self):
        sent = []

        def transport(url, body, token):
            sent.append((url, body, token))

        summary = emit_pipeline(
            "http://datahub-gms:8080",
            PIPELINES["cinestream_dp1_raw_to_bronze"],
            {"ready": False, "reason": "bronze không đạt", "violations": [{"dataset": "raw_ratings"}]},
            run_id="run-fail",
            transport=transport,
        )
        self.assertEqual(summary["proposals"], len(sent))
        self.assertTrue(all(item[0] == "http://datahub-gms:8080" for item in sent))
        self.assertIn("assertionRunEvent", _names([item[1] for item in sent]))
        source = (Path(__file__).resolve().parents[2] / "governance/datahub/emit.py").read_text()
        self.assertNotIn("DROP TABLE", source)
        self.assertNotIn("spark.sql", source)


def _result_types(bodies: list[dict]) -> set[str]:
    found = set()
    for body in bodies:
        proposal = body["proposal"]
        if proposal["aspectName"] != "assertionRunEvent":
            continue
        aspect = json.loads(proposal["aspect"]["value"])
        found.add(aspect["result"]["type"])
    return found


if __name__ == "__main__":
    unittest.main()
