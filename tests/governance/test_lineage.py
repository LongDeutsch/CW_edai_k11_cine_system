"""Lineage inventory khớp source và không có cạnh mồ côi."""

from __future__ import annotations

import unittest

from governance.extract import clickhouse_tables, fastapi_routes, spark_tables
from governance.inventory import datasets, edges
from governance.validate import problems


class LineageTests(unittest.TestCase):
    def test_lineage_has_no_dangling_or_inconsistent_references(self):
        self.assertEqual(problems(), [])

    def test_feature_tables_read_silver_and_do_not_feed_serving(self):
        features = {
            "iceberg.gold.feat_user_movie_stats_daily",
            "iceberg.gold.feat_movie_engagement_stats_daily",
        }
        for item in datasets():
            if item.id in features:
                self.assertEqual(item.lineage_status, "verified")
        incoming = {edge.downstream for edge in edges() if edge.downstream in features}
        self.assertEqual(incoming, features)
        outgoing = {edge.downstream for edge in edges() if edge.upstream in features}
        self.assertEqual(outgoing, set())

    def test_integration_resources_are_not_on_production_edges(self):
        for edge in edges():
            self.assertFalse(edge.upstream.startswith("integration."))
            self.assertFalse(edge.downstream.startswith("integration."))

    def test_clickhouse_stage_clones_reader_columns(self):
        tables = clickhouse_tables()
        self.assertEqual(tables["movie_engagement_stage"], tables["movie_engagement"])
        self.assertEqual(tables["movie_windows_stage"], tables["movie_windows"])
        names = {name for name, _ in tables["movie_engagement"]}
        self.assertIn("session_sk", names)
        window_names = {name for name, _ in tables["movie_windows"]}
        self.assertIn("iceberg_snapshot_id", window_names)

    def test_gold_ddl_includes_unwritten_feature_tables(self):
        names = set(spark_tables("storage/lakehouse/gold/gold_tables.sql"))
        self.assertIn("feat_movie_engagement_stats_daily", names)
        self.assertIn("obt_movie_engagement", names)

    def test_api_routes_are_inventoried(self):
        routes = set(fastapi_routes())
        locations = {item.location.removeprefix("GET ") for item in datasets() if item.id.startswith("api.")}
        self.assertEqual(routes, locations)


if __name__ == "__main__":
    unittest.main()
