"""Hợp đồng schema thuần. Không cần Spark."""

from __future__ import annotations

import unittest
from pathlib import Path

from transformation.batch.contracts.schema_consistency import (
    compare_fields,
    expected_partitions,
    null_constraint_findings,
    partition_difference,
    write_decision,
)

ROOT = Path(__file__).resolve().parents[2]


class SchemaDecisionTests(unittest.TestCase):
    def test_extra_dt_is_reported_and_does_not_overwrite(self):
        differences = compare_fields(
            [{"name": "rating_id", "type": "bigint", "nullable": True}],
            [
                {"name": "rating_id", "type": "bigint", "nullable": True},
                {"name": "dt", "type": "date", "nullable": True},
            ],
        )
        decision = write_decision(True, differences)
        self.assertEqual(differences[0]["kind"], "extra_on_table")
        self.assertEqual(decision["status"], "unchanged")
        self.assertFalse(decision["overwrite"])
        self.assertFalse(decision["schema_equal"])
        self.assertFalse(decision["repartition"])

    def test_missing_column_does_not_overwrite_when_shared_rows_match(self):
        differences = compare_fields(
            [
                {"name": "rating_id", "type": "bigint", "nullable": True},
                {"name": "review_text", "type": "string", "nullable": True},
            ],
            [{"name": "rating_id", "type": "bigint", "nullable": True}],
        )
        decision = write_decision(True, differences)
        self.assertEqual(differences[0]["kind"], "missing_on_table")
        self.assertFalse(decision["overwrite"])

    def test_row_change_overwrites_even_when_schema_also_differs(self):
        differences = compare_fields(
            [{"name": "rating_val", "type": "decimal(2,1)", "nullable": False}],
            [{"name": "rating_val", "type": "double", "nullable": True}],
        )
        kinds = {item["kind"] for item in differences}
        self.assertEqual(kinds, {"type", "nullability"})
        decision = write_decision(False, differences)
        self.assertTrue(decision["overwrite"])
        self.assertEqual(decision["status"], "overwritten")

    def test_schema_match_and_equal_rows_stay_unchanged(self):
        decision = write_decision(True, [])
        self.assertTrue(decision["schema_equal"])
        self.assertEqual(decision["status"], "unchanged")

    def test_incomparable_rows_do_not_overwrite(self):
        decision = write_decision(None, [{"column": "rating_ts", "kind": "type", "frame": "timestamp", "table": "timestamp_ntz"}])
        self.assertIsNone(decision["rows_equal"])
        self.assertFalse(decision["overwrite"])

    def test_null_values_are_a_constraint_finding(self):
        found = null_constraint_findings({"rating_id": 2, "movie_id": 0}, ["rating_id", "movie_id"])
        self.assertEqual(found, [{"column": "rating_id", "kind": "null_value", "nulls": 2, "constraint": "not_null"}])
        decision = write_decision(True, found)
        self.assertFalse(decision["overwrite"])
        self.assertTrue(decision["schema_equal"])

    def test_source_folder_dt_is_not_an_iceberg_partition(self):
        report = partition_difference(["event_date"], [], ("dt",))
        self.assertEqual(report["missing_partition_fields"], ["event_date"])
        self.assertEqual(report["actual_iceberg_fields"], [])
        self.assertEqual(report["source_folder_is_iceberg_partition"], [])
        self.assertFalse(report["repartition"])

    def test_ddl_partitions_match_the_sql_files(self):
        silver = expected_partitions((ROOT / "storage/lakehouse/silver/stg_tables.sql").read_text())
        gold = expected_partitions((ROOT / "storage/lakehouse/gold/gold_tables.sql").read_text())
        self.assertEqual(silver["stg_ratings"], ["event_date"])
        self.assertEqual(silver["stg_watch_sessions"], ["event_date"])
        self.assertEqual(silver["stg_users"], [])
        self.assertEqual(gold["fact_ratings"], ["rating_date_key"])
        self.assertEqual(gold["fact_watch_sessions"], ["session_date_key"])
        self.assertEqual(gold["obt_movie_engagement"], ["session_date_key"])
        self.assertEqual(gold["feat_movie_engagement_stats_daily"], ["days(event_timestamp)"])


if __name__ == "__main__":
    unittest.main()
