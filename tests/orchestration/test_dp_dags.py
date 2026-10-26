"""Ba DAG rubric gọi lại callable sẵn có. Không import Airflow."""

from __future__ import annotations

import unittest
from pathlib import Path

from governance.extract import airflow_task_ids
from governance.inventory import expected_tasks

REPO = Path(__file__).resolve().parents[2]
DAGS = REPO / "orchestration" / "dags"


class PipelineLayoutTests(unittest.TestCase):
    def test_monolith_task_list_stays_the_inventory_contract(self):
        self.assertEqual(airflow_task_ids(), list(expected_tasks()))
        self.assertNotIn("spark_offline_features", airflow_task_ids())

    def test_dp1_stops_at_bronze_validation(self):
        text = (DAGS / "cinestream_dp1_raw_to_bronze.py").read_text()
        self.assertIn("cinestream_dp1_raw_to_bronze", text)
        self.assertIn("spark_raw_to_bronze", text)
        self.assertIn("validate_bronze", text)
        self.assertIn("publish_datahub", text)
        self.assertIn("validate >> hub", text)
        self.assertLess(text.index('task_id="spark_raw_to_bronze"'), text.index('task_id="publish_datahub"'))
        self.assertNotIn("spark_bronze_to_gold", text)
        self.assertIn("is_paused_upon_creation=True", text)

    def test_dp2_waits_for_dp1_and_publishes_gold(self):
        text = (DAGS / "cinestream_dp2_bronze_to_gold.py").read_text()
        self.assertIn("external_dag_id=\"cinestream_dp1_raw_to_bronze\"", text)
        self.assertIn("external_task_id=\"validate_bronze\"", text)
        self.assertIn("upstream_failed", text)
        self.assertIn("spark_bronze_to_gold", text)
        self.assertIn("validate_gold", text)
        self.assertIn("publish_success", text)
        self.assertIn("sync_clickhouse", text)
        self.assertIn("publish_datahub", text)
        self.assertIn("validate >> marker >> serving", text)
        self.assertIn("validate >> hub", text)
        self.assertNotIn("hub >> marker", text)
        self.assertNotIn("spark_offline_features", text)

    def test_dp3_waits_for_publish_and_validates_features(self):
        text = (DAGS / "cinestream_dp3_offline_features.py").read_text()
        self.assertIn("external_dag_id=\"cinestream_dp2_bronze_to_gold\"", text)
        self.assertIn("external_task_id=\"publish_success\"", text)
        self.assertIn("upstream_failed", text)
        self.assertIn("spark_offline_features", text)
        self.assertIn("validate_features", text)
        self.assertIn("publish_datahub", text)
        self.assertIn("validate >> hub", text)
        self.assertLess(text.index('task_id="spark_offline_features"'), text.index('task_id="publish_datahub"'))
        self.assertNotIn("sync_clickhouse", text)

    def test_dp4_materializes_only_after_dp3(self):
        text = (DAGS / "cinestream_dp4_materialize_online.py").read_text()
        self.assertIn("cinestream_dp4_materialize_online", text)
        self.assertIn("external_dag_id=DP3", text)
        self.assertIn("external_task_id=\"validate_features\"", text)
        self.assertIn("upstream_failed", text)
        self.assertIn("materialize_online", text)
        self.assertIn("is_paused_upon_creation=True", text)
        self.assertNotIn("task_id=\"materialize_online\"", (DAGS / "cinestream_daily_batch_lakehouse.py").read_text())


if __name__ == "__main__":
    unittest.main()
