"""DP2: Bronze → Silver/Gold, validate Gold, ghi marker, sync ClickHouse.

Chờ DP1 ``validate_bronze`` cùng logical date. ``batch_id`` lấy từ XCom
của DP1 khi có, hoặc từ conf khi trigger tay sau DP1.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from governance.datahub.emit import publish_datahub
from orchestration.dags.cinestream_daily_batch_lakehouse import (
    _on_failure,
    _schedule,
    publish_success,
    spark_bronze_to_gold,
    sync_clickhouse,
    validate_gold_task,
)

with DAG(
    dag_id="cinestream_dp2_bronze_to_gold",
    description="DP2 Bronze → Silver/Gold. Ingest, validate, publish, sync.",
    schedule=_schedule(),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "cinestream",
        "retries": 1,
        "retry_delay": timedelta(seconds=20),
        "execution_timeout": timedelta(minutes=20),
        "on_failure_callback": _on_failure,
    },
    tags=["cinestream", "dp2", "gold"],
    params={
        "warehouse": "s3a://lakehouse",
        "bronze_namespace": "bronze",
        "silver_namespace": "silver",
        "gold_namespace": "gold",
        "error_threshold": 0.05,
        "batch_id": None,
    },
) as dag:
    wait_dp1 = ExternalTaskSensor(
        task_id="wait_for_dp1",
        external_dag_id="cinestream_dp1_raw_to_bronze",
        external_task_id="validate_bronze",
        allowed_states=["success"],
        failed_states=["failed", "skipped", "upstream_failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 30,
        check_existence=True,
    )
    publish = PythonOperator(task_id="spark_bronze_to_gold", python_callable=spark_bronze_to_gold)
    validate = PythonOperator(task_id="validate_gold", python_callable=validate_gold_task, retries=0)
    marker = PythonOperator(task_id="publish_success", python_callable=publish_success, retries=0)
    serving = PythonOperator(task_id="sync_clickhouse", python_callable=sync_clickhouse, retries=1)
    hub = PythonOperator(
        task_id="publish_datahub",
        python_callable=publish_datahub,
        trigger_rule="all_done",
        retries=0,
    )
    wait_dp1 >> publish >> validate >> marker >> serving
    validate >> hub
