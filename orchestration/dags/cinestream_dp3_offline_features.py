"""DP3: Silver đã publish → bảng feat_* , rồi kiểm tra khóa và count.

Chờ DP2 ``publish_success`` cùng logical date. Không sync ClickHouse và
không bật streaming. Namespace gold vẫn bị chặn khi chưa allow production.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from orchestration.dags.cinestream_daily_batch_lakehouse import (
    _on_failure,
    _schedule,
    spark_offline_features,
    validate_offline_features,
)

with DAG(
    dag_id="cinestream_dp3_offline_features",
    description="DP3 Gold/Silver → offline feature tables.",
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
    tags=["cinestream", "dp3", "features"],
    params={
        "warehouse": "s3a://lakehouse",
        "bronze_namespace": "bronze",
        "silver_namespace": "silver",
        "gold_namespace": "gold",
        "as_of": None,
        "created_timestamp": None,
        "batch_id": None,
    },
) as dag:
    wait_dp2 = ExternalTaskSensor(
        task_id="wait_for_dp2",
        external_dag_id="cinestream_dp2_bronze_to_gold",
        external_task_id="publish_success",
        allowed_states=["success"],
        failed_states=["failed", "skipped", "upstream_failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 30,
        check_existence=True,
    )
    features = PythonOperator(task_id="spark_offline_features", python_callable=spark_offline_features)
    validate = PythonOperator(task_id="validate_features", python_callable=validate_offline_features, retries=0)
    wait_dp2 >> features >> validate
