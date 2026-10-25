"""DP1: Raw MinIO → Bronze, rồi validate Bronze.

Cùng callable với DAG batch hiện có. Schedule vẫn tắt khi chưa
``CINESTREAM_ALLOW_PRODUCTION``. DAG cũ không bị xóa.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from orchestration.dags.cinestream_daily_batch_lakehouse import (
    SourceReadySensor,
    _on_failure,
    _schedule,
    spark_raw_to_bronze,
    validate_bronze_task,
)

with DAG(
    dag_id="cinestream_dp1_raw_to_bronze",
    description="DP1 Raw → Bronze. Ingest rồi validate.",
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
    tags=["cinestream", "dp1", "bronze"],
    params={
        "source_dir": "s3a://external-source-lake",
        "warehouse": "s3a://lakehouse",
        "bronze_namespace": "bronze",
        "silver_namespace": "silver",
        "gold_namespace": "gold",
        "error_threshold": 0.05,
        "source_timeout_seconds": 180,
        "source_poke_seconds": 5,
        "fail_once": False,
    },
) as dag:
    ready = SourceReadySensor(
        task_id="check_source_ready",
        retries=0,
        execution_timeout=timedelta(minutes=15),
    )
    ingest = PythonOperator(task_id="spark_raw_to_bronze", python_callable=spark_raw_to_bronze)
    validate = PythonOperator(task_id="validate_bronze", python_callable=validate_bronze_task, retries=0)
    ready >> ingest >> validate
