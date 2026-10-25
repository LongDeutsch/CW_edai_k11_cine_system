"""DP4: export đã khớp marker DP3 được materialize sang Redis.

Chờ ``validate_features`` cùng logical date. Không ghi Iceberg, không sync
ClickHouse và không bật streaming. Feast chỉ được import khi task chạy.

DP4 chỉ nạp hai Feature View batch ``user_movie_stats`` và
``movie_engagement_stats`` vào Redis database 1. Cửa sổ Flink nằm ở view
``movie_minute_playback`` trên Redis database 2. Task này không mở database 2.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from ml.online.publication import PublicationMismatch, assert_publication
from orchestration.dags.cinestream_daily_batch_lakehouse import _on_failure, _schedule

DP2 = "cinestream_dp2_bronze_to_gold"
DP3 = "cinestream_dp3_offline_features"
FEATURE_TABLES = ("feat_user_movie_stats_daily", "feat_movie_engagement_stats_daily")


def materialize_published_features(**context) -> dict:
    conf = context["dag_run"].conf or {}
    export_dir = conf.get("export_dir")
    if not export_dir:
        raise AirflowException("export_dir trống")
    marker = context["ti"].xcom_pull(dag_id=DP2, task_ids="publish_success")
    report = context["ti"].xcom_pull(dag_id=DP3, task_ids="validate_features")
    meta_path = Path(export_dir) / "snapshot.json"
    if not meta_path.exists():
        raise AirflowException("snapshot.json không có")
    export_meta = json.loads(meta_path.read_text())
    try:
        batch_id = assert_publication(
            marker,
            report,
            export_meta,
            conf.get("batch_id"),
            _live_snapshots(conf),
        )
    except PublicationMismatch as exc:
        raise AirflowException(str(exc)) from exc
    result = _run_materialize(
        Path(export_dir),
        Path(conf.get("repo_dir") or "/opt/cinestream/ml/feast/data/online-repo"),
        Path(conf.get("state_dir") or "/opt/cinestream/ml/feast/data/materialize-state"),
        str(conf.get("redis") or "redis:6379,db=1"),
    )
    result["batch_id"] = batch_id
    return result


def _run_materialize(export_dir: Path, repo_dir: Path, state_dir: Path, redis: str) -> dict:
    """Gọi Feast ở /opt/feast-packages để không đụng dependency của Airflow."""
    packages = "/opt/feast-packages"
    if not Path(packages, "feast").exists():
        raise AirflowException("thiếu /opt/feast-packages. Image Airflow không kèm Feast.")
    env = os.environ.copy()
    env["PYTHONPATH"] = packages + os.pathsep + "/opt/cinestream"
    env["PATH"] = str(Path(packages) / "bin") + os.pathsep + env.get("PATH", "")
    env["FEAST_USAGE"] = "False"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "ml.online.run_materialize",
            "--export-dir",
            str(export_dir),
            "--repo-dir",
            str(repo_dir),
            "--state-dir",
            str(state_dir),
            "--redis",
            redis,
        ],
        cwd="/opt/cinestream",
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    if completed.returncode != 0:
        raise AirflowException((completed.stderr or completed.stdout)[-2000:])
    for line in completed.stdout.splitlines():
        if line.startswith("RESULT_JSON="):
            return json.loads(line.split("=", 1)[1])
    raise AirflowException(completed.stdout[-2000:] or "không có RESULT_JSON")


def _live_snapshots(conf: dict) -> dict:
    base = str(conf.get("iceberg_rest") or "http://iceberg-rest:8181").rstrip("/")
    namespace = conf.get("gold_namespace")
    if namespace in (None, "", "gold", "silver", "bronze"):
        raise AirflowException("namespace materialize phải là namespace cô lập")
    found = {}
    for table in FEATURE_TABLES:
        url = f"{base}/v1/namespaces/{namespace}/tables/{table}"
        with urllib.request.urlopen(url, timeout=10) as response:
            metadata = json.loads(response.read().decode())["metadata"]
        found[table] = str(metadata.get("current-snapshot-id"))
    return found


with DAG(
    dag_id="cinestream_dp4_materialize_online",
    description="DP3 feature publication → Redis online store.",
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
    tags=["cinestream", "dp4", "feast"],
) as dag:
    wait_dp3 = ExternalTaskSensor(
        task_id="wait_for_dp3",
        external_dag_id=DP3,
        external_task_id="validate_features",
        allowed_states=["success"],
        failed_states=["failed", "skipped", "upstream_failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 30,
        check_existence=True,
    )
    materialize = PythonOperator(
        task_id="materialize_online",
        python_callable=materialize_published_features,
    )
    wait_dp3 >> materialize
