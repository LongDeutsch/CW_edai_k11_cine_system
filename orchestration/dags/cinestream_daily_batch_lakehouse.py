"""Một DAG batch hằng ngày cho lakehouse Cinestream.

Airflow chỉ lập lịch, chờ nguồn, gửi spark-submit và ghi dấu publish.
Phép biến đổi Bronze, Silver, Gold nằm trong job Spark Phase 1 và chạy
trên cụm spark-master, không chạy trong worker Airflow.
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowException, AirflowSensorTimeout
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator

from orchestration.batch_identity import assess_listing, parse_s3_uri
from serving.clickhouse.airflow_sync import sync_arguments

DAG_ID = "cinestream_daily_batch_lakehouse"
SPARK_MASTER = "spark://spark-master:7077"
ENTRY = "/opt/cinestream/orchestration/spark_entry.py"
INTENDED_SCHEDULE = os.environ.get("CINESTREAM_BATCH_SCHEDULE", "0 2 * * *").strip()


def _env_allows_production() -> bool:
    return os.environ.get("CINESTREAM_ALLOW_PRODUCTION", "").strip().lower() in {"1", "true", "yes"}


def _schedule():
    """Lịch chỉ gắn khi production được phép.

    Cron vẫn nằm ở ``CINESTREAM_BATCH_SCHEDULE``. Khi ``CINESTREAM_ALLOW_PRODUCTION``
    chưa bật, schedule là None nên scheduler không tạo run để rồi fail vì guard.
    Bật biến môi trường là cách ủy quyền cho mọi run lịch, không sửa JSON từng lần.
    """
    raw = INTENDED_SCHEDULE
    if raw.lower() in {"", "none", "manual"}:
        return None
    if not _env_allows_production():
        return None
    return raw


def _on_failure(context) -> None:
    task_id = context["task_instance"].task_id
    print(
        f"task {task_id} thất bại, run_id={context.get('run_id')}, "
        f"logical_date={context.get('logical_date')}. "
        "publish_success không chạy nên batch chưa được đánh dấu đã phát hành."
    )


def _conf(context) -> dict:
    dag_run = context.get("dag_run")
    params = dict(context.get("params") or {})
    incoming = dict(getattr(dag_run, "conf", None) or {})
    params.update({key: value for key, value in incoming.items() if value is not None})
    return params


def _namespaces(conf: dict) -> tuple[str, str, str]:
    bronze = str(conf.get("bronze_namespace") or "bronze")
    silver = str(conf.get("silver_namespace") or "silver")
    gold = str(conf.get("gold_namespace") or "gold")
    names = (bronze, silver, gold)
    protected = {"bronze", "silver", "gold"}
    conf_allows = conf.get("allow_production") in (True, "true", "True", "1", 1)
    if any(name in protected for name in names) and not (conf_allows or _env_allows_production()):
        raise AirflowException(
            "bảng bronze/silver/gold chỉ chạy khi CINESTREAM_ALLOW_PRODUCTION=true "
            "hoặc conf có allow_production=true"
        )
    return bronze, silver, gold


def _effective_ts(context) -> str:
    conf = _conf(context)
    override = conf.get("effective_ts")
    if override:
        return str(override)
    start = context["data_interval_start"]
    if hasattr(start, "in_timezone"):
        start = start.in_timezone("UTC")
    return start.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _source_assessment(context) -> dict:
    conf = _conf(context)
    bucket, prefix = parse_s3_uri(str(conf.get("source_dir") or "s3a://external-source-lake"))
    objects = _list_objects(bucket, prefix, str(conf.get("s3_endpoint") or "http://minio:9000"))
    found = assess_listing(objects, prefix)
    found["source_dir"] = str(conf.get("source_dir"))
    found["bucket"] = bucket
    found["prefix"] = prefix
    return found


def _list_objects(bucket: str, prefix: str, endpoint: str) -> list[dict]:
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name="us-east-1",
    )
    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix.rstrip("/") + "/"
    found = []
    for page in client.get_paginator("list_objects_v2").paginate(**kwargs):
        for item in page.get("Contents") or []:
            found.append({"key": item["Key"], "size": item["Size"], "etag": item["ETag"]})
    return found


class SourceReadySensor(BaseSensorOperator):
    """Chờ đủ bốn folder parquet. Timeout lấy từ conf của từng lần chạy."""

    def __init__(self, **kwargs):
        super().__init__(poke_interval=5, timeout=180, mode="poke", **kwargs)
        self._result: dict = {}

    def execute(self, context):
        conf = _conf(context)
        self.timeout = int(conf.get("source_timeout_seconds", 180))
        self.poke_interval = int(conf.get("source_poke_seconds", 5))
        try:
            super().execute(context)
        except AirflowSensorTimeout as exc:
            reason = self._result.get("reason") or "hết thời gian chờ"
            raise AirflowSensorTimeout(f"nguồn chưa sẵn sàng: {reason}") from exc
        return self._result

    def poke(self, context) -> bool:
        conf = _conf(context)
        _exclusive(context, _namespaces(conf))
        self._result = _source_assessment(context)
        if self._result["ready"]:
            self.log.info("batch nguồn %s, %s file", self._result["batch_id"], self._result["files"])
            return True
        self.log.info("chưa đủ nguồn: %s", self._result["reason"])
        return False


def _batch(context) -> dict:
    pulled = context["ti"].xcom_pull(task_ids="check_source_ready")
    if pulled and pulled.get("batch_id"):
        return pulled
    conf = _conf(context)
    if conf.get("batch_id"):
        return {
            "batch_id": str(conf["batch_id"]),
            "source_dir": conf.get("source_dir"),
            "files": conf.get("files"),
        }
    raise AirflowException("check_source_ready chưa trả batch_id")


def _exclusive(context, names: tuple[str, ...]) -> None:
    from orchestration.batch_guard import assert_exclusive_writer

    assert_exclusive_writer(context["dag"].dag_id, names)


def _submit(context, command: str, extra: list[str]) -> dict:
    conf = _conf(context)
    bronze, silver, gold = _namespaces(conf)
    _exclusive(context, (bronze, silver, gold))
    cmd = [
        "spark-submit",
        "--master",
        SPARK_MASTER,
        "--deploy-mode",
        "client",
        "--driver-memory",
        "1g",
        "--executor-memory",
        "1g",
        "--executor-cores",
        "1",
        "--conf",
        "spark.executor.instances=1",
        "--conf",
        "spark.dynamicAllocation.enabled=false",
        "--conf",
        "spark.cores.max=1",
        ENTRY,
        command,
        "--warehouse",
        str(conf.get("warehouse") or "s3a://lakehouse"),
        "--master",
        SPARK_MASTER,
        "--iceberg-rest",
        str(conf.get("iceberg_rest") or "http://iceberg-rest:8181"),
        "--s3-endpoint",
        str(conf.get("s3_endpoint") or "http://minio:9000"),
        "--s3-access-key",
        os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "--s3-secret-key",
        os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "--source-dir",
        str(conf.get("source_dir") or "s3a://external-source-lake"),
        "--no-ui",
        "--shuffle-partitions",
        "4",
        *extra,
    ]
    if command == "publish":
        cmd.append("--enable-aqe")
    print("spark-submit " + " ".join(cmd[1:]))
    env = os.environ.copy()
    env["PYTHONPATH"] = "/opt/cinestream"
    env.setdefault("SPARK_DRIVER_HOST", "airflow-scheduler")
    completed = subprocess.run(cmd, text=True, capture_output=True, env=env)
    if completed.stdout:
        print(completed.stdout)
    if completed.stderr:
        print(completed.stderr)
    if completed.returncode != 0:
        raise AirflowException(f"spark-submit {command} thoát {completed.returncode}")
    return _result_json(completed.stdout + "\n" + completed.stderr) | {
        "bronze_namespace": bronze,
        "silver_namespace": silver,
        "gold_namespace": gold,
    }


def _result_json(text: str) -> dict:
    found = {}
    for line in text.splitlines():
        if line.startswith("RESULT_JSON="):
            found = json.loads(line.split("=", 1)[1])
    return found


def spark_raw_to_bronze(**context) -> dict:
    batch = _batch(context)
    conf = _conf(context)
    bronze, _, _ = _namespaces(conf)
    extra = [
        "--namespace",
        bronze,
        "--batch-id",
        batch["batch_id"],
        "--run-id",
        str(context["run_id"]),
        "--error-threshold",
        str(conf.get("error_threshold", 0.05)),
    ]
    if _fail_once(conf):
        marker = _fail_marker(context)
        if not marker.exists():
            try:
                _submit(context, "ingest", [*extra, "--fail-after", "users"])
            except Exception:
                marker.parent.mkdir(parents=True, exist_ok=True)
                marker.write_text("failed-after-users")
                raise
            marker.write_text("unexpected")
            raise AirflowException("fail_once phải dừng sau khi commit users")
    result = _submit(context, "ingest", extra)
    result["batch_id"] = batch["batch_id"]
    return result


def validate_bronze_task(**context) -> dict:
    batch = _batch(context)
    conf = _conf(context)
    bronze, _, _ = _namespaces(conf)
    result = _submit(
        context,
        "validate-bronze",
        [
            "--namespace",
            bronze,
            "--batch-id",
            batch["batch_id"],
            "--error-threshold",
            str(conf.get("error_threshold", 0.05)),
        ],
    )
    if result.get("ready") is False:
        raise AirflowException(result.get("reason") or "bronze không đạt")
    return result


def spark_bronze_to_gold(**context) -> dict:
    batch = _batch(context)
    conf = _conf(context)
    bronze, silver, gold = _namespaces(conf)
    effective = _effective_ts(context)
    result = _submit(
        context,
        "publish",
        [
            "--bronze-namespace",
            bronze,
            "--silver-namespace",
            silver,
            "--gold-namespace",
            gold,
            "--effective-ts",
            effective,
            "--batch-id",
            f"{batch['batch_id']}-publish",
            "--run-id",
            str(context["run_id"]),
            "--error-threshold",
            str(conf.get("error_threshold", 0.05)),
        ],
    )
    result["effective_ts"] = effective
    result["batch_id"] = batch["batch_id"]
    return result


def validate_gold_task(**context) -> dict:
    conf = _conf(context)
    _, silver, gold = _namespaces(conf)
    result = _submit(
        context,
        "validate-gold",
        ["--silver-namespace", silver, "--gold-namespace", gold],
    )
    if result.get("ready") is False:
        raise AirflowException(result.get("reason") or "gold không đạt")
    return result


_SERVING_TABLES = ("dim_movies", "fact_ratings", "obt_movie_engagement")
_FEATURE_SOURCE_TABLES = ("stg_ratings", "stg_watch_sessions")


def _current_snapshots(conf: dict, namespace: str, tables: tuple[str, ...]) -> dict:
    """Đọc snapshot hiện tại qua Iceberg REST. Không tự chọn snapshot mới hơn."""
    import urllib.error
    import urllib.request

    rest = str(conf.get("iceberg_rest") or "http://iceberg-rest:8181").rstrip("/")
    found = {}
    for name in tables:
        url = f"{rest}/v1/namespaces/{namespace}/tables/{name}"
        try:
            with urllib.request.urlopen(url, timeout=15) as response:
                meta = json.loads(response.read().decode())["metadata"]
        except urllib.error.HTTPError as exc:
            raise AirflowException(f"không đọc snapshot {namespace}.{name}: HTTP {exc.code}") from exc
        snapshot_id = meta.get("current-snapshot-id")
        if snapshot_id in (None, -1):
            raise AirflowException(f"{namespace}.{name} chưa có snapshot để phát hành")
        found[name] = int(snapshot_id)
    return found


def _gold_snapshots(conf: dict, gold: str) -> dict:
    return _current_snapshots(conf, gold, _SERVING_TABLES)


def publish_success(**context) -> dict:
    batch = _batch(context)
    conf = _conf(context)
    bronze, silver, gold = _namespaces(conf)
    _exclusive(context, (bronze, silver, gold))
    payload = {
        "published": True,
        "dag_id": context["dag"].dag_id,
        "run_id": str(context["run_id"]),
        "logical_date": str(context["logical_date"]),
        "data_interval_start": context["data_interval_start"].isoformat(),
        "data_interval_end": context["data_interval_end"].isoformat(),
        "effective_ts": _effective_ts(context),
        "batch_id": batch["batch_id"],
        "source_dir": batch.get("source_dir"),
        "files": batch.get("files"),
        "namespaces": {"bronze": bronze, "silver": silver, "gold": gold},
        "bronze_quality": context["ti"].xcom_pull(task_ids="validate_bronze")
        or context["ti"].xcom_pull(task_ids="validate_bronze", dag_id="cinestream_dp1_raw_to_bronze"),
        "gold_quality": context["ti"].xcom_pull(task_ids="validate_gold"),
        "gold_snapshots": _gold_snapshots(conf, gold),
        "silver_snapshots": _current_snapshots(conf, silver, _FEATURE_SOURCE_TABLES),
    }
    _put_marker(conf, bronze, batch["batch_id"], payload)
    print("RESULT_JSON=" + json.dumps({"published": True, "batch_id": batch["batch_id"], "gold_snapshots": payload["gold_snapshots"]}))
    return payload


def sync_clickhouse(**context) -> dict:
    """Nạp snapshot trong marker. Không đọc snapshot hiện tại của Iceberg."""
    batch = _batch(context)
    conf = _conf(context)
    bronze, _, gold = _namespaces(conf)
    _exclusive(context, (bronze, bronze, gold))
    args = sync_arguments(
        batch_id=batch["batch_id"],
        marker_namespace=bronze,
        gold_namespace=gold,
        warehouse=str(conf.get("warehouse") or "s3a://lakehouse"),
        iceberg_rest=str(conf.get("iceberg_rest") or "http://iceberg-rest:8181"),
        s3_endpoint=str(conf.get("s3_endpoint") or "http://minio:9000"),
        s3_access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        s3_secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        clickhouse_http=str(conf.get("clickhouse_http") or os.environ.get("CINESTREAM_CLICKHOUSE_HTTP", "http://clickhouse:8123")),
        clickhouse_database=conf.get("clickhouse_database"),
    )
    cmd = [
        "spark-submit",
        "--master",
        "local[1]",
        "--deploy-mode",
        "client",
        "--driver-memory",
        "1g",
        "/opt/cinestream/serving/clickhouse/sync_gold.py",
        *args,
    ]
    print("spark-submit " + " ".join(cmd[1:]))
    env = os.environ.copy()
    env["PYTHONPATH"] = "/opt/cinestream"
    env["SPARK_USE_IMAGE_JARS"] = "1"
    env.pop("SPARK_DRIVER_HOST", None)
    env.pop("SPARK_DRIVER_PORT", None)
    completed = subprocess.run(cmd, text=True, capture_output=True, env=env)
    if completed.stdout:
        print(completed.stdout)
    if completed.stderr:
        print(completed.stderr)
    if completed.returncode != 0:
        raise AirflowException(f"sync ClickHouse thoát {completed.returncode}")
    found = {}
    for line in (completed.stdout + "\n" + completed.stderr).splitlines():
        if line.startswith("SERVING_JSON="):
            found = json.loads(line.split("=", 1)[1])
    if not found:
        raise AirflowException("sync ClickHouse không trả SERVING_JSON")
    return found


def _put_marker(conf: dict, namespace: str, batch_id: str, payload: dict) -> None:
    import boto3

    bucket, _ = parse_s3_uri(str(conf.get("warehouse") or "s3a://lakehouse"))
    key = f"_orchestration/{namespace}/{batch_id}.json"
    client = boto3.client(
        "s3",
        endpoint_url=str(conf.get("s3_endpoint") or "http://minio:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name="us-east-1",
    )
    body = json.dumps(payload, default=str).encode()
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    print(f"đã ghi s3://{bucket}/{key}")


def _fail_once(conf: dict) -> bool:
    return conf.get("fail_once") in (True, "true", "True", "1", 1)


def _fail_marker(context):
    from pathlib import Path

    return Path("/opt/airflow/logs/fail-once") / f"{context['run_id']}.txt"


def _require_published_batch(context, conf: dict, silver: str, gold: str) -> dict:
    """DP3 chỉ chạy khi snapshot Gold và Silver còn đúng batch DP2 đã phát hành."""
    published = context["ti"].xcom_pull(
        task_ids="publish_success",
        dag_id="cinestream_dp2_bronze_to_gold",
    )
    if not published or published.get("published") is not True:
        raise AirflowException("DP3 không thấy publish_success của DP2 cùng logical date")
    if conf.get("batch_id") and str(conf["batch_id"]) != str(published.get("batch_id")):
        raise AirflowException("batch_id DP3 khác batch DP2 đã phát hành")
    expected_gold = {name: int(published["gold_snapshots"][name]) for name in _SERVING_TABLES}
    current_gold = _gold_snapshots(conf, gold)
    if current_gold != expected_gold:
        raise AirflowException(f"gold snapshot lệch DP2: {current_gold} != {expected_gold}")
    expected_silver = {name: int(published["silver_snapshots"][name]) for name in _FEATURE_SOURCE_TABLES}
    current_silver = _current_snapshots(conf, silver, _FEATURE_SOURCE_TABLES)
    if current_silver != expected_silver:
        raise AirflowException(f"silver snapshot lệch DP2: {current_silver} != {expected_silver}")
    return published


def spark_offline_features(**context) -> dict:
    """DP3 đọc Silver của đúng snapshot DP2 đã ghim, rồi ghi feat_*.

    ``fact_ratings`` không có ``user_id``. Mốc thời gian của watch cũng chỉ
    còn trên Silver, nên phép tính feature không đổi. Task này từ chối chạy
    nếu snapshot Gold hoặc Silver hiện tại khác marker ``publish_success``.
    """
    conf = _conf(context)
    _, silver, gold = _namespaces(conf)
    _require_published_batch(context, conf, silver, gold)
    as_of = str(conf.get("as_of") or _effective_ts(context))
    created = str(conf.get("created_timestamp") or as_of)
    extra = [
        "--silver-namespace",
        silver,
        "--gold-namespace",
        gold,
        "--as-of",
        as_of,
        "--created-timestamp",
        created,
    ]
    if conf.get("allow_production") in (True, "true", "True", "1", 1) or _env_allows_production():
        extra.append("--allow-production")
    result = _submit(context, "features", extra)
    if result.get("ready") is False:
        raise AirflowException("feature không đạt: " + "; ".join(result.get("reasons") or []))
    return result


def validate_offline_features(**context) -> dict:
    result = context["ti"].xcom_pull(task_ids="spark_offline_features")
    if not result or result.get("ready") is not True:
        raise AirflowException("chưa có feature đạt kiểm tra")
    if result.get("user_rows") != result.get("user_distinct"):
        raise AirflowException("trùng (user_id, event_timestamp)")
    if result.get("movie_rows") != result.get("movie_distinct"):
        raise AirflowException("trùng (movie_id, event_timestamp)")
    return result


default_args = {
    "owner": "cinestream",
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": _on_failure,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Raw MinIO → Bronze → Silver → Gold. Lịch 02:00 UTC chỉ bật khi CINESTREAM_ALLOW_PRODUCTION=true.",
    schedule=_schedule(),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cinestream", "batch", "lakehouse"],
    params={
        "source_dir": "s3a://external-source-lake",
        "warehouse": "s3a://lakehouse",
        "bronze_namespace": "bronze",
        "silver_namespace": "silver",
        "gold_namespace": "gold",
        "iceberg_rest": "http://iceberg-rest:8181",
        "s3_endpoint": "http://minio:9000",
        "error_threshold": 0.05,
        "source_timeout_seconds": 180,
        "source_poke_seconds": 5,
        "fail_once": False,
    },
) as dag:
    check_source_ready = SourceReadySensor(
        task_id="check_source_ready",
        retries=0,
        execution_timeout=timedelta(minutes=15),
    )
    raw_to_bronze = PythonOperator(
        task_id="spark_raw_to_bronze",
        python_callable=spark_raw_to_bronze,
        retries=1,
    )
    bronze_gate = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze_task,
        retries=0,
    )
    to_gold = PythonOperator(
        task_id="spark_bronze_to_gold",
        python_callable=spark_bronze_to_gold,
        retries=1,
    )
    gold_gate = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold_task,
        retries=0,
    )
    published = PythonOperator(
        task_id="publish_success",
        python_callable=publish_success,
        retries=1,
        execution_timeout=timedelta(minutes=5),
    )
    serving = PythonOperator(
        task_id="sync_clickhouse",
        python_callable=sync_clickhouse,
        retries=1,
        execution_timeout=timedelta(minutes=20),
    )
    check_source_ready >> raw_to_bronze >> bronze_gate >> to_gold >> gold_gate >> published >> serving
