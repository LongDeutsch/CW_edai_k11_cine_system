"""Chạy DAG trên Airflow thật và đối chiếu Iceberg.

Cần ``make airflow-up`` trước. Namespace chỉ dùng ``it_af_*``.
"""

from __future__ import annotations

import base64
import json
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote

import boto3
from botocore.client import Config

from orchestration.batch_identity import assess_listing
from transformation.batch.common import build_parser, start_spark
from transformation.batch.it_phase1 import (
    DAY1,
    TS1,
    _dimensions,
    _grains,
    _rating,
    _source,
    _user_file,
    _watch,
    _write_ratings,
    _write_watch,
)
from transformation.batch.jobs.table_updates import snapshots

DAG_ID = "cinestream_daily_batch_lakehouse"
API = "http://127.0.0.1:18082/api/v1"
REPO = Path(__file__).resolve().parents[1]
REPORT = REPO / "docs" / "proof" / "logs" / "phase2_airflow_it.json"
_READER = None
PRODUCTION = (
    "namespaces/silver/tables/stg_watch_sessions",
    "namespaces/bronze/tables/raw_users",
    "namespaces/silver/tables/stg_playback_heartbeats",
    "namespaces/silver/tables/stg_playback_windows",
)
ORDER = (
    "check_source_ready",
    "spark_raw_to_bronze",
    "validate_bronze",
    "spark_bronze_to_gold",
    "validate_gold",
    "publish_success",
    "sync_clickhouse",
)


def main() -> None:
    wait_http("http://127.0.0.1:18082/health")
    wait_worker()
    pause_dag()
    before = production_fingerprint()
    report = {"status": "ERROR", "failed": ["interrupted"], "cases": []}
    try:
        structure = case_structure()
        root = Path(tempfile.mkdtemp(prefix="cinestream-airflow-it-"))
        # Một SparkContext cho cả process. Session local không có jar Iceberg
        # sẽ khóa classloader, session sau không thấy SparkCatalog.
        spark = open_reader()
        ensure_buckets()
        seed_all(spark, root)
        # Mỗi lần seed tạo file parquet mới. File identity đổi nên Bronze append thêm.
        # Xóa namespace it_af_* còn lại từ lần chạy trước, rồi mới ingest.
        reset_namespaces(spark, ["it_af_contract", "it_af_ok", "it_af_retry", "it_af_backfill"])
        cases = {"1 dag import": structure}
        cases["3 missing source"] = case_missing()
        cases["4 contract blocks gold"] = case_contract()
        happy = case_success()
        cases["2 successful run"] = happy
        cases["8 gold integrity"] = case_gold(happy)
        cases["6 rerun"] = case_rerun(happy)
        cases["5 spark retry"] = case_retry()
        cases["7 backfill"] = case_backfill()
        after = production_fingerprint()
        flink = {
            "name": "9 flink tables untouched",
            "status": "PASS" if before == after else "FAIL",
            "expected": "snapshot production và bảng streaming không đổi",
            "actual": {"unchanged": before == after, "before": before, "after": after},
        }
        cases["9 flink tables untouched"] = flink
        ordered = [
            cases["1 dag import"],
            cases["2 successful run"],
            cases["3 missing source"],
            cases["4 contract blocks gold"],
            cases["5 spark retry"],
            cases["6 rerun"],
            cases["7 backfill"],
            cases["8 gold integrity"],
            cases["9 flink tables untouched"],
        ]
        failed = [item["name"] for item in ordered if item["status"] != "PASS"]
        report = {"status": "FAIL" if failed else "PASS", "failed": failed, "cases": ordered}
    finally:
        close_reader()
        try:
            api("PATCH", f"/dags/{DAG_ID}", {"is_paused": True})
        except Exception as exc:
            print(f"không pause lại được DAG: {exc}", flush=True)
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    if report.get("status") != "PASS":
        raise SystemExit(f"FAIL {report.get('failed')}")


def case_structure() -> dict:
    raw = dagbag()
    downstream = raw.get("downstream") or {}
    expected_edges = {
        "check_source_ready": ["spark_raw_to_bronze"],
        "spark_raw_to_bronze": ["validate_bronze"],
        "validate_bronze": ["spark_bronze_to_gold"],
        "spark_bronze_to_gold": ["validate_gold"],
        "validate_gold": ["publish_success"],
        "publish_success": ["sync_clickhouse"],
        "sync_clickhouse": [],
    }
    actual = {
        "import_errors": raw.get("import_errors"),
        "tasks": raw.get("task_ids"),
        "downstream": downstream,
        "retries": raw.get("retries"),
        "catchup": raw.get("catchup"),
        "max_active_runs": raw.get("max_active_runs"),
        "schedule": raw.get("schedule"),
        "timetable": raw.get("timetable"),
        "intended_schedule": raw.get("intended_schedule"),
        "allow_production": raw.get("allow_production"),
    }
    ok = (
        not raw.get("import_errors")
        and set(raw.get("task_ids") or []) == set(ORDER)
        and downstream == expected_edges
        and raw.get("catchup") is False
        and raw.get("max_active_runs") == 1
        and raw.get("retries", {}).get("check_source_ready") == 0
        and raw.get("retries", {}).get("spark_raw_to_bronze") == 1
        and raw.get("retries", {}).get("validate_bronze") == 0
        and raw.get("retries", {}).get("validate_gold") == 0
        and raw.get("retries", {}).get("sync_clickhouse") == 1
        and str(raw.get("schedule")) == "None"
        and str(raw.get("intended_schedule")) == "0 2 * * *"
        and str(raw.get("allow_production") or "false").lower() not in {"1", "true", "yes"}
    )
    return _case("1 dag import", "bảy task, sync sau publish, lịch tắt đến khi cho phép production", actual, ok)


def case_missing() -> dict:
    run = trigger(
        {
            "source_dir": "s3a://external-source-lake/it-airflow/missing",
            "bronze_namespace": "it_af_missing",
            "silver_namespace": "it_af_missing",
            "gold_namespace": "it_af_missing",
            "source_timeout_seconds": 20,
            "source_poke_seconds": 5,
        }
    )
    tasks = task_map(run)
    marker = marker_exists("it_af_missing", "")
    actual = {"dag": run.get("state"), "tasks": states(tasks), "marker": marker}
    ok = (
        run.get("state") == "failed"
        and tasks["check_source_ready"]["state"] == "failed"
        and tasks["spark_raw_to_bronze"]["state"] == "upstream_failed"
        and tasks["publish_success"]["state"] == "upstream_failed"
        and marker is False
    )
    return _case("3 missing source", "sensor hết hạn, không có marker", actual, ok)


def case_contract() -> dict:
    namespace = "it_af_contract"
    run = trigger(_run_conf("it-airflow/contract", namespace))
    tasks = task_map(run)
    batch_id = local_batch_id("it-airflow/contract")

    def read(reader) -> dict:
        return {
            "dag": run.get("state"),
            "tasks": states(tasks),
            "bronze_try": tasks["spark_raw_to_bronze"].get("try_number"),
            "ratings": count(reader, namespace, "raw_ratings"),
            "facts": count(reader, namespace, "fact_ratings"),
            "quarantine": count(reader, namespace, "quarantine_records"),
            "quality": quality_rows(reader, namespace, batch_id),
            "marker": marker_exists(namespace, batch_id),
        }

    actual = read_iceberg(read)
    rejected = any(row[1] == "threshold_rejected" for row in actual["quality"])
    ok = (
        run.get("state") == "failed"
        and tasks["spark_raw_to_bronze"]["state"] == "failed"
        and tasks["spark_bronze_to_gold"]["state"] == "upstream_failed"
        and tasks["publish_success"]["state"] == "upstream_failed"
        and actual["ratings"] == 0
        and actual["facts"] == 0
        and actual["quarantine"] > 0
        and rejected
        and actual["marker"] is False
    )
    return _case("4 contract blocks gold", "ngưỡng lỗi chặn publish, fact trống", actual, ok)


def case_success() -> dict:
    namespace = "it_af_ok"
    run = trigger(_run_conf("it-airflow/ok", namespace))
    tasks = task_map(run)
    batch_id = local_batch_id("it-airflow/ok")
    marker = read_marker(namespace, batch_id)

    def read(reader) -> dict:
        return {
            "users": count(reader, namespace, "raw_users"),
            "ratings": count(reader, namespace, "raw_ratings"),
            "facts": count(reader, namespace, "fact_ratings"),
            "watches": count(reader, namespace, "fact_watch_sessions"),
            "obt": count(reader, namespace, "obt_movie_engagement"),
            "snapshots": snap(reader, namespace, "raw_users", "raw_ratings", "fact_ratings"),
        }

    counted = read_iceberg(read)
    actual = {
        "dag": run.get("state"),
        "tasks": states(tasks),
        "batch_id": batch_id,
        "marker_published": bool(marker and marker.get("published") is True),
        "marker_batch": None if not marker else marker.get("batch_id"),
        "effective_ts": None if not marker else marker.get("effective_ts"),
        "data_interval_start": None if not marker else marker.get("data_interval_start"),
        **counted,
    }
    ok = (
        run.get("state") == "success"
        and all(tasks[name]["state"] == "success" for name in ORDER)
        and actual["users"] == 2
        and actual["ratings"] == 1
        and actual["facts"] == 1
        and actual["watches"] == 1
        and actual["obt"] == 1
        and actual["marker_published"] is True
        and actual["marker_batch"] == batch_id
    )
    found = _case("2 successful run", "đủ fact và marker published", actual, ok)
    found["namespace"] = namespace
    found["batch_id"] = batch_id
    found["snapshots"] = actual["snapshots"]
    return found


def case_gold(happy: dict) -> dict:
    if happy["status"] != "PASS":
        return _case("8 gold integrity", "bỏ qua vì lần chạy thành công đã lỗi", happy["actual"], False)
    namespace = "it_af_ok"
    actual = read_iceberg(lambda reader: gold_invariants(reader, namespace))
    ok = (
        actual["users_without_one_current"] == 0
        and actual["movies_without_one_current"] == 0
        and actual["user_overlaps"] == 0
        and actual["movie_overlaps"] == 0
        and actual["duplicate_rating_sk"] == 0
        and actual["duplicate_session_sk"] == 0
        and actual["orphan_user_sk"] == 0
        and actual["orphan_movie_sk"] == 0
        and actual["fact_ratings"] == actual["stg_ratings"] == 1
        and actual["obt"] == actual["fact_watch"] == 1
    )
    return _case("8 gold integrity", "một current, không overlap, fact khớp silver", actual, ok)


def case_rerun(happy: dict) -> dict:
    if happy["status"] != "PASS":
        return _case("6 rerun", "bỏ qua vì lần chạy thành công đã lỗi", {}, False)
    namespace = "it_af_ok"
    before = happy["snapshots"]
    run = trigger(_run_conf("it-airflow/ok", namespace))
    after = read_iceberg(lambda reader: snap(reader, namespace, "raw_users", "raw_ratings", "fact_ratings", "dim_users"))
    counts = read_iceberg(
        lambda reader: {
            "users": count(reader, namespace, "raw_users"),
            "ratings": count(reader, namespace, "raw_ratings"),
            "facts": count(reader, namespace, "fact_ratings"),
        }
    )
    actual = {
        "dag": run.get("state"),
        **counts,
        "snapshots_before": before,
        "snapshots_after": after,
        "bronze_unchanged": before.get("raw_users") == after.get("raw_users") and before.get("raw_ratings") == after.get("raw_ratings"),
    }
    ok = (
        run.get("state") == "success"
        and actual["users"] == 2
        and actual["ratings"] == 1
        and actual["facts"] == 1
        and actual["bronze_unchanged"] is True
    )
    return _case("6 rerun", "cùng fingerprint, Bronze không thêm snapshot", actual, ok)


def case_retry() -> dict:
    namespace = "it_af_retry"
    run = trigger(_run_conf("it-airflow/retry", namespace, fail_once=True))
    tasks = task_map(run)
    batch_id = local_batch_id("it-airflow/retry")
    counted = read_iceberg(
        lambda reader: {
            "users": count(reader, namespace, "raw_users"),
            "ratings": count(reader, namespace, "raw_ratings"),
            "facts": count(reader, namespace, "fact_ratings"),
        }
    )
    actual = {
        "dag": run.get("state"),
        "tasks": states(tasks),
        "bronze_try": tasks["spark_raw_to_bronze"].get("try_number"),
        **counted,
        "marker": marker_exists(namespace, batch_id),
    }
    ok = (
        run.get("state") == "success"
        and actual["bronze_try"] >= 2
        and actual["users"] == 2
        and actual["ratings"] == 1
        and actual["facts"] == 1
        and actual["marker"] is True
    )
    return _case("5 spark retry", "lần hai không nhân đôi users", actual, ok)


def case_backfill() -> dict:
    namespace = "it_af_backfill"
    run = None
    logical = ""
    day = 16
    last_error: Exception | None = None
    for day in range(16, 28):
        logical = f"2026-01-{day:02d}T04:00:00+00:00"
        try:
            run = trigger(_run_conf("it-airflow/backfill", namespace), logical_date=logical)
            break
        except RuntimeError as exc:
            if "409" not in str(exc):
                raise
            last_error = exc
            print(f"logical date {logical} đã có run, thử ngày kế", flush=True)
    if run is None:
        raise RuntimeError(f"không tạo được backfill: {last_error}")
    # Cron 0 2 * * *: logical date 04:00 nằm sau mốc 02:00 cùng ngày,
    # nên data interval kết thúc 02:00 ngày đó và bắt đầu 02:00 ngày hôm trước.
    batch_id = local_batch_id("it-airflow/backfill")
    marker = read_marker(namespace, batch_id)
    counted = read_iceberg(
        lambda reader: {
            "facts": count(reader, namespace, "fact_ratings"),
            "users": count(reader, namespace, "raw_users"),
        }
    )
    actual = {
        "dag": run.get("state"),
        "logical_date": run.get("logical_date"),
        "data_interval_start": run.get("data_interval_start"),
        "marker_interval": None if not marker else marker.get("data_interval_start"),
        "marker_effective": None if not marker else marker.get("effective_ts"),
        **counted,
    }
    interval = str(actual["marker_interval"] or "")
    expected_start = f"2026-01-{day - 1:02d}T02:00:00"
    ok = (
        run.get("state") == "success"
        and str(actual["logical_date"]).startswith(logical[:10])
        and interval.startswith(expected_start)
        and str(actual["marker_effective"]).startswith(expected_start)
        and actual["facts"] == 1
        and actual["users"] == 2
    )
    return _case(
        "7 backfill",
        f"logical date {logical}, effective_ts lấy data_interval_start {expected_start}Z",
        actual,
        ok,
    )


def reset_namespaces(spark, names: list[str]) -> None:
    """Xóa bảng Iceberg của namespace test để lần chạy sau không append đè dữ liệu cũ."""
    pending: list[tuple[str, str]] = []
    for namespace in names:
        if not namespace.startswith("it_af_"):
            raise RuntimeError(namespace)
        pending.extend((namespace, table) for table in _namespace_tables(namespace))
    if not pending:
        print("reset: không có bảng cũ", flush=True)
        return
    for namespace, table in pending:
        spark.sql(f"DROP TABLE IF EXISTS cinestream.{namespace}.{table} PURGE")
        print(f"drop cinestream.{namespace}.{table}", flush=True)


def _namespace_tables(namespace: str) -> list[str]:
    request = urllib.request.Request(f"http://127.0.0.1:8181/v1/namespaces/{namespace}/tables")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return []
        raise
    return [item["name"] for item in body.get("identifiers") or []]


def seed_all(spark, root: Path) -> None:
    seed_happy(spark, root, "ok")
    seed_happy(spark, root, "retry")
    seed_happy(spark, root, "backfill")
    seed_contract(spark, root)
    seed_missing(spark, root)


def seed_happy(spark, root: Path, name: str) -> None:
    source = _source(root, name)
    _dimensions(spark, source)
    _write_ratings(spark, source / "ratings" / "dt=2026-01-15", [_rating(1, 4.0, TS1, DAY1)])
    _write_watch(spark, source / "watch_history" / "dt=2026-01-15", [_watch(1, 600, 0.5, TS1, DAY1)])
    upload_tree(source, f"it-airflow/{name}")


def seed_contract(spark, root: Path) -> None:
    source = _source(root, "contract")
    _dimensions(spark, source)
    ratings = [
        _rating(index, value, TS1, DAY1, movie_id=movie_id, user_id=user_id)
        for index, (user_id, movie_id, value) in enumerate(_grains(8), start=1)
    ]
    ratings.append(_rating(9, 9.0, TS1, DAY1))
    ratings.append(_rating(10, 0.1, TS1, DAY1))
    _write_ratings(spark, source / "ratings" / "dt=2026-01-15", ratings)
    _write_watch(spark, source / "watch_history" / "dt=2026-01-15", [_watch(1, 600, 0.5, TS1, DAY1)])
    upload_tree(source, "it-airflow/contract")


def seed_missing(spark, root: Path) -> None:
    source = _source(root, "missing")
    _user_file(spark, source / "users" / "base", 7, "Free")
    upload_tree(source, "it-airflow/missing")


def upload_tree(local: Path, prefix: str) -> None:
    client = s3()
    _delete_prefix(client, "external-source-lake", prefix)
    for path in sorted(local.rglob("*.parquet")):
        key = f"{prefix}/{path.relative_to(local).as_posix()}"
        client.upload_file(str(path), "external-source-lake", key)


def _delete_prefix(client, bucket: str, prefix: str) -> None:
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix.rstrip("/") + "/"}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        keys = [{"Key": item["Key"]} for item in page.get("Contents") or []]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        if not page.get("IsTruncated"):
            return
        token = page.get("NextContinuationToken")


def local_batch_id(prefix: str) -> str:
    client = s3()
    objects = []
    token = None
    while True:
        kwargs = {"Bucket": "external-source-lake", "Prefix": prefix.rstrip("/") + "/"}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for item in page.get("Contents") or []:
            objects.append({"key": item["Key"], "size": item["Size"], "etag": item["ETag"]})
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    found = assess_listing(objects, prefix)
    if not found["ready"]:
        raise RuntimeError(found["reason"])
    return found["batch_id"]


def _run_conf(prefix: str, namespace: str, **extra) -> dict:
    if not namespace.startswith("it_af_"):
        raise RuntimeError(namespace)
    body = {
        "source_dir": f"s3a://external-source-lake/{prefix}",
        "warehouse": "s3a://lakehouse",
        "bronze_namespace": namespace,
        "silver_namespace": namespace,
        "gold_namespace": namespace,
        "iceberg_rest": "http://iceberg-rest:8181",
        "s3_endpoint": "http://minio:9000",
        "error_threshold": 0.05,
        "source_timeout_seconds": 60,
        "source_poke_seconds": 5,
        "fail_once": False,
    }
    body.update(extra)
    return body


def trigger(conf: dict, logical_date: str | None = None, timeout: int = 1200) -> dict:
    body: dict = {"conf": conf}
    if logical_date:
        body["logical_date"] = logical_date
    print(f"trigger namespace={conf.get('bronze_namespace')} logical={logical_date}", flush=True)
    created = api("POST", f"/dags/{DAG_ID}/dagRuns", body)
    run_id = created["dag_run_id"]
    deadline = time.time() + timeout
    current = created
    while current.get("state") in {None, "queued", "running"}:
        if time.time() > deadline:
            raise TimeoutError(run_id)
        time.sleep(5)
        current = api("GET", f"/dags/{DAG_ID}/dagRuns/{quote(run_id, safe='')}")
    print(f"run {run_id} -> {current.get('state')}", flush=True)
    return current


def task_map(run: dict) -> dict:
    tasks = {}
    for _ in range(8):
        payload = api("GET", f"/dags/{DAG_ID}/dagRuns/{quote(run['dag_run_id'], safe='')}/taskInstances")
        tasks = {item["task_id"]: item for item in payload.get("task_instances") or []}
        current = [tasks.get(name, {}).get("state") for name in ORDER]
        if all(state in {"success", "failed", "upstream_failed", "skipped"} for state in current):
            return tasks
        time.sleep(1)
    return tasks


def states(tasks: dict) -> dict:
    return {name: tasks.get(name, {}).get("state") for name in ORDER}


def pause_dag() -> None:
    """Scheduler bỏ qua DAG đang pause, kể cả lần trigger tay."""
    api("PATCH", f"/dags/{DAG_ID}", {"is_paused": False})
    payload = api("GET", f"/dags/{DAG_ID}/dagRuns?limit=50")
    for run in payload.get("dag_runs") or []:
        if run.get("state") in {"queued", "running"}:
            try:
                api("DELETE", f"/dags/{DAG_ID}/dagRuns/{quote(run['dag_run_id'], safe='')}")
            except RuntimeError as exc:
                print(f"giữ lại dag run {run['dag_run_id']}: {exc}", flush=True)


def dagbag() -> dict:
    script = r"""
import json
import os
from airflow.models.dagbag import DagBag
bag = DagBag(dag_folder="/opt/cinestream/orchestration/dags", include_examples=False)
errors = {str(key): str(value) for key, value in bag.import_errors.items()}
dag = bag.dags.get("cinestream_daily_batch_lakehouse")
if dag is None:
    print("DAGBAG_JSON=" + json.dumps({"import_errors": errors, "task_ids": []}))
    raise SystemExit(0)
print("DAGBAG_JSON=" + json.dumps({
    "import_errors": errors,
    "task_ids": [task.task_id for task in dag.tasks],
    "downstream": {task.task_id: sorted(task.downstream_task_ids) for task in dag.tasks},
    "retries": {task.task_id: task.retries for task in dag.tasks},
    "catchup": dag.catchup,
    "max_active_runs": dag.max_active_runs,
    "schedule": str(dag.schedule_interval),
    "timetable": getattr(dag.timetable, "summary", ""),
    "intended_schedule": os.environ.get("CINESTREAM_BATCH_SCHEDULE", ""),
    "allow_production": os.environ.get("CINESTREAM_ALLOW_PRODUCTION", ""),
}))
"""
    import subprocess

    completed = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(REPO / "infra/compose/docker-compose.yml"),
            "-f",
            str(REPO / "infra/compose/docker-compose.airflow.yml"),
            "exec",
            "-T",
            "airflow-scheduler",
            "python",
            "-c",
            script,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    text = (completed.stdout or "") + "\n" + (completed.stderr or "")
    for line in text.splitlines():
        if line.startswith("DAGBAG_JSON="):
            return json.loads(line.split("=", 1)[1])
    return {"import_errors": {"exec": text[-2000:]}, "task_ids": []}


def s3():
    return boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def read_marker(namespace: str, batch_id: str):
    try:
        body = s3().get_object(Bucket="lakehouse", Key=f"_orchestration/{namespace}/{batch_id}.json")
    except Exception:
        return None
    return json.loads(body["Body"].read().decode())


def marker_exists(namespace: str, batch_id: str) -> bool:
    if not batch_id:
        listed = s3().list_objects_v2(Bucket="lakehouse", Prefix=f"_orchestration/{namespace}/")
        return bool(listed.get("Contents"))
    return read_marker(namespace, batch_id) is not None


def production_fingerprint() -> dict:
    found = {}
    for path in PRODUCTION:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/{path}", timeout=5) as response:
                found[path] = response.read().decode()
        except Exception as exc:
            found[path] = f"unavailable:{exc.__class__.__name__}"
    return found


def api(method: str, path: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Basic " + base64.b64encode(b"airflow:airflow").decode(),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()
        raise RuntimeError(f"{method} {path} -> {exc.code} {detail}") from exc
    return json.loads(raw) if raw else {}


def wait_http(url: str) -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                if response.status < 500:
                    return
        except Exception:
            time.sleep(3)
            continue
        time.sleep(3)
    raise SystemExit(f"không tới {url}. Chạy make airflow-up trước.")


def wait_worker() -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            with urllib.request.urlopen("http://127.0.0.1:18080/json/", timeout=3) as response:
                body = json.loads(response.read().decode())
            if int(body.get("aliveworkers") or 0) >= 1:
                return
        except Exception:
            pass
        time.sleep(3)
    raise SystemExit("spark-worker chưa đăng ký với spark-master")


def _local_spark():
    parser = build_parser("seed airflow source")
    args = parser.parse_args([])
    args.no_ui = True
    args.shuffle_partitions = 2
    args.master = "local[*]"
    return start_spark("cinestream-airflow-seed", args)


def _iceberg_spark():
    parser = build_parser("verify airflow iceberg")
    args = parser.parse_args([])
    args.warehouse = "s3a://lakehouse"
    args.iceberg_rest = "http://127.0.0.1:8181"
    args.s3_endpoint = "http://127.0.0.1:9000"
    args.s3_access_key = "minioadmin"
    args.s3_secret_key = "minioadmin"
    args.no_ui = True
    args.shuffle_partitions = 2
    args.master = "local[*]"
    return start_spark("cinestream-airflow-verify", args)


def read_iceberg(reader_fn):
    return reader_fn(open_reader())


def open_reader():
    global _READER
    if _READER is None:
        _READER = _iceberg_spark()
    return _READER


def close_reader() -> None:
    global _READER
    if _READER is not None:
        _READER.stop()
        _READER = None


def ensure_buckets() -> None:
    client = s3()
    existing = {item["Name"] for item in client.list_buckets().get("Buckets", [])}
    for name in ("external-source-lake", "lakehouse"):
        if name not in existing:
            client.create_bucket(Bucket=name)


def count(spark, namespace: str, table: str) -> int:
    name = f"cinestream.{namespace}.{table}"
    last_error = None
    for _ in range(3):
        try:
            spark.sql(f"REFRESH TABLE {name}")
            return int(spark.table(name).count())
        except Exception as exc:
            last_error = exc
            text = str(exc)
            if "TABLE_OR_VIEW_NOT_FOUND" in text or "UNRESOLVED_RELATION" in text:
                return 0
            time.sleep(2)
    first = str(last_error).splitlines()[0] if last_error else ""
    print(f"không đọc được {name}: {first}", flush=True)
    return 0


def snap(spark, namespace: str, *tables: str) -> dict:
    found = {}
    for table in tables:
        name = f"cinestream.{namespace}.{table}"
        try:
            found[table] = snapshots(spark, name)
        except Exception:
            found[table] = []
    return found


def quality_rows(spark, namespace: str, batch_id: str) -> list:
    name = f"cinestream.{namespace}.quality_results"
    last_error = None
    for _ in range(5):
        try:
            spark.sql(f"REFRESH TABLE {name}")
            rows = spark.sql(
                f"""
                SELECT dataset, status, invalid_rows, total_rows
                FROM {name}
                WHERE batch_id = '{batch_id}'
                ORDER BY dataset
                """
            ).collect()
            return [
                (row["dataset"], row["status"], int(row["invalid_rows"] or 0), int(row["total_rows"] or 0))
                for row in rows
            ]
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    print(f"không đọc được {name}: {last_error}", flush=True)
    return []


def gold_invariants(spark, namespace: str) -> dict:
    def scalar(query: str) -> int:
        return int(spark.sql(query).collect()[0][0] or 0)

    gold = f"cinestream.{namespace}"
    for table in (
        "dim_users",
        "dim_movies",
        "fact_ratings",
        "fact_watch_sessions",
        "obt_movie_engagement",
        "stg_ratings",
        "stg_watch_sessions",
    ):
        spark.sql(f"REFRESH TABLE {gold}.{table}")
    return {
        "stg_ratings": count(spark, namespace, "stg_ratings"),
        "fact_ratings": count(spark, namespace, "fact_ratings"),
        "stg_watch": count(spark, namespace, "stg_watch_sessions"),
        "fact_watch": count(spark, namespace, "fact_watch_sessions"),
        "obt": count(spark, namespace, "obt_movie_engagement"),
        "users_without_one_current": scalar(
            f"""
            SELECT count(*) FROM (
                SELECT user_id FROM {gold}.dim_users
                GROUP BY user_id
                HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
            )
            """
        ),
        "movies_without_one_current": scalar(
            f"""
            SELECT count(*) FROM (
                SELECT movie_id FROM {gold}.dim_movies
                GROUP BY movie_id
                HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
            )
            """
        ),
        "user_overlaps": scalar(
            f"""
            SELECT count(*) FROM {gold}.dim_users a
            JOIN {gold}.dim_users b
              ON a.user_id = b.user_id AND a.user_sk < b.user_sk
            WHERE a.valid_from_ts < COALESCE(b.valid_to_ts, TIMESTAMP '9999-12-31 00:00:00')
              AND b.valid_from_ts < COALESCE(a.valid_to_ts, TIMESTAMP '9999-12-31 00:00:00')
            """
        ),
        "movie_overlaps": scalar(
            f"""
            SELECT count(*) FROM {gold}.dim_movies a
            JOIN {gold}.dim_movies b
              ON a.movie_id = b.movie_id AND a.movie_sk < b.movie_sk
            WHERE a.valid_from_ts < COALESCE(b.valid_to_ts, TIMESTAMP '9999-12-31 00:00:00')
              AND b.valid_from_ts < COALESCE(a.valid_to_ts, TIMESTAMP '9999-12-31 00:00:00')
            """
        ),
        "duplicate_rating_sk": scalar(
            f"SELECT count(*) - count(DISTINCT rating_sk) FROM {gold}.fact_ratings"
        ),
        "duplicate_session_sk": scalar(
            f"SELECT count(*) - count(DISTINCT session_sk) FROM {gold}.fact_watch_sessions"
        ),
        "orphan_user_sk": scalar(
            f"""
            SELECT count(*) FROM {gold}.fact_ratings f
            LEFT JOIN {gold}.dim_users d ON f.user_sk = d.user_sk
            WHERE d.user_sk IS NULL
            """
        ),
        "orphan_movie_sk": scalar(
            f"""
            SELECT count(*) FROM {gold}.fact_ratings f
            LEFT JOIN {gold}.dim_movies d ON f.movie_sk = d.movie_sk
            WHERE d.movie_sk IS NULL
            """
        ),
    }


def _case(name: str, expected: str, actual: dict, ok: bool) -> dict:
    if not ok:
        print(f"CASE FAIL {name} {json.dumps(actual, default=str)[:2500]}", flush=True)
    return {"name": name, "status": "PASS" if ok else "FAIL", "expected": expected, "actual": actual}


if __name__ == "__main__":
    main()
