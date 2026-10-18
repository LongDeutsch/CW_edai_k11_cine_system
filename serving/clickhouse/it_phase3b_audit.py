"""Kiểm tra phục hồi và endpoint vận hành của Phase 3B.

Namespace ``it_audit_gold``, database ``it_audit``. Không ghi bronze/silver/gold.
Retry ở đây là lần gọi thứ hai của cùng worker mà task ``sync_clickhouse`` chạy lại.
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from serving.clickhouse.it_phase3a import _fingerprint, _v1, _v2, _v3
from serving.clickhouse.sync_gold import (
    ServingSyncError,
    _read_snapshot,
    apply_ddl,
    clickhouse,
    sync_published,
)
from transformation.batch.common import ICEBERG_CATALOG, build_parser, start_spark
from transformation.batch.jobs.table_updates import create_or_overwrite, snapshots

HTTP = "http://127.0.0.1:18123"
API = "http://127.0.0.1:18084"
DOWN_API = "http://127.0.0.1:18085"
DATABASE = "it_audit"
NAMESPACE = "it_audit_gold"
MARKER_NS = "it_audit_marker"
REPO = Path(__file__).resolve().parents[2]
REPORT = REPO / "docs" / "proof" / "logs" / "phase3b_audit_it.json"


def main() -> None:
    _wait_clickhouse()
    run_id = "audit-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before = _fingerprint()
    cases = []
    parser = build_parser("seed audit gold")
    args = parser.parse_args([])
    args.no_ui = True
    args.shuffle_partitions = 2
    args.warehouse = "s3a://lakehouse"
    spark = start_spark("cinestream-audit-it", args)
    server = None
    down = None
    try:
        _reset(spark)
        server = _start_api(API, 18084, HTTP)
        cases.append(_case_ops_empty())
        versions = _write_versions(spark)
        cases.append(_case_publish_and_latency(spark, versions, run_id))
        cases.append(_case_stage_failure(spark, versions))
        cases.append(_case_log_gap_then_retry(spark, versions))
        cases.append(_case_switch(spark, versions))
        cases.append(_case_torn(spark, versions))
        down = _start_api(DOWN_API, 18085, "http://127.0.0.1:9")
        cases.append(_case_clickhouse_down())
        cases.append(_case_dag_retry_contract())
    finally:
        for process in (server, down):
            if process is not None:
                process.terminate()
                process.wait(timeout=10)
        spark.stop()
    after = _fingerprint()
    unchanged = before == after
    cases.append(
        {
            "name": "production untouched",
            "status": "PASS" if unchanged else "FAIL",
            "expected": "snapshot production không đổi",
            "actual": {"before": before, "after": after, "unchanged": unchanged},
        }
    )
    failed = [item["name"] for item in cases if item["status"] != "PASS"]
    report = {
        "status": "FAIL" if failed else "PASS",
        "run_id": run_id,
        "namespace": NAMESPACE,
        "database": DATABASE,
        "failed": failed,
        "cases": cases,
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    if failed:
        raise SystemExit(f"FAIL {failed}")


def _case_ops_empty() -> dict:
    health = _get(API, "/health")
    ready = _get(API, "/ready")
    overview = _get(API, "/analytics/overview")
    status = _get(API, "/serving/status")
    bad_limit = _get(API, "/movies/top-rated?limit=0")
    bad_count = _get(API, "/movies/top-rated?min_rating_count=0")
    bad_type = _get(API, "/movies/most-watched?limit=abc")
    ok = (
        health["status"] == 200
        and health["body"]["status"] == "ok"
        and ready["status"] == 200
        and ready["body"]["database"] == DATABASE
        and overview["status"] == 200
        and overview["body"]["session_count"] == 0
        and overview["body"]["mean_rating"] is None
        and status["status"] == 200
        and status["body"]["consistent"] is True
        and status["body"]["log_present"] is False
        and status["body"]["engagement_rows"] == 0
        and bad_limit["status"] == 422
        and bad_count["status"] == 422
        and bad_type["status"] == 422
    )
    return _case(
        "endpoint vận hành khi serving rỗng",
        "health và ready 200, overview rỗng 200, limit sai 422",
        {"health": health, "ready": ready, "overview": overview["body"], "status": status["body"], "bad": [bad_limit["status"], bad_count["status"], bad_type["status"]]},
        ok,
    )


def _case_publish_and_latency(spark, versions: dict, run_id: str) -> dict:
    _put_marker("it-audit-v1", versions["v1"])
    started = time.perf_counter()
    result = sync_published(spark, batch_id="it-audit-v1", marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
    sync_ms = (time.perf_counter() - started) * 1000
    overview = _get(API, "/analytics/overview")
    iceberg = _iceberg_counts(spark, versions["v1"])
    samples = []
    for _ in range(30):
        t0 = time.perf_counter()
        item = _get(API, "/analytics/overview")
        samples.append((time.perf_counter() - t0) * 1000)
        if item["status"] != 200:
            break
    ordered = sorted(samples)
    latency = {
        "n": len(samples),
        "min_ms": round(ordered[0], 3),
        "p50_ms": round(ordered[len(ordered) // 2], 3),
        "mean_ms": round(statistics.fmean(samples), 3),
        "max_ms": round(ordered[-1], 3),
    }
    body = overview["body"]
    ok = (
        result["status"] == "published"
        and overview["status"] == 200
        and body["engagement_snapshot_id"] == versions["v1"]["obt_movie_engagement"]
        and body["ratings_snapshot_id"] == versions["v1"]["fact_ratings"]
        and body["session_count"] == iceberg["sessions"] == 2
        and body["rating_count"] == iceberg["ratings"] == 2
        and body["mean_rating"] == 4.5
        and body["batch_id"] == "it-audit-v1"
        and body["published_at"]
        and result["current_snapshots"]["obt_movie_engagement"] == versions["v3"]["obt_movie_engagement"]
        and all(item == 200 for item in [_get(API, "/analytics/overview")["status"]])
        and len(samples) == 30
    )
    return _case(
        "snapshot marker khớp Iceberg và API",
        "API trả v1 trong khi current là v3, số dòng bằng VERSION AS OF",
        {"run_id": run_id, "sync_ms": round(sync_ms, 3), "requested": versions["v1"], "current": result["current_snapshots"], "overview": body, "iceberg": iceberg, "latency_ms": latency},
        ok,
    )


def _case_stage_failure(spark, versions: dict) -> dict:
    before = _get(API, "/analytics/overview")["body"]
    _put_marker("it-audit-v2", versions["v2"])
    try:
        sync_published(
            spark,
            batch_id="it-audit-v2",
            marker_namespace=MARKER_NS,
            http=HTTP,
            database=DATABASE,
            fail_after_rows=1,
        )
        error = ""
    except ServingSyncError as exc:
        error = str(exc)
    during = _get(API, "/analytics/overview")["body"]
    stage_rows = int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.movie_engagement_stage").strip())
    ok = (
        "dừng giữa bulk load" in error
        and during["engagement_snapshot_id"] == before["engagement_snapshot_id"]
        and during["session_count"] == 2
        and during["batch_id"] == "it-audit-v1"
        and stage_rows == 1
    )
    return _case(
        "stage load lỗi giữ reader cũ",
        "API vẫn v1, stage còn 1 dòng",
        {"error": error, "before_batch": before["batch_id"], "during": during, "stage_rows": stage_rows},
        ok,
    )


def _case_log_gap_then_retry(spark, versions: dict) -> dict:
    try:
        sync_published(
            spark,
            batch_id="it-audit-v2",
            marker_namespace=MARKER_NS,
            http=HTTP,
            database=DATABASE,
            fail_before_log=True,
        )
        error = ""
    except ServingSyncError as exc:
        error = str(exc)
    gap = _get(API, "/analytics/overview")
    status = _get(API, "/serving/status")
    log_rows = int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.sync_log").strip())
    started = time.perf_counter()
    retry = sync_published(spark, batch_id="it-audit-v2", marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
    retry_ms = (time.perf_counter() - started) * 1000
    after = _get(API, "/analytics/overview")["body"]
    iceberg = _iceberg_counts(spark, versions["v2"])
    distinct_sessions = int(clickhouse(HTTP, f"SELECT uniqExact(session_sk) FROM {DATABASE}.movie_engagement").strip())
    again = sync_published(spark, batch_id="it-audit-v2", marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
    log_after = int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.sync_log").strip())
    ok = (
        "dừng trước sync_log" in error
        and gap["status"] == 200
        and gap["body"]["engagement_snapshot_id"] == versions["v2"]["obt_movie_engagement"]
        and gap["body"]["ratings_snapshot_id"] == versions["v2"]["fact_ratings"]
        and gap["body"]["published_at"] is None
        and gap["body"]["session_count"] == 2
        and status["body"]["consistent"] is True
        and status["body"]["log_present"] is False
        and log_rows == 1
        and retry["status"] == "logged"
        and retry["engagement_rows"] == iceberg["sessions"] == 2
        and after["rating_count"] == iceberg["ratings"] == 2
        and after["published_at"]
        and after["mean_rating"] == 2.5
        and distinct_sessions == after["session_count"]
        and again["status"] == "unchanged"
        and log_after == 2
    )
    return _case(
        "reader đã publish nhưng thiếu sync_log",
        "API thấy v2 với published_at null, retry chỉ ghi log, lần sau unchanged",
        {
            "error": error,
            "gap": gap["body"],
            "status": status["body"],
            "log_rows_during": log_rows,
            "retry": {"status": retry["status"], "ms": round(retry_ms, 3), "rows": retry["engagement_rows"]},
            "after": after,
            "iceberg": iceberg,
            "distinct_sessions": distinct_sessions,
            "second": again["status"],
            "log_rows_after": log_after,
            "snapshots": {"obt": versions["v2"]["obt_movie_engagement"], "fact": versions["v2"]["fact_ratings"]},
        },
        ok,
    )


def _case_switch(spark, versions: dict) -> dict:
    _put_marker("it-audit-v3", versions["v3"])
    seen = []
    stop = threading.Event()

    def read() -> None:
        while not stop.is_set():
            item = _get(API, "/analytics/overview")
            body = item["body"] if isinstance(item["body"], dict) else {}
            seen.append(
                {
                    "status": item["status"],
                    "engagement": body.get("engagement_snapshot_id"),
                    "ratings": body.get("ratings_snapshot_id"),
                    "batch_id": body.get("batch_id"),
                }
            )

    threads = [threading.Thread(target=read) for _ in range(4)]
    for thread in threads:
        thread.start()
    time.sleep(0.2)
    result = sync_published(spark, batch_id="it-audit-v3", marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
    time.sleep(0.2)
    stop.set()
    for thread in threads:
        thread.join(timeout=5)
    v2 = (versions["v2"]["obt_movie_engagement"], versions["v2"]["fact_ratings"], "it-audit-v2")
    v3 = (versions["v3"]["obt_movie_engagement"], versions["v3"]["fact_ratings"], "it-audit-v3")
    pairs = {}
    mixed_200 = []
    for item in seen:
        key = item["status"]
        pairs[key] = pairs.get(key, 0) + 1
        if item["status"] == 200:
            pair = (item["engagement"], item["ratings"], item["batch_id"])
            if pair not in {v2, v3}:
                mixed_200.append(pair)
    after = _get(API, "/analytics/overview")["body"]
    ok = (
        result["status"] == "published"
        and not mixed_200
        and pairs.get(500, 0) == 0
        and after["engagement_snapshot_id"] == versions["v3"]["obt_movie_engagement"]
        and after["ratings_snapshot_id"] == versions["v3"]["fact_ratings"]
        and after["session_count"] == 3
        and after["batch_id"] == "it-audit-v3"
        and len(seen) > 0
    )
    return _case(
        "đọc song song lúc đổi v2 sang v3",
        "mỗi 200 là một cặp đã publish, không có 200 trộn hai snapshot",
        {"reads": len(seen), "codes": pairs, "mixed_200": mixed_200[:5], "after": after, "v2": v2, "v3": v3},
        ok,
    )


def _case_torn(spark, versions: dict) -> dict:
    clickhouse(
        HTTP,
        f"""
        INSERT INTO {DATABASE}.movie_ratings
            (movie_id, title, content_rating, rating_count, avg_rating, iceberg_snapshot_id, batch_id)
        VALUES (999, 'torn', 'PG', 1, 1.0, 1, 'torn')
        """,
    )
    overview = _get(API, "/analytics/overview")
    status = _get(API, "/serving/status")
    top = _get(API, "/movies/top-rated")
    repaired = sync_published(spark, batch_id="it-audit-v3", marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
    after = _get(API, "/serving/status")["body"]
    ok = (
        overview["status"] == 409
        and status["status"] == 200
        and status["body"]["consistent"] is False
        and top["status"] == 409
        and repaired["status"] == "published"
        and after["consistent"] is True
        and after["ratings_snapshot_id"] == versions["v3"]["fact_ratings"]
        and after["log_present"] is True
    )
    return _case(
        "hai reader không được trả trộn",
        "overview và top-rated 409, status 200 consistent false, sync lại thì hết trộn",
        {"overview": overview, "status": status["body"], "top": top["status"], "repair": repaired["status"], "after": after},
        ok,
    )


def _case_clickhouse_down() -> dict:
    health = _get(DOWN_API, "/health")
    ready = _get(DOWN_API, "/ready")
    overview = _get(DOWN_API, "/analytics/overview")
    status = _get(DOWN_API, "/serving/status")
    watched = _get(DOWN_API, "/movies/most-watched")
    ok = (
        health["status"] == 200
        and ready["status"] == 503
        and overview["status"] == 503
        and status["status"] == 503
        and watched["status"] == 503
    )
    return _case(
        "ClickHouse không tới được",
        "health 200, ready và analytics 503",
        {"health": health["status"], "ready": ready, "overview": overview["status"], "status": status["status"], "watched": watched["status"]},
        ok,
    )


def _case_dag_retry_contract() -> dict:
    script = r"""
import json, os
from airflow.models.dagbag import DagBag
bag = DagBag(dag_folder="/opt/cinestream/orchestration/dags", include_examples=False)
dag = bag.dags.get("cinestream_daily_batch_lakehouse")
payload = {"import_errors": {str(k): str(v) for k, v in bag.import_errors.items()}}
if dag is not None:
    task = dag.get_task("sync_clickhouse")
    payload.update({
        "task_id": task.task_id,
        "retries": task.retries,
        "downstream_of_publish": sorted(dag.get_task("publish_success").downstream_task_ids),
        "schedule": str(dag.schedule_interval),
        "clickhouse_http": os.environ.get("CINESTREAM_CLICKHOUSE_HTTP", ""),
    })
print("DAGBAG_JSON=" + json.dumps(payload))
"""
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
    found = {}
    for line in text.splitlines():
        if line.startswith("DAGBAG_JSON="):
            found = json.loads(line.split("=", 1)[1])
    ok = (
        not found.get("import_errors")
        and found.get("task_id") == "sync_clickhouse"
        and found.get("retries") == 1
        and found.get("downstream_of_publish") == ["sync_clickhouse"]
        and found.get("schedule") == "None"
    )
    return _case(
        "Airflow retry của sync_clickhouse",
        "task id là sync_clickhouse, retry 1, đứng sau publish_success",
        found or {"stderr": text[-500:]},
        ok,
    )


def _iceberg_counts(spark, snaps: dict) -> dict:
    sessions, _ = _read_snapshot(spark, NAMESPACE, "obt_movie_engagement", snaps["obt_movie_engagement"])
    ratings, _ = _read_snapshot(spark, NAMESPACE, "fact_ratings", snaps["fact_ratings"])
    return {"sessions": sessions.count(), "ratings": ratings.count()}


def _write_versions(spark) -> dict:
    versions = {}
    for name, builder in (("v1", _v1), ("v2", _v2), ("v3", _v3)):
        frames = builder(spark)
        for table, frame in frames.items():
            create_or_overwrite(spark, frame, f"{ICEBERG_CATALOG}.{NAMESPACE}.{table}")
        versions[name] = {table: snapshots(spark, f"{ICEBERG_CATALOG}.{NAMESPACE}.{table}")[-1]["snapshot_id"] for table in frames}
    return versions


def _reset(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{NAMESPACE}")
    for table in ("dim_movies", "fact_ratings", "obt_movie_engagement"):
        spark.sql(f"DROP TABLE IF EXISTS {ICEBERG_CATALOG}.{NAMESPACE}.{table}")
    clickhouse(HTTP, f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
    apply_ddl(HTTP, DATABASE)
    client = _s3()
    listed = client.list_objects_v2(Bucket="lakehouse", Prefix=f"_orchestration/{MARKER_NS}/")
    keys = [{"Key": item["Key"]} for item in listed.get("Contents") or []]
    if keys:
        client.delete_objects(Bucket="lakehouse", Delete={"Objects": keys})


def _put_marker(batch_id: str, snaps: dict) -> None:
    payload = {
        "published": True,
        "batch_id": batch_id,
        "namespaces": {"bronze": MARKER_NS, "silver": NAMESPACE, "gold": NAMESPACE},
        "gold_snapshots": snaps,
    }
    _s3().put_object(
        Bucket="lakehouse",
        Key=f"_orchestration/{MARKER_NS}/{batch_id}.json",
        Body=json.dumps(payload).encode(),
        ContentType="application/json",
    )


def _s3():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _start_api(base: str, port: int, clickhouse_http: str) -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["CINESTREAM_CLICKHOUSE_DATABASE"] = DATABASE
    env["CINESTREAM_CLICKHOUSE_HTTP"] = clickhouse_http
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "serving.api.app:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=str(REPO),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            _get(base, "/health")
            return process
        except Exception:
            if process.poll() is not None:
                output = process.stdout.read().decode() if process.stdout else ""
                raise SystemExit(f"API không lên: {output[-1000:]}")
            time.sleep(0.3)
    raise SystemExit(f"API không lên tại {base}")


def _get(base: str, path: str) -> dict:
    request = urllib.request.Request(base + path)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            raw = response.read().decode()
            return {"status": response.status, "body": json.loads(raw) if raw else None}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = raw
        return {"status": exc.code, "body": body}


def _wait_clickhouse() -> None:
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            clickhouse(HTTP, "SELECT 1")
            return
        except Exception:
            time.sleep(2)
    raise SystemExit("ClickHouse chưa sẵn sàng. Chạy make serving-up trước.")


def _case(name: str, expected: str, actual: dict, ok: bool) -> dict:
    status = "PASS" if ok else "FAIL"
    print(f"CASE {status} {name}", flush=True)
    return {"name": name, "status": status, "expected": expected, "actual": actual}


if __name__ == "__main__":
    main()
