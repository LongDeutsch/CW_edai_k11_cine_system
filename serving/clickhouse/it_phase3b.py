"""API và lệnh sync Airflow trên Iceberg REST, MinIO và ClickHouse thật.

Namespace ``it_api_gold``, database ``it_api``. Không ghi bronze/silver/gold.
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
from pathlib import Path

from serving.clickhouse.airflow_sync import serving_database, sync_arguments
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
API = "http://127.0.0.1:18083"
DATABASE = "it_api"
NAMESPACE = "it_api_gold"
MARKER_NS = "it_api_marker"
REPO = Path(__file__).resolve().parents[2]
REPORT = REPO / "docs" / "proof" / "logs" / "phase3b_api_it.json"


def main() -> None:
    _wait_clickhouse()
    before = _fingerprint()
    cases = [_case_command()]
    parser = build_parser("seed api gold")
    args = parser.parse_args([])
    args.no_ui = True
    args.shuffle_partitions = 2
    args.warehouse = "s3a://lakehouse"
    spark = start_spark("cinestream-api-it", args)
    server = None
    try:
        _reset(spark)
        versions = _write_versions(spark)
        server = _start_api()
        cases.append(_case_empty_and_errors())
        cases.append(_case_initial(spark, versions))
        cases.append(_case_latency())
        cases.append(_case_resync(spark))
        cases.append(_case_concurrent(spark, versions))
        cases.append(_case_failure(spark, versions))
        cases.append(_case_bad_markers(spark, versions))
        cases.append(_case_torn_then_repair(spark, versions))
        cases.append(_case_dagbag())
    finally:
        if server is not None:
            server.terminate()
            server.wait(timeout=10)
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
    report = {"status": "FAIL" if failed else "PASS", "failed": failed, "cases": cases}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    if failed:
        raise SystemExit(f"FAIL {failed}")


def _case_command() -> dict:
    gold_args = sync_arguments(
        batch_id="it-api-v1",
        marker_namespace="bronze",
        gold_namespace="gold",
        warehouse="s3a://lakehouse",
        iceberg_rest="http://iceberg-rest:8181",
        s3_endpoint="http://minio:9000",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        clickhouse_http="http://clickhouse:8123",
    )
    test_args = sync_arguments(
        batch_id="it-api-v1",
        marker_namespace=MARKER_NS,
        gold_namespace=NAMESPACE,
        warehouse="s3a://lakehouse",
        iceberg_rest="http://127.0.0.1:8181",
        s3_endpoint="http://127.0.0.1:9000",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        clickhouse_http=HTTP,
        clickhouse_database=DATABASE,
    )
    actual = {
        "production_database": serving_database("gold", None),
        "isolated_database": serving_database(NAMESPACE, None),
        "override_database": serving_database("gold", DATABASE),
        "gold_has_snapshot_flag": any(item.startswith("--snapshot") for item in gold_args),
        "marker_namespace": test_args[test_args.index("--marker-namespace") + 1],
        "database": test_args[test_args.index("--database") + 1],
        "clickhouse_http": test_args[test_args.index("--clickhouse-http") + 1],
    }
    ok = (
        actual["production_database"] == "cinestream_serving"
        and actual["isolated_database"] == "it_it_api_gold"
        and actual["override_database"] == DATABASE
        and actual["gold_has_snapshot_flag"] is False
        and actual["marker_namespace"] == MARKER_NS
        and actual["database"] == DATABASE
        and actual["clickhouse_http"] == HTTP
    )
    return _case("tham số sync Airflow", "id snapshot chỉ đến từ marker, namespace test không ghi cinestream_serving", actual, ok)


def _case_empty_and_errors() -> dict:
    empty = _get("/analytics/overview")
    top = _get("/movies/top-rated")
    watched = _get("/movies/most-watched")
    bad_limit = _get("/movies/top-rated?limit=0")
    bad_type = _get("/movies/most-watched?limit=abc")
    ok = (
        empty["status"] == 200
        and empty["body"]["session_count"] == 0
        and empty["body"]["rating_count"] == 0
        and empty["body"]["mean_rating"] is None
        and empty["body"]["engagement_snapshot_id"] is None
        and top["status"] == 200
        and top["body"]["items"] == []
        and watched["status"] == 200
        and watched["body"]["items"] == []
        and bad_limit["status"] == 422
        and bad_type["status"] == 422
    )
    return _case(
        "dataset rỗng và tham số sai",
        "200 với số 0, limit sai thì 422",
        {"empty": empty, "top": top, "watched": watched, "bad_limit": bad_limit["status"], "bad_type": bad_type["status"]},
        ok,
    )


def _case_initial(spark, versions: dict) -> dict:
    _put_marker("it-api-v1", versions["v1"])
    synced = sync_published(spark, batch_id="it-api-v1", marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
    overview = _get("/analytics/overview")
    rated = _get("/movies/top-rated?limit=10")
    watched = _get("/movies/most-watched?limit=10")
    iceberg = _iceberg_counts(spark, versions["v1"])
    body = overview["body"]
    ok = (
        synced["status"] == "published"
        and overview["status"] == 200
        and body["engagement_snapshot_id"] == versions["v1"]["obt_movie_engagement"]
        and body["ratings_snapshot_id"] == versions["v1"]["fact_ratings"]
        and body["session_count"] == iceberg["sessions"] == 2
        and body["rating_count"] == iceberg["ratings"] == 2
        and body["batch_id"] == "it-api-v1"
        and body["published_at"]
        and [item["movie_id"] for item in rated["body"]["items"]] == [20, 10]
        and [item["avg_rating"] for item in rated["body"]["items"]] == [5.0, 4.0]
        and [item["movie_id"] for item in watched["body"]["items"]] == [10, 20]
        and [item["watch_seconds"] for item in watched["body"]["items"]] == [60, 30]
        and synced["current_snapshots"]["obt_movie_engagement"] == versions["v3"]["obt_movie_engagement"]
    )
    return _case(
        "publish rồi query API",
        "API khớp snapshot v1 trong marker, không phải current v3",
        {"sync": synced, "overview": body, "rated": rated["body"], "watched": watched["body"], "iceberg": iceberg},
        ok,
    )


def _case_latency() -> dict:
    samples = []
    for _ in range(30):
        started = time.perf_counter()
        response = _get("/analytics/overview")
        samples.append(round((time.perf_counter() - started) * 1000, 3))
        if response["status"] != 200:
            return _case("độ trễ API", "30 request overview", {"status": response["status"], "samples_ms": samples}, False)
    ordered = sorted(samples)
    actual = {
        "requests": len(samples),
        "min_ms": ordered[0],
        "p50_ms": ordered[len(ordered) // 2],
        "max_ms": ordered[-1],
        "mean_ms": round(statistics.fmean(samples), 3),
        "published_at": _get("/analytics/overview")["body"].get("published_at"),
    }
    return _case("độ trễ API", "đo 30 GET /analytics/overview trên localhost", actual, actual["max_ms"] > 0)


def _case_resync(spark) -> dict:
    before = _get("/analytics/overview")["body"]
    synced = sync_published(spark, batch_id="it-api-v1", marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
    after = _get("/analytics/overview")["body"]
    log_rows = int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.sync_log").strip())
    ok = synced["status"] == "unchanged" and before == after and log_rows == 1
    return _case("sync lại cùng snapshot", "API và sync_log không đổi", {"sync": synced, "before": before, "after": after, "log_rows": log_rows}, ok)


def _case_concurrent(spark, versions: dict) -> dict:
    _put_marker("it-api-v2", versions["v2"])
    stop = threading.Event()
    seen: list[dict] = []
    lock = threading.Lock()

    def loop() -> None:
        while not stop.is_set():
            started = time.perf_counter()
            response = _get("/analytics/overview")
            item = {"status": response["status"], "ms": round((time.perf_counter() - started) * 1000, 3)}
            if response["status"] == 200:
                item["engagement_snapshot_id"] = response["body"]["engagement_snapshot_id"]
                item["ratings_snapshot_id"] = response["body"]["ratings_snapshot_id"]
                item["session_count"] = response["body"]["session_count"]
            elif response["status"] == 409:
                item["detail"] = response["body"]
            with lock:
                seen.append(item)

    threads = [threading.Thread(target=loop) for _ in range(4)]
    for thread in threads:
        thread.start()
    time.sleep(0.2)
    synced = sync_published(spark, batch_id="it-api-v2", marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
    time.sleep(0.2)
    stop.set()
    for thread in threads:
        thread.join()
    v1 = versions["v1"]["obt_movie_engagement"]
    v2 = versions["v2"]["obt_movie_engagement"]
    ok_pairs = [
        item
        for item in seen
        if item["status"] == 200 and item["engagement_snapshot_id"] in {v1, v2} and item["ratings_snapshot_id"] in {
            versions["v1"]["fact_ratings"] if item["engagement_snapshot_id"] == v1 else versions["v2"]["fact_ratings"],
        }
    ]
    torn = [item for item in seen if item["status"] == 409]
    unavailable = [item for item in seen if item["status"] == 503]
    other = [item for item in seen if item not in ok_pairs and item not in torn and item not in unavailable]
    after = _get("/movies/most-watched")
    ok = (
        synced["status"] == "published"
        and not other
        and any(item["engagement_snapshot_id"] == v2 for item in ok_pairs)
        and [item["movie_id"] for item in after["body"]["items"]] == [10, 30]
    )
    return _case(
        "đọc API trong lúc publish",
        "response 200 chỉ thấy một cặp snapshot đã publish; 409 nếu hai bảng lệch",
        {
            "reads": len(seen),
            "consistent_200": len(ok_pairs),
            "conflict_409": len(torn),
            "unavailable_503": len(unavailable),
            "other": other[:5],
            "watched": after["body"],
        },
        ok,
    )


def _case_failure(spark, versions: dict) -> dict:
    _put_marker("it-api-v3", versions["v3"])
    before = _get("/analytics/overview")["body"]
    try:
        sync_published(
            spark,
            batch_id="it-api-v3",
            marker_namespace=MARKER_NS,
            http=HTTP,
            database=DATABASE,
            fail_after_rows=1,
        )
        failed = "không dừng"
    except ServingSyncError as exc:
        failed = str(exc)
    during = _get("/analytics/overview")["body"]
    stage = int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.movie_engagement_stage").strip())
    synced = sync_published(spark, batch_id="it-api-v3", marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
    after = _get("/analytics/overview")["body"]
    iceberg = _iceberg_counts(spark, versions["v3"])
    ok = (
        "dừng giữa bulk load" in failed
        and during["engagement_snapshot_id"] == before["engagement_snapshot_id"] == versions["v2"]["obt_movie_engagement"]
        and during["session_count"] == before["session_count"] == 2
        and stage == 1
        and synced["status"] == "published"
        and after["session_count"] == iceberg["sessions"] == 3
        and after["engagement_snapshot_id"] == versions["v3"]["obt_movie_engagement"]
    )
    return _case(
        "sync lỗi rồi retry",
        "API giữ v2 khi stage dở, retry mới trả v3",
        {"failed": failed, "during": during, "stage_rows": stage, "after": after, "iceberg": iceberg},
        ok,
    )


def _case_bad_markers(spark, versions: dict) -> dict:
    before = _get("/analytics/overview")["body"]
    _put_marker("it-api-draft", versions["v3"], published=False)
    _put_marker("it-api-nosnap", versions["v3"], include_snapshots=False)
    bad = dict(versions["v3"])
    bad["obt_movie_engagement"] = 1
    _put_marker("it-api-badsnap", bad)
    found = {}
    for batch_id in ("it-api-missing", "it-api-draft", "it-api-nosnap", "it-api-badsnap"):
        try:
            sync_published(spark, batch_id=batch_id, marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
            found[batch_id] = "published"
        except ServingSyncError as exc:
            found[batch_id] = str(exc)
    after = _get("/analytics/overview")["body"]
    ok = before == after and all(token in found[batch_id] for batch_id, token in (
        ("it-api-missing", "không có marker"),
        ("it-api-draft", "marker chưa published"),
        ("it-api-nosnap", "marker thiếu gold_snapshots"),
        ("it-api-badsnap", "không có snapshot"),
    ))
    return _case("marker hỏng", "API vẫn là v3", {"errors": found, "overview": after}, ok)


def _case_torn_then_repair(spark, versions: dict) -> dict:
    clickhouse(
        HTTP,
        f"""
        INSERT INTO {DATABASE}.movie_ratings
            (movie_id, title, content_rating, rating_count, avg_rating, iceberg_snapshot_id, batch_id)
        VALUES (999, 'torn', 'PG', 1, 1.0, 1, 'torn')
        """,
    )
    torn = _get("/analytics/overview")
    repaired = sync_published(spark, batch_id="it-api-v3", marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
    after = _get("/analytics/overview")
    ok = (
        torn["status"] == 409
        and repaired["status"] == "published"
        and after["status"] == 200
        and after["body"]["ratings_snapshot_id"] == versions["v3"]["fact_ratings"]
        and after["body"]["rated_movie_count"] == 3
    )
    return _case(
        "reader lệch snapshot",
        "409 khi ratings bị trộn, sync lại v3 thì API khớp marker",
        {"torn": torn, "repair": repaired, "after": after["body"]},
        ok,
    )


def _case_dagbag() -> dict:
    script = r"""
import json, os
from airflow.models.dagbag import DagBag
bag = DagBag(dag_folder="/opt/cinestream/orchestration/dags", include_examples=False)
dag = bag.dags.get("cinestream_daily_batch_lakehouse")
payload = {"import_errors": {str(k): str(v) for k, v in bag.import_errors.items()}}
if dag is not None:
    payload.update({
        "downstream": {task.task_id: sorted(task.downstream_task_ids) for task in dag.tasks},
        "retries": dag.get_task("sync_clickhouse").retries,
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
    downstream = found.get("downstream") or {}
    ok = (
        not found.get("import_errors")
        and downstream.get("publish_success") == ["sync_clickhouse"]
        and downstream.get("sync_clickhouse") == []
        and found.get("retries") == 1
        and found.get("schedule") == "None"
    )
    return _case("DAG nối sync sau publish", "publish_success rồi sync_clickhouse, retry 1", found or {"stderr": text[-500:]}, ok)


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
        versions[name] = {
            table: snapshots(spark, f"{ICEBERG_CATALOG}.{NAMESPACE}.{table}")[-1]["snapshot_id"] for table in frames
        }
    return versions


def _reset(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{NAMESPACE}")
    for table in ("dim_movies", "fact_ratings", "obt_movie_engagement"):
        spark.sql(f"DROP TABLE IF EXISTS {ICEBERG_CATALOG}.{NAMESPACE}.{table}")
    clickhouse(HTTP, f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
    apply_ddl(HTTP, DATABASE)
    _delete_markers()


def _put_marker(batch_id: str, snaps: dict, *, published: bool = True, include_snapshots: bool = True) -> None:
    payload = {
        "published": published,
        "batch_id": batch_id,
        "namespaces": {"bronze": MARKER_NS, "silver": NAMESPACE, "gold": NAMESPACE},
    }
    if include_snapshots:
        payload["gold_snapshots"] = snaps
    client = _s3()
    client.put_object(
        Bucket="lakehouse",
        Key=f"_orchestration/{MARKER_NS}/{batch_id}.json",
        Body=json.dumps(payload).encode(),
        ContentType="application/json",
    )


def _delete_markers() -> None:
    client = _s3()
    listed = client.list_objects_v2(Bucket="lakehouse", Prefix=f"_orchestration/{MARKER_NS}/")
    keys = [{"Key": item["Key"]} for item in listed.get("Contents") or []]
    if keys:
        client.delete_objects(Bucket="lakehouse", Delete={"Objects": keys})


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


def _start_api() -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["CINESTREAM_CLICKHOUSE_DATABASE"] = DATABASE
    env["CINESTREAM_CLICKHOUSE_HTTP"] = HTTP
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "serving.api.app:app", "--host", "127.0.0.1", "--port", "18083"],
        cwd=str(REPO),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            _get("/analytics/overview")
            return process
        except Exception:
            if process.poll() is not None:
                output = process.stdout.read().decode() if process.stdout else ""
                raise SystemExit(f"API không lên: {output[-1000:]}")
            time.sleep(0.3)
    raise SystemExit("API không lên")


def _get(path: str) -> dict:
    request = urllib.request.Request(API + path)
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
