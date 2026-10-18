"""Nguồn Parquet thật trên MinIO, namespace Iceberg cô lập.

Không ghi bronze/silver/gold. ``it-airflow/ok`` không phải bằng chứng của file này.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from orchestration.it_phase2 import (
    REPO,
    _case,
    _run_conf,
    api,
    close_reader,
    local_batch_id,
    marker_exists,
    open_reader,
    pause_dag,
    production_fingerprint,
    reset_namespaces,
    s3,
    states,
    task_map,
    trigger,
    upload_tree,
    wait_http,
    wait_worker,
)

REPORT = Path(__file__).resolve().parents[1] / "docs" / "proof" / "logs" / "phase2_real_source_it.json"
REAL = "it_af_real"
RETRY = "it_af_real_retry"
BAD = "it_af_badts"
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
        spark_reader = open_reader()
        reset_namespaces(spark_reader, [BAD, REAL, RETRY])
        close_reader()
        for namespace in (BAD, REAL, RETRY):
            _delete_marker(namespace)
        root = Path(tempfile.mkdtemp(prefix="cinestream-real-source-"))
        seed_bad_schema(root)
        cases = [
            case_bad_schema(),
            case_real(),
        ]
        happy = cases[-1]
        cases.append(case_evolution_and_gold(happy))
        cases.append(case_rerun(happy))
        cases.append(case_retry())
        after = production_fingerprint()
        cases.append(
            _case(
                "production untouched",
                "snapshot production không đổi",
                {"unchanged": before == after},
                before == after,
            )
        )
        failed = [item["name"] for item in cases if item["status"] != "PASS"]
        report = {"status": "FAIL" if failed else "PASS", "failed": failed, "cases": cases}
    finally:
        close_reader()
        try:
            api("PATCH", "/dags/cinestream_daily_batch_lakehouse", {"is_paused": True})
        except Exception as exc:
            print(f"không pause lại được DAG: {exc}", flush=True)
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    if report.get("status") != "PASS":
        raise SystemExit(f"FAIL {report.get('failed')}")


def verify(action: str, namespace: str, batch_id: str = "") -> dict:
    """Spark local[1] trong airflow-scheduler, Java 17, không đụng bảng production."""
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
            "spark-submit",
            "--master",
            "local[1]",
            "/opt/cinestream/orchestration/cluster_verify.py",
            action,
            namespace,
            batch_id,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    text = (completed.stdout or "") + "\n" + (completed.stderr or "")
    for line in text.splitlines():
        if line.startswith("VERIFY_JSON="):
            return json.loads(line.split("=", 1)[1])
    raise RuntimeError(text[-2500:])


def case_bad_schema() -> dict:
    namespace = BAD
    run = trigger(_run_conf("it-airflow/bad-ts", namespace), timeout=600)
    tasks = task_map(run)
    batch_id = local_batch_id("it-airflow/bad-ts")
    counted = verify("bad", namespace, batch_id)
    actual = {
        "dag": run.get("state"),
        "tasks": states(tasks),
        "marker": marker_exists(namespace, batch_id),
        **counted,
    }
    rejected = any(row[1] == "schema_rejected" for row in actual["quality"])
    ok = (
        run.get("state") == "failed"
        and tasks["spark_raw_to_bronze"]["state"] == "failed"
        and tasks["spark_bronze_to_gold"]["state"] == "upstream_failed"
        and actual["users"] == 0
        and actual["facts"] == 0
        and actual["quarantine"] > 0
        and rejected
        and actual["marker"] is False
    )
    return _case("schema string bị chặn", "signup_ts string vào quarantine, không có fact", actual, ok)


def case_real() -> dict:
    namespace = REAL
    run = trigger(_run_conf("", namespace), timeout=3600)
    tasks = task_map(run)
    counted = verify("success", namespace)
    marker = _marker(namespace)
    actual = {
        "dag": run.get("state"),
        "tasks": states(tasks),
        "marker_published": bool(marker and marker.get("published") is True),
        **counted,
    }
    ok = (
        run.get("state") == "success"
        and all(tasks[name]["state"] == "success" for name in ORDER)
        and actual["users"] == 5000
        and actual["movies"] == 100
        and actual["ratings"] == 256410
        and actual["watches"] == 820513
        and actual["facts"] == 57665
        and actual["fact_watch"] == 662830
        and actual["signup_type"] == "timestamp"
        and actual["created_type"] == "timestamp"
        and actual["rating_type"] == "timestamp"
        and actual["marker_published"] is True
    )
    found = _case("nguồn thật", "đủ dòng, timestamp UTC, marker published", actual, ok)
    found["snapshots"] = counted["snapshots"]
    found["namespace"] = namespace
    return found


def case_evolution_and_gold(happy: dict) -> dict:
    if happy["status"] != "PASS":
        return _case("schema evolution và SCD2", "bỏ qua vì nguồn thật đã lỗi", {}, False)
    actual = verify("evolution", REAL)
    ok = (
        actual["movies"] == 100
        and actual["old_schema"] == 44
        and actual["evolved"] == 56
        and actual["users_without_one_current"] == 0
        and actual["movies_without_one_current"] == 0
        and actual["user_overlaps"] == 0
        and actual["movie_overlaps"] == 0
        and actual["orphan_user_sk"] == 0
        and actual["orphan_movie_sk"] == 0
        and actual["fact_ratings"] == actual["stg_ratings"] == 57665
        and actual["obt"] == actual["fact_watch"] == 662830
    )
    return _case("schema evolution và SCD2", "44 phim schema cũ null, một current, không overlap", actual, ok)


def case_rerun(happy: dict) -> dict:
    if happy["status"] != "PASS":
        return _case("rerun nguồn thật", "bỏ qua vì nguồn thật đã lỗi", {}, False)
    before = happy["snapshots"]
    run = trigger(_run_conf("", REAL), timeout=3600)
    counted = verify("rerun", REAL)
    after = counted["snapshots"]
    actual = {
        "dag": run.get("state"),
        "users": counted["users"],
        "ratings": counted["ratings"],
        "watches": counted["watches"],
        "snapshots_before": before,
        "snapshots_after": after,
    }
    ok = (
        run.get("state") == "success"
        and counted["users"] == 5000
        and counted["ratings"] == 256410
        and counted["watches"] == 820513
        and before.get("raw_users") == after.get("raw_users")
        and before.get("raw_ratings") == after.get("raw_ratings")
        and before.get("raw_watch_history") == after.get("raw_watch_history")
    )
    return _case("rerun nguồn thật", "cùng file, Bronze không thêm snapshot", actual, ok)


def case_retry() -> dict:
    run = trigger(_run_conf("", RETRY, fail_once=True), timeout=3600)
    tasks = task_map(run)
    counted = verify("retry", RETRY)
    actual = {
        "dag": run.get("state"),
        "tasks": states(tasks),
        "bronze_try": tasks["spark_raw_to_bronze"].get("try_number"),
        **counted,
        "marker": _marker(RETRY) is not None and bool(_marker(RETRY).get("published")),
    }
    ok = (
        run.get("state") == "success"
        and actual["bronze_try"] >= 2
        and actual["users"] == 5000
        and actual["ratings"] == 256410
        and actual["facts"] == 57665
        and actual["marker"] is True
    )
    return _case("retry nguồn thật", "fail sau users, lần hai không nhân đôi", actual, ok)


def seed_bad_schema(root: Path) -> None:
    base = root / "bad-ts"
    _write(
        base / "users" / "part.parquet",
        pa.table(
            {
                "user_id": pa.array([1], type=pa.int64()),
                "signup_ts": pa.array(["2018-01-01T00:00:00"]),
                "country": ["VN"],
                "subscription_tier": ["Free"],
                "preferred_language": ["vi"],
                "age_group": ["18-24"],
                "gender": ["F"],
            }
        ),
    )
    created = pa.array([datetime(2010, 1, 1)], type=pa.timestamp("ms"))
    _write(
        base / "movies" / "part.parquet",
        pa.table(
            {
                "movie_id": pa.array([1], type=pa.int64()),
                "title": ["A"],
                "release_year": pa.array([2020], type=pa.int32()),
                "runtime_minutes": pa.array([90], type=pa.int32()),
                "original_language": ["en"],
                "budget": pa.array([1], type=pa.int64()),
                "director_id": pa.array([1], type=pa.int64()),
                "created_ts": created,
            }
        ),
    )
    event = pa.array([datetime(2026, 1, 15)], type=pa.date32())
    stamp = pa.array([datetime(2026, 1, 15)], type=pa.timestamp("ms"))
    _write(
        base / "ratings" / "dt=2026-01-15" / "part.parquet",
        pa.table(
            {
                "rating_id": pa.array([1], type=pa.int64()),
                "user_id": pa.array([1], type=pa.int64()),
                "movie_id": pa.array([1], type=pa.int64()),
                "rating_val": pa.array([4.0], type=pa.float64()),
                "rating_ts": stamp,
                "review_text": ["ok"],
                "event_date": event,
            }
        ),
    )
    _write(
        base / "watch_history" / "dt=2026-01-15" / "part.parquet",
        pa.table(
            {
                "session_id": pa.array([1], type=pa.int64()),
                "user_id": pa.array([1], type=pa.int64()),
                "movie_id": pa.array([1], type=pa.int64()),
                "start_ts": stamp,
                "end_ts": stamp,
                "watch_duration_seconds": pa.array([60], type=pa.int32()),
                "completion_rate": pa.array([0.5], type=pa.float64()),
                "device_type": ["tv"],
                "resolution": ["1080p"],
                "event_date": event,
            }
        ),
    )
    upload_tree(base, "it-airflow/bad-ts")


def _write(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def _field_type(spark, namespace: str, table: str, column: str) -> str:
    spark.sql(f"REFRESH TABLE cinestream.{namespace}.{table}")
    field = next(item for item in spark.table(f"cinestream.{namespace}.{table}").schema.fields if item.name == column)
    return field.dataType.simpleString()


def _delete_marker(namespace: str) -> None:
    client = s3()
    listed = client.list_objects_v2(Bucket="lakehouse", Prefix=f"_orchestration/{namespace}/")
    keys = [{"Key": item["Key"]} for item in listed.get("Contents") or []]
    if keys:
        client.delete_objects(Bucket="lakehouse", Delete={"Objects": keys})


def _marker(namespace: str) -> dict | None:
    listed = s3().list_objects_v2(Bucket="lakehouse", Prefix=f"_orchestration/{namespace}/")
    contents = [item for item in listed.get("Contents") or [] if item["Key"].endswith(".json")]
    if not contents:
        return None
    key = sorted(contents, key=lambda item: item["LastModified"])[-1]["Key"]
    body = s3().get_object(Bucket="lakehouse", Key=key)
    return json.loads(body["Body"].read().decode())


if __name__ == "__main__":
    main()
