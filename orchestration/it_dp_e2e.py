"""Chạy DP1, DP2, DP3 trên Airflow thật và đối chiếu Iceberg.

Cần ``make airflow-up`` trước. Namespace chỉ ``it_af_dp*``.
Không ghi ``bronze`` / ``silver`` / ``gold`` và không ghi ``cinestream_serving``.
"""

from __future__ import annotations

import ast
import json
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote

from orchestration.it_phase2 import (
    API,
    REPO,
    api,
    close_reader,
    ensure_buckets,
    local_batch_id,
    open_reader,
    read_marker,
    reset_namespaces,
    seed_happy,
    snap,
    wait_http,
    wait_worker,
)

REPORT = REPO / "docs" / "proof" / "logs" / "phase_a2_airflow_e2e.json"
DP1 = "cinestream_dp1_raw_to_bronze"
DP2 = "cinestream_dp2_bronze_to_gold"
DP3 = "cinestream_dp3_offline_features"
LEGACY = "cinestream_daily_batch_lakehouse"
OK = "it_af_dp"
FAIL_NS = "it_af_dp_fail"
RETRY_NS = "it_af_dp_retry"
CLICKHOUSE_DB = "it_af_dp_ch"
AS_OF = "2026-01-20T00:00:00+00:00"
CREATED = "2026-01-21T03:00:00+00:00"
LEAK_AS_OF = "2026-01-15T00:00:00+00:00"
LOGICAL_OK = "2026-05-01T00:00:00+00:00"
LOGICAL_FAIL = "2026-05-02T00:00:00+00:00"
LOGICAL_RETRY = "2026-05-03T00:00:00+00:00"
LOGICAL_GUARD = "2026-05-04T00:00:00+00:00"
PRODUCTION = (
    "namespaces/gold/tables/fact_ratings",
    "namespaces/gold/tables/fact_watch_sessions",
    "namespaces/gold/tables/feat_user_movie_stats_daily",
    "namespaces/gold/tables/feat_movie_engagement_stats_daily",
    "namespaces/silver/tables/stg_ratings",
    "namespaces/silver/tables/stg_watch_sessions",
    "namespaces/silver/tables/stg_playback_heartbeats",
    "namespaces/bronze/tables/raw_users",
)
SERVING_COUNTS = (
    "SELECT count() FROM cinestream_serving.movie_ratings",
    "SELECT count() FROM cinestream_serving.movie_engagement",
)


def main() -> None:
    wait_http("http://127.0.0.1:18082/health")
    wait_worker()
    ensure_buckets()
    before = production_fingerprint()
    serving_before = {sql: clickhouse(sql) for sql in SERVING_COUNTS}
    entry = scheduler_entry()
    legacy_before = api("GET", f"/dags/{LEGACY}").get("is_paused")
    if legacy_before is not True:
        api("PATCH", f"/dags/{LEGACY}", {"is_paused": True})
    wait_dags()
    cases = []
    try:
        for dag_id in (DP1, DP2, DP3):
            set_paused(dag_id, False)
        cases.append(case_guard(entry))
        root = Path(tempfile.mkdtemp(prefix="cinestream-dp-e2e-"))
        spark = open_reader()
        reset_namespaces(spark, [OK, FAIL_NS, RETRY_NS])
        seed_happy(spark, root, "dp")
        seed_happy(spark, root, "dp-retry")
        batch_id = local_batch_id("it-airflow/dp")
        retry_batch = local_batch_id("it-airflow/dp-retry")
        cases.append(case_success(batch_id))
        cases.append(case_upstream_block())
        cases.append(case_retry(retry_batch))
    except Exception as exc:
        cases.append({"name": "harness", "ok": False, "error": f"{exc.__class__.__name__}: {exc}"})
    finally:
        for dag_id in (DP1, DP2, DP3):
            try:
                set_paused(dag_id, True)
            except Exception as exc:
                print(f"không pause được {dag_id}: {exc}", flush=True)
        close_reader()
    after = production_fingerprint()
    serving_after = {sql: clickhouse(sql) for sql in SERVING_COUNTS}
    failed = [item["name"] for item in cases if not item.get("ok")]
    report = {
        "status": "FAIL" if failed or before != after or serving_before != serving_after else "PASS",
        "failed": failed,
        "batch_entry": entry,
        "legacy_paused_before": legacy_before,
        "legacy_paused_after": api("GET", f"/dags/{LEGACY}").get("is_paused"),
        "production_unchanged": before == after,
        "serving_unchanged": serving_before == serving_after,
        "production_before": before,
        "production_after": after,
        "serving_before": serving_before,
        "serving_after": serving_after,
        "cases": cases,
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps({"status": report["status"], "failed": failed, "report": str(REPORT)}, indent=2))
    if report["status"] != "PASS":
        raise SystemExit(report["status"])


def case_guard(entry: str) -> dict:
    """DP1 được phép ghi production nhưng entry mặc định từ chối trước spark-submit."""
    if entry not in {"", "legacy"}:
        return {"name": "guard", "ok": False, "reason": f"entry đang là {entry}, không kích hoạt ghi production"}
    conf = {
        "allow_production": True,
        "bronze_namespace": "bronze",
        "silver_namespace": "silver",
        "gold_namespace": "gold",
        "source_dir": "s3a://external-source-lake/it-airflow/dp-guard-absent",
        "source_timeout_seconds": 20,
        "source_poke_seconds": 5,
        "warehouse": "s3a://lakehouse",
    }
    run = trigger(DP1, conf, LOGICAL_GUARD, timeout=180)
    tasks = task_map(DP1, run, ("check_source_ready", "spark_raw_to_bronze", "validate_bronze"))
    log = task_log(DP1, run["dag_run_id"], "check_source_ready", 1)
    blocked = "không được ghi cùng bảng" in log
    no_ingest = tasks.get("spark_raw_to_bronze", {}).get("state") in {None, "upstream_failed", "skipped", "none"}
    ok = run.get("state") == "failed" and blocked and tasks.get("check_source_ready", {}).get("state") == "failed" and no_ingest
    return {
        "name": "guard",
        "ok": ok,
        "dag": run.get("state"),
        "tasks": {key: value.get("state") for key, value in tasks.items()},
        "blocked_before_ingest": blocked,
        "log_tail": log[-1500:],
    }


def case_success(batch_id: str) -> dict:
    conf = run_conf("it-airflow/dp", OK, batch_id=batch_id)
    dp1 = trigger(DP1, conf, LOGICAL_OK, timeout=900)
    dp1_tasks = task_map(DP1, dp1, ("check_source_ready", "spark_raw_to_bronze", "validate_bronze"))
    dp2 = trigger(DP2, conf, LOGICAL_OK, timeout=1200)
    dp2_tasks = task_map(
        DP2,
        dp2,
        ("wait_for_dp1", "spark_bronze_to_gold", "validate_gold", "publish_success", "sync_clickhouse"),
    )
    marker = read_marker(OK, batch_id)
    dp3 = trigger(DP3, {**conf, "as_of": AS_OF, "created_timestamp": CREATED}, LOGICAL_OK, timeout=900)
    dp3_tasks = task_map(DP3, dp3, ("wait_for_dp2", "spark_offline_features", "validate_features"))
    feature_xcom = xcom(DP3, dp3["dag_run_id"], "spark_offline_features")
    pinned = snapshots_of(OK)
    leaked = rerun_dp3(conf, batch_id, LEAK_AS_OF, CREATED, LOGICAL_OK)
    leaked_snapshots = snapshots_of(OK)
    again = rerun_dp3(conf, batch_id, AS_OF, CREATED, LOGICAL_OK)
    again_xcom = xcom(DP3, again["dag_run_id"], "spark_offline_features")
    again_snapshots = snapshots_of(OK)
    features = read_features()
    logs = {
        "dp1_validate": task_log(DP1, dp1["dag_run_id"], "validate_bronze", 1),
        "dp2_validate": task_log(DP2, dp2["dag_run_id"], "validate_gold", 1),
        "dp2_publish": task_log(DP2, dp2["dag_run_id"], "publish_success", 1),
        "dp3_features": task_log(DP3, dp3["dag_run_id"], "spark_offline_features", 1),
        "dp3_validate": task_log(DP3, dp3["dag_run_id"], "validate_features", 1),
        "dp3_leak": task_log(DP3, leaked["dag_run_id"], "spark_offline_features", 1),
        "dp3_rerun": task_log(DP3, again["dag_run_id"], "spark_offline_features", 1),
    }
    gold_ids = {name: str(value) for name, value in (marker or {}).get("gold_snapshots", {}).items()}
    silver_ids = {name: str(value) for name, value in (marker or {}).get("silver_snapshots", {}).items()}
    current_gold = {name: pinned.get(name) for name in ("dim_movies", "fact_ratings", "obt_movie_engagement")}
    current_silver = {name: pinned.get(name) for name in ("stg_ratings", "stg_watch_sessions")}
    ok = (
        dp1.get("state") == "success"
        and dp2.get("state") == "success"
        and dp3.get("state") == "success"
        and all(dp1_tasks[name]["state"] == "success" for name in dp1_tasks)
        and all(dp2_tasks[name]["state"] == "success" for name in dp2_tasks)
        and all(dp3_tasks[name]["state"] == "success" for name in dp3_tasks)
        and marker
        and marker.get("dag_id") == DP2
        and marker.get("batch_id") == batch_id
        and gold_ids == current_gold
        and silver_ids == current_silver
        and features["reconciled"]
        and leaked.get("state") == "failed"
        and leaked_snapshots == pinned
        and again.get("state") == "success"
        and again_xcom.get("user_write") == "unchanged"
        and again_xcom.get("movie_write") == "unchanged"
        and again_snapshots == pinned
        and _positive(clickhouse(f"SELECT count() FROM {CLICKHOUSE_DB}.movie_ratings"))
    )
    return {
        "name": "success",
        "ok": ok,
        "batch_id": batch_id,
        "dp1": dp1.get("state"),
        "dp2": dp2.get("state"),
        "dp3": dp3.get("state"),
        "dp1_tasks": states(dp1_tasks),
        "dp2_tasks": states(dp2_tasks),
        "dp3_tasks": states(dp3_tasks),
        "marker_dag": (marker or {}).get("dag_id"),
        "gold_snapshots": gold_ids,
        "silver_snapshots": silver_ids,
        "feature_xcom": feature_xcom,
        "features": features,
        "leakage_run": leaked.get("state"),
        "leakage_snapshots_unchanged": leaked_snapshots == pinned,
        "rerun": again.get("state"),
        "rerun_writes": {"user": again_xcom.get("user_write"), "movie": again_xcom.get("movie_write")},
        "rerun_snapshots_unchanged": again_snapshots == pinned,
        "clickhouse_database": CLICKHOUSE_DB,
        "clickhouse_ratings": clickhouse(f"SELECT count() FROM {CLICKHOUSE_DB}.movie_ratings"),
        "logs": {key: value[-2000:] for key, value in logs.items()},
    }


def case_upstream_block() -> dict:
    conf = run_conf("it-airflow/dp-absent", FAIL_NS, batch_id="absent-batch")
    conf["source_timeout_seconds"] = 25
    dp1 = trigger(DP1, conf, LOGICAL_FAIL, timeout=180)
    dp1_tasks = task_map(DP1, dp1, ("check_source_ready", "spark_raw_to_bronze", "validate_bronze"))
    dp2 = trigger(DP2, conf, LOGICAL_FAIL, timeout=180)
    dp2_tasks = task_map(DP2, dp2, ("wait_for_dp1", "spark_bronze_to_gold", "validate_gold", "publish_success", "sync_clickhouse"))
    dp3 = trigger(DP3, {**conf, "as_of": AS_OF, "created_timestamp": CREATED}, LOGICAL_FAIL, timeout=180)
    dp3_tasks = task_map(DP3, dp3, ("wait_for_dp2", "spark_offline_features", "validate_features"))
    feat = table_names(FAIL_NS)
    ok = (
        dp1.get("state") == "failed"
        and dp1_tasks.get("validate_bronze", {}).get("state") == "upstream_failed"
        and dp2.get("state") == "failed"
        and dp2_tasks.get("wait_for_dp1", {}).get("state") == "failed"
        and dp2_tasks.get("spark_bronze_to_gold", {}).get("state") == "upstream_failed"
        and dp3.get("state") == "failed"
        and dp3_tasks.get("wait_for_dp2", {}).get("state") == "failed"
        and dp3_tasks.get("spark_offline_features", {}).get("state") == "upstream_failed"
        and "feat_user_movie_stats_daily" not in feat
        and "feat_movie_engagement_stats_daily" not in feat
    )
    return {
        "name": "upstream_block",
        "ok": ok,
        "dp1": states(dp1_tasks),
        "dp2": states(dp2_tasks),
        "dp3": states(dp3_tasks),
        "fail_tables": feat,
        "dp2_log": task_log(DP2, dp2["dag_run_id"], "wait_for_dp1", 1)[-800:],
    }


def case_retry(batch_id: str) -> dict:
    """Xóa marker fail_once của logical date này. Lần chạy trước để lại file nên retry không xảy ra nữa."""
    marker = f"/opt/airflow/logs/fail-once/manual__{LOGICAL_RETRY}.txt"
    subprocess.run(
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
            "rm",
            "-f",
            marker,
        ],
        check=False,
    )
    conf = run_conf("it-airflow/dp-retry", RETRY_NS, batch_id=batch_id, fail_once=True)
    run = trigger(DP1, conf, LOGICAL_RETRY, timeout=1200)
    tasks = task_map(DP1, run, ("check_source_ready", "spark_raw_to_bronze", "validate_bronze"))
    tries = int(tasks.get("spark_raw_to_bronze", {}).get("try_number") or 0)
    first = task_log(DP1, run["dag_run_id"], "spark_raw_to_bronze", 1)
    second = task_log(DP1, run["dag_run_id"], "spark_raw_to_bronze", max(tries, 2))
    ok = (
        run.get("state") == "success"
        and tasks.get("spark_raw_to_bronze", {}).get("state") == "success"
        and tries >= 2
        and "thoát" in first
        and "Marking task as SUCCESS" in second
    )
    return {
        "name": "retry",
        "ok": ok,
        "dag": run.get("state"),
        "tasks": states(tasks),
        "try_number": tries,
        "first_log": first[-800:],
        "second_log": second[-800:],
    }


def rerun_dp3(conf: dict, batch_id: str, as_of: str, created: str, logical_date: str) -> dict:
    return trigger(DP3, {**conf, "batch_id": batch_id, "as_of": as_of, "created_timestamp": created}, logical_date, timeout=900)


def _delete(path: str) -> None:
    """DELETE không gửi Content-Type. Airflow trả 400 HTML nếu DELETE có header đó mà không có body."""
    request = urllib.request.Request(
        API + path,
        method="DELETE",
        headers={"Authorization": "Basic YWlyZmxvdzphaXJmbG93"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()
        raise RuntimeError(f"DELETE {path} -> {exc.code} {detail}") from exc


def _delete_logical(dag_id: str, logical_date: str) -> None:
    current = api("GET", f"/dags/{dag_id}/dagRuns?limit=50")
    for run in current.get("dag_runs") or []:
        if not str(run.get("logical_date", "")).startswith(logical_date[:19]):
            continue
        if run.get("state") in {"queued", "running"}:
            raise RuntimeError(f"{dag_id} vẫn đang chạy {run['dag_run_id']}")
        _delete(f"/dags/{dag_id}/dagRuns/{quote(run['dag_run_id'], safe='')}")
    deadline = time.time() + 30
    while time.time() < deadline:
        leftover = api("GET", f"/dags/{dag_id}/dagRuns?limit=50").get("dag_runs") or []
        if not any(str(item.get("logical_date", "")).startswith(logical_date[:19]) for item in leftover):
            return
        time.sleep(2)
    raise RuntimeError(f"chưa xóa được dag run {dag_id} {logical_date}")


def read_features() -> dict:
    spark = open_reader()
    for table in ("stg_ratings", "stg_watch_sessions", "feat_user_movie_stats_daily", "feat_movie_engagement_stats_daily"):
        spark.sql(f"REFRESH TABLE cinestream.{OK}.{table}")
    users = [row.asDict(recursive=True) for row in spark.sql(f"SELECT * FROM cinestream.{OK}.feat_user_movie_stats_daily").collect()]
    movies = [row.asDict(recursive=True) for row in spark.sql(f"SELECT * FROM cinestream.{OK}.feat_movie_engagement_stats_daily").collect()]
    schema = [row.asDict() for row in spark.sql(f"DESCRIBE cinestream.{OK}.feat_user_movie_stats_daily").collect()]
    expected_user = spark.sql(
        f"""
        SELECT COUNT(*) AS ratings, AVG(rating_val) AS avg_rating
        FROM cinestream.{OK}.stg_ratings
        WHERE rating_ts >= TIMESTAMP '2025-10-22 00:00:00'
          AND rating_ts < TIMESTAMP '2026-01-20 00:00:00'
          AND user_id = 7
        """
    ).collect()[0]
    expected_hours = spark.sql(
        f"""
        SELECT COALESCE(SUM(watch_duration_seconds), 0) / 3600.0 AS hours
        FROM cinestream.{OK}.stg_watch_sessions
        WHERE start_ts >= TIMESTAMP '2025-12-21 00:00:00'
          AND start_ts < TIMESTAMP '2026-01-20 00:00:00'
          AND user_id = 7
        """
    ).collect()[0]["hours"]
    future = spark.sql(
        f"""
        SELECT COUNT(*) AS c FROM cinestream.{OK}.stg_ratings
        WHERE rating_ts >= TIMESTAMP '2026-01-20 00:00:00'
        """
    ).collect()[0]["c"]
    boundary = spark.sql(
        f"""
        SELECT COUNT(*) AS c FROM cinestream.{OK}.stg_ratings
        WHERE rating_ts >= TIMESTAMP '2026-01-15 00:00:00'
          AND rating_ts < TIMESTAMP '2026-01-20 00:00:00'
        """
    ).collect()[0]["c"]
    columns = {row.get("col_name") for row in schema}
    user = users[0] if len(users) == 1 else {}
    movie = movies[0] if len(movies) == 1 else {}
    reconciled = (
        len(users) == 1
        and len(movies) == 1
        and int(user.get("user_id") or -1) == 7
        and int(movie.get("movie_id") or -1) == 11
        and int(user.get("user_total_ratings_90d") or -1) == int(expected_user["ratings"]) == 1
        and abs(float(user.get("user_avg_rating_90d")) - float(expected_user["avg_rating"])) < 1e-6
        and abs(float(user.get("user_total_watch_hours_30d")) - float(expected_hours)) < 1e-6
        and user.get("user_favorite_genre") is None
        and int(movie.get("movie_rating_count_30d") or -1) == 1
        and abs(float(movie.get("movie_avg_rating_30d")) - 4.0) < 1e-6
        and abs(float(movie.get("movie_completion_rate_30d")) - 0.5) < 1e-6
        and abs(float(movie.get("movie_trending_score_7d")) - float(expected_hours)) < 1e-6
        and str(user.get("event_timestamp")).startswith("2026-01-20")
        and str(user.get("created_timestamp")).startswith("2026-01-21 03:00:00")
        and str(movie.get("event_timestamp")).startswith("2026-01-20")
        and int(future) == 0
        and int(boundary) == 1
        and {"event_timestamp", "created_timestamp"} <= columns
    )
    return {
        "reconciled": reconciled,
        "user_rows": len(users),
        "movie_rows": len(movies),
        "ratings_at_or_after_as_of": int(future),
        "ratings_inside_window": int(boundary),
        "schema": schema,
        "user_sample": _plain(user),
        "movie_sample": _plain(movie),
        "expected_ratings": int(expected_user["ratings"] or 0),
        "expected_hours": float(expected_hours or 0),
    }


def snapshots_of(namespace: str) -> dict:
    spark = open_reader()
    found = snap(
        spark,
        namespace,
        "dim_movies",
        "fact_ratings",
        "obt_movie_engagement",
        "stg_ratings",
        "stg_watch_sessions",
        "feat_user_movie_stats_daily",
        "feat_movie_engagement_stats_daily",
    )
    current = {}
    for table, items in found.items():
        if not items:
            current[table] = None
            continue
        latest = max(items, key=lambda item: str(item.get("committed_at")))
        current[table] = str(latest.get("snapshot_id"))
    return current


def run_conf(prefix: str, namespace: str, **extra) -> dict:
    if not namespace.startswith("it_af_dp"):
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
        "clickhouse_http": "http://clickhouse:8123",
        "clickhouse_database": CLICKHOUSE_DB,
    }
    body.update(extra)
    return body


def trigger(dag_id: str, conf: dict, logical_date: str, timeout: int) -> dict:
    print(f"trigger {dag_id} logical={logical_date}", flush=True)
    _delete_logical(dag_id, logical_date)
    created = api("POST", f"/dags/{dag_id}/dagRuns", {"conf": conf, "logical_date": logical_date})
    run_id = created["dag_run_id"]
    deadline = time.time() + timeout
    current = created
    while current.get("state") in {None, "queued", "running"}:
        if time.time() > deadline:
            raise TimeoutError(f"{dag_id} {run_id}")
        time.sleep(5)
        current = api("GET", f"/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}")
    print(f"{dag_id} {run_id} -> {current.get('state')}", flush=True)
    return current


def task_map(dag_id: str, run: dict, order: tuple[str, ...]) -> dict:
    tasks = {}
    for _ in range(10):
        payload = api("GET", f"/dags/{dag_id}/dagRuns/{quote(run['dag_run_id'], safe='')}/taskInstances")
        tasks = {item["task_id"]: item for item in payload.get("task_instances") or []}
        states_now = [tasks.get(name, {}).get("state") for name in order]
        if all(state in {"success", "failed", "upstream_failed", "skipped"} for state in states_now):
            return tasks
        time.sleep(1)
    return tasks


def states(tasks: dict) -> dict:
    return {name: {"state": item.get("state"), "try_number": item.get("try_number")} for name, item in tasks.items()}


def set_paused(dag_id: str, paused: bool) -> None:
    api("PATCH", f"/dags/{dag_id}", {"is_paused": paused})


def wait_dags() -> None:
    subprocess.run(
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
            "airflow",
            "dags",
            "reserialize",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    deadline = time.time() + 90
    while time.time() < deadline:
        missing = []
        for dag_id in (DP1, DP2, DP3):
            try:
                payload = api("GET", f"/dags/{dag_id}")
            except RuntimeError:
                missing.append(dag_id)
                continue
            if payload.get("has_import_errors"):
                missing.append(dag_id)
        errors = api("GET", "/importErrors")
        if not missing and not (errors.get("import_errors") or []):
            return
        time.sleep(5)
    raise RuntimeError(f"DAG chưa sẵn sàng: {missing}")


def xcom(dag_id: str, run_id: str, task_id: str) -> dict:
    try:
        payload = api(
            "GET",
            f"/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances/{task_id}/xcomEntries/return_value",
        )
    except RuntimeError:
        return {}
    value = payload.get("value")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return ast.literal_eval(value)
    return value or {}


def task_log(dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    path = f"/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances/{task_id}/logs/{try_number}"
    request = urllib.request.Request(
        API + path,
        headers={"Accept": "application/json", "Authorization": "Basic YWlyZmxvdzphaXJmbG93"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.read().decode()[-1500:]
    try:
        body = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    content = body.get("content") if isinstance(body, dict) else raw
    if isinstance(content, list):
        content = "\n".join(str(item) for item in content)
    return str(content or raw)


def table_names(namespace: str) -> list[str]:
    request = urllib.request.Request(f"http://127.0.0.1:8181/v1/namespaces/{namespace}/tables")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return []
        raise
    return [item["name"] for item in body.get("identifiers") or []]


def production_fingerprint() -> dict:
    found = {}
    for path in PRODUCTION:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/{path}", timeout=5) as response:
                found[path] = response.read().decode()
        except Exception as exc:
            found[path] = f"unavailable:{exc.__class__.__name__}"
    return found


def clickhouse(sql: str) -> str:
    request = urllib.request.Request(
        "http://127.0.0.1:18123/?user=default&password=cinestream",
        data=sql.encode(),
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.read().decode().strip()
    except Exception as exc:
        return f"unavailable:{exc.__class__.__name__}"


def scheduler_entry() -> str:
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
            "printenv",
            "CINESTREAM_BATCH_ENTRY",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    return (completed.stdout or "").strip()


def _plain(row: dict) -> dict:
    return {key: _jsonable(value) for key, value in row.items()}


def _positive(text: str) -> bool:
    try:
        return int(text) > 0
    except ValueError:
        return False


def _jsonable(value):
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.decode()
    try:
        json.dumps(value)
    except TypeError:
        return str(value)
    return value


if __name__ == "__main__":
    main()
