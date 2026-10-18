"""Sẵn sàng vận hành trending. Không bật streaming production.

Topic ``it.p2.ops.*``, bảng ``it_p2_ops_*``, database ``it_ops``.
"""

from __future__ import annotations

import json
import os
import resource
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from serving.clickhouse.sync_gold import clickhouse
from serving.clickhouse.sync_windows import poll_windows, sync_windows
from streaming.flink.it_phase2 import (
    checkpoint_view,
    compose,
    end_offsets,
    event,
    fail_taskmanager,
    recreate_topics,
    running_jobs,
    send_records,
    start_spark,
    submit_pipeline,
    wait_new_checkpoint,
    wait_restored,
)
from streaming.flink.runtime_guard import (
    PRODUCTION_DATABASE,
    PRODUCTION_WINDOWS,
    require_streaming_approval,
)

HTTP = "http://127.0.0.1:18123"
API = "http://127.0.0.1:18087"
STALE_API = "http://127.0.0.1:18088"
DOWN_API = "http://127.0.0.1:18089"
DATABASE = "it_ops"
NAMESPACE_TABLE = "cinestream.silver.it_p2_ops_windows"
HEARTBEATS = "cinestream.silver.it_p2_ops_heartbeats"
TOPIC = "it.p2.ops.heartbeat"
DLQ = "it.p2.ops.deadletter"
REPO = Path(__file__).resolve().parents[2]
REPORT = REPO / "docs" / "proof" / "logs" / "phase3d_ops_it.json"
PRODUCTION = (
    "namespaces/silver/tables/stg_watch_sessions",
    "namespaces/silver/tables/stg_playback_heartbeats",
    "namespaces/silver/tables/stg_playback_windows",
    "namespaces/bronze/tables/raw_users",
)
EXPECTED = {(42, 0, 4, 40, 4)}
SECOND = {(42, 0, 4, 40, 4), (42, 60_000, 1, 10, 1)}


def main() -> None:
    _wait_clickhouse()
    before = _fingerprint()
    offsets_before = _offsets()
    cases = [_case_guard()]
    spark = start_spark("trending-ops")
    servers = []
    job_id = None
    try:
        from streaming.flink.it_phase2 import drop_tables

        _release_test_jobs()
        clickhouse(HTTP, f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
        recreate_topics([(TOPIC, 1), (DLQ, 1)])
        drop_tables(spark, [HEARTBEATS, NAMESPACE_TABLE])
        servers.append(_start_api(API, 18087, HTTP, "180"))
        servers.append(_start_api(STALE_API, 18088, HTTP, "0"))
        servers.append(_start_api(DOWN_API, 18089, "http://127.0.0.1:9", "180"))
        job_id = submit_pipeline(
            TOPIC,
            DLQ,
            "it-ops",
            HEARTBEATS,
            NAMESPACE_TABLE,
            "it-ops-dlq",
            ["--checkpoint-interval-ms", "10000", "--parallelism", "1", "--idle-timeout-s", "30"],
        )
        absent = poll_windows(
            spark,
            interval_seconds=0,
            max_polls=1,
            source_table=NAMESPACE_TABLE,
            http=HTTP,
            database=DATABASE,
        )
        empty_api = _get(API, "/movies/trending?window_minutes=15&limit=10")
        cases.append(_case_empty(absent, empty_api))
        send_records(
            TOPIC,
            [
                (_beat(1, 10, 0), 0),
                (_beat(2, 11, 20_000), 0),
                (_beat(3, 12, 50_000), 0),
                (_beat(4, 13, 10_000), 0),
                (_beat(5, 14, 80_000), 0),
            ],
        )
        logical = _wait_rows(spark, EXPECTED)
        checkpoint = wait_new_checkpoint(job_id, 0, timeout=90)
        restored_before = int((checkpoint_view(job_id).get("counts") or {}).get("restored") or 0)
        fail_taskmanager()
        restored = wait_restored(job_id, restored_before)
        after_restart = _logical(spark)
        cases.append(_case_restart(logical, after_restart, checkpoint, restored, spark))
        usage_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        synced = sync_windows(spark, source_table=NAMESPACE_TABLE, http=HTTP, database=DATABASE)
        usage_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        first_poll = poll_windows(
            spark,
            interval_seconds=0,
            max_polls=1,
            source_table=NAMESPACE_TABLE,
            http=HTTP,
            database=DATABASE,
        )
        idle = _get(API, "/movies/trending?window_minutes=15&limit=10")
        status = _get(API, "/serving/status")
        stale = _get(STALE_API, "/movies/trending?window_minutes=15&limit=10")
        sent = time.time()
        send_records(TOPIC, [(_beat(6, 15, 140_000), 0)])
        polls = []
        found = None
        deadline = time.time() + 90
        while time.time() < deadline:
            time.sleep(5)
            polled = sync_windows(spark, source_table=NAMESPACE_TABLE, http=HTTP, database=DATABASE)
            _record = {
                "status": polled["status"],
                "snapshot_id": polled.get("snapshot_id"),
                "elapsed_ms": polled.get("elapsed_ms"),
                "at_s": round(time.time() - sent, 3),
            }
            polls.append(_record)
            print("poll", _record, flush=True)
            if polled["status"] == "published" and polled.get("snapshot_id") != synced["snapshot_id"]:
                found = polled
                break
        api_at = time.time()
        visible = _get(API, "/movies/trending?window_minutes=15&limit=10") if found else None
        second_logical = _logical(spark)
        keys = int(clickhouse(HTTP, f"SELECT uniqExact(movie_id, window_start) FROM {DATABASE}.movie_windows").strip())
        row_count = int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.movie_windows").strip())
        log_rows = int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.trending_sync_log").strip())
        cases.append(
            _case_poll(
                synced,
                first_poll,
                idle,
                status,
                stale,
                visible,
                sent,
                api_at,
                usage_before,
                usage_after,
                second_logical,
                keys,
                row_count,
                log_rows,
                polls,
                found,
            )
        )
        restarted = _worker_process()
        refused = poll_windows(
            spark,
            interval_seconds=30,
            max_polls=1,
            source_table=NAMESPACE_TABLE,
            http="http://127.0.0.1:9",
            database=DATABASE,
        )
        down = _get(DOWN_API, "/movies/trending?window_minutes=15&limit=10")
        current = _get(API, "/movies/trending?window_minutes=15&limit=10")
        clickhouse(
            HTTP,
            f"""
            INSERT INTO {DATABASE}.trending_worker_status
                (source_table, status, iceberg_snapshot_id, window_rows, latest_window_end, polled_at, detail)
            VALUES ('{NAMESPACE_TABLE}', 'retry', NULL, {row_count}, NULL, now64(3, 'UTC'), 'mo phong iceberg ngat')
            """,
        )
        failed = _get(API, "/movies/trending?window_minutes=15&limit=10")
        cases.append(_case_failure(restarted, refused, down, failed, current))
    except (SystemExit, Exception) as exc:
        detail = exc.code if isinstance(exc, SystemExit) and isinstance(exc.code, str) else str(exc)
        cases.append(_case("suite dừng", "các case phía trên chạy hết", {"error": detail[-1500:]}, False))
    finally:
        if job_id is not None:
            compose("exec", "-T", "jobmanager", "flink", "cancel", job_id, check=False)
        for process in servers:
            process.terminate()
            process.wait(timeout=10)
        after = _fingerprint()
        offsets_after = _offsets()
        spark.stop()
    unchanged = before == after and offsets_before == offsets_after
    cases.append(
        _case(
            "production không đổi",
            "REST và offset topic production giữ nguyên",
            {"before": before, "after": after, "offsets_before": offsets_before, "offsets_after": offsets_after},
            unchanged,
        )
    )
    failed_names = [item["name"] for item in cases if item["status"] != "PASS"]
    report = {"status": "FAIL" if failed_names else "PASS", "failed": failed_names, "cases": cases}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    if failed_names:
        raise SystemExit(f"FAIL {failed_names}")


def _case_guard() -> dict:
    env = os.environ.copy()
    env["CINESTREAM_ALLOW_STREAMING"] = ""
    approve = {}
    for target in ("stream-job-approve", "serving-trending-approve"):
        completed = subprocess.run(["make", target], cwd=REPO, env=env, text=True, capture_output=True, check=False)
        approve[target] = {"code": completed.returncode, "out": (completed.stdout + completed.stderr)[-300:]}
    blocked = False
    try:
        require_streaming_approval(windows_table=PRODUCTION_WINDOWS, database=PRODUCTION_DATABASE)
    except RuntimeError as exc:
        blocked = "CINESTREAM_ALLOW_STREAMING" in str(exc)
    require_streaming_approval(windows_table=NAMESPACE_TABLE, database=DATABASE)
    ok = all(item["code"] != 0 for item in approve.values()) and blocked
    return _case(
        "chặn streaming production",
        "make approve và runtime guard từ chối mục tiêu production khi chưa có cờ",
        {"approve": approve, "guard_blocked": blocked},
        ok,
    )


def _case_empty(polled: dict, api: dict) -> dict:
    body = api["body"]
    consistent = (
        (polled["status"] == "source_empty" and body.get("freshness") == "empty" and body.get("items") == [])
        or (polled["status"] == "source_absent" and body.get("freshness") == "sync_failed" and body.get("items") == [])
    )
    ok = api["status"] == 200 and body.get("live") is False and consistent
    return _case(
        "bảng chưa có snapshot",
        "source_empty là empty, source_absent là sync_failed, không gọi là live",
        {"poll": polled, "api": body},
        ok,
    )


def _case_restart(before: list, after: list, checkpoint: dict, restored: dict, spark) -> dict:
    path = ((checkpoint.get("completed") or {}).get("external_path")) or ""
    ok = (
        before == after == sorted(EXPECTED)
        and _copies_ok(spark)
        and checkpoint.get("completed", {}).get("id") not in (None, 0)
        and "/opt/flink/checkpoints/" in path
        and int((restored.get("counts") or {}).get("restored") or 0) >= 1
    )
    return _case(
        "Flink restart sau checkpoint",
        "cửa sổ logic không đổi và không trùng khóa",
        {"before": before, "after": after, "checkpoint": checkpoint, "restored": restored},
        ok,
    )


def _case_poll(synced, first_poll, idle, status, stale, visible, sent, api_at, rss_before, rss_after, logical, keys, row_count, log_rows, polls, found) -> dict:
    body = idle["body"]
    late_body = (visible or {}).get("body") or {}
    ok = (
        synced["status"] == "published"
        and first_poll["status"] == "unchanged"
        and idle["status"] == 200
        and body.get("freshness") == "idle"
        and body.get("live") is False
        and body.get("iceberg_snapshot_id") == synced["snapshot_id"]
        and body.get("items")
        and status["status"] == 200
        and status["body"].get("trending_freshness") == "idle"
        and status["body"].get("trending_live") is False
        and stale["body"].get("freshness") == "stale"
        and stale["body"].get("live") is False
        and stale["body"].get("items")
        and found is not None
        and late_body.get("iceberg_snapshot_id") == found.get("snapshot_id")
        and keys == row_count == 2
        and logical == sorted(SECOND)
        and log_rows == 2
        and sum(1 for item in polls if item["status"] == "published") == 1
    )
    lags = {
        "event_to_api_s": round(api_at - sent, 3),
        "poll_interval_s": 5,
        "polls": polls,
        "sync_elapsed_ms": synced.get("elapsed_ms"),
        "publish_elapsed_ms": None if found is None else found.get("elapsed_ms"),
        "ru_maxrss_before": rss_before,
        "ru_maxrss_after": rss_after,
    }
    return _case(
        "poll lặp, freshness và tài nguyên Spark",
        "idle khi event cũ mà poll mới, stale khi quá ngưỡng, hai khóa, log không nhân theo số poll",
        {"synced": synced, "replay": first_poll["status"], "idle": body, "status": status["body"], "stale": stale["body"], "late_api": late_body, "lags": lags, "keys": keys, "log_rows": log_rows},
        ok,
    )


def _case_failure(restarted, refused, down, failed, current) -> dict:
    second = _json_line(restarted[1].stdout)
    ok = (
        restarted[0].returncode == 0
        and restarted[1].returncode == 0
        and _json_line(restarted[0].stdout).get("status") in {"unchanged", "published"}
        and second.get("status") == "unchanged"
        and refused["status"] == "retry"
        and "không tới được" in refused.get("error", "")
        and down["status"] == 503
        and failed["body"].get("freshness") == "sync_failed"
        and failed["body"].get("live") is False
        and failed["body"].get("items") == current["body"].get("items")
    )
    return _case(
        "worker restart và lỗi kết nối",
        "process mới cùng snapshot là unchanged, ClickHouse tắt thì API 503, poll retry thì sync_failed nhưng vẫn trả dòng cũ",
        {"first": _json_line(restarted[0].stdout), "second": second, "refused": refused, "down": down["status"], "failed": failed["body"]},
        ok,
    )


def _worker_process():
    runs = []
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["SPARK_LOCAL_IP"] = "127.0.0.1"
    env.pop("CINESTREAM_ALLOW_STREAMING", None)
    for _ in range(2):
        runs.append(
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "serving.clickhouse.sync_windows",
                    "--once",
                    "--master",
                    "local[1]",
                    "--warehouse",
                    "s3a://lakehouse",
                    "--iceberg-rest",
                    "http://127.0.0.1:8181",
                    "--s3-endpoint",
                    "http://127.0.0.1:9000",
                    "--source-table",
                    NAMESPACE_TABLE,
                    "--database",
                    DATABASE,
                    "--clickhouse-http",
                    HTTP,
                ],
                cwd=REPO,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
        )
    return runs


def _json_line(text: str) -> dict:
    for line in reversed(text.splitlines()):
        if line.startswith("TRENDING_JSON="):
            return json.loads(line.split("=", 1)[1])
    return {"stdout": text[-400:]}


def _wait_rows(spark, expected: set[tuple], timeout: int = 180) -> list[tuple]:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            found = _logical(spark)
            last = found
            print("windows", found, flush=True)
            if set(found) == expected and _copies_ok(spark):
                return found
        except Exception as exc:
            print("wait", str(exc).splitlines()[-1][:180], flush=True)
        time.sleep(3)
    raise SystemExit(f"cửa sổ chưa khớp {expected}: {last}")


def _logical(spark) -> list[tuple]:
    spark.sql(f"REFRESH TABLE {NAMESPACE_TABLE}")
    rows = spark.sql(
        f"""
        SELECT movie_id,
               CAST(unix_millis(CAST(window_start AS TIMESTAMP)) AS BIGINT) AS start_ms,
               heartbeat_count, total_watch_seconds, active_users
        FROM {NAMESPACE_TABLE}
        ORDER BY movie_id, start_ms
        """
    ).collect()
    return [tuple(int(value) for value in row) for row in rows]


def _copies_ok(spark) -> bool:
    rows = spark.sql(
        f"""
        SELECT movie_id, window_start
        FROM {NAMESPACE_TABLE}
        GROUP BY movie_id, window_start
        HAVING COUNT(*) > 1
        """
    ).collect()
    return not rows


def _wait_api_snapshot(base: str, previous: int, timeout: int = 45) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = _get(base, "/movies/trending?window_minutes=15&limit=10")
        body = last.get("body") or {}
        if last["status"] == 200 and body.get("iceberg_snapshot_id") not in (None, previous):
            return last
        time.sleep(0.5)
    raise SystemExit(f"API chưa thấy snapshot mới sau {previous}: {last}")


def _beat(heartbeat_id: int, user_id: int, event_ms: int) -> dict:
    payload = event(heartbeat_id, user_id, event_ms)
    payload["movie_id"] = 42
    return payload


def _release_test_jobs() -> None:
    blockers = []
    for job in running_jobs():
        name = job.get("name") or ""
        if "it_p1" in name or "it_p2" in name or "it-ops" in name or "it-trending" in name:
            compose("exec", "-T", "jobmanager", "flink", "cancel", job["jid"], check=False)
            continue
        blockers.append({"jid": job.get("jid"), "name": name})
    if blockers:
        raise SystemExit(f"không hủy job đang chạy: {blockers}")
    deadline = time.time() + 40
    while time.time() < deadline:
        if not running_jobs():
            return
        time.sleep(1)
    raise SystemExit(f"job chưa tắt: {running_jobs()}")


def _start_api(base: str, port: int, clickhouse_http: str, stale_seconds: str):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["CINESTREAM_CLICKHOUSE_DATABASE"] = DATABASE
    env["CINESTREAM_CLICKHOUSE_HTTP"] = clickhouse_http
    env["CINESTREAM_TRENDING_STALE_SECONDS"] = stale_seconds
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
                raise SystemExit(f"API không lên: {output[-800:]}")
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


def _fingerprint() -> dict:
    found = {}
    for path in PRODUCTION:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/{path}", timeout=5) as response:
                meta = json.loads(response.read().decode())["metadata"]
                found[path] = meta.get("current-snapshot-id")
        except urllib.error.HTTPError as exc:
            found[path] = f"http:{exc.code}"
        except Exception as exc:
            found[path] = f"unavailable:{exc.__class__.__name__}"
    return found


def _offsets() -> dict:
    try:
        return {name: end_offsets(name) for name in ("movie.playback.heartbeat", "movie.playback.deadletter")}
    except Exception as exc:
        return {"error": str(exc)[:200]}


def _wait_clickhouse() -> None:
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            clickhouse(HTTP, "SELECT 1")
            return
        except Exception:
            time.sleep(2)
    raise SystemExit("ClickHouse chưa sẵn sàng")


def _case(name: str, expected: str, actual: dict, ok: bool) -> dict:
    status = "PASS" if ok else "FAIL"
    print(f"CASE {status} {name}", flush=True)
    return {"name": name, "status": status, "expected": expected, "actual": actual}


if __name__ == "__main__":
    main()
