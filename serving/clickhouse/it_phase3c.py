"""Phase 3C: cửa sổ Flink thật sang ClickHouse và API trending.

Topic ``it.p2.trending.*``, bảng ``cinestream.silver.it_p2_trending_*``,
database ``it_trend``. Không ghi ``stg_playback_windows`` và không hủy job khác.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from serving.clickhouse.sync_gold import ServingSyncError, clickhouse
from serving.clickhouse.sync_windows import poll_windows, sync_windows
from streaming.flink.it_phase2 import (
    compose,
    end_offsets,
    event,
    running_jobs,
    send_records,
    start_spark,
    submit_pipeline,
)

HTTP = "http://127.0.0.1:18123"
API = "http://127.0.0.1:18086"
DATABASE = "it_trend"
EMPTY_DATABASE = "it_trend_empty"
HEARTBEATS = "cinestream.silver.it_p2_trending_heartbeats"
WINDOWS = "cinestream.silver.it_p2_trending_windows"
EMPTY = "cinestream.silver.it_p2_trending_empty"
TOPIC = "it.p2.trending.heartbeat"
DLQ = "it.p2.trending.deadletter"
REPO = Path(__file__).resolve().parents[2]
REPORT = REPO / "docs" / "proof" / "logs" / "phase3c_trending_it.json"
PRODUCTION = (
    "namespaces/silver/tables/stg_watch_sessions",
    "namespaces/silver/tables/stg_playback_heartbeats",
    "namespaces/silver/tables/stg_playback_windows",
    "namespaces/bronze/tables/raw_users",
)


def main() -> None:
    _wait_clickhouse()
    before = _fingerprint()
    offsets_before = _offsets()
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True, check=False)
    cases = []
    spark = start_spark("trending-it")
    server = None
    job_id = None
    try:
        from streaming.flink.it_phase2 import drop_tables, recreate_topics

        _release_test_jobs()
        clickhouse(HTTP, f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
        clickhouse(HTTP, f"DROP DATABASE IF EXISTS {EMPTY_DATABASE} SYNC")
        recreate_topics([(TOPIC, 1), (DLQ, 1)])
        drop_tables(spark, [HEARTBEATS, WINDOWS, EMPTY])
        server = _start_api()
        job_id = submit_pipeline(
            TOPIC,
            DLQ,
            "it-trending",
            HEARTBEATS,
            WINDOWS,
            "it-trending-dlq",
            ["--checkpoint-interval-ms", "10000", "--parallelism", "1", "--idle-timeout-s", "30"],
        )
        vertices = _vertices(job_id)
        absent = sync_windows(
            spark,
            source_table="cinestream.silver.it_p2_trending_absent",
            http=HTTP,
            database=DATABASE,
        )
        before_data = sync_windows(spark, source_table=WINDOWS, http=HTTP, database=DATABASE)
        cases.append(_case_source(vertices, absent, before_data, head.stdout.strip()))
        first_sent = time.time()
        send_records(
            TOPIC,
            [
                (_beat(1, 10, 42, 0), 0),
                (_beat(2, 11, 42, 20_000), 0),
                (_beat(3, 12, 42, 50_000), 0),
                (_beat(4, 13, 42, 10_000), 0),
                (_beat(6, 8, 7, 30_000), 0),
                (_beat(5, 14, 42, 80_000), 0),
            ],
        )
        first_rows = _wait_rows(spark, {(7, 0, 1, 10, 1), (42, 0, 4, 40, 4)})
        first_visible = time.time()
        first_snap = _snapshot(spark)
        first = sync_windows(spark, source_table=WINDOWS, http=HTTP, database=DATABASE)
        first_synced = time.time()
        first_api = _wait_api(first_snap["snapshot_id"])
        first_api_at = time.time()
        cases.append(
            _case_sync(
                "sync cửa sổ đầu",
                "API khớp đọc logic của snapshot Flink, không lấy current nếu khác",
                spark,
                first,
                first_rows,
                first_api,
                {
                    "sent_at": first_sent,
                    "iceberg_visible_at": first_visible,
                    "synced_at": first_synced,
                    "api_visible_at": first_api_at,
                    "snapshot": first_snap,
                },
            )
        )
        late_sent = time.time()
        send_records(TOPIC, [(_beat(7, 15, 42, 5_000), 0)])
        late_rows = _wait_rows(spark, {(7, 0, 1, 10, 1), (42, 0, 5, 50, 5)})
        late_visible = time.time()
        late_snap = _snapshot(spark)
        deletes = _delete_evidence(spark)
        late = sync_windows(spark, source_table=WINDOWS, http=HTTP, database=DATABASE)
        late_synced = time.time()
        late_api = _wait_api(late_snap["snapshot_id"])
        late_api_at = time.time()
        again = sync_windows(spark, source_table=WINDOWS, http=HTTP, database=DATABASE)
        cases.append(
            _case_late(
                spark,
                late,
                again,
                late_rows,
                late_api,
                deletes,
                first_snap,
                late_snap,
                {
                    "sent_at": late_sent,
                    "iceberg_visible_at": late_visible,
                    "synced_at": late_synced,
                    "api_visible_at": late_api_at,
                },
            )
        )
        send_records(
            TOPIC,
            [
                (_beat(8, 19, 42, 70_000), 0),
                (_beat(9, 20, 42, 90_000), 0),
                (_beat(10, 21, 7, 140_000), 0),
            ],
        )
        second_rows = _wait_rows(spark, {(7, 0, 1, 10, 1), (42, 0, 5, 50, 5), (42, 60_000, 3, 30, 3)})
        held = _get(API, "/movies/trending?window_minutes=15&limit=10")
        try:
            failed = sync_windows(
                spark,
                source_table=WINDOWS,
                http=HTTP,
                database=DATABASE,
                fail_after_rows=1,
            )
            error = ""
        except ServingSyncError as exc:
            failed = None
            error = str(exc)
        during = _get(API, "/movies/trending?window_minutes=15&limit=10")
        stage_rows = int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.movie_windows_stage").strip())
        retried = sync_windows(spark, source_table=WINDOWS, http=HTTP, database=DATABASE)
        after = _get(API, "/movies/trending?window_minutes=15&limit=10")
        minute = _get(API, "/movies/trending?window_minutes=1&limit=10")
        limited = _get(API, "/movies/trending?window_minutes=15&limit=1")
        bad = _get(API, "/movies/trending?window_minutes=0")
        cases.append(_case_retry(error, failed, held, during, stage_rows, retried, after, minute, limited, bad, second_rows, spark))
        missing = sync_windows(spark, source_table="cinestream.silver.it_p2_trending_absent", http=HTTP, database=DATABASE)
        stayed = _get(API, "/movies/trending?window_minutes=15&limit=10")
        retry_poll = poll_windows(
            spark,
            interval_seconds=30,
            max_polls=1,
            source_table=WINDOWS,
            http="http://127.0.0.1:9",
            database=DATABASE,
        )
        recovered = sync_windows(spark, source_table=WINDOWS, http=HTTP, database=DATABASE)
        empty = _empty_table(spark)
        cases.append(_case_edges(missing, stayed, after, retry_poll, recovered, empty))
    except (SystemExit, Exception) as exc:
        detail = exc.code if isinstance(exc, SystemExit) and isinstance(exc.code, str) else str(exc)
        cases.append(_case("suite dừng", "các case phía trên chạy hết", {"error": detail[-1500:]}, False))
    finally:
        if job_id is not None:
            compose("exec", "-T", "jobmanager", "flink", "cancel", job_id, check=False)
        if server is not None:
            server.terminate()
            server.wait(timeout=10)
        after = _fingerprint()
        offsets_after = _offsets()
        spark.stop()
    unchanged = before == after and offsets_before == offsets_after
    cases.append(
        _case(
            "production không đổi",
            "REST streaming production và offset topic production giữ nguyên",
            {"before": before, "after": after, "offsets_before": offsets_before, "offsets_after": offsets_after},
            unchanged,
        )
    )
    failed = [item["name"] for item in cases if item["status"] != "PASS"]
    report = {"status": "FAIL" if failed else "PASS", "failed": failed, "head": head.stdout.strip(), "cases": cases}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    if failed:
        raise SystemExit(f"FAIL {failed}")


def _case_source(vertices: list[str], absent: dict, before_data: dict, head: str) -> dict:
    props = before_data.get("properties")
    ok = (
        any("tumbling-1m" in name for name in vertices)
        and not any("sliding" in name.lower() for name in vertices)
        and absent["status"] == "source_absent"
        and before_data["status"] in {"source_absent", "source_empty"}
    )
    return _case(
        "nguồn Flink và bảng chưa có dữ liệu",
        "chỉ tumbling-1m, bảng production không được tạo, nguồn thiếu trả source_absent",
        {"head": head, "vertices": vertices, "absent": absent, "before_data": before_data, "properties": props},
        ok,
    )


def _case_sync(name, expected, spark, result, logical, api, freshness) -> dict:
    compared = _compare(spark, result["snapshot_id"], logical)
    body = api["body"]
    ok = (
        result["status"] == "published"
        and compared["match"]
        and api["status"] == 200
        and body["iceberg_snapshot_id"] == result["snapshot_id"]
        and _items(body) == _expect_api(logical, 15)
        and body["synced_at"]
    )
    return _case(name, expected, {"sync": result, "compare": compared, "api": body, "freshness_s": _lags(freshness)}, ok)


def _case_late(spark, late, again, logical, api, deletes, first_snap, late_snap, freshness) -> dict:
    compared = _compare(spark, late["snapshot_id"], logical)
    equality = deletes["equality_records"] > 0 or deletes["data_records"] > deletes["logical_rows"]
    body = api["body"]
    ok = (
        late["status"] == "published"
        and late_snap["snapshot_id"] != first_snap["snapshot_id"]
        and compared["match"]
        and equality
        and body["iceberg_snapshot_id"] == late["snapshot_id"]
        and _items(body) == _expect_api(logical, 15)
        and again["status"] == "unchanged"
        and again["serving"]["log_rows"] == late["serving"]["log_rows"]
    )
    return _case(
        "late firing sửa cửa sổ cũ rồi sync lại cùng snapshot",
        "một khóa logic, ClickHouse bằng Spark VERSION AS OF, lần sau unchanged",
        {"late": late, "again": again["status"], "compare": compared, "deletes": deletes, "api": body, "freshness_s": _lags(freshness), "snapshots": {"first": first_snap, "late": late_snap}},
        ok,
    )


def _case_retry(error, failed, held, during, stage_rows, retried, after, minute, limited, bad, logical, spark) -> dict:
    compared = _compare(spark, retried.get("snapshot_id"), logical) if retried.get("snapshot_id") else {"match": False}
    ok = (
        "dừng giữa bulk load" in error
        and failed is None
        and during["body"]["iceberg_snapshot_id"] == held["body"]["iceberg_snapshot_id"]
        and stage_rows == 1
        and retried["status"] == "published"
        and compared["match"]
        and _items(after["body"]) == _expect_api(logical, 15) == [{"movie_id": 42, "recent_heartbeats": 8, "recent_watch_seconds": 80, "peak_minute_unique_viewers": 5}, {"movie_id": 7, "recent_heartbeats": 1, "recent_watch_seconds": 10, "peak_minute_unique_viewers": 1}]
        and _items(minute["body"]) == [{"movie_id": 42, "recent_heartbeats": 3, "recent_watch_seconds": 30, "peak_minute_unique_viewers": 3}]
        and _items(limited["body"]) == [_items(after["body"])[0]]
        and bad["status"] == 422
        and len({(row[0], row[1]) for row in logical}) == len(logical)
    )
    return _case(
        "hai cửa sổ cùng phim, sync lỗi rồi retry",
        "reader giữ snapshot cũ khi stage dở, retry khớp Iceberg, window_minutes=1 chỉ lấy phút mới",
        {"error": error, "stage_rows": stage_rows, "during": during["body"], "retry": retried, "compare": compared, "fifteen": after["body"], "one": minute["body"], "bad": bad["status"]},
        ok,
    )


def _case_edges(missing, stayed, after, retry_poll, recovered, empty) -> dict:
    ok = (
        missing["status"] == "source_absent"
        and stayed["body"]["iceberg_snapshot_id"] == after["body"]["iceberg_snapshot_id"]
        and retry_poll["status"] == "retry"
        and "không tới được" in retry_poll.get("error", "")
        and recovered["status"] == "unchanged"
        and empty["status"] in {"published", "source_empty"}
        and empty.get("window_rows", 0) == 0
        and (empty.get("flink_properties") or {}).get("format-version") == "2"
        and (empty.get("flink_properties") or {}).get("write.upsert.enabled") == "true"
    )
    return _case(
        "thiếu bảng, ClickHouse ngắt và bảng rỗng",
        "reader it_trend giữ nguyên, poll trả retry, bảng rỗng không có dòng",
        {"missing": missing, "poll": retry_poll, "recovered": recovered["status"], "empty": empty},
        ok,
    )


def _compare(spark, snapshot_id: int, logical: list[tuple]) -> dict:
    spark.sql(f"REFRESH TABLE {WINDOWS}")
    rows = spark.sql(
        f"""
        SELECT movie_id, CAST(unix_millis(CAST(window_start AS TIMESTAMP)) AS BIGINT) AS start_ms,
               heartbeat_count, total_watch_seconds, active_users
        FROM {WINDOWS} VERSION AS OF {int(snapshot_id)}
        ORDER BY movie_id, start_ms
        """
    ).collect()
    iceberg = [tuple(int(value) for value in row) for row in rows]
    stored = clickhouse(
        HTTP,
        f"""
        SELECT movie_id, toUnixTimestamp64Milli(window_start), heartbeat_count, total_watch_seconds, active_users
        FROM {DATABASE}.movie_windows
        ORDER BY movie_id, window_start
        FORMAT TSV
        """,
    )
    serving = []
    for line in stored.splitlines():
        if line.strip():
            serving.append(tuple(int(float(part)) for part in line.split("\t")))
    return {"iceberg": iceberg, "serving": serving, "logical": logical, "match": iceberg == serving == logical}


def _expect_api(logical: list[tuple], minutes: int) -> list[dict]:
    anchor = max(start + 60_000 for movie, start, *_rest in logical)
    cutoff = anchor - minutes * 60_000
    grouped: dict[int, dict] = {}
    for movie, start, beats, watch, users in logical:
        end = start + 60_000
        if not (cutoff < end <= anchor):
            continue
        item = grouped.setdefault(movie, {"movie_id": movie, "recent_heartbeats": 0, "recent_watch_seconds": 0, "peak_minute_unique_viewers": 0})
        item["recent_heartbeats"] += beats
        item["recent_watch_seconds"] += watch
        item["peak_minute_unique_viewers"] = max(item["peak_minute_unique_viewers"], users)
    return sorted(grouped.values(), key=lambda item: (-item["recent_heartbeats"], -item["recent_watch_seconds"], item["movie_id"]))


def _delete_evidence(spark) -> dict:
    spark.sql(f"REFRESH TABLE {WINDOWS}")
    logical_rows = int(spark.sql(f"SELECT count(*) AS n FROM {WINDOWS}").collect()[0].n)
    evidence = {"logical_rows": logical_rows, "equality_records": 0, "data_records": 0, "snapshots": [], "error": None}
    try:
        snaps = spark.sql(
            f"SELECT snapshot_id, operation, summary FROM {WINDOWS}.snapshots ORDER BY committed_at"
        ).collect()
        evidence["snapshots"] = [
            {"snapshot_id": int(row.snapshot_id), "operation": row.operation, "summary": dict(row.summary or {})} for row in snaps
        ]
        for row in snaps:
            summary = dict(row.summary or {})
            evidence["equality_records"] += int(summary.get("total-equality-deletes") or 0)
    except Exception as exc:
        evidence["error"] = str(exc).splitlines()[-1][:300]
    try:
        data = spark.sql(f"SELECT coalesce(sum(record_count), 0) AS records FROM {WINDOWS}.data_files").collect()[0]
        evidence["data_records"] = int(data.records)
    except Exception as exc:
        evidence["data_error"] = str(exc).splitlines()[-1][:300]
    try:
        deletes = spark.sql(f"SELECT content, coalesce(sum(record_count), 0) AS records FROM {WINDOWS}.delete_files GROUP BY content").collect()
        evidence["delete_files"] = [{"content": int(row.content), "records": int(row.records)} for row in deletes]
        evidence["equality_records"] += sum(item["records"] for item in evidence["delete_files"] if item["content"] == 2)
    except Exception as exc:
        evidence["delete_error"] = str(exc).splitlines()[-1][:300]
    return evidence


def _empty_table(spark) -> dict:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {EMPTY} (
          movie_id BIGINT,
          window_start TIMESTAMP,
          window_end TIMESTAMP,
          total_watch_seconds BIGINT,
          heartbeat_count BIGINT,
          active_users BIGINT
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """
    )
    result = sync_windows(spark, source_table=EMPTY, http=HTTP, database=EMPTY_DATABASE)
    props = {}
    try:
        spark.sql(f"REFRESH TABLE {WINDOWS}")
        rows = spark.sql(f"SHOW TBLPROPERTIES {WINDOWS}").collect()
        props = {row.key: row.value for row in rows}
        result["flink_properties"] = {key: props.get(key) for key in ("format-version", "write.upsert.enabled")}
    except Exception as exc:
        result["flink_properties_error"] = str(exc).splitlines()[-1][:200]
    return result


def _snapshot(spark) -> dict:
    spark.sql(f"REFRESH TABLE {WINDOWS}")
    row = spark.sql(
        f"SELECT snapshot_id, operation, committed_at FROM {WINDOWS}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]
    committed = row.committed_at
    return {
        "snapshot_id": int(row.snapshot_id),
        "operation": row.operation,
        "committed_at": str(committed),
    }


def _wait_rows(spark, expected: set[tuple], timeout: int = 180) -> list[tuple]:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            found = set(_logical(spark))
            last = sorted(found)
            print("windows", last, flush=True)
            if found == expected and _copies_ok(spark):
                return last
        except Exception as exc:
            print("wait", str(exc).splitlines()[-1][:180], flush=True)
        time.sleep(3)
    raise SystemExit(f"cửa sổ chưa khớp {expected}: {last}")


def _logical(spark) -> list[tuple]:
    spark.sql(f"REFRESH TABLE {WINDOWS}")
    rows = spark.sql(
        f"""
        SELECT movie_id,
               CAST(unix_millis(CAST(window_start AS TIMESTAMP)) AS BIGINT) AS start_ms,
               heartbeat_count, total_watch_seconds, active_users
        FROM {WINDOWS}
        ORDER BY movie_id, start_ms
        """
    ).collect()
    return [tuple(int(value) for value in row) for row in rows]


def _copies_ok(spark) -> bool:
    rows = spark.sql(
        f"""
        SELECT movie_id, window_start, COUNT(*) AS copies
        FROM {WINDOWS}
        GROUP BY movie_id, window_start
        HAVING COUNT(*) > 1
        """
    ).collect()
    return not rows


def _wait_api(snapshot_id: int, timeout: int = 20) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = _get(API, "/movies/trending?window_minutes=15&limit=10")
        body = last.get("body") or {}
        if last["status"] == 200 and body.get("iceberg_snapshot_id") == snapshot_id:
            return last
        time.sleep(0.2)
    raise SystemExit(f"API chưa thấy snapshot {snapshot_id}: {last}")


def _lags(points: dict) -> dict:
    sent = points["sent_at"]
    return {
        "event_to_iceberg_s": round(points["iceberg_visible_at"] - sent, 3),
        "iceberg_to_sync_s": round(points["synced_at"] - points["iceberg_visible_at"], 3),
        "sync_to_api_s": round(points["api_visible_at"] - points["synced_at"], 3),
        "event_to_api_s": round(points["api_visible_at"] - sent, 3),
    }


def _items(body: dict) -> list[dict]:
    return [
        {
            "movie_id": item["movie_id"],
            "recent_heartbeats": item["recent_heartbeats"],
            "recent_watch_seconds": item["recent_watch_seconds"],
            "peak_minute_unique_viewers": item["peak_minute_unique_viewers"],
        }
        for item in body.get("items") or []
    ]


def _beat(heartbeat_id: int, user_id: int, movie_id: int, event_ms: int) -> dict:
    payload = event(heartbeat_id, user_id, event_ms)
    payload["movie_id"] = movie_id
    return payload


def _vertices(job_id: str) -> list[str]:
    with urllib.request.urlopen(f"http://127.0.0.1:18081/jobs/{job_id}") as response:
        job = json.load(response)
    return [vertex["name"] for vertex in job["vertices"]]


def _release_test_jobs() -> None:
    blockers = []
    for job in running_jobs():
        name = job.get("name") or ""
        if "it_p1" in name or "it_p2" in name or "it-trending" in name:
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


def _start_api() -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["CINESTREAM_CLICKHOUSE_DATABASE"] = DATABASE
    env["CINESTREAM_CLICKHOUSE_HTTP"] = HTTP
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "serving.api.app:app", "--host", "127.0.0.1", "--port", "18086"],
        cwd=str(REPO),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            _get(API, "/health")
            return process
        except Exception:
            if process.poll() is not None:
                output = process.stdout.read().decode() if process.stdout else ""
                raise SystemExit(f"API không lên: {output[-1000:]}")
            time.sleep(0.3)
    raise SystemExit("API không lên")


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
