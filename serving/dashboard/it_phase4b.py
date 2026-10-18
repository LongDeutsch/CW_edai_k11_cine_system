"""Một FastAPI đọc batch và trending ở hai database. Không ghi production.

Batch ``it_it_af_real``. Trending ``it_e2e``. Không bật streaming production.
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

from serving.clickhouse.sync_gold import apply_ddl, clickhouse, clickhouse_json

REPO = Path(__file__).resolve().parents[2]
REPORT = REPO / "docs" / "proof" / "logs" / "phase4b_unified_it.json"
HTTP = "http://127.0.0.1:18123"
API_PORT = 18120
DASH_PORT = 18121
BATCH = "it_it_af_real"
TREND = "it_e2e"
OVERVIEW = "/analytics/overview"
TOP = "/movies/top-rated?limit=10"
WATCHED = "/movies/most-watched?limit=10"
TRENDING = "/movies/trending?window_minutes=15&limit=10"
STATUS = "/serving/status"
PRODUCTION = (
    "namespaces/silver/tables/stg_watch_sessions",
    "namespaces/silver/tables/stg_playback_heartbeats",
    "namespaces/silver/tables/stg_playback_windows",
    "namespaces/bronze/tables/raw_users",
)


def main() -> None:
    before = {"iceberg": _fingerprint(), "clickhouse": _serving_counts()}
    cases = []
    processes = []
    try:
        cases.append(_case_both(processes))
        _stop(processes)
        cases.append(_case_trend_down(processes))
        _stop(processes)
        cases.append(_case_batch_down(processes))
        _stop(processes)
        cases.append(_case_conflict(processes))
        _stop(processes)
        cases.append(_case_clickhouse_down(processes))
    except (SystemExit, Exception) as exc:
        detail = exc.code if isinstance(exc, SystemExit) and not isinstance(exc.code, int) else str(exc)
        cases.append(_case("suite dừng", "các case phía trên chạy hết", {"error": str(detail)[-1500:]}, False))
    finally:
        _stop(processes)
        clickhouse(HTTP, "DROP DATABASE IF EXISTS it_dash_conflict SYNC")
    after = {"iceberg": _fingerprint(), "clickhouse": _serving_counts()}
    cases.append(
        _case(
            "production không đổi",
            "REST production và số dòng cinestream_serving, it_it_af_real, it_e2e giữ nguyên",
            {"before": before, "after": after},
            before == after,
        )
    )
    failed = [item["name"] for item in cases if item["status"] != "PASS"]
    report = {"status": "FAIL" if failed else "PASS", "failed": failed, "cases": cases}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "failed": failed}, ensure_ascii=False))
    if failed:
        raise SystemExit(f"FAIL {failed}")


def _case_both(processes: list) -> dict:
    _boot(processes, BATCH, TREND, HTTP)
    overview = _get(OVERVIEW)
    top = _get(TOP)
    watched = _get(WATCHED)
    trending = _get(TRENDING)
    status = _get(STATUS)
    ranked = _rankings()
    windows = _windows()
    body = overview["body"]
    trend = trending["body"]
    ok = (
        overview["status"] == 200
        and top["status"] == 200
        and watched["status"] == 200
        and trending["status"] == 200
        and status["status"] == 200
        and status["body"]["batch_database"] == BATCH
        and status["body"]["trending_database"] == TREND
        and status["body"]["database"] == BATCH
        and trend["database"] == TREND
        and body["session_count"] == ranked["sessions"]
        and body["session_count"] > 1000
        and status["body"]["consistent"] is True
        and trend["freshness"] == "stale"
        and trend["live"] is False
        and _movies(top) == ranked["top"]
        and _movies(watched) == ranked["watched"]
        and _trend_rows(trend) == windows
    )
    return _case(
        "cả hai nguồn",
        "overview batch và trending stale cùng một process",
        {
            "sessions": body.get("session_count") if isinstance(body, dict) else overview["status"],
            "freshness": trend.get("freshness") if isinstance(trend, dict) else trending["status"],
            "live": trend.get("live") if isinstance(trend, dict) else None,
            "top": _movies(top),
            "watched": _movies(watched),
            "windows": windows,
            "status_databases": {
                "batch": status["body"].get("batch_database") if isinstance(status["body"], dict) else None,
                "trending": status["body"].get("trending_database") if isinstance(status["body"], dict) else None,
            },
        },
        ok,
    )


def _case_trend_down(processes: list) -> dict:
    _boot(processes, BATCH, "it_dash_absent_stream", HTTP)
    overview = _get(OVERVIEW)
    trending = _get(TRENDING)
    status = _get(STATUS)
    ok = (
        overview["status"] == 200
        and overview["body"]["session_count"] > 1000
        and trending["status"] == 503
        and status["status"] == 200
        and status["body"]["consistent"] is True
        and status["body"]["batch_database"] == BATCH
        and status["body"]["trending_freshness"] == "sync_failed"
        and status["body"]["trending_live"] is False
    )
    return _case(
        "trending không có, batch còn",
        "overview 200, trending 503, status vẫn trả batch",
        {"overview": overview["status"], "sessions": overview["body"].get("session_count"), "trending": trending["status"], "status": _status_brief(status)},
        ok,
    )


def _case_batch_down(processes: list) -> dict:
    _boot(processes, "it_dash_absent_batch", TREND, HTTP)
    overview = _get(OVERVIEW)
    trending = _get(TRENDING)
    status = _get(STATUS)
    trend = trending["body"]
    ok = (
        overview["status"] == 503
        and trending["status"] == 200
        and trend["database"] == TREND
        and trend["items"]
        and trend["items"][0]["movie_id"] == 42
        and trend["freshness"] == "stale"
        and trend["live"] is False
        and status["status"] == 200
        and status["body"]["consistent"] is False
        and status["body"]["trending_database"] == TREND
        and status["body"]["trending_snapshot_id"] is not None
    )
    return _case(
        "batch không có, trending còn",
        "overview 503, trending 200 và stale, status không nuốt trending",
        {"overview": overview["status"], "trending": _trend_rows(trend) if isinstance(trend, dict) else trending["status"], "status": _status_brief(status)},
        ok,
    )


def _case_conflict(processes: list) -> dict:
    clickhouse(HTTP, "DROP DATABASE IF EXISTS it_dash_conflict SYNC")
    apply_ddl(HTTP, "it_dash_conflict")
    clickhouse(
        HTTP,
        """
        INSERT INTO it_dash_conflict.movie_engagement VALUES
        (1, 20200101, 1, 'basic', 10, 'A', 'PG', 30, 0.2, 'phone', 101, 'b1'),
        (2, 20200101, 2, 'basic', 11, 'B', 'PG', 40, 0.3, 'phone', 202, 'b1')
        """,
    )
    clickhouse(
        HTTP,
        """
        INSERT INTO it_dash_conflict.movie_ratings VALUES
        (10, 'A', 'PG', 1, 4.0, 101, 'b1'),
        (11, 'B', 'PG', 1, 3.0, 202, 'b1')
        """,
    )
    _boot(processes, "it_dash_conflict", TREND, HTTP)
    overview = _get(OVERVIEW)
    top = _get(TOP)
    watched = _get(WATCHED)
    trending = _get(TRENDING)
    status = _get(STATUS)
    ok = (
        overview["status"] == 409
        and top["status"] == 409
        and watched["status"] == 409
        and trending["status"] == 200
        and trending["body"]["items"][0]["movie_id"] == 42
        and status["status"] == 200
        and status["body"]["consistent"] is False
        and status["body"]["trending_database"] == TREND
    )
    return _case(
        "409 batch, trending vẫn đọc được",
        "overview và hai bảng xếp hạng 409, trending 200, status consistent false",
        {
            "overview": overview["status"],
            "top": top["status"],
            "watched": watched["status"],
            "trending": trending["status"],
            "status": _status_brief(status),
        },
        ok,
    )


def _case_clickhouse_down(processes: list) -> dict:
    _boot(processes, BATCH, TREND, "http://127.0.0.1:9")
    found = {path: _get(path)["status"] for path in (OVERVIEW, TOP, WATCHED, TRENDING, STATUS)}
    ok = set(found.values()) == {503}
    return _case(
        "ClickHouse không tới được",
        "cả năm endpoint 503, kể cả status khi cả hai database cùng mất kết nối",
        found,
        ok,
    )


def _rankings() -> dict:
    sessions = int(clickhouse(HTTP, f"SELECT count() FROM {BATCH}.movie_engagement").strip())
    top = clickhouse_json(
        HTTP,
        f"""
        SELECT movie_id, rating_count, avg_rating
        FROM {BATCH}.movie_ratings
        ORDER BY avg_rating DESC, rating_count DESC, movie_id ASC
        LIMIT 10
        """,
    )
    watched = clickhouse_json(
        HTTP,
        f"""
        SELECT movie_id, toInt64(sum(watch_duration_seconds)) AS watch_seconds, count() AS session_count
        FROM {BATCH}.movie_engagement
        GROUP BY movie_id
        ORDER BY watch_seconds DESC, session_count DESC, movie_id ASC
        LIMIT 10
        """,
    )
    return {
        "sessions": sessions,
        "top": [(int(row["movie_id"]), int(row["rating_count"]), float(row["avg_rating"])) for row in top],
        "watched": [(int(row["movie_id"]), int(row["watch_seconds"]), int(row["session_count"])) for row in watched],
    }


def _windows() -> list[tuple[int, int, int, int]]:
    rows = clickhouse_json(
        HTTP,
        f"""
        SELECT
            movie_id,
            toInt64(sum(heartbeat_count)) AS recent_heartbeats,
            toInt64(sum(total_watch_seconds)) AS recent_watch_seconds,
            toInt64(max(active_users)) AS peak_minute_unique_viewers
        FROM {TREND}.movie_windows
        WHERE window_end > (SELECT max(window_end) FROM {TREND}.movie_windows) - toIntervalMinute(15)
          AND window_end <= (SELECT max(window_end) FROM {TREND}.movie_windows)
        GROUP BY movie_id
        ORDER BY recent_heartbeats DESC, recent_watch_seconds DESC, movie_id ASC
        LIMIT 10
        """,
    )
    return [
        (int(row["movie_id"]), int(row["recent_heartbeats"]), int(row["recent_watch_seconds"]), int(row["peak_minute_unique_viewers"]))
        for row in rows
    ]


def _movies(response: dict) -> list[tuple]:
    body = response["body"]
    if "avg_rating" in body["items"][0]:
        return [(int(item["movie_id"]), int(item["rating_count"]), float(item["avg_rating"])) for item in body["items"]]
    return [(int(item["movie_id"]), int(item["watch_seconds"]), int(item["session_count"])) for item in body["items"]]


def _trend_rows(body: dict) -> list[tuple[int, int, int, int]]:
    return [
        (int(item["movie_id"]), int(item["recent_heartbeats"]), int(item["recent_watch_seconds"]), int(item["peak_minute_unique_viewers"]))
        for item in body["items"]
    ]


def _serving_counts() -> dict:
    rows = clickhouse_json(
        HTTP,
        """
        SELECT database, `table` AS table_name, sum(rows) AS rows
        FROM system.parts
        WHERE active AND database IN ('cinestream_serving', 'it_it_af_real', 'it_e2e')
        GROUP BY database, table_name
        ORDER BY database, table_name
        """,
    )
    return {f"{row['database']}.{row['table_name']}": int(row["rows"]) for row in rows}


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


def _boot(processes: list, batch: str, trending: str, clickhouse_http: str) -> None:
    processes.append(_start_api(batch, trending, clickhouse_http))
    processes.append(_start_dashboard())
    _wait(f"http://127.0.0.1:{API_PORT}/health")
    _wait(f"http://127.0.0.1:{DASH_PORT}/")


def _start_api(batch: str, trending: str, clickhouse_http: str):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["CINESTREAM_CLICKHOUSE_DATABASE"] = batch
    env["CINESTREAM_TRENDING_DATABASE"] = trending
    env["CINESTREAM_CLICKHOUSE_HTTP"] = clickhouse_http
    env.pop("CINESTREAM_ALLOW_STREAMING", None)
    env.pop("CINESTREAM_ALLOW_PRODUCTION", None)
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "serving.api.app:app", "--host", "127.0.0.1", "--port", str(API_PORT)],
        cwd=REPO,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _start_dashboard():
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["CINESTREAM_API_URL"] = f"http://127.0.0.1:{API_PORT}"
    env["CINESTREAM_DASHBOARD_PORT"] = str(DASH_PORT)
    return subprocess.Popen(
        [sys.executable, "-m", "serving.dashboard.server"],
        cwd=REPO,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _get(path: str) -> dict:
    direct = _request(f"http://127.0.0.1:{API_PORT}{path}")
    proxied = _request(f"http://127.0.0.1:{DASH_PORT}/api{path}")
    if direct["status"] != proxied["status"] or _stable(direct["body"]) != _stable(proxied["body"]):
        raise SystemExit(f"proxy lệch {path}: {direct['status']} vs {proxied['status']}")
    return proxied


def _request(url: str) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            raw = response.read().decode()
            return {"status": response.status, "body": json.loads(raw) if raw else None}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = raw
        return {"status": exc.code, "body": body}


def _stable(value):
    ages = {"event_age_seconds", "sync_age_seconds", "trending_event_age_seconds", "trending_sync_age_seconds"}
    if isinstance(value, dict):
        return {key: _stable(item) for key, item in value.items() if key not in ages}
    if isinstance(value, list):
        return [_stable(item) for item in value]
    return value


def _status_brief(response: dict) -> dict:
    body = response["body"] if isinstance(response["body"], dict) else {}
    return {
        "http": response["status"],
        "consistent": body.get("consistent"),
        "batch_database": body.get("batch_database"),
        "trending_database": body.get("trending_database"),
        "trending_freshness": body.get("trending_freshness"),
        "trending_live": body.get("trending_live"),
    }


def _wait(url: str) -> None:
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):
                return
        except Exception:
            time.sleep(0.2)
    raise SystemExit(f"không lên {url}")


def _stop(processes: list) -> None:
    while processes:
        process = processes.pop()
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()


def _case(name: str, expected: str, actual: dict, ok: bool) -> dict:
    status = "PASS" if ok else "FAIL"
    print(f"CASE {status} {name}", flush=True)
    return {"name": name, "status": status, "expected": expected, "actual": actual}


if __name__ == "__main__":
    main()
