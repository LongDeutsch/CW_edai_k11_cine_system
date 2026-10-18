"""Đối chiếu dashboard với FastAPI thật. Không bật streaming production.

Database batch ``it_it_af_real``, trending ``it_e2e``, rỗng ``cinestream_serving``.
Database ``it_dash_conflict`` chỉ để gây HTTP 409 rồi bị xóa.
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

from serving.clickhouse.sync_gold import apply_ddl, clickhouse

REPO = Path(__file__).resolve().parents[2]
STATIC = REPO / "serving" / "dashboard" / "static"
REPORT = REPO / "docs" / "proof" / "logs" / "phase4_dashboard_it.json"
HTTP = "http://127.0.0.1:18123"
API_PORT = 18110
DASH_PORT = 18111
ID_KEYS = {"engagement_snapshot_id", "ratings_snapshot_id", "iceberg_snapshot_id", "trending_snapshot_id"}
PATHS = (
    "/analytics/overview",
    "/movies/top-rated?limit=10",
    "/movies/most-watched?limit=10",
    "/movies/trending?window_minutes=15&limit=10",
    "/serving/status",
)


def main() -> None:
    cases = [_case_static(), _case_client()]
    processes = []
    try:
        cases.append(_case_live("batch it_it_af_real", "it_it_af_real", HTTP, processes))
        _stop(processes)
        cases.append(_case_live("trending it_e2e", "it_e2e", HTTP, processes))
        _stop(processes)
        cases.append(_case_live("serving rỗng", "cinestream_serving", HTTP, processes))
        _stop(processes)
        cases.append(_case_live("ClickHouse tắt", "cinestream_serving", "http://127.0.0.1:9", processes))
        _stop(processes)
        cases.append(_case_conflict(processes))
        cases.append(_case_compose())
    except (SystemExit, Exception) as exc:
        detail = exc.code if isinstance(exc, SystemExit) and not isinstance(exc.code, int) else str(exc)
        cases.append(_case("suite dừng", "các case phía trên chạy hết", {"error": str(detail)[-1500:]}, False))
    finally:
        _stop(processes)
        clickhouse(HTTP, "DROP DATABASE IF EXISTS it_dash_conflict SYNC")
    failed = [item["name"] for item in cases if item["status"] != "PASS"]
    report = {"status": "FAIL" if failed else "PASS", "failed": failed, "cases": cases}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "failed": failed}, ensure_ascii=False))
    if failed:
        raise SystemExit(f"FAIL {failed}")


def _case_static() -> dict:
    text = "\n".join(path.read_text() for path in STATIC.glob("*"))
    leaked = [token for token in ("662830", "3725663032", "Don't Be a Menace", "4517552679501749559") if token in text]
    ok = not leaked and "/analytics/overview" in text and "/movies/trending" in text
    return _case("static không nhúng số liệu", "trang chỉ có tên endpoint", {"leaked": leaked}, ok)


def _case_client() -> dict:
    script = r"""
import { getJson, parseApi } from "./serving/dashboard/static/client.js";
import { present } from "./serving/dashboard/static/view.js";

const sleep = async () => {};
const result = {};

const raw = '{"engagement_snapshot_id":1073738621869077082,"mean_rating":3.5,"items":[{"iceberg_snapshot_id":4517552679501749559}]}';
const parsed = parseApi(raw);
result.snapshot = parsed.engagement_snapshot_id;
result.itemSnapshot = parsed.items[0].iceberg_snapshot_id;
result.mean = parsed.mean_rating;

let calls = 0;
result.recovered = await getJson("/analytics/overview", {
  retries: 2,
  retryDelayMs: 1,
  sleep,
  fetchImpl: async () => {
    calls += 1;
    if (calls < 3) return jsonResponse(503, { detail: "ClickHouse không tới được: refused" });
    return jsonResponse(200, { session_count: 4 });
  },
});
result.recoverCalls = calls;

calls = 0;
try {
  await getJson("/analytics/overview", {
    retries: 2,
    retryDelayMs: 1,
    sleep,
    fetchImpl: async () => {
      calls += 1;
      return jsonResponse(409, { detail: "reader table đang trộn nhiều snapshot" });
    },
  });
  result.conflict = null;
} catch (error) {
  result.conflict = { status: error.status, detail: error.detail, calls };
}

try {
  await getJson("/analytics/overview", {
    timeoutMs: 30,
    retries: 1,
    retryDelayMs: 1,
    sleep,
    fetchImpl: (_url, opts) => new Promise((_resolve, reject) => {
      opts.signal.addEventListener("abort", () => {
        const error = new Error("aborted");
        error.name = "AbortError";
        reject(error);
      });
    }),
  });
  result.timeout = null;
} catch (error) {
  result.timeout = { status: error.status, detail: error.detail };
}

const held = present({
  trending: { live: true, freshness: "live", items: [] },
  hold: true,
  transport: { status: 0, detail: "hết thời gian chờ FastAPI" },
});
result.heldLive = held.live;
result.banner = held.banner;

const fresh = present({ trending: { live: true, freshness: "idle", items: [] } });
result.idleLive = fresh.live;
const really = present({ trending: { live: true, freshness: "live", items: [], latest_window_end: "x" } });
result.reallyLive = really.live;
console.log(JSON.stringify(result));

function jsonResponse(status, body) {
  return { ok: status < 400, status, async text() { return JSON.stringify(body); } };
}
"""
    payload = _node(script)
    ok = (
        payload["snapshot"] == "1073738621869077082"
        and payload["itemSnapshot"] == "4517552679501749559"
        and payload["mean"] == 3.5
        and payload["recovered"]["session_count"] == 4
        and payload["recoverCalls"] == 3
        and payload["conflict"]["status"] == 409
        and payload["conflict"]["calls"] == 3
        and payload["timeout"]["detail"] == "hết thời gian chờ FastAPI"
        and payload["heldLive"] is False
        and payload["idleLive"] is False
        and payload["reallyLive"] is True
        and "Không gắn nhãn live" in payload["banner"]
    )
    return _case(
        "retry, timeout và nhãn live",
        "503 được thử lại, 409 trả về, dữ liệu cũ không thành live",
        payload,
        ok,
    )


def _case_live(name: str, database: str, clickhouse_http: str, processes: list) -> dict:
    api = _start_api(database, clickhouse_http)
    dash = _start_dashboard(f"http://127.0.0.1:{API_PORT}")
    processes.extend([api, dash])
    _wait(f"http://127.0.0.1:{API_PORT}/health")
    _wait(f"http://127.0.0.1:{DASH_PORT}/")
    compared = []
    for path in PATHS:
        direct = _request(f"http://127.0.0.1:{API_PORT}{path}")
        proxied = _request(f"http://127.0.0.1:{DASH_PORT}/api{path}")
        compared.append(
            {
                "path": path,
                "direct": direct["status"],
                "proxy": proxied["status"],
                "same": direct["status"] == proxied["status"] and _stable(direct["body"]) == _stable(proxied["body"]),
            }
        )
    bodies = {item["path"]: _request(f"http://127.0.0.1:{API_PORT}{item['path']}") for item in compared}
    model = _present(bodies)
    checks = _expect(database, clickhouse_http, bodies, model)
    ok = all(item["same"] for item in compared) and all(checks.values())
    return _case(
        name,
        "proxy trả đúng body FastAPI và model hiển thị đúng field",
        {"proxy": compared, "model": _brief(model), "checks": checks, "samples": _samples(bodies)},
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
        (10, 'A', 'PG', 1, 4.0, 101, 'b1')
        """,
    )
    api = _start_api("it_dash_conflict", HTTP)
    dash = _start_dashboard(f"http://127.0.0.1:{API_PORT}")
    processes.extend([api, dash])
    _wait(f"http://127.0.0.1:{API_PORT}/health")
    overview = _request(f"http://127.0.0.1:{DASH_PORT}/api/analytics/overview")
    status = _request(f"http://127.0.0.1:{DASH_PORT}/api/serving/status")
    model = _present(
        {
            "/analytics/overview": overview,
            "/movies/top-rated?limit=10": _request(f"http://127.0.0.1:{API_PORT}/movies/top-rated?limit=10"),
            "/movies/most-watched?limit=10": _request(f"http://127.0.0.1:{API_PORT}/movies/most-watched?limit=10"),
            "/movies/trending?window_minutes=15&limit=10": _request(
                f"http://127.0.0.1:{API_PORT}/movies/trending?window_minutes=15&limit=10"
            ),
            "/serving/status": status,
        }
    )
    ok = (
        overview["status"] == 409
        and status["status"] == 200
        and status["body"]["consistent"] is False
        and model["overview"] is None
        and model["live"] is False
        and any(error["status"] == 409 for error in model["errors"])
    )
    return _case(
        "snapshot lệch 409",
        "overview 409, status consistent=false, không vẽ metric batch",
        {"overview": overview["status"], "status": status["body"], "banner": model["banner"]},
        ok,
    )


def _case_compose() -> dict:
    completed = subprocess.run(
        ["docker", "compose", "-f", "infra/compose/docker-compose.dashboard.yml", "config"],
        cwd=REPO,
        text=True,
        capture_output=True,
        check=False,
    )
    text = completed.stdout + completed.stderr
    ok = completed.returncode == 0 and "18085" in text and "nginx:1.27-alpine" in text
    return _case("compose dashboard", "file compose hợp lệ, cổng 18085", {"code": completed.returncode, "tail": text[-400:]}, ok)


def _expect(database: str, clickhouse_http: str, bodies: dict, model: dict) -> dict:
    overview = bodies["/analytics/overview"]
    trending = bodies["/movies/trending?window_minutes=15&limit=10"]
    status = bodies["/serving/status"]
    checks = {"live_matches_api": model["live"] is False or _api_live(trending)}
    if clickhouse_http.endswith(":9"):
        checks["all_503"] = all(item["status"] == 503 for item in bodies.values())
        checks["no_overview"] = model["overview"] is None
        checks["not_live"] = model["live"] is False
        return checks
    if overview["status"] != 200:
        checks["overview_status"] = False
        return checks
    body = overview["body"]
    shown = model["overview"]
    checks["sessions"] = shown["session_count"] == body["session_count"]
    checks["watch"] = shown["total_watch_seconds"] == body["total_watch_seconds"]
    checks["watched_movies"] = shown["watched_movie_count"] == body["watched_movie_count"]
    checks["rated_movies"] = shown["rated_movie_count"] == body["rated_movie_count"]
    checks["ratings"] = shown["rating_count"] == body["rating_count"]
    checks["mean"] = shown["mean_rating"] == body["mean_rating"]
    checks["engagement_snapshot"] = _same_id(shown["engagement_snapshot_id"], body["engagement_snapshot_id"])
    checks["ratings_snapshot"] = _same_id(shown["ratings_snapshot_id"], body["ratings_snapshot_id"])
    if database == "cinestream_serving":
        checks["empty_batch"] = body["session_count"] == 0 and body["engagement_snapshot_id"] is None
    if database == "it_it_af_real":
        checks["real_batch"] = body["session_count"] > 1000 and body["published_at"] is not None
    if trending["status"] == 200:
        trend = trending["body"]
        checks["freshness"] = model["freshness"] == trend["freshness"]
        checks["window_end"] = model["trending"]["latest_window_end"] == trend["latest_window_end"]
        checks["synced_at"] = model["trending"]["synced_at"] == trend["synced_at"]
        checks["trend_items"] = [item["movie_id"] for item in model["trending"]["items"]] == [
            item["movie_id"] for item in trend["items"]
        ]
        checks["trend_heartbeats"] = [item["recent_heartbeats"] for item in model["trending"]["items"]] == [
            item["recent_heartbeats"] for item in trend["items"]
        ]
        checks["not_live_unless_api"] = model["live"] == (trend["live"] is True and trend["freshness"] == "live")
    else:
        checks["trend_error_visible"] = any(error["section"] == "trending" for error in model["errors"])
        checks["batch_still_shown"] = shown is not None
    if status["status"] == 200:
        checks["status_database"] = model["database"] == status["body"]["database"] == database
        checks["status_consistent"] = model["batchConsistent"] == status["body"]["consistent"]
    top = bodies["/movies/top-rated?limit=10"]
    if top["status"] == 200:
        checks["top_ids"] = [item["movie_id"] for item in model["topRated"]["items"]] == [
            item["movie_id"] for item in top["body"]["items"]
        ]
        checks["top_avg"] = [item["avg_rating"] for item in model["topRated"]["items"]] == [
            item["avg_rating"] for item in top["body"]["items"]
        ]
    watched = bodies["/movies/most-watched?limit=10"]
    if watched["status"] == 200:
        checks["watched_ids"] = [item["movie_id"] for item in model["mostWatched"]["items"]] == [
            item["movie_id"] for item in watched["body"]["items"]
        ]
        checks["watched_seconds"] = [item["watch_seconds"] for item in model["mostWatched"]["items"]] == [
            item["watch_seconds"] for item in watched["body"]["items"]
        ]
    return checks


def _present(bodies: dict) -> dict:
    payload = {}
    mapping = {
        "/analytics/overview": "overview",
        "/movies/top-rated?limit=10": "topRated",
        "/movies/most-watched?limit=10": "mostWatched",
        "/movies/trending?window_minutes=15&limit=10": "trending",
        "/serving/status": "status",
    }
    for path, key in mapping.items():
        found = bodies[path]
        if found["status"] == 200:
            payload[key] = _ids_as_strings(found["body"])
            payload[f"{key}Error"] = None
        else:
            payload[key] = None
            detail = found["body"]["detail"] if isinstance(found["body"], dict) else str(found["body"])
            payload[f"{key}Error"] = {"status": found["status"], "detail": detail}
    return _node(
        """
import { present } from "./serving/dashboard/static/view.js";
let raw = "";
for await (const chunk of process.stdin) raw += chunk;
const input = JSON.parse(raw);
console.log(JSON.stringify(present(input)));
""",
        payload,
    )


def _ids_as_strings(value):
    if isinstance(value, dict):
        return {
            key: str(item) if key in ID_KEYS and isinstance(item, int) else _ids_as_strings(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_ids_as_strings(item) for item in value]
    return value


def _node(script: str, payload: dict | None = None) -> dict:
    completed = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        cwd=REPO,
        input=json.dumps(payload) if payload is not None else None,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(completed.stderr[-800:] or completed.stdout[-800:])
    return json.loads(completed.stdout)


def _start_api(database: str, clickhouse_http: str):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["CINESTREAM_CLICKHOUSE_DATABASE"] = database
    env["CINESTREAM_CLICKHOUSE_HTTP"] = clickhouse_http
    env.pop("CINESTREAM_ALLOW_STREAMING", None)
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "serving.api.app:app", "--host", "127.0.0.1", "--port", str(API_PORT)],
        cwd=REPO,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _start_dashboard(api_url: str):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["CINESTREAM_API_URL"] = api_url
    env["CINESTREAM_DASHBOARD_PORT"] = str(DASH_PORT)
    return subprocess.Popen(
        [sys.executable, "-m", "serving.dashboard.server"],
        cwd=REPO,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait(url: str) -> None:
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):
                return
        except Exception:
            time.sleep(0.2)
    raise SystemExit(f"không lên {url}")


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


def _api_live(trending: dict) -> bool:
    body = trending.get("body") or {}
    return trending["status"] == 200 and body.get("live") is True and body.get("freshness") == "live"


def _stable(value):
    ages = {"event_age_seconds", "sync_age_seconds", "trending_event_age_seconds", "trending_sync_age_seconds"}
    if isinstance(value, dict):
        return {key: _stable(item) for key, item in value.items() if key not in ages}
    if isinstance(value, list):
        return [_stable(item) for item in value]
    return value


def _same_id(shown, raw) -> bool:
    if raw is None:
        return shown is None
    return str(shown) == str(raw)


def _samples(bodies: dict) -> dict:
    found = {}
    for path, item in bodies.items():
        body = item["body"]
        if not isinstance(body, dict):
            found[path] = {"status": item["status"]}
            continue
        found[path] = {
            "status": item["status"],
            "session_count": body.get("session_count"),
            "total_watch_seconds": body.get("total_watch_seconds"),
            "freshness": body.get("freshness") or body.get("trending_freshness"),
            "live": body.get("live", body.get("trending_live")),
            "items": len(body["items"]) if isinstance(body.get("items"), list) else None,
        }
    return found


def _brief(model: dict) -> dict:
    overview = model.get("overview") or {}
    trending = model.get("trending") or {}
    return {
        "live": model.get("live"),
        "freshness": model.get("freshness"),
        "session_count": overview.get("session_count"),
        "database": model.get("database"),
        "banner": model.get("banner"),
        "trend_items": len((trending.get("items") or [])),
    }


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
