"""Độ bền checkpoint và poll trending 30 giây. Không bật streaming production.

Topic ``it.p2.e2e.*``, bảng ``it_p2_e2e_*``, database ``it_e2e``.
"""

from __future__ import annotations

import inspect
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

from serving.clickhouse.sync_gold import clickhouse
from serving.clickhouse.sync_windows import poll_windows
from streaming.flink.it_phase2 import (
    checkpoint_view,
    compose,
    end_offsets,
    event,
    fail_taskmanager,
    read_dlq,
    recreate_topics,
    running_jobs,
    send_records,
    start_spark,
    wait_new_checkpoint,
    wait_restored,
    wait_running,
)
from streaming.flink.runtime_guard import (
    PRODUCTION_DATABASE,
    PRODUCTION_WINDOWS,
    require_streaming_approval,
)

HTTP = "http://127.0.0.1:18123"
API = "http://127.0.0.1:18090"
FLINK = "http://127.0.0.1:18081"
DATABASE = "it_e2e"
NAMESPACE_TABLE = "cinestream.silver.it_p2_e2e_windows"
HEARTBEATS = "cinestream.silver.it_p2_e2e_heartbeats"
TOPIC = "it.p2.e2e.heartbeat"
DLQ = "it.p2.e2e.deadletter"
REPO = Path(__file__).resolve().parents[2]
REPORT = REPO / "docs" / "proof" / "logs" / "phase3e_reliability_it.json"
JOB_PY = "/opt/cinestream/streaming/flink/jobs/heartbeat_window_job.py"
PRODUCTION = (
    "namespaces/silver/tables/stg_watch_sessions",
    "namespaces/silver/tables/stg_playback_heartbeats",
    "namespaces/silver/tables/stg_playback_windows",
    "namespaces/bronze/tables/raw_users",
)
CLOSED = (42, 0, 4, 40, 4)
CORRECTED = (42, 0, 5, 50, 5)


def main() -> None:
    _wait_clickhouse()
    before = _fingerprint()
    offsets_before = _offsets()
    cases = [_case_guard()]
    samples: list[dict] = []
    spark = start_spark("trending-e2e")
    servers = []
    worker = None
    job_id = None
    clickhouse_stopped = False
    try:
        from streaming.flink.it_phase2 import drop_tables

        _release_test_jobs()
        clickhouse(HTTP, f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
        recreate_topics([(TOPIC, 1), (DLQ, 1)])
        drop_tables(spark, [HEARTBEATS, NAMESPACE_TABLE])
        servers.append(_start_api())
        job_id = _submit()
        sent = time.time()
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
        logical = _wait_rows(spark, {CLOSED})
        samples.append(
            {
                "name": "dong_cua_so_dau",
                "event_to_iceberg_s": round(time.time() - sent, 3),
                "note": "Cận trên vì vòng đọc Spark 3 giây",
            }
        )
        checkpoint = wait_new_checkpoint(job_id, 0, timeout=90)
        path = ((checkpoint.get("completed") or {}).get("external_path")) or ""
        restored_before = int((checkpoint_view(job_id).get("counts") or {}).get("restored") or 0)
        id_before_kill = _container_id("cinestream-taskmanager-1")
        dirs_before_kill = _checkpoint_dirs()
        fail_taskmanager()
        restored_kill = wait_restored(job_id, restored_before)
        after_kill = _logical(spark)
        cases.append(
            _case(
                "TaskManager chết rồi start lại",
                "cùng container, job restore từ checkpoint, cửa sổ không đổi",
                {
                    "checkpoint": checkpoint,
                    "dirs": dirs_before_kill,
                    "container_id": id_before_kill,
                    "restored": restored_kill,
                    "rows": after_kill,
                },
                after_kill == [CLOSED]
                and "/opt/flink/checkpoints/" in path
                and int((restored_kill.get("counts") or {}).get("restored") or 0) > restored_before
                and _container_id("cinestream-taskmanager-1") == id_before_kill,
            )
        )
        restored_before = int((checkpoint_view(job_id).get("counts") or {}).get("restored") or 0)
        id_before = _container_id("cinestream-taskmanager-1")
        dirs_before = _checkpoint_dirs()
        recreate = _recreate("taskmanager")
        restored_container = wait_restored(job_id, restored_before, timeout=180)
        after_container = _logical(spark)
        id_after = _container_id("cinestream-taskmanager-1")
        cases.append(
            _case(
                "checkpoint sống sau khi tạo lại container TaskManager",
                "volume flink_checkpoints còn thư mục checkpoint và job tự restore",
                {
                    "path": path,
                    "dirs_before": dirs_before,
                    "dirs_after": recreate["dirs"],
                    "container_before": id_before,
                    "container_after": id_after,
                    "recreate_code": recreate["code"],
                    "restored": restored_container,
                    "rows": after_container,
                },
                after_container == [CLOSED]
                and recreate["code"] == 0
                and id_after not in ("", id_before)
                and _path_survived(path, recreate["dirs"])
                and int((restored_container.get("counts") or {}).get("restored") or 0) > restored_before,
            )
        )
        late = _late_after_restore(spark, samples)
        cases.append(late["case"])
        if late["case"]["status"] != "PASS":
            raise SystemExit("late event sau restore không vào cửa sổ")
        view_now = checkpoint_view(job_id)
        current_id = int((view_now.get("completed") or {}).get("id") or 0)
        mid = wait_new_checkpoint(job_id, current_id, timeout=90)
        durable = wait_new_checkpoint(job_id, int((mid.get("completed") or {}).get("id") or current_id), timeout=90)
        durable_path = ((durable.get("completed") or {}).get("external_path")) or path
        snapshot_before_resume = _snapshot_id(spark)
        rows_before_resume = _logical(spark)
        compose("exec", "-T", "jobmanager", "flink", "cancel", job_id, check=False)
        job_id = None
        _wait_job_gone(timeout=60)
        jm_before = _container_id("cinestream-jobmanager-1")
        tm_before = _container_id("cinestream-taskmanager-1")
        recreated = _recreate("jobmanager", "taskmanager")
        _wait_flink()
        jm_after = _container_id("cinestream-jobmanager-1")
        tm_after = _container_id("cinestream-taskmanager-1")
        job_id = _submit(savepoint=_savepoint_uri(durable_path))
        time.sleep(20)
        snapshot_after_resume = _snapshot_id(spark)
        rows_after_resume = _logical(spark)
        cases.append(
            _case(
                "checkpoint sống sau khi tạo lại JobManager",
                "resume bằng flink run -s, không replay thành snapshot mới, không reset offset",
                {
                    "path": durable_path,
                    "dirs": recreated["dirs"],
                    "jobmanager_before": jm_before,
                    "jobmanager_after": jm_after,
                    "taskmanager_before": tm_before,
                    "taskmanager_after": tm_after,
                    "resumed_job": job_id,
                    "snapshot_before": snapshot_before_resume,
                    "snapshot_after": snapshot_after_resume,
                    "rows_before": rows_before_resume,
                    "rows_after": rows_after_resume,
                },
                recreated["code"] == 0
                and _path_survived(durable_path, recreated["dirs"])
                and jm_after not in ("", jm_before)
                and tm_after not in ("", tm_before)
                and snapshot_after_resume == snapshot_before_resume
                and rows_after_resume == rows_before_resume
                and CORRECTED in rows_after_resume,
            )
        )
        worker, worker_events = _start_worker()
        first_two = _wait_events(worker, worker_events, 2, timeout=150)
        period = _poll_period(first_two[0], first_two[1])
        counts_after_same = _serving_counts()
        cases.append(
            _case(
                "poll cùng snapshot mỗi 30 giây",
                "worker thật dùng interval mặc định, lần sau là unchanged",
                {
                    "default_interval_seconds": inspect.signature(poll_windows).parameters["interval_seconds"].default,
                    "polls": [_public_poll(item) for item in first_two],
                    "period_s": period,
                    "counts": counts_after_same,
                    "rss_kb": _rss(worker.pid),
                },
                inspect.signature(poll_windows).parameters["interval_seconds"].default == 30
                and first_two[0]["body"]["status"] == "published"
                and first_two[1]["body"]["status"] == "unchanged"
                and first_two[0]["body"].get("snapshot_id") == first_two[1]["body"].get("snapshot_id")
                and period is not None
                and 20 <= period <= 70
                and counts_after_same["keys"] == counts_after_same["rows"]
                and counts_after_same["log_rows"] == 1,
            )
        )
        previous_snapshot = first_two[1]["body"].get("snapshot_id")
        seen = len(worker_events)
        next_window = (42, 60_000, 2, 20, 2) if late["pusher_sent"] else (42, 60_000, 1, 10, 1)
        sent_next = time.time()
        send_records(TOPIC, [(_beat(8, 17, 140_000), 0)])
        iceberg_at = _wait_rows_at(spark, {CORRECTED, next_window})
        detected = _wait_new_poll(worker, worker_events, seen, previous_snapshot, timeout=120)
        api_at = time.time()
        visible = _wait_api_snapshot(previous_snapshot)
        samples.append(
            {
                "name": "poll_phat_hien_snapshot_moi",
                "event_to_iceberg_s": round(iceberg_at - sent_next, 3),
                "iceberg_to_sync_s": round(detected["at"] - iceberg_at, 3),
                "sync_to_api_s": round(api_at - detected["at"], 3),
                "event_to_api_s": round(api_at - sent_next, 3),
                "snapshot_id": str(detected["body"].get("snapshot_id")),
                "note": "Worker đang ngủ theo interval 30 giây",
            }
        )
        counts_new = _serving_counts()
        rss_during = _rss(worker.pid)
        cases.append(
            _case(
                "poll phát hiện snapshot mới",
                "worker đang chạy thấy cửa sổ phút tiếp theo, API thấy cùng snapshot",
                {
                    "poll": _public_poll(detected),
                    "api": visible.get("body"),
                    "counts": counts_new,
                    "rss_kb": rss_during,
                    "sample": samples[-1],
                },
                detected["body"]["status"] == "published"
                and visible["status"] == 200
                and str(visible["body"].get("iceberg_snapshot_id")) == str(detected["body"].get("snapshot_id"))
                and counts_new["keys"] == counts_new["rows"] == 2
                and counts_new["log_rows"] == 2,
            )
        )
        log_before_restart = counts_new["log_rows"]
        _stop_process(worker)
        worker, worker_events = _start_worker()
        restarted = _wait_events(worker, worker_events, 1, timeout=120)
        counts_restart = _serving_counts()
        clickhouse_stopped = True
        _clickhouse_lifecycle("stop")
        seen_outage = len(worker_events)
        retried = _wait_status(worker, worker_events, seen_outage, {"retry"}, timeout=80)
        down_api = _wait_api_down()
        _clickhouse_lifecycle("start")
        clickhouse_stopped = False
        _wait_clickhouse()
        recovered = _wait_status(
            worker,
            worker_events,
            worker_events.index(retried) + 1,
            {"unchanged", "published"},
            timeout=80,
        )
        counts_recovered = _serving_counts()
        cases.append(
            _case(
                "restart worker và ClickHouse ngắt tạm",
                "không sync tay, không thêm khóa, không thêm sync_log khi snapshot không đổi",
                {
                    "restart": _public_poll(restarted[0]),
                    "retry": _public_poll(retried),
                    "down_api": down_api,
                    "recovered": _public_poll(recovered),
                    "log_before_restart": log_before_restart,
                    "counts_restart": counts_restart,
                    "counts_recovered": counts_recovered,
                    "rss_kb": [_rss(worker.pid)],
                },
                restarted[0]["body"]["status"] == "unchanged"
                and counts_restart["log_rows"] == log_before_restart
                and counts_restart["keys"] == counts_restart["rows"] == 2
                and retried["body"]["status"] == "retry"
                and down_api["status"] == 503
                and recovered["body"]["status"] in {"unchanged", "published"}
                and counts_recovered["keys"] == counts_recovered["rows"] == 2
                and counts_recovered["log_rows"] == log_before_restart,
            )
        )
    except (SystemExit, Exception) as exc:
        detail = exc.code if isinstance(exc, SystemExit) and not isinstance(exc.code, int) else str(exc)
        cases.append(_case("suite dừng", "các case phía trên chạy hết", {"error": str(detail)[-1500:]}, False))
    finally:
        if clickhouse_stopped:
            _clickhouse_lifecycle("start")
            try:
                _wait_clickhouse()
            except SystemExit:
                pass
        if worker is not None and worker.poll() is None:
            _stop_process(worker)
        if job_id is not None:
            compose("exec", "-T", "jobmanager", "flink", "cancel", job_id, check=False)
        for process in servers:
            _stop_process(process)
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
    hops = [item["event_to_iceberg_s"] for item in samples if "event_to_iceberg_s" in item]
    report = {
        "status": "FAIL" if failed_names else "PASS",
        "failed": failed_names,
        "samples": samples,
        "freshness_note": "Không phải SLA. Mỗi mẫu là một lần đo có vòng poll, không đủ để cam kết độ trễ.",
        "event_to_iceberg_s": {"n": len(hops), "values": hops},
        "cases": cases,
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    if failed_names:
        raise SystemExit(f"FAIL {failed_names}")


def _late_after_restore(spark, samples: list[dict]) -> dict:
    """Event 5s sau restore. Cửa sổ chỉ phát lại khi watermark được dựng lại."""
    before = _snapshot_id(spark)
    sent = time.time()
    send_records(TOPIC, [(_beat(6, 15, 5_000), 0)])
    series = []
    deadline = time.time() + 45
    while True:
        observed = {"at_s": round(time.time() - sent, 3), "rows": _safe_rows(spark), "snapshot_id": _snapshot_id(spark)}
        series.append(observed)
        print("late-observe", observed, flush=True)
        if observed["snapshot_id"] != before and CORRECTED in (observed["rows"] or []):
            break
        if time.time() >= deadline:
            break
        time.sleep(min(5, max(0, deadline - time.time())))
    dlq = read_dlq(DLQ, "read_uncommitted")
    late_dlq = [item for item in dlq if int(item.get("heartbeat_id") or -1) == 6]
    committed_early = series[-1]["snapshot_id"] != before and CORRECTED in (series[-1]["rows"] or [])
    pusher_sent = False
    pusher_sample = None
    if not committed_early and not late_dlq:
        pusher_sent = True
        pushed = time.time()
        send_records(TOPIC, [(_beat(7, 16, 75_000), 0)])
        found = _wait_rows(spark, {CORRECTED})
        pusher_sample = {"event_to_iceberg_s": round(time.time() - pushed, 3), "rows": found}
        samples.append({"name": "sua_muon_sau_khi_dung_lai_watermark", **pusher_sample, "note": "Event 75s dựng watermark 59999"})
    elif committed_early:
        samples.append(
            {
                "name": "sua_muon_trong_45_giay",
                "event_to_iceberg_s": series[-1]["at_s"],
                "note": "Snapshot mới xuất hiện trong cửa sổ quan sát 45 giây",
            }
        )
    final_rows = _logical(spark)
    ok = (
        not late_dlq
        and CORRECTED in final_rows
        and _copies_ok(spark)
        and (committed_early or pusher_sent)
    )
    case = _case(
        "late event sau restore",
        "event 5s không vào DLQ; cửa sổ thành 5/50/5 khi watermark tới lại mốc đóng",
        {
            "watermark_after_80s_ms": 64_999,
            "cleanup_ms": 74_999,
            "snapshot_before": before,
            "observations_45s": series,
            "committed_within_45s": committed_early,
            "dlq": late_dlq,
            "pusher_sent": pusher_sent,
            "pusher": pusher_sample,
            "rows": final_rows,
        },
        ok,
    )
    return {"case": case, "pusher_sent": pusher_sent}


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


def _submit(savepoint: str | None = None) -> str:
    command = ["exec", "-T", "jobmanager", "flink", "run", "-d"]
    if savepoint:
        command.extend(["-s", savepoint])
    command.extend(
        [
            "-py",
            JOB_PY,
            "--engine",
            "flink",
            "--bootstrap",
            "kafka:29092",
            "--iceberg-rest",
            "http://iceberg-rest:8181",
            "--s3-endpoint",
            "http://minio:9000",
            "--source-topic",
            TOPIC,
            "--dlq-topic",
            DLQ,
            "--group-id",
            "it-e2e",
            "--sessions-table",
            HEARTBEATS,
            "--windows-table",
            NAMESPACE_TABLE,
            "--transactional-id-prefix",
            "it-e2e-dlq",
            "--checkpoint-interval-ms",
            "10000",
            "--parallelism",
            "1",
            "--idle-timeout-s",
            "30",
        ]
    )
    completed = compose(*command, check=False)
    text = completed.stdout + completed.stderr
    print(text[-1500:], flush=True)
    marker = "JobID "
    if completed.returncode != 0 or marker not in text:
        raise SystemExit(text[-1500:])
    job_id = text.split(marker, 1)[1].split()[0].strip()
    wait_running(job_id)
    return job_id


def _savepoint_uri(path: str) -> str:
    if path.startswith("file:/") and not path.startswith("file:///"):
        return "file://" + path[len("file:") :]
    return path


def _recreate(*services: str) -> dict:
    completed = compose("up", "-d", "--no-deps", "--force-recreate", *services, check=False)
    deadline = time.time() + 90
    dirs: list[str] = []
    while time.time() < deadline:
        running = _container_running("cinestream-taskmanager-1") and _container_running("cinestream-jobmanager-1")
        dirs = _checkpoint_dirs()
        if running and dirs:
            break
        time.sleep(2)
    return {"code": completed.returncode, "dirs": dirs, "tail": (completed.stdout + completed.stderr)[-400:]}


def _path_survived(path: str, dirs: list[str]) -> bool:
    marker = path.split("/opt/flink/checkpoints/")[-1].split("/")[0]
    if not marker or marker == path:
        return False
    return any(marker in item for item in dirs)


def _checkpoint_dirs() -> list[str]:
    completed = subprocess.run(
        ["docker", "exec", "cinestream-taskmanager-1", "find", "/opt/flink/checkpoints", "-maxdepth", "2", "-type", "d"],
        text=True,
        capture_output=True,
        check=False,
    )
    return sorted(line.strip() for line in completed.stdout.splitlines() if line.strip())


def _container_id(name: str) -> str:
    completed = subprocess.run(
        ["docker", "inspect", "-f", "{{.Id}}", name],
        text=True,
        capture_output=True,
        check=False,
    )
    return completed.stdout.strip()


def _container_running(name: str) -> bool:
    completed = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        text=True,
        capture_output=True,
        check=False,
    )
    return completed.stdout.strip() == "true"


def _wait_flink(timeout: int = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{FLINK}/taskmanagers", timeout=5) as response:
                body = json.loads(response.read().decode())
            if body.get("taskmanagers"):
                return
        except Exception as exc:
            print("chờ Flink", exc, flush=True)
        time.sleep(2)
    raise SystemExit("Flink chưa có TaskManager sau recreate")


def _wait_job_gone(timeout: int) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if not running_jobs():
                return
        except Exception:
            return
        time.sleep(2)
    raise SystemExit(f"job chưa tắt: {running_jobs()}")


def _start_worker():
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["PYTHONUNBUFFERED"] = "1"
    env["SPARK_LOCAL_IP"] = "127.0.0.1"
    env.pop("CINESTREAM_ALLOW_STREAMING", None)
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "serving.clickhouse.sync_windows",
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
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    events: list[dict] = []

    def _pump() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            text = line.rstrip("\n")
            if text.startswith("TRENDING_JSON="):
                body = json.loads(text.split("=", 1)[1])
                events.append({"at": time.time(), "body": body})
                print("worker", body.get("status"), body.get("snapshot_id"), body.get("elapsed_ms"), flush=True)
            elif "Error" in text or "Exception" in text:
                print("worker", text[-300:], flush=True)

    threading.Thread(target=_pump, daemon=True).start()
    return process, events


def _wait_events(process, events: list[dict], count: int, timeout: int) -> list[dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            raise SystemExit(f"worker thoát {process.returncode}")
        if len(events) >= count:
            return list(events[:count])
        time.sleep(1)
    raise SystemExit(f"worker chưa đủ {count} poll, có {len(events)}")


def _wait_new_poll(process, events: list[dict], seen: int, previous, timeout: int) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            raise SystemExit(f"worker thoát {process.returncode}")
        for item in events[seen:]:
            if item["body"].get("status") == "published" and item["body"].get("snapshot_id") != previous:
                return item
        time.sleep(1)
    raise SystemExit(f"worker không thấy snapshot mới sau {previous}")


def _wait_status(process, events: list[dict], seen: int, statuses: set[str], timeout: int) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            raise SystemExit(f"worker thoát {process.returncode}")
        for item in list(events)[seen:]:
            if item["body"].get("status") in statuses:
                return item
        time.sleep(1)
    raise SystemExit(f"worker không có status {statuses}")


def _poll_period(first: dict, second: dict) -> float | None:
    elapsed_1 = float(first["body"].get("elapsed_ms") or 0) / 1000
    elapsed_2 = float(second["body"].get("elapsed_ms") or 0) / 1000
    return round((second["at"] - first["at"]) + elapsed_1 - elapsed_2, 3)


def _public_poll(item: dict) -> dict:
    body = item["body"]
    return {
        "status": body.get("status"),
        "snapshot_id": body.get("snapshot_id"),
        "elapsed_ms": body.get("elapsed_ms"),
        "window_rows": body.get("window_rows"),
        "error": body.get("error"),
    }


def _clickhouse_lifecycle(action: str) -> None:
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "infra/compose/docker-compose.yml",
            "-f",
            "infra/compose/docker-compose.clickhouse.yml",
            action,
            "clickhouse",
        ],
        cwd=REPO,
        text=True,
        capture_output=True,
        check=False,
    )
    print(action, "clickhouse", completed.returncode, (completed.stdout + completed.stderr)[-200:], flush=True)
    if completed.returncode != 0:
        raise SystemExit((completed.stdout + completed.stderr)[-400:])


def _serving_counts() -> dict:
    keys = int(clickhouse(HTTP, f"SELECT uniqExact(movie_id, window_start) FROM {DATABASE}.movie_windows").strip())
    rows = int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.movie_windows").strip())
    logs = int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.trending_sync_log").strip())
    return {"keys": keys, "rows": rows, "log_rows": logs}


def _rss(pid: int) -> int | None:
    completed = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], text=True, capture_output=True, check=False)
    text = completed.stdout.strip()
    return int(text) if text.isdigit() else None


def _wait_rows(spark, expected: set[tuple], timeout: int = 120) -> list[tuple]:
    found, _at = _wait_rows_until(spark, expected, timeout)
    return found


def _wait_rows_at(spark, expected: set[tuple], timeout: int = 120) -> float:
    _found, at = _wait_rows_until(spark, expected, timeout)
    return at


def _wait_rows_until(spark, expected: set[tuple], timeout: int) -> tuple[list[tuple], float]:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            found = _logical(spark)
            last = found
            print("windows", found, flush=True)
            if set(found) == expected and _copies_ok(spark):
                return found, time.time()
        except Exception as exc:
            print("wait", str(exc).splitlines()[-1][:180], flush=True)
        time.sleep(3)
    raise SystemExit(f"cửa sổ chưa khớp {expected}: {last}")


def _safe_rows(spark):
    try:
        return _logical(spark)
    except Exception as exc:
        return [str(exc).splitlines()[-1][:160]]


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


def _snapshot_id(spark):
    try:
        spark.sql(f"REFRESH TABLE {NAMESPACE_TABLE}")
        rows = spark.sql(
            f"SELECT snapshot_id FROM {NAMESPACE_TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()
    except Exception as exc:
        return f"error:{str(exc).splitlines()[-1][:120]}"
    if not rows:
        return None
    return int(rows[0].snapshot_id)


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


def _beat(heartbeat_id: int, user_id: int, event_ms: int) -> dict:
    payload = event(heartbeat_id, user_id, event_ms)
    payload["movie_id"] = 42
    return payload


def _release_test_jobs() -> None:
    blockers = []
    for job in running_jobs():
        name = job.get("name") or ""
        if any(token in name for token in ("it_p1", "it_p2", "it-ops", "it-trending", "it-e2e")):
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


def _start_api():
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO)
    env["CINESTREAM_CLICKHOUSE_DATABASE"] = DATABASE
    env["CINESTREAM_CLICKHOUSE_HTTP"] = HTTP
    env["CINESTREAM_TRENDING_STALE_SECONDS"] = "180"
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "serving.api.app:app", "--host", "127.0.0.1", "--port", "18090"],
        cwd=str(REPO),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            _get("/health")
            return process
        except Exception:
            if process.poll() is not None:
                output = process.stdout.read().decode() if process.stdout else ""
                raise SystemExit(f"API không lên: {output[-800:]}")
            time.sleep(0.3)
    raise SystemExit("API không lên tại 18090")


def _wait_api_snapshot(previous, timeout: int = 30) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = _get("/movies/trending?window_minutes=15&limit=10")
        body = last.get("body") or {}
        if last["status"] == 200 and str(body.get("iceberg_snapshot_id")) not in {str(previous), "None"}:
            return last
        time.sleep(0.5)
    raise SystemExit(f"API chưa thấy snapshot mới sau {previous}: {last}")


def _wait_api_down() -> dict:
    last = {"status": 0, "body": None}
    deadline = time.time() + 20
    while time.time() < deadline:
        last = _get("/movies/trending?window_minutes=15&limit=10")
        if last["status"] == 503:
            return last
        time.sleep(1)
    return last


def _get(path: str) -> dict:
    request = urllib.request.Request(API + path)
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            raw = response.read().decode()
            return {"status": response.status, "body": json.loads(raw) if raw else None}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = raw
        return {"status": exc.code, "body": body}
    except urllib.error.URLError as exc:
        return {"status": 0, "body": str(exc.reason)}


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


def _stop_process(process) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def _case(name: str, expected: str, actual: dict, ok: bool) -> dict:
    status = "PASS" if ok else "FAIL"
    print(f"CASE {status} {name}", flush=True)
    return {"name": name, "status": status, "expected": expected, "actual": actual}


if __name__ == "__main__":
    main()
