"""Job Flink cô lập ``it_p2_b3`` rồi sync cửa sổ sang Feast.

Không submit topic ``movie.playback.heartbeat`` và không ghi
``stg_playback_windows``. Redis streaming là database 2. Database 1 của batch
chỉ được đọc qua một sentinel.
"""

from __future__ import annotations

import json
import shutil
import time
import traceback
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import redis

from ml.feast.streaming_features import historical_windows
from ml.online.streaming_sync import (
    STREAM_REDIS,
    StaleSnapshot,
    SyncFailed,
    read_iceberg_windows,
    read_online,
    sync_logical,
)
from streaming.flink.it_phase2 import (
    checkpoint_view,
    compose,
    drop_tables,
    end_offsets,
    event,
    fail_taskmanager,
    prepare_cluster,
    production_snapshots,
    recreate_topics,
    send_records,
    start_spark,
    submit,
    wait_restored,
    wait_running,
    window_rows,
)

REPO = Path(__file__).resolve().parents[2]
HEARTBEATS = "cinestream.silver.it_p2_b3_heartbeats"
WINDOWS = "cinestream.silver.it_p2_b3_windows"
TOPIC = "it.p2.b3.heartbeat"
DLQ = "it.p2.b3.deadletter"
LOG = REPO / "docs" / "proof" / "logs" / "phase_b3_streaming_features.json"
PRODUCTION_TOPIC = "movie.playback.heartbeat"


def main() -> None:
    report: dict = {"redis": STREAM_REDIS, "table": WINDOWS, "topic": TOPIC}
    spark = None
    job_id = None
    redis.Redis.from_url("redis://localhost:16379/2").flushdb()
    batch = redis.Redis.from_url("redis://localhost:16379/1")
    batch.set("b3-batch-sentinel", "keep")
    state = REPO / "ml" / "feast" / "data" / "streaming-b3-state"
    repo = REPO / "ml" / "feast" / "data" / "streaming-b3-repo"
    shutil.rmtree(state, ignore_errors=True)
    shutil.rmtree(repo, ignore_errors=True)
    try:
        report["production_offsets_before"] = end_offsets(PRODUCTION_TOPIC)
        prepare_cluster()
        spark = start_spark("streaming-feast-b3")
        report["production_snapshots_before"] = production_snapshots(spark)
        recreate_topics([(TOPIC, 1), (DLQ, 1)])
        drop_tables(spark, [HEARTBEATS, WINDOWS])
        job_id = _submit_isolated()
        report["job_id"] = job_id
        report["vertices"] = _vertices(job_id)
        opened_wait = _wait_window(spark, "1970-01-01 00:00:00", 4, 40, 4, _send_initial)
        report["flink_initial_ms"] = opened_wait["wait_ms"]
        opened = read_iceberg_windows(spark, WINDOWS)
        report["initial_snapshot"] = _public_snapshot(opened)
        initial = _sync(opened, repo, state)
        report["initial_sync"] = _brief(initial)
        _expect_online(repo, 42, 4, 40, 4)
        late_wait = _wait_window(
            spark,
            "1970-01-01 00:00:00",
            5,
            50,
            5,
            lambda: send_records(TOPIC, [(event(6, 15, 5_000, 10), 0)]),
        )
        report["flink_late_ms"] = late_wait["wait_ms"]
        corrected = read_iceberg_windows(spark, WINDOWS)
        report["late_snapshot"] = _public_snapshot(corrected)
        report["late_evidence"] = corrected.get("evidence")
        if _equality_added(corrected.get("evidence") or {}) <= 0:
            raise SystemExit(f"late firing chưa có equality-delete: {corrected.get('evidence')}")
        evidence = corrected.get("evidence") or {}
        if evidence.get("logical_rows") != 1 or int(evidence.get("data_records") or 0) <= 1:
            raise SystemExit(f"upsert chưa tách dòng logic khỏi data file: {evidence}")
        epoch = [
            row
            for row in corrected["rows"]
            if row["window_end"].isoformat().startswith("1970-01-01T00:01:00")
        ]
        if len(epoch) != 1 or (epoch[0]["heartbeat_count"], epoch[0]["total_watch_seconds"], epoch[0]["active_users"]) != (5, 50, 5):
            raise SystemExit(f"snapshot logic sau late firing: {corrected['rows']}")
        late = _sync(corrected, repo, state)
        report["late_sync"] = _brief(late)
        _expect_online(repo, 42, 5, 50, 5)
        _expect_history(repo, "1970-01-01 00:01:00.001+00:00", 4, 40, 4)
        _expect_history(repo, "1970-01-01 00:01:00.002+00:00", 5, 50, 5)
        second_wait = _wait_window(
            spark,
            "1970-01-01 00:01:00",
            1,
            10,
            1,
            lambda: send_records(TOPIC, [(event(9, 21, 140_000, 10), 0)]),
        )
        report["flink_second_window_ms"] = second_wait["wait_ms"]
        second = read_iceberg_windows(spark, WINDOWS)
        report["second_snapshot"] = _public_snapshot(second)
        failed = False
        try:
            _sync(second, repo, state, after_write=lambda: (_ for _ in ()).throw(RuntimeError("worker dừng")))
        except SyncFailed:
            failed = True
        if not failed:
            raise SystemExit("sync lỗi giả không thất bại")
        committed = json.loads((state / "committed.json").read_text())
        if committed["snapshot_id"] != corrected["snapshot_id"]:
            raise SystemExit("lỗi sync đã tiến committed")
        retried = _sync(second, repo, state)
        report["second_sync"] = _brief(retried)
        _expect_online(repo, 42, 1, 10, 1)
        _expect_online(repo, 99, None, None, None)
        unchanged = _sync(second, repo, state)
        if unchanged["result"] != "unchanged":
            raise SystemExit(unchanged)
        report["rerun"] = unchanged["result"]
        redis.Redis.from_url("redis://localhost:16379/2").flushdb()
        reloaded = _sync(second, repo, state)
        report["reloaded"] = reloaded["result"]
        _expect_online(repo, 42, 1, 10, 1)
        try:
            sync_logical(
                opened["rows"],
                snapshot_id=opened["snapshot_id"],
                committed_at=opened["committed_at"],
                repo_dir=repo,
                state_dir=state,
            )
        except StaleSnapshot as exc:
            report["stale_refused"] = str(exc)
        else:
            raise SystemExit("snapshot cũ vẫn được ghi")
        _expect_online(repo, 42, 1, 10, 1)
        missing = read_iceberg_windows(spark, "cinestream.silver.it_p2_b3_missing")
        if not missing.get("absent"):
            raise SystemExit(missing)
        try:
            sync_logical(None, snapshot_id="missing", committed_at=datetime.now(timezone.utc), repo_dir=repo, state_dir=state)
        except SyncFailed as exc:
            report["missing_source"] = str(exc)
        holding = checkpoint_view(job_id)
        restored_before = int((holding.get("counts") or {}).get("restored") or 0)
        fail_taskmanager()
        report["restored"] = wait_restored(job_id, restored_before)
        recovered = read_iceberg_windows(spark, WINDOWS)
        if _signature(recovered) != _signature(second):
            raise SystemExit(f"logic đổi sau restart: {recovered.get('rows')}")
        recovered_sync = _sync(recovered, repo, state)
        report["recovered_sync"] = recovered_sync["result"]
        _expect_online(repo, 42, 1, 10, 1)
        _expect_history(repo, "1970-01-01 00:02:00.003+00:00", 1, 10, 1)
        if batch.get("b3-batch-sentinel") != b"keep":
            raise SystemExit("sentinel Redis db=1 đổi")
        report["status"] = "PASS"
    except BaseException as exc:
        report["status"] = "FAIL"
        report["error"] = exc.code if isinstance(exc, SystemExit) and isinstance(exc.code, str) else f"{exc.__class__.__name__}: {exc}"
        if not isinstance(exc, SystemExit):
            report["traceback"] = traceback.format_exc()
    finally:
        if spark is not None:
            try:
                report["production_snapshots_after"] = production_snapshots(spark)
            except Exception as exc:
                report["production_snapshots_after_error"] = str(exc)[:300]
            spark.stop()
        try:
            report["production_offsets_after"] = end_offsets(PRODUCTION_TOPIC)
        except Exception as exc:
            report["production_offsets_after_error"] = str(exc)[:300]
        if job_id:
            compose("exec", "-T", "jobmanager", "flink", "cancel", job_id, check=False)
        LOG.parent.mkdir(parents=True, exist_ok=True)
        report["production_offsets_unchanged"] = report.get("production_offsets_before") == report.get("production_offsets_after")
        if report.get("status") == "PASS" and not report["production_offsets_unchanged"]:
            report["status"] = "FAIL"
            report["error"] = "offset Kafka production đổi"
        LOG.write_text(json.dumps(report, indent=2, default=str) + "\n")
        print(json.dumps({key: report.get(key) for key in ("status", "job_id", "initial_sync", "late_sync", "second_sync", "rerun", "reloaded", "recovered_sync", "stale_refused", "error")}, default=str))
        if report.get("status") != "PASS":
            raise SystemExit(report.get("error") or "FAIL")


def _submit_isolated() -> str:
    """Image Flink đang chạy chưa có ``--job-name``. Tên job đi qua cấu hình client."""
    job_id = submit(
        [
            "--source-topic",
            TOPIC,
            "--dlq-topic",
            DLQ,
            "--group-id",
            "it-p2-b3",
            "--sessions-table",
            HEARTBEATS,
            "--windows-table",
            WINDOWS,
            "--transactional-id-prefix",
            "it-p2-b3-dlq",
            "--checkpoint-interval-ms",
            "10000",
            "--parallelism",
            "1",
        ],
        flink_args=["-Dpipeline.name=it_p2_b3_windows"],
    )
    wait_running(job_id)
    return job_id


def _send_initial() -> None:
    first = event(1, 10, 0, 10)
    send_records(
        TOPIC,
        [
            (first, 0),
            (event(2, 11, 20_000, 500), 0),
            (dict(first), 0),
            (event(3, 12, 50_000, 10), 0),
            (event(4, 13, 10_000, 10), 0),
            (event(5, 14, 80_000, 10), 0),
        ],
        gap_s=1,
    )


def _wait_window(spark, start_prefix: str, beats: int, watched: int, users: int, send) -> dict:
    started = time.perf_counter()
    send()
    deadline = time.time() + 180
    last = None
    while time.time() < deadline:
        try:
            last = window_rows(spark, WINDOWS)
        except Exception as exc:
            print("windows", str(exc).splitlines()[-1][:180], flush=True)
            time.sleep(3)
            continue
        print("windows", last, flush=True)
        for row in last:
            if str(row[0]).startswith(start_prefix) and (row[1], row[2], row[3], row[4]) == (beats, watched, users, 1):
                return {"wait_ms": round((time.perf_counter() - started) * 1000, 3), "row": row}
        time.sleep(3)
    raise SystemExit(f"chưa thấy {start_prefix} {(beats, watched, users)}: {last}")


def _sync(snapshot: dict, repo: Path, state: Path, after_write=None) -> dict:
    return sync_logical(
        snapshot["rows"],
        snapshot_id=snapshot["snapshot_id"],
        committed_at=snapshot["committed_at"],
        repo_dir=repo,
        state_dir=state,
        after_write=after_write,
    )


def _expect_online(repo: Path, movie_id: int, beats, watched, users) -> None:
    from feast import FeatureStore

    found = read_online(FeatureStore(repo_path=str(repo)), [movie_id])[0]
    if (found.get("heartbeat_count"), found.get("total_watch_seconds"), found.get("active_users")) != (beats, watched, users):
        raise SystemExit(f"online movie {movie_id} = {found}")


def _expect_history(repo: Path, event_time: str, beats: int, watched: int, users: int) -> None:
    from feast import FeatureStore

    frame = historical_windows(
        FeatureStore(repo_path=str(repo)),
        pd.DataFrame({"movie_id": [42], "event_timestamp": [pd.Timestamp(event_time)]}),
    )
    row = frame.iloc[0]
    got = (int(row["heartbeat_count"]), int(row["total_watch_seconds"]), int(row["active_users"]))
    if got != (beats, watched, users):
        raise SystemExit(f"offline tại {event_time} = {got}")


def _signature(snapshot: dict) -> list[tuple]:
    return sorted(
        (
            int(row["movie_id"]),
            row["window_end"].isoformat(),
            int(row["heartbeat_count"]),
            int(row["total_watch_seconds"]),
            int(row["active_users"]),
        )
        for row in snapshot.get("rows") or []
    )


def _equality_added(evidence: dict) -> int:
    added = sum(int(item.get("added_equality_deletes") or 0) for item in evidence.get("snapshots") or [])
    if added:
        return added
    return sum(
        int(item["records"])
        for item in evidence.get("delete_files") or []
        if int(item["content"]) in (1, 2)
    )


def _public_snapshot(snapshot: dict) -> dict:
    return {
        "snapshot_id": snapshot.get("snapshot_id"),
        "parent_id": snapshot.get("parent_id"),
        "committed_at": snapshot.get("committed_at"),
        "operation": snapshot.get("operation"),
        "rows": [
            {
                "movie_id": row["movie_id"],
                "window_end": row["window_end"].isoformat(),
                "heartbeat_count": row["heartbeat_count"],
                "total_watch_seconds": row["total_watch_seconds"],
                "active_users": row["active_users"],
            }
            for row in snapshot.get("rows") or []
        ],
    }


def _brief(result: dict) -> dict:
    return {key: result.get(key) for key in ("result", "snapshot_id", "sequence", "elapsed_ms", "window_rows")}


def _vertices(job_id: str) -> list[str]:
    with urllib.request.urlopen(f"http://127.0.0.1:18081/jobs/{job_id}") as response:
        job = json.load(response)
    return [vertex["name"] for vertex in job["vertices"]]


if __name__ == "__main__":
    main()
