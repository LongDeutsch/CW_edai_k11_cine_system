"""Validate recovery-late-gate against the tumbling window.

Isolated topics start with ``it.p2.val``. Isolated tables start with
``cinestream.silver.it_p2_val``. Production topics and tables are only read.
"""

from __future__ import annotations

import json
import time

from streaming.flink.it_phase2 import (
    checkpoint_view,
    compact_metrics,
    compose,
    dlq_summary,
    drop_tables,
    duplicate_window_keys,
    end_offsets,
    event,
    fail_taskmanager,
    prepare_cluster,
    production_snapshots,
    recreate_topics,
    running_jobs,
    sample_metrics,
    send_records,
    start_spark,
    submit_pipeline,
    wait_heartbeats,
    wait_restored,
    window_rows,
)

CLEANUP_EPOCH_MS = 74_999


def watermarks(job_id: str) -> list[dict]:
    rows = []
    for sample in compact_metrics(sample_metrics(job_id)):
        values = []
        for metric in sample["metrics"]:
            ident = str(metric.get("id", ""))
            lowered = ident.lower()
            if "watermark" not in lowered or "mailbox" in lowered:
                continue
            values.append({"id": ident, "value": metric.get("value")})
        if values:
            rows.append({"name": sample["name"], "subtask": sample["subtask"], "watermarks": values})
    return rows


def _numbers(snapshot: list[dict], name_part: str, metric_part: str) -> list[int]:
    found = []
    for row in snapshot:
        if name_part not in str(row.get("name", "")):
            continue
        for metric in row.get("watermarks", []):
            if metric_part not in str(metric.get("id", "")):
                continue
            try:
                found.append(int(metric["value"]))
            except (TypeError, ValueError):
                continue
    return found


def window_watermark(snapshot: list[dict]) -> int | None:
    values = _numbers(snapshot, "tumbling-1m", "currentInputWatermark")
    if not values:
        values = _numbers(snapshot, "bounded-out-of-orderness", "currentOutputWatermark")
    return min(values) if values else None


def source_watermarks(snapshot: list[dict]) -> dict[int, int]:
    found = {}
    for row in snapshot:
        if "bounded-out-of-orderness" not in str(row.get("name", "")):
            continue
        values = []
        for metric in row.get("watermarks", []):
            if "currentOutputWatermark" not in str(metric.get("id", "")):
                continue
            try:
                values.append(int(metric["value"]))
            except (TypeError, ValueError):
                continue
        if values:
            found[int(row["subtask"])] = min(values)
    return found


def epoch_window(rows: list[tuple]) -> tuple | None:
    matches = [row for row in rows if str(row[0]).startswith("1970-01-01 00:00:00")]
    if len(matches) != 1:
        return None
    return matches[0]


def wait_epoch(spark, table: str, count: int, watched: int, users: int, timeout: int = 120) -> tuple:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = window_rows(spark, table)
        except Exception as exc:
            print("windows", str(exc).splitlines()[-1][:180], flush=True)
            time.sleep(3)
            continue
        row = epoch_window(last)
        print("epoch", row, flush=True)
        if row is not None and (row[1], row[2], row[3], row[4]) == (count, watched, users, 1):
            return row
        time.sleep(3)
    raise SystemExit(f"cửa sổ epoch chưa là {(count, watched, users)}: {last}")


def cancel_it_jobs() -> None:
    for job in running_jobs():
        name = job.get("name", "")
        if "it_p2" in name or "it_p1" in name:
            print("cancel", job["jid"], name, flush=True)
            compose("exec", "-T", "jobmanager", "flink", "cancel", job["jid"], check=False)
    deadline = time.time() + 60
    while time.time() < deadline:
        left = [
            job
            for job in running_jobs()
            if "it_p2" in job.get("name", "") or "it_p1" in job.get("name", "")
        ]
        if not left:
            return
        time.sleep(2)
    raise SystemExit(f"job integration chưa tắt: {running_jobs()}")


def assert_graph(job_id: str, parallelism: int) -> list[str]:
    import urllib.request

    with urllib.request.urlopen(f"http://127.0.0.1:18081/jobs/{job_id}") as response:
        job = json.load(response)
    names = [vertex["name"] for vertex in job["vertices"]]
    print("vertices", names, flush=True)
    if not any("tumbling-1m" in name for name in names):
        raise SystemExit(f"graph không có tumbling-1m: {names}")
    if any("sliding" in name.lower() for name in names):
        raise SystemExit(f"graph còn sliding: {names}")
    if not any("recovery-late-gate" in name for name in names):
        raise SystemExit(f"graph không có recovery-late-gate: {names}")
    widths = {vertex["name"]: vertex.get("parallelism") for vertex in job["vertices"]}
    gate = next(value for name, value in widths.items() if "recovery-late-gate" in name)
    window = next(value for name, value in widths.items() if "tumbling-1m" in name)
    if gate != parallelism or window != parallelism:
        raise SystemExit(f"parallelism {widths}")
    return names


def inflight_then_kill(job_id: str, topic: str, payload: dict, partition: int, retries: int = 1, settle_s: float = 4) -> dict:
    """Send one event and kill before its checkpoint. One retry uses a new id."""
    attempts = []
    current = payload
    dlq = topic.replace(".heartbeat", ".deadletter")
    for attempt_index in range(retries + 1):
        holding = checkpoint_view(job_id)
        held_id = (holding.get("completed") or {}).get("id")
        send_records(topic, [(current, partition)], gap_s=0)
        time.sleep(settle_s)
        during = checkpoint_view(job_id)
        during_id = (during.get("completed") or {}).get("id")
        summary = dlq_summary(dlq)
        attempt = {
            "heartbeat_id": current["heartbeat_id"],
            "held_checkpoint": held_id,
            "during_checkpoint": during_id,
            "dlq_before_kill": summary,
            "checkpointed_before_kill": during_id != held_id,
        }
        attempts.append(attempt)
        if during_id == held_id and held_id is not None:
            restored_before = int(holding["counts"]["restored"])
            fail_taskmanager()
            restored = wait_restored(job_id, restored_before)
            return {
                "killed": True,
                "attempts": attempts,
                "held_checkpoint": held_id,
                "restored_before": restored_before,
                "restored": restored,
                "heartbeat_id": current["heartbeat_id"],
            }
        if attempt_index == retries:
            break
        current = event(int(payload["heartbeat_id"]) + 50, int(payload["user_id"]) + 50, _event_ms(payload))
    return {"killed": False, "attempts": attempts, "heartbeat_id": payload["heartbeat_id"]}


def _event_ms(payload: dict) -> int:
    raw = payload["event_ts"]
    if isinstance(raw, (int, float)):
        value = int(raw)
        return value if value > 10_000_000_000 else value * 1000
    text = str(raw).replace("Z", "+00:00")
    from datetime import datetime

    parsed = datetime.fromisoformat(text)
    return int(parsed.timestamp() * 1000)


def wait_committed_dead(job_id: str, topic: str, heartbeat_id: int, previous_checkpoint: int, timeout: int = 150) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        view = checkpoint_view(job_id)
        completed = (view.get("completed") or {}).get("id")
        summary = dlq_summary(topic)
        print("after restore", completed, summary["read_committed"], flush=True)
        last = {"checkpoint": view, "dlq": summary}
        dead = summary["read_committed"]["dead"]
        if completed is not None and completed != previous_checkpoint and heartbeat_id in dead:
            copies = dead.count(heartbeat_id)
            last["copies"] = copies
            return last
        time.sleep(3)
    raise SystemExit(f"read_committed chưa thấy {heartbeat_id}: {last}")


def case_parallelism_1(spark) -> dict:
    topic = "it.p2.valp1.heartbeat"
    dlq = "it.p2.valp1.deadletter"
    heartbeats = "cinestream.silver.it_p2_valp1_heartbeats"
    windows = "cinestream.silver.it_p2_valp1_windows"
    recreate_topics([(topic, 3), (dlq, 1)])
    drop_tables(spark, [heartbeats, windows])
    job_id = submit_pipeline(
        topic,
        dlq,
        "it-valp1",
        heartbeats,
        windows,
        "it-valp1-dlq",
        ["--checkpoint-interval-ms", "30000", "--parallelism", "1", "--idle-timeout-s", "30"],
    )
    vertices = assert_graph(job_id, 1)
    first = event(1, 10, 0, 10)
    send_records(
        topic,
        [
            (first, 0),
            (event(2, 11, 20_000, 500), 1),
            (dict(first), 0),
            (event(3, 12, 50_000, 10), 2),
            (event(4, 13, 10_000, 10), 0),
            (event(5, 14, 80_000, 10), 1),
        ],
        gap_s=2,
    )
    opened = wait_epoch(spark, windows, 4, 40, 4)
    seen = wait_heartbeats(spark, heartbeats, {1, 2, 3, 4, 5})
    if seen[2][1] != 500 or seen[2][2] != 10:
        raise SystemExit(f"seek đổi watch duration: {seen[2]}")
    send_records(topic, [(event(6, 15, 5_000, 10), 0)], gap_s=0)
    updated = wait_epoch(spark, windows, 5, 50, 5)
    if opened[0] != updated[0]:
        raise SystemExit(f"late firing tạo cửa sổ khác: {opened[0]} -> {updated[0]}")
    send_records(topic, [(event(7, 16, 330_000, 10), 2)], gap_s=0)
    wait_heartbeats(spark, heartbeats, {1, 2, 3, 4, 5, 6, 7}, timeout=180)
    stayed = wait_epoch(spark, windows, 5, 50, 5)
    send_records(topic, [(event(9, 18, 320_000, 10), 1)], gap_s=0)
    wait_heartbeats(spark, heartbeats, {1, 2, 3, 4, 5, 6, 7, 9}, timeout=120)
    after_valid = dlq_summary(dlq)
    if 9 in after_valid["read_committed"]["dead"] or 9 in after_valid["read_uncommitted"]["dead"]:
        raise SystemExit(f"event hợp lệ 320s bị DLQ: {after_valid}")
    kill = inflight_then_kill(job_id, topic, event(8, 17, 0, 10), 0, retries=0)
    if not kill["killed"]:
        raise SystemExit(f"không kịp kill trước checkpoint: {kill}")
    committed = wait_committed_dead(job_id, dlq, kill["heartbeat_id"], kill["held_checkpoint"])
    dead = committed["dlq"]["read_committed"]["dead"]
    if dead.count(kill["heartbeat_id"]) != 1:
        raise SystemExit(f"DLQ committed không đúng một lần: {committed['dlq']}")
    if set(dead) - {kill["heartbeat_id"]}:
        raise SystemExit(f"DLQ committed có heartbeat khác: {dead}")
    final_ids = {1, 2, 3, 4, 5, 6, 7, 9, kill["heartbeat_id"]}
    final_heartbeats = wait_heartbeats(spark, heartbeats, final_ids, timeout=120)
    final_window = wait_epoch(spark, windows, 5, 50, 5)
    if final_window != stayed:
        raise SystemExit(f"cửa sổ đổi sau recovery {stayed} -> {final_window}")
    send_records(topic, [(event(10, 19, 340_000, 10), 2)], gap_s=0)
    wait_heartbeats(spark, heartbeats, final_ids | {10}, timeout=120)
    after_recovery_valid = dlq_summary(dlq)
    if 10 in after_recovery_valid["read_committed"]["dead"] or 10 in after_recovery_valid["read_uncommitted"]["dead"]:
        raise SystemExit(f"event hợp lệ sau recovery bị DLQ: {after_recovery_valid}")
    duplicates = duplicate_window_keys(spark, windows)
    if duplicates:
        raise SystemExit(f"trùng khóa cửa sổ: {duplicates}")
    return {
        "status": "passed",
        "job_id": job_id,
        "vertices": vertices,
        "partitions": {"fast": 0, "slow_valid_320s": 1, "idle_after_330s": 2},
        "window": final_window,
        "duplicate_heartbeat_copies": final_heartbeats[1][0],
        "seek_position": final_heartbeats[2][1],
        "watch_seconds": final_heartbeats[2][2],
        "dlq_read_committed": after_recovery_valid["read_committed"],
        "dlq_read_uncommitted": after_recovery_valid["read_uncommitted"],
        "valid_before_kill_not_rejected": 9,
        "valid_after_restore_not_rejected": 10,
        "too_late_committed_once": kill["heartbeat_id"],
    }


def case_parallelism_2_held(spark) -> dict:
    """Fast, slow, and idle partitions while the min watermark is still held."""
    topic = "it.p2.valhold.heartbeat"
    dlq = "it.p2.valhold.deadletter"
    heartbeats = "cinestream.silver.it_p2_valhold_heartbeats"
    windows = "cinestream.silver.it_p2_valhold_windows"
    recreate_topics([(topic, 3), (dlq, 1)])
    drop_tables(spark, [heartbeats, windows])
    job_id = submit_pipeline(
        topic,
        dlq,
        "it-valhold",
        heartbeats,
        windows,
        "it-valhold-dlq",
        ["--checkpoint-interval-ms", "20000", "--parallelism", "2", "--idle-timeout-s", "300"],
    )
    assert_graph(job_id, 2)
    send_records(
        topic,
        [
            (event(1, 10, 330_000, 10), 0),
            (event(2, 11, 0, 10), 1),
        ],
        gap_s=0,
    )
    wait_heartbeats(spark, heartbeats, {1, 2}, timeout=180)
    before = watermarks(job_id)
    live = window_watermark(before)
    sources = source_watermarks(before)
    window_would_accept = live is None or live < CLEANUP_EPOCH_MS
    kill = inflight_then_kill(job_id, topic, event(3, 12, 5_000, 10), 0)
    outcome = {
        "job_id": job_id,
        "watermarks_before_inflight": before,
        "source_output_watermarks": sources,
        "window_input_watermark": live,
        "checkpointed_max_event_ts_ms": 330_000,
        "derived_watermark_ms": 330_000 - 15_000 - 1,
        "window_would_accept_ts_5000": window_would_accept,
        "max_equivalent_to_window_watermark": live is not None and live >= 314_999,
        "kill": {key: value for key, value in kill.items() if key != "restored"},
    }
    if not kill["killed"]:
        outcome["status"] = "not_killed"
        outcome["dlq"] = dlq_summary(dlq)
        return outcome
    deadline = time.time() + 150
    last = None
    while time.time() < deadline:
        view = checkpoint_view(job_id)
        completed = (view.get("completed") or {}).get("id")
        summary = dlq_summary(dlq)
        print("hold after", completed, summary["read_committed"], flush=True)
        last = summary
        if completed is not None and completed != kill["held_checkpoint"]:
            break
        time.sleep(3)
    else:
        outcome["status"] = "no_checkpoint_after_restore"
        outcome["dlq"] = last
        return outcome
    expected_ids = {1, 2, *(attempt["heartbeat_id"] for attempt in kill["attempts"])}
    final_heartbeats = wait_heartbeats(spark, heartbeats, expected_ids, timeout=120)
    try:
        rows = window_rows(spark, windows)
    except Exception as exc:
        rows = str(exc).splitlines()[-1][:300]
    dead = last["read_committed"]["dead"] if last else []
    rejected = kill["heartbeat_id"] in dead
    outcome.update(
        {
            "status": "observed",
            "dlq_read_committed": last["read_committed"] if last else None,
            "dlq_read_uncommitted": last["read_uncommitted"] if last else None,
            "rejected_by_gate": rejected,
            "false_rejection": bool(window_would_accept and rejected),
            "heartbeat_copies": {str(key): value[0] for key, value in final_heartbeats.items()},
            "windows": rows,
        }
    )
    return outcome


def case_parallelism_2_caught_up(spark) -> dict:
    """After idleness releases the idle subtask, a too-late replay should be one DLQ."""
    topic = "it.p2.validle.heartbeat"
    dlq = "it.p2.validle.deadletter"
    heartbeats = "cinestream.silver.it_p2_validle_heartbeats"
    windows = "cinestream.silver.it_p2_validle_windows"
    recreate_topics([(topic, 3), (dlq, 1)])
    drop_tables(spark, [heartbeats, windows])
    job_id = submit_pipeline(
        topic,
        dlq,
        "it-validle",
        heartbeats,
        windows,
        "it-validle-dlq",
        ["--checkpoint-interval-ms", "20000", "--parallelism", "2", "--idle-timeout-s", "15"],
    )
    assert_graph(job_id, 2)
    send_records(
        topic,
        [
            (event(1, 10, 0, 10), 0),
            (event(2, 11, 330_000, 10), 0),
        ],
        gap_s=0,
    )
    wait_heartbeats(spark, heartbeats, {1, 2}, timeout=180)
    deadline = time.time() + 80
    live = None
    snapshot = []
    fired = None
    while time.time() < deadline:
        snapshot = watermarks(job_id)
        live = window_watermark(snapshot)
        try:
            fired = epoch_window(window_rows(spark, windows))
        except Exception as exc:
            fired = str(exc).splitlines()[-1][:180]
        print("idle release", live, fired, flush=True)
        if live is not None and live >= CLEANUP_EPOCH_MS and fired is not None and not isinstance(fired, str):
            break
        time.sleep(4)
    outcome = {
        "job_id": job_id,
        "watermarks_after_idle": snapshot,
        "source_output_watermarks": source_watermarks(snapshot),
        "window_input_watermark": live,
        "epoch_before_late": fired,
    }
    if live is None or live < CLEANUP_EPOCH_MS:
        outcome["status"] = "watermark_never_caught_up"
        return outcome
    kill = inflight_then_kill(job_id, topic, event(3, 12, 0, 10), 0)
    outcome["kill"] = {key: value for key, value in kill.items() if key != "restored"}
    if not kill["killed"]:
        outcome["status"] = "not_killed"
        outcome["dlq"] = dlq_summary(dlq)
        return outcome
    committed = wait_committed_dead(job_id, dlq, kill["heartbeat_id"], kill["held_checkpoint"])
    expected_ids = {1, 2, *(attempt["heartbeat_id"] for attempt in kill["attempts"])}
    final_heartbeats = wait_heartbeats(spark, heartbeats, expected_ids, timeout=120)
    stayed = epoch_window(window_rows(spark, windows))
    dead = committed["dlq"]["read_committed"]["dead"]
    outcome.update(
        {
            "status": "observed",
            "dlq_read_committed": committed["dlq"]["read_committed"],
            "dlq_read_uncommitted": committed["dlq"]["read_uncommitted"],
            "copies": dead.count(kill["heartbeat_id"]),
            "agrees_with_window": dead.count(kill["heartbeat_id"]) == 1 and stayed == fired,
            "epoch_after": stayed,
            "heartbeat_copies": {str(key): value[0] for key, value in final_heartbeats.items()},
        }
    )
    return outcome


def main() -> None:
    result: dict = {"offsets_before": end_offsets()}
    spark = None
    try:
        prepare_cluster()
        spark = start_spark("validate-gate")
        result["snapshots_before"] = production_snapshots(spark)
        result["parallelism_1"] = case_parallelism_1(spark)
        cancel_it_jobs()
        result["parallelism_2_held"] = case_parallelism_2_held(spark)
        cancel_it_jobs()
        result["parallelism_2_caught_up"] = case_parallelism_2_caught_up(spark)
        result["status"] = "completed"
    except SystemExit as exc:
        result["status"] = "failed"
        result["error"] = exc.code if isinstance(exc.code, str) else str(exc.code)
        raise
    finally:
        if spark is not None:
            try:
                result["snapshots_after"] = production_snapshots(spark)
            except Exception as exc:
                result["snapshots_after_error"] = str(exc)[:300]
            spark.stop()
        try:
            result["offsets_after"] = end_offsets()
        except Exception as exc:
            result["offsets_after_error"] = str(exc)[:300]
        path = "/tmp/validate_gate_result.json"
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2, default=str)
        print("wrote", path, result.get("status"), flush=True)
        for job in running_jobs():
            if "it_p2" in job.get("name", "") or "it_p1" in job.get("name", ""):
                compose("exec", "-T", "jobmanager", "flink", "cancel", job["jid"], check=False)


if __name__ == "__main__":
    main()
