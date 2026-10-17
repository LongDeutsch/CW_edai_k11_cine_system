"""Recovery correctness at parallelism 1 and 2, then a small throughput baseline.

Topics start with ``it.p2.rec``. Tables start with ``cinestream.silver.it_p2_rec``.
Production topics and tables are only read.
"""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request

from streaming.flink.it_phase2 import (
    FLINK,
    _get,
    checkpoint_view,
    dlq_summary,
    drop_tables,
    duplicate_window_keys,
    end_offsets,
    event,
    latest_snapshot,
    prepare_cluster,
    production_snapshots,
    recreate_topics,
    send_records,
    start_spark,
    submit_pipeline,
    wait_heartbeats,
    window_rows,
)
from streaming.flink.it_validate_gate import (
    assert_graph,
    cancel_it_jobs,
    inflight_then_kill,
    wait_committed_dead,
    wait_epoch,
)

CLEANUP_EPOCH_MS = 74_999


def _metrics(job_id: str, vertex_id: str, subtask: int, wanted: list[str]) -> list[dict]:
    listed_url = f"{FLINK}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask}/metrics"
    listed = _get(listed_url)
    ids = [item["id"] for item in listed if any(token in item["id"] for token in wanted)]
    found = []
    for offset in range(0, len(ids), 8):
        chunk = ids[offset : offset + 8]
        query = ",".join(urllib.parse.quote(item, safe="") for item in chunk)
        found.extend(_get(f"{listed_url}?get={query}"))
    return found


def operator_watermarks(job_id: str) -> list[dict]:
    job = _get(f"{FLINK}/jobs/{job_id}")
    rows = []
    for vertex in job["vertices"]:
        for index in range(int(vertex.get("parallelism") or 1)):
            values = []
            try:
                metrics = _metrics(
                    job_id,
                    vertex["id"],
                    index,
                    ["currentInputWatermark", "currentOutputWatermark"],
                )
            except Exception as exc:
                rows.append({"name": vertex["name"], "subtask": index, "error": str(exc)[:160]})
                continue
            for metric in metrics:
                if "Watermark" not in str(metric.get("id", "")):
                    continue
                values.append({"id": metric.get("id"), "value": metric.get("value")})
            if values:
                rows.append({"name": vertex["name"], "subtask": index, "watermarks": values})
    return rows


def _metric_values(snapshot: list[dict], metric_id: str) -> dict[int, int]:
    """One operator watermark per subtask. Chained operators are not mixed in."""
    found = {}
    for row in snapshot:
        for metric in row.get("watermarks", []):
            if metric.get("id") != metric_id:
                continue
            try:
                found[int(row["subtask"])] = int(metric["value"])
            except (TypeError, ValueError):
                continue
    return found


def window_mark(snapshot: list[dict]) -> int | None:
    values = _metric_values(snapshot, "tumbling-1m.currentOutputWatermark")
    return min(values.values()) if values else None


def source_marks(snapshot: list[dict]) -> dict[int, int]:
    return _metric_values(snapshot, "bounded-out-of-orderness.currentOutputWatermark")


def wait_mark(job_id: str, minimum: int, timeout: int = 90) -> tuple[int, list[dict]]:
    deadline = time.time() + timeout
    last: list[dict] = []
    while time.time() < deadline:
        last = operator_watermarks(job_id)
        value = window_mark(last)
        print("watermark", value, source_marks(last), flush=True)
        if value is not None and value >= minimum:
            return value, last
        time.sleep(2)
    raise SystemExit(f"watermark did not reach {minimum}: {window_mark(last)} sources {source_marks(last)}")


def wait_checkpoint_advances(job_id: str, start_id: int, advances: int, timeout: int = 90) -> dict:
    target = start_id + advances
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = checkpoint_view(job_id)
        current = (last.get("completed") or {}).get("id")
        print("checkpoint", current, flush=True)
        if current is not None and int(current) >= target:
            return last
        time.sleep(3)
    raise SystemExit(f"checkpoint did not advance {advances} from {start_id}: {last}")


def require_kill(job_id: str, topic: str, payload: dict, partition: int, settle_s: float = 4) -> dict:
    killed = inflight_then_kill(job_id, topic, payload, partition, retries=0, settle_s=settle_s)
    if not killed["killed"]:
        raise SystemExit(f"checkpoint completed before kill: {killed}")
    return killed


def assert_absent(summary: dict, heartbeat_id: int) -> None:
    for view in ("read_committed", "read_uncommitted"):
        if heartbeat_id in summary[view]["dead"]:
            raise SystemExit(f"{heartbeat_id} rejected in {view}: {summary}")


def placement(job_id: str) -> dict:
    managers = _get(f"{FLINK}/taskmanagers").get("taskmanagers") or []
    job = _get(f"{FLINK}/jobs/{job_id}")
    hosts = []
    for vertex in job["vertices"]:
        if "bounded-out-of-orderness" not in vertex["name"] and "tumbling-1m" not in vertex["name"]:
            continue
        for index in range(int(vertex.get("parallelism") or 1)):
            subtask = _get(f"{FLINK}/jobs/{job_id}/vertices/{vertex['id']}/subtasks/{index}")
            hosts.append(
                {
                    "vertex": vertex["name"].split("->", 1)[0].strip(),
                    "subtask": index,
                    "host": subtask.get("host"),
                    "taskmanager-id": subtask.get("taskmanager-id"),
                }
            )
    return {
        "taskmanagers": [
            {"id": item.get("id"), "slots": item.get("slotsNumber"), "path": item.get("path")}
            for item in managers
        ],
        "operators": hosts,
    }


def source_records(job_id: str) -> list[dict]:
    job = _get(f"{FLINK}/jobs/{job_id}")
    rows = []
    for vertex in job["vertices"]:
        if "kafka-heartbeats" not in vertex["name"]:
            continue
        for index in range(int(vertex.get("parallelism") or 1)):
            metrics = _metrics(job_id, vertex["id"], index, ["numRecordsIn", "numRecordsOut"])
            rows.append(
                {
                    "subtask": index,
                    "metrics": [
                        {"id": item.get("id"), "value": item.get("value")}
                        for item in metrics
                        if "numRecords" in str(item.get("id", ""))
                    ],
                }
            )
    return rows


def checkpoint_durations(job_id: str) -> list[int]:
    body = _get(f"{FLINK}/jobs/{job_id}/checkpoints")
    durations = []
    for item in body.get("history") or []:
        if item.get("status") == "COMPLETED" and item.get("end_to_end_duration") is not None:
            durations.append(int(item["end_to_end_duration"]))
    return durations


def group_lag(group: str, topic: str) -> dict:
    from kafka import KafkaConsumer, TopicPartition
    from kafka.admin import KafkaAdminClient

    admin = KafkaAdminClient(bootstrap_servers="localhost:19092", client_id="it-rec-lag")
    try:
        offsets = admin.list_consumer_group_offsets(group)
    except Exception as exc:
        admin.close()
        return {"error": str(exc)[:240]}
    admin.close()
    consumer = KafkaConsumer(bootstrap_servers="localhost:19092")
    lag = {}
    for tp, meta in offsets.items():
        if tp.topic != topic:
            continue
        end = consumer.end_offsets([tp])[tp]
        committed = meta.offset
        lag[tp.partition] = {"committed": committed, "end": end, "lag": end - committed}
    consumer.close()
    return lag


def open_job(spark, name: str, parallelism: int, idle_s: int, checkpoint_ms: int):
    topic = f"it.p2.rec{name}.heartbeat"
    dlq = f"it.p2.rec{name}.deadletter"
    heartbeats = f"cinestream.silver.it_p2_rec{name}_heartbeats"
    windows = f"cinestream.silver.it_p2_rec{name}_windows"
    recreate_topics([(topic, 3), (dlq, 1)])
    drop_tables(spark, [heartbeats, windows])
    job_id = submit_pipeline(
        topic,
        dlq,
        f"it-rec{name}",
        heartbeats,
        windows,
        f"it-rec{name}-dlq",
        [
            "--checkpoint-interval-ms",
            str(checkpoint_ms),
            "--parallelism",
            str(parallelism),
            "--idle-timeout-s",
            str(idle_s),
        ],
    )
    assert_graph(job_id, parallelism)
    return topic, dlq, heartbeats, windows, job_id


def case_parallelism_1(spark) -> dict:
    topic, dlq, heartbeats, windows, job_id = open_job(spark, "p1", 1, 30, 30000)
    first = event(1, 10, 0, 10)
    send_records(
        topic,
        [
            (first, 0),
            (event(2, 11, 20_000, 500), 1),
            (dict(first), 2),
            (event(3, 12, 50_000, 10), 2),
            (event(4, 13, 10_000, 10), 0),
            (event(5, 14, 80_000, 10), 1),
        ],
        gap_s=1,
    )
    opened = wait_epoch(spark, windows, 4, 40, 4)
    seen = wait_heartbeats(spark, heartbeats, {1, 2, 3, 4, 5})
    if seen[1][0] != 1 or seen[2][1] != 500 or seen[2][2] != 10:
        raise SystemExit(f"dedup or seek mismatch: {seen}")
    send_records(topic, [(event(6, 15, 5_000, 10), 0)], gap_s=0)
    updated = wait_epoch(spark, windows, 5, 50, 5)
    if opened[0] != updated[0]:
        raise SystemExit(f"allowed late created a new window: {opened[0]} -> {updated[0]}")
    send_records(topic, [(event(7, 16, 330_000, 10), 2)], gap_s=0)
    wait_heartbeats(spark, heartbeats, {1, 2, 3, 4, 5, 6, 7}, timeout=180)
    before = operator_watermarks(job_id)
    send_records(topic, [(event(9, 18, 320_000, 10), 1)], gap_s=0)
    wait_heartbeats(spark, heartbeats, {1, 2, 3, 4, 5, 6, 7, 9}, timeout=120)
    assert_absent(dlq_summary(dlq), 9)
    first_kill = require_kill(job_id, topic, event(8, 17, 0, 10), 0)
    committed = wait_committed_dead(job_id, dlq, 8, first_kill["held_checkpoint"])
    if committed["dlq"]["read_committed"]["dead"] != [8] or committed["dlq"]["read_committed"]["messages"] != 1:
        raise SystemExit(f"first committed DLQ: {committed['dlq']}")
    final_ids = {1, 2, 3, 4, 5, 6, 7, 8, 9}
    wait_heartbeats(spark, heartbeats, final_ids, timeout=120)
    stayed = wait_epoch(spark, windows, 5, 50, 5)
    send_records(topic, [(dict(first), 0), (event(10, 19, 340_000, 10), 2)], gap_s=0)
    wait_heartbeats(spark, heartbeats, final_ids | {10}, timeout=120)
    assert_absent(dlq_summary(dlq), 10)
    if wait_heartbeats(spark, heartbeats, final_ids | {10})[1][0] != 1:
        raise SystemExit("duplicate counted after restore")
    second_kill = require_kill(job_id, topic, event(11, 20, 0, 10), 1)
    second = wait_committed_dead(job_id, dlq, 11, second_kill["held_checkpoint"])
    dead = second["dlq"]["read_committed"]["dead"]
    if sorted(dead) != [8, 11] or dead.count(8) != 1 or dead.count(11) != 1:
        raise SystemExit(f"repeated recovery DLQ: {second['dlq']}")
    if second["dlq"]["read_committed"]["messages"] != 2:
        raise SystemExit(f"committed DLQ message count: {second['dlq']}")
    wait_heartbeats(spark, heartbeats, final_ids | {10, 11}, timeout=120)
    final_window = wait_epoch(spark, windows, 5, 50, 5)
    if final_window != stayed:
        raise SystemExit(f"window changed across recoveries: {stayed} -> {final_window}")
    if duplicate_window_keys(spark, windows):
        raise SystemExit(f"duplicate window keys: {duplicate_window_keys(spark, windows)}")
    return {
        "status": "passed",
        "job_id": job_id,
        "watermarks_before_late": before,
        "window_watermark_before_late": window_mark(before),
        "source_watermarks_before_late": source_marks(before),
        "window": final_window,
        "dlq_read_committed": second["dlq"]["read_committed"],
        "dlq_read_uncommitted": second["dlq"]["read_uncommitted"],
        "snapshot": latest_snapshot(spark, windows),
    }


def case_slow_partition(spark) -> dict:
    topic, dlq, heartbeats, windows, job_id = open_job(spark, "slow", 2, 300, 15000)
    send_records(
        topic,
        [(event(1, 10, 330_000, 10), 0), (event(2, 11, 0, 10), 1)],
        gap_s=0,
    )
    wait_heartbeats(spark, heartbeats, {1, 2}, timeout=180)
    before = operator_watermarks(job_id)
    live = window_mark(before)
    sources = source_marks(before)
    if live is None or live >= CLEANUP_EPOCH_MS:
        raise SystemExit(f"slow partition did not hold the window watermark: {live} sources {sources}")
    killed = require_kill(job_id, topic, event(3, 12, 5_000, 10), 0)
    deadline = time.time() + 120
    summary = None
    while time.time() < deadline:
        view = checkpoint_view(job_id)
        summary = dlq_summary(dlq)
        current = (view.get("completed") or {}).get("id")
        print("slow after", current, summary["read_committed"], flush=True)
        if current is not None and current != killed["held_checkpoint"]:
            break
        time.sleep(3)
    else:
        raise SystemExit(f"no checkpoint after slow restore: {summary}")
    assert_absent(summary, 3)
    send_records(
        topic,
        [
            (event(4, 13, 330_000, 10), 0),
            (event(5, 14, 330_000, 10), 1),
            (event(6, 15, 330_000, 10), 2),
        ],
        gap_s=0,
    )
    updated = wait_epoch(spark, windows, 2, 20, 2, timeout=150)
    assert_absent(dlq_summary(dlq), 3)
    copies = wait_heartbeats(spark, heartbeats, {1, 2, 3, 4, 5, 6}, timeout=120)
    return {
        "status": "passed",
        "job_id": job_id,
        "window_watermark": live,
        "source_watermarks": sources,
        "watermarks": before,
        "window": updated,
        "dlq_read_committed": dlq_summary(dlq)["read_committed"],
        "heartbeat_copies": {str(key): value[0] for key, value in copies.items()},
        "snapshot": latest_snapshot(spark, windows),
    }


def case_idle_then_late(spark) -> dict:
    topic, dlq, heartbeats, windows, job_id = open_job(spark, "idle", 2, 15, 10000)
    send_records(
        topic,
        [(event(1, 10, 0, 10), 0), (event(2, 11, 330_000, 10), 0)],
        gap_s=0,
    )
    wait_heartbeats(spark, heartbeats, {1, 2}, timeout=180)
    value, snapshot = wait_mark(job_id, CLEANUP_EPOCH_MS, timeout=80)
    seen_id = int((checkpoint_view(job_id).get("completed") or {}).get("id") or 0)
    stored = wait_checkpoint_advances(job_id, seen_id, 2, timeout=90)
    opened = wait_epoch(spark, windows, 1, 10, 1, timeout=90)
    after = operator_watermarks(job_id)
    killed = require_kill(job_id, topic, event(3, 12, 0, 10), 0)
    committed = wait_committed_dead(job_id, dlq, 3, killed["held_checkpoint"])
    dead = committed["dlq"]["read_committed"]["dead"]
    if dead != [3] or committed["dlq"]["read_committed"]["messages"] != 1:
        raise SystemExit(f"idle recovery DLQ: {committed['dlq']}")
    if committed["dlq"]["read_uncommitted"]["dead"].count(3) < 1:
        raise SystemExit(f"uncommitted view lost the replay: {committed['dlq']}")
    stayed = wait_epoch(spark, windows, 1, 10, 1)
    if stayed != opened:
        raise SystemExit(f"window absorbed the too-late event: {opened} -> {stayed}")
    copies = wait_heartbeats(spark, heartbeats, {1, 2, 3}, timeout=120)
    return {
        "status": "passed",
        "job_id": job_id,
        "watermark_when_idle_released": value,
        "source_watermarks": source_marks(after),
        "window_watermark": window_mark(after),
        "checkpoint_after_idle": stored.get("completed"),
        "window": stayed,
        "dlq_read_committed": committed["dlq"]["read_committed"],
        "dlq_read_uncommitted": committed["dlq"]["read_uncommitted"],
        "heartbeat_copies": {str(key): value[0] for key, value in copies.items()},
        "snapshot": latest_snapshot(spark, windows),
        "watermarks_at_release": snapshot,
    }


def case_uncheckpointed_gap(spark) -> dict:
    topic, dlq, heartbeats, windows, job_id = open_job(spark, "gap", 2, 16, 10000)
    send_records(
        topic,
        [(event(1, 10, 0, 10), 0), (event(2, 11, 330_000, 10), 0)],
        gap_s=0,
    )
    wait_heartbeats(spark, heartbeats, {1, 2}, timeout=120)
    deadline = time.time() + 80
    low_id = None
    low_mark = None
    value = None
    released: list[dict] = []
    while time.time() < deadline:
        released = operator_watermarks(job_id)
        mark = window_mark(released)
        current = (checkpoint_view(job_id).get("completed") or {}).get("id")
        print("gap poll", mark, current, flush=True)
        if mark is None or mark < CLEANUP_EPOCH_MS:
            low_id = current
            low_mark = mark
        elif low_id is not None and current == low_id:
            value = mark
            break
        elif low_id is not None:
            raise SystemExit(f"checkpoint {low_id} -> {current} landed as watermark became {mark}")
        time.sleep(2)
    else:
        raise SystemExit(f"idle release was not observed on checkpoint {low_id}")
    killed = require_kill(job_id, topic, event(3, 12, 0, 10), 0, settle_s=0.5)
    if killed["held_checkpoint"] != low_id:
        raise SystemExit(
            f"watermark advance was checkpointed; gap was not isolated: {low_id} -> {killed['held_checkpoint']}"
        )
    held_mark = low_mark
    held_checkpoint = low_id
    deadline = time.time() + 90
    summary = None
    while time.time() < deadline:
        view = checkpoint_view(job_id)
        summary = dlq_summary(dlq)
        current = (view.get("completed") or {}).get("id")
        print("gap after", current, summary, flush=True)
        if current is not None and current != killed["held_checkpoint"]:
            break
        time.sleep(3)
    else:
        raise SystemExit(f"no checkpoint after gap restore: {summary}")
    if 3 in summary["read_committed"]["dead"]:
        raise SystemExit(f"uncheckpointed watermark still produced a committed DLQ: {summary}")
    # The 330s event is already checkpointed, so the restored generator does not
    # rebuild that watermark. A new event on every partition closes the window.
    send_records(
        topic,
        [
            (event(4, 13, 330_000, 10), 0),
            (event(5, 14, 330_000, 10), 1),
            (event(6, 15, 330_000, 10), 2),
        ],
        gap_s=0,
    )
    copies = wait_heartbeats(spark, heartbeats, {1, 2, 3, 4, 5, 6}, timeout=120)
    updated = wait_epoch(spark, windows, 2, 20, 2, timeout=120)
    if 3 in dlq_summary(dlq)["read_committed"]["dead"]:
        raise SystemExit("gap event became a committed dead letter after the window fired")
    return {
        "status": "passed",
        "job_id": job_id,
        "watermark_in_checkpoint": held_mark,
        "watermark_at_send": value,
        "source_watermarks_at_send": source_marks(released),
        "held_checkpoint": held_checkpoint,
        "window": updated,
        "dlq_read_committed": dlq_summary(dlq)["read_committed"],
        "dlq_read_uncommitted": summary["read_uncommitted"],
        "heartbeat_copies": {str(key): value[0] for key, value in copies.items()},
        "limitation": "event became late after the last checkpoint and was aggregated on replay",
    }


def case_parallelism_2_contract(spark) -> dict:
    topic, dlq, heartbeats, windows, job_id = open_job(spark, "norm", 2, 30, 10000)
    first = event(1, 10, 0, 10)
    send_records(
        topic,
        [
            (first, 0),
            (event(2, 11, 20_000, 500), 1),
            (dict(first), 2),
            (event(3, 12, 50_000, 10), 2),
            (event(4, 13, 10_000, 10), 0),
            (event(5, 14, 80_000, 10), 0),
            (event(6, 15, 80_000, 10), 1),
            (event(7, 16, 80_000, 10), 2),
        ],
        gap_s=1,
    )
    opened = wait_epoch(spark, windows, 4, 40, 4, timeout=150)
    seen = wait_heartbeats(spark, heartbeats, {1, 2, 3, 4, 5, 6, 7}, timeout=150)
    if seen[1][0] != 1 or seen[2][1] != 500 or seen[2][2] != 10:
        raise SystemExit(f"p2 dedup or seek mismatch: {seen}")
    send_records(topic, [(event(8, 17, 5_000, 10), 1)], gap_s=0)
    updated = wait_epoch(spark, windows, 5, 50, 5, timeout=120)
    if opened[0] != updated[0]:
        raise SystemExit(f"p2 allowed late changed window start: {opened[0]} -> {updated[0]}")
    send_records(
        topic,
        [
            (event(9, 18, 200_000, 10), 0),
            (event(10, 19, 200_000, 10), 1),
            (event(11, 20, 200_000, 10), 2),
        ],
        gap_s=0,
    )
    wait_heartbeats(spark, heartbeats, set(range(1, 12)), timeout=150)
    send_records(topic, [(event(12, 21, 0, 10), 0)], gap_s=0)
    deadline = time.time() + 90
    summary = None
    while time.time() < deadline:
        summary = dlq_summary(dlq)
        print("norm dlq", summary["read_committed"], flush=True)
        if summary["read_committed"]["dead"] == [12] and summary["read_committed"]["messages"] == 1:
            break
        time.sleep(3)
    else:
        raise SystemExit(f"live too-late was not committed once: {summary}")
    stayed = wait_epoch(spark, windows, 5, 50, 5)
    if stayed != updated:
        raise SystemExit(f"too-late changed the aggregate: {updated} -> {stayed}")
    if duplicate_window_keys(spark, windows):
        raise SystemExit("duplicate window keys at parallelism 2")
    rows = window_rows(spark, windows)
    minute = [row for row in rows if str(row[0]).startswith("1970-01-01 00:01:00")]
    if len(minute) != 1 or (minute[0][1], minute[0][2], minute[0][3], minute[0][4]) != (3, 30, 3, 1):
        raise SystemExit(f"minute window aggregate: {rows}")
    return {
        "status": "passed",
        "job_id": job_id,
        "window": stayed,
        "windows": rows,
        "dlq_read_committed": summary["read_committed"],
        "snapshot": latest_snapshot(spark, windows),
    }


def case_benchmark(spark, name: str, parallelism: int) -> dict:
    topic, dlq, heartbeats, windows, job_id = open_job(spark, name, parallelism, 30, 10000)
    where = placement(job_id)
    count = 300
    payloads = [(event(index, 100 + index, index * 1000, 10), index % 3) for index in range(1, count + 1)]
    started = time.monotonic()
    send_records(topic, payloads, gap_s=0)
    send_s = time.monotonic() - started
    wait_heartbeats(spark, heartbeats, set(range(1, count + 1)), timeout=180)
    visible_s = time.monotonic() - started
    return {
        "parallelism": parallelism,
        "events": count,
        "send_s": round(send_s, 3),
        "until_iceberg_s": round(visible_s, 3),
        "events_per_s": round(count / visible_s, 2),
        "checkpoint_durations_ms": checkpoint_durations(job_id),
        "source_records": source_records(job_id),
        "lag": group_lag(f"it-rec{name}", topic),
        "placement": where,
        "window_rows": len(window_rows(spark, windows)),
        "snapshot": latest_snapshot(spark, heartbeats),
    }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--from-case", default="parallelism_1")
    args = parser.parse_args()
    order = [
        ("parallelism_1", case_parallelism_1),
        ("slow_partition", case_slow_partition),
        ("idle_recovery", case_idle_then_late),
        ("uncheckpointed_gap", case_uncheckpointed_gap),
        ("parallelism_2_contract", case_parallelism_2_contract),
        ("benchmark_parallelism_1", lambda spark: case_benchmark(spark, "bench1", 1)),
        ("benchmark_parallelism_2", lambda spark: case_benchmark(spark, "bench2", 2)),
    ]
    names = [name for name, _fn in order]
    if args.from_case not in names:
        raise SystemExit(f"unknown case {args.from_case}")
    start = names.index(args.from_case)
    result: dict = {"offsets_before": end_offsets()}
    spark = None
    try:
        prepare_cluster()
        spark = start_spark("parallel-recovery")
        result["snapshots_before"] = production_snapshots(spark)
        for name, fn in order[start:]:
            result[name] = fn(spark)
            cancel_it_jobs()
        result["status"] = "passed"
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
        path = (
            "/tmp/parallel_recovery_result.json"
            if args.from_case == "parallelism_1"
            else f"/tmp/parallel_recovery_{args.from_case}.json"
        )
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2, default=str)
        print("wrote", path, result.get("status"), flush=True)
        try:
            cancel_it_jobs()
        except Exception as exc:
            print("cancel skipped", exc, flush=True)


if __name__ == "__main__":
    main()
