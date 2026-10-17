"""Phase 2.5: DLQ transaction visibility after a TaskManager kill.

Topic và bảng tách biệt (``it.p25`` / ``it_p25``). Không đổi offset production.
Không cancel job cho đến khi đã ghi checkpoint, transaction và visibility.
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

from streaming.flink.config import StreamJobConfig
from streaming.flink.it_phase2 import (
    checkpoint_view,
    compose,
    dlq_summary,
    end_offsets,
    event,
    fail_taskmanager,
    heartbeat_counts,
    latest_snapshot,
    prepare_cluster,
    production_snapshots,
    recreate_topics,
    running_jobs,
    send_records,
    start_spark,
    submit_pipeline,
    wait_heartbeats,
    wait_restored,
    window_rows,
)
from streaming.flink.operators.window_engine import HeartbeatWindowEngine

RESULT_PATH = Path("/tmp/phase25_result.json")
HEARTBEATS = "cinestream.silver.it_p25e_heartbeats"
WINDOWS = "cinestream.silver.it_p25e_windows"
TOPIC = "it.p25e.heartbeat"
DLQ = "it.p25e.deadletter"
PREFIX = "it-p25e-dlq"
BOOTSTRAP = "kafka:29092"


def _docker(container: str, *args: str) -> str:
    completed = subprocess.run(
        ["docker", "exec", container, *args],
        text=True,
        capture_output=True,
        check=False,
    )
    return (completed.stdout + completed.stderr).strip()


def transaction_report(prefix: str) -> dict:
    listed = _docker(
        "cinestream-kafka-1",
        "/opt/kafka/bin/kafka-transactions.sh",
        "--bootstrap-server",
        BOOTSTRAP,
        "list",
    )
    described = {}
    for line in listed.splitlines():
        if prefix not in line:
            continue
        transactional_id = line.split()[0]
        described[transactional_id] = _docker(
            "cinestream-kafka-1",
            "/opt/kafka/bin/kafka-transactions.sh",
            "--bootstrap-server",
            BOOTSTRAP,
            "describe",
            "--transactional-id",
            transactional_id,
        )
    producers = _docker(
        "cinestream-kafka-1",
        "/opt/kafka/bin/kafka-transactions.sh",
        "--bootstrap-server",
        BOOTSTRAP,
        "describe-producers",
        "--topic",
        DLQ,
        "--partition",
        "0",
    )
    offsets = _docker(
        "cinestream-kafka-1",
        "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        DLQ,
    )
    return {"list": listed, "described": described, "producers": producers, "end_offsets": offsets}


def segment_markers() -> str:
    listing = _docker("cinestream-kafka-1", "bash", "-lc", f"ls /tmp/kafka-logs/{DLQ}-0/*.log")
    if not listing or "No such" in listing:
        return listing
    path = listing.splitlines()[-1].strip()
    dumped = _docker(
        "cinestream-kafka-1",
        "/opt/kafka/bin/kafka-dump-log.sh",
        "--files",
        path,
        "--deep-iteration",
    )
    kept = []
    for line in dumped.splitlines():
        if any(token in line for token in ("baseOffset:", "endTxnMarker:", "offset:")):
            kept.append(line.strip())
    return "\n".join(kept)


def operator_metrics(job_id: str) -> list[dict]:
    from streaming.flink.it_phase2 import FLINK, _get

    job = _get(f"{FLINK}/jobs/{job_id}")
    found = []
    needles = ("deadLetters", "lateObservations", "currentOutputWatermark", "currentInputWatermark", "numRecordsOut", "numRecordsIn")
    for vertex in job.get("vertices", []):
        listed_url = f"{FLINK}/jobs/{job_id}/vertices/{vertex['id']}/subtasks/0/metrics"
        try:
            listed = _get(listed_url)
        except Exception:
            continue
        ids = [item["id"] for item in listed if any(needle in item["id"] for needle in needles)]
        if not ids:
            continue
        import urllib.parse

        query = ",".join(urllib.parse.quote(item, safe="") for item in ids[:40])
        try:
            values = _get(f"{listed_url}?get={query}")
        except Exception:
            continue
        found.append({"name": vertex["name"][:180], "metrics": values})
    return found


def flink_txn_logs() -> str:
    completed = subprocess.run(
        ["docker", "logs", "--since", "8m", "cinestream-taskmanager-1"],
        text=True,
        capture_output=True,
        check=False,
    )
    kept = []
    for line in (completed.stdout + completed.stderr).splitlines():
        if PREFIX not in line and "endTxn" not in line and "Aborting incomplete" not in line:
            continue
        if any(token in line for token in ("Created new transactional", "Aborting incomplete", "ProducerId set", "Initialized transactional", "commitTransaction", "Committing")):
            kept.append(line[-360:])
    return "\n".join(kept[-80:])


def observe(spark, job_id: str, label: str) -> dict:
    heartbeats = None
    snapshot = None
    windows = None
    try:
        heartbeats = heartbeat_counts(spark, HEARTBEATS)
    except Exception as exc:
        heartbeats = str(exc).splitlines()[-1][:200]
    try:
        snapshot = latest_snapshot(spark, HEARTBEATS)
    except Exception as exc:
        snapshot = str(exc).splitlines()[-1][:200]
    try:
        windows = window_rows(spark, WINDOWS)
    except Exception as exc:
        windows = str(exc).splitlines()[-1][:200]
    try:
        dlq = dlq_summary(DLQ)
    except Exception as exc:
        dlq = str(exc).splitlines()[-1][:200]
    observation = {
        "label": label,
        "at": time.strftime("%H:%M:%S"),
        "checkpoint": checkpoint_view(job_id),
        "heartbeats": heartbeats,
        "snapshot": snapshot,
        "windows": windows,
        "dlq": dlq,
        "transactions": transaction_report(PREFIX),
        "segment": segment_markers(),
        "metrics": operator_metrics(job_id),
    }
    print(
        label,
        observation["checkpoint"].get("completed"),
        observation["heartbeats"],
        observation["dlq"],
        flush=True,
    )
    return observation


def wait_checkpoints(spark, job_id: str, count: int, timeout: int) -> list[dict]:
    seen = None
    observations = []
    deadline = time.time() + timeout
    while time.time() < deadline and len(observations) < count:
        view = checkpoint_view(job_id)
        completed = (view.get("completed") or {}).get("id")
        if completed is not None and completed != seen:
            observations.append(observe(spark, job_id, f"checkpoint-{completed}"))
            seen = completed
        time.sleep(3)
    return observations


def main() -> None:
    sequence = [event(1, 10, 0), event(2, 11, 20_000, 500), event(3, 12, 80_000), event(4, 13, 330_000), event(5, 15, 0)]
    dead = [item["heartbeat_id"] for item in HeartbeatWindowEngine(StreamJobConfig()).process_all(sequence).dead_letters]
    if dead != [5]:
        raise SystemExit(f"chuỗi không còn dead letter 5: {dead}")

    result: dict = {"offsets_before": end_offsets()}
    spark = None
    job_id = None
    try:
        prepare_cluster()
        spark = start_spark("phase25-it")
        result["snapshots_before"] = production_snapshots(spark)
        from streaming.flink.it_phase2 import drop_tables

        recreate_topics([(TOPIC, 3), (DLQ, 1)])
        drop_tables(spark, [HEARTBEATS, WINDOWS])
        job_id = submit_pipeline(
            TOPIC,
            DLQ,
            "it-p25",
            HEARTBEATS,
            WINDOWS,
            PREFIX,
            ["--checkpoint-interval-ms", "30000", "--parallelism", "1"],
        )
        result["job_id"] = job_id
        send_records(
            TOPIC,
            [
                (event(1, 10, 0), 0),
                (event(2, 11, 20_000, 500), 1),
                (event(3, 12, 80_000), 0),
                (event(4, 13, 330_000), 2),
            ],
            gap_s=1,
        )
        wait_heartbeats(spark, HEARTBEATS, {1, 2, 3, 4}, timeout=180)
        holding = checkpoint_view(job_id)
        result["checkpoint_holding_1_4"] = holding
        if not holding["completed"] or int(holding["completed"]["state_size"] or 0) <= 0:
            raise SystemExit(f"checkpoint trước record đang bay không có state: {holding}")
        held_id = holding["completed"]["id"]
        send_records(TOPIC, [(event(5, 15, 0), 0)], gap_s=0)
        time.sleep(4)
        during = checkpoint_view(job_id)
        result["before_kill"] = observe(spark, job_id, "before-kill")
        if not during["completed"] or during["completed"]["id"] != held_id:
            raise SystemExit(f"checkpoint đã hoàn tất trước kill: {during}")
        restored_before = int(holding["counts"]["restored"])
        fail_taskmanager()
        result["restored"] = wait_restored(job_id, restored_before)
        result["after_restore_checkpoints"] = wait_checkpoints(spark, job_id, 3, 150)
        result["logs"] = flink_txn_logs()
        committed = []
        for item in result["after_restore_checkpoints"]:
            dlq = item.get("dlq") or {}
            if isinstance(dlq, dict) and dlq.get("read_committed", {}).get("dead") == [5]:
                committed.append(item["checkpoint"]["completed"]["id"])
        result["read_committed_checkpoint_ids"] = committed
        result["status"] = "visible" if committed else "still_invisible"
    except SystemExit as exc:
        result["status"] = "failed"
        result["error"] = exc.code if isinstance(exc.code, str) else str(exc.code)
        raise
    finally:
        if spark is not None and job_id is not None and result.get("status") != "failed":
            try:
                result["before_cancel"] = observe(spark, job_id, "before-cancel")
            except Exception as exc:
                result["before_cancel_error"] = str(exc)[:300]
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
        RESULT_PATH.write_text(json.dumps(result, indent=2, default=str))
        print("wrote", RESULT_PATH, result.get("status"), flush=True)
        for job in running_jobs():
            if "it_p25" in job.get("name", "") or "it_p2" in job.get("name", ""):
                compose("exec", "-T", "jobmanager", "flink", "cancel", job["jid"], check=False)
        time.sleep(3)
        result["after_cancel_segment"] = segment_markers()
        result["after_cancel_transactions"] = transaction_report(PREFIX)
        result["after_cancel_logs"] = flink_txn_logs()
        RESULT_PATH.write_text(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
