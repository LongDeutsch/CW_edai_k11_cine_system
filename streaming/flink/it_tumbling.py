"""Tumbling-only integration: đúng giờ, trùng, lệch thứ tự, late firing, DLQ, recovery.

Topic và bảng tách biệt (``it.p2.tumbling`` / ``it_p2_tumbling``). Không đổi
offset production và không hủy job trước khi ghi visibility sau restore.
"""

from __future__ import annotations

import json
import time
import urllib.request

from streaming.flink.it_phase2 import (
    checkpoint_view,
    compose,
    dlq_summary,
    end_offsets,
    event,
    fail_taskmanager,
    heartbeat_counts,
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

HEARTBEATS = "cinestream.silver.it_p2_tumbling_heartbeats"
WINDOWS = "cinestream.silver.it_p2_tumbling_windows"
TOPIC = "it.p2.tumbling.heartbeat"
DLQ = "it.p2.tumbling.deadletter"
PREFIX = "it-tumbling-dlq"


def _get(url: str) -> dict:
    with urllib.request.urlopen(url) as response:
        return json.load(response)


def assert_graph(job_id: str) -> list[str]:
    names = [vertex["name"] for vertex in _get(f"http://127.0.0.1:18081/jobs/{job_id}")["vertices"]]
    print("vertices", names, flush=True)
    if not any("tumbling-1m" in name for name in names):
        raise SystemExit(f"graph không có tumbling-1m: {names}")
    if any("sliding" in name.lower() for name in names):
        raise SystemExit(f"graph còn sliding: {names}")
    return names


def epoch_window(rows: list[tuple]) -> tuple | None:
    matches = [row for row in rows if str(row[0]).startswith("1970-01-01 00:00:00")]
    if len(matches) != 1:
        return None
    return matches[0]


def wait_epoch(spark, count: int, watched: int, users: int, timeout: int = 120) -> tuple:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = window_rows(spark, WINDOWS)
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


def main() -> None:
    result: dict = {"offsets_before": end_offsets()}
    spark = None
    job_id = None
    try:
        prepare_cluster()
        spark = start_spark("tumbling-it")
        result["snapshots_before"] = production_snapshots(spark)
        from streaming.flink.it_phase2 import drop_tables

        recreate_topics([(TOPIC, 3), (DLQ, 1)])
        drop_tables(spark, [HEARTBEATS, WINDOWS])
        job_id = submit_pipeline(
            TOPIC,
            DLQ,
            "it-tumbling",
            HEARTBEATS,
            WINDOWS,
            PREFIX,
            ["--checkpoint-interval-ms", "30000", "--parallelism", "1"],
        )
        result["job_id"] = job_id
        result["vertices"] = assert_graph(job_id)
        first = event(1, 10, 0, 10)
        send_records(
            TOPIC,
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
        opened = wait_epoch(spark, 4, 40, 4)
        heartbeats = wait_heartbeats(spark, HEARTBEATS, {1, 2, 3, 4, 5})
        if heartbeats[1][0] != 1 or heartbeats[2][1] != 500 or heartbeats[2][2] != 10:
            raise SystemExit(f"heartbeat sau dedup/seek sai: {heartbeats}")
        send_records(TOPIC, [(event(6, 15, 5_000, 10), 0)], gap_s=0)
        updated = wait_epoch(spark, 5, 50, 5)
        if opened[0] != updated[0]:
            raise SystemExit(f"late firing tạo cửa sổ khác: {opened[0]} -> {updated[0]}")
        send_records(TOPIC, [(event(7, 16, 330_000, 10), 2)], gap_s=0)
        wait_heartbeats(spark, HEARTBEATS, {1, 2, 3, 4, 5, 6, 7}, timeout=180)
        holding = checkpoint_view(job_id)
        result["checkpoint_before_late"] = holding
        if not holding["completed"] or int(holding["completed"]["state_size"] or 0) <= 0:
            raise SystemExit(f"checkpoint trước dead letter không có state: {holding}")
        held_id = holding["completed"]["id"]
        stayed = wait_epoch(spark, 5, 50, 5)
        send_records(TOPIC, [(event(8, 17, 0, 10), 0)], gap_s=0)
        time.sleep(4)
        during = checkpoint_view(job_id)
        result["checkpoint_during_inflight"] = during
        if not during["completed"] or during["completed"]["id"] != held_id:
            raise SystemExit(f"checkpoint đã hoàn tất trước kill: {during}")
        before_dlq = dlq_summary(DLQ)
        result["dlq_before_kill"] = before_dlq
        print("dlq before kill", before_dlq, flush=True)
        restored_before = int(holding["counts"]["restored"])
        fail_taskmanager()
        result["restored"] = wait_restored(job_id, restored_before)
        deadline = time.time() + 150
        committed = None
        while time.time() < deadline:
            view = checkpoint_view(job_id)
            completed = (view.get("completed") or {}).get("id")
            summary = dlq_summary(DLQ)
            print("after restore", completed, summary["read_committed"], flush=True)
            if completed is not None and completed != held_id and summary["read_committed"]["dead"] == [8]:
                committed = summary
                result["checkpoint_after"] = view
                break
            time.sleep(3)
        if committed is None:
            raise SystemExit(f"read_committed chưa thấy dead letter 8: {dlq_summary(DLQ)}")
        if committed["read_committed"]["messages"] != 1:
            raise SystemExit(f"DLQ committed không phải một bản ghi: {committed}")
        final_heartbeats = wait_heartbeats(spark, HEARTBEATS, {1, 2, 3, 4, 5, 6, 7, 8}, timeout=120)
        final_window = wait_epoch(spark, 5, 50, 5)
        if final_window != stayed:
            raise SystemExit(f"cửa sổ đổi sau recovery {stayed} -> {final_window}")
        from streaming.flink.it_phase2 import duplicate_window_keys

        duplicates = duplicate_window_keys(spark, WINDOWS)
        if duplicates:
            raise SystemExit(f"trùng khóa cửa sổ: {duplicates}")
        if final_heartbeats[8][0] != 1 or final_heartbeats[1][0] != 1:
            raise SystemExit(f"heartbeat sau recovery: {final_heartbeats}")
        result["heartbeats"] = {str(key): value for key, value in final_heartbeats.items()}
        result["window"] = final_window
        result["dlq_after"] = committed
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
        path = "/tmp/tumbling_it_result.json"
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2, default=str)
        print("wrote", path, result.get("status"), flush=True)
        for job in running_jobs():
            if "it_p2" in job.get("name", "") or "it_p1" in job.get("name", ""):
                compose("exec", "-T", "jobmanager", "flink", "cancel", job["jid"], check=False)


if __name__ == "__main__":
    main()
