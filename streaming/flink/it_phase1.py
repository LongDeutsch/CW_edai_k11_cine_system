"""Chạy chuỗi Phase 1 lên job Flink đang chạy, rồi đối chiếu Spark và DLQ.

Job phải đã submit với topic ``it.p1.heartbeat`` / ``it.p1.deadletter`` và bảng
``cinestream.silver.it_p1_heartbeats``, ``cinestream.silver.it_p1_windows``.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone

from streaming.flink.config import StreamJobConfig
from streaming.flink.operators.window_engine import HeartbeatWindowEngine


def event(heartbeat_id: int, user_id: int, event_ms: int, position: int) -> dict:
    stamp = datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return {
        "heartbeat_id": heartbeat_id,
        "session_id": f"ses-{heartbeat_id:06d}",
        "user_id": user_id,
        "movie_id": 42,
        "event_ts": stamp,
        "current_position_seconds": position,
        "playback_speed": 1.0,
        "bitrate_kbps": 2400,
    }


def batches() -> list[list[dict]]:
    """Mỗi đợt là một mốc watermark. Đợt sau chỉ gửi khi đợt trước đã vào Iceberg."""
    first = event(2, 11, 20_000, 500)
    return [
        [event(1, 10, 0, 10), first, dict(first), event(3, 12, 80_000, 10)],
        [event(4, 13, 5_000, 10)],
        [event(5, 14, 90_000, 10)],
        [event(6, 15, 10_000, 10)],
        [event(7, 16, 330_000, 10)],
        [event(8, 17, 0, 10)],
    ]


def all_events() -> list[dict]:
    return [item for batch in batches() for item in batch]


def expected_engine():
    return HeartbeatWindowEngine(StreamJobConfig()).process_all(all_events())


def _get(url: str) -> dict:
    with urllib.request.urlopen(url) as response:
        return json.load(response)


def running_job_id() -> str:
    overview = _get("http://127.0.0.1:18081/jobs/overview")
    running = [job for job in overview.get("jobs", []) if job.get("state") == "RUNNING"]
    if len(running) != 1:
        raise SystemExit(f"cần đúng một job RUNNING, thấy {len(running)}")
    return running[0]["jid"]


def publish(topic: str, start_batch: int = 0):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers="localhost:19092",
        value_serializer=lambda value: json.dumps(value).encode(),
        key_serializer=lambda key: str(key).encode(),
    )
    for index, batch in enumerate(batches()):
        if index < start_batch:
            continue
        for payload in batch:
            producer.send(topic, key=payload["heartbeat_id"], value=payload)
            producer.flush()
            print("sent", payload["heartbeat_id"], payload["event_ts"], flush=True)
            time.sleep(3.0)
        yield


def tumbling_epoch_zero(spark, table: str):
    spark.sql(f"REFRESH TABLE {table}")
    frame = spark.sql(
        f"""
        SELECT heartbeat_count, total_watch_seconds, active_users
        FROM {table}
        WHERE movie_id = 42
          AND window_type = 'tumbling_1m'
          AND CAST(window_start AS STRING) LIKE '1970-01-01 00:00:00%'
        """
    )
    return [tuple(row) for row in frame.collect()]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="it.p1.heartbeat")
    parser.add_argument("--dlq-topic", default="it.p1.deadletter")
    parser.add_argument("--heartbeats-table", default="cinestream.silver.it_p1_heartbeats")
    parser.add_argument("--windows-table", default="cinestream.silver.it_p1_windows")
    parser.add_argument("--start-batch", type=int, default=0)
    args = parser.parse_args()
    job_id = running_job_id()
    print("job", job_id)
    expected = expected_engine()
    target = next(
        row
        for row in expected.fired_windows
        if row.window_type == "tumbling_1m" and row.window_start_ms == 0
    )
    print(
        "expected tumbling[0]",
        target.heartbeat_ids,
        target.heartbeat_count,
        target.total_watch_seconds,
        target.active_users,
    )

    import argparse as spark_args

    from transformation.batch.common import start_spark

    spark = start_spark(
        "phase1-it",
        spark_args.Namespace(
            master="local[1]",
            warehouse="s3a://lakehouse",
            iceberg_rest="http://127.0.0.1:8181",
            s3_endpoint="http://127.0.0.1:9000",
            s3_access_key="minioadmin",
            s3_secret_key="minioadmin",
            shuffle_partitions=2,
            salt_buckets=1,
            hold_seconds=0,
            no_ui=True,
            ui_port=4040,
            source_dir="data_generator/output/scale_0_1",
            enable_aqe=False,
        ),
    )
    spark.sparkContext.setLogLevel("ERROR")
    sender = publish(args.topic, args.start_batch)
    if args.start_batch <= 0:
        next(sender)
        print("after main fire", _wait_count(spark, args.windows_table, 2))
    if args.start_batch <= 1:
        next(sender)
        print("after late fire", _wait_count(spark, args.windows_table, 3))
    if args.start_batch <= 2:
        next(sender)
        _wait_checkpoint(job_id)
    if args.start_batch <= 3:
        next(sender)
        _wait_checkpoint(job_id)
        stayed = _wait_count(spark, args.windows_table, 3)
        print("after tumbling-only rejection", stayed)
    if args.start_batch <= 4:
        next(sender)
        _wait_checkpoint(job_id)
    if args.start_batch <= 5:
        next(sender)
        _wait_checkpoint(job_id)
        print("final key", _wait_count(spark, args.windows_table, 3))
    _assert_tables(spark, args, expected)
    _assert_dlq(args.dlq_topic)
    _assert_job(job_id)
    spark.stop()
    print("PHASE1_IT_OK")


def _wait_checkpoint(job_id: str) -> None:
    before = _get(f"http://127.0.0.1:18081/jobs/{job_id}/checkpoints")["counts"]["completed"]
    deadline = time.time() + 40
    while time.time() < deadline:
        completed = _get(f"http://127.0.0.1:18081/jobs/{job_id}/checkpoints")["counts"]["completed"]
        if completed > before:
            print("checkpoint", before, "->", completed, flush=True)
            failure = _get(f"http://127.0.0.1:18081/jobs/{job_id}/exceptions").get("root-exception")
            if failure:
                raise SystemExit(failure[:500])
            return
        time.sleep(2)
    raise SystemExit("không có checkpoint mới sau đợt cuối")


def _wait_count(spark, table: str, count: int, timeout_s: int = 50):
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            last = tumbling_epoch_zero(spark, table)
        except Exception as exc:  # bảng chưa có snapshot
            last = str(exc).splitlines()[-1][:180]
        print("poll", last, flush=True)
        if last == [(count, count * 10, count)]:
            return last
        time.sleep(3)
    raise SystemExit(f"không thấy count={count}, cuối cùng {last}")


def _assert_tables(spark, args, expected) -> None:
    spark.sql(f"REFRESH TABLE {args.windows_table}")
    props = {
        row["key"]: row["value"]
        for row in spark.sql(f"SHOW TBLPROPERTIES {args.windows_table}").collect()
    }
    print("window properties", {key: props.get(key) for key in ("format-version", "write.upsert.enabled")})
    if props.get("format-version") != "2" or props.get("write.upsert.enabled") != "true":
        raise SystemExit(f"property cửa sổ sai: {props}")
    rows = spark.sql(
        f"""
        SELECT window_type,
               CAST(unix_millis(CAST(window_start AS TIMESTAMP)) AS BIGINT) AS start_ms,
               heartbeat_count, total_watch_seconds, active_users, COUNT(*) AS copies
        FROM {args.windows_table}
        GROUP BY window_type, window_start, heartbeat_count, total_watch_seconds, active_users
        """
    ).collect()
    actual: dict[tuple, tuple] = {}
    for row in rows:
        key = (row["window_type"], int(row["start_ms"]))
        if key in actual:
            raise SystemExit(f"hai aggregate cho cùng key {key}")
        actual[key] = (
            int(row["heartbeat_count"]),
            int(row["total_watch_seconds"]),
            int(row["active_users"]),
            int(row["copies"]),
        )
    for row in expected.fired_windows:
        key = (row.window_type, row.window_start_ms)
        got = actual.get(key)
        want = (row.heartbeat_count, row.total_watch_seconds, row.active_users, 1)
        if got != want:
            raise SystemExit(f"cửa sổ {key} spark={got} engine={want}")
    extra = set(actual) - {(row.window_type, row.window_start_ms) for row in expected.fired_windows}
    if extra:
        raise SystemExit(f"spark có cửa sổ thừa {extra}")
    spark.sql(f"REFRESH TABLE {args.heartbeats_table}")
    heartbeats = spark.sql(
        f"""
        SELECT heartbeat_id, playback_position_seconds, watch_duration_seconds
        FROM {args.heartbeats_table}
        ORDER BY heartbeat_id
        """
    ).collect()
    print("heartbeats", [(row.heartbeat_id, row.playback_position_seconds, row.watch_duration_seconds) for row in heartbeats])
    by_id = {int(row.heartbeat_id): row for row in heartbeats}
    if set(by_id) != {1, 2, 3, 4, 5, 6, 7, 8}:
        raise SystemExit(f"heartbeat ids {set(by_id)}")
    if int(by_id[2].playback_position_seconds) != 500 or int(by_id[2].watch_duration_seconds) != 10:
        raise SystemExit("seek bị tính vào watch duration")
    snaps = spark.sql(
        f"SELECT snapshot_id, operation FROM {args.windows_table}.snapshots ORDER BY committed_at"
    ).collect()
    print("snapshots", [(row.snapshot_id, row.operation) for row in snaps])
    counts = []
    for snap in snaps:
        version = spark.read.option("snapshot-id", str(snap.snapshot_id)).table(args.windows_table)
        version.createOrReplaceTempView("window_version")
        found = spark.sql(
            """
            SELECT heartbeat_count
            FROM window_version
            WHERE movie_id = 42 AND window_type = 'tumbling_1m'
              AND CAST(window_start AS STRING) LIKE '1970-01-01 00:00:00%'
            """
        ).collect()
        counts.append(int(found[0].heartbeat_count) if found else None)
    print("tumbling0 history", counts)
    if 2 not in counts or 3 not in counts or counts.index(2) > counts.index(3):
        raise SystemExit(f"late firing không thay aggregate cũ: {counts}")


def _assert_dlq(topic: str) -> None:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers="localhost:19092",
        group_id="it-p1-dlq-read",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
        isolation_level="read_committed",
    )
    consumer.assign([TopicPartition(topic, 0)])
    consumer.seek_to_beginning()
    records = [json.loads(message.value.decode()) for message in consumer]
    dead = [item for item in records if item.get("record_kind") == "dead_letter"]
    observations = [item for item in records if item.get("record_kind") == "late_observation"]
    print("dlq dead", [item["heartbeat_id"] for item in dead], "observations", len(observations))
    if [item["heartbeat_id"] for item in dead] != [8]:
        raise SystemExit(f"dead letter {dead}")
    tumbling_only = [item for item in observations if item["heartbeat_id"] == 6 and item["window_type"] == "tumbling_1m"]
    if not tumbling_only:
        raise SystemExit("thiếu observation tumbling của heartbeat 6")
    if any(item["heartbeat_id"] == 6 and item.get("record_kind") == "dead_letter" for item in records):
        raise SystemExit("heartbeat 6 không được là dead letter")


def _assert_job(job_id: str) -> None:
    job = _get(f"http://127.0.0.1:18081/jobs/{job_id}")
    names = [vertex["name"] for vertex in job["vertices"]]
    print("vertices", names)
    if not any("dlq" in name for name in names):
        raise SystemExit("graph không có DLQ")
    checkpoints = _get(f"http://127.0.0.1:18081/jobs/{job_id}/checkpoints")
    print("checkpoints", checkpoints["counts"])
    if checkpoints["counts"]["failed"] != 0 or checkpoints["counts"]["completed"] < 1:
        raise SystemExit(checkpoints["counts"])
    exceptions = _get(f"http://127.0.0.1:18081/jobs/{job_id}/exceptions")
    if exceptions.get("root-exception"):
        raise SystemExit(exceptions["root-exception"][:500])


if __name__ == "__main__":
    main()
