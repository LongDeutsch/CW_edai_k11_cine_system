"""Phase 2 trên topic và bảng Iceberg tách biệt. Không đụng offset production.

Chạy từ thư mục repo, Python 3.12 có PySpark và kafka-python::

    PYTHONPATH=. uv run --no-project --python 3.12 --with pyspark==3.5.5 --with kafka-python \\
        python -m streaming.flink.it_phase2
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from streaming.flink.config import StreamJobConfig
from streaming.flink.operators.window_engine import HeartbeatWindowEngine

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "infra/compose/docker-compose.streaming.yml"
FLINK = "http://127.0.0.1:18081"
BOOTSTRAP = "localhost:19092"
RESULT_PATH = Path("/tmp/phase2_result.json")

TABLES = {
    "after_heartbeats": "cinestream.silver.it_p2_heartbeats",
    "after_windows": "cinestream.silver.it_p2_windows",
    "before_heartbeats": "cinestream.silver.it_p2b_heartbeats",
    "before_windows": "cinestream.silver.it_p2b_windows",
    "wm1_heartbeats": "cinestream.silver.it_p2_wm1_heartbeats",
    "wm1_windows": "cinestream.silver.it_p2_wm1_windows",
    "wm2_heartbeats": "cinestream.silver.it_p2_wm2_heartbeats",
    "wm2_windows": "cinestream.silver.it_p2_wm2_windows",
    "conflict": "cinestream.silver.it_p2_conflict",
    "replace": "cinestream.silver.it_p2_replace",
}

WRITER_INVENTORY = [
    {"system": "flink", "table": "silver.stg_playback_heartbeats", "mode": "APPEND", "writers": "streaming job only"},
    {"system": "flink", "table": "silver.stg_playback_windows", "mode": "UPSERT", "writers": "streaming job only"},
    {"system": "flink", "table": "kafka DLQ", "mode": "transactional append", "writers": "streaming job only"},
    {"system": "spark", "table": "silver.stg_ratings", "mode": "CREATE_OR_REPLACE", "writers": "build_gold_layer only"},
    {"system": "spark", "table": "silver.stg_movies", "mode": "CREATE_OR_REPLACE", "writers": "build_gold_layer only"},
    {"system": "spark", "table": "silver.stg_watch_sessions", "mode": "CREATE_OR_REPLACE", "writers": "build_gold_layer only"},
    {"system": "spark", "table": "gold.dim_users", "mode": "CREATE_OR_REPLACE", "writers": "build_gold_layer only"},
    {"system": "spark", "table": "gold.dim_movies", "mode": "CREATE_OR_REPLACE", "writers": "build_gold_layer only"},
    {"system": "spark", "table": "gold.fact_ratings", "mode": "CREATE_OR_REPLACE", "writers": "build_gold_layer only"},
    {"system": "spark", "table": "gold.fact_watch_sessions", "mode": "CREATE_OR_REPLACE", "writers": "build_gold_layer only"},
    {"system": "spark", "table": "gold.obt_movie_engagement", "mode": "CREATE_OR_REPLACE", "writers": "build_gold_layer only"},
]


def event(heartbeat_id: int, user_id: int, event_ms: int, position: int = 10) -> dict:
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


def _get(url: str) -> dict | list:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=check,
    )


def running_jobs() -> list[dict]:
    overview = _get(f"{FLINK}/jobs/overview")
    return [job for job in overview.get("jobs", []) if job.get("state") in ("RUNNING", "RESTARTING", "CREATED", "CANCELLING")]


def prepare_cluster() -> None:
    for job in running_jobs():
        name = job.get("name", "")
        if "it_p1" in name or "it_p2" in name:
            print("cancel", job["jid"], name, flush=True)
            compose("exec", "-T", "jobmanager", "flink", "cancel", job["jid"], check=False)
            continue
        raise SystemExit(f"không kill TaskManager khi còn job khác: {name}")
    deadline = time.time() + 60
    while time.time() < deadline:
        if not running_jobs():
            return
        time.sleep(2)
    raise SystemExit(f"job chưa tắt: {running_jobs()}")


def recreate_topics(specs: list[tuple[str, int]]) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic

    for name, _partitions in specs:
        if not name.startswith("it.p2"):
            raise SystemExit(name)
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="it-p2")
    existing = set(admin.list_topics())
    owned = [name for name, _partitions in specs if name in existing]
    if owned:
        admin.delete_topics(owned)
        deadline = time.time() + 40
        while time.time() < deadline:
            left = [name for name in owned if name in set(admin.list_topics())]
            if not left:
                break
            time.sleep(1)
        else:
            raise SystemExit(f"topic chưa xóa: {left}")
    admin.create_topics([NewTopic(name, partitions, 1) for name, partitions in specs])
    admin.close()


def drop_tables(spark, names: list[str]) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS cinestream.silver")
    for name in names:
        if not name.startswith("cinestream.silver.it_p2"):
            raise SystemExit(name)
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def submit(args: list[str]) -> str:
    completed = compose(
        "exec",
        "-T",
        "jobmanager",
        "flink",
        "run",
        "-d",
        "-py",
        "/opt/cinestream/streaming/flink/jobs/heartbeat_window_job.py",
        "--engine",
        "flink",
        "--bootstrap",
        "kafka:29092",
        "--iceberg-rest",
        "http://iceberg-rest:8181",
        "--s3-endpoint",
        "http://minio:9000",
        *args,
        check=False,
    )
    text = completed.stdout + completed.stderr
    print(text, flush=True)
    marker = "JobID "
    if completed.returncode != 0 or marker not in text:
        raise SystemExit(text[-1500:])
    return text.split(marker, 1)[1].split()[0].strip()


def wait_running(job_id: str, timeout: int = 180) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            job = _get(f"{FLINK}/jobs/{job_id}")
        except urllib.error.URLError as exc:
            last = str(exc)
            time.sleep(2)
            continue
        last = job.get("state")
        print("job", job_id, last, flush=True)
        if last == "RUNNING":
            return job
        if last in ("FAILED", "CANCELED"):
            failure = _get(f"{FLINK}/jobs/{job_id}/exceptions")
            raise SystemExit(str(failure)[:800])
        time.sleep(2)
    raise SystemExit(f"job không RUNNING: {last}")


def checkpoint_view(job_id: str) -> dict:
    body = _get(f"{FLINK}/jobs/{job_id}/checkpoints")
    latest = body.get("latest") or {}

    def brief(item):
        if not item:
            return None
        return {
            "id": item.get("id"),
            "status": item.get("status"),
            "state_size": item.get("state_size"),
            "end_to_end_duration": item.get("end_to_end_duration"),
            "external_path": item.get("external_path"),
        }

    return {
        "counts": body.get("counts"),
        "completed": brief(latest.get("completed")),
        "restored": brief(latest.get("restored")),
        "failed": brief(latest.get("failed")),
    }


def fail_taskmanager() -> None:
    print("docker kill cinestream-taskmanager-1", flush=True)
    subprocess.run(["docker", "kill", "cinestream-taskmanager-1"], check=True)
    time.sleep(2)
    print("docker start cinestream-taskmanager-1", flush=True)
    subprocess.run(["docker", "start", "cinestream-taskmanager-1"], check=True)


def wait_new_checkpoint(job_id: str, previous_id: int, timeout: int = 90) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = checkpoint_view(job_id)
        completed = last.get("completed") or {}
        print("checkpoint", completed.get("id"), flush=True)
        if completed.get("id") not in (None, previous_id):
            return last
        time.sleep(3)
    raise SystemExit(f"không có checkpoint mới sau {previous_id}: {last}")


def wait_restored(job_id: str, restored_before: int, timeout: int = 180) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            managers = _get(f"{FLINK}/taskmanagers").get("taskmanagers") or []
            job = _get(f"{FLINK}/jobs/{job_id}")
        except urllib.error.URLError as exc:
            print("chờ JM", exc, flush=True)
            time.sleep(2)
            continue
        view = checkpoint_view(job_id) if job.get("state") == "RUNNING" else None
        print("recover", job.get("state"), "tm", len(managers), view, flush=True)
        if job.get("state") == "FAILED":
            raise SystemExit(str(_get(f"{FLINK}/jobs/{job_id}/exceptions"))[:800])
        restored = (view or {}).get("counts", {}).get("restored", 0)
        if job.get("state") == "RUNNING" and managers and restored > restored_before:
            return view
        time.sleep(3)
    raise SystemExit("job không restore từ checkpoint")


def log_excerpt() -> list[str]:
    keys = ("restor", "checkpoint", "commit", "transaction", "abort", "snapshot", "iceberg")
    lines: list[str] = []
    for container in ("cinestream-jobmanager-1", "cinestream-taskmanager-1"):
        completed = subprocess.run(
            ["docker", "logs", "--since", "4m", container],
            text=True,
            capture_output=True,
            check=False,
        )
        for line in (completed.stdout + completed.stderr).splitlines():
            if any(key in line.lower() for key in keys):
                lines.append(f"{container}: {line[-400:]}")
    return lines[-50:]


def sample_metrics(job_id: str) -> list[dict]:
    interesting = ("watermark", "pending", "lag", "estimate", "dedup", "late", "dead", "error")
    job = _get(f"{FLINK}/jobs/{job_id}")
    found = []
    for vertex in job.get("vertices", []):
        for index in range(int(vertex.get("parallelism") or 1)):
            listed_url = f"{FLINK}/jobs/{job_id}/vertices/{vertex['id']}/subtasks/{index}/metrics"
            try:
                listed = _get(listed_url)
            except urllib.error.URLError:
                continue
            ids = [
                item["id"]
                for item in listed
                if any(token in item["id"].lower() for token in interesting)
            ][:12]
            if not ids:
                continue
            query = ",".join(urllib.parse.quote(item, safe="") for item in ids)
            try:
                values = _get(f"{listed_url}?get={query}")
            except urllib.error.URLError:
                continue
            found.append({"name": vertex["name"], "subtask": index, "metrics": values})
    return found


def compact_metrics(samples: list[dict]) -> list[dict]:
    kept = []
    for sample in samples:
        metrics = [
            {"id": item.get("id"), "value": item.get("value")}
            for item in sample.get("metrics", [])
            if any(token in str(item.get("id", "")).lower() for token in ("watermark", "pending", "estimate", "dedup", "late", "dead"))
        ]
        if metrics:
            kept.append({"name": sample["name"], "subtask": sample["subtask"], "metrics": metrics})
    return kept


def producer():
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        linger_ms=0,
        value_serializer=lambda value: json.dumps(value).encode(),
        key_serializer=lambda key: str(key).encode(),
    )


def send_records(topic: str, records: list[tuple[dict, int | None]], gap_s: float = 0) -> None:
    client = producer()
    for payload, partition in records:
        client.send(topic, key=payload["heartbeat_id"], value=payload, partition=partition)
        client.flush()
        print("sent", payload["heartbeat_id"], payload["event_ts"], "p", partition, flush=True)
        if gap_s:
            time.sleep(gap_s)
    client.close()


def start_spark(name: str):
    import argparse as spark_args

    from transformation.batch.common import start_spark as build

    spark = build(
        name,
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
    return spark


def heartbeat_counts(spark, table: str) -> dict[int, tuple[int, int]]:
    spark.sql(f"REFRESH TABLE {table}")
    rows = spark.sql(
        f"""
        SELECT heartbeat_id, COUNT(*) AS copies, MIN(playback_position_seconds) AS position,
               MIN(watch_duration_seconds) AS watched
        FROM {table}
        GROUP BY heartbeat_id
        """
    ).collect()
    return {
        int(row.heartbeat_id): (int(row.copies), int(row.position), int(row.watched))
        for row in rows
    }


def window_rows(spark, table: str) -> list[tuple]:
    spark.sql(f"REFRESH TABLE {table}")
    rows = spark.sql(
        f"""
        SELECT CAST(window_start AS STRING) AS start_s,
               heartbeat_count, total_watch_seconds, active_users, COUNT(*) AS copies
        FROM {table}
        GROUP BY window_start, heartbeat_count, total_watch_seconds, active_users
        ORDER BY start_s
        """
    ).collect()
    return [tuple(row) for row in rows]


def duplicate_window_keys(spark, table: str) -> list[tuple]:
    spark.sql(f"REFRESH TABLE {table}")
    rows = spark.sql(
        f"""
        SELECT CAST(window_start AS STRING) AS start_s, COUNT(*) AS copies
        FROM {table}
        GROUP BY window_start
        HAVING COUNT(*) > 1
        """
    ).collect()
    return [tuple(row) for row in rows]


def tumbling_epoch(spark, table: str):
    spark.sql(f"REFRESH TABLE {table}")
    rows = spark.sql(
        f"""
        SELECT heartbeat_count, total_watch_seconds, active_users
        FROM {table}
        WHERE movie_id = 42
          AND CAST(window_start AS STRING) LIKE '1970-01-01 00:00:00%'
        """
    ).collect()
    return [tuple(row) for row in rows]


def read_dlq(topic: str, isolation: str) -> list[dict]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=f"it-p2-{isolation}-{time.time_ns()}",
        enable_auto_commit=False,
        consumer_timeout_ms=2000,
        isolation_level=isolation,
    )
    partitions = consumer.partitions_for_topic(topic) or {0}
    assigned = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    consumer.assign(assigned)
    consumer.seek_to_beginning()
    records = []
    for message in consumer:
        records.append(json.loads(message.value.decode()))
    consumer.close()
    return records


def dlq_summary(topic: str) -> dict:
    committed = read_dlq(topic, "read_committed")
    dirty = read_dlq(topic, "read_uncommitted")

    def kinds(records: list[dict]) -> dict:
        dead = [int(item["heartbeat_id"]) for item in records if item.get("record_kind") == "dead_letter"]
        return {"dead": dead, "messages": len(records)}

    return {"read_committed": kinds(committed), "read_uncommitted": kinds(dirty)}


def end_offsets(topic: str = "movie.playback.heartbeat") -> dict[int, int]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    partitions = consumer.partitions_for_topic(topic) or []
    offsets = {}
    for partition in sorted(partitions):
        tp = TopicPartition(topic, partition)
        offsets[partition] = consumer.end_offsets([tp])[tp]
    consumer.close()
    return offsets


def latest_snapshot(spark, table: str):
    try:
        rows = spark.sql(
            f"SELECT snapshot_id, operation FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()
    except Exception as exc:
        return {"error": str(exc).splitlines()[-1][:180]}
    if not rows:
        return None
    return {"snapshot_id": str(rows[0].snapshot_id), "operation": rows[0].operation}


def production_snapshots(spark) -> dict:
    return {
        "stg_watch_sessions": latest_snapshot(spark, "cinestream.silver.stg_watch_sessions"),
        "stg_playback_heartbeats": latest_snapshot(spark, "cinestream.silver.stg_playback_heartbeats"),
        "stg_playback_windows": latest_snapshot(spark, "cinestream.silver.stg_playback_windows"),
    }


def wait_heartbeats(spark, table: str, expected: set[int], timeout: int = 150) -> dict:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = heartbeat_counts(spark, table)
        except Exception as exc:
            print("heartbeats", str(exc).splitlines()[-1][:180], flush=True)
            time.sleep(3)
            continue
        print("heartbeats", last, flush=True)
        if set(last) == expected and all(item[0] == 1 for item in last.values()):
            return last
        time.sleep(3)
    raise SystemExit(f"heartbeat {table} = {last}, muốn {expected}")


def wait_tumbling(spark, table: str, count: int, timeout: int) -> tuple[list, float]:
    started = time.monotonic()
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = tumbling_epoch(spark, table)
        except Exception as exc:
            print("tumbling", str(exc).splitlines()[-1][:160], flush=True)
            time.sleep(3)
            continue
        print("tumbling", last, flush=True)
        if last == [(count, count * 10, count)]:
            return last, time.monotonic() - started
        time.sleep(3)
    raise SystemExit(f"không thấy tumbling count={count}, cuối {last}")


def submit_pipeline(topic: str, dlq: str, group: str, heartbeats: str, windows: str, prefix: str, extra: list[str]) -> str:
    job_id = submit(
        [
            "--source-topic",
            topic,
            "--dlq-topic",
            dlq,
            "--group-id",
            group,
            "--sessions-table",
            heartbeats,
            "--windows-table",
            windows,
            "--transactional-id-prefix",
            prefix,
            *extra,
        ]
    )
    wait_running(job_id)
    return job_id


def phase_after(spark) -> dict:
    topic = "it.p2.heartbeat"
    dlq = "it.p2.deadletter"
    recreate_topics([(topic, 3), (dlq, 1)])
    drop_tables(spark, [TABLES["after_heartbeats"], TABLES["after_windows"]])
    job_id = submit_pipeline(
        topic,
        dlq,
        "it-p2-after",
        TABLES["after_heartbeats"],
        TABLES["after_windows"],
        "it-p2-after-dlq",
        ["--checkpoint-interval-ms", "10000", "--parallelism", "1", "--idle-timeout-s", "30"],
    )
    original = event(2, 11, 20_000, 500)
    send_records(
        topic,
        [
            (event(1, 10, 0, 10), 0),
            (original, 0),
            (dict(original), 1),
            (event(3, 12, 80_000, 10), 0),
            (event(4, 13, 330_000, 10), 2),
        ],
        gap_s=2,
    )
    committed = wait_heartbeats(spark, TABLES["after_heartbeats"], {1, 2, 3, 4})
    if committed[2][1] != 500 or committed[2][2] != 10:
        raise SystemExit(f"seek bị ghi vào duration: {committed[2]}")
    send_records(topic, [(event(5, 15, 0, 10), 0)], gap_s=0)
    wait_heartbeats(spark, TABLES["after_heartbeats"], {1, 2, 3, 4, 5})
    deadline = time.time() + 40
    dlq_before = None
    while time.time() < deadline:
        dlq_before = dlq_summary(dlq)
        print("dlq before kill", dlq_before["read_committed"], flush=True)
        if dlq_before["read_committed"]["dead"] == [5]:
            break
        time.sleep(3)
    else:
        raise SystemExit(f"DLQ chưa commit dead letter: {dlq_before}")
    view = checkpoint_view(job_id)
    print("checkpoint before kill", view, flush=True)
    if not view["completed"] or int(view["completed"]["state_size"] or 0) <= 0:
        raise SystemExit(f"checkpoint không có state: {view}")
    path = str(view["completed"]["external_path"])
    if "/opt/flink/checkpoints/" not in path:
        raise SystemExit(f"checkpoint nằm ngoài thư mục đã cấu hình: {path}")
    heartbeats_before = heartbeat_counts(spark, TABLES["after_heartbeats"])
    windows_before = window_rows(spark, TABLES["after_windows"])
    duplicate_keys = duplicate_window_keys(spark, TABLES["after_windows"])
    if duplicate_keys:
        raise SystemExit(f"trùng khóa cửa sổ trước kill: {duplicate_keys}")
    restored_before = int(view["counts"]["restored"])
    completed_id = view["completed"]["id"]
    fail_taskmanager()
    restored = wait_restored(job_id, restored_before)
    post_restore_checkpoint = wait_new_checkpoint(job_id, completed_id)
    logs = log_excerpt()
    heartbeats_after = wait_heartbeats(spark, TABLES["after_heartbeats"], {1, 2, 3, 4, 5}, timeout=90)
    windows_after = window_rows(spark, TABLES["after_windows"])
    dlq_after = dlq_summary(dlq)
    if heartbeats_after != heartbeats_before:
        raise SystemExit(f"heartbeat đổi sau restore {heartbeats_before} -> {heartbeats_after}")
    if windows_after != windows_before:
        raise SystemExit(f"cửa sổ đổi sau restore {windows_before} -> {windows_after}")
    if dlq_after["read_committed"] != dlq_before["read_committed"]:
        raise SystemExit(f"DLQ commit đổi sau restore {dlq_before} -> {dlq_after}")
    send_records(topic, [(event(2, 11, 20_000, 999), 0)], gap_s=0)
    time.sleep(20)
    after_dup = wait_heartbeats(spark, TABLES["after_heartbeats"], {1, 2, 3, 4, 5}, timeout=40)
    if after_dup[2][1] != 500:
        raise SystemExit(f"dedup sau restore không giữ bản position 500: {after_dup[2]}")
    send_records(topic, [(event(6, 16, 400_000, 10), 0)], gap_s=0)
    with_new = wait_heartbeats(spark, TABLES["after_heartbeats"], {1, 2, 3, 4, 5, 6})
    return {
        "job_id": job_id,
        "checkpoint_before_kill": view,
        "checkpoint_after_restore": post_restore_checkpoint,
        "restored": restored,
        "heartbeats": {str(key): value for key, value in with_new.items()},
        "windows_unchanged": windows_after == windows_before,
        "duplicate_window_keys": duplicate_keys,
        "dlq_before": dlq_before,
        "dlq_after_restore": dlq_after,
        "logs": logs,
        "metrics": compact_metrics(sample_metrics(job_id)),
    }


def phase_before(spark) -> dict:
    topic = "it.p2b.heartbeat"
    dlq = "it.p2b.deadletter"
    recreate_topics([(topic, 3), (dlq, 1)])
    drop_tables(spark, [TABLES["before_heartbeats"], TABLES["before_windows"]])
    job_id = submit_pipeline(
        topic,
        dlq,
        "it-p2-before",
        TABLES["before_heartbeats"],
        TABLES["before_windows"],
        "it-p2-before-dlq",
        ["--checkpoint-interval-ms", "60000", "--parallelism", "1"],
    )
    send_records(
        topic,
        [
            (event(1, 10, 0), 0),
            (event(2, 11, 20_000, 500), 1),
            (event(3, 12, 80_000), 0),
            (event(4, 13, 330_000), 2),
        ],
        gap_s=2,
    )
    wait_heartbeats(spark, TABLES["before_heartbeats"], {1, 2, 3, 4}, timeout=180)
    view = checkpoint_view(job_id)
    print("checkpoint holding 1-4", view, flush=True)
    if not view["completed"] or int(view["completed"]["state_size"] or 0) <= 0:
        raise SystemExit(f"checkpoint trước record đang bay không có state: {view}")
    completed_id = view["completed"]["id"]
    windows_before = window_rows(spark, TABLES["before_windows"])
    send_records(topic, [(event(5, 15, 0), 0)], gap_s=0)
    time.sleep(4)
    during = checkpoint_view(job_id)
    inflight_counts = None
    try:
        inflight_counts = heartbeat_counts(spark, TABLES["before_heartbeats"])
    except Exception as exc:
        inflight_counts = str(exc).splitlines()[-1][:180]
    inflight_dlq = dlq_summary(dlq)
    print("in flight", during["completed"], inflight_counts, inflight_dlq["read_committed"], flush=True)
    if not during["completed"] or during["completed"]["id"] != completed_id:
        raise SystemExit(f"checkpoint đã hoàn tất trước khi kill, không phải case before: {during}")
    if isinstance(inflight_counts, dict) and 5 in inflight_counts:
        raise SystemExit("Iceberg đã commit heartbeat 5 trước checkpoint")
    if inflight_dlq["read_committed"]["dead"]:
        raise SystemExit("DLQ read_committed đã thấy dead letter trước checkpoint")
    restored_before = int(view["counts"]["restored"])
    fail_taskmanager()
    restored = wait_restored(job_id, restored_before)
    logs = log_excerpt()
    final_counts = wait_heartbeats(spark, TABLES["before_heartbeats"], {1, 2, 3, 4, 5}, timeout=180)
    deadline = time.time() + 60
    final_dlq = None
    while time.time() < deadline:
        final_dlq = dlq_summary(dlq)
        print("dlq after replay", final_dlq["read_committed"], flush=True)
        if final_dlq["read_committed"]["dead"] == [5]:
            break
        time.sleep(3)
    else:
        raise SystemExit(f"sau replay DLQ = {final_dlq}")
    windows_after = window_rows(spark, TABLES["before_windows"])
    if windows_after != windows_before:
        raise SystemExit(f"cửa sổ đổi vì replay late event {windows_before} -> {windows_after}")
    if duplicate_window_keys(spark, TABLES["before_windows"]):
        raise SystemExit("trùng khóa cửa sổ sau replay")
    return {
        "job_id": job_id,
        "checkpoint_committed": view,
        "checkpoint_during_inflight": during,
        "inflight_heartbeats": inflight_counts,
        "inflight_dlq_committed": inflight_dlq["read_committed"],
        "inflight_dlq_uncommitted": inflight_dlq["read_uncommitted"],
        "restored": restored,
        "final_heartbeats": {str(key): value for key, value in final_counts.items()},
        "final_dlq": final_dlq,
        "windows_unchanged": True,
        "logs": logs,
    }


def _min_watermark(sample: dict, needle: str):
    if "vertices" not in sample:
        return None
    values = []
    for vertex in sample["vertices"]:
        if needle not in vertex["name"]:
            continue
        for metric in vertex["metrics"]:
            if not str(metric.get("id", "")).endswith("currentInputWatermark"):
                continue
            try:
                values.append(int(metric["value"]))
            except (TypeError, ValueError):
                continue
    if not values:
        return None
    return min(values)


def _first_watermark(samples: list[dict], needle: str, threshold: int):
    for sample in samples:
        value = _min_watermark(sample, needle)
        if value is not None and value >= threshold:
            return sample["elapsed_s"], value
    return None, None


def phase_watermarks(spark, name: str, parallelism: int, topic: str, dlq: str) -> dict:
    heartbeats = TABLES[f"{name}_heartbeats"]
    windows = TABLES[f"{name}_windows"]
    recreate_topics([(topic, 3), (dlq, 1)])
    drop_tables(spark, [heartbeats, windows])
    job_id = submit_pipeline(
        topic,
        dlq,
        f"it-p2-{name}",
        heartbeats,
        windows,
        f"it-p2-{name}-dlq",
        ["--parallelism", str(parallelism), "--idle-timeout-s", "30", "--checkpoint-interval-ms", "10000"],
    )
    started = time.monotonic()
    send_records(
        topic,
        [
            (event(1, 10, 0), 0),
            (event(2, 11, 0), 1),
            (event(3, 12, 0), 2),
            (event(4, 13, 80_000), 0),
        ],
        gap_s=0,
    )
    samples = []
    iceberg_elapsed = None
    timeout = 50 if parallelism == 1 else 100
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        elapsed = round(time.monotonic() - started, 1)
        try:
            samples.append({"elapsed_s": elapsed, "vertices": compact_metrics(sample_metrics(job_id))})
        except Exception as exc:
            samples.append({"elapsed_s": elapsed, "error": str(exc)[:180]})
        try:
            last = tumbling_epoch(spark, windows)
        except Exception as exc:
            last = str(exc).splitlines()[-1][:160]
        print("wm", name, elapsed, last, flush=True)
        if last == [(3, 30, 3)]:
            iceberg_elapsed = time.monotonic() - started
            break
        time.sleep(4)
    watermark_elapsed, watermark_value = _first_watermark(samples, "tumbling", 59_999)
    if iceberg_elapsed is None:
        raise SystemExit(f"{name} không đóng cửa sổ, cuối {last}, watermark {watermark_elapsed}")
    late_elapsed = None
    if parallelism == 1:
        send_records(topic, [(event(5, 14, 10_000), 1)], gap_s=0)
        _rows, late_elapsed = wait_tumbling(spark, windows, 4, 50)
    return {
        "job_id": job_id,
        "parallelism": parallelism,
        "idle_timeout_s": 30,
        "partitions": {"active": 0, "slow": 1, "idle_after_prime": 2},
        "iceberg_elapsed_s": round(iceberg_elapsed, 1),
        "tumbling_input_watermark_elapsed_s": watermark_elapsed,
        "tumbling_input_watermark": watermark_value,
        "late_partition_elapsed_s": None if late_elapsed is None else round(late_elapsed, 1),
        "samples": samples,
    }


def append_worker(args: argparse.Namespace) -> None:
    ready = Path(args.ready)
    go = Path(args.go)
    result = Path(args.result)
    try:
        spark = start_spark(f"p2-writer-{args.writer}")
        ready.write_text("ready")
        deadline = time.time() + 180
        while not go.exists():
            if time.time() > deadline:
                raise TimeoutError("không thấy tín hiệu append")
            time.sleep(0.1)
        start = 0 if args.writer == "a" else args.rows
        frame = spark.createDataFrame([(start + index, args.writer) for index in range(args.rows)], "seq LONG, writer STRING")
        frame.writeTo(args.table).append()
        result.write_text("ok")
        spark.stop()
    except Exception as exc:
        result.write_text("fail\n" + str(exc)[:2000])
        raise


def phase_writers(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS cinestream.silver")
    drop_tables(spark, [TABLES["replace"], TABLES["conflict"]])
    first = spark.createDataFrame([(1, "seed"), (2, "seed")], "seq LONG, writer STRING")
    first.writeTo(TABLES["replace"]).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    second = spark.createDataFrame([(3, "replacement")], "seq LONG, writer STRING")
    second.writeTo(TABLES["replace"]).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    replaced = [tuple(row) for row in spark.sql(f"SELECT seq, writer FROM {TABLES['replace']} ORDER BY seq").collect()]
    seed = spark.createDataFrame([(-1, "seed")], "seq LONG, writer STRING")
    seed.writeTo(TABLES["conflict"]).using("iceberg").tableProperty("format-version", "2").create()
    spark.sql(f"ALTER TABLE {TABLES['conflict']} SET TBLPROPERTIES ('commit.retry.num-retries'='0')")
    spark.stop()

    directory = Path(tempfile.mkdtemp(prefix="p2-writers-"))
    go = directory / "go"
    procs = []
    for writer in ("a", "b"):
        procs.append(
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "streaming.flink.it_phase2",
                    "--phase",
                    "append-worker",
                    "--table",
                    TABLES["conflict"],
                    "--writer",
                    writer,
                    "--rows",
                    "2000",
                    "--result",
                    str(directory / f"{writer}.txt"),
                    "--ready",
                    str(directory / f"{writer}.ready"),
                    "--go",
                    str(go),
                ],
                cwd=ROOT,
                env={**os.environ, "PYTHONPATH": str(ROOT)},
            )
        )
    deadline = time.time() + 180
    while time.time() < deadline:
        if all((directory / f"{writer}.ready").exists() for writer in ("a", "b")):
            break
        time.sleep(0.2)
    else:
        go.write_text("stop")
        for proc in procs:
            proc.kill()
        raise SystemExit("worker Spark không sẵn sàng")
    go.write_text("go")
    codes = [proc.wait(timeout=240) for proc in procs]
    outcomes = {}
    for writer in ("a", "b"):
        path = directory / f"{writer}.txt"
        outcomes[writer] = path.read_text()[:500] if path.exists() else f"missing exit {codes}"
    spark = start_spark("p2-writer-check")
    rows = spark.sql(f"SELECT seq, writer FROM {TABLES['conflict']}").collect()
    present = {int(row.seq) for row in rows}
    expected_a = set(range(0, 2000))
    expected_b = set(range(2000, 4000))
    partial = []
    for label, expected in (("a", expected_a), ("b", expected_b)):
        overlap = present & expected
        if overlap and overlap != expected:
            partial.append(label)
    return spark, {
        "inventory": WRITER_INVENTORY,
        "shared_production_tables": [],
        "replace_rows": replaced,
        "conflict_outcomes": outcomes,
        "conflict_exit_codes": codes,
        "conflict_rows": len(rows),
        "partial_writers": partial,
        "retry_property": "commit.retry.num-retries=0",
    }


def _watermark_gate(row: dict, parallelism: int) -> bool:
    """Parallelism 1 không được chờ idle 30s. Parallelism 2 phải chờ subtask im."""
    watermark = row.get("tumbling_input_watermark_elapsed_s")
    iceberg = row.get("iceberg_elapsed_s") or 0
    if parallelism == 1:
        fast_watermark = watermark is not None and watermark < 20
        return fast_watermark or iceberg < 32
    if watermark is not None:
        return watermark >= 25
    return iceberg >= 30


def _watermark_separated(first: dict, second: dict) -> bool:
    return _watermark_gate(first, 1) and _watermark_gate(second, 2)


def _assert_writers(writers: dict) -> None:
    if writers["partial_writers"]:
        raise SystemExit(f"commit Iceberg ghi dở: {writers['partial_writers']}")
    succeeded = [name for name, text in writers["conflict_outcomes"].items() if text.startswith("ok")]
    expected_rows = 1 + 2000 * len(succeeded)
    if writers["conflict_rows"] != expected_rows:
        raise SystemExit(f"số dòng conflict {writers['conflict_rows']} khác {expected_rows}: {writers['conflict_outcomes']}")
    if succeeded:
        return
    text = " ".join(writers["conflict_outcomes"].values()).lower()
    if writers["conflict_rows"] == 1 and any(token in text for token in ("conflict", "commit", "validation")):
        return
    raise SystemExit(f"hai writer không commit và cũng không báo conflict: {writers['conflict_outcomes']}")


def cancel_it_jobs() -> None:
    for job in running_jobs():
        if "it_p2" in job.get("name", "") or "it_p1" in job.get("name", ""):
            compose("exec", "-T", "jobmanager", "flink", "cancel", job["jid"], check=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", default="all")
    parser.add_argument("--table", default="")
    parser.add_argument("--writer", default="a")
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--result", default="")
    parser.add_argument("--ready", default="")
    parser.add_argument("--go", default="")
    args = parser.parse_args()
    if args.phase == "append-worker":
        append_worker(args)
        return

    sequence = [event(1, 10, 0), event(2, 11, 20_000, 500), event(3, 12, 80_000), event(4, 13, 330_000), event(5, 15, 0)]
    dead = [item["heartbeat_id"] for item in HeartbeatWindowEngine(StreamJobConfig()).process_all(sequence).dead_letters]
    if dead != [5]:
        raise SystemExit(f"chuỗi recovery không còn là dead letter: {dead}")

    result: dict = {"offsets_before": end_offsets()}
    spark = None
    try:
        prepare_cluster()
        spark = start_spark("phase2-it")
        result["snapshots_before"] = production_snapshots(spark)
        print("production before", result["offsets_before"], result["snapshots_before"], flush=True)
        if args.phase in ("all", "recover"):
            result["recover_after"] = phase_after(spark)
            cancel_it_jobs()
            prepare_cluster()
            result["recover_before"] = phase_before(spark)
            cancel_it_jobs()
            prepare_cluster()
        if args.phase in ("all", "rest"):
            result["watermark_parallelism_1"] = phase_watermarks(spark, "wm1", 1, "it.p2.wm1.heartbeat", "it.p2.wm1.deadletter")
            cancel_it_jobs()
            prepare_cluster()
            result["watermark_parallelism_2"] = phase_watermarks(spark, "wm2", 2, "it.p2.wm2.heartbeat", "it.p2.wm2.deadletter")
            cancel_it_jobs()
            prepare_cluster()
            spark, writers = phase_writers(spark)
            result["writers"] = writers
        if writers["replace_rows"] != [(3, "replacement")]:
            raise SystemExit(f"createOrReplace không thay bảng: {writers['replace_rows']}")
        _assert_writers(writers)
        result["offsets_after"] = end_offsets()
        result["snapshots_after"] = production_snapshots(spark)
        if result["offsets_before"] != result["offsets_after"]:
            raise SystemExit(f"offset production đổi {result['offsets_before']} -> {result['offsets_after']}")
        if result["snapshots_before"] != result["snapshots_after"]:
            raise SystemExit(f"snapshot production đổi {result['snapshots_before']} -> {result['snapshots_after']}")
        if not _watermark_separated(result["watermark_parallelism_1"], result["watermark_parallelism_2"]):
            brief = {
                "p1": {
                    "iceberg_s": result["watermark_parallelism_1"].get("iceberg_elapsed_s"),
                    "watermark_s": result["watermark_parallelism_1"].get("tumbling_input_watermark_elapsed_s"),
                },
                "p2": {
                    "iceberg_s": result["watermark_parallelism_2"].get("iceberg_elapsed_s"),
                    "watermark_s": result["watermark_parallelism_2"].get("tumbling_input_watermark_elapsed_s"),
                },
            }
            raise SystemExit(f"watermark không tách parallelism 1 và 2: {brief}")
        result["status"] = "ok"
        print("PHASE2_IT_OK", flush=True)
    except SystemExit as exc:
        result["status"] = "failed"
        result["error"] = exc.code if isinstance(exc.code, str) else str(exc.code)
        raise
    finally:
        if spark is not None:
            try:
                result["snapshots_after"] = result.get("snapshots_after") or production_snapshots(spark)
            except Exception as exc:
                result["snapshots_after_error"] = str(exc)[:300]
            try:
                spark.stop()
            except Exception as exc:
                result["spark_stop_error"] = str(exc)[:300]
        try:
            result["offsets_after"] = result.get("offsets_after") or end_offsets()
        except Exception as exc:
            result["offsets_after_error"] = str(exc)[:300]
        RESULT_PATH.write_text(json.dumps(result, indent=2, default=str))
        print("wrote", RESULT_PATH, flush=True)
        cancel_it_jobs()


if __name__ == "__main__":
    main()
