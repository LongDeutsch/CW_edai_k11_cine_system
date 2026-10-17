"""Đọc topic DLQ và metadata Iceberg trên MinIO. Không ghi thêm dữ liệu."""

from __future__ import annotations

import json
import os
import sys
import urllib.request


def _kafka_dlq(bootstrap: str, topic: str) -> None:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        consumer_timeout_ms=4000,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        print(f"topic {topic} chưa có trên {bootstrap}")
        consumer.close()
        return
    assigned = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    consumer.assign(assigned)
    consumer.seek_to_beginning(*assigned)
    end = consumer.end_offsets(assigned)
    total = sum(end.values())
    print(f"topic {topic} offsets={dict((tp.partition, end[tp]) for tp in assigned)} total={total}")
    shown = 0
    for message in consumer:
        if shown >= 3:
            break
        text = message.value.decode("utf-8", errors="replace")
        print(f"dlq partition={message.partition} offset={message.offset} {text[:400]}")
        shown += 1
    if total == 0:
        print("DLQ chưa có message. Job Flink mới đẩy event quá hạn vào topic này.")
    consumer.close()


def _minio_metadata(endpoint: str) -> None:
    import boto3
    from botocore.client import Config

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    bucket = "lakehouse"
    for prefix in (
        "silver/stg_watch_sessions/metadata/",
        "silver/stg_playback_windows/metadata/",
        "bronze/raw_watch_deadletter/metadata/",
    ):
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=20)
        keys = [item["Key"] for item in response.get("Contents", [])]
        meta = [key for key in keys if key.endswith(".metadata.json") or "/snap-" in key or key.endswith(".avro")]
        print(f"s3://{bucket}/{prefix} objects={response.get('KeyCount', 0)} metadata={len(meta)}")
        for key in meta[:4]:
            print(f"  {key}")


def _rest_config(url: str) -> None:
    with urllib.request.urlopen(url, timeout=3) as response:
        print(f"iceberg {url} {response.status} {response.read(200).decode()}")


def main() -> int:
    bootstrap = os.environ.get("KAFKA_BROKER", "localhost:19092")
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
    rest = os.environ.get("ICEBERG_REST", "http://127.0.0.1:8181/v1/config")
    failed = False
    try:
        _rest_config(rest)
    except Exception as exc:
        failed = True
        print(f"iceberg rest: {exc}")
    try:
        _kafka_dlq(bootstrap, os.environ.get("DLQ_TOPIC", "movie.playback.deadletter"))
    except Exception as exc:
        failed = True
        print(f"kafka: {exc}")
    try:
        _minio_metadata(endpoint)
    except Exception as exc:
        failed = True
        print(f"minio: {exc}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
