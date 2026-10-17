"""Checkpoint exactly-once cho job heartbeat.

RocksDB giữ state khử trùng ``heartbeat_id``. Barrier 10 giây, mode
exactly-once: từng sink Kafka và Iceberg chỉ commit sau khi checkpoint của
chính sink đó thành công. Hai sink không nằm trong một giao dịch phân tán.

TTL dedup là processing time, mặc định một giờ. Producer chỉ replay trong
200–1500 ms (``--dup-delay-min-ms`` / ``--dup-delay-max-ms``). Một giờ không
phải cam kết nghiệp vụ: đó là trần state, đủ dài hơn một lần restart và phần
Kafka replay từ checkpoint vừa xong. Hết TTL, cùng ``heartbeat_id`` được nhận lại.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CheckpointConfig:
    interval_ms: int = 10_000
    min_pause_ms: int = 5_000
    timeout_ms: int = 60_000
    mode: str = "EXACTLY_ONCE"
    state_backend: str = "rocksdb"
    storage_dir: str = "file:///opt/flink/checkpoints"
    incremental: bool = True


@dataclass(frozen=True)
class StreamJobConfig:
    bootstrap_servers: str = "kafka:29092"
    source_topic: str = "movie.playback.heartbeat"
    dlq_topic: str = "movie.playback.deadletter"
    group_id: str = "cinestream-heartbeat-windows"
    out_of_orderness_ms: int = 15_000
    allowed_lateness_ms: int = 15_000
    heartbeat_seconds: int = 10
    tumbling_ms: int = 60_000
    iceberg_rest_uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3a://lakehouse/"
    s3_endpoint: str = "http://minio:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    catalog: str = "cinestream"
    silver_sessions_table: str = "cinestream.silver.stg_playback_heartbeats"
    silver_windows_table: str = "cinestream.silver.stg_playback_windows"
    checkpoint: CheckpointConfig = CheckpointConfig()
    # Processing time. 0 tắt TTL (state không bị chặn). Mặc định một giờ.
    dedup_ttl_ms: int = 3_600_000
    parallelism: int = 1
    idle_timeout_s: int = 30
    restart_attempts: int = 10
    restart_delay_ms: int = 5_000
    transactional_id_prefix: str = "cinestream-heartbeat-dlq"


def flink_checkpoint_configs(checkpoint: CheckpointConfig) -> dict[str, str]:
    """Các key Flink đặt trước khi ``execute``. Mode exactly-once là bắt buộc."""
    return {
        "execution.checkpointing.interval": str(checkpoint.interval_ms),
        "execution.checkpointing.min-pause": str(checkpoint.min_pause_ms),
        "execution.checkpointing.timeout": str(checkpoint.timeout_ms),
        "execution.checkpointing.mode": checkpoint.mode,
        "execution.checkpointing.externalized-checkpoint-retention": "RETAIN_ON_CANCELLATION",
        "state.backend": checkpoint.state_backend,
        "state.backend.incremental": str(checkpoint.incremental).lower(),
        "state.checkpoints.dir": checkpoint.storage_dir,
        "execution.checkpointing.tolerable-failed-checkpoints": "3",
    }
