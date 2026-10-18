"""Chặn streaming production cho đến khi được duyệt tường minh.

Job local và bảng ``it_p2_*`` không cần cờ. Topic, bảng Iceberg và database
ClickHouse của production chỉ chạy khi ``CINESTREAM_ALLOW_STREAMING`` là
true, 1 hoặc yes.
"""

from __future__ import annotations

import os

PRODUCTION_SOURCE_TOPIC = "movie.playback.heartbeat"
PRODUCTION_DLQ_TOPIC = "movie.playback.deadletter"
PRODUCTION_HEARTBEATS = "cinestream.silver.stg_playback_heartbeats"
PRODUCTION_WINDOWS = "cinestream.silver.stg_playback_windows"
PRODUCTION_DATABASE = "cinestream_serving"


def allow_streaming() -> bool:
    return os.environ.get("CINESTREAM_ALLOW_STREAMING", "").strip().lower() in {"1", "true", "yes"}


def production_hits(
    *,
    source_topic: str | None = None,
    dlq_topic: str | None = None,
    heartbeats_table: str | None = None,
    windows_table: str | None = None,
    database: str | None = None,
) -> list[str]:
    found = []
    if source_topic == PRODUCTION_SOURCE_TOPIC:
        found.append(source_topic)
    if dlq_topic == PRODUCTION_DLQ_TOPIC:
        found.append(dlq_topic)
    if heartbeats_table == PRODUCTION_HEARTBEATS:
        found.append(heartbeats_table)
    if windows_table == PRODUCTION_WINDOWS:
        found.append(windows_table)
    if database == PRODUCTION_DATABASE:
        found.append(database)
    return found


def require_streaming_approval(**targets: str | None) -> None:
    hits = production_hits(**targets)
    if hits and not allow_streaming():
        joined = ", ".join(hits)
        raise RuntimeError(f"chưa duyệt streaming production. CINESTREAM_ALLOW_STREAMING=true mới được dùng: {joined}")
