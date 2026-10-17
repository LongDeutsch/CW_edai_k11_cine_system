"""Mô phỏng WindowOperator của Flink cho tumbling 1 phút và sliding 5 phút.

Dedup theo ``heartbeat_id`` xảy ra trước, với TTL processing time. Đồng hồ local
chỉ chạy khi gọi ``advance_processing_time``. Trên Flink, processing time vẫn
trôi khi task chết, nên downtime dài hơn TTL làm checkpoint trả lại id đã thấy.
Hết hạn thì id đó được nhận lại và cửa sổ đếm thêm một lần. Mỗi cửa sổ tự xét
cleanup. Main firing khi watermark qua ``end - 1``. Late firing khi event còn kịp allowed
lateness sau main firing. Cleanup xóa state, không phát thêm. Event không bị
loại chỉ vì timestamp nhỏ hơn watermark.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from streaming.flink.config import StreamJobConfig
from streaming.flink.operators.late_policy import WindowSpec, classify, late_records
from streaming.flink.operators.watermark_assigner import (
    WatermarkClock,
    extract_event_time_ms,
    is_main_fire_due,
    is_window_late,
)
from streaming.flink.operators.watch_metrics import watched_delta_seconds


@dataclass
class WindowRow:
    movie_id: int
    window_type: str
    window_start_ms: int
    window_end_ms: int
    heartbeat_count: int
    total_watch_seconds: int
    active_users: int
    heartbeat_ids: tuple[int, ...] = ()

    def as_dict(self) -> dict:
        return {
            "movie_id": self.movie_id,
            "window_type": self.window_type,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "heartbeat_count": self.heartbeat_count,
            "total_watch_seconds": self.total_watch_seconds,
            "active_users": self.active_users,
            "heartbeat_ids": list(self.heartbeat_ids),
        }


@dataclass
class WindowFiring:
    kind: str
    row: WindowRow


@dataclass
class _Bucket:
    movie_id: int
    window_type: str
    start_ms: int
    end_ms: int
    heartbeat_ids: list[int] = field(default_factory=list)
    users: set[int] = field(default_factory=set)
    watched_seconds: int = 0
    main_fired: bool = False

    def add(self, heartbeat_id: int, user_id: int, watched_seconds: int) -> None:
        self.heartbeat_ids.append(heartbeat_id)
        self.users.add(user_id)
        self.watched_seconds += watched_seconds

    def row(self) -> WindowRow:
        return WindowRow(
            movie_id=self.movie_id,
            window_type=self.window_type,
            window_start_ms=self.start_ms,
            window_end_ms=self.end_ms,
            heartbeat_count=len(self.heartbeat_ids),
            total_watch_seconds=self.watched_seconds,
            active_users=len(self.users),
            heartbeat_ids=tuple(self.heartbeat_ids),
        )


@dataclass
class EngineResult:
    accepted: int = 0
    duplicates: int = 0
    dead_letters: list[dict] = field(default_factory=list)
    late_observations: list[dict] = field(default_factory=list)
    firings: list[WindowFiring] = field(default_factory=list)
    closed_windows: list[WindowRow] = field(default_factory=list)
    open_windows: list[WindowRow] = field(default_factory=list)
    fired_windows: list[WindowRow] = field(default_factory=list)
    session_rows: list[dict] = field(default_factory=list)
    final_watermark_ms: int | None = None


class HeartbeatWindowEngine:
    def __init__(self, config: StreamJobConfig):
        self.config = config
        self.spec = WindowSpec.from_config(config)
        self.clock = WatermarkClock(config.out_of_orderness_ms)
        self._seen: dict[int, int] = {}
        self._processing_time_ms = 0
        self._open: dict[tuple, _Bucket] = {}

    def advance_processing_time(self, delta_ms: int) -> None:
        """Đẩy đồng hồ processing time. Flink làm việc này cả khi job đang restart."""
        if delta_ms < 0:
            raise ValueError("processing time chỉ đi tới")
        self._processing_time_ms += delta_ms
        self._purge_expired()

    def process_all(self, payloads: list[dict]) -> EngineResult:
        result = EngineResult()
        for payload in payloads:
            self._process_one(payload, result)
        result.final_watermark_ms = self.clock.watermark_ms
        result.open_windows = _sorted_rows(bucket.row() for bucket in self._open.values())
        result.closed_windows = _sorted_rows(result.closed_windows)
        fired = [row for row in result.closed_windows]
        fired.extend(bucket.row() for bucket in self._open.values() if bucket.main_fired)
        result.fired_windows = _sorted_rows(fired)
        return result

    def _expired(self, seen_at_ms: int) -> bool:
        ttl_ms = self.config.dedup_ttl_ms
        return ttl_ms > 0 and self._processing_time_ms - seen_at_ms >= ttl_ms

    def _purge_expired(self) -> None:
        if self.config.dedup_ttl_ms <= 0:
            return
        expired = [heartbeat_id for heartbeat_id, seen_at in self._seen.items() if self._expired(seen_at)]
        for heartbeat_id in expired:
            del self._seen[heartbeat_id]

    def _process_one(self, payload: dict, result: EngineResult) -> None:
        heartbeat_id = int(payload["heartbeat_id"])
        seen_at = self._seen.get(heartbeat_id)
        if seen_at is not None and not self._expired(seen_at):
            result.duplicates += 1
            return
        self._seen[heartbeat_id] = self._processing_time_ms
        event_ts = extract_event_time_ms(payload)
        previous = self.clock.observe(event_ts)
        watched = watched_delta_seconds(payload, self.config.heartbeat_seconds)
        result.session_rows.append(_session_row(payload, event_ts, watched))
        decision = classify(event_ts, previous, self.spec)
        for record in late_records(payload, event_ts, previous, self.spec):
            if record["record_kind"] == "dead_letter":
                result.dead_letters.append(record)
            else:
                result.late_observations.append(record)
        if not decision.accepted:
            self._advance(result)
            return
        result.accepted += 1
        movie_id = int(payload["movie_id"])
        user_id = int(payload["user_id"])
        for span in decision.accepted:
            key = (span.window_type, movie_id, span.start_ms)
            bucket = self._open.get(key)
            if bucket is None:
                bucket = _Bucket(movie_id, span.window_type, span.start_ms, span.end_ms)
                self._open[key] = bucket
            already_fired = bucket.main_fired
            bucket.add(heartbeat_id, user_id, watched)
            if already_fired:
                result.firings.append(WindowFiring("late", bucket.row()))
        self._advance(result)

    def _advance(self, result: EngineResult) -> None:
        watermark = self.clock.watermark_ms
        if watermark is None:
            return
        for bucket in self._open.values():
            if not bucket.main_fired and is_main_fire_due(bucket.end_ms, watermark):
                bucket.main_fired = True
                result.firings.append(WindowFiring("main", bucket.row()))
        ready = [
            key
            for key, bucket in self._open.items()
            if is_window_late(bucket.end_ms, watermark, self.spec.allowed_lateness_ms)
        ]
        for key in ready:
            result.closed_windows.append(self._open.pop(key).row())


def _sorted_rows(rows) -> list[WindowRow]:
    return sorted(rows, key=lambda row: (row.window_type, row.window_start_ms, row.movie_id))


def _session_row(payload: dict, event_ts_ms: int, watched_seconds: int) -> dict:
    """Heartbeat đã khử trùng. Không phải một phiên trong ``stg_watch_sessions``."""
    position = int(payload.get("current_position_seconds") or 0)
    return {
        "heartbeat_id": int(payload["heartbeat_id"]),
        "session_id": _session_as_long(payload.get("session_id")),
        "user_id": int(payload["user_id"]),
        "movie_id": int(payload["movie_id"]),
        "event_ts_ms": event_ts_ms,
        "playback_position_seconds": position,
        "watched_delta_seconds": watched_seconds,
        "watch_duration_seconds": watched_seconds,
    }


def _session_as_long(session_id: object) -> int:
    text = str(session_id)
    digits = "".join(character for character in text if character.isdigit())
    if digits:
        return int(digits)
    return abs(hash(text)) % 1_000_000_000
