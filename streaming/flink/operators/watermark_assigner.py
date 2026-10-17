"""Event-time theo Flink BoundedOutOfOrderness và biên cửa sổ milli-giây.

Watermark chu kỳ của Flink là ``max_ts - out_of_orderness - 1``
(``BoundedOutOfOrdernessWatermarks.onPeriodicEmit``). Cửa sổ ``[start, end)``
có ``maxTimestamp = end - 1``. Main firing khi watermark >= maxTimestamp.
Cleanup khi watermark >= maxTimestamp + allowed lateness. Event chỉ bị loại
khỏi một cửa sổ sau cleanup của cửa sổ đó, không phải vì timestamp nhỏ hơn
watermark hiện tại.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def extract_event_time_ms(payload: dict) -> int:
    """Đọc ``event_ts`` (ISO-8601 hoặc epoch milli) thành epoch milli UTC."""
    raw = payload.get("event_ts", payload.get("event_timestamp"))
    if raw is None:
        raise ValueError("thiếu event_ts")
    if isinstance(raw, (int, float)):
        value = int(raw)
        return value if value > 10_000_000_000 else value * 1000
    text = str(raw).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def flink_watermark_ms(max_event_ts_ms: int, out_of_orderness_ms: int) -> int:
    """Watermark Flink phát định kỳ sau khi đã thấy ``max_event_ts_ms``."""
    return max_event_ts_ms - out_of_orderness_ms - 1


def dlq_watermark_ms(
    live_watermark_ms: int | None,
    restored_max_event_ts_ms: int | None,
    out_of_orderness_ms: int,
) -> int | None:
    """Watermark để phân loại DLQ.

    ``InternalTimersSnapshot`` không lưu ``currentWatermark``.
    ``TimestampsAndWatermarksOperator`` cũng không checkpoint max timestamp.
    Record replay sau restore tới khi watermark vẫn là ``Long.MIN_VALUE``.
    Nếu checkpoint đã có max event timestamp, dùng đúng công thức watermark
    chu kỳ. Watermark sống (kể cả giá trị âm hợp lệ) giữ nguyên.
    """
    if live_watermark_ms is not None and live_watermark_ms >= -1 << 62:
        return live_watermark_ms
    if restored_max_event_ts_ms is None:
        return None
    return flink_watermark_ms(int(restored_max_event_ts_ms), out_of_orderness_ms)


def window_start_ms(timestamp_ms: int, size_ms: int, offset_ms: int = 0) -> int:
    """``TimeWindow.getWindowStartWithOffset`` với timestamp không âm."""
    if size_ms <= 0:
        raise ValueError("size cửa sổ phải > 0")
    if timestamp_ms < 0:
        raise ValueError("timestamp cửa sổ phải >= 0")
    return timestamp_ms - (timestamp_ms - offset_ms + size_ms) % size_ms


def tumbling_start_ms(event_ts_ms: int, size_ms: int) -> int:
    return window_start_ms(event_ts_ms, size_ms)


def sliding_starts_ms(event_ts_ms: int, size_ms: int, slide_ms: int) -> list[int]:
    """Mọi start của cửa sổ trượt chứa ``event_ts``, cùng công thức Flink."""
    if slide_ms <= 0 or size_ms <= 0:
        raise ValueError("size và slide phải > 0")
    last = window_start_ms(event_ts_ms, slide_ms)
    starts: list[int] = []
    start = last
    while start > event_ts_ms - size_ms:
        starts.append(start)
        start -= slide_ms
    return sorted(starts)


def window_max_timestamp_ms(end_ms: int) -> int:
    return end_ms - 1


def cleanup_time_ms(end_ms: int, allowed_lateness_ms: int) -> int:
    return window_max_timestamp_ms(end_ms) + allowed_lateness_ms


def is_window_late(end_ms: int, watermark_ms: int | None, allowed_lateness_ms: int) -> bool:
    """Cửa sổ đã cleanup. Event tới sau mốc này không vào aggregate."""
    if watermark_ms is None:
        return False
    return cleanup_time_ms(end_ms, allowed_lateness_ms) <= watermark_ms


def is_main_fire_due(end_ms: int, watermark_ms: int | None) -> bool:
    """EventTimeTrigger đã qua ``window.maxTimestamp``."""
    if watermark_ms is None:
        return False
    return watermark_ms >= window_max_timestamp_ms(end_ms)


@dataclass
class WatermarkClock:
    """Bounded-out-of-orderness. Watermark trả về là mốc trước event hiện tại."""

    out_of_orderness_ms: int
    max_event_ts_ms: int | None = None
    watermark_ms: int | None = None

    def observe(self, event_ts_ms: int) -> int | None:
        previous = self.watermark_ms
        if self.max_event_ts_ms is None or event_ts_ms > self.max_event_ts_ms:
            self.max_event_ts_ms = event_ts_ms
        self.watermark_ms = flink_watermark_ms(self.max_event_ts_ms, self.out_of_orderness_ms)
        return previous
