"""Một heartbeat, một cửa sổ tumbling. Quá cleanup thì một dead letter.

Không còn late observation theo từng cửa sổ. Event còn trong allowed lateness
đi vào aggregate và có thể làm cửa sổ phát lại. Event sau cleanup không vào
cửa sổ.
"""

from __future__ import annotations

from dataclasses import dataclass

from streaming.flink.operators.watermark_assigner import is_window_late, tumbling_start_ms


@dataclass(frozen=True)
class WindowSpec:
    tumbling_ms: int = 60_000
    allowed_lateness_ms: int = 15_000

    @classmethod
    def from_config(cls, config) -> "WindowSpec":
        return cls(
            tumbling_ms=config.tumbling_ms,
            allowed_lateness_ms=config.allowed_lateness_ms,
        )


def tumbling_bounds(event_ts_ms: int, tumbling_ms: int) -> tuple[int, int]:
    start = tumbling_start_ms(event_ts_ms, tumbling_ms)
    return start, start + tumbling_ms


def is_too_late(event_ts_ms: int, watermark_ms: int | None, spec: WindowSpec) -> bool:
    _start, end = tumbling_bounds(event_ts_ms, spec.tumbling_ms)
    return is_window_late(end, watermark_ms, spec.allowed_lateness_ms)


def dead_letter_record(
    payload: dict,
    event_ts_ms: int,
    watermark_ms: int | None,
    tumbling_ms: int,
) -> dict:
    """Một logical dead letter cho heartbeat tới sau cleanup."""
    start, end = tumbling_bounds(event_ts_ms, tumbling_ms)
    record = {
        **payload,
        "record_kind": "dead_letter",
        "reason": "window_past_cleanup",
        "event_ts_ms": event_ts_ms,
        "window_start_ms": start,
        "window_end_ms": end,
    }
    if watermark_ms is not None:
        record["watermark_ms"] = watermark_ms
    return record
