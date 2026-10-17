"""Trễ theo từng cửa sổ, tách khỏi dead-letter toàn cục.

Một heartbeat thuộc một cửa sổ tumbling và nhiều cửa sổ sliding. Cửa sổ đã
qua cleanup thì ghi một late observation. Dead-letter chỉ phát một bản ghi
khi mọi cửa sổ được gán đều đã cleanup. Không giữ state để ghép hai side
output: quyết định chỉ phụ thuộc timestamp và watermark lúc event tới.
"""

from __future__ import annotations

from dataclasses import dataclass

from streaming.flink.operators.watermark_assigner import (
    is_window_late,
    sliding_starts_ms,
    tumbling_start_ms,
)


@dataclass(frozen=True)
class WindowSpec:
    tumbling_ms: int = 60_000
    sliding_size_ms: int = 300_000
    sliding_slide_ms: int = 60_000
    allowed_lateness_ms: int = 15_000

    @classmethod
    def from_config(cls, config) -> "WindowSpec":
        return cls(
            tumbling_ms=config.tumbling_ms,
            sliding_size_ms=config.sliding_size_ms,
            sliding_slide_ms=config.sliding_slide_ms,
            allowed_lateness_ms=config.allowed_lateness_ms,
        )


@dataclass(frozen=True)
class WindowSpan:
    window_type: str
    start_ms: int
    end_ms: int
    late: bool

    def as_dict(self) -> dict[str, int | str]:
        return {
            "window_type": self.window_type,
            "window_start_ms": self.start_ms,
            "window_end_ms": self.end_ms,
        }


@dataclass(frozen=True)
class LateClassification:
    spans: tuple[WindowSpan, ...]

    @property
    def rejected(self) -> tuple[WindowSpan, ...]:
        return tuple(span for span in self.spans if span.late)

    @property
    def accepted(self) -> tuple[WindowSpan, ...]:
        return tuple(span for span in self.spans if not span.late)

    @property
    def global_dead_letter(self) -> bool:
        return bool(self.spans) and not self.accepted


def assign_windows(event_ts_ms: int, spec: WindowSpec) -> tuple[WindowSpan, ...]:
    tumbling = tumbling_start_ms(event_ts_ms, spec.tumbling_ms)
    spans = [
        WindowSpan("tumbling_1m", tumbling, tumbling + spec.tumbling_ms, False),
    ]
    for start in sliding_starts_ms(event_ts_ms, spec.sliding_size_ms, spec.sliding_slide_ms):
        spans.append(WindowSpan("sliding_5m", start, start + spec.sliding_size_ms, False))
    return tuple(spans)


def classify(event_ts_ms: int, watermark_ms: int | None, spec: WindowSpec) -> LateClassification:
    spans = tuple(
        WindowSpan(
            span.window_type,
            span.start_ms,
            span.end_ms,
            is_window_late(span.end_ms, watermark_ms, spec.allowed_lateness_ms),
        )
        for span in assign_windows(event_ts_ms, spec)
    )
    return LateClassification(spans)


def late_records(
    payload: dict,
    event_ts_ms: int,
    watermark_ms: int | None,
    spec: WindowSpec,
) -> list[dict]:
    """Observation cho từng cửa sổ đã cleanup, cộng một dead-letter nếu tất cả đều vậy."""
    decision = classify(event_ts_ms, watermark_ms, spec)
    records: list[dict] = []
    for span in decision.rejected:
        records.append(
            {
                **payload,
                "record_kind": "late_observation",
                "reason": "window_past_cleanup",
                "event_ts_ms": event_ts_ms,
                "watermark_ms": watermark_ms,
                **span.as_dict(),
            }
        )
    if decision.global_dead_letter:
        records.append(
            {
                **payload,
                "record_kind": "dead_letter",
                "reason": "late_for_all_windows",
                "event_ts_ms": event_ts_ms,
                "watermark_ms": watermark_ms,
                "rejected_windows": [span.as_dict() for span in decision.rejected],
            }
        )
    return records
