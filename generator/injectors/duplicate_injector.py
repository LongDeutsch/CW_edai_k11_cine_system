"""Tiêm duplicate offline cho ``ratings`` và ``watch_history``.

Đặc tả mục 4.3: một dòng hợp lệ được nhân thêm một bản sao cùng khóa
nghiệp vụ, nhưng ``id`` mới và timestamp lệch vài giây (mặc định +3).
Một phần bản sao bị đẩy sang partition ngày kế tiếp để giả lập retry
sau khi rớt mạng (client ghi lại sang batch hôm sau).

``--duplicate-rate`` là tỷ lệ bản sao trên **tổng số dòng sau khi tiêm**:

    n_extra / (n_base + n_extra) = rate
    n_extra = round(rate / (1 - rate) * n_base)

Mỗi dòng gốc bị chọn nhiều nhất một lần. Ví dụ rate = 0.025 cho
đúng 2.5% dòng trong file Parquet là bản sao được tiêm.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np


@dataclass(frozen=True)
class DuplicateInjectionResult:
    """Bảng sau tiêm và số liệu để in log kiểm chứng."""

    columns: dict[str, np.ndarray]
    n_base: int
    n_extra: int
    n_next_partition: int
    example: dict[str, object]

    @property
    def n_total(self) -> int:
        return self.n_base + self.n_extra

    @property
    def actual_rate(self) -> float:
        if self.n_total == 0:
            return 0.0
        return self.n_extra / self.n_total


def inject_duplicates(
    columns: dict[str, np.ndarray],
    *,
    rate: float,
    id_column: str,
    timestamp_columns: Sequence[str],
    rng: np.random.Generator,
    event_date_column: str | None = "event_date",
    timestamp_offset_seconds: int = 3,
    next_partition_rate: float = 0.5,
) -> DuplicateInjectionResult:
    """Nhân bản một phần dòng, giữ khóa nghiệp vụ, đổi id và timestamp.

    Mọi cột không nằm trong ``id_column`` / timestamp được sao nguyên,
    nên ``(user_id, movie_id, rating_val)`` của ratings (và các thước
    đo phiên xem) trùng bản gốc. ``event_date`` được tính lại từ
    timestamp đầu tiên sau khi cộng offset, để bản sao rơi đúng
    partition ``dt=``.
    """
    if not 0 <= rate < 1:
        raise ValueError("duplicate rate phải nằm trong [0, 1)")
    if timestamp_offset_seconds < 0:
        raise ValueError("timestamp_offset_seconds phải >= 0")
    if not 0 <= next_partition_rate <= 1:
        raise ValueError("next_partition_rate phải nằm trong [0, 1]")
    if not timestamp_columns:
        raise ValueError("cần ít nhất một cột timestamp")

    base_ids = np.asarray(columns[id_column])
    n_base = int(base_ids.size)
    if n_base == 0 or rate == 0:
        return DuplicateInjectionResult(columns=columns, n_base=n_base, n_extra=0, n_next_partition=0, example={})

    n_extra = int(round((rate / (1.0 - rate)) * n_base))
    if n_extra <= 0:
        return DuplicateInjectionResult(columns=columns, n_base=n_base, n_extra=0, n_next_partition=0, example={})
    if n_extra > n_base:
        raise ValueError(
            "duplicate rate quá cao để mỗi dòng gốc chỉ được nhân một lần. "
            "Hãy giữ rate < 0.5."
        )

    chosen = rng.choice(n_base, size=n_extra, replace=False)
    offset_seconds = np.full(n_extra, timestamp_offset_seconds, dtype=np.int64)
    if next_partition_rate > 0:
        move_next_day = rng.random(n_extra) < next_partition_rate
        offset_seconds = offset_seconds + move_next_day.astype(np.int64) * 86_400
    delta = offset_seconds.astype("timedelta64[s]")

    extras: dict[str, np.ndarray] = {}
    for key, values in columns.items():
        extras[key] = np.asarray(values)[chosen].copy()

    start_id = int(np.max(base_ids)) + 1
    extras[id_column] = start_id + rng.permutation(n_extra).astype(np.int64, copy=False)
    for ts_column in timestamp_columns:
        extras[ts_column] = np.asarray(columns[ts_column])[chosen] + delta
    if event_date_column is not None:
        primary = extras[timestamp_columns[0]]
        extras[event_date_column] = primary.astype("datetime64[D]")

    merged = {
        key: np.concatenate([np.asarray(columns[key]), extras[key]])
        for key in columns
    }
    same_partition = np.flatnonzero(offset_seconds == timestamp_offset_seconds)
    example_at = int(same_partition[0]) if same_partition.size else 0
    example = _example_pair(
        columns,
        extras,
        source_index=int(chosen[example_at]),
        extra_index=example_at,
        timestamp_columns=timestamp_columns,
        id_column=id_column,
        delta_seconds=int(offset_seconds[example_at]),
    )
    n_next_partition = int(np.count_nonzero(offset_seconds >= 86_400))
    return DuplicateInjectionResult(
        columns=merged,
        n_base=n_base,
        n_extra=n_extra,
        n_next_partition=n_next_partition,
        example=example,
    )


def _example_pair(
    base: dict[str, np.ndarray],
    extras: dict[str, np.ndarray],
    *,
    source_index: int,
    extra_index: int,
    timestamp_columns: Sequence[str],
    id_column: str,
    delta_seconds: int,
) -> dict[str, object]:
    """Một cặp gốc/bản sao để đọc log mà không cần mở Parquet."""
    primary_ts = timestamp_columns[0]
    shown: dict[str, object] = {
        "id_column": id_column,
        "source_id": int(np.asarray(base[id_column])[source_index]),
        "duplicate_id": int(np.asarray(extras[id_column])[extra_index]),
        "source_ts": str(np.asarray(base[primary_ts])[source_index]),
        "duplicate_ts": str(np.asarray(extras[primary_ts])[extra_index]),
        "delta_seconds": delta_seconds,
    }
    for key in ("user_id", "movie_id", "rating_val"):
        if key in base:
            value = np.asarray(base[key])[source_index]
            shown[key] = value.item() if hasattr(value, "item") else value
    return shown
