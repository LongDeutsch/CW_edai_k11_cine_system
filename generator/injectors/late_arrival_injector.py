"""Tiêm late arrival cho sự kiện ``movie.playback.heartbeat``.

Đặc tả mục 4.4: khoảng 6% sự kiện mô phỏng xem offline có ``event_ts``
lùi 10–35 phút so với đồng hồ producer. Sự kiện đúng giờ chỉ lệch
dưới 2 giây. Producer giữ nhóm muộn rồi xả một lúc khi client nối lại mạng.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np

DEFAULT_LATE_MIN_MINUTES = 10
DEFAULT_LATE_MAX_MINUTES = 35
ON_TIME_MAX_LAG_SECONDS = 2


def message_quota(n_messages: int, ratio: float) -> int:
    """Số message cần mang một đặc tính, làm tròn từ ``ratio * n_messages``."""
    if n_messages < 0:
        raise ValueError("n_messages phải >= 0")
    if not 0 <= ratio <= 1:
        raise ValueError("ratio phải nằm trong [0, 1]")
    return int(round(ratio * n_messages))


def sample_late_delay_seconds(
    rng: np.random.Generator,
    *,
    min_minutes: int = DEFAULT_LATE_MIN_MINUTES,
    max_minutes: int = DEFAULT_LATE_MAX_MINUTES,
) -> int:
    """Số giây lùi ``event_ts``, gồm cả hai mốc 10 phút và 35 phút."""
    if min_minutes < 0 or max_minutes < min_minutes:
        raise ValueError("khoảng phút late arrival không hợp lệ")
    low = min_minutes * 60
    high = max_minutes * 60
    return int(rng.integers(low, high + 1))


def sample_on_time_lag_seconds(rng: np.random.Generator) -> int:
    """Độ lệch đúng giờ: 0 hoặc 1 giây, luôn nhỏ hơn 2 giây."""
    return int(rng.integers(0, ON_TIME_MAX_LAG_SECONDS))


def event_timestamp(now: datetime, lag_seconds: int) -> str:
    """``event_ts`` UTC dạng ``YYYY-MM-DDTHH:MM:SSZ``."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    stamp = now.astimezone(timezone.utc) - timedelta(seconds=int(lag_seconds))
    return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")
