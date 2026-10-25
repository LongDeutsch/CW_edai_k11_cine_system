"""Bài toán dự đoán điểm cao và quy tắc thời gian của feature offline.

Không train model, không gọi Feast hay KServe. Hàm ở đây chỉ định nghĩa
nhãn, cửa sổ và cách chia train/test để job Spark dùng cùng một quy tắc.
"""

from __future__ import annotations

from datetime import datetime, timedelta

POSITIVE_RATING = 4.0
LABEL_WINDOW = timedelta(days=1)
RATING_LOOKBACK = timedelta(days=90)
WATCH_LOOKBACK = timedelta(days=30)
TREND_LOOKBACK = timedelta(days=7)


def is_positive(rating_val: float) -> bool:
    """Nhãn dương khi điểm trong cửa sổ nhãn đạt ngưỡng."""
    return float(rating_val) >= POSITIVE_RATING


def label_high_ratings(
    events: list[dict],
    event_timestamp: datetime,
    horizon: timedelta = LABEL_WINDOW,
) -> list[dict]:
    """Một nhãn cho mỗi (user_id, movie_id) có rating trong [T, T + horizon).

    Rating đúng lúc T thuộc nhãn, không thuộc feature. Nếu có nhiều rating
    trong cửa sổ, lấy rating sớm nhất.
    """
    end = event_timestamp + horizon
    chosen: dict[tuple[int, int], dict] = {}
    for event in events:
        ts = event["rating_ts"]
        if ts < event_timestamp or ts >= end:
            continue
        key = (int(event["user_id"]), int(event["movie_id"]))
        current = chosen.get(key)
        if current is None or ts < current["rating_ts"]:
            chosen[key] = event
    rows = []
    for event in chosen.values():
        rows.append(
            {
                "user_id": int(event["user_id"]),
                "movie_id": int(event["movie_id"]),
                "event_timestamp": event_timestamp,
                "rating_ts": event["rating_ts"],
                "rating_val": float(event["rating_val"]),
                "label": 1 if is_positive(event["rating_val"]) else 0,
            }
        )
    return sorted(rows, key=lambda row: (row["user_id"], row["movie_id"]))


def split_by_time(examples: list[dict], cutoff: datetime) -> tuple[list[dict], list[dict]]:
    """Train là event_timestamp < cutoff. Test là phần còn lại.

    Không xáo dòng. Cùng một user có thể ở cả hai phía nếu các mốc khác nhau.
    """
    train = [row for row in examples if row["event_timestamp"] < cutoff]
    test = [row for row in examples if row["event_timestamp"] >= cutoff]
    return train, test
