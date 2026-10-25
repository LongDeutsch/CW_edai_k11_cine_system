"""Ghi Parquet cho Feast từ một snapshot đã chọn.

File được ghi đè, không append. Snapshot id và batch id nằm trong snapshot.json.
Hàm này không mở catalog production và không ghi Iceberg.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

PROTECTED = frozenset({"bronze", "silver", "gold"})

USER_TABLE = "feat_user_movie_stats_daily"
MOVIE_TABLE = "feat_movie_engagement_stats_daily"
USER_COLUMNS = (
    "user_id",
    "event_timestamp",
    "created_timestamp",
    "user_total_ratings_90d",
    "user_avg_rating_90d",
    "user_favorite_genre",
    "user_total_watch_hours_30d",
)
MOVIE_COLUMNS = (
    "movie_id",
    "event_timestamp",
    "created_timestamp",
    "movie_rating_count_30d",
    "movie_avg_rating_30d",
    "movie_completion_rate_30d",
    "movie_trending_score_7d",
)
COLUMNS = {USER_TABLE: USER_COLUMNS, MOVIE_TABLE: MOVIE_COLUMNS}


def assert_isolated(namespace: str, warehouse: str) -> None:
    if namespace in PROTECTED:
        raise RuntimeError(f"không export từ namespace {namespace}")
    if str(warehouse).startswith("s3"):
        raise RuntimeError("không export từ warehouse s3")


def export_frames(directory: Path, tables: dict[str, pd.DataFrame], metadata: dict) -> dict:
    """Ghi đè Parquet và snapshot.json. Schema thiếu cột thì không ghi file."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    counts = {}
    prepared = {}
    for name, frame in tables.items():
        expected = COLUMNS[name]
        missing = [column for column in expected if column not in frame.columns]
        if missing:
            raise ValueError(f"{name} thiếu cột {missing}")
        prepared[name] = frame.loc[:, list(expected)].copy()
        counts[name] = int(len(prepared[name]))
    for name, frame in prepared.items():
        frame.to_parquet(directory / f"{name}.parquet", index=False)
    document = {
        **metadata,
        "format": "parquet",
        "rows": counts,
        "snapshot_ids": {key: str(value) for key, value in metadata.get("snapshot_ids", {}).items()},
    }
    (directory / "snapshot.json").write_text(json.dumps(document, indent=2, default=str) + "\n")
    return document
