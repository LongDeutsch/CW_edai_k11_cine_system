"""Feature View cho cửa sổ playback một phút của Flink.

Grain online là một ``movie_id``. ``event_timestamp`` là ``window_end`` của
cửa sổ đã đóng mới nhất. ``active_users`` là số ``user_id`` khác nhau trong
phút đó, không phải số người đang xem cùng lúc.

Nguồn là Parquet đã đọc từ snapshot Iceberg sau equality-delete. Project và
Redis database tách khỏi hai view batch của DP4.
"""

import os
from datetime import timedelta

import pandas as pd
from feast import Entity, FeatureStore, FeatureView, Field, FileSource, ValueType
from feast.data_format import ParquetFormat
from feast.types import Int64

VIEW = "movie_minute_playback"
SOURCE = os.environ.get(
    "CINESTREAM_FEAST_WINDOW_PARQUET",
    "data/movie_minute_playback.parquet",
)
OWNER = "streaming.flink.jobs.heartbeat_window_job"
FEATURES = [
    f"{VIEW}:heartbeat_count",
    f"{VIEW}:total_watch_seconds",
    f"{VIEW}:active_users",
]

movie = Entity(
    name="movie",
    join_keys=["movie_id"],
    value_type=ValueType.INT64,
    description="Phim của cửa sổ playback. Cùng khóa với feature batch, khác Feature View.",
    owner=OWNER,
)
window_source = FileSource(
    name="movie_minute_playback_source",
    path=SOURCE,
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    file_format=ParquetFormat(),
    description="Lịch sử cửa sổ đã đóng. event_timestamp là window_end.",
    owner=OWNER,
)
minute_playback = FeatureView(
    name=VIEW,
    entities=[movie],
    ttl=timedelta(0),
    schema=[
        Field(name="heartbeat_count", dtype=Int64),
        Field(name="total_watch_seconds", dtype=Int64),
        Field(name="active_users", dtype=Int64, description="Số user khác nhau trong phút. Không phải concurrent viewers."),
    ],
    source=window_source,
    online=True,
    description=(
        "Cửa sổ tumbling 1 phút mới nhất của một phim. "
        "Giá trị online đổi khi có cửa sổ đóng muộn hơn, hoặc khi late firing sửa cùng window_end."
    ),
    owner=OWNER,
    tags={"store": "streaming", "online_store": "redis", "redis_db": "2"},
)


def historical_windows(store: FeatureStore, entity_df: pd.DataFrame) -> pd.DataFrame:
    """Lấy cửa sổ đã publish tại hoặc trước event_timestamp của entity."""
    job = store.get_historical_features(
        entity_df=entity_df,
        features=FEATURES,
        filter_by_created_timestamp=True,
    )
    return job.to_df()
