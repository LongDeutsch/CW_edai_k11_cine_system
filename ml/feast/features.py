"""Feature View offline cho hai bảng feat_* .

View cửa sổ streaming ``movie_minute_playback`` nằm ở
``ml/feast/streaming_features.py``, project và Redis database khác. File này
không khai báo view đó, nên ``feast apply`` của DP4 không đụng key streaming.


Nguồn là Parquet đã export từ một snapshot Iceberg. Feast 0.67 không nhận
snapshot id trên IcebergSource, nên repo này không trỏ REST Catalog.

``ttl`` bằng 0 vì DuckDB/ibis của bản 0.67 bỏ qua hạn dùng khi ttl là giá trị
falsy. Dòng cũ vẫn được chọn nếu đó là event_timestamp mới nhất không vượt
mốc của entity.

``get_historical_features`` mặc định ``filter_by_created_timestamp=False``.
Khi đó created_timestamp chỉ dùng để phá hòa, và một bản ghi có
created_timestamp nằm sau mốc entity vẫn có thể được chọn. ``historical_features``
luôn bật bộ lọc đó.
"""

import os
from datetime import timedelta

import pandas as pd
from feast import Entity, FeatureStore, FeatureView, Field, FileSource, ValueType
from feast.data_format import ParquetFormat
from feast.types import Float64, Int64, String

USER_VIEW = "user_movie_stats"
MOVIE_VIEW = "movie_engagement_stats"
USER_SOURCE = os.environ.get(
    "CINESTREAM_FEAST_USER_PARQUET",
    "data/feat_user_movie_stats_daily.parquet",
)
MOVIE_SOURCE = os.environ.get(
    "CINESTREAM_FEAST_MOVIE_PARQUET",
    "data/feat_movie_engagement_stats_daily.parquet",
)
OWNER = "transformation.batch.jobs.build_offline_features"

USER_FEATURES = [
    f"{USER_VIEW}:user_total_ratings_90d",
    f"{USER_VIEW}:user_avg_rating_90d",
    f"{USER_VIEW}:user_favorite_genre",
    f"{USER_VIEW}:user_total_watch_hours_30d",
]
MOVIE_FEATURES = [
    f"{MOVIE_VIEW}:movie_rating_count_30d",
    f"{MOVIE_VIEW}:movie_avg_rating_30d",
    f"{MOVIE_VIEW}:movie_completion_rate_30d",
    f"{MOVIE_VIEW}:movie_trending_score_7d",
]
ALL_FEATURES = USER_FEATURES + MOVIE_FEATURES

user = Entity(
    name="user",
    join_keys=["user_id"],
    value_type=ValueType.INT64,
    description="Người xem trong feat_user_movie_stats_daily.",
    owner=OWNER,
)
movie = Entity(
    name="movie",
    join_keys=["movie_id"],
    value_type=ValueType.INT64,
    description="Phim trong feat_movie_engagement_stats_daily.",
    owner=OWNER,
)

user_source = FileSource(
    name="feat_user_movie_stats_daily",
    path=USER_SOURCE,
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    file_format=ParquetFormat(),
    description="Export Parquet ghim snapshot của feat_user_movie_stats_daily.",
    owner=OWNER,
)
movie_source = FileSource(
    name="feat_movie_engagement_stats_daily",
    path=MOVIE_SOURCE,
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    file_format=ParquetFormat(),
    description="Export Parquet ghim snapshot của feat_movie_engagement_stats_daily.",
    owner=OWNER,
)

user_stats = FeatureView(
    name=USER_VIEW,
    entities=[user],
    ttl=timedelta(0),
    schema=[
        Field(name="user_total_ratings_90d", dtype=Int64),
        Field(name="user_avg_rating_90d", dtype=Float64),
        Field(name="user_favorite_genre", dtype=String),
        Field(name="user_total_watch_hours_30d", dtype=Float64),
    ],
    source=user_source,
    online=True,
    description="Feature user theo ngày. Owner là job offline feature. Online chỉ có sau materialize Redis.",
    owner=OWNER,
    tags={"store": "offline", "online_store": "redis"},
)
movie_stats = FeatureView(
    name=MOVIE_VIEW,
    entities=[movie],
    ttl=timedelta(0),
    schema=[
        Field(name="movie_rating_count_30d", dtype=Int64),
        Field(name="movie_avg_rating_30d", dtype=Float64),
        Field(name="movie_completion_rate_30d", dtype=Float64),
        Field(name="movie_trending_score_7d", dtype=Float64),
    ],
    source=movie_source,
    online=True,
    description="Feature movie theo ngày. Owner là job offline feature. Online chỉ có sau materialize Redis.",
    owner=OWNER,
    tags={"store": "offline", "online_store": "redis"},
)


def historical_features(store: FeatureStore, entity_df: pd.DataFrame) -> pd.DataFrame:
    """Lấy feature đã có tại hoặc trước event_timestamp của từng dòng entity."""
    job = store.get_historical_features(
        entity_df=entity_df,
        features=ALL_FEATURES,
        filter_by_created_timestamp=True,
    )
    return job.to_df()
