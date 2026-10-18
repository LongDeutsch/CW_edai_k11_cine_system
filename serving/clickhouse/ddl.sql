-- Serving Gold đã publish. Grain lấy từ bảng Iceberg thật, không từ DDL đề xuất
-- có genre, event_date hay trending score.
-- MergeTree vì mỗi lần sync thay cả snapshot. ReplacingMergeTree không cần.

CREATE DATABASE IF NOT EXISTS __DATABASE__;

CREATE TABLE IF NOT EXISTS __DATABASE__.movie_engagement (
    session_sk Int64,
    session_date_key Int32,
    user_id Int64,
    subscription_tier Nullable(String),
    movie_id Int64,
    title Nullable(String),
    content_rating Nullable(String),
    watch_duration_seconds Nullable(Int32),
    completion_rate Nullable(Float64),
    device_type Nullable(String),
    iceberg_snapshot_id Int64,
    batch_id String
) ENGINE = MergeTree
ORDER BY (movie_id, session_date_key, session_sk);

CREATE TABLE IF NOT EXISTS __DATABASE__.movie_engagement_stage AS __DATABASE__.movie_engagement;

CREATE TABLE IF NOT EXISTS __DATABASE__.movie_ratings (
    movie_id Int64,
    title Nullable(String),
    content_rating Nullable(String),
    rating_count UInt64,
    avg_rating Float64,
    iceberg_snapshot_id Int64,
    batch_id String
) ENGINE = MergeTree
ORDER BY (movie_id);

CREATE TABLE IF NOT EXISTS __DATABASE__.movie_ratings_stage AS __DATABASE__.movie_ratings;

-- Cửa sổ Flink. Một dòng logic cho (movie_id, window_start) sau equality-delete.
-- MergeTree và thay cả snapshot: ReplacingMergeTree không xóa khóa đã biến mất
-- cho đến khi merge, và FINAL không phải trạng thái vừa sync.

CREATE TABLE IF NOT EXISTS __DATABASE__.movie_windows (
    movie_id Int64,
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    total_watch_seconds Int64,
    heartbeat_count Int64,
    active_users Int64,
    iceberg_snapshot_id Int64
) ENGINE = MergeTree
ORDER BY (movie_id, window_start);

CREATE TABLE IF NOT EXISTS __DATABASE__.movie_windows_stage AS __DATABASE__.movie_windows;

CREATE TABLE IF NOT EXISTS __DATABASE__.trending_worker_status (
    source_table String,
    status String,
    iceberg_snapshot_id Nullable(Int64),
    window_rows UInt64,
    latest_window_end Nullable(DateTime64(3, 'UTC')),
    polled_at DateTime64(3, 'UTC'),
    detail Nullable(String)
) ENGINE = MergeTree
ORDER BY (source_table, polled_at);

CREATE TABLE IF NOT EXISTS __DATABASE__.trending_sync_log (
    iceberg_snapshot_id Int64,
    source_table String,
    window_rows UInt64,
    latest_window_end Nullable(DateTime64(3, 'UTC')),
    synced_at DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (synced_at, iceberg_snapshot_id);

CREATE TABLE IF NOT EXISTS __DATABASE__.sync_log (
    batch_id String,
    gold_namespace String,
    marker_namespace String,
    dim_movies_snapshot Int64,
    fact_ratings_snapshot Int64,
    obt_snapshot Int64,
    engagement_rows UInt64,
    rating_rows UInt64,
    published_at DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (published_at, batch_id);
