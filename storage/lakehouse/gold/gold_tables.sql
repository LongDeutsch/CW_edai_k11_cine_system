-- Gold Zone: mô hình chiều Kimball, đặc trưng Feast và một bảng phẳng.
-- dim_users / dim_movies là SCD Type 2.
-- Một mốc thời gian t thuộc phiên bản có valid_from_ts <= t và
-- (valid_to_ts IS NULL OR t < valid_to_ts). is_current đánh dấu phiên bản mở.

CREATE NAMESPACE IF NOT EXISTS cinestream.gold;

CREATE TABLE IF NOT EXISTS cinestream.gold.dim_users (
    user_sk BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    country STRING,
    subscription_tier STRING NOT NULL,
    valid_from_ts TIMESTAMP NOT NULL,
    valid_to_ts TIMESTAMP,
    is_current BOOLEAN NOT NULL
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.gold.dim_movies (
    movie_sk BIGINT NOT NULL,
    movie_id BIGINT NOT NULL,
    title STRING,
    release_year INT,
    content_rating STRING,
    hdr_format STRING,
    valid_from_ts TIMESTAMP NOT NULL,
    valid_to_ts TIMESTAMP,
    is_current BOOLEAN NOT NULL
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.gold.fact_ratings (
    rating_sk BIGINT NOT NULL,
    user_sk BIGINT NOT NULL,
    movie_sk BIGINT NOT NULL,
    rating_val DECIMAL(2, 1) NOT NULL,
    rating_date_key INT NOT NULL,
    created_ts TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (rating_date_key)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.gold.fact_watch_sessions (
    session_sk BIGINT NOT NULL,
    user_sk BIGINT NOT NULL,
    movie_sk BIGINT NOT NULL,
    watch_duration_seconds INT NOT NULL,
    completion_rate DOUBLE NOT NULL,
    device_type STRING,
    session_date_key INT NOT NULL
)
USING iceberg
PARTITIONED BY (session_date_key)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.gold.feat_user_movie_stats_daily (
    user_id BIGINT NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    user_total_ratings_90d BIGINT,
    user_avg_rating_90d DOUBLE,
    user_favorite_genre STRING,
    user_total_watch_hours_30d DOUBLE
)
USING iceberg
PARTITIONED BY (days(event_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.gold.feat_movie_engagement_stats_daily (
    movie_id BIGINT NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    movie_rating_count_30d BIGINT,
    movie_avg_rating_30d DOUBLE,
    movie_completion_rate_30d DOUBLE,
    movie_trending_score_7d DOUBLE
)
USING iceberg
PARTITIONED BY (days(event_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.gold.obt_movie_engagement (
    session_sk BIGINT NOT NULL,
    session_date_key INT NOT NULL,
    user_id BIGINT NOT NULL,
    subscription_tier STRING,
    movie_id BIGINT NOT NULL,
    title STRING,
    content_rating STRING,
    watch_duration_seconds INT,
    completion_rate DOUBLE,
    device_type STRING
)
USING iceberg
PARTITIONED BY (session_date_key)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);
