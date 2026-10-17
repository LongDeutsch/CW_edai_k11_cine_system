-- Bronze Zone: append-only. Writer chỉ INSERT.
-- Mỗi bảng có _ingest_ts và _source_file để truy vết file nguồn.
-- File mục tiêu 128MB được khai báo sẵn; job compaction tôn trọng cùng ngưỡng.

CREATE NAMESPACE IF NOT EXISTS cinestream.bronze;

CREATE TABLE IF NOT EXISTS cinestream.bronze.raw_users (
    user_id BIGINT,
    signup_ts TIMESTAMP,
    country STRING,
    subscription_tier STRING,
    preferred_language STRING,
    age_group STRING,
    gender STRING,
    _ingest_ts TIMESTAMP NOT NULL,
    _source_file STRING
)
USING iceberg
PARTITIONED BY (days(_ingest_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.bronze.raw_movies (
    movie_id BIGINT,
    title STRING,
    release_year INT,
    runtime_minutes INT,
    original_language STRING,
    budget BIGINT,
    director_id BIGINT,
    created_ts TIMESTAMP,
    content_rating STRING,
    hdr_format STRING,
    audio_spatial BOOLEAN,
    _ingest_ts TIMESTAMP NOT NULL,
    _source_file STRING
)
USING iceberg
PARTITIONED BY (days(_ingest_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.bronze.raw_ratings (
    rating_id BIGINT,
    user_id BIGINT,
    movie_id BIGINT,
    rating_val DOUBLE,
    rating_ts TIMESTAMP,
    review_text STRING,
    event_date DATE,
    _ingest_ts TIMESTAMP NOT NULL,
    _source_file STRING
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.bronze.raw_watch_history (
    session_id BIGINT,
    user_id BIGINT,
    movie_id BIGINT,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    watch_duration_seconds INT,
    completion_rate DOUBLE,
    device_type STRING,
    resolution STRING,
    event_date DATE,
    _ingest_ts TIMESTAMP NOT NULL,
    _source_file STRING
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);
