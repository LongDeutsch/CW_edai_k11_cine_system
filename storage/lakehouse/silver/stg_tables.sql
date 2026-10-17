-- Silver Zone: đã khử trùng, đã ép kiểu, đã hợp nhất schema evolution.
-- raw_ratings trùng (user_id, movie_id, rating_val) chỉ giữ rating_ts mới nhất.
-- content_rating / hdr_format / audio_spatial luôn có mặt; file Bronze cũ thành NULL.

CREATE NAMESPACE IF NOT EXISTS cinestream.silver;

CREATE TABLE IF NOT EXISTS cinestream.silver.stg_users (
    user_id BIGINT NOT NULL,
    signup_ts TIMESTAMP,
    country STRING,
    subscription_tier STRING,
    preferred_language STRING,
    age_group STRING,
    gender STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.silver.stg_movies (
    movie_id BIGINT NOT NULL,
    title STRING,
    release_year INT,
    runtime_minutes INT,
    original_language STRING,
    budget BIGINT,
    director_id BIGINT,
    created_ts TIMESTAMP,
    content_rating STRING,
    hdr_format STRING,
    audio_spatial BOOLEAN
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.silver.stg_ratings (
    rating_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    movie_id BIGINT NOT NULL,
    rating_val DECIMAL(2, 1) NOT NULL,
    rating_ts TIMESTAMP NOT NULL,
    review_text STRING,
    event_date DATE NOT NULL
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS cinestream.silver.stg_watch_sessions (
    session_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    movie_id BIGINT NOT NULL,
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP,
    watch_duration_seconds INT NOT NULL,
    completion_rate DOUBLE NOT NULL,
    device_type STRING,
    resolution STRING,
    event_date DATE NOT NULL
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);
