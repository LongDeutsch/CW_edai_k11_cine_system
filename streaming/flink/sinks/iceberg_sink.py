"""Iceberg REST catalog trên MinIO. Warehouse ``s3a://lakehouse/``.

Grain các bảng streaming ghi ra:

- ``stg_playback_heartbeats``: một heartbeat đã khử trùng ``heartbeat_id``.
  ``playback_position_seconds`` là vị trí seek. ``watch_duration_seconds``
  bằng ``watched_delta_seconds`` (ước lượng một nhịp), không phải vị trí.
- ``stg_watch_sessions``: một phiên xem của batch Spark. Không phải heartbeat.
  Job streaming không ghi vào bảng này.
- ``stg_playback_windows``: một aggregate theo ``(movie_id, window_type, window_start)``.
  ``total_watch_seconds`` là tổng delta của heartbeat còn trong cửa sổ.

Heartbeat vẫn nằm trong bảng heartbeat khi một cửa sổ đã cleanup mà cửa sổ khác
còn nhận, vì nhánh heartbeat tách trước phép loại của từng cửa sổ.
"""

from __future__ import annotations

from streaming.flink.config import StreamJobConfig


def catalog_ddl(config: StreamJobConfig) -> str:
    warehouse = config.warehouse if config.warehouse.endswith("/") else config.warehouse + "/"
    return f"""
CREATE CATALOG {config.catalog} WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = '{config.iceberg_rest_uri}',
  'warehouse' = '{warehouse}',
  'io-impl' = 'org.apache.iceberg.hadoop.HadoopFileIO',
  's3.endpoint' = '{config.s3_endpoint}',
  's3.path-style-access' = 'true',
  's3.access-key-id' = '{config.s3_access_key}',
  's3.secret-access-key' = '{config.s3_secret_key}'
)
""".strip()


def heartbeat_table_ddl(config: StreamJobConfig) -> str:
    """Bảng heartbeat. Không sửa schema ``stg_watch_sessions``."""
    return f"""
CREATE TABLE IF NOT EXISTS {config.silver_sessions_table} (
  heartbeat_id BIGINT,
  session_id BIGINT,
  user_id BIGINT,
  movie_id BIGINT,
  event_ts TIMESTAMP(6),
  playback_position_seconds INT,
  watched_delta_seconds INT,
  watch_duration_seconds INT,
  event_date DATE,
  dt DATE
) WITH (
  'format-version' = '2'
)
""".strip()


def windows_table_ddl(config: StreamJobConfig) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {config.silver_windows_table} (
  movie_id BIGINT,
  window_type STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  total_watch_seconds BIGINT,
  heartbeat_count BIGINT,
  active_users BIGINT,
  PRIMARY KEY (movie_id, window_type, window_start) NOT ENFORCED
) WITH (
  'format-version' = '2',
  'write.upsert.enabled' = 'true'
)
""".strip()


def heartbeat_insert_sql(config: StreamJobConfig, view_name: str = "deduped_heartbeats") -> str:
    """Append heartbeat. ``watch_duration_seconds`` là delta, không phải vị trí seek."""
    return f"""
INSERT INTO {config.silver_sessions_table} (
  heartbeat_id, session_id, user_id, movie_id, event_ts,
  playback_position_seconds, watched_delta_seconds, watch_duration_seconds,
  event_date, dt
)
SELECT
  heartbeat_id,
  session_id,
  user_id,
  movie_id,
  CAST(TO_TIMESTAMP_LTZ(event_ts_ms, 3) AS TIMESTAMP(6)) AS event_ts,
  CAST(playback_position_seconds AS INT) AS playback_position_seconds,
  CAST(watched_delta_seconds AS INT) AS watched_delta_seconds,
  CAST(watched_delta_seconds AS INT) AS watch_duration_seconds,
  CAST(TO_TIMESTAMP_LTZ(event_ts_ms, 3) AS DATE) AS event_date,
  CAST(TO_TIMESTAMP_LTZ(event_ts_ms, 3) AS DATE) AS dt
FROM {view_name}
""".strip()


def apply_s3a(table_env, config: StreamJobConfig) -> None:
    """HadoopFileIO của Flink đọc MinIO path-style qua cấu hình s3a."""
    configuration = table_env.get_config().get_configuration()
    configuration.set_string("fs.s3a.endpoint", config.s3_endpoint)
    configuration.set_string("fs.s3a.path.style.access", "true")
    configuration.set_string("fs.s3a.access.key", config.s3_access_key)
    configuration.set_string("fs.s3a.secret.key", config.s3_secret_key)
    configuration.set_string("fs.s3a.connection.ssl.enabled", "false")
    configuration.set_string("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
