"""Phân loại cạnh lineage đã có evidence. Không thêm cạnh mới.

data_flow: source ghi dòng hoặc cột của dataset trước vào dataset sau.
control_dependency: dataset sau chỉ dùng snapshot hoặc marker để quyết định, không nhận dòng dữ liệu.
observability: bảng chất lượng, log, trạng thái poll, hoặc endpoint sức khỏe.
api_consumption: endpoint hoặc dashboard đọc bảng nghiệp vụ để trả cho người dùng.
"""

from __future__ import annotations

KINDS = ("data_flow", "control_dependency", "observability", "api_consumption")

_CLASSIFIED: dict[tuple[str, str, str, str], str] = {
    ("minio.users", "iceberg.bronze.raw_users", "transformation/batch/jobs/ingest_to_bronze.py", "raw_users"): "data_flow",
    ("minio.movies", "iceberg.bronze.raw_movies", "transformation/batch/jobs/ingest_to_bronze.py", "raw_movies"): "data_flow",
    ("minio.ratings", "iceberg.bronze.raw_ratings", "transformation/batch/contracts/validators.py", 'table="raw_ratings"'): "data_flow",
    ("minio.watch_history", "iceberg.bronze.raw_watch_history", "transformation/batch/contracts/validators.py", 'table="raw_watch_history"'): "data_flow",
    ("iceberg.bronze.raw_users", "iceberg.bronze.quarantine_records", "transformation/batch/contracts/validators.py", "quarantine_records"): "data_flow",
    ("iceberg.bronze.raw_movies", "iceberg.bronze.quarantine_records", "transformation/batch/contracts/validators.py", "quarantine_records"): "data_flow",
    ("iceberg.bronze.raw_ratings", "iceberg.bronze.quarantine_records", "transformation/batch/contracts/validators.py", "quarantine_records"): "data_flow",
    ("iceberg.bronze.raw_watch_history", "iceberg.bronze.quarantine_records", "transformation/batch/contracts/validators.py", "quarantine_records"): "data_flow",
    ("iceberg.bronze.raw_users", "iceberg.bronze.quality_results", "transformation/batch/contracts/validators.py", "quality_results"): "observability",
    ("iceberg.bronze.raw_movies", "iceberg.bronze.quality_results", "transformation/batch/contracts/validators.py", "quality_results"): "observability",
    ("iceberg.bronze.raw_ratings", "iceberg.bronze.quality_results", "transformation/batch/contracts/validators.py", "quality_results"): "observability",
    ("iceberg.bronze.raw_watch_history", "iceberg.bronze.quality_results", "transformation/batch/contracts/validators.py", "quality_results"): "observability",
    ("iceberg.bronze.raw_users", "iceberg.silver.stg_users", "transformation/batch/jobs/publish_silver_gold.py", "stg_users"): "data_flow",
    ("iceberg.bronze.raw_movies", "iceberg.silver.stg_movies", "transformation/batch/jobs/publish_silver_gold.py", "stg_movies"): "data_flow",
    ("iceberg.bronze.raw_ratings", "iceberg.silver.stg_ratings", "transformation/batch/jobs/publish_silver_gold.py", "stg_ratings"): "data_flow",
    ("iceberg.bronze.raw_watch_history", "iceberg.silver.stg_watch_sessions", "transformation/batch/jobs/publish_silver_gold.py", "stg_watch_sessions"): "data_flow",
    ("iceberg.silver.stg_users", "iceberg.gold.dim_users", "transformation/batch/jobs/publish_silver_gold.py", "dim_users"): "data_flow",
    ("iceberg.silver.stg_movies", "iceberg.gold.dim_movies", "transformation/batch/jobs/publish_silver_gold.py", "dim_movies"): "data_flow",
    ("iceberg.silver.stg_ratings", "iceberg.gold.fact_ratings", "transformation/batch/jobs/build_gold_layer.py", "fact_ratings"): "data_flow",
    ("iceberg.gold.dim_users", "iceberg.gold.fact_ratings", "transformation/batch/jobs/build_gold_layer.py", "dim_users u"): "data_flow",
    ("iceberg.gold.dim_movies", "iceberg.gold.fact_ratings", "transformation/batch/jobs/build_gold_layer.py", "dim_movies m"): "data_flow",
    ("iceberg.silver.stg_ratings", "iceberg.gold.feat_user_movie_stats_daily", "transformation/batch/jobs/build_offline_features.py", "user_total_ratings_90d"): "data_flow",
    ("iceberg.silver.stg_watch_sessions", "iceberg.gold.feat_user_movie_stats_daily", "transformation/batch/jobs/build_offline_features.py", "user_total_watch_hours_30d"): "data_flow",
    ("iceberg.silver.stg_ratings", "iceberg.gold.feat_movie_engagement_stats_daily", "transformation/batch/jobs/build_offline_features.py", "movie_rating_count_30d"): "data_flow",
    ("iceberg.silver.stg_watch_sessions", "iceberg.gold.feat_movie_engagement_stats_daily", "transformation/batch/jobs/build_offline_features.py", "movie_completion_rate_30d"): "data_flow",
    ("iceberg.silver.stg_watch_sessions", "iceberg.gold.fact_watch_sessions", "transformation/batch/jobs/build_gold_layer.py", "fact_watch"): "data_flow",
    ("iceberg.gold.dim_users", "iceberg.gold.fact_watch_sessions", "transformation/batch/jobs/build_gold_layer.py", "subscription_tier"): "data_flow",
    ("iceberg.gold.dim_movies", "iceberg.gold.fact_watch_sessions", "transformation/batch/jobs/build_gold_layer.py", "m.is_current"): "data_flow",
    ("iceberg.silver.stg_watch_sessions", "iceberg.gold.obt_movie_engagement", "transformation/batch/jobs/build_gold_layer.py", "obt = fact_watch.select"): "data_flow",
    ("iceberg.gold.dim_users", "iceberg.gold.obt_movie_engagement", "transformation/batch/jobs/build_gold_layer.py", "subscription_tier"): "data_flow",
    ("iceberg.gold.dim_movies", "iceberg.gold.obt_movie_engagement", "transformation/batch/jobs/build_gold_layer.py", "content_rating"): "data_flow",
    ("iceberg.gold.dim_movies", "minio.orchestration_marker", "orchestration/dags/cinestream_daily_batch_lakehouse.py", "dim_movies"): "control_dependency",
    ("iceberg.gold.fact_ratings", "minio.orchestration_marker", "orchestration/dags/cinestream_daily_batch_lakehouse.py", "fact_ratings"): "control_dependency",
    ("iceberg.gold.obt_movie_engagement", "minio.orchestration_marker", "orchestration/dags/cinestream_daily_batch_lakehouse.py", "obt_movie_engagement"): "control_dependency",
    ("minio.orchestration_marker", "clickhouse.movie_engagement_stage", "serving/clickhouse/sync_gold.py", "read_marker"): "control_dependency",
    ("iceberg.gold.obt_movie_engagement", "clickhouse.movie_engagement_stage", "serving/clickhouse/sync_gold.py", "_engagement_rows"): "data_flow",
    ("minio.orchestration_marker", "clickhouse.movie_ratings_stage", "serving/clickhouse/sync_gold.py", "read_marker"): "control_dependency",
    ("iceberg.gold.fact_ratings", "clickhouse.movie_ratings_stage", "serving/clickhouse/sync_gold.py", "_rating_rows"): "data_flow",
    ("iceberg.gold.dim_movies", "clickhouse.movie_ratings_stage", "serving/clickhouse/sync_gold.py", "dim_movies"): "data_flow",
    ("clickhouse.movie_engagement_stage", "clickhouse.movie_engagement", "serving/clickhouse/sync_gold.py", "RENAME TABLE"): "data_flow",
    ("clickhouse.movie_ratings_stage", "clickhouse.movie_ratings", "serving/clickhouse/sync_gold.py", "RENAME TABLE"): "data_flow",
    ("clickhouse.movie_engagement", "clickhouse.sync_log", "serving/clickhouse/sync_gold.py", "sync_log"): "observability",
    ("clickhouse.movie_ratings", "clickhouse.sync_log", "serving/clickhouse/sync_gold.py", "sync_log"): "observability",
    ("kafka.movie.playback.heartbeat", "iceberg.silver.stg_playback_heartbeats", "streaming/flink/sinks/iceberg_sink.py", "INSERT INTO"): "data_flow",
    ("kafka.movie.playback.heartbeat", "iceberg.silver.stg_playback_windows", "streaming/flink/jobs/heartbeat_window_job.py", "playback_windows"): "data_flow",
    ("kafka.movie.playback.heartbeat", "kafka.movie.playback.deadletter", "streaming/flink/sinks/kafka_dlq_sink.py", "dead_letter"): "data_flow",
    ("iceberg.silver.stg_playback_windows", "clickhouse.movie_windows_stage", "serving/clickhouse/sync_windows.py", "source_table"): "data_flow",
    ("clickhouse.movie_windows_stage", "clickhouse.movie_windows", "serving/clickhouse/sync_windows.py", "movie_windows"): "data_flow",
    ("clickhouse.movie_windows", "clickhouse.trending_sync_log", "serving/clickhouse/sync_windows.py", "trending_sync_log"): "observability",
    ("iceberg.silver.stg_playback_windows", "clickhouse.trending_worker_status", "serving/clickhouse/sync_windows.py", "trending_worker_status"): "observability",
    ("clickhouse.movie_engagement", "api.analytics.overview", "serving/api/app.py", "movie_engagement"): "api_consumption",
    ("clickhouse.movie_ratings", "api.analytics.overview", "serving/api/app.py", "movie_ratings"): "api_consumption",
    ("clickhouse.sync_log", "api.analytics.overview", "serving/api/app.py", "sync_log"): "observability",
    ("clickhouse.movie_ratings", "api.movies.top_rated", "serving/api/app.py", "movie_ratings"): "api_consumption",
    ("clickhouse.movie_engagement", "api.movies.most_watched", "serving/api/app.py", "movie_engagement"): "api_consumption",
    ("clickhouse.movie_windows", "api.movies.trending", "serving/api/app.py", "movie_windows"): "api_consumption",
    ("clickhouse.trending_sync_log", "api.movies.trending", "serving/api/app.py", "trending_sync_log"): "observability",
    ("clickhouse.trending_worker_status", "api.movies.trending", "serving/api/app.py", "trending_worker_status"): "observability",
    ("clickhouse.movie_engagement", "api.serving.status", "serving/api/app.py", "serving/status"): "observability",
    ("clickhouse.movie_ratings", "api.serving.status", "serving/api/app.py", "serving/status"): "observability",
    ("clickhouse.movie_windows", "api.serving.status", "serving/api/app.py", "trending"): "observability",
    ("clickhouse.movie_engagement", "api.ready", "serving/api/app.py", "movie_engagement"): "observability",
    ("clickhouse.movie_ratings", "api.ready", "serving/api/app.py", "movie_ratings"): "observability",
    ("api.analytics.overview", "dashboard.analytics", "serving/dashboard/static/app.js", "/analytics/overview"): "api_consumption",
    ("api.movies.top_rated", "dashboard.analytics", "serving/dashboard/static/app.js", "/movies/top-rated"): "api_consumption",
    ("api.movies.most_watched", "dashboard.analytics", "serving/dashboard/static/app.js", "/movies/most-watched"): "api_consumption",
    ("api.movies.trending", "dashboard.analytics", "serving/dashboard/static/app.js", "/movies/trending"): "api_consumption",
    ("api.serving.status", "dashboard.analytics", "serving/dashboard/static/app.js", "/serving/status"): "api_consumption",
}


def classify_lineage(upstream: str, downstream: str, path: str, token: str) -> str:
    kind = _CLASSIFIED.get((upstream, downstream, path, token))
    if kind not in KINDS:
        raise ValueError(f"không đủ evidence để phân loại {upstream} → {downstream} ({path}: {token})")
    return kind
