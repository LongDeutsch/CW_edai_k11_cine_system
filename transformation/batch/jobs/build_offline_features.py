"""Tính hai bảng feat_* từ Silver. Không ghi namespace gold nếu chưa được phép.

Cửa sổ feature là [T - lookback, T). Sự kiện tại đúng T thuộc nhãn, không
đi vào feature. ``created_timestamp`` là thời điểm job được gọi, truyền từ
ngoài vào để lần chạy lại cùng input không đổi dòng.

``user_favorite_genre`` chỉ có giá trị khi caller đưa bảng genre. Silver
production không có genre, nên cột đó là null cho đến khi có nguồn genre.
``movie_trending_score_7d`` là tổng giờ xem trong 7 ngày trước T, tính từ
``watch_duration_seconds``. Không đọc bảng playback streaming.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from transformation.batch.common import build_parser, start_spark
from transformation.batch.contracts.validators import check_identifier
from transformation.batch.jobs.lakehouse_ddl import execute_zone
from transformation.batch.jobs.table_updates import create_or_overwrite

USER_TABLE = "feat_user_movie_stats_daily"
MOVIE_TABLE = "feat_movie_engagement_stats_daily"
_PROTECTED = {"bronze", "silver", "gold"}


def assemble_features(spark, ratings, watches, as_of: list[datetime], created_timestamp: datetime, genres=None):
    """Trả (user_features, movie_features) đúng cột DDL Gold."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructField, StructType, TimestampType

    if not as_of:
        raise ValueError("cần ít nhất một event_timestamp")
    as_of_frame = spark.createDataFrame(
        [(ts,) for ts in sorted(set(as_of))],
        StructType([StructField("event_timestamp", TimestampType(), False)]),
    )
    created = spark.createDataFrame(
        [(created_timestamp,)],
        StructType([StructField("created_timestamp", TimestampType(), False)]),
    )
    ratings.select(
        F.col("user_id").cast("long").alias("user_id"),
        F.col("movie_id").cast("long").alias("movie_id"),
        F.col("rating_val").cast("double").alias("rating_val"),
        F.col("rating_ts").cast("timestamp").alias("rating_ts"),
    ).createOrReplaceTempView("_feat_ratings")
    watches.select(
        F.col("user_id").cast("long").alias("user_id"),
        F.col("movie_id").cast("long").alias("movie_id"),
        F.col("start_ts").cast("timestamp").alias("start_ts"),
        F.col("watch_duration_seconds").cast("long").alias("watch_duration_seconds"),
        F.col("completion_rate").cast("double").alias("completion_rate"),
    ).createOrReplaceTempView("_feat_watches")
    as_of_frame.createOrReplaceTempView("_feat_as_of")
    created.createOrReplaceTempView("_feat_created")
    if genres is None:
        spark.sql("SELECT CAST(NULL AS BIGINT) AS movie_id, CAST(NULL AS STRING) AS genre_name WHERE 1 = 0").createOrReplaceTempView(
            "_feat_genres"
        )
    else:
        genres.select(
            F.col("movie_id").cast("long").alias("movie_id"),
            F.col("genre_name").cast("string").alias("genre_name"),
        ).createOrReplaceTempView("_feat_genres")
    users = spark.sql(
        """
        WITH rating_agg AS (
            SELECT a.event_timestamp, r.user_id,
                   COUNT(*) AS user_total_ratings_90d,
                   AVG(r.rating_val) AS user_avg_rating_90d
            FROM _feat_as_of a
            JOIN _feat_ratings r
              ON r.rating_ts < a.event_timestamp
             AND r.rating_ts >= a.event_timestamp - INTERVAL 90 DAYS
            WHERE r.user_id IS NOT NULL AND r.movie_id IS NOT NULL
              AND r.rating_ts IS NOT NULL AND r.rating_val IS NOT NULL
            GROUP BY a.event_timestamp, r.user_id
        ),
        watch_agg AS (
            SELECT a.event_timestamp, w.user_id,
                   SUM(w.watch_duration_seconds) / 3600.0 AS user_total_watch_hours_30d
            FROM _feat_as_of a
            JOIN _feat_watches w
              ON w.start_ts < a.event_timestamp
             AND w.start_ts >= a.event_timestamp - INTERVAL 30 DAYS
            WHERE w.user_id IS NOT NULL AND w.start_ts IS NOT NULL
              AND w.watch_duration_seconds IS NOT NULL
            GROUP BY a.event_timestamp, w.user_id
        ),
        genre_counts AS (
            SELECT a.event_timestamp, r.user_id, g.genre_name, COUNT(*) AS votes
            FROM _feat_as_of a
            JOIN _feat_ratings r
              ON r.rating_ts < a.event_timestamp
             AND r.rating_ts >= a.event_timestamp - INTERVAL 90 DAYS
            JOIN _feat_genres g ON g.movie_id = r.movie_id
            WHERE g.genre_name IS NOT NULL
            GROUP BY a.event_timestamp, r.user_id, g.genre_name
        ),
        genre_ranked AS (
            SELECT event_timestamp, user_id, genre_name, votes,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_timestamp, user_id
                       ORDER BY votes DESC, genre_name ASC
                   ) AS rn
            FROM genre_counts
        ),
        entities AS (
            SELECT event_timestamp, user_id FROM rating_agg
            UNION
            SELECT event_timestamp, user_id FROM watch_agg
        )
        SELECT
            CAST(e.user_id AS BIGINT) AS user_id,
            e.event_timestamp AS event_timestamp,
            c.created_timestamp AS created_timestamp,
            CAST(COALESCE(r.user_total_ratings_90d, 0) AS BIGINT) AS user_total_ratings_90d,
            r.user_avg_rating_90d AS user_avg_rating_90d,
            gr.genre_name AS user_favorite_genre,
            CAST(COALESCE(w.user_total_watch_hours_30d, 0.0) AS DOUBLE) AS user_total_watch_hours_30d
        FROM entities e
        CROSS JOIN _feat_created c
        LEFT JOIN rating_agg r
          ON r.event_timestamp = e.event_timestamp AND r.user_id = e.user_id
        LEFT JOIN watch_agg w
          ON w.event_timestamp = e.event_timestamp AND w.user_id = e.user_id
        LEFT JOIN genre_ranked gr
          ON gr.event_timestamp = e.event_timestamp AND gr.user_id = e.user_id AND gr.rn = 1
        """
    )
    movies = spark.sql(
        """
        WITH rating_agg AS (
            SELECT a.event_timestamp, r.movie_id,
                   COUNT(*) AS movie_rating_count_30d,
                   AVG(r.rating_val) AS movie_avg_rating_30d
            FROM _feat_as_of a
            JOIN _feat_ratings r
              ON r.rating_ts < a.event_timestamp
             AND r.rating_ts >= a.event_timestamp - INTERVAL 30 DAYS
            WHERE r.movie_id IS NOT NULL AND r.rating_ts IS NOT NULL AND r.rating_val IS NOT NULL
            GROUP BY a.event_timestamp, r.movie_id
        ),
        watch_agg AS (
            SELECT a.event_timestamp, w.movie_id,
                   AVG(w.completion_rate) AS movie_completion_rate_30d
            FROM _feat_as_of a
            JOIN _feat_watches w
              ON w.start_ts < a.event_timestamp
             AND w.start_ts >= a.event_timestamp - INTERVAL 30 DAYS
            WHERE w.movie_id IS NOT NULL AND w.start_ts IS NOT NULL
              AND w.completion_rate IS NOT NULL
            GROUP BY a.event_timestamp, w.movie_id
        ),
        trend_agg AS (
            SELECT a.event_timestamp, w.movie_id,
                   SUM(w.watch_duration_seconds) / 3600.0 AS movie_trending_score_7d
            FROM _feat_as_of a
            JOIN _feat_watches w
              ON w.start_ts < a.event_timestamp
             AND w.start_ts >= a.event_timestamp - INTERVAL 7 DAYS
            WHERE w.movie_id IS NOT NULL AND w.start_ts IS NOT NULL
              AND w.watch_duration_seconds IS NOT NULL
            GROUP BY a.event_timestamp, w.movie_id
        ),
        entities AS (
            SELECT event_timestamp, movie_id FROM rating_agg
            UNION
            SELECT event_timestamp, movie_id FROM watch_agg
        )
        SELECT
            CAST(e.movie_id AS BIGINT) AS movie_id,
            e.event_timestamp AS event_timestamp,
            c.created_timestamp AS created_timestamp,
            CAST(COALESCE(r.movie_rating_count_30d, 0) AS BIGINT) AS movie_rating_count_30d,
            r.movie_avg_rating_30d AS movie_avg_rating_30d,
            w.movie_completion_rate_30d AS movie_completion_rate_30d,
            CAST(COALESCE(t.movie_trending_score_7d, 0.0) AS DOUBLE) AS movie_trending_score_7d
        FROM entities e
        CROSS JOIN _feat_created c
        LEFT JOIN rating_agg r
          ON r.event_timestamp = e.event_timestamp AND r.movie_id = e.movie_id
        LEFT JOIN watch_agg w
          ON w.event_timestamp = e.event_timestamp AND w.movie_id = e.movie_id
        LEFT JOIN trend_agg t
          ON t.event_timestamp = e.event_timestamp AND t.movie_id = e.movie_id
        """
    )
    return users, movies


def validate_frames(users, movies) -> dict:
    """Khóa không null, không trùng, count 0 thì average null."""
    reasons = []
    user_rows = users.count()
    movie_rows = movies.count()
    if user_rows == 0 or movie_rows == 0:
        reasons.append("feature rỗng")
    user_distinct = users.select("user_id", "event_timestamp").distinct().count()
    movie_distinct = movies.select("movie_id", "event_timestamp").distinct().count()
    if user_distinct != user_rows:
        reasons.append("trùng (user_id, event_timestamp)")
    if movie_distinct != movie_rows:
        reasons.append("trùng (movie_id, event_timestamp)")
    user_nulls = users.filter(
        "user_id IS NULL OR event_timestamp IS NULL OR created_timestamp IS NULL "
        "OR user_total_ratings_90d IS NULL OR user_total_watch_hours_30d IS NULL"
    ).count()
    movie_nulls = movies.filter(
        "movie_id IS NULL OR event_timestamp IS NULL OR created_timestamp IS NULL "
        "OR movie_rating_count_30d IS NULL OR movie_trending_score_7d IS NULL"
    ).count()
    if user_nulls or movie_nulls:
        reasons.append("khóa hoặc count bị null")
    bad_user_avg = users.filter(
        "(user_total_ratings_90d = 0 AND user_avg_rating_90d IS NOT NULL) "
        "OR (user_total_ratings_90d > 0 AND user_avg_rating_90d IS NULL)"
    ).count()
    bad_movie_avg = movies.filter(
        "(movie_rating_count_30d = 0 AND movie_avg_rating_30d IS NOT NULL) "
        "OR (movie_rating_count_30d > 0 AND movie_avg_rating_30d IS NULL)"
    ).count()
    if bad_user_avg or bad_movie_avg:
        reasons.append("average không khớp count")
    return {
        "ready": not reasons,
        "reasons": reasons,
        "user_rows": user_rows,
        "user_distinct": user_distinct,
        "movie_rows": movie_rows,
        "movie_distinct": movie_distinct,
    }


def publish_features(
    spark,
    silver_namespace: str,
    gold_namespace: str,
    as_of: list[datetime],
    created_timestamp: datetime,
    *,
    allow_production: bool = False,
) -> dict:
    """Đọc Silver đã có và ghi đè feat_* khi dòng đổi. Lần chạy cùng input giữ snapshot."""
    check_identifier(silver_namespace, "silver_namespace")
    check_identifier(gold_namespace, "gold_namespace")
    if gold_namespace in _PROTECTED and not allow_production:
        raise RuntimeError(f"không ghi namespace {gold_namespace} khi chưa allow production")
    ratings = spark.table(f"cinestream.{silver_namespace}.stg_ratings")
    watches = spark.table(f"cinestream.{silver_namespace}.stg_watch_sessions")
    genre_table = f"cinestream.{silver_namespace}.stg_movie_genres"
    genres = spark.table(genre_table) if spark.catalog.tableExists(genre_table) else None
    users, movies = assemble_features(spark, ratings, watches, as_of, created_timestamp, genres)
    report = validate_frames(users, movies)
    if not report["ready"]:
        report["user_write"] = None
        report["movie_write"] = None
        return report
    execute_zone(spark, "gold", gold_namespace)
    catalog = "cinestream"
    report["user_write"] = create_or_overwrite(
        spark,
        users,
        f"{catalog}.{gold_namespace}.{USER_TABLE}",
        required=["user_id", "event_timestamp", "created_timestamp"],
    )
    report["movie_write"] = create_or_overwrite(
        spark,
        movies,
        f"{catalog}.{gold_namespace}.{MOVIE_TABLE}",
        required=["movie_id", "event_timestamp", "created_timestamp"],
    )
    return report


def _parse_instant(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def main(argv: list[str] | None = None) -> None:
    parser = build_parser("offline feature tables")
    parser.add_argument("--warehouse", default="s3a://lakehouse")
    parser.add_argument("--silver-namespace", required=True)
    parser.add_argument("--gold-namespace", required=True)
    parser.add_argument("--as-of", required=True, help="Một hoặc nhiều mốc ISO-8601, cách nhau bởi dấu phẩy.")
    parser.add_argument("--created-timestamp", required=True)
    parser.add_argument("--allow-production", action="store_true")
    args = parser.parse_args(argv)
    spark = start_spark("cinestream-offline-features", args)
    try:
        report = publish_features(
            spark,
            args.silver_namespace,
            args.gold_namespace,
            [_parse_instant(item) for item in args.as_of.split(",") if item.strip()],
            _parse_instant(args.created_timestamp),
            allow_production=args.allow_production,
        )
    finally:
        spark.stop()
    print("RESULT_JSON=" + json.dumps(report))
    if not report["ready"]:
        raise SystemExit("feature không đạt: " + "; ".join(report["reasons"]))


if __name__ == "__main__":
    main()
