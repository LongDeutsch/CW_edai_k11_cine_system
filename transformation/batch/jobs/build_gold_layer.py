"""Ghi Silver đã khử trùng và Gold fact, SCD2 dim_users, OBT.

Warehouse ``s3a://lakehouse/`` đi qua Iceberg REST catalog. Mỗi bảng có
``metadata/*.metadata.json`` và ``snap-*.avro``. ``dim_users`` đi qua
``apply_scd2``: năm user đầu đổi gói, dòng cũ đóng và ``user_sk`` mới được mở.

Ví dụ::

    python -m transformation.batch.jobs.build_gold_layer --warehouse s3a://lakehouse
"""

from __future__ import annotations

from datetime import timedelta, timezone
from pathlib import Path

from transformation.batch.common import (
    DEFAULT_WAREHOUSE,
    ICEBERG_CATALOG,
    build_parser,
    dedup_ratings,
    hold_ui,
    require_source,
    start_spark,
)

_NEXT_TIER = {
    "Free": "Standard",
    "Standard": "VIP",
    "VIP": "Premium",
    "Premium": "Free",
}


def run(spark, source, warehouse: str) -> dict[str, int]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType

    from storage.lakehouse.scd.apply_scd2 import apply_scd2, format_table

    movies = spark.read.option("mergeSchema", "true").parquet(str(source / "movies"))
    ratings = dedup_ratings(spark.read.parquet(str(source / "ratings")))
    watch = _dedup_watch(spark.read.parquet(str(source / "watch_history")))

    _store(spark, ratings, warehouse, "silver", "stg_ratings")
    _store(spark, movies, warehouse, "silver", "stg_movies")
    _store(spark, watch, warehouse, "silver", "stg_watch_sessions")

    users = spark.read.parquet(str(source / "users")).orderBy("user_id").collect()
    opened = _to_dim_users(users)
    changes = _tier_changes(opened[:5])
    versioned = apply_scd2(opened, changes)
    print("SCD2 dim_users, 5 user đầu trước khi đổi gói:")
    print(format_table(opened[:5]))
    print("SCD2 dim_users, cùng 5 user sau apply_scd2:")
    print(format_table([row for row in versioned if row.user_id <= 5]))

    dim_users = spark.createDataFrame(_dim_records(versioned), schema=_dim_schema())
    dim_movies = movies.select(
        F.col("movie_id").cast("long").alias("movie_sk"),
        F.col("movie_id").cast("long").alias("movie_id"),
        "title",
        F.col("release_year").cast("int").alias("release_year"),
        "content_rating",
        "hdr_format",
        F.col("created_ts").alias("valid_from_ts"),
        F.lit(None).cast(TimestampType()).alias("valid_to_ts"),
        F.lit(True).alias("is_current"),
    )
    _store(spark, dim_users, warehouse, "gold", "dim_users")
    _store(spark, dim_movies, warehouse, "gold", "dim_movies")

    dim_users.createOrReplaceTempView("dim_users")
    dim_movies.createOrReplaceTempView("dim_movies")
    ratings.createOrReplaceTempView("stg_ratings")
    watch.createOrReplaceTempView("stg_watch_sessions")

    fact_ratings_out, fact_watch_out, obt = assemble_facts(spark, ratings, watch, dim_users, dim_movies)
    _store(spark, fact_ratings_out, warehouse, "gold", "fact_ratings")
    _store(spark, fact_watch_out, warehouse, "gold", "fact_watch_sessions")
    _store(spark, obt, warehouse, "gold", "obt_movie_engagement")

    current_users = dim_users.where("is_current").count()
    distinct_current = dim_users.where("is_current").select("user_id").distinct().count()
    counts = {
        "stg_ratings": ratings.count(),
        "stg_movies": movies.count(),
        "stg_watch_sessions": watch.count(),
        "dim_users": dim_users.count(),
        "dim_users_current": current_users,
        "fact_ratings": fact_ratings_out.count(),
        "fact_watch_sessions": fact_watch_out.count(),
        "obt_movie_engagement": obt.count(),
    }
    print(f"warehouse          : {warehouse}")
    for name, count in counts.items():
        print(f"  {name:<22} {count:,}")
    if current_users != distinct_current:
        raise SystemExit("is_current không còn đúng một dòng cho mỗi user_id")
    if counts["fact_ratings"] != counts["stg_ratings"]:
        raise SystemExit("fact_ratings lệch số dòng so với stg_ratings")
    if counts["obt_movie_engagement"] != counts["fact_watch_sessions"]:
        raise SystemExit("obt_movie_engagement lệch số dòng so với fact_watch_sessions")
    return counts


def assemble_facts(spark, ratings, watch, dim_users, dim_movies):
    """Fact và OBT. User lấy phiên bản đúng thời điểm. Movie lấy bản hiện tại."""
    dim_users.createOrReplaceTempView("dim_users")
    dim_movies.createOrReplaceTempView("dim_movies")
    ratings.createOrReplaceTempView("stg_ratings")
    watch.createOrReplaceTempView("stg_watch_sessions")
    fact_ratings = spark.sql(
        """
        SELECT
            CAST(r.rating_id AS BIGINT) AS rating_sk,
            u.user_sk,
            m.movie_sk,
            CAST(r.rating_val AS DECIMAL(2, 1)) AS rating_val,
            CAST(date_format(r.rating_ts, 'yyyyMMdd') AS INT) AS rating_date_key,
            r.rating_ts AS created_ts
        FROM stg_ratings r
        JOIN dim_users u
          ON r.user_id = u.user_id
         AND r.rating_ts >= u.valid_from_ts
         AND (u.valid_to_ts IS NULL OR r.rating_ts < u.valid_to_ts)
        JOIN dim_movies m
          ON r.movie_id = m.movie_id
         AND m.is_current
        """
    )
    fact_watch = spark.sql(
        """
        SELECT
            CAST(w.session_id AS BIGINT) AS session_sk,
            u.user_sk,
            m.movie_sk,
            w.watch_duration_seconds,
            w.completion_rate,
            w.device_type,
            CAST(date_format(w.start_ts, 'yyyyMMdd') AS INT) AS session_date_key,
            w.user_id,
            u.subscription_tier,
            m.movie_id,
            m.title,
            m.content_rating
        FROM stg_watch_sessions w
        JOIN dim_users u
          ON w.user_id = u.user_id
         AND w.start_ts >= u.valid_from_ts
         AND (u.valid_to_ts IS NULL OR w.start_ts < u.valid_to_ts)
        JOIN dim_movies m
          ON w.movie_id = m.movie_id
         AND m.is_current
        """
    )
    obt = fact_watch.select(
        "session_sk",
        "session_date_key",
        "user_id",
        "subscription_tier",
        "movie_id",
        "title",
        "content_rating",
        "watch_duration_seconds",
        "completion_rate",
        "device_type",
    )
    fact_ratings_out = fact_ratings.select(
        "rating_sk",
        "user_sk",
        "movie_sk",
        "rating_val",
        "rating_date_key",
        "created_ts",
    )
    fact_watch_out = fact_watch.select(
        "session_sk",
        "user_sk",
        "movie_sk",
        "watch_duration_seconds",
        "completion_rate",
        "device_type",
        "session_date_key",
    )
    return fact_ratings_out, fact_watch_out, obt


def _dedup_watch(watch):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("user_id", "movie_id", "watch_duration_seconds").orderBy(
        F.col("start_ts").desc(),
        F.col("session_id").desc(),
    )
    return watch.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def _to_dim_users(rows) -> list:
    from storage.lakehouse.scd.apply_scd2 import DimUser

    opened = []
    for index, row in enumerate(rows, start=1):
        signup = row["signup_ts"]
        if signup.tzinfo is None:
            signup = signup.replace(tzinfo=timezone.utc)
        # Rating và watch bắt đầu 2026-01-01. Signup trễ hơn mốc đó thì
        # kéo valid_from về trước để fact vẫn khớp đúng một phiên bản.
        floor = signup.replace(year=2026, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        valid_from = signup if signup <= floor else floor
        opened.append(
            DimUser(
                user_sk=index,
                user_id=int(row["user_id"]),
                subscription_tier=str(row["subscription_tier"]),
                valid_from_ts=valid_from,
                valid_to_ts=None,
                is_current=True,
                country=str(row["country"]),
            )
        )
    return opened


def _tier_changes(rows) -> list:
    from storage.lakehouse.scd.apply_scd2 import TierChange

    return [
        TierChange(
            user_id=row.user_id,
            subscription_tier=_NEXT_TIER.get(row.subscription_tier, "Premium"),
            change_ts=row.valid_from_ts + timedelta(days=30),
        )
        for row in rows
    ]


def _dim_schema():
    from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("user_sk", LongType(), False),
            StructField("user_id", LongType(), False),
            StructField("country", StringType(), True),
            StructField("subscription_tier", StringType(), False),
            StructField("valid_from_ts", TimestampType(), False),
            StructField("valid_to_ts", TimestampType(), True),
            StructField("is_current", BooleanType(), False),
        ]
    )


def _dim_records(rows) -> list[tuple]:
    records = []
    for row in rows:
        opened = row.valid_from_ts
        closed = row.valid_to_ts
        if opened.tzinfo is not None:
            opened = opened.astimezone(timezone.utc).replace(tzinfo=None)
        if closed is not None and closed.tzinfo is not None:
            closed = closed.astimezone(timezone.utc).replace(tzinfo=None)
        records.append(
            (
                int(row.user_sk),
                int(row.user_id),
                row.country,
                row.subscription_tier,
                opened,
                closed,
                bool(row.is_current),
            )
        )
    return records


def _store(spark, frame, warehouse: str, namespace: str, table: str) -> None:
    if str(warehouse).startswith(("s3a://", "s3://", "file:")):
        from transformation.batch.jobs.table_updates import create_or_overwrite

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{namespace}")
        create_or_overwrite(spark, frame, f"{ICEBERG_CATALOG}.{namespace}.{table}")
        return
    path = Path(warehouse) / namespace / table
    frame.write.mode("overwrite").parquet(str(path))


def main(argv: list[str] | None = None) -> None:
    parser = build_parser("Ghi stg_ratings, stg_movies, dim_users SCD2, fact và OBT.")
    parser.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    args = parser.parse_args(argv)
    source = require_source(args.source_dir)
    warehouse = str(args.warehouse)
    if not warehouse.startswith(("s3a://", "s3://", "file:")):
        path = Path(warehouse).resolve()
        path.mkdir(parents=True, exist_ok=True)
        warehouse = str(path)
        args.warehouse = warehouse
    spark = start_spark("cinestream-gold", args)
    try:
        run(spark, source, warehouse)
        hold_ui(spark, args)
    finally:
        if args.hold_seconds <= 0:
            spark.stop()


if __name__ == "__main__":
    main()
