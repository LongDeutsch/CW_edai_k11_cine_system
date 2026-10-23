"""Bronze Iceberg → Silver → Gold, MERGE logic cho thực thể và SCD2 cho user.

Mỗi lần publish đọc toàn bộ Bronze của namespace rồi ghi lại phần thay đổi.
Ratings trùng ``(user_id, movie_id, rating_val)`` giữ ``rating_ts`` mới nhất,
kể cả khi bản replay nằm ở ``event_date`` khác. Watch trùng
``(user_id, movie_id, watch_duration_seconds)`` giữ ``start_ts`` mới nhất.
Đó là grain Silver đã có trong job Gold cũ.

SCD2 user đi qua ``apply_scd2``. ``effective_ts`` là mốc hiệu lực của lần đổi gói,
không phải ``_ingest_ts`` và không phải ``event_date``. Lần chạy lại với cùng
mốc và cùng gói không mở thêm phiên bản.

Không chèn một phiên bản vào giữa lịch sử đã đóng. Batch đó bị từ chối trước
khi ghi, nên Silver và Gold giữ snapshot trước đó.

Movie cũng đóng/mở phiên bản khi title, năm, content rating hoặc HDR đổi.
Fact movie lấy bản ``is_current``, đúng join cũ. Fact user lấy bản đang hiệu lực
tại ``rating_ts`` hoặc ``start_ts``. ``rating_sk`` là ``rating_id`` còn lại sau
khử trùng, ``session_sk`` là ``session_id`` còn lại. Cả hai ổn định khi rerun.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import datetime, timezone

from transformation.batch.common import ICEBERG_CATALOG, build_parser, dedup_ratings, start_spark
from transformation.batch.contracts.validators import (
    CONTRACTS,
    DEFAULT_ERROR_RATE_THRESHOLD,
    QualityThresholdExceeded,
    assert_within_threshold,
    replace_quarantine,
    upsert_quality,
    validate_frame,
)
from transformation.batch.jobs.build_gold_layer import (
    _dedup_watch,
    _dim_records,
    _dim_schema,
    _to_dim_users,
    assemble_facts,
)
from transformation.batch.jobs.lakehouse_ddl import execute_zone
from transformation.batch.jobs.table_updates import create_or_overwrite, snapshots, table_exists, take_schema_reports
from storage.lakehouse.scd.apply_scd2 import DimUser, TierChange, apply_scd2, stable_surrogate

_SILVER = {
    "users": "stg_users",
    "movies": "stg_movies",
    "ratings": "stg_ratings",
    "watch": "stg_watch_sessions",
}
_DDL = {
    "stg_users": "storage/lakehouse/silver/stg_tables.sql",
    "stg_movies": "storage/lakehouse/silver/stg_tables.sql",
    "stg_ratings": "storage/lakehouse/silver/stg_tables.sql",
    "stg_watch_sessions": "storage/lakehouse/silver/stg_tables.sql",
    "dim_users": "storage/lakehouse/gold/gold_tables.sql",
    "dim_movies": "storage/lakehouse/gold/gold_tables.sql",
    "fact_ratings": "storage/lakehouse/gold/gold_tables.sql",
    "fact_watch_sessions": "storage/lakehouse/gold/gold_tables.sql",
    "obt_movie_engagement": "storage/lakehouse/gold/gold_tables.sql",
}


def _not_null(table: str) -> list[str]:
    from governance.extract import spark_tables

    return [name for name, kind in spark_tables(_DDL[table])[table] if "NOT NULL" in kind.upper()]


@dataclass(frozen=True)
class MovieVersion:
    movie_sk: int
    movie_id: int
    title: str | None
    release_year: int | None
    content_rating: str | None
    hdr_format: str | None
    valid_from_ts: datetime
    valid_to_ts: datetime | None
    is_current: bool


def publish(
    spark,
    *,
    bronze_namespace: str,
    silver_namespace: str,
    gold_namespace: str,
    effective_ts: datetime,
    batch_id: str = "publish",
    run_id: str = "",
    threshold: float = DEFAULT_ERROR_RATE_THRESHOLD,
) -> dict:
    """Dựng Silver và Gold. ``effective_ts`` phải có timezone hoặc được hiểu là UTC."""
    effective = _aware(effective_ts)
    spark.catalog.clearCache()
    execute_zone(spark, "silver", silver_namespace)
    execute_zone(spark, "gold", gold_namespace)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    staged = _gate(spark, bronze_namespace, batch_id, run_id, threshold, now)
    users = staged["users"]
    movies = staged["movies"]
    ratings = dedup_ratings(staged["ratings"])
    watch = _dedup_watch(staged["watch"])
    dim_rows = plan_users(_load_users(spark, _table(gold_namespace, "dim_users")), users, effective)
    movie_rows = plan_movies(_load_movies(spark, _table(gold_namespace, "dim_movies")), movies, effective)
    dim_users = spark.createDataFrame(_dim_records(dim_rows), schema=_dim_schema()) if dim_rows else _empty_dim_users(spark)
    dim_movies = _movie_frame(spark, movie_rows)
    silver_users = _project_users(users)
    silver_movies = _project_movies(movies)
    silver_ratings = _project_ratings(ratings)
    silver_watch = _project_watch(watch)
    facts, fact_watch, obt = assemble_facts(spark, silver_ratings, silver_watch, dim_users, dim_movies)
    _assert_counts(silver_ratings, silver_watch, facts, fact_watch, obt)
    take_schema_reports()
    writes = {
        name: create_or_overwrite(spark, frame, _table(namespace, name), required=_not_null(name))
        for namespace, name, frame in (
            (silver_namespace, "stg_users", silver_users),
            (silver_namespace, "stg_movies", silver_movies),
            (silver_namespace, "stg_ratings", silver_ratings),
            (silver_namespace, "stg_watch_sessions", silver_watch),
            (gold_namespace, "dim_users", dim_users),
            (gold_namespace, "dim_movies", dim_movies),
            (gold_namespace, "fact_ratings", facts),
            (gold_namespace, "fact_watch_sessions", fact_watch),
            (gold_namespace, "obt_movie_engagement", obt),
        )
    }
    return {
        "effective_ts": effective.isoformat(),
        "writes": writes,
        "schema_reports": take_schema_reports(),
        "snapshots": _snapshot_map(spark, silver_namespace, gold_namespace),
    }


def plan_users(current: list[DimUser], incoming, effective: datetime) -> list[DimUser]:
    """Áp thay đổi gói. Cùng gói thì chỉ sửa country của bản đang mở."""
    rows = list(current)
    for raw in incoming.collect():
        drafted = _to_dim_users(
            [
                {
                    "user_id": int(raw["user_id"]),
                    "signup_ts": _aware(raw["signup_ts"]),
                    "subscription_tier": str(raw["subscription_tier"]),
                    "country": str(raw["country"]),
                }
            ]
        )[0]
        active = next((row for row in rows if row.user_id == drafted.user_id and row.is_current), None)
        if active is None:
            rows.append(replace(drafted, user_sk=stable_surrogate(drafted.user_id, rows)))
            continue
        unchanged = active.subscription_tier == drafted.subscription_tier and active.country == drafted.country
        if unchanged:
            continue
        if effective <= active.valid_from_ts:
            raise ValueError(
                f"user_id={drafted.user_id}: không ghi đè lịch sử, "
                f"effective_ts {effective} không sau valid_from_ts {active.valid_from_ts}"
            )
        if active.subscription_tier != drafted.subscription_tier:
            rows = apply_scd2(
                rows,
                [TierChange(drafted.user_id, drafted.subscription_tier, effective)],
                next_surrogate=stable_surrogate,
            )
            opened = next(row for row in rows if row.user_id == drafted.user_id and row.is_current)
            if opened.country != drafted.country:
                rows = [replace(opened, country=drafted.country) if row.user_sk == opened.user_sk else row for row in rows]
        else:
            rows = [replace(active, country=drafted.country) if row.user_sk == active.user_sk else row for row in rows]
    _assert_user_history(rows)
    return rows


def plan_movies(current: list[MovieVersion], incoming, effective: datetime) -> list[MovieVersion]:
    """Đóng bản movie cũ khi thuộc tính mô tả đổi. Không chèn vào khoảng đã đóng."""
    rows = list(current)
    if incoming.limit(1).count() == 0:
        _assert_movie_history(rows)
        return rows
    for raw in incoming.collect():
        movie_id = int(raw["movie_id"])
        opened_at = _aware(raw["created_ts"]) if raw["created_ts"] is not None else effective
        drafted = MovieVersion(
            movie_sk=0,
            movie_id=movie_id,
            title=raw["title"],
            release_year=None if raw["release_year"] is None else int(raw["release_year"]),
            content_rating=raw["content_rating"] if "content_rating" in raw.asDict() else None,
            hdr_format=raw["hdr_format"] if "hdr_format" in raw.asDict() else None,
            valid_from_ts=opened_at,
            valid_to_ts=None,
            is_current=True,
        )
        active = next((row for row in rows if row.movie_id == movie_id and row.is_current), None)
        if active is None:
            rows.append(replace(drafted, movie_sk=_movie_surrogate(movie_id, rows)))
            continue
        if _movie_attrs(active) == _movie_attrs(drafted):
            continue
        if effective <= active.valid_from_ts:
            raise ValueError(
                f"movie_id={movie_id}: không ghi đè lịch sử, effective_ts {effective} không sau {active.valid_from_ts}"
            )
        rows = [replace(active, is_current=False, valid_to_ts=effective) if row.movie_sk == active.movie_sk else row for row in rows]
        rows.append(
            replace(
                drafted,
                movie_sk=_movie_surrogate(movie_id, rows),
                valid_from_ts=effective,
                valid_to_ts=None,
                is_current=True,
            )
        )
    _assert_movie_history(rows)
    return rows


def _gate(spark, namespace: str, batch_id: str, run_id: str, threshold: float, now) -> dict:
    """Loại dòng lỗi. Vượt ngưỡng thì chưa ghi Silver."""
    users = _latest(_bronze(spark, namespace, "users"), "user_id")
    movies = _latest(_bronze(spark, namespace, "movies"), "movie_id")
    valid_users, _ = _split(spark, namespace, "users", users, None, None, batch_id, run_id, threshold, now)
    valid_movies, _ = _split(spark, namespace, "movies", movies, None, None, batch_id, run_id, threshold, now)
    ratings = _bronze(spark, namespace, "ratings")
    watch = _bronze(spark, namespace, "watch")
    valid_ratings, _ = _split(spark, namespace, "ratings", ratings, valid_users, valid_movies, batch_id, run_id, threshold, now)
    valid_watch, _ = _split(spark, namespace, "watch", watch, valid_users, valid_movies, batch_id, run_id, threshold, now)
    return {"users": valid_users, "movies": valid_movies, "ratings": valid_ratings, "watch": valid_watch}


def _split(spark, namespace, dataset, frame, users, movies, batch_id, run_id, threshold, now):
    contract = CONTRACTS[dataset]
    if frame.limit(1).count() == 0:
        upsert_quality(spark, namespace, dataset, batch_id, run_id, 0, 0, "empty", now)
        return frame, frame.limit(0)
    valid, invalid = validate_frame(frame, contract, users, movies)
    valid = valid.cache()
    invalid = invalid.cache()
    total = valid.count() + invalid.count()
    invalid_rows = invalid.count()
    replace_quarantine(spark, namespace, dataset, batch_id, invalid, now)
    try:
        assert_within_threshold(dataset, invalid_rows, total, threshold)
    except QualityThresholdExceeded:
        upsert_quality(spark, namespace, dataset, batch_id, run_id, total, invalid_rows, "threshold_rejected", now)
        raise
    upsert_quality(spark, namespace, dataset, batch_id, run_id, total, invalid_rows, "accepted", now)
    return valid, invalid


def _bronze(spark, namespace: str, dataset: str):
    contract = CONTRACTS[dataset]
    table = _table(namespace, contract.table)
    if not table_exists(spark, table):
        raise RuntimeError(f"chưa có Bronze {table}. Chạy ingest_to_bronze trước.")
    spark.sql(f"REFRESH TABLE {table}")
    return spark.table(table)


def _latest(frame, key: str):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if "_ingest_ts" not in frame.columns:
        return frame
    window = Window.partitionBy(key).orderBy(F.col("_ingest_ts").desc(), F.col("_file_id").desc())
    return frame.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def _project_users(frame):
    from pyspark.sql import functions as F

    return frame.select(
        F.col("user_id").cast("long").alias("user_id"),
        F.col("signup_ts").cast("timestamp").alias("signup_ts"),
        F.col("country").cast("string").alias("country"),
        F.col("subscription_tier").cast("string").alias("subscription_tier"),
        F.col("preferred_language").cast("string").alias("preferred_language"),
        F.col("age_group").cast("string").alias("age_group"),
        F.col("gender").cast("string").alias("gender"),
    )


def _project_movies(frame):
    from pyspark.sql import functions as F

    def optional(name, dtype):
        if name in frame.columns:
            return F.col(name).cast(dtype).alias(name)
        return F.lit(None).cast(dtype).alias(name)

    return frame.select(
        F.col("movie_id").cast("long").alias("movie_id"),
        F.col("title").cast("string").alias("title"),
        F.col("release_year").cast("int").alias("release_year"),
        F.col("runtime_minutes").cast("int").alias("runtime_minutes"),
        F.col("original_language").cast("string").alias("original_language"),
        F.col("budget").cast("long").alias("budget"),
        F.col("director_id").cast("long").alias("director_id"),
        F.col("created_ts").cast("timestamp").alias("created_ts"),
        optional("content_rating", "string"),
        optional("hdr_format", "string"),
        optional("audio_spatial", "boolean"),
    )


def _project_ratings(frame):
    from pyspark.sql import functions as F

    return frame.select(
        F.col("rating_id").cast("long").alias("rating_id"),
        F.col("user_id").cast("long").alias("user_id"),
        F.col("movie_id").cast("long").alias("movie_id"),
        F.col("rating_val").cast("decimal(2,1)").alias("rating_val"),
        F.col("rating_ts").cast("timestamp").alias("rating_ts"),
        F.col("review_text").cast("string").alias("review_text"),
        F.col("event_date").cast("date").alias("event_date"),
    )


def _project_watch(frame):
    from pyspark.sql import functions as F

    return frame.select(
        F.col("session_id").cast("long").alias("session_id"),
        F.col("user_id").cast("long").alias("user_id"),
        F.col("movie_id").cast("long").alias("movie_id"),
        F.col("start_ts").cast("timestamp").alias("start_ts"),
        F.col("end_ts").cast("timestamp").alias("end_ts"),
        F.col("watch_duration_seconds").cast("int").alias("watch_duration_seconds"),
        F.col("completion_rate").cast("double").alias("completion_rate"),
        F.col("device_type").cast("string").alias("device_type"),
        F.col("resolution").cast("string").alias("resolution"),
        F.col("event_date").cast("date").alias("event_date"),
    )


def _load_users(spark, table: str) -> list[DimUser]:
    if not table_exists(spark, table) or spark.table(table).limit(1).count() == 0:
        return []
    spark.sql(f"REFRESH TABLE {table}")
    rows = []
    for raw in spark.table(table).collect():
        rows.append(
            DimUser(
                user_sk=int(raw["user_sk"]),
                user_id=int(raw["user_id"]),
                subscription_tier=str(raw["subscription_tier"]),
                valid_from_ts=_aware(raw["valid_from_ts"]),
                valid_to_ts=_aware(raw["valid_to_ts"]),
                is_current=bool(raw["is_current"]),
                country="" if raw["country"] is None else str(raw["country"]),
            )
        )
    return rows


def _load_movies(spark, table: str) -> list[MovieVersion]:
    if not table_exists(spark, table) or spark.table(table).limit(1).count() == 0:
        return []
    rows = []
    for raw in spark.table(table).collect():
        rows.append(
            MovieVersion(
                movie_sk=int(raw["movie_sk"]),
                movie_id=int(raw["movie_id"]),
                title=raw["title"],
                release_year=None if raw["release_year"] is None else int(raw["release_year"]),
                content_rating=raw["content_rating"],
                hdr_format=raw["hdr_format"],
                valid_from_ts=_aware(raw["valid_from_ts"]),
                valid_to_ts=_aware(raw["valid_to_ts"]),
                is_current=bool(raw["is_current"]),
            )
        )
    return rows


def _movie_frame(spark, rows: list[MovieVersion]):
    from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("movie_sk", LongType(), False),
            StructField("movie_id", LongType(), False),
            StructField("title", StringType(), True),
            StructField("release_year", IntegerType(), True),
            StructField("content_rating", StringType(), True),
            StructField("hdr_format", StringType(), True),
            StructField("valid_from_ts", TimestampType(), False),
            StructField("valid_to_ts", TimestampType(), True),
            StructField("is_current", BooleanType(), False),
        ]
    )
    payload = [
        (
            row.movie_sk,
            row.movie_id,
            row.title,
            row.release_year,
            row.content_rating,
            row.hdr_format,
            _naive(row.valid_from_ts),
            _naive(row.valid_to_ts),
            row.is_current,
        )
        for row in rows
    ]
    return spark.createDataFrame(payload, schema=schema)


def _empty_dim_users(spark):
    return spark.createDataFrame([], schema=_dim_schema())


def _assert_counts(ratings, watch, facts, fact_watch, obt) -> None:
    rating_count = ratings.count()
    watch_count = watch.count()
    fact_count = facts.count()
    session_count = fact_watch.count()
    obt_count = obt.count()
    if fact_count != rating_count:
        raise RuntimeError(f"fact_ratings={fact_count} lệch stg_ratings={rating_count}")
    if session_count != watch_count:
        raise RuntimeError(f"fact_watch_sessions={session_count} lệch stg_watch_sessions={watch_count}")
    if obt_count != session_count:
        raise RuntimeError(f"obt_movie_engagement={obt_count} lệch fact_watch_sessions={session_count}")


def _assert_user_history(rows: list[DimUser]) -> None:
    if len({row.user_sk for row in rows}) != len(rows):
        raise ValueError("user_sk không duy nhất")
    grouped: dict[int, list[DimUser]] = {}
    for row in rows:
        grouped.setdefault(row.user_id, []).append(row)
    for user_id, versions in grouped.items():
        ordered = sorted(versions, key=lambda item: (item.valid_from_ts, item.user_sk))
        currents = [item for item in ordered if item.is_current]
        if len(currents) != 1 or ordered[-1].valid_to_ts is not None:
            raise ValueError(f"user_id={user_id} không còn đúng một bản hiện tại")
        for previous, nxt in zip(ordered, ordered[1:]):
            if previous.is_current or previous.valid_to_ts != nxt.valid_from_ts:
                raise ValueError(f"user_id={user_id} có khoảng hiệu lực chồng nhau")


def _assert_movie_history(rows: list[MovieVersion]) -> None:
    if len({row.movie_sk for row in rows}) != len(rows):
        raise ValueError("movie_sk không duy nhất")
    grouped: dict[int, list[MovieVersion]] = {}
    for row in rows:
        grouped.setdefault(row.movie_id, []).append(row)
    for movie_id, versions in grouped.items():
        ordered = sorted(versions, key=lambda item: (item.valid_from_ts, item.movie_sk))
        currents = [item for item in ordered if item.is_current]
        if len(currents) != 1 or ordered[-1].valid_to_ts is not None:
            raise ValueError(f"movie_id={movie_id} không còn đúng một bản hiện tại")
        for previous, nxt in zip(ordered, ordered[1:]):
            if previous.is_current or previous.valid_to_ts != nxt.valid_from_ts:
                raise ValueError(f"movie_id={movie_id} có khoảng hiệu lực chồng nhau")


def _movie_attrs(row: MovieVersion) -> tuple:
    return (row.title, row.release_year, row.content_rating, row.hdr_format)


def _movie_surrogate(movie_id: int, rows: list[MovieVersion]) -> int:
    version = sum(1 for row in rows if row.movie_id == movie_id) + 1
    return int(movie_id) * 1_000_000 + version


def _snapshot_map(spark, silver_namespace: str, gold_namespace: str) -> dict:
    names = {
        "stg_users": _table(silver_namespace, "stg_users"),
        "stg_movies": _table(silver_namespace, "stg_movies"),
        "stg_ratings": _table(silver_namespace, "stg_ratings"),
        "stg_watch_sessions": _table(silver_namespace, "stg_watch_sessions"),
        "dim_users": _table(gold_namespace, "dim_users"),
        "dim_movies": _table(gold_namespace, "dim_movies"),
        "fact_ratings": _table(gold_namespace, "fact_ratings"),
        "fact_watch_sessions": _table(gold_namespace, "fact_watch_sessions"),
        "obt_movie_engagement": _table(gold_namespace, "obt_movie_engagement"),
    }
    found = {}
    for name, table in names.items():
        history = snapshots(spark, table) if table_exists(spark, table) else []
        found[name] = history[-1] if history else None
    return found


def _table(namespace: str, name: str) -> str:
    return f"{ICEBERG_CATALOG}.{namespace}.{name}"


def _aware(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _naive(value):
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def main(argv: list[str] | None = None) -> None:
    parser = build_parser("Dựng Silver và Gold từ Bronze Iceberg.")
    parser.add_argument("--warehouse", default="s3a://lakehouse")
    parser.add_argument("--bronze-namespace", default="bronze")
    parser.add_argument("--silver-namespace", default="silver")
    parser.add_argument("--gold-namespace", default="gold")
    parser.add_argument("--effective-ts", required=True, help="Mốc hiệu lực SCD2, ISO-8601, ví dụ 2026-03-01T00:00:00+00:00.")
    parser.add_argument("--batch-id", default="publish")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--error-threshold", type=float, default=DEFAULT_ERROR_RATE_THRESHOLD)
    args = parser.parse_args(argv)
    spark = start_spark("cinestream-publish", args)
    try:
        summary = publish(
            spark,
            bronze_namespace=args.bronze_namespace,
            silver_namespace=args.silver_namespace,
            gold_namespace=args.gold_namespace,
            effective_ts=datetime.fromisoformat(args.effective_ts),
            batch_id=args.batch_id,
            run_id=args.run_id,
            threshold=args.error_threshold,
        )
        print(f"effective_ts={summary['effective_ts']}")
        for name, status in summary["writes"].items():
            snap = summary["snapshots"][name]
            print(f"  {name:<24} {status:<12} snapshot={snap}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
