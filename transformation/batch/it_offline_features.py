"""Feature offline trên warehouse file:// .

Không ghi bronze/silver/gold production. Chạy::

    make spark-features-it
"""

from __future__ import annotations

import json
import tempfile
import traceback
import urllib.request
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from pyspark.sql.types import DateType, DecimalType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from transformation.batch.common import ICEBERG_CATALOG, build_parser, start_spark
from transformation.batch.jobs.build_offline_features import assemble_features, publish_features
from transformation.batch.jobs.lakehouse_ddl import execute_zone
from transformation.batch.jobs.table_updates import snapshots

REPO = Path(__file__).resolve().parents[2]
LOG = REPO / "docs" / "proof" / "logs" / "phase_a1_offline_features.json"
NAMESPACE = "it_a1"
AS_OF = datetime(2026, 6, 1, 0, 0, 0)
CREATED = datetime(2026, 6, 2, 3, 0, 0)


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="cinestream-features-it-"))
    warehouse = (root / "warehouse").resolve()
    warehouse.mkdir()
    if warehouse.as_uri().startswith("s3"):
        raise SystemExit("warehouse kiểm tra phải là file://")
    before = _production_fingerprint()
    parser = build_parser("offline feature test")
    args = parser.parse_args([])
    args.warehouse = warehouse.as_uri()
    args.no_ui = True
    args.shuffle_partitions = 2
    spark = start_spark("cinestream-features-it", args)
    report = {"namespace": NAMESPACE, "warehouse": str(warehouse)}
    try:
        execute_zone(spark, "silver", NAMESPACE)
        _load_fixture(spark)
        blocked = _production_namespace_is_blocked(spark)
        first = publish_features(spark, NAMESPACE, NAMESPACE, [AS_OF], CREATED)
        second = publish_features(spark, NAMESPACE, NAMESPACE, [AS_OF], CREATED)
        user_table = f"{ICEBERG_CATALOG}.{NAMESPACE}.feat_user_movie_stats_daily"
        movie_table = f"{ICEBERG_CATALOG}.{NAMESPACE}.feat_movie_engagement_stats_daily"
        users = {int(row["user_id"]): row.asDict() for row in spark.table(user_table).collect()}
        movies = {int(row["movie_id"]): row.asDict() for row in spark.table(movie_table).collect()}
        report.update(
            {
                "status": "PASS",
                "production_namespace_blocked": blocked,
                "first_write": {"user": first["user_write"], "movie": first["movie_write"], "ready": first["ready"]},
                "second_write": {"user": second["user_write"], "movie": second["movie_write"]},
                "user_snapshots": len(snapshots(spark, user_table)),
                "movie_snapshots": len(snapshots(spark, movie_table)),
                "users": _public_rows(users),
                "movies": _public_rows(movies),
            }
        )
        _expect(first["ready"] is True and second["ready"] is True, "validation")
        _expect(first["user_write"] == "overwritten" and second["user_write"] == "unchanged", "user rerun")
        _expect(first["movie_write"] == "overwritten" and second["movie_write"] == "unchanged", "movie rerun")
        _expect(report["user_snapshots"] == 1 and report["movie_snapshots"] == 1, "snapshot count")
        _expect_user(users[1], ratings=1, average=5.0, genre="Action", hours=1.0)
        _expect_user(users[2], ratings=0, average=None, genre=None, hours=2.0)
        movie = movies[10]
        _expect(int(movie["movie_rating_count_30d"]) == 1, "movie count")
        _expect(_close(movie["movie_avg_rating_30d"], 5.0), "movie average")
        _expect(_close(movie["movie_completion_rate_30d"], 0.5), "completion")
        _expect(_close(movie["movie_trending_score_7d"], 1.0), "trending")
        _expect(12 not in movies, "label movie stays out of the feature grain")
        silver = f"{ICEBERG_CATALOG}.{NAMESPACE}"
        plain_users, _plain_movies = assemble_features(
            spark,
            spark.table(f"{silver}.stg_ratings"),
            spark.table(f"{silver}.stg_watch_sessions"),
            [AS_OF],
            CREATED,
            None,
        )
        plain = {int(row["user_id"]): row["user_favorite_genre"] for row in plain_users.collect()}
        _expect(plain[1] is None and plain[2] is None, "genre absent")
        report["genre_without_source"] = "null"
    except Exception as exc:
        report["status"] = "FAIL"
        report["error"] = f"{exc.__class__.__name__}: {exc}"
        report["traceback"] = traceback.format_exc()
    finally:
        spark.stop()
    after = _production_fingerprint()
    report["production_unchanged"] = before == after
    report["production_before"] = before
    report["production_after"] = after
    if report.get("status") == "PASS" and not report["production_unchanged"]:
        report["status"] = "FAIL"
        report["error"] = "production snapshot đổi"
    LOG.parent.mkdir(parents=True, exist_ok=True)
    LOG.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps({key: report[key] for key in ("status", "production_unchanged", "first_write", "second_write") if key in report}, default=str))
    if report["status"] != "PASS":
        raise SystemExit(report.get("error") or report.get("traceback") or "feature it thất bại")


def _load_fixture(spark) -> None:
    rating_schema = StructType(
        [
            StructField("rating_id", LongType(), False),
            StructField("user_id", LongType(), False),
            StructField("movie_id", LongType(), False),
            StructField("rating_val", DecimalType(2, 1), False),
            StructField("rating_ts", TimestampType(), False),
            StructField("review_text", StringType(), True),
            StructField("event_date", DateType(), False),
        ]
    )
    watch_schema = StructType(
        [
            StructField("session_id", LongType(), False),
            StructField("user_id", LongType(), False),
            StructField("movie_id", LongType(), False),
            StructField("start_ts", TimestampType(), False),
            StructField("end_ts", TimestampType(), True),
            StructField("watch_duration_seconds", LongType(), False),
            StructField("completion_rate", DoubleType(), False),
            StructField("device_type", StringType(), True),
            StructField("resolution", StringType(), True),
            StructField("event_date", DateType(), False),
        ]
    )
    ratings = [
        _rating(1, 1, 10, "5.0", datetime(2026, 5, 20, 12, 0, 0)),
        _rating(2, 1, 12, "4.0", AS_OF),
        _rating(3, 1, 10, "1.0", AS_OF + timedelta(hours=8)),
    ]
    watches = [
        _watch(1, 1, 10, datetime(2026, 5, 28, 12, 0, 0), 3600, 0.25),
        _watch(2, 2, 10, datetime(2026, 5, 10, 12, 0, 0), 7200, 0.75),
        _watch(3, 1, 10, datetime(2026, 6, 3, 12, 0, 0), 99999, 1.0),
    ]
    silver = f"{ICEBERG_CATALOG}.{NAMESPACE}"
    spark.createDataFrame(ratings, rating_schema).writeTo(f"{silver}.stg_ratings").append()
    spark.createDataFrame(watches, watch_schema).writeTo(f"{silver}.stg_watch_sessions").append()
    spark.sql(
        f"""
        CREATE TABLE {silver}.stg_movie_genres (
            movie_id BIGINT,
            genre_name STRING
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        INSERT INTO {silver}.stg_movie_genres VALUES
        (10, 'Drama'),
        (10, 'Action')
        """
    )


def _rating(rating_id, user_id, movie_id, value, ts):
    return (rating_id, user_id, movie_id, Decimal(value), ts, None, date(ts.year, ts.month, ts.day))


def _watch(session_id, user_id, movie_id, start, seconds, completion):
    return (
        session_id,
        user_id,
        movie_id,
        start,
        None,
        seconds,
        completion,
        "tv",
        None,
        date(start.year, start.month, start.day),
    )


def _production_namespace_is_blocked(spark) -> bool:
    try:
        publish_features(spark, NAMESPACE, "gold", [AS_OF], CREATED)
    except RuntimeError as exc:
        return "gold" in str(exc)
    return False


def _expect(condition: bool, name: str) -> None:
    if not condition:
        raise AssertionError(name)


def _expect_user(row, ratings, average, genre, hours) -> None:
    _expect(int(row["user_total_ratings_90d"]) == ratings, "user ratings")
    if average is None:
        _expect(row["user_avg_rating_90d"] is None, "user average null")
    else:
        _expect(_close(row["user_avg_rating_90d"], average), "user average")
    _expect(row["user_favorite_genre"] == genre, "genre")
    _expect(_close(row["user_total_watch_hours_30d"], hours), "hours")


def _close(actual, expected) -> bool:
    return actual is not None and abs(float(actual) - expected) < 1e-9


def _public_rows(rows: dict) -> dict:
    public = {}
    for key, row in rows.items():
        public[str(key)] = {name: _jsonable(value) for name, value in row.items()}
    return public


def _jsonable(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _production_fingerprint() -> dict:
    paths = (
        "namespaces/gold/tables/fact_ratings",
        "namespaces/gold/tables/fact_watch_sessions",
        "namespaces/gold/tables/feat_user_movie_stats_daily",
        "namespaces/gold/tables/feat_movie_engagement_stats_daily",
        "namespaces/silver/tables/stg_ratings",
    )
    found = {}
    for path in paths:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/{path}", timeout=2) as response:
                metadata = json.loads(response.read().decode())["metadata"]
                found[path] = {
                    "snapshot": metadata.get("current-snapshot-id"),
                    "schema": metadata.get("current-schema-id"),
                }
        except Exception as exc:
            found[path] = f"unavailable:{exc.__class__.__name__}"
    return found


if __name__ == "__main__":
    main()
