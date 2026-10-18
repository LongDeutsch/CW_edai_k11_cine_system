"""Đọc Iceberg bằng Spark trong container Airflow.

Chạy bằng ``spark-submit`` để dùng Java 17 của image. JVM Homebrew trên máy host
bị SIGSEGV khi đọc fact lớn.
"""

from __future__ import annotations

import json
import sys

from transformation.batch.common import build_parser, start_spark
from transformation.batch.jobs.table_updates import snapshots


def main() -> None:
    action, namespace = sys.argv[1], sys.argv[2]
    batch_id = sys.argv[3] if len(sys.argv) > 3 else ""
    parser = build_parser("verify real source")
    args = parser.parse_args(
        [
            "--iceberg-rest",
            "http://iceberg-rest:8181",
            "--s3-endpoint",
            "http://minio:9000",
            "--s3-access-key",
            "minioadmin",
            "--s3-secret-key",
            "minioadmin",
            "--master",
            "local[1]",
            "--no-ui",
            "--shuffle-partitions",
            "4",
        ]
    )
    args.warehouse = "s3a://lakehouse"
    spark = start_spark("cinestream-real-verify", args)
    try:
        if action == "bad":
            result = {
                "users": _count(spark, namespace, "raw_users"),
                "facts": _count(spark, namespace, "fact_ratings"),
                "quarantine": _count(spark, namespace, "quarantine_records"),
                "quality": _quality(spark, namespace, batch_id),
            }
        elif action == "success":
            result = {
                "users": _count(spark, namespace, "raw_users"),
                "movies": _count(spark, namespace, "raw_movies"),
                "ratings": _count(spark, namespace, "raw_ratings"),
                "watches": _count(spark, namespace, "raw_watch_history"),
                "facts": _count(spark, namespace, "fact_ratings"),
                "fact_watch": _count(spark, namespace, "fact_watch_sessions"),
                "signup_type": _field_type(spark, namespace, "raw_users", "signup_ts"),
                "created_type": _field_type(spark, namespace, "raw_movies", "created_ts"),
                "rating_type": _field_type(spark, namespace, "raw_ratings", "rating_ts"),
                "snapshots": _snap(spark, namespace, "raw_users", "raw_ratings", "raw_watch_history", "fact_ratings"),
            }
        elif action == "evolution":
            result = _evolution(spark, namespace)
        elif action == "rerun":
            result = {
                "users": _count(spark, namespace, "raw_users"),
                "ratings": _count(spark, namespace, "raw_ratings"),
                "watches": _count(spark, namespace, "raw_watch_history"),
                "snapshots": _snap(spark, namespace, "raw_users", "raw_ratings", "raw_watch_history"),
            }
        elif action == "retry":
            result = {
                "users": _count(spark, namespace, "raw_users"),
                "ratings": _count(spark, namespace, "raw_ratings"),
                "facts": _count(spark, namespace, "fact_ratings"),
            }
        else:
            raise SystemExit(f"action không hỗ trợ: {action}")
    finally:
        spark.stop()
    print("VERIFY_JSON=" + json.dumps(result, default=str), flush=True)


def _count(spark, namespace: str, table: str) -> int:
    name = f"cinestream.{namespace}.{table}"
    try:
        spark.sql(f"REFRESH TABLE {name}")
        return int(spark.table(name).count())
    except Exception:
        return 0


def _field_type(spark, namespace: str, table: str, column: str) -> str:
    spark.sql(f"REFRESH TABLE cinestream.{namespace}.{table}")
    field = next(item for item in spark.table(f"cinestream.{namespace}.{table}").schema.fields if item.name == column)
    return field.dataType.simpleString()


def _snap(spark, namespace: str, *tables: str) -> dict:
    found = {}
    for table in tables:
        try:
            found[table] = snapshots(spark, f"cinestream.{namespace}.{table}")
        except Exception:
            found[table] = []
    return found


def _quality(spark, namespace: str, batch_id: str) -> list:
    name = f"cinestream.{namespace}.quality_results"
    try:
        spark.sql(f"REFRESH TABLE {name}")
        rows = spark.sql(
            f"""
            SELECT dataset, status, invalid_rows, total_rows
            FROM {name}
            WHERE batch_id = '{batch_id}'
            ORDER BY dataset
            """
        ).collect()
    except Exception:
        return []
    return [
        [row["dataset"], row["status"], int(row["invalid_rows"] or 0), int(row["total_rows"] or 0)]
        for row in rows
    ]


def _scalar(spark, query: str) -> int:
    return int(spark.sql(query).collect()[0][0] or 0)


def _evolution(spark, namespace: str) -> dict:
    gold = f"cinestream.{namespace}"
    spark.sql(f"REFRESH TABLE {gold}.raw_movies")
    row = spark.sql(
        f"""
        SELECT
          count(*) AS movies,
          sum(CASE WHEN content_rating IS NULL THEN 1 ELSE 0 END) AS old_schema,
          sum(CASE WHEN content_rating IS NOT NULL THEN 1 ELSE 0 END) AS evolved
        FROM {gold}.raw_movies
        """
    ).collect()[0]
    for table in (
        "dim_users",
        "dim_movies",
        "fact_ratings",
        "fact_watch_sessions",
        "obt_movie_engagement",
        "stg_ratings",
        "stg_watch_sessions",
    ):
        spark.sql(f"REFRESH TABLE {gold}.{table}")
    return {
        "movies": int(row["movies"]),
        "old_schema": int(row["old_schema"]),
        "evolved": int(row["evolved"]),
        "stg_ratings": _count(spark, namespace, "stg_ratings"),
        "fact_ratings": _count(spark, namespace, "fact_ratings"),
        "stg_watch": _count(spark, namespace, "stg_watch_sessions"),
        "fact_watch": _count(spark, namespace, "fact_watch_sessions"),
        "obt": _count(spark, namespace, "obt_movie_engagement"),
        "users_without_one_current": _scalar(
            spark,
            f"""
            SELECT count(*) FROM (
                SELECT user_id FROM {gold}.dim_users
                GROUP BY user_id
                HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
            )
            """,
        ),
        "movies_without_one_current": _scalar(
            spark,
            f"""
            SELECT count(*) FROM (
                SELECT movie_id FROM {gold}.dim_movies
                GROUP BY movie_id
                HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
            )
            """,
        ),
        "user_overlaps": _scalar(
            spark,
            f"""
            SELECT count(*) FROM {gold}.dim_users a
            JOIN {gold}.dim_users b
              ON a.user_id = b.user_id AND a.user_sk < b.user_sk
            WHERE a.valid_from_ts < COALESCE(b.valid_to_ts, TIMESTAMP '9999-12-31 00:00:00')
              AND b.valid_from_ts < COALESCE(a.valid_to_ts, TIMESTAMP '9999-12-31 00:00:00')
            """,
        ),
        "movie_overlaps": _scalar(
            spark,
            f"""
            SELECT count(*) FROM {gold}.dim_movies a
            JOIN {gold}.dim_movies b
              ON a.movie_id = b.movie_id AND a.movie_sk < b.movie_sk
            WHERE a.valid_from_ts < COALESCE(b.valid_to_ts, TIMESTAMP '9999-12-31 00:00:00')
              AND b.valid_from_ts < COALESCE(a.valid_to_ts, TIMESTAMP '9999-12-31 00:00:00')
            """,
        ),
        "duplicate_rating_sk": _scalar(spark, f"SELECT count(*) - count(DISTINCT rating_sk) FROM {gold}.fact_ratings"),
        "duplicate_session_sk": _scalar(
            spark, f"SELECT count(*) - count(DISTINCT session_sk) FROM {gold}.fact_watch_sessions"
        ),
        "orphan_user_sk": _scalar(
            spark,
            f"""
            SELECT count(*) FROM {gold}.fact_ratings f
            LEFT JOIN {gold}.dim_users d ON f.user_sk = d.user_sk
            WHERE d.user_sk IS NULL
            """,
        ),
        "orphan_movie_sk": _scalar(
            spark,
            f"""
            SELECT count(*) FROM {gold}.fact_ratings f
            LEFT JOIN {gold}.dim_movies d ON f.movie_sk = d.movie_sk
            WHERE d.movie_sk IS NULL
            """,
        ),
    }


if __name__ == "__main__":
    main()
