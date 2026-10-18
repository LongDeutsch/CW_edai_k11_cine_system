"""Đọc Iceberg để chặn batch trước khi đánh dấu đã publish.

Module này không ghi bảng. Ingest và publish vẫn là hai entrypoint Phase 1.
"""

from __future__ import annotations

import json

from transformation.batch.common import ICEBERG_CATALOG, build_parser, start_spark
from transformation.batch.contracts.validators import check_batch_id, check_identifier

_BAD_STATUS = {"threshold_rejected", "schema_rejected", "unreadable"}


def validate_bronze(argv: list[str] | None = None) -> None:
    parser = _parser("Kiểm tra quality_results của một batch Bronze.")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--error-threshold", type=float, default=0.05)
    args = parser.parse_args(argv)
    namespace = check_identifier(args.namespace, "namespace")
    batch_id = check_batch_id(args.batch_id)
    spark = start_spark("cinestream-validate-bronze", args)
    try:
        table = f"{ICEBERG_CATALOG}.{namespace}.quality_results"
        if not spark.catalog.tableExists(table):
            _fail({"ready": False, "reason": "chưa có quality_results"})
        spark.sql(f"REFRESH TABLE {table}")
        rows = spark.sql(
            f"""
            SELECT dataset, status, error_rate, invalid_rows, total_rows
            FROM {table}
            WHERE batch_id = '{batch_id}'
            ORDER BY dataset
            """
        ).collect()
        found = [
            {
                "dataset": row["dataset"],
                "status": row["status"],
                "error_rate": row["error_rate"],
                "invalid_rows": int(row["invalid_rows"] or 0),
                "total_rows": int(row["total_rows"] or 0),
            }
            for row in rows
        ]
        bad = [
            item
            for item in found
            if item["status"] in _BAD_STATUS or float(item["error_rate"] or 0) > args.error_threshold
        ]
        payload = {"ready": not bad and bool(found), "batch_id": batch_id, "datasets": found, "violations": bad}
        if not found:
            payload["reason"] = f"không có quality_results cho {batch_id}"
        elif bad:
            payload["reason"] = "vi phạm hợp đồng hoặc vượt ngưỡng lỗi"
        else:
            payload["reason"] = "bronze đạt hợp đồng"
        _emit(payload)
        if not payload["ready"]:
            raise SystemExit(payload["reason"])
    finally:
        spark.stop()


def validate_gold(argv: list[str] | None = None) -> None:
    parser = _parser("Kiểm tra grain, khóa và SCD2 trên Silver/Gold.")
    parser.add_argument("--silver-namespace", required=True)
    parser.add_argument("--gold-namespace", required=True)
    args = parser.parse_args(argv)
    silver = check_identifier(args.silver_namespace, "silver-namespace")
    gold = check_identifier(args.gold_namespace, "gold-namespace")
    spark = start_spark("cinestream-validate-gold", args)
    try:
        required = {
            silver: ("stg_ratings", "stg_watch_sessions"),
            gold: ("dim_users", "dim_movies", "fact_ratings", "fact_watch_sessions", "obt_movie_engagement"),
        }
        missing = [
            f"{namespace}.{table}"
            for namespace, tables in required.items()
            for table in tables
            if not spark.catalog.tableExists(f"{ICEBERG_CATALOG}.{namespace}.{table}")
        ]
        if missing:
            _fail({"ready": False, "reason": "thiếu bảng " + ", ".join(missing)})
        for namespace, tables in required.items():
            for table in tables:
                spark.sql(f"REFRESH TABLE {ICEBERG_CATALOG}.{namespace}.{table}")
        counts = {
            "stg_ratings": _count(spark, silver, "stg_ratings"),
            "fact_ratings": _count(spark, gold, "fact_ratings"),
            "stg_watch_sessions": _count(spark, silver, "stg_watch_sessions"),
            "fact_watch_sessions": _count(spark, gold, "fact_watch_sessions"),
            "obt_movie_engagement": _count(spark, gold, "obt_movie_engagement"),
        }
        users_without_one_current = _scalar(
            spark,
            f"""
            SELECT count(*) FROM (
                SELECT user_id
                FROM {_table(gold, "dim_users")}
                GROUP BY user_id
                HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
            )
            """,
        )
        movies_without_one_current = _scalar(
            spark,
            f"""
            SELECT count(*) FROM (
                SELECT movie_id
                FROM {_table(gold, "dim_movies")}
                GROUP BY movie_id
                HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
            )
            """,
        )
        user_overlaps = _overlaps(spark, gold, "dim_users", "user_id", "user_sk")
        movie_overlaps = _overlaps(spark, gold, "dim_movies", "movie_id", "movie_sk")
        duplicate_rating_sk = _scalar(
            spark,
            f"SELECT count(*) - count(DISTINCT rating_sk) FROM {_table(gold, 'fact_ratings')}",
        )
        duplicate_session_sk = _scalar(
            spark,
            f"SELECT count(*) - count(DISTINCT session_sk) FROM {_table(gold, 'fact_watch_sessions')}",
        )
        orphan_users = _scalar(
            spark,
            f"""
            SELECT count(*)
            FROM {_table(gold, "fact_ratings")} f
            LEFT JOIN {_table(gold, "dim_users")} d ON f.user_sk = d.user_sk
            WHERE d.user_sk IS NULL
            """,
        )
        orphan_movies = _scalar(
            spark,
            f"""
            SELECT count(*)
            FROM {_table(gold, "fact_ratings")} f
            LEFT JOIN {_table(gold, "dim_movies")} d ON f.movie_sk = d.movie_sk
            WHERE d.movie_sk IS NULL
            """,
        )
        payload = {
            "ready": True,
            "counts": counts,
            "users_without_one_current": users_without_one_current,
            "movies_without_one_current": movies_without_one_current,
            "user_overlaps": user_overlaps,
            "movie_overlaps": movie_overlaps,
            "duplicate_rating_sk": duplicate_rating_sk,
            "duplicate_session_sk": duplicate_session_sk,
            "orphan_user_sk": orphan_users,
            "orphan_movie_sk": orphan_movies,
        }
        problems = []
        if counts["fact_ratings"] != counts["stg_ratings"]:
            problems.append("fact_ratings lệch stg_ratings")
        if counts["fact_watch_sessions"] != counts["stg_watch_sessions"]:
            problems.append("fact_watch_sessions lệch stg_watch_sessions")
        if counts["obt_movie_engagement"] != counts["fact_watch_sessions"]:
            problems.append("obt lệch fact_watch_sessions")
        if users_without_one_current or movies_without_one_current:
            problems.append("SCD2 không còn đúng một bản hiện tại")
        if user_overlaps or movie_overlaps:
            problems.append("khoảng hiệu lực SCD2 chồng nhau")
        if duplicate_rating_sk or duplicate_session_sk:
            problems.append("khóa nghiệp vụ fact bị trùng")
        if orphan_users or orphan_movies:
            problems.append("fact có khóa không tồn tại ở dimension")
        payload["ready"] = not problems
        payload["reason"] = "; ".join(problems) if problems else "gold đạt bất biến"
        _emit(payload)
        if problems:
            raise SystemExit(payload["reason"])
    finally:
        spark.stop()


def _parser(description: str):
    parser = build_parser(description)
    parser.add_argument("--warehouse", default="s3a://lakehouse")
    return parser


def _table(namespace: str, table: str) -> str:
    return f"{ICEBERG_CATALOG}.{namespace}.{table}"


def _count(spark, namespace: str, table: str) -> int:
    return int(spark.table(_table(namespace, table)).count())


def _scalar(spark, query: str) -> int:
    return int(spark.sql(query).collect()[0][0] or 0)


def _overlaps(spark, namespace: str, table: str, business_key: str, surrogate: str) -> int:
    full_name = _table(namespace, table)
    return _scalar(
        spark,
        f"""
        SELECT count(*)
        FROM {full_name} a
        JOIN {full_name} b
          ON a.{business_key} = b.{business_key}
         AND a.{surrogate} < b.{surrogate}
        WHERE a.valid_from_ts < COALESCE(b.valid_to_ts, TIMESTAMP '9999-12-31 00:00:00')
          AND b.valid_from_ts < COALESCE(a.valid_to_ts, TIMESTAMP '9999-12-31 00:00:00')
        """,
    )


def _emit(payload: dict) -> None:
    print("RESULT_JSON=" + json.dumps(payload, default=str), flush=True)


def _fail(payload: dict) -> None:
    _emit(payload)
    raise SystemExit(payload["reason"])
