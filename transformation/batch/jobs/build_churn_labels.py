"""Tính bảng nhãn Churn 14 ngày (label_user_churn_14d) từ stg_watch_sessions.

Định nghĩa: Tại mốc thời gian T (as_of_date), nếu người dùng không có phiên xem nào
trong khoảng [T, T + 14 ngày] thì is_churned = 1, ngược lại is_churned = 0.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone

from transformation.batch.common import build_parser, start_spark
from transformation.batch.contracts.validators import check_identifier
from transformation.batch.jobs.lakehouse_ddl import execute_zone
from transformation.batch.jobs.table_updates import create_or_overwrite

TABLE_NAME = "label_user_churn_14d"
_PROTECTED = {"bronze", "silver", "gold"}


def compute_churn_labels(spark, users, watches, as_of_dates: list[date], created_ts: datetime):
    """Tính nhãn churn 14 ngày cho các as_of_date được truyền vào.

    Parameters:
        spark: SparkSession
        users: DataFrame chứa ít nhất user_id
        watches: DataFrame chứa user_id, start_ts
        as_of_dates: Danh sách các ngày (date) mốc đánh giá
        created_ts: Thời điểm tính toán
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, IntegerType, LongType, StructField, StructType, TimestampType

    if not as_of_dates:
        raise ValueError("cần ít nhất một as_of_date")

    dates_frame = spark.createDataFrame(
        [(d,) for d in sorted(set(as_of_dates))],
        StructType([StructField("as_of_date", DateType(), False)]),
    )

    users.select(F.col("user_id").cast("long").alias("user_id")).distinct().createOrReplaceTempView("_churn_users")
    watches.select(
        F.col("user_id").cast("long").alias("user_id"),
        F.col("start_ts").cast("timestamp").alias("start_ts"),
    ).createOrReplaceTempView("_churn_watches")
    dates_frame.createOrReplaceTempView("_churn_dates")

    sql = f"""
    WITH user_dates AS (
        SELECT u.user_id, d.as_of_date,
               CAST(d.as_of_date AS TIMESTAMP) AS window_start,
               CAST(d.as_of_date + INTERVAL 14 DAYS AS TIMESTAMP) AS window_end
        FROM _churn_users u
        CROSS JOIN _churn_dates d
    ),
    active_in_window AS (
        SELECT ud.user_id, ud.as_of_date, COUNT(w.start_ts) AS session_count
        FROM user_dates ud
        LEFT JOIN _churn_watches w
          ON ud.user_id = w.user_id
         AND w.start_ts >= ud.window_start
         AND w.start_ts < ud.window_end
        GROUP BY ud.user_id, ud.as_of_date
    )
    SELECT
        user_id,
        as_of_date,
        CASE WHEN session_count = 0 THEN 1 ELSE 0 END AS is_churned,
        CAST('{created_ts.strftime("%Y-%m-%d %H:%M:%S")}' AS TIMESTAMP) AS created_ts
    FROM active_in_window
    """
    return spark.sql(sql)


def publish_churn_labels(
    spark,
    silver_namespace: str,
    gold_namespace: str,
    as_of_dates: list[date],
    created_ts: datetime,
    *,
    allow_production: bool = False,
) -> dict:
    check_identifier(silver_namespace, "silver_namespace")
    check_identifier(gold_namespace, "gold_namespace")
    if gold_namespace in _PROTECTED and not allow_production:
        raise RuntimeError(f"không ghi namespace {gold_namespace} khi chưa allow production")

    users = spark.table(f"cinestream.{silver_namespace}.stg_users")
    watches = spark.table(f"cinestream.{silver_namespace}.stg_watch_sessions")

    labels = compute_churn_labels(spark, users, watches, as_of_dates, created_ts)
    total_rows = labels.count()
    churned_count = labels.filter("is_churned = 1").count()

    execute_zone(spark, "gold", gold_namespace)
    table_path = f"cinestream.{gold_namespace}.{TABLE_NAME}"
    write_status = create_or_overwrite(
        spark,
        labels,
        table_path,
        required=["user_id", "as_of_date", "is_churned", "created_ts"],
    )

    return {
        "table": table_path,
        "write_status": write_status,
        "total_rows": total_rows,
        "churned_count": churned_count,
        "active_count": total_rows - churned_count,
        "as_of_dates": [d.isoformat() for d in as_of_dates],
    }


def main(argv: list[str] | None = None) -> None:
    parser = build_parser("Tính bảng nhãn Churn 14 ngày (label_user_churn_14d)")
    parser.add_argument("--warehouse", default="s3a://lakehouse")
    parser.add_argument("--silver-namespace", required=True)
    parser.add_argument("--gold-namespace", required=True)
    parser.add_argument("--as-of-date", required=True, help="Một hoặc nhiều ngày YYYY-MM-DD, cách nhau bởi dấu phẩy.")
    parser.add_argument("--allow-production", action="store_true")
    args = parser.parse_args(argv)

    spark = start_spark("cinestream-churn-labels", args)
    try:
        dates_list = [date.fromisoformat(item.strip()) for item in args.as_of_date.split(",") if item.strip()]
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        report = publish_churn_labels(
            spark,
            args.silver_namespace,
            args.gold_namespace,
            dates_list,
            now,
            allow_production=args.allow_production,
        )
        print("RESULT_JSON=" + json.dumps(report))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
