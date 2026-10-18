"""Ghi Iceberg theo từng grain, không dùng createOrReplace.

Một writer. ``overwrite(lit(true))`` chỉ chạy khi dữ liệu mới khác snapshot
hiện tại, nên rerun không mở snapshot mới. Partition chỉ bị thay khi frame
là toàn bộ bảng fact đã dựng lại từ Silver.
"""

from __future__ import annotations


def table_exists(spark, table: str) -> bool:
    return spark.catalog.tableExists(table)


def snapshots(spark, table: str) -> list[dict]:
    spark.sql(f"REFRESH TABLE {table}")
    rows = spark.sql(
        f"SELECT snapshot_id, operation FROM {table}.snapshots ORDER BY committed_at"
    ).collect()
    return [{"snapshot_id": int(row["snapshot_id"]), "operation": row["operation"]} for row in rows]


def same_rows(spark, frame, table: str) -> bool:
    spark.sql(f"REFRESH TABLE {table}")
    current = spark.table(table).select(*frame.columns)
    aligned = frame.select(*frame.columns)
    left = current.exceptAll(aligned).limit(1).count()
    right = aligned.exceptAll(current).limit(1).count()
    return left == 0 and right == 0


def create_or_overwrite(spark, frame, table: str) -> str:
    """Tạo bảng nếu chưa có. Nếu đã có và dữ liệu đổi thì ghi đè toàn bộ."""
    if not table_exists(spark, table):
        frame.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()
        return "created"
    if same_rows(spark, frame, table):
        return "unchanged"
    from pyspark.sql import functions as F

    frame.writeTo(table).overwrite(F.lit(True))
    return "overwritten"


def append_new_files(spark, frame, table: str, file_column: str = "_file_id") -> int:
    """Append các file chưa có trong Bronze. File đã có thì bỏ qua."""
    if not table_exists(spark, table):
        count = frame.count()
        if count == 0:
            return 0
        frame.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()
        return count
    existing = spark.table(table).select(file_column).distinct()
    fresh = frame.join(existing, file_column, "left_anti")
    count = fresh.count()
    if count == 0:
        return 0
    fresh.writeTo(table).append()
    return count
