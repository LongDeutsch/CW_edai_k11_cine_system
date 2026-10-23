"""Ghi Iceberg theo từng grain, không dùng createOrReplace.

Một writer. ``overwrite(lit(true))`` chỉ chạy khi dòng của các cột chung khác
snapshot hiện tại. Lệch schema được báo và không tự ghi đè, không ALTER và
không đổi partition.
"""

from __future__ import annotations

from transformation.batch.contracts.schema_consistency import (
    compare_fields,
    mark_source_folders,
    null_constraint_findings,
    write_decision,
)

_REPORTS: list[dict] = []


def table_exists(spark, table: str) -> bool:
    return spark.catalog.tableExists(table)


def snapshots(spark, table: str) -> list[dict]:
    spark.sql(f"REFRESH TABLE {table}")
    rows = spark.sql(
        f"SELECT snapshot_id, operation FROM {table}.snapshots ORDER BY committed_at"
    ).collect()
    return [{"snapshot_id": int(row["snapshot_id"]), "operation": row["operation"]} for row in rows]


def take_schema_reports() -> list[dict]:
    """Lấy báo cáo từ lần ``create_or_overwrite`` vừa chạy rồi xóa bộ đệm."""
    found = list(_REPORTS)
    _REPORTS.clear()
    return found


def same_rows(spark, frame, table: str) -> bool:
    """True khi các cột chung bằng nhau. Cột thừa hoặc thiếu không làm hàm này thành False."""
    report = assess_table(spark, frame, table)
    return report["rows_equal"] is True


def assess_table(spark, frame, table: str, required: list[str] | None = None) -> dict:
    """Đọc schema và dòng. Không ghi.

    ``required`` là cột hợp đồng không được NULL. Khi bỏ trống, chỉ xét field
    Spark ``nullable=False``.
    """
    spark.sql(f"REFRESH TABLE {table}")
    current = spark.table(table)
    differences = compare_fields(_spark_fields(frame.schema), _spark_fields(current.schema))
    partition_fields = iceberg_partition_fields(spark, table)
    differences = mark_source_folders(differences, partition_fields)
    if required is None:
        required = [field.name for field in frame.schema.fields if not field.nullable]
    differences.extend(null_constraint_findings(_null_counts(frame, required), required))
    decision = write_decision(_rows_equal(frame, current), differences)
    decision["partition_fields"] = partition_fields
    decision["table"] = table
    return decision


def create_or_overwrite(spark, frame, table: str, required: list[str] | None = None) -> str:
    """Tạo bảng nếu chưa có. Ghi đè chỉ khi dòng đổi. Schema lệch thì vẫn trả ``unchanged``."""
    if not table_exists(spark, table):
        frame.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()
        return "created"
    report = assess_table(spark, frame, table, required=required)
    _REPORTS.append(report)
    if report["overwrite"]:
        from pyspark.sql import functions as F

        frame.writeTo(table).overwrite(F.lit(True))
        return "overwritten"
    return "unchanged"


def iceberg_partition_fields(spark, table: str) -> list[str]:
    """Field trong spec partition. Thư mục ``dt=`` không xuất hiện ở đây nếu không được khai báo."""
    rows = spark.sql(f"DESCRIBE {table}").collect()
    fields = []
    in_partition = False
    for row in rows:
        name = str(row["col_name"] or "").strip()
        if name.startswith("# Partition"):
            in_partition = True
            continue
        if not in_partition:
            continue
        if name.startswith("#"):
            if "col_name" in name:
                continue
            break
        if name:
            fields.append(name)
    return fields


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


def _rows_equal(frame, current) -> bool | None:
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException

    shared = [name for name in frame.columns if name in current.columns]
    if not shared:
        return True
    frame_types = {field.name: field.dataType for field in frame.schema.fields}
    table_types = {field.name: field.dataType for field in current.schema.fields}
    exprs = []
    for name in shared:
        expression = F.col(name)
        if frame_types[name].simpleString() != table_types[name].simpleString():
            expression = expression.cast(frame_types[name])
        exprs.append(expression.alias(name))
    try:
        left = current.select(*exprs)
        right = frame.select(*[F.col(name) for name in shared])
        return left.exceptAll(right).limit(1).count() == 0 and right.exceptAll(left).limit(1).count() == 0
    except AnalysisException:
        return None


def _null_counts(frame, required: list[str]) -> dict[str, int]:
    if not required:
        return {}
    from pyspark.sql import functions as F

    row = frame.select(
        *[F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(name) for name in required]
    ).collect()[0]
    return {name: int(row[name] or 0) for name in required}


def _spark_fields(schema) -> list[dict]:
    return [{"name": field.name, "type": field.dataType.simpleString(), "nullable": field.nullable} for field in schema.fields]
