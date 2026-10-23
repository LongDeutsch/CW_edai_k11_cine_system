"""Schema contract trên warehouse file:// cách ly.

Không ALTER, DROP hay overwrite bảng production. ``dt=`` của path nguồn
không được đưa vào spec partition.

Chạy::

    make spark-schema-it
"""

from __future__ import annotations

import json
import tempfile
import traceback
import urllib.request
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from transformation.batch.common import ICEBERG_CATALOG, build_parser, start_spark
from transformation.batch.contracts.schema_consistency import expected_partitions, partition_difference
from transformation.batch.jobs.lakehouse_ddl import execute_zone
from transformation.batch.jobs.table_updates import (
    assess_table,
    create_or_overwrite,
    iceberg_partition_fields,
    snapshots,
    take_schema_reports,
)

REPO = Path(__file__).resolve().parents[2]
LOG = REPO / "docs" / "proof" / "logs" / "phase5d_schema_contract.json"
INSTANT = datetime(2026, 1, 15, 12, 0, 0)
NAMESPACE = "it_schema5d"


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="cinestream-schema-it-"))
    warehouse = root / "warehouse"
    warehouse.mkdir()
    before = _production_fingerprint()
    parser = build_parser("schema contract test")
    args = parser.parse_args([])
    args.warehouse = warehouse.resolve().as_uri()
    args.no_ui = True
    args.shuffle_partitions = 2
    spark = start_spark("cinestream-schema-it", args)
    cases = []
    try:
        _guard(args.warehouse)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{NAMESPACE}")
        cases.append(_run("extra dt and decimal", lambda: case_extra_dt_and_decimal(spark)))
        cases.append(_run("missing column", lambda: case_missing_column(spark)))
        cases.append(_run("row change overwrites", lambda: case_row_change(spark)))
        cases.append(_run("utc instants", lambda: case_utc(spark)))
        cases.append(_run("null values", lambda: case_nulls(spark)))
        cases.append(_run("ddl partition versus dataframe create", lambda: case_creation(spark)))
    finally:
        spark.stop()
    after = _production_fingerprint()
    failed = [item["name"] for item in cases if item["status"] != "PASS"]
    report = {
        "warehouse": str(warehouse),
        "namespace": NAMESPACE,
        "production_unchanged": before == after,
        "production_before": before,
        "production_after": after,
        "cases": cases,
        "status": "FAIL" if failed else "PASS",
        "failed": failed,
    }
    LOG.parent.mkdir(parents=True, exist_ok=True)
    LOG.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps(report, indent=2, default=str))
    if failed or before != after:
        raise SystemExit(f"FAIL {failed or 'production fingerprint đổi'}")


def case_extra_dt_and_decimal(spark) -> dict:
    table = _table("ratings_historical")
    historical = _frame(
        spark,
        StructType(
            [
                StructField("rating_id", LongType(), True),
                StructField("rating_val", DoubleType(), True),
                StructField("rating_ts", TimestampNTZType(), True),
                StructField("dt", DateType(), True),
            ]
        ),
        [(1, 4.0, INSTANT, INSTANT.date())],
    )
    created = create_or_overwrite(spark, historical, table)
    before = _snap(spark, table)
    contract = _frame(
        spark,
        StructType(
            [
                StructField("rating_id", LongType(), False),
                StructField("rating_val", DecimalType(2, 1), False),
                StructField("rating_ts", TimestampType(), False),
            ]
        ),
        [(1, Decimal("4.0"), INSTANT)],
    )
    take_schema_reports()
    status = create_or_overwrite(spark, contract, table)
    after = _snap(spark, table)
    report = take_schema_reports()[0]
    kinds = {(item["kind"], item["column"]) for item in report["differences"]}
    dt = next(item for item in report["differences"] if item["column"] == "dt")
    actual = {
        "created": created,
        "status": status,
        "snapshot_unchanged": before == after,
        "overwrite": report["overwrite"],
        "schema_equal": report["schema_equal"],
        "rows_equal": report["rows_equal"],
        "kinds": sorted(kinds),
        "dt_origin": dt.get("origin"),
        "dt_is_partition": dt.get("iceberg_partition_field"),
        "partition_fields": report["partition_fields"],
        "repartition": report["repartition"],
    }
    _expect(created == "created" and status == "unchanged", actual)
    _expect(before == after and report["overwrite"] is False, actual)
    _expect(report["rows_equal"] is True and report["schema_equal"] is False, actual)
    _expect(("type", "rating_val") in kinds and ("type", "rating_ts") in kinds, actual)
    _expect(dt.get("origin") == "source_folder" and dt.get("iceberg_partition_field") is False, actual)
    _expect(report["partition_fields"] == [] and report["repartition"] is False, actual)
    return {
        "input": "bảng double + timestamp_ntz + dt, frame decimal + timestamp không có dt",
        "expected": "dòng bằng nhau, snapshot giữ nguyên, báo type và dt nguồn",
        "actual": actual,
    }


def case_missing_column(spark) -> dict:
    table = _table("ratings_missing")
    stored = _frame(
        spark,
        StructType(
            [
                StructField("rating_id", LongType(), True),
                StructField("review_text", StringType(), True),
            ]
        ),
        [(1, "ok")],
    )
    create_or_overwrite(spark, stored, table)
    before = _snap(spark, table)
    incoming = _frame(spark, StructType([StructField("rating_id", LongType(), True)]), [(1,)])
    take_schema_reports()
    status = create_or_overwrite(spark, incoming, table)
    report = take_schema_reports()[0]
    wider = _frame(
        spark,
        StructType(
            [
                StructField("rating_id", LongType(), True),
                StructField("review_text", StringType(), True),
                StructField("note", StringType(), True),
            ]
        ),
        [(1, "ok", "ignored")],
    )
    take_schema_reports()
    added = create_or_overwrite(spark, wider, table)
    added_report = take_schema_reports()[0]
    actual = {
        "status": status,
        "snapshot_unchanged": before == _snap(spark, table),
        "overwrite": report["overwrite"],
        "kinds": [(item["kind"], item["column"]) for item in report["differences"]],
        "added_status": added,
        "added_kinds": [(item["kind"], item["column"]) for item in added_report["differences"]],
        "added_overwrite": added_report["overwrite"],
    }
    _expect(status == "unchanged" and actual["snapshot_unchanged"], actual)
    _expect(actual["kinds"] == [("extra_on_table", "review_text")], actual)
    _expect(added == "unchanged" and added_report["overwrite"] is False, actual)
    _expect(actual["added_kinds"] == [("missing_on_table", "note")], actual)
    return {
        "input": "frame thiếu review_text, rồi frame thêm note",
        "expected": "báo extra và missing, không overwrite",
        "actual": actual,
    }


def case_row_change(spark) -> dict:
    table = _table("ratings_changed")
    schema = StructType(
        [
            StructField("rating_id", LongType(), True),
            StructField("rating_val", DecimalType(2, 1), True),
        ]
    )
    create_or_overwrite(spark, _frame(spark, schema, [(1, Decimal("4.0"))]), table)
    before = _snap(spark, table)
    take_schema_reports()
    status = create_or_overwrite(spark, _frame(spark, schema, [(1, Decimal("4.5"))]), table)
    report = take_schema_reports()[0]
    actual = {"status": status, "snapshot_changed": before != _snap(spark, table), "schema_equal": report["schema_equal"]}
    _expect(status == "overwritten" and actual["snapshot_changed"] and report["schema_equal"], actual)
    return {
        "input": "cùng schema, rating_val 4.0 rồi 4.5",
        "expected": "overwrite vì dòng đổi",
        "actual": actual,
    }


def case_utc(spark) -> dict:
    ntz_table = _table("ts_ntz")
    ltz_table = _table("ts_ltz")
    create_or_overwrite(
        spark,
        _frame(spark, StructType([StructField("event_ts", TimestampNTZType(), True)]), [(INSTANT,)]),
        ntz_table,
    )
    create_or_overwrite(
        spark,
        _frame(spark, StructType([StructField("event_ts", TimestampType(), True)]), [(INSTANT,)]),
        ltz_table,
    )
    ntz = spark.table(ntz_table).collect()[0]["event_ts"]
    ltz = spark.table(ltz_table).collect()[0]["event_ts"]
    casted = spark.table(ntz_table).selectExpr("CAST(event_ts AS timestamp) AS event_ts").collect()[0]["event_ts"]
    actual = {"ntz": str(ntz), "ltz": str(ltz), "casted": str(casted)}
    _expect(ntz == INSTANT and ltz == INSTANT and casted == INSTANT, actual)
    return {
        "input": "2026-01-15 12:00:00 ghi timestamp_ntz và timestamp, session UTC",
        "expected": "cả ba lần đọc trả cùng giờ trên đồng hồ",
        "actual": actual,
    }


def case_nulls(spark) -> dict:
    table = _table("ratings_null")
    stored = _frame(
        spark,
        StructType([StructField("rating_id", LongType(), True), StructField("rating_val", DoubleType(), True)]),
        [(None, 4.0)],
    )
    create_or_overwrite(spark, stored, table)
    before = _snap(spark, table)
    actual_rows = spark.table(table)
    report = assess_table(spark, actual_rows, table, required=["rating_id"])
    take_schema_reports()
    status = create_or_overwrite(spark, actual_rows, table)
    actual = {
        "status": status,
        "snapshot_unchanged": before == _snap(spark, table),
        "nulls": [item for item in report["differences"] if item["kind"] == "null_value"],
        "overwrite": report["overwrite"],
        "table_nulls": spark.table(table).where("rating_id IS NULL").count(),
    }
    _expect(actual["table_nulls"] == 1, actual)
    _expect(actual["nulls"] == [{"column": "rating_id", "kind": "null_value", "nulls": 1, "constraint": "not_null"}], actual)
    _expect(status == "unchanged" and actual["snapshot_unchanged"] and report["overwrite"] is False, actual)
    return {
        "input": "Iceberg đang chứa rating_id NULL, hợp đồng yêu cầu not_null",
        "expected": "báo null_value từ giá trị thật, không overwrite",
        "actual": actual,
    }


def case_creation(spark) -> dict:
    ddl_namespace = "it_schema5d_ddl"
    execute_zone(spark, "silver", ddl_namespace)
    ddl_table = f"{ICEBERG_CATALOG}.{ddl_namespace}.stg_ratings"
    frame = _frame(
        spark,
        StructType(
            [
                StructField("rating_id", LongType(), True),
                StructField("rating_val", DoubleType(), True),
                StructField("event_date", DateType(), True),
                StructField("dt", DateType(), True),
            ]
        ),
        [(1, 4.0, INSTANT.date(), INSTANT.date())],
    )
    df_table = _table("ratings_created")
    create_or_overwrite(spark, frame, df_table)
    ddl_fields = iceberg_partition_fields(spark, ddl_table)
    df_fields = iceberg_partition_fields(spark, df_table)
    silver_sql = (REPO / "storage/lakehouse/silver/stg_tables.sql").read_text()
    expected = expected_partitions(silver_sql)["stg_ratings"]
    ddl_report = partition_difference(expected, ddl_fields)
    df_report = partition_difference(expected, df_fields)
    ddl_type = spark.table(ddl_table).schema["rating_val"].dataType.simpleString()
    df_type = spark.table(df_table).schema["rating_val"].dataType.simpleString()
    actual = {
        "ddl_type": ddl_type,
        "dataframe_type": df_type,
        "ddl_partition": ddl_report,
        "dataframe_partition": df_report,
    }
    _expect(ddl_type.replace(" ", "") == "decimal(2,1)", actual)
    _expect(df_type == "double", actual)
    _expect(ddl_report["actual_iceberg_fields"] == ["event_date"], actual)
    _expect(ddl_report["missing_partition_fields"] == [], actual)
    _expect(df_report["actual_iceberg_fields"] == [], actual)
    _expect(df_report["missing_partition_fields"] == ["event_date"], actual)
    _expect(df_report["source_folder_is_iceberg_partition"] == [], actual)
    _expect(ddl_report["repartition"] is False and df_report["repartition"] is False, actual)
    return {
        "input": "CREATE TABLE DDL và writeTo().create() cùng một frame có cột dt",
        "expected": "DDL có partition event_date và decimal. DataFrame create không partition và giữ double",
        "actual": actual,
    }


def _frame(spark, schema, rows):
    return spark.createDataFrame(rows, schema=schema)


def _table(name: str) -> str:
    return f"{ICEBERG_CATALOG}.{NAMESPACE}.{name}"


def _snap(spark, table: str) -> list:
    return snapshots(spark, table)


def _guard(warehouse: str) -> None:
    if not warehouse.startswith("file:"):
        raise SystemExit("schema test chỉ chạy trên warehouse file://")
    if NAMESPACE in {"bronze", "silver", "gold"}:
        raise SystemExit("namespace production bị cấm")


def _expect(condition: bool, actual: dict) -> None:
    if not condition:
        raise AssertionError(json.dumps(actual, default=str))


def _run(name: str, function) -> dict:
    try:
        result = function()
    except Exception as exc:
        return {"name": name, "status": "FAIL", "error": f"{exc.__class__.__name__}: {exc}", "trace": traceback.format_exc()}
    result["name"] = name
    result["status"] = "PASS"
    return result


def _production_fingerprint() -> dict:
    paths = (
        "namespaces/silver/tables/stg_ratings",
        "namespaces/silver/tables/stg_watch_sessions",
        "namespaces/gold/tables/fact_ratings",
        "namespaces/gold/tables/dim_movies",
    )
    found = {}
    for path in paths:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/{path}", timeout=2) as response:
                payload = json.loads(response.read().decode())
                metadata = payload["metadata"]
                found[path] = {
                    "snapshot": metadata.get("current-snapshot-id"),
                    "schema": metadata.get("current-schema-id"),
                }
        except Exception as exc:
            found[path] = f"unavailable:{exc.__class__.__name__}"
    return found


if __name__ == "__main__":
    main()
