"""Đọc DDL Iceberg và tạo cùng bảng trong một namespace cách ly."""

from __future__ import annotations

from pathlib import Path

from transformation.batch.contracts.validators import check_identifier

_ROOT = Path(__file__).resolve().parents[3]
_SCRIPTS = {
    "bronze": _ROOT / "storage" / "lakehouse" / "bronze" / "raw_tables.sql",
    "silver": _ROOT / "storage" / "lakehouse" / "silver" / "stg_tables.sql",
    "gold": _ROOT / "storage" / "lakehouse" / "gold" / "gold_tables.sql",
}


def execute_zone(spark, zone: str, namespace: str) -> None:
    """Chạy DDL của zone, đổi namespace ``bronze`` / ``silver`` / ``gold``."""
    check_identifier(namespace, "namespace")
    text = _SCRIPTS[zone].read_text()
    text = text.replace(f"cinestream.{zone}", f"cinestream.{namespace}")
    for statement in _statements(text):
        spark.sql(statement)


def ensure_bronze_audit_columns(spark, namespace: str) -> None:
    """Thêm danh tính batch và file. DDL gốc chỉ có ``_ingest_ts`` và ``_source_file``."""
    check_identifier(namespace, "namespace")
    for table in ("raw_users", "raw_movies", "raw_ratings", "raw_watch_history"):
        full_name = f"cinestream.{namespace}.{table}"
        if not spark.catalog.tableExists(full_name):
            continue
        present = set(spark.table(full_name).columns)
        for column in ("_batch_id", "_file_id"):
            if column not in present:
                spark.sql(f"ALTER TABLE {full_name} ADD COLUMN {column} STRING")


def _statements(text: str) -> list[str]:
    buffered: list[str] = []
    statements: list[str] = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        buffered.append(line)
        if line.strip().endswith(";"):
            statement = "\n".join(buffered).strip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            buffered = []
    return statements
