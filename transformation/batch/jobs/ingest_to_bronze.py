"""MinIO hoặc Parquet local → Bronze Iceberg, append và không ghi trùng file.

Định danh file là ``đường dẫn|kích thước|mtime``, không phải Airflow run id.
Rerun cùng file thì không append. File mới cùng khóa logic vẫn vào Bronze;
Silver mới khử replay. Một dataset commit một lần. Dataset sau chưa chạy
thì retry chạy tiếp, dataset trước không bị ghi lại.

Users và movies là snapshot ``partition_*.parquet``. Ratings và watch là
sự kiện trong ``dt=YYYY-MM-DD``, với ``dt`` bằng ``event_date``. ``_ingest_ts``
là lúc commit Bronze. Mốc hiệu lực SCD2 không lấy từ mtime của file.
"""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from transformation.batch.common import ICEBERG_CATALOG, build_parser, require_source, start_spark
from transformation.batch.contracts.validators import (
    CONTRACTS,
    DEFAULT_ERROR_RATE_THRESHOLD,
    ContractError,
    check_batch_id,
    check_identifier,
    normalize_timestamps,
    record_quarantine_failure,
    replace_quarantine,
    upsert_quality,
    validate_frame,
)
from transformation.batch.jobs.lakehouse_ddl import ensure_bronze_audit_columns, execute_zone
from transformation.batch.jobs.table_updates import append_new_files, table_exists

_ORDER = ("users", "movies", "ratings", "watch")
_INT32 = {"release_year", "runtime_minutes", "watch_duration_seconds"}


class SimulatedTaskFailure(RuntimeError):
    """Dừng có chủ đích sau một dataset đã commit, để thử retry."""


@dataclass(frozen=True)
class SourceFile:
    path: str
    identity: str


def ingest(
    spark,
    source: str,
    namespace: str,
    *,
    batch_id: str = "",
    run_id: str = "",
    threshold: float = DEFAULT_ERROR_RATE_THRESHOLD,
    fail_after: str = "",
) -> dict:
    """Đọc bốn dataset. Trả về số dòng append và trạng thái từng dataset."""
    check_identifier(namespace, "namespace")
    if fail_after and fail_after not in _ORDER:
        raise ValueError("fail_after phải là users, movies, ratings hoặc watch")
    execute_zone(spark, "bronze", namespace)
    ensure_bronze_audit_columns(spark, namespace)
    discovered = {name: discover_parquet(spark, f"{str(source).rstrip('/')}/{CONTRACTS[name].folder}") for name in _ORDER}
    resolved_batch = check_batch_id(batch_id) if batch_id else _batch_id(discovered)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    summary = {"batch_id": resolved_batch, "run_id": run_id, "datasets": {}}
    for name in _ORDER:
        summary["datasets"][name] = _ingest_dataset(
            spark,
            CONTRACTS[name],
            discovered[name],
            namespace,
            resolved_batch,
            run_id,
            threshold,
            now,
        )
        if fail_after == name:
            raise SimulatedTaskFailure(f"simulated failure after {name}")
    return summary


def discover_parquet(spark, root: str) -> list[SourceFile]:
    """Liệt kê Parquet thật. Không giả định mọi dataset đều có ``dt=``."""
    if root.startswith(("s3a://", "s3://", "file:")):
        return _discover_hadoop(spark, root)
    folder = Path(root)
    if not folder.exists():
        return []
    found = []
    for path in sorted(folder.rglob("*.parquet")):
        stat = path.stat()
        identity = f"{path.resolve()}|{stat.st_size}|{stat.st_mtime_ns}"
        found.append(SourceFile(str(path.resolve()), identity))
    return found


def _discover_hadoop(spark, root: str) -> list[SourceFile]:
    jvm = spark._jvm
    base = jvm.org.apache.hadoop.fs.Path(root)
    filesystem = base.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not filesystem.exists(base):
        return []
    found: list[SourceFile] = []

    def walk(path) -> None:
        for status in filesystem.listStatus(path):
            if status.isDirectory():
                walk(status.getPath())
            elif status.getPath().getName().endswith(".parquet"):
                uri = str(status.getPath())
                identity = f"{uri}|{status.getLen()}|{status.getModificationTime()}"
                found.append(SourceFile(uri, identity))

    walk(base)
    return sorted(found, key=lambda item: item.path)


def _batch_id(discovered: dict[str, list[SourceFile]]) -> str:
    identities = [item.identity for files in discovered.values() for item in files]
    if not identities:
        return "batch-empty"
    digest = hashlib.sha256("\n".join(sorted(identities)).encode()).hexdigest()[:16]
    return f"batch-{digest}"


def _ingest_dataset(spark, contract, files: list[SourceFile], namespace: str, batch_id: str, run_id: str, threshold: float, now) -> dict:
    from pyspark.sql import functions as F

    table = f"{ICEBERG_CATALOG}.{namespace}.{contract.table}"
    pending = _pending(spark, table, files)
    if not pending:
        upsert_quality(spark, namespace, contract.name, batch_id, run_id, 0, 0, "skipped_duplicate", now)
        return {"appended": 0, "invalid": 0, "total": 0, "status": "skipped_duplicate"}

    frames = []
    for item in pending:
        try:
            reader = normalize_timestamps(spark.read.option("mergeSchema", "true").parquet(item.path), contract)
        except ContractError as exc:
            record_quarantine_failure(
                spark, namespace, contract.name, batch_id, item.path, "SCHEMA_VIOLATION", str(exc), now
            )
            upsert_quality(spark, namespace, contract.name, batch_id, run_id, 0, 1, "schema_rejected", now)
            raise
        except Exception as exc:
            record_quarantine_failure(
                spark, namespace, contract.name, batch_id, item.path, "UNREADABLE_FILE", str(exc), now
            )
            upsert_quality(spark, namespace, contract.name, batch_id, run_id, 0, 1, "unreadable", now)
            raise ContractError(f"{contract.name}: không đọc được {item.path}") from exc
        frames.append(reader.withColumn("_source_file", F.lit(item.path)).withColumn("_file_id", F.lit(item.identity)))

    frame = frames[0]
    for part in frames[1:]:
        frame = frame.unionByName(part, allowMissingColumns=True)
    ignored = sorted(set(frame.columns) - set(contract.required) - set(contract.optional) - {"_source_file", "_file_id"})
    users, movies = _references(spark, namespace, contract.name)
    valid, invalid = validate_frame(frame, contract, users, movies)
    valid = valid.cache()
    invalid = invalid.cache()
    try:
        total = valid.count() + invalid.count()
        invalid_rows = invalid.count()
        replace_quarantine(spark, namespace, contract.name, batch_id, invalid, now)
        try:
            from transformation.batch.contracts.validators import QualityThresholdExceeded, assert_within_threshold

            rate = assert_within_threshold(contract.name, invalid_rows, total, threshold)
            status = "accepted" if not ignored else "accepted_ignored:" + ",".join(ignored)
        except QualityThresholdExceeded:
            upsert_quality(spark, namespace, contract.name, batch_id, run_id, total, invalid_rows, "threshold_rejected", now)
            raise
        projected = _project(valid, contract, batch_id, now)
        appended = append_new_files(spark, projected, table)
        upsert_quality(
            spark,
            namespace,
            contract.name,
            batch_id,
            run_id,
            total,
            invalid_rows,
            status,
            now,
        )
        return {
            "appended": appended,
            "invalid": invalid_rows,
            "total": total,
            "error_rate": rate,
            "status": status,
            "ignored_columns": ignored,
        }
    finally:
        valid.unpersist()
        invalid.unpersist()


def _pending(spark, table: str, files: list[SourceFile]) -> list[SourceFile]:
    if not files or not table_exists(spark, table):
        return list(files)
    existing = {row["_file_id"] for row in spark.table(table).select("_file_id").distinct().collect()}
    return [item for item in files if item.identity not in existing]


def _references(spark, namespace: str, dataset: str):
    if dataset not in {"ratings", "watch"}:
        return None, None
    return _keys(spark, namespace, "raw_users", "user_id"), _keys(spark, namespace, "raw_movies", "movie_id")


def _keys(spark, namespace: str, table: str, column: str):
    from pyspark.sql.types import LongType, StructField, StructType

    full_name = f"{ICEBERG_CATALOG}.{namespace}.{table}"
    if not table_exists(spark, full_name):
        return spark.createDataFrame([], StructType([StructField(column, LongType(), True)]))
    from pyspark.sql import functions as F

    return spark.table(full_name).select(F.col(column).cast("long").alias(column))


def _project(frame, contract, batch_id: str, now):
    from pyspark.sql import functions as F

    columns = []
    for name in (*contract.required, *contract.optional):
        if name not in frame.columns:
            columns.append(F.lit(None).cast(_sql_type(name)).alias(name))
            continue
        columns.append(F.col(name).cast(_sql_type(name)).alias(name))
    columns.extend(
        [
            F.lit(now).cast("timestamp").alias("_ingest_ts"),
            F.col("_source_file").cast("string").alias("_source_file"),
            F.lit(batch_id).alias("_batch_id"),
            F.col("_file_id").cast("string").alias("_file_id"),
        ]
    )
    return frame.select(*columns)


def _sql_type(name: str) -> str:
    if name in _INT32:
        return "int"
    if name in {"rating_val", "completion_rate"}:
        return "double"
    if name.endswith("_ts"):
        return "timestamp"
    if name == "event_date":
        return "date"
    if name == "audio_spatial":
        return "boolean"
    if name.endswith("_id") or name in {"user_id", "budget"}:
        return "long"
    return "string"


def main(argv: list[str] | None = None) -> None:
    parser = build_parser("Đưa Parquet nguồn vào Bronze Iceberg, append theo file và không ghi trùng.")
    parser.add_argument("--warehouse", default="s3a://lakehouse")
    parser.add_argument("--namespace", default="bronze")
    parser.add_argument("--batch-id", default="")
    parser.add_argument("--run-id", default="", help="Định danh lần chạy. Không tham gia khóa chống trùng.")
    parser.add_argument("--error-threshold", type=float, default=DEFAULT_ERROR_RATE_THRESHOLD)
    parser.add_argument("--fail-after", default="", help="Chỉ dùng khi thử retry: users, movies, ratings, watch.")
    args = parser.parse_args(argv)
    source = require_source(args.source_dir)
    spark = start_spark("cinestream-bronze", args)
    try:
        summary = ingest(
            spark,
            str(source),
            args.namespace,
            batch_id=args.batch_id,
            run_id=args.run_id,
            threshold=args.error_threshold,
            fail_after=args.fail_after,
        )
        print(f"batch_id={summary['batch_id']} run_id={summary['run_id'] or '-'}")
        for name, item in summary["datasets"].items():
            print(f"  {name:<8} appended={item['appended']:<6} invalid={item['invalid']:<4} status={item['status']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
