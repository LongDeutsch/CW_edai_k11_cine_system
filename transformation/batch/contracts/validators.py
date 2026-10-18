"""Kiểm tra hợp đồng dòng và schema trước khi ghi Silver hoặc Gold.

Ngưỡng lỗi dòng mặc định là 5%. Vượt ngưỡng thì ghi quarantine rồi fail.
Schema thiếu cột bắt buộc hoặc sai kiểu khóa thì fail ngay, không ghi Bronze.

Grain khóa nghiệp vụ dùng ở đây là khóa của một bản ghi nguồn:

- users: ``user_id``
- movies: ``movie_id``
- ratings: ``rating_id``
- watch: ``session_id``

Hai rating khác ``rating_id`` nhưng trùng ``(user_id, movie_id, rating_val)``
là replay hợp lệ của generator. Chúng vào Bronze và bị khử ở Silver.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

DEFAULT_ERROR_RATE_THRESHOLD = 0.05
_IDENT = re.compile(r"[A-Za-z0-9_]+")
_BATCH = re.compile(r"[A-Za-z0-9_.:-]{1,160}")

DESCRIPTIONS = {
    "NULL_BUSINESS_KEY": "Khóa nghiệp vụ bị null.",
    "RATING_RANGE": "rating_val phải nằm trong [0.5, 5.0].",
    "NEGATIVE_DURATION": "watch_duration_seconds phải >= 0.",
    "COMPLETION_RANGE": "completion_rate phải nằm trong [0, 1].",
    "FK_USER": "user_id không có trong users đã biết.",
    "FK_MOVIE": "movie_id không có trong movies đã biết.",
    "DUPLICATE_BUSINESS_KEY": "Trùng khóa nghiệp vụ trong cùng một batch.",
    "UNREADABLE_FILE": "Không đọc được file Parquet.",
    "SCHEMA_VIOLATION": "Schema không tương thích với hợp đồng.",
}


class ContractError(RuntimeError):
    """Schema không tương thích. Cả batch của dataset đó dừng."""


class QualityThresholdExceeded(RuntimeError):
    """Tỷ lệ lỗi dòng vượt ngưỡng. Silver và Gold không được ghi."""

    def __init__(self, dataset: str, invalid_rows: int, total_rows: int, error_rate: float, threshold: float):
        self.dataset = dataset
        self.invalid_rows = invalid_rows
        self.total_rows = total_rows
        self.error_rate = error_rate
        self.threshold = threshold
        super().__init__(
            f"{dataset}: error_rate={error_rate:.4f} vượt ngưỡng {threshold:.4f} "
            f"({invalid_rows}/{total_rows})"
        )


@dataclass(frozen=True)
class DatasetContract:
    name: str
    folder: str
    table: str
    required: tuple[str, ...]
    business_keys: tuple[str, ...]
    optional: tuple[str, ...] = ()
    integral: tuple[str, ...] = ()
    timestamps: tuple[str, ...] = ()
    dates: tuple[str, ...] = ()
    numeric: tuple[str, ...] = ()


USERS = DatasetContract(
    name="users",
    folder="users",
    table="raw_users",
    required=(
        "user_id",
        "signup_ts",
        "country",
        "subscription_tier",
        "preferred_language",
        "age_group",
        "gender",
    ),
    business_keys=("user_id",),
    integral=("user_id",),
    timestamps=("signup_ts",),
)
MOVIES = DatasetContract(
    name="movies",
    folder="movies",
    table="raw_movies",
    required=(
        "movie_id",
        "title",
        "release_year",
        "runtime_minutes",
        "original_language",
        "budget",
        "director_id",
        "created_ts",
    ),
    optional=("content_rating", "hdr_format", "audio_spatial"),
    business_keys=("movie_id",),
    integral=("movie_id", "release_year", "runtime_minutes", "budget", "director_id"),
    timestamps=("created_ts",),
)
RATINGS = DatasetContract(
    name="ratings",
    folder="ratings",
    table="raw_ratings",
    required=("rating_id", "user_id", "movie_id", "rating_val", "rating_ts", "review_text", "event_date"),
    business_keys=("rating_id",),
    integral=("rating_id", "user_id", "movie_id"),
    numeric=("rating_val",),
    timestamps=("rating_ts",),
    dates=("event_date",),
)
WATCH = DatasetContract(
    name="watch",
    folder="watch_history",
    table="raw_watch_history",
    required=(
        "session_id",
        "user_id",
        "movie_id",
        "start_ts",
        "end_ts",
        "watch_duration_seconds",
        "completion_rate",
        "device_type",
        "resolution",
        "event_date",
    ),
    business_keys=("session_id",),
    integral=("session_id", "user_id", "movie_id", "watch_duration_seconds"),
    numeric=("completion_rate",),
    timestamps=("start_ts", "end_ts"),
    dates=("event_date",),
)
CONTRACTS = {
    USERS.name: USERS,
    MOVIES.name: MOVIES,
    RATINGS.name: RATINGS,
    WATCH.name: WATCH,
}


def check_identifier(value: str, label: str) -> str:
    if not _IDENT.fullmatch(value or ""):
        raise ValueError(f"{label} không hợp lệ: {value!r}")
    return value


def check_batch_id(value: str) -> str:
    if not _BATCH.fullmatch(value or ""):
        raise ValueError(f"batch_id không hợp lệ: {value!r}")
    return value


def check_schema(schema, contract: DatasetContract) -> None:
    """Fail cả batch nếu thiếu cột bắt buộc hoặc sai họ kiểu.

    ``timestamp`` và ``timestamp_ntz`` đều là mốc thời gian. ``timestamp_ntz``
    là Parquet không gắn offset. String, số và date không được nhận vào cột timestamp.
    """
    from pyspark.sql.types import (
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        TimestampNTZType,
        TimestampType,
    )

    fields = {field.name: field for field in schema.fields}
    missing = [name for name in contract.required if name not in fields]
    if missing:
        raise ContractError(f"{contract.name}: thiếu cột bắt buộc {missing}")
    integral = (IntegerType, LongType)
    numeric = (IntegerType, LongType, DoubleType, FloatType, DecimalType)
    timestamps = (TimestampType, TimestampNTZType)
    for name in contract.integral:
        if name in fields and not isinstance(fields[name].dataType, integral):
            raise ContractError(f"{contract.name}: cột {name} phải là số nguyên")
    for name in contract.numeric:
        if name in fields and not isinstance(fields[name].dataType, numeric):
            raise ContractError(f"{contract.name}: cột {name} phải là số")
    for name in contract.timestamps:
        if name in fields and not isinstance(fields[name].dataType, timestamps):
            raise ContractError(f"{contract.name}: cột {name} phải là timestamp")
    for name in contract.dates:
        if name in fields and not isinstance(fields[name].dataType, DateType):
            raise ContractError(f"{contract.name}: cột {name} phải là date")


def normalize_timestamps(frame, contract: DatasetContract):
    """Đưa ``timestamp_ntz`` về ``timestamp`` theo UTC. Không cast kiểu khác.

    Generator ghi ``datetime64`` UTC và Parquet để ``isAdjustedToUTC=false``.
    Spark 3.5 đọc thành ``timestamp_ntz``. Session của ``start_spark`` là UTC,
    nên cast giữ nguyên giờ trên đồng hồ, đúng instant UTC.
    """
    check_schema(frame.schema, contract)
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampNTZType

    result = frame
    for name in contract.timestamps:
        field = next(item for item in result.schema.fields if item.name == name)
        if isinstance(field.dataType, TimestampNTZType):
            result = result.withColumn(name, F.col(name).cast("timestamp"))
    return result


def validate_frame(frame, contract: DatasetContract, users=None, movies=None):
    """Tách dòng hợp lệ và dòng lỗi. Mỗi dòng lỗi mang đúng một mã."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    check_schema(frame.schema, contract)
    work = frame
    for column in contract.optional:
        if column not in work.columns:
            work = work.withColumn(column, F.lit(None))
    keys_null = None
    for key in contract.business_keys:
        check = F.col(key).isNull()
        keys_null = check if keys_null is None else (keys_null | check)
    if contract.name in {"ratings", "watch"}:
        keys_null = keys_null | F.col("user_id").isNull() | F.col("movie_id").isNull()

    code = F.when(keys_null, F.lit("NULL_BUSINESS_KEY"))
    if contract.name == "ratings":
        code = code.when(
            F.col("rating_val").isNull() | (F.col("rating_val") < F.lit(0.5)) | (F.col("rating_val") > F.lit(5.0)),
            F.lit("RATING_RANGE"),
        )
    if contract.name == "watch":
        code = code.when(
            F.col("watch_duration_seconds").isNull() | (F.col("watch_duration_seconds") < F.lit(0)),
            F.lit("NEGATIVE_DURATION"),
        )
        code = code.when(
            F.col("completion_rate").isNull()
            | (F.col("completion_rate") < F.lit(0.0))
            | (F.col("completion_rate") > F.lit(1.0)),
            F.lit("COMPLETION_RANGE"),
        )
    if contract.name in {"ratings", "watch"} and users is not None:
        known_users = users.select(F.col("user_id").alias("_known_user")).where(F.col("_known_user").isNotNull()).distinct()
        work = work.join(known_users, work["user_id"] == known_users["_known_user"], "left")
        code = code.when(F.col("_known_user").isNull(), F.lit("FK_USER"))
    if contract.name in {"ratings", "watch"} and movies is not None:
        known_movies = movies.select(F.col("movie_id").alias("_known_movie")).where(F.col("_known_movie").isNotNull()).distinct()
        work = work.join(known_movies, work["movie_id"] == known_movies["_known_movie"], "left")
        code = code.when(F.col("_known_movie").isNull(), F.lit("FK_MOVIE"))

    order = F.lit(1)
    if contract.name == "ratings":
        order = F.col("rating_ts").desc()
    elif contract.name == "watch":
        order = F.col("start_ts").desc()
    elif contract.name == "users":
        order = F.col("signup_ts").desc()
    elif contract.name == "movies":
        order = F.col("created_ts").desc()
    window = Window.partitionBy(*contract.business_keys).orderBy(order)
    work = work.withColumn("_rn", F.row_number().over(window))
    code = code.when(F.col("_rn") > 1, F.lit("DUPLICATE_BUSINESS_KEY"))
    described = F.create_map(*[item for pair in DESCRIPTIONS.items() for item in (F.lit(pair[0]), F.lit(pair[1]))])
    work = work.withColumn("_error_code", code).withColumn(
        "_error_description",
        F.when(F.col("_error_code").isNull(), F.lit(None)).otherwise(described[F.col("_error_code")]),
    )
    valid = _drop_helpers(work.where(F.col("_error_code").isNull()))
    invalid = work.where(F.col("_error_code").isNotNull())
    return valid, invalid


def _drop_helpers(frame):
    helpers = [name for name in ("_error_code", "_error_description", "_rn", "_known_user", "_known_movie") if name in frame.columns]
    if not helpers:
        return frame
    return frame.drop(*helpers)


def error_rate(invalid_rows: int, total_rows: int) -> float:
    if total_rows <= 0:
        return 0.0
    return invalid_rows / total_rows


def assert_within_threshold(dataset: str, invalid_rows: int, total_rows: int, threshold: float) -> float:
    rate = error_rate(invalid_rows, total_rows)
    if rate > threshold:
        raise QualityThresholdExceeded(dataset, invalid_rows, total_rows, rate, threshold)
    return rate


def quarantine_table(namespace: str) -> str:
    check_identifier(namespace, "namespace")
    return f"cinestream.{namespace}.quarantine_records"


def quality_table(namespace: str) -> str:
    check_identifier(namespace, "namespace")
    return f"cinestream.{namespace}.quality_results"


def ensure_audit_tables(spark, namespace: str) -> None:
    check_identifier(namespace, "namespace")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS cinestream.{namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {quarantine_table(namespace)} (
            dataset STRING,
            batch_id STRING,
            source_file STRING,
            business_key STRING,
            error_code STRING,
            error_description STRING,
            raw_payload STRING,
            quarantined_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (dataset, batch_id)
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {quality_table(namespace)} (
            dataset STRING,
            batch_id STRING,
            run_id STRING,
            total_rows BIGINT,
            invalid_rows BIGINT,
            error_rate DOUBLE,
            status STRING,
            checked_ts TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def replace_quarantine(spark, namespace: str, dataset: str, batch_id: str, invalid, quarantined_ts) -> int:
    """Ghi lại đúng phần quarantine của một dataset và một batch."""
    from pyspark.sql import functions as F

    check_batch_id(batch_id)
    ensure_audit_tables(spark, namespace)
    table = quarantine_table(namespace)
    spark.sql(f"DELETE FROM {table} WHERE dataset = '{dataset}' AND batch_id = '{batch_id}'")
    count = invalid.count()
    if count == 0:
        return 0
    payload_columns = [
        name
        for name in invalid.columns
        if name not in {"_error_code", "_error_description", "_rn", "_known_user", "_known_movie"}
        and not name.startswith("_")
    ]
    key_expr = F.concat_ws("|", *[F.col(name).cast("string") for name in CONTRACTS[dataset].business_keys])
    staged = invalid.select(
        F.lit(dataset).alias("dataset"),
        F.lit(batch_id).alias("batch_id"),
        F.coalesce(F.col("_source_file").cast("string"), F.lit("")).alias("source_file")
        if "_source_file" in invalid.columns
        else F.lit("").alias("source_file"),
        key_expr.alias("business_key"),
        F.col("_error_code").alias("error_code"),
        F.col("_error_description").alias("error_description"),
        F.to_json(F.struct(*[F.col(name) for name in payload_columns])).alias("raw_payload"),
        F.lit(quarantined_ts).cast("timestamp").alias("quarantined_ts"),
    )
    staged.writeTo(table).append()
    return count


def record_quarantine_failure(spark, namespace: str, dataset: str, batch_id: str, source_file: str, code: str, description: str, quarantined_ts) -> None:
    from pyspark.sql import functions as F

    ensure_audit_tables(spark, namespace)
    table = quarantine_table(namespace)
    spark.sql(f"DELETE FROM {table} WHERE dataset = '{dataset}' AND batch_id = '{batch_id}'")
    row = spark.range(1).select(
        F.lit(dataset).alias("dataset"),
        F.lit(batch_id).alias("batch_id"),
        F.lit(source_file).alias("source_file"),
        F.lit("*").alias("business_key"),
        F.lit(code).alias("error_code"),
        F.lit(description).alias("error_description"),
        F.lit(source_file).alias("raw_payload"),
        F.lit(quarantined_ts).cast("timestamp").alias("quarantined_ts"),
    )
    row.writeTo(table).append()


def upsert_quality(spark, namespace: str, dataset: str, batch_id: str, run_id: str, total_rows: int, invalid_rows: int, status: str, checked_ts) -> None:
    from pyspark.sql import functions as F

    check_batch_id(batch_id)
    ensure_audit_tables(spark, namespace)
    table = quality_table(namespace)
    rate = error_rate(invalid_rows, total_rows)
    incoming = spark.range(1).select(
        F.lit(dataset).alias("dataset"),
        F.lit(batch_id).alias("batch_id"),
        F.lit(run_id).alias("run_id"),
        F.lit(int(total_rows)).cast("long").alias("total_rows"),
        F.lit(int(invalid_rows)).cast("long").alias("invalid_rows"),
        F.lit(float(rate)).alias("error_rate"),
        F.lit(status).alias("status"),
        F.lit(checked_ts).cast("timestamp").alias("checked_ts"),
    )
    incoming.createOrReplaceTempView("quality_incoming")
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING quality_incoming s
        ON t.dataset = s.dataset AND t.batch_id = s.batch_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
