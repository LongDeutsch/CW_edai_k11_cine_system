"""Spark local dùng chung cho baseline, salting và gold.

PySpark 3.5 chạy trên Python 3.12. Makefile gọi ``uv run --python 3.12``
nên các module này không nằm trong venv 3.13 của project.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

EVOLVED_COLUMNS = ("content_rating", "hdr_format", "audio_spatial")
DEFAULT_SOURCE = "data_generator/output/scale_0_1"
DEFAULT_S3_SOURCE = "s3a://external-source-lake"
DEFAULT_S3_ENDPOINT = "http://127.0.0.1:9000"
DEFAULT_WAREHOUSE = "s3a://lakehouse"
DEFAULT_ICEBERG_REST = "http://127.0.0.1:8181"
ICEBERG_CATALOG = "cinestream"
_REPO_ROOT = Path(__file__).resolve().parents[2]
_S3A_JARS = (
    (
        "hadoop-aws-3.3.4.jar",
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    ),
    (
        "aws-java-sdk-bundle-1.12.262.jar",
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
    ),
    (
        "iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
    ),
)


class DataSource:
    """Đường dẫn local hoặc ``s3a://bucket`` dùng cùng phép ``/``."""

    def __init__(self, root: str):
        self.root = root.rstrip("/")
        self.remote = self.root.startswith(("s3a://", "s3://"))

    def __truediv__(self, part: str) -> "DataSource":
        return DataSource(f"{self.root}/{str(part).strip('/')}")

    def __str__(self) -> str:
        return self.root

    def is_file(self) -> bool:
        if self.remote:
            return self.root.endswith(".parquet")
        return Path(self.root).is_file()


@dataclass(frozen=True)
class PartitionStat:
    partition_id: int
    rows: int
    share: float


def build_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--source-dir",
        default=DEFAULT_SOURCE,
        help="Thư mục local hoặc URI s3a://external-source-lake.",
    )
    parser.add_argument("--s3-endpoint", default=DEFAULT_S3_ENDPOINT)
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    parser.add_argument(
        "--iceberg-rest",
        default=DEFAULT_ICEBERG_REST,
        help="URI REST catalog. Warehouse mặc định là s3a://lakehouse/.",
    )
    parser.add_argument("--master", default="local[*]", help="local[*] để đo trên CPU máy này.")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--salt-buckets", type=int, default=16)
    parser.add_argument("--enable-aqe", action="store_true", help="Bật Adaptive Query Execution (AQE) cho Spark.")
    parser.add_argument(
        "--aqe-skew-factor",
        type=int,
        default=2,
        help="Hệ số coi là skewed partition so với median (mặc định 2).",
    )
    parser.add_argument(
        "--aqe-skew-threshold-bytes",
        type=int,
        default=131072,
        help="Ngưỡng bytes coi là skewed partition (mặc định 128KB để kích hoạt trên dữ liệu mẫu scale 0.1).",
    )
    parser.add_argument(
        "--aqe-advisory-size-bytes",
        type=int,
        default=131072,
        help="Kích thước mong muốn khi AQE chia nhỏ partition bị lệch (mặc định 128KB).",
    )
    parser.add_argument("--ui-port", type=int, default=4040)
    parser.add_argument("--no-ui", action="store_true", help="Tắt Spark UI để job thoát nhanh hơn.")
    parser.add_argument(
        "--hold-seconds",
        type=int,
        default=0,
        help="Giữ process sau khi tính xong để mở Spark UI. 0 thì thoát ngay.",
    )
    return parser


def require_source(source: str | Path) -> DataSource:
    located = DataSource(str(source))
    if located.remote:
        return located
    resolved = DataSource(str(Path(located.root).resolve()))
    ratings = Path(str(resolved / "ratings"))
    movies = Path(str(resolved / "movies"))
    if not ratings.is_dir() or not movies.is_dir():
        raise SystemExit(
            f"không thấy ratings/ và movies/ trong {resolved}. Chạy make gen-offline trước."
        )
    return resolved


def uses_iceberg(args: argparse.Namespace) -> bool:
    warehouse = str(getattr(args, "warehouse", ""))
    return warehouse.startswith(("s3a://", "s3://"))


def _classpath(include_iceberg: bool) -> str:
    jar_dir = _REPO_ROOT / "infra" / "spark" / "jars"
    jar_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for name, url in _S3A_JARS:
        if not include_iceberg and name.startswith("iceberg-"):
            continue
        path = jar_dir / name
        if not path.is_file() or path.stat().st_size < 1000:
            import urllib.request

            print(f"tải {name}")
            urllib.request.urlretrieve(url, path)
        paths.append(path)
    return ",".join(str(path) for path in paths)


def _apply_s3a(builder, args: argparse.Namespace, jars: str):
    import os

    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {jars} pyspark-shell"
    endpoint = getattr(args, "s3_endpoint", DEFAULT_S3_ENDPOINT)
    return (
        builder.config("spark.jars", jars)
        .config("spark.driver.extraClassPath", jars.replace(",", ":"))
        .config("spark.executor.extraClassPath", jars.replace(",", ":"))
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
    )


def _apply_iceberg(builder, args: argparse.Namespace):
    catalog = ICEBERG_CATALOG
    warehouse = str(args.warehouse).rstrip("/") + "/"
    rest = getattr(args, "iceberg_rest", DEFAULT_ICEBERG_REST)
    return (
        builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", rest)
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.defaultCatalog", catalog)
    )


def start_spark(app_name: str, args: argparse.Namespace, enable_aqe: bool | None = None):
    if args.shuffle_partitions < 1:
        raise SystemExit("--shuffle-partitions phải >= 1")
    if args.salt_buckets < 1:
        raise SystemExit("--salt-buckets phải >= 1")
    if args.hold_seconds < 0:
        raise SystemExit("--hold-seconds phải >= 0")

    use_aqe = enable_aqe if enable_aqe is not None else getattr(args, "enable_aqe", False)

    from pyspark.sql import SparkSession

    local_scratch = _REPO_ROOT / "data_generator" / "output" / "lakehouse"
    local_scratch.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.master(args.master)
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false" if args.no_ui else "true")
        .config("spark.ui.port", str(args.ui_port))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.warehouse.dir", str(local_scratch / "_spark"))
    )
    remote_source = DataSource(str(getattr(args, "source_dir", ""))).remote
    iceberg = uses_iceberg(args)
    if remote_source or iceberg:
        builder = _apply_s3a(builder, args, _classpath(include_iceberg=iceberg))
    if iceberg:
        builder = _apply_iceberg(builder, args)

    if use_aqe:
        skew_factor = str(getattr(args, "aqe_skew_factor", 2))
        skew_threshold = str(getattr(args, "aqe_skew_threshold_bytes", 131072))
        advisory_size = str(getattr(args, "aqe_advisory_size_bytes", 131072))
        builder = (
            builder.config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", skew_factor)
            .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", skew_threshold)
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", advisory_size)
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        )
    else:
        builder = (
            builder.config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.skewJoin.enabled", "false")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def hold_ui(spark, args: argparse.Namespace) -> None:
    if args.hold_seconds <= 0 or args.no_ui:
        return
    import time

    print(
        f"Spark UI đang mở tại http://127.0.0.1:{args.ui_port} "
        f"trong {args.hold_seconds} giây. Vào tab Stages để xem task lệch."
    )
    time.sleep(args.hold_seconds)
    spark.stop()


def partition_stats(frame) -> list[PartitionStat]:
    from pyspark.sql import functions as F

    counted = frame.groupBy(F.spark_partition_id().alias("partition_id")).count().collect()
    total = sum(int(row["count"]) for row in counted) or 1
    ordered = sorted(counted, key=lambda row: int(row["partition_id"]))
    return [
        PartitionStat(
            partition_id=int(row["partition_id"]),
            rows=int(row["count"]),
            share=int(row["count"]) / total,
        )
        for row in ordered
    ]


def format_partitions(stats: list[PartitionStat]) -> str:
    lines = [f"{'partition':>9}  {'rows':>10}  {'share':>8}"]
    for stat in stats:
        lines.append(f"{stat.partition_id:>9}  {stat.rows:>10,}  {stat.share:>7.2%}")
    if stats:
        hottest = max(stats, key=lambda stat: stat.rows)
        lines.append(
            f"nặng nhất: partition {hottest.partition_id} = {hottest.rows:,} dòng ({hottest.share:.2%})"
        )
    return "\n".join(lines)


def movie_42_share(frame) -> tuple[int, int]:
    """Số dòng movie_id=42 và số Spark partition đang chứa phim đó."""
    from pyspark.sql import functions as F

    rows = (
        frame.where(F.col("movie_id") == 42)
        .groupBy(F.spark_partition_id().alias("partition_id"))
        .count()
        .collect()
    )
    return sum(int(row["count"]) for row in rows), len(rows)


def dedup_ratings(ratings):
    """Giữ rating_ts mới nhất cho mỗi (user_id, movie_id, rating_val)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("user_id", "movie_id", "rating_val").orderBy(
        F.col("rating_ts").desc(),
        F.col("rating_id").desc(),
    )
    return ratings.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")
