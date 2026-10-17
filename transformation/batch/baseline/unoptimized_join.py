"""Join ratings với movies theo movie_id, không hợp schema và không salt.

Shuffle theo ``movie_id`` dồn khoảng 80% dòng của movie_id=42 vào một
Spark partition. Đó là task chạy lâu hơn các task còn lại.

Ví dụ::

    python -m transformation.batch.baseline.unoptimized_join --shuffle-partitions 16
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from transformation.batch.common import (
    EVOLVED_COLUMNS,
    format_partitions,
    hold_ui,
    movie_42_share,
    partition_stats,
    require_source,
    start_spark,
    build_parser,
)


@dataclass(frozen=True)
class BaselineReport:
    elapsed_s: float
    rows: int
    columns: tuple[str, ...]
    missing_evolved: tuple[str, ...]
    partition_45_columns: tuple[str, ...]
    partitions: tuple
    movie_42_rows: int
    movie_42_partitions: int


def run(spark, source, shuffle_partitions: int) -> BaselineReport:
    movies_dir = str(source / "movies")
    first = source / "movies" / "partition_00001.parquet"
    evolved_file = source / "movies" / "partition_00045.parquet"
    movies = spark.read.parquet(movies_dir)
    columns = tuple(movies.columns)
    missing = tuple(name for name in EVOLVED_COLUMNS if name not in columns)
    file_45_columns = tuple(spark.read.parquet(str(evolved_file)).columns) if evolved_file.is_file() else ()
    if first.is_file() and missing:
        print(
            "baseline đọc cả thư mục movies không bật mergeSchema. "
            f"Schema lấy từ các file đầu, thiếu {', '.join(missing)}."
        )
    elif not missing:
        print(
            "Spark đã ghép schema thư mục movies dù mergeSchema=false. "
            "Ba cột tiến hóa vẫn có trên partition_00045."
        )
    print(f"cột movies thư mục : {', '.join(columns)}")
    print(f"cột partition_00045: {', '.join(file_45_columns) if file_45_columns else '(không có file)'}")
    print(f"cột bị mất         : {', '.join(missing) if missing else '(không mất cột)'}")

    ratings = spark.read.parquet(str(source / "ratings"))
    joined = ratings.join(movies.select("movie_id", "title"), "movie_id", "inner")
    skewed = joined.repartition(shuffle_partitions, "movie_id").persist()

    started = time.perf_counter()
    rows = skewed.count()
    elapsed = time.perf_counter() - started
    stats = tuple(partition_stats(skewed))
    movie_rows, movie_parts = movie_42_share(skewed)
    skewed.unpersist()

    print(f"join rows          : {rows:,}")
    print(f"thời gian count    : {elapsed:.3f}s sau shuffle {shuffle_partitions} partition theo movie_id")
    print(f"movie_id=42        : {movie_rows:,} dòng trên {movie_parts} partition")
    print(format_partitions(list(stats)))
    return BaselineReport(
        elapsed_s=elapsed,
        rows=rows,
        columns=columns,
        missing_evolved=missing,
        partition_45_columns=file_45_columns,
        partitions=stats,
        movie_42_rows=movie_rows,
        movie_42_partitions=movie_parts,
    )


def main(argv: list[str] | None = None) -> BaselineReport:
    parser = build_parser("Baseline join ratings-movies, không salt, không mergeSchema.")
    args = parser.parse_args(argv)
    source = require_source(args.source_dir)
    spark = start_spark("cinestream-baseline-join", args)
    try:
        report = run(spark, source, args.shuffle_partitions)
        hold_ui(spark, args)
        return report
    finally:
        if args.hold_seconds <= 0:
            spark.stop()


if __name__ == "__main__":
    main()
