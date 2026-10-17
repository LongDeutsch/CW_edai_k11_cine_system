"""Xử lý lệch dữ liệu (Data Skew) bằng Adaptive Query Execution (AQE) của Apache Spark.

Khác với Salting (phải tự viết thêm cột salt và nhân bản bảng nhỏ), AQE của Spark 3.0+
tự động phát hiện các partition bị lệch kích thước ở giai đoạn shuffle runtime,
sau đó tự động chia nhỏ (split) skewed partition thành các sub-partitions.

Cấu hình AQE Skew Join kích hoạt:
    - spark.sql.adaptive.enabled = true
    - spark.sql.adaptive.skewJoin.enabled = true
    - spark.sql.adaptive.skewJoin.skewedPartitionFactor = 2
    - spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 131072 (128KB)
    - spark.sql.adaptive.advisoryPartitionSizeInBytes = 131072 (128KB)

Ví dụ::

    python -m transformation.batch.optimized.handle_skew_aqe --shuffle-partitions 16
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from transformation.batch.common import (
    EVOLVED_COLUMNS,
    build_parser,
    dedup_ratings,
    format_partitions,
    hold_ui,
    movie_42_share,
    partition_stats,
    require_source,
    start_spark,
)


@dataclass(frozen=True)
class AQEReport:
    elapsed_s: float
    rows: int
    ratings_before: int
    ratings_after: int
    evolved_columns: tuple[str, ...]
    null_evolved: dict[str, int]
    partitions: tuple
    movie_42_rows: int
    movie_42_partitions: int
    broadcast_join: bool
    genre_movies: int
    aqe_triggered: bool
    plan_summary: str


def run(spark, source, shuffle_partitions: int) -> AQEReport:
    from pyspark.sql import functions as F
    from pyspark.sql.functions import broadcast

    # 1. Đọc movies với mergeSchema = true (xử lý Schema Evolution)
    movies = spark.read.option("mergeSchema", "true").parquet(str(source / "movies"))
    evolved = tuple(name for name in EVOLVED_COLUMNS if name in movies.columns)
    missing = [name for name in EVOLVED_COLUMNS if name not in movies.columns]
    if missing:
        raise SystemExit(f"mergeSchema vẫn thiếu cột: {', '.join(missing)}")

    null_counts = {
        name: int(movies.where(F.col(name).isNull()).count())
        for name in evolved
    }
    print("mergeSchema bật. Cột tiến hóa: " + ", ".join(evolved))
    for name, nulls in null_counts.items():
        print(f"  {name}: {nulls} dòng NULL (file schema cũ) / {movies.count()} phim")

    # 2. Khử trùng ratings (2.5% duplicate offline)
    ratings_raw = spark.read.parquet(str(source / "ratings"))
    before = ratings_raw.count()
    ratings = dedup_ratings(ratings_raw).persist()
    after = ratings.count()
    removed = before - after
    rate = removed / before if before else 0.0
    print(
        f"dedup ratings      : {before:,} -> {after:,} "
        f"(bỏ {removed:,} = {rate:.2%}, khóa user_id, movie_id, rating_val)"
    )

    # 3. Join TỰ NHIÊN trên movie_id (KHÔNG cần thêm cột salt)
    # AQE sẽ tự động phân tích shuffle statistics sau stage map và chia nhỏ skewed partition
    movies_selected = movies.select("movie_id", "title")
    joined = ratings_raw.join(movies_selected, "movie_id", "inner")
    repartitioned = joined.repartition(shuffle_partitions, "movie_id").persist()

    started = time.perf_counter()
    rows = repartitioned.count()
    elapsed = time.perf_counter() - started
    stats = tuple(partition_stats(repartitioned))
    movie_rows, movie_parts = movie_42_share(repartitioned)

    # Kiểm tra executed plan xem AQE có can thiệp hay không
    plan = repartitioned._jdf.queryExecution().executedPlan().toString()
    aqe_triggered = (
        "isSkew=true" in plan
        or "AQEShuffleRead" in plan
        or "CustomShuffleReader" in plan
        or "AdaptiveSparkPlan" in plan
    )

    repartitioned.unpersist()
    ratings.unpersist()

    print(f"AQE join rows      : {rows:,}")
    print(f"thời gian count    : {elapsed:.3f}s (tự động chia skewed partition bằng AQE)")
    print(f"movie_id=42        : {movie_rows:,} dòng trên {movie_parts} partition")
    print(f"AQE kích hoạt      : {'CÓ (AdaptiveSparkPlan)' if aqe_triggered else 'Chưa thấy cờ skew'}")
    print(format_partitions(list(stats)))

    # 4. Broadcast join với thể loại
    genres = spark.read.parquet(str(source / "movie_genres"))
    genre_dim = genres.groupBy("movie_id").agg(
        F.concat_ws(",", F.sort_array(F.collect_set("genre_name"))).alias("genres")
    )
    with_genres = movies.join(broadcast(genre_dim), "movie_id", "left")
    genre_plan = with_genres._jdf.queryExecution().executedPlan().toString()
    uses_broadcast = "BroadcastHashJoin" in genre_plan
    genre_movies = with_genres.where(F.col("genres").isNotNull()).count()
    print(f"broadcast genres   : {'có BroadcastHashJoin' if uses_broadcast else 'KHÔNG thấy BroadcastHashJoin'}")

    return AQEReport(
        elapsed_s=elapsed,
        rows=rows,
        ratings_before=before,
        ratings_after=after,
        evolved_columns=evolved,
        null_evolved=null_counts,
        partitions=stats,
        movie_42_rows=movie_rows,
        movie_42_partitions=movie_parts,
        broadcast_join=uses_broadcast,
        genre_movies=genre_movies,
        aqe_triggered=aqe_triggered,
        plan_summary="AdaptiveSparkPlan with skewJoin=true",
    )


def main(argv: list[str] | None = None) -> AQEReport:
    parser = build_parser("AQE Skew Join tự động, mergeSchema, row_number và broadcast movie_genres.")
    args = parser.parse_args(argv)
    source = require_source(args.source_dir)
    spark = start_spark("cinestream-aqe-skew-join", args, enable_aqe=True)
    try:
        report = run(spark, source, args.shuffle_partitions)
        hold_ui(spark, args)
        return report
    finally:
        if args.hold_seconds <= 0:
            spark.stop()


if __name__ == "__main__":
    main()
