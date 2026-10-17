"""Khử lệch movie_id bằng salt, hợp schema, khử trùng và broadcast thể loại.

Ratings nhận ``salt = floor(rand() * N)``. Movies được nhân N lần.
Join trên ``(movie_id, salt)`` rồi shuffle theo ``salt`` để movie_id=42
không còn nằm trọn trong một partition.

Ví dụ::

    python -m transformation.batch.optimized.handle_skew_salting --salt-buckets 16
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
class OptimizedReport:
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


def run(spark, source, shuffle_partitions: int, salt_buckets: int) -> OptimizedReport:
    from pyspark.sql import functions as F
    from pyspark.sql.functions import broadcast

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
    print(
        "Khóa silver này gộp mọi lần chấm cùng số sao, không chỉ 2.5% dòng tiêm. "
        "Các bản sao tiêm (timestamp +3 giây) nằm trong phần bị bỏ."
    )

    salts = spark.range(salt_buckets).select(F.col("id").cast("int").alias("salt"))
    movies_salted = movies.crossJoin(F.broadcast(salts))
    ratings_salted = ratings_raw.withColumn(
        "salt",
        F.floor(F.rand(42) * F.lit(salt_buckets)).cast("int"),
    )
    joined = ratings_salted.join(movies_salted, ["movie_id", "salt"], "inner")
    spread = joined.repartition(shuffle_partitions, "salt").persist()

    started = time.perf_counter()
    rows = spread.count()
    elapsed = time.perf_counter() - started
    stats = tuple(partition_stats(spread))
    movie_rows, movie_parts = movie_42_share(spread)
    spread.unpersist()
    ratings.unpersist()

    print(f"salt join rows     : {rows:,}")
    print(f"thời gian count    : {elapsed:.3f}s sau shuffle theo salt ({salt_buckets} bucket)")
    print(f"movie_id=42        : {movie_rows:,} dòng trên {movie_parts} partition")
    print(format_partitions(list(stats)))

    genres = spark.read.parquet(str(source / "movie_genres"))
    genre_dim = genres.groupBy("movie_id").agg(
        F.concat_ws(",", F.sort_array(F.collect_set("genre_name"))).alias("genres")
    )
    with_genres = movies.join(broadcast(genre_dim), "movie_id", "left")
    plan = with_genres._jdf.queryExecution().executedPlan().toString()
    uses_broadcast = "BroadcastHashJoin" in plan
    genre_movies = with_genres.where(F.col("genres").isNotNull()).count()
    print(f"broadcast genres   : {'có BroadcastHashJoin' if uses_broadcast else 'KHÔNG thấy BroadcastHashJoin'}")
    print(f"phim có thể loại   : {genre_movies:,}")
    if not uses_broadcast:
        print(plan)

    return OptimizedReport(
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
    )


def main(argv: list[str] | None = None) -> OptimizedReport:
    parser = build_parser("Salting, mergeSchema, row_number và broadcast movie_genres.")
    args = parser.parse_args(argv)
    source = require_source(args.source_dir)
    spark = start_spark("cinestream-salted-join", args)
    try:
        report = run(spark, source, args.shuffle_partitions, args.salt_buckets)
        hold_ui(spark, args)
        return report
    finally:
        if args.hold_seconds <= 0:
            spark.stop()


if __name__ == "__main__":
    main()
