"""So sánh 3 phương pháp xử lý Skew trên một SparkSession local: Baseline vs Salting vs AQE.

Một JVM cho cả ba job để ``make demo-layer3`` xong trong vài giây
sau khi PySpark đã có trong cache.

Ví dụ::

    python -m transformation.batch.demo_layer3 --source-dir data_generator/output/scale_0_1
"""

from __future__ import annotations

from transformation.batch.baseline.unoptimized_join import run as run_baseline
from transformation.batch.common import build_parser, require_source, start_spark
from transformation.batch.optimized.handle_skew_aqe import run as run_aqe
from transformation.batch.optimized.handle_skew_salting import run as run_salting


def main(argv: list[str] | None = None) -> None:
    parser = build_parser("So sánh thời gian và độ lệch partition: Baseline vs Salting vs AQE.")
    args = parser.parse_args(argv)
    source = require_source(args.source_dir)
    spark = start_spark("cinestream-layer3-demo", args, enable_aqe=False)
    try:
        print("\n" + "=" * 30 + " 1. BASELINE (KHÔNG TỐI ƯU) " + "=" * 30)
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        baseline = run_baseline(spark, source, args.shuffle_partitions)

        print("\n" + "=" * 30 + " 2. OPTIMIZED: SALTING KEY " + "=" * 30)
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        salting = run_salting(spark, source, args.shuffle_partitions, args.salt_buckets)

        print("\n" + "=" * 30 + " 3. OPTIMIZED: ADAPTIVE QUERY EXECUTION (AQE) " + "=" * 30)
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", str(args.aqe_skew_factor))
        spark.conf.set(
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
            str(args.aqe_skew_threshold_bytes),
        )
        spark.conf.set(
            "spark.sql.adaptive.advisoryPartitionSizeInBytes",
            str(args.aqe_advisory_size_bytes),
        )
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        aqe = run_aqe(spark, source, args.shuffle_partitions)

        _print_compare_three(baseline, salting, aqe)
    finally:
        spark.stop()


def _print_compare_three(baseline, salting, aqe) -> None:
    base_hot = max(baseline.partitions, key=lambda stat: stat.rows)
    salt_hot = max(salting.partitions, key=lambda stat: stat.rows)
    aqe_hot = max(aqe.partitions, key=lambda stat: stat.rows)

    salt_delta = baseline.elapsed_s - salting.elapsed_s
    aqe_delta = baseline.elapsed_s - aqe.elapsed_s

    salt_ratio = salting.elapsed_s / baseline.elapsed_s if baseline.elapsed_s else 0.0
    aqe_ratio = aqe.elapsed_s / baseline.elapsed_s if baseline.elapsed_s else 0.0

    print("\n" + "=" * 32 + " BẢNG SO SÁNH LAYER 3 " + "=" * 32)
    print(f"{'Tiêu chí đánh giá':<28} {'1. Baseline':>14} {'2. Salting':>14} {'3. Spark AQE':>14}")
    print("-" * 74)
    print(f"{'Thời gian count (s)':<28} {baseline.elapsed_s:>14.3f} {salting.elapsed_s:>14.3f} {aqe.elapsed_s:>14.3f}")
    print(f"{'So với baseline':<28} {'100.0% (gốc)':>14} {salt_ratio:>13.1%} {aqe_ratio:>13.1%}")
    print(f"{'Thời gian giảm (s)':<28} {'0.000':>14} {salt_delta:>14.3f} {aqe_delta:>14.3f}")
    print(f"{'Tổng dòng sau join':<28} {baseline.rows:>14,} {salting.rows:>14,} {aqe.rows:>14,}")
    print(f"{'Partition nặng nhất':<28} {base_hot.share:>13.2%} {salt_hot.share:>13.2%} {aqe_hot.share:>13.2%}")
    print(f"{'Partition chứa movie_id=42':<28} {baseline.movie_42_partitions:>14} {salting.movie_42_partitions:>14} {aqe.movie_42_partitions:>14}")
    print(f"{'Cần sửa code / DDL':<28} {'Không':>14} {'Có (thêm salt)':>14} {'Không (tự động)':>14}")
    print(f"{'Khử duplicate 2.5%':<28} {'Chưa':>14} {'Đã khử (window)':>14} {'Đã khử (window)':>14}")
    print(f"{'Schema Evolution (3 cột)':<28} {'Bị mất cột':>14} {'Đủ (mergeSchema)':>14} {'Đủ (mergeSchema)':>14}")
    print(f"{'Broadcast dim_genres':<28} {'Không':>14} {'Có (BHJ)':>14} {'Có (BHJ)':>14}")
    print("-" * 74)

    print("\nChi tiết phân bổ dòng trên từng partition giữa 3 phương pháp:")
    print("part |  baseline_rows (share)  |   salted_rows (share)   |    aqe_rows (share)")
    print("-" * 74)
    salt_map = {stat.partition_id: stat for stat in salting.partitions}
    aqe_map = {stat.partition_id: stat for stat in aqe.partitions}

    for stat in baseline.partitions:
        p_id = stat.partition_id
        s_stat = salt_map.get(p_id)
        a_stat = aqe_map.get(p_id)

        s_rows = s_stat.rows if s_stat else 0
        s_share = s_stat.share if s_stat else 0.0
        a_rows = a_stat.rows if a_stat else 0
        a_share = a_stat.share if a_stat else 0.0

        print(
            f"{p_id:>4} | {stat.rows:>12,} ({stat.share:>6.2%}) | "
            f"{s_rows:>12,} ({s_share:>6.2%}) | "
            f"{a_rows:>12,} ({a_share:>6.2%})"
        )
    print("=" * 74)


if __name__ == "__main__":
    main()
