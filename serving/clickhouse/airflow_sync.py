"""Tham số spark-submit cho sync ClickHouse. Không tự chọn snapshot Iceberg."""

from __future__ import annotations

from transformation.batch.contracts.validators import check_batch_id, check_identifier


def serving_database(gold_namespace: str, override: str | None = None) -> str:
    """Namespace ``gold`` dùng database serving. Namespace test dùng database riêng."""
    if override:
        return check_identifier(str(override), "database")
    gold_namespace = check_identifier(gold_namespace, "gold")
    if gold_namespace == "gold":
        return "cinestream_serving"
    return check_identifier(f"it_{gold_namespace}", "database")


def sync_arguments(
    *,
    batch_id: str,
    marker_namespace: str,
    gold_namespace: str,
    warehouse: str,
    iceberg_rest: str,
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
    clickhouse_http: str,
    clickhouse_database: str | None = None,
) -> list[str]:
    """Đối số của sync_gold. Không có cờ snapshot: id chỉ lấy từ marker."""
    database = serving_database(gold_namespace, clickhouse_database)
    args = [
        "--warehouse",
        warehouse,
        "--master",
        "local[1]",
        "--iceberg-rest",
        iceberg_rest,
        "--s3-endpoint",
        s3_endpoint,
        "--s3-access-key",
        s3_access_key,
        "--s3-secret-key",
        s3_secret_key,
        "--no-ui",
        "--shuffle-partitions",
        "2",
        "--batch-id",
        check_batch_id(batch_id),
        "--marker-namespace",
        check_identifier(marker_namespace, "marker-namespace"),
        "--clickhouse-http",
        clickhouse_http,
        "--database",
        database,
    ]
    if any(item.startswith("--snapshot") for item in args):
        raise RuntimeError("sync không nhận snapshot id từ dòng lệnh")
    return args
