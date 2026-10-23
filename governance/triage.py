"""Phân loại finding Phase 5B. Không ALTER, không DROP, không điền snapshot id."""

from __future__ import annotations

import re
from pathlib import Path

from governance.inventory import datasets, edges

REPO = Path(__file__).resolve().parents[1]
_MISSING = object()

# Khóa marker của publish_success trước commit 4e90eda. Không chứa snapshot id.
LEGACY_MARKER_KEYS = (
    "published",
    "dag_id",
    "run_id",
    "logical_date",
    "data_interval_start",
    "data_interval_end",
    "effective_ts",
    "batch_id",
    "source_dir",
    "files",
    "namespaces",
    "bronze_quality",
    "gold_quality",
)


def current_serving_tables() -> tuple[str, ...]:
    """Ba bảng mà DAG hiện tại ghim vào gold_snapshots."""
    text = (REPO / "orchestration/dags/cinestream_daily_batch_lakehouse.py").read_text()
    match = re.search(r"_SERVING_TABLES = \((.*?)\)", text, re.S)
    if match is None or "gold_snapshots" not in text:
        raise RuntimeError("DAG hiện tại không còn hợp đồng gold_snapshots")
    names = tuple(re.findall(r'"([^"]+)"', match.group(1)))
    if names != ("dim_movies", "fact_ratings", "obt_movie_engagement"):
        raise RuntimeError(f"bảng serving đổi: {names}")
    return names


def classify_marker(payload: dict, required: tuple[str, ...] | None = None) -> dict:
    """Marker thiếu pin thì sync từ chối. Hàm này không tạo snapshot id."""
    required = current_serving_tables() if required is None else required
    snaps = payload.get("gold_snapshots", _MISSING)
    if snaps is _MISSING:
        return {"generation": "legacy_without_pins", "sync": "reject", "backfill_ids": False, "missing": list(required)}
    if not isinstance(snaps, dict):
        return {"generation": "invalid_pins", "sync": "reject", "backfill_ids": False, "missing": list(required)}
    missing = [name for name in required if name not in snaps]
    if missing:
        return {"generation": "incomplete_pins", "sync": "reject", "backfill_ids": False, "missing": missing}
    return {"generation": "current", "sync": "eligible", "backfill_ids": False, "missing": []}


def comparison_ignores_extra_physical_columns() -> bool:
    """Cột vật lý thừa được báo và không buộc overwrite."""
    from transformation.batch.contracts.schema_consistency import write_decision

    decision = write_decision(True, [{"column": "dt", "kind": "extra_on_table"}])
    return decision["overwrite"] is False and decision["status"] == "unchanged" and decision["schema_equal"] is False


def drift_disposition(kind: str, column: str) -> dict:
    """Tách lệch lịch sử vô hại với lệch hợp đồng cần migration riêng."""
    if kind == "extra" and column == "dt":
        return {
            "disposition": "historical_column",
            "contract_violation": False,
            "named_readers_affected": False,
            "change_in_5c": False,
        }
    if kind == "type" and column == "rating_val":
        return {
            "disposition": "sticky_decimal_contract",
            "contract_violation": True,
            "named_readers_affected": False,
            "change_in_5c": False,
        }
    if kind == "nullability":
        return {
            "disposition": "storage_constraint_gap",
            "contract_violation": False,
            "named_readers_affected": False,
            "change_in_5c": False,
        }
    if kind == "type" and column.endswith("_ts"):
        return {
            "disposition": "sticky_timestamp_semantics",
            "contract_violation": False,
            "named_readers_affected": False,
            "change_in_5c": False,
        }
    if kind == "missing":
        return {
            "disposition": "contract_break",
            "contract_violation": True,
            "named_readers_affected": True,
            "change_in_5c": False,
        }
    return {
        "disposition": "manual_review",
        "contract_violation": True,
        "named_readers_affected": False,
        "change_in_5c": False,
    }


def serving_downstream(dataset_id: str) -> list[str]:
    children: dict[str, list[str]] = {}
    for edge in edges():
        children.setdefault(edge.upstream, []).append(edge.downstream)
    found = []
    seen = {dataset_id}
    queue = [dataset_id]
    while queue:
        current = queue.pop(0)
        for child in children.get(current, []):
            if child in seen:
                continue
            seen.add(child)
            found.append(child)
            queue.append(child)
    return found


def lineage_review() -> dict:
    """Giữ cạnh có reader/writer trong source. Không thêm cạnh fact_watch sang OBT."""
    by_id = {item.id: item for item in datasets()}
    features = (
        "iceberg.gold.feat_user_movie_stats_daily",
        "iceberg.gold.feat_movie_engagement_stats_daily",
    )
    playback = "iceberg.silver.stg_playback_windows"
    fact_watch = "iceberg.gold.fact_watch_sessions"
    return {
        "fact_watch_downstream": serving_downstream(fact_watch),
        "feature_downstream": {item: serving_downstream(item) for item in features},
        "feature_status": {item: by_id[item].lineage_status for item in features},
        "playback_status": by_id[playback].lineage_status,
        "playback_downstream": serving_downstream(playback),
        "heartbeats_downstream": serving_downstream("iceberg.silver.stg_playback_heartbeats"),
    }


def cleanup_action(disposition: str) -> str:
    if disposition == "cleanup_candidate":
        return "future_drop_after_dry_run"
    if disposition == "keep_referenced":
        return "keep_until_test_leaves_silver"
    return "ignore"


FINDINGS = (
    {
        "id": "F-07",
        "title": "Marker production thiếu gold_snapshots",
        "severity": "medium",
        "change_required": False,
        "resolution": "Giữ marker cũ. DAG hiện tại đã ghi pin. Không điền snapshot id và không chạy sync.",
    },
    {
        "id": "F-05",
        "title": "Bốn bảng Silver/Gold giữ snapshot 16/10",
        "severity": "medium",
        "change_required": False,
        "resolution": "same_rows bỏ qua cột thừa và lệch kiểu. Không buộc overwrite trong phase này.",
    },
    {
        "id": "F-01",
        "title": "Cột dt trên stg_ratings và stg_watch_sessions",
        "severity": "low",
        "change_required": False,
        "resolution": "Cột lịch sử từ lần đọc Parquet có thư mục dt=. Reader theo tên không dùng. Không DROP.",
    },
    {
        "id": "F-02",
        "title": "stg_ratings.rating_val là double",
        "severity": "low",
        "change_required": False,
        "resolution": "DDL Silver và frame publish hiện là decimal(2,1). Schema Iceberg tạo ngày 16/10 vẫn là double. Gold fact đã là decimal. Không ALTER.",
    },
    {
        "id": "F-04",
        "title": "timestamp không timezone còn trên schema cũ",
        "severity": "low",
        "change_required": False,
        "resolution": "Session Spark là UTC. Overwrite ngày 18/10 không đổi schema-id. Không ép timestamptz.",
    },
    {
        "id": "F-06",
        "title": "DDL PARTITIONED BY không có trên spec Iceberg",
        "severity": "low",
        "change_required": False,
        "resolution": "Bảng tạo bằng writeTo().create(). DDL IF NOT EXISTS không sửa bảng đã có. Không tạo bảng mới.",
    },
    {
        "id": "F-03",
        "title": "Iceberg optional trong khi DDL NOT NULL",
        "severity": "low",
        "change_required": False,
        "resolution": "DataFrame create ghi cột optional, kể cả sau overwrite. Chưa đếm NULL. Không SET REQUIRED.",
    },
    {
        "id": "F-08",
        "title": "65 bảng it_* trong namespace silver",
        "severity": "low",
        "change_required": False,
        "resolution": "36 tên không còn literal trong source: chờ dry-run. 29 tên vẫn là đích test: giữ đến khi test sang namespace it_*.",
    },
    {
        "id": "F-09",
        "title": "fact_watch_sessions không có consumer serving",
        "severity": "info",
        "change_required": False,
        "resolution": "OBT được chiếu từ cùng frame trong bộ nhớ. Không thêm cạnh từ bảng fact watch.",
    },
    {
        "id": "F-10",
        "title": "Hai bảng feature chỉ có DDL",
        "severity": "info",
        "change_required": False,
        "resolution": "publish() không ghi. Không thêm cạnh và không cài Feast.",
    },
    {
        "id": "F-11",
        "title": "Đường streaming production đang tắt",
        "severity": "info",
        "change_required": False,
        "resolution": "Cạnh code giữ nguyên. Bảng 404 là expected_absent khi cổng streaming tắt.",
    },
)
