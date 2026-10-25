"""Trạng thái dataset, sức khỏe pipeline, ứng viên dọn và tác động lineage.

Hàm này chỉ nhận quan sát đã thu. Không tự suy thành công từ việc bảng tồn tại.
"""

from __future__ import annotations

from governance.inventory import datasets, edges

PRODUCTION_NAMESPACES = {"bronze", "silver", "gold"}
PINNED_GOLD = (
    "iceberg.gold.dim_movies",
    "iceberg.gold.fact_ratings",
    "iceberg.gold.obt_movie_engagement",
)
_FAILURE_STATES = {"failed", "upstream_failed"}


def descendants(dataset_id: str) -> list[str]:
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


def classify_dataset(dataset, observation: dict) -> dict:
    """observation: catalog, snapshot_id, total_records, environment, streaming_enabled, marker_snapshot_id."""
    catalog = observation.get("catalog", "not_applicable")
    environment = observation.get("environment", "production")
    snapshot_id = observation.get("snapshot_id")
    total = observation.get("total_records")
    if dataset.lineage_status == "integration" or environment == "integration":
        status = "integration_only"
    elif observation.get("catalog") == "not_applicable":
        status = "declared"
    elif dataset.lineage_status == "writer_gated" and catalog == "absent" and not observation.get("streaming_enabled"):
        status = "expected_absent"
    elif catalog == "absent":
        status = "missing_unexpected"
    elif dataset.lineage_status == "declared_only" and snapshot_id in (None, -1):
        status = "declared_empty"
    elif dataset.id.startswith("iceberg.gold.feat_") and snapshot_id in (None, -1):
        status = "declared_empty"
    elif snapshot_id in (None, -1):
        status = "catalog_without_snapshot"
    elif total == 0:
        status = "empty_committed"
    elif _stale(observation):
        status = "populated_stale"
    elif total is None and snapshot_id not in (None, -1):
        status = "snapshot_without_count"
    else:
        status = "populated"
    return {
        "id": dataset.id,
        "status": status,
        "lineage_status": dataset.lineage_status,
        "snapshot_id": snapshot_id,
        "total_records": total,
        "committed_at": observation.get("committed_at"),
        "failure": status in {"missing_unexpected", "populated_stale"},
    }


def impact_for(dataset_id: str, status: str, classification: str) -> dict:
    downstream = descendants(dataset_id)
    if status == "expected_absent":
        effect = "disabled_path"
    elif classification == "breaking" or status in {"missing_unexpected", "populated_stale"}:
        effect = "downstream_review"
    elif classification == "manual_review":
        effect = "downstream_review"
    else:
        effect = "none"
    return {"dataset": dataset_id, "effect": effect, "downstream": downstream}


def summarize_airflow(runs: list[dict]) -> dict:
    """runs mới nhất trước. production=True khi namespace là bronze/silver/gold hoặc allow_production."""
    latest = runs[0] if runs else None
    production = [item for item in runs if item.get("production")]
    latest_production = production[0] if production else None
    failures = [item for item in runs if item.get("state") == "failed"]
    latest_failure = failures[0] if failures else None
    failed_tasks = []
    if latest_failure:
        failed_tasks = [task["task_id"] for task in latest_failure.get("tasks", []) if task.get("state") in _FAILURE_STATES]
    production_failures = [item for item in production if item.get("state") == "failed"]
    latest_production_failure = production_failures[0] if production_failures else None
    production_failed_tasks = []
    if latest_production_failure:
        production_failed_tasks = [
            task["task_id"] for task in latest_production_failure.get("tasks", []) if task.get("state") in _FAILURE_STATES
        ]
    production_tasks = []
    if latest_production:
        production_tasks = [task["task_id"] for task in latest_production.get("tasks", [])]
    return {
        "latest_state": None if latest is None else latest.get("state"),
        "latest_is_production": bool(latest and latest.get("production")),
        "production_run_state": None if latest_production is None else latest_production.get("state"),
        "production_run_id": None if latest_production is None else latest_production.get("run_id"),
        "production_tasks": production_tasks,
        "production_success_implies_current_data": False,
        "failed_run_count": len(failures),
        "latest_failed_run": None if latest_failure is None else latest_failure.get("run_id"),
        "latest_failed_tasks": failed_tasks,
        "latest_failed_is_production": bool(latest_failure and latest_failure.get("production")),
        "latest_production_failure": None if latest_production_failure is None else latest_production_failure.get("run_id"),
        "latest_production_failed_tasks": production_failed_tasks,
    }


def summarize_serving(observation: dict) -> dict:
    """Không gọi serving thành công chỉ vì marker hoặc snapshot Iceberg tồn tại."""
    if observation.get("error"):
        return {"status": "sync_failed", "reason": observation["error"]}
    versions = observation.get("engagement_versions")
    rating_versions = observation.get("rating_versions")
    if (versions or 0) > 1 or (rating_versions or 0) > 1 or observation.get("same_batch") is False:
        return {"status": "sync_failed", "reason": "reader snapshot lệch"}
    tasks = observation.get("production_tasks") or []
    rows = observation.get("rows")
    ran = "sync_clickhouse" in tasks
    if not ran and not rows:
        return {"status": "not_run", "reason": "run production gần nhất không có task sync_clickhouse hoặc chưa có dòng"}
    if ran and not rows:
        return {"status": "sync_failed", "reason": "task sync_clickhouse thành công nhưng reader không còn dòng"}
    if ran and rows and (versions in (None, 0, 1)) and observation.get("same_batch") is not False:
        return {"status": "consistent", "reason": "reader production có một snapshot"}
    return {"status": "not_run", "reason": "chưa có sync production để đối chiếu"}


def cleanup_candidates(tables: list[dict]) -> list[dict]:
    """Bảng it_* nằm trong namespace production. Không xóa."""
    found = []
    for table in tables:
        namespace = table.get("namespace")
        name = table.get("name") or ""
        if namespace not in PRODUCTION_NAMESPACES or not name.startswith("it_"):
            continue
        referenced = bool(table.get("referenced"))
        found.append(
            {
                "namespace": namespace,
                "name": name,
                "snapshot_id": table.get("snapshot_id"),
                "committed_at": table.get("committed_at"),
                "total_records": table.get("total_records"),
                "referenced_by_tests": referenced,
                "disposition": "keep_referenced" if referenced else "cleanup_candidate",
                "reason": "bảng test nằm trong namespace production",
            }
        )
    found.sort(key=lambda item: (item["disposition"] != "cleanup_candidate", item["namespace"], item["name"]))
    return found


def is_production_run(conf: dict | None) -> bool:
    conf = conf or {}
    if conf.get("allow_production") in (True, "true", "True", "1", 1):
        return True
    names = {conf.get("bronze_namespace"), conf.get("silver_namespace"), conf.get("gold_namespace")}
    return bool(names & PRODUCTION_NAMESPACES)


def batch_freshness(statuses: list[dict], marker_present: bool, pinned: dict | None) -> str:
    """Ba bảng Gold có dòng chưa đủ để gọi là khớp marker."""
    if not marker_present:
        return "marker_unread"
    if not pinned:
        return "published_without_snapshot_pins"
    by_id = {item["id"]: item["status"] for item in statuses}
    watched = [by_id.get(dataset_id) for dataset_id in PINNED_GOLD]
    if any(item == "populated_stale" for item in watched):
        return "stale_vs_marker"
    if all(item == "populated" for item in watched):
        return "aligned_with_marker"
    return "incomplete"


def _stale(observation: dict) -> bool:
    marker = observation.get("marker_snapshot_id")
    current = observation.get("snapshot_id")
    if marker is None or current in (None, -1):
        event_age = observation.get("event_age_seconds")
        threshold = observation.get("stale_after_seconds")
        return event_age is not None and threshold is not None and event_age > threshold
    return int(current) != int(marker)
