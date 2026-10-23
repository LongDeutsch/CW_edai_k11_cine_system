"""Ghép inventory code với quan sát live thành payload dashboard.

Không đọc log phase 5A–5D. Không ghi catalog, marker hay ClickHouse.
Catalog không đọc được thì trạng thái là unknown, không phải bảng thiếu.
"""

from __future__ import annotations

from datetime import datetime, timezone

from governance.drift import classify_schema
from governance.inventory import datasets, edges, schema_of
from governance.observe import batch_freshness, classify_dataset, descendants, impact_for, summarize_serving
from transformation.batch.contracts.schema_consistency import partition_difference

MARKER_TABLES = {
    "iceberg.gold.dim_movies": "dim_movies",
    "iceberg.gold.fact_ratings": "fact_ratings",
    "iceberg.gold.obt_movie_engagement": "obt_movie_engagement",
}
_DRIFT = {"match", "compatible", "manual_review", "breaking"}
_LAYERS = ("source", "bronze", "silver", "gold", "serving", "consume")


def build_view(raw: dict) -> dict:
    """raw là kết quả read_live hoặc fixture. Khóa health/proof bên trong raw không được dùng."""
    iceberg = _source(raw, "iceberg")
    airflow = _source(raw, "airflow")
    clickhouse = _source(raw, "clickhouse")
    flink = _source(raw, "flink")
    marker = _source(raw, "marker")
    streaming_enabled = bool(raw.get("streaming_enabled"))
    iceberg_read = iceberg["status"] == "read"
    tables = iceberg.get("tables") or {}
    pins = {}
    if marker["status"] == "read":
        pins = (marker.get("payload") or {}).get("gold_snapshots") or {}
    by_edge = _edge_index()
    partition_expected = _partition_expectations()
    rows = []
    drift_rows = []
    unread_tables = 0
    compared = 0
    for dataset in datasets():
        row, drift = _dataset_row(
            dataset,
            iceberg_read=iceberg_read,
            tables=tables,
            pins=pins if marker["status"] == "read" else None,
            streaming_enabled=streaming_enabled,
            clickhouse=clickhouse,
            airflow=airflow,
            by_edge=by_edge,
            partition_expected=partition_expected,
        )
        rows.append(row)
        if drift == "unread":
            unread_tables += 1
        elif drift is not None:
            compared += 1
            if drift["classification"] != "match":
                drift_rows.append(drift)
    integration = _integration(raw)
    rows.extend(integration["rows"])
    drift = _drift(iceberg, drift_rows, compared, unread_tables)
    health = _health(
        iceberg=iceberg,
        airflow=airflow,
        clickhouse=clickhouse,
        flink=flink,
        marker=marker,
        statuses=rows,
        streaming_enabled=streaming_enabled,
        iceberg_read=iceberg_read,
        pins=pins,
    )
    return {
        "generated_at": raw.get("generated_at") or datetime.now(timezone.utc).isoformat(),
        "git_head": raw.get("git_head") or None,
        "historical_proof_used_as_current_health": False,
        "streaming_enabled": streaming_enabled,
        "sources": {
            "inventory": {"status": "read", "error": None},
            "iceberg": _public_source(iceberg),
            "airflow": _public_source(airflow),
            "clickhouse": _public_source(clickhouse),
            "flink": _public_source(flink),
            "marker": _public_source(marker),
        },
        "datasets": rows,
        "edges": [_public_edge(edge) for edge in edges()],
        "drift": drift,
        "health": health,
        "integration": {key: value for key, value in integration.items() if key != "rows"},
        "layers": list(_LAYERS),
    }


def filter_datasets(rows: list[dict], q: str = "", environment: str = "", status: str = "", zone: str = "") -> list[dict]:
    needle = (q or "").strip().lower()
    found = []
    for row in rows:
        if environment and row.get("environment") != environment:
            continue
        if status and row.get("status") != status:
            continue
        if zone and row.get("zone") != zone:
            continue
        if needle and needle not in f"{row.get('id', '')} {row.get('name', '')} {row.get('location', '')}".lower():
            continue
        found.append(row)
    return found


def dataset_row(view: dict, dataset_id: str) -> dict:
    for row in view["datasets"]:
        if row["id"] == dataset_id:
            return row
    raise KeyError(dataset_id)


def lineage_of(view: dict, dataset_id: str) -> dict:
    row = dataset_row(view, dataset_id)
    return {
        "dataset": dataset_id,
        "upstream": row["upstream"],
        "downstream": row["downstream"],
        "ancestors": row["ancestors"],
        "descendants": row["descendants"],
        "lineage_source": "code",
    }


def impact_of(view: dict, dataset_id: str) -> dict:
    row = dataset_row(view, dataset_id)
    return {
        "dataset": dataset_id,
        "effect": row["impact"]["effect"],
        "reason": row["impact"]["reason"],
        "downstream": row["descendants"],
        "edges": row["downstream"],
        "lineage_source": "code",
    }


def _dataset_row(
    dataset,
    *,
    iceberg_read: bool,
    tables: dict,
    pins: dict | None,
    streaming_enabled: bool,
    clickhouse: dict,
    airflow: dict,
    by_edge: dict,
    partition_expected: dict,
) -> tuple[dict, dict | str | None]:
    expected = [{"name": name, "type": type_sql} for name, type_sql in schema_of(dataset)]
    schema_source = None if dataset.schema_ref is None else dataset.schema_ref[1]
    upstream = by_edge["upstream"].get(dataset.id, [])
    downstream = by_edge["downstream"].get(dataset.id, [])
    base = {
        "id": dataset.id,
        "name": dataset.name,
        "location": dataset.location,
        "zone": _zone(dataset.id),
        "layer": _layer(dataset.id),
        "environment": "integration" if dataset.lineage_status == "integration" else "production",
        "lineage_status": dataset.lineage_status,
        "declared_in_code": True,
        "pipeline": _pipeline(dataset, streaming_enabled, airflow),
        "owner": dataset.owner,
        "freshness_contract": dataset.freshness,
        "update_mode": dataset.update_mode,
        "schema_source": schema_source,
        "expected_schema": expected,
        "evidence": [{"path": path, "token": token} for path, token in dataset.evidence],
        "upstream": upstream,
        "downstream": downstream,
        "ancestors": _walk(dataset.id, by_edge["parents"]),
        "descendants": descendants(dataset.id),
    }
    if dataset.id.startswith("iceberg."):
        live, drift = _iceberg_live(
            dataset,
            iceberg_read=iceberg_read,
            tables=tables,
            pins=pins,
            streaming_enabled=streaming_enabled,
            expected=expected,
            schema_source=schema_source,
            partition_expected=partition_expected,
        )
    elif dataset.id.startswith("clickhouse."):
        live, drift = _clickhouse_live(clickhouse), None
    else:
        live, drift = _declared_live(), None
    impact = _impact(dataset.id, live["status"], live["drift_classification"])
    return {**base, **live, "impact": impact}, drift


def _iceberg_live(
    dataset,
    *,
    iceberg_read: bool,
    tables: dict,
    pins: dict | None,
    streaming_enabled: bool,
    expected: list[dict],
    schema_source: str | None,
    partition_expected: dict,
) -> tuple[dict, dict | str | None]:
    if not iceberg_read:
        return _unknown_live("iceberg chưa đọc được"), "unread"
    key = dataset.id.removeprefix("iceberg.")
    found = tables.get(key)
    if found is None:
        classified = classify_dataset(
            dataset,
            {"catalog": "absent", "streaming_enabled": streaming_enabled, "environment": "production"},
        )
        return _from_classified(classified, actual_schema=None, partitions=None, drift_classification="not_compared"), None
    if found.get("catalog") == "unread":
        return _unknown_live(found.get("error") or "metadata bảng chưa đọc được"), "unread"
    observation = {
        "catalog": "present",
        "environment": "production",
        "streaming_enabled": streaming_enabled,
        "snapshot_id": found.get("snapshot_id"),
        "total_records": found.get("total_records"),
        "committed_at": found.get("committed_at"),
        "marker_snapshot_id": None if pins is None else pins.get(MARKER_TABLES.get(dataset.id, "")),
    }
    classified = classify_dataset(dataset, observation)
    fields = found.get("fields") or []
    compared = classify_schema([(item["name"], item["type"]) for item in expected], fields)
    name = dataset.id.split(".")[-1]
    partitions = None
    if name in partition_expected and isinstance(found.get("partition_fields"), list):
        partitions = partition_difference(partition_expected[name], found["partition_fields"])
    drift = {
        "table": dataset.id,
        "classification": compared["classification"],
        "expected_source": schema_source,
        "physical_source": "iceberg-rest",
        "findings": compared["findings"],
        "partition": partitions,
    }
    live = _from_classified(
        classified,
        actual_schema=[
            {"name": field.get("name"), "type": field.get("type"), "required": field.get("required")} for field in fields
        ],
        partitions=found.get("partition_fields") or [],
        drift_classification=compared["classification"],
    )
    return live, drift


def _clickhouse_live(clickhouse: dict) -> dict:
    if clickhouse["status"] != "read":
        return _unknown_live(clickhouse.get("error") or "ClickHouse chưa đọc được")
    return {
        "status": "declared",
        "presence": "declared",
        "failure": False,
        "snapshot_id": None,
        "total_records": None,
        "committed_at": None,
        "actual_schema": None,
        "actual_partitions": None,
        "drift_classification": "not_applicable",
        "live_note": "trạng thái serving nằm ở health, không suy từ từng bảng",
    }


def _declared_live() -> dict:
    return {
        "status": "declared",
        "presence": "declared",
        "failure": False,
        "snapshot_id": None,
        "total_records": None,
        "committed_at": None,
        "actual_schema": None,
        "actual_partitions": None,
        "drift_classification": "not_applicable",
        "live_note": None,
    }


def _unknown_live(note: str) -> dict:
    return {
        "status": "unknown",
        "presence": "unknown",
        "failure": False,
        "snapshot_id": None,
        "total_records": None,
        "committed_at": None,
        "actual_schema": None,
        "actual_partitions": None,
        "drift_classification": "unavailable",
        "live_note": note,
    }


def _from_classified(classified: dict, *, actual_schema, partitions, drift_classification: str) -> dict:
    status = classified["status"]
    if status in {"expected_absent", "missing_unexpected"}:
        presence = "absent"
    elif status == "declared":
        presence = "declared"
    else:
        presence = "physical"
    return {
        "status": status,
        "presence": presence,
        "failure": bool(classified["failure"]),
        "snapshot_id": _sid(classified.get("snapshot_id")),
        "total_records": classified.get("total_records"),
        "committed_at": classified.get("committed_at"),
        "actual_schema": actual_schema,
        "actual_partitions": partitions,
        "drift_classification": drift_classification,
        "live_note": None,
    }


def _impact(dataset_id: str, status: str, classification: str) -> dict:
    downstream = descendants(dataset_id)
    if status == "unknown" or classification == "unavailable":
        return {
            "effect": "unknown",
            "reason": "chưa đọc được catalog nên không kết luận tác động",
            "downstream": downstream,
        }
    used = classification if classification in _DRIFT else "match"
    base = impact_for(dataset_id, status, used)
    reasons = {
        "disabled_path": "writer bị chặn hoặc streaming tắt; không phải catalog rỗng",
        "downstream_review": "drift hoặc trạng thái dataset cần xem phía dưới",
        "none": "không có tín hiệu tác động từ trạng thái vừa đọc",
    }
    return {
        "effect": base["effect"],
        "reason": reasons[base["effect"]],
        "downstream": base["downstream"],
    }


def _health(
    *,
    iceberg: dict,
    airflow: dict,
    clickhouse: dict,
    flink: dict,
    marker: dict,
    statuses: list[dict],
    streaming_enabled: bool,
    iceberg_read: bool,
    pins: dict,
) -> dict:
    airflow_summary = airflow.get("summary") if airflow["status"] == "read" else None
    if airflow_summary is not None:
        airflow_summary = {**airflow_summary, "production_success_implies_current_data": False}
    if clickhouse["status"] != "read":
        serving = {"status": "unavailable", "reason": clickhouse.get("error") or "ClickHouse chưa đọc được"}
    elif airflow_summary is None:
        serving = {"status": "unknown", "reason": "chưa đọc được task production", "production_rows": clickhouse.get("production_rows")}
    else:
        serving = summarize_serving(
            {
                "error": clickhouse.get("error"),
                "engagement_versions": clickhouse.get("engagement_versions"),
                "rating_versions": clickhouse.get("rating_versions"),
                "same_batch": clickhouse.get("same_batch"),
                "rows": clickhouse.get("production_rows"),
                "production_tasks": airflow_summary.get("production_tasks") or [],
            }
        )
    if marker["status"] == "unavailable":
        batch = "unavailable"
    elif marker["status"] == "absent":
        batch = "marker_unread"
    elif not iceberg_read:
        batch = "unavailable"
    else:
        batch = batch_freshness(statuses, True, pins)
    playback = next((item["status"] for item in statuses if item["id"] == "iceberg.silver.stg_playback_windows"), None)
    if not streaming_enabled:
        streaming = "disabled"
    elif not iceberg_read:
        streaming = "unavailable"
    elif playback == "missing_unexpected":
        streaming = "missing"
    else:
        streaming = playback or "unknown"
    flink_view = _public_source(flink)
    if flink["status"] == "read":
        flink_view = {
            **flink_view,
            "jobs_running": flink.get("jobs_running"),
            "jobs_failed": flink.get("jobs_failed"),
            "jobs_cancelled": flink.get("jobs_cancelled"),
            "checkpoint": flink.get("checkpoint"),
            "historical_cancelled_is_not_current_health": True,
        }
    paused = airflow.get("paused") if airflow["status"] == "read" else None
    if airflow["status"] != "read":
        batch_pipeline = "unknown"
    elif paused is True:
        batch_pipeline = "disabled"
    elif paused is False:
        batch_pipeline = "active"
    else:
        batch_pipeline = "unknown"
    return {
        "status": _overall([iceberg, airflow, clickhouse, flink, marker]),
        "success_inferred_from_snapshot": False,
        "historical_proof_used_as_current_health": False,
        "batch_pipeline": batch_pipeline,
        "airflow_paused": paused,
        "airflow": airflow_summary,
        "serving": serving,
        "flink": flink_view,
        "marker": _public_marker(marker),
        "freshness": {"batch": batch, "streaming": streaming},
    }


def _integration(raw: dict) -> dict:
    names = raw.get("integration_names")
    if names is None:
        return {
            "status": "unavailable",
            "error": raw.get("integration_error") or "danh sách bảng integration chưa đọc được",
            "count": None,
            "cleanup_candidate": None,
            "keep_referenced": None,
            "rows": [],
        }
    referenced = set(raw.get("integration_referenced") or [])
    extras = []
    cleanup = 0
    kept = 0
    for name in names:
        disposition = "keep_referenced" if name in referenced else "cleanup_candidate"
        if disposition == "keep_referenced":
            kept += 1
        else:
            cleanup += 1
        extras.append(
            {
                "id": f"integration.silver.{name}",
                "name": name,
                "location": f"cinestream.silver.{name}",
                "zone": "silver",
                "layer": "silver",
                "environment": "integration",
                "lineage_status": "integration",
                "declared_in_code": False,
                "pipeline": "not_applicable",
                "owner": None,
                "freshness_contract": None,
                "update_mode": None,
                "schema_source": None,
                "expected_schema": [],
                "evidence": [],
                "upstream": [],
                "downstream": [],
                "ancestors": [],
                "descendants": [],
                "status": "integration_only",
                "presence": "physical",
                "failure": False,
                "snapshot_id": None,
                "total_records": None,
                "committed_at": None,
                "actual_schema": None,
                "actual_partitions": None,
                "drift_classification": "not_compared",
                "live_note": "có tên trên catalog; schema không được tải để tránh đọc metadata từng bảng test",
                "impact": {
                    "effect": "none",
                    "reason": "không có cạnh lineage production",
                    "downstream": [],
                },
                "disposition": disposition,
            }
        )
    return {
        "status": "read",
        "error": None,
        "count": len(names),
        "cleanup_candidate": cleanup,
        "keep_referenced": kept,
        "rows": extras,
    }


def _drift(iceberg: dict, rows: list[dict], compared: int, unread_tables: int) -> dict:
    if iceberg["status"] != "read":
        return {"status": "unavailable", "error": iceberg.get("error"), "findings": None, "compared": None}
    return {
        "status": "partial" if unread_tables else "read",
        "error": None,
        "findings": rows,
        "compared": compared,
        "unread_tables": unread_tables,
    }


def _pipeline(dataset, streaming_enabled: bool, airflow: dict) -> str:
    if dataset.lineage_status == "writer_gated" or dataset.id.startswith("flink."):
        return "enabled" if streaming_enabled else "disabled"
    if dataset.id.startswith("airflow."):
        if airflow["status"] != "read":
            return "unknown"
        if airflow.get("paused") is True:
            return "disabled"
        if airflow.get("paused") is False:
            return "active"
        return "unknown"
    return "not_applicable"


def _overall(sources: list[dict]) -> str:
    states = [item["status"] for item in sources]
    if all(item in {"read", "absent"} for item in states):
        return "read"
    if all(item == "unavailable" for item in states):
        return "unavailable"
    return "partial"


def _source(raw: dict, name: str) -> dict:
    value = raw.get(name) or {}
    status = value.get("status") or "unavailable"
    return {**value, "status": status, "error": value.get("error")}


def _public_source(source: dict) -> dict:
    return {"status": source["status"], "error": source.get("error")}


def _public_marker(marker: dict) -> dict:
    if marker["status"] != "read":
        return {"status": marker["status"], "error": marker.get("error"), "payload": None}
    payload = dict(marker.get("payload") or {})
    snaps = payload.get("gold_snapshots")
    if isinstance(snaps, dict):
        payload["gold_snapshots"] = {key: _sid(value) for key, value in snaps.items()}
    return {"status": "read", "error": None, "payload": payload}


def _public_edge(edge) -> dict:
    return {
        "upstream": edge.upstream,
        "downstream": edge.downstream,
        "path": edge.evidence[0],
        "token": edge.evidence[1],
        "kind": edge.kind,
    }


def _edge_index() -> dict:
    upstream: dict[str, list[dict]] = {}
    downstream: dict[str, list[dict]] = {}
    parents: dict[str, list[str]] = {}
    for edge in edges():
        item = _public_edge(edge)
        upstream.setdefault(edge.downstream, []).append(item)
        downstream.setdefault(edge.upstream, []).append(item)
        parents.setdefault(edge.downstream, []).append(edge.upstream)
    return {"upstream": upstream, "downstream": downstream, "parents": parents}


def _walk(dataset_id: str, parents: dict[str, list[str]]) -> list[str]:
    found = []
    seen = {dataset_id}
    queue = [dataset_id]
    while queue:
        current = queue.pop(0)
        for parent in parents.get(current, []):
            if parent in seen:
                continue
            seen.add(parent)
            found.append(parent)
            queue.append(parent)
    return found


def _zone(dataset_id: str) -> str:
    if dataset_id.startswith("iceberg."):
        return dataset_id.split(".")[1]
    return dataset_id.split(".")[0]


def _layer(dataset_id: str) -> str:
    if dataset_id == "minio.orchestration_marker" or dataset_id.startswith("clickhouse."):
        return "serving"
    if dataset_id.startswith("iceberg."):
        zone = dataset_id.split(".")[1]
        return zone if zone in {"bronze", "silver", "gold"} else "gold"
    if dataset_id.startswith(("minio.", "kafka.")):
        return "source"
    return "consume"


def _partition_expectations() -> dict[str, list[str]]:
    from pathlib import Path

    from governance.extract import REPO
    from transformation.batch.contracts.schema_consistency import expected_partitions

    found = {}
    for relative in (
        "storage/lakehouse/bronze/raw_tables.sql",
        "storage/lakehouse/silver/stg_tables.sql",
        "storage/lakehouse/gold/gold_tables.sql",
    ):
        found.update(expected_partitions((Path(REPO) / relative).read_text()))
    return found


def _sid(value):
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return str(value)
    return value
