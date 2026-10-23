"""Đối chiếu inventory với Iceberg REST và ClickHouse. Chỉ đọc."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from governance.inventory import datasets, schema_of

REPO = Path(__file__).resolve().parents[1]
REPORT = REPO / "docs" / "proof" / "logs" / "phase5a_governance_audit.json"
REST = "http://127.0.0.1:8181/v1"
CLICKHOUSE = "http://127.0.0.1:18123"
PRODUCTION_NAMESPACES = {"bronze", "silver", "gold"}


def main() -> None:
    iceberg = _iceberg()
    clickhouse = _clickhouse()
    drift = _drift(iceberg)
    report = {
        "iceberg": iceberg,
        "clickhouse": clickhouse,
        "schema_drift": drift,
        "writer_gated_absent": [
            item["name"]
            for item in iceberg.get("tables", [])
            if item["status"] == "absent" and item["name"] in {"stg_playback_heartbeats", "stg_playback_windows"}
        ],
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"drift": drift, "absent": report["writer_gated_absent"]}, ensure_ascii=False))


def _drift(iceberg: dict) -> list[dict]:
    if iceberg.get("status") != "read":
        return []
    declared = {}
    for item in datasets():
        if not item.id.startswith("iceberg."):
            continue
        zone, name = item.id.split(".")[1:]
        declared[f"{zone}.{name}"] = [column for column, _ in schema_of(item)]
    found = []
    for table in iceberg["tables"]:
        if table["status"] != "present":
            continue
        key = f"{table['namespace']}.{table['name']}"
        if key not in declared:
            continue
        expected = declared[key]
        observed = table["columns"]
        extra = [name for name in observed if name not in expected]
        missing = [name for name in expected if name not in observed]
        if extra or missing:
            found.append({"table": key, "extra": extra, "missing": missing, "snapshot_id": table["snapshot_id"]})
    return found


def _iceberg() -> dict:
    try:
        namespaces = [
            item[0] if isinstance(item, list) else item.get("namespace", [""])[0]
            for item in _get(f"{REST}/namespaces").get("namespaces", [])
        ]
    except Exception as exc:
        return {"status": f"unread:{exc.__class__.__name__}"}
    integration = sorted(name for name in namespaces if name not in PRODUCTION_NAMESPACES)
    tables = []
    silver_it: list[str] = []
    for zone in ("bronze", "silver", "gold"):
        listing = _get(f"{REST}/namespaces/{zone}/tables")
        present = set()
        for item in listing.get("identifiers", []):
            if isinstance(item, dict):
                present.add(item.get("name"))
            elif isinstance(item, list):
                present.add(item[-1])
        expected = [item for item in datasets() if item.id.startswith(f"iceberg.{zone}.")]
        integration_here = sorted(value for value in present if str(value).startswith("it_"))
        for item in expected:
            table_name = item.id.split(".")[-1]
            if table_name not in present:
                tables.append({"namespace": zone, "name": table_name, "status": "absent"})
                continue
            meta = _get(f"{REST}/namespaces/{zone}/tables/{table_name}")["metadata"]
            tables.append(
                {
                    "namespace": zone,
                    "name": table_name,
                    "status": "present",
                    "snapshot_id": meta.get("current-snapshot-id"),
                    "columns": _field_names(meta),
                }
            )
        if zone == "silver":
            silver_it = integration_here
    return {"status": "read", "integration_namespaces": integration, "silver_it_tables": silver_it, "tables": tables}


def _field_names(metadata: dict) -> list[str]:
    schemas = metadata.get("schemas") or []
    current = metadata.get("current-schema-id")
    chosen = schemas[-1] if schemas else {"fields": []}
    for item in schemas:
        if item.get("schema-id") == current:
            chosen = item
    return [field["name"] for field in chosen.get("fields", [])]


def _clickhouse() -> dict:
    query = """
    SELECT database, `table` AS table_name, sum(rows) AS rows
    FROM system.parts
    WHERE active
      AND (database = 'cinestream_serving' OR database LIKE 'it_%')
    GROUP BY database, table_name
    ORDER BY database, table_name
    """
    try:
        rows = _clickhouse_json(query)
    except Exception as exc:
        return {"status": f"unread:{exc.__class__.__name__}"}
    return {"status": "read", "parts": rows}


def _get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=8) as response:
        return json.loads(response.read().decode())


def _clickhouse_json(query: str) -> list[dict]:
    params = urllib.parse.urlencode({"user": "default", "password": "cinestream", "query": query + " FORMAT JSON"})
    request = urllib.request.Request(f"{CLICKHOUSE}/?{params}", method="POST")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode()).get("data") or []
    except urllib.error.HTTPError as exc:
        raise RuntimeError(exc.read().decode()[:300]) from exc


if __name__ == "__main__":
    main()
