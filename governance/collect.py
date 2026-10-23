"""Thu metadata chỉ đọc rồi lập báo cáo Phase 5B. Không ALTER, DROP hay submit job."""

from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from datetime import datetime, timezone
from pathlib import Path

from governance.drift import classify_schema
from governance.inventory import datasets, schema_of
from governance.observe import (
    classify_dataset,
    cleanup_candidates,
    batch_freshness,
    impact_for,
    is_production_run,
    summarize_airflow,
    summarize_serving,
)

REPO = Path(__file__).resolve().parents[1]
REPORT = REPO / "docs" / "proof" / "logs" / "phase5b_observability.json"
REST = os.environ.get("CINESTREAM_ICEBERG_REST", "http://127.0.0.1:8181/v1")
CLICKHOUSE = os.environ.get("CINESTREAM_CLICKHOUSE_HTTP", "http://127.0.0.1:18123")
AIRFLOW = os.environ.get("CINESTREAM_AIRFLOW_URL", "http://127.0.0.1:18082")
FLINK = os.environ.get("CINESTREAM_FLINK_URL", "http://127.0.0.1:18081")
S3_ENDPOINT = os.environ.get("CINESTREAM_S3_ENDPOINT", "http://127.0.0.1:9000")
MARKER_TABLES = {
    "iceberg.gold.dim_movies": "dim_movies",
    "iceberg.gold.fact_ratings": "fact_ratings",
    "iceberg.gold.obt_movie_engagement": "obt_movie_engagement",
}


def main() -> None:
    report = collect()
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    drift = [item["table"] for item in report["drift"] if item["classification"] != "match"]
    print(json.dumps({"drift": drift, "serving": report["health"]["serving"]["status"], "cleanup": report["cleanup_summary"]}, ensure_ascii=False))


def collect() -> dict:
    streaming_enabled = os.environ.get("CINESTREAM_ALLOW_STREAMING", "").strip().lower() in {"1", "true", "yes"}
    iceberg = _iceberg_index()
    marker = _production_marker()
    marker_ids = (marker or {}).get("gold_snapshots") or {}
    airflow = _airflow()
    clickhouse = _clickhouse()
    flink = _flink(streaming_enabled)
    referenced = _referenced_test_names(set(iceberg.get("silver_it_names") or []))
    cleanup_input = []
    for item in iceberg.get("silver_it") or []:
        item = dict(item)
        item["referenced"] = item["name"] in referenced
        cleanup_input.append(item)
    cleanup = cleanup_candidates(cleanup_input)
    drift_rows = []
    statuses = []
    impacts = []
    for dataset in datasets():
        observation = _observation(dataset, iceberg, marker_ids, streaming_enabled)
        status = classify_dataset(dataset, observation)
        statuses.append(status)
        classification = "match"
        if dataset.id.startswith("iceberg.") and observation.get("fields") is not None:
            compared = classify_schema(schema_of(dataset), observation["fields"])
            classification = compared["classification"]
            if classification != "match":
                drift_rows.append(
                    {
                        "table": dataset.id,
                        "classification": classification,
                        "partition_fields": observation.get("partition_fields") or [],
                        "findings": compared["findings"],
                    }
                )
        if classification != "match" or status["status"] in {"expected_absent", "missing_unexpected", "populated_stale", "declared_empty"}:
            impacts.append({**impact_for(dataset.id, status["status"], classification), "classification": classification})
    health = {
        "success_inferred_from_snapshot": False,
        "airflow": airflow["summary"],
        "marker": {
            "present": marker is not None,
            "batch_id": None if marker is None else marker.get("batch_id"),
            "run_id": None if marker is None else marker.get("run_id"),
            "published": None if marker is None else marker.get("published"),
            "snapshot_pins": bool(marker_ids),
            "gold_snapshots": marker_ids,
        },
        "serving": summarize_serving(
            {
                "error": clickhouse.get("error"),
                "rows": clickhouse.get("production_rows"),
                "engagement_versions": clickhouse.get("engagement_versions"),
                "rating_versions": clickhouse.get("rating_versions"),
                "same_batch": clickhouse.get("same_batch"),
                "production_tasks": airflow["summary"].get("production_tasks") or [],
            }
        ),
        "clickhouse": {
            "production_rows": clickhouse.get("production_rows"),
            "engagement_versions": clickhouse.get("engagement_versions"),
            "rating_versions": clickhouse.get("rating_versions"),
            "same_batch": clickhouse.get("same_batch"),
            "databases": clickhouse.get("databases") or [],
        },
        "flink": flink,
        "freshness": {
            "batch": batch_freshness(statuses, marker is not None, marker_ids),
            "streaming": _streaming_freshness(statuses, streaming_enabled),
            "success_inferred_from_snapshot": False,
        },
    }
    return {
        "head": _head(),
        "sources": {
            "inventory": "static code in governance/inventory.py",
            "schema": "DDL and writer columns parsed from pipeline source",
            "iceberg": iceberg.get("status"),
            "airflow": airflow.get("status"),
            "clickhouse": clickhouse.get("status"),
            "flink": flink.get("status"),
            "marker": "read" if marker is not None else marker,
            "historical_logs": "not used as the current verdict",
        },
        "drift": drift_rows,
        "datasets": statuses,
        "health": health,
        "cleanup": cleanup,
        "cleanup_summary": {
            "silver_it_tables": len(cleanup),
            "cleanup_candidate": sum(1 for item in cleanup if item["disposition"] == "cleanup_candidate"),
            "keep_referenced": sum(1 for item in cleanup if item["disposition"] == "keep_referenced"),
        },
        "impact": impacts,
        "namespaces": iceberg.get("namespaces") or [],
        "clickhouse_databases": clickhouse.get("databases"),
    }


def _observation(dataset, iceberg: dict, marker_ids: dict, streaming_enabled: bool) -> dict:
    if not dataset.id.startswith("iceberg."):
        return {"catalog": "not_applicable", "streaming_enabled": streaming_enabled}
    zone, name = dataset.id.split(".")[1:]
    tables = iceberg.get("tables") or {}
    found = tables.get(f"{zone}.{name}")
    if not found:
        return {"catalog": "absent", "streaming_enabled": streaming_enabled, "environment": "production"}
    marker_key = MARKER_TABLES.get(dataset.id)
    return {
        "catalog": "present",
        "environment": "production",
        "streaming_enabled": streaming_enabled,
        "snapshot_id": found.get("snapshot_id"),
        "total_records": found.get("total_records"),
        "committed_at": found.get("committed_at"),
        "fields": found.get("fields"),
        "partition_fields": found.get("partition_fields"),
        "marker_snapshot_id": None if marker_key is None else marker_ids.get(marker_key),
    }


def _streaming_freshness(statuses: list[dict], streaming_enabled: bool) -> str:
    by_id = {item["id"]: item["status"] for item in statuses}
    playback = by_id.get("iceberg.silver.stg_playback_windows")
    if playback == "expected_absent" and not streaming_enabled:
        streaming = "disabled"
    elif playback == "missing_unexpected":
        streaming = "missing"
    else:
        streaming = playback or "unknown"
    return streaming


def _iceberg_index() -> dict:
    try:
        _get(f"{REST}/namespaces")
    except Exception as exc:
        return {"status": f"unread:{exc.__class__.__name__}", "tables": {}, "silver_it": [], "silver_it_names": [], "namespaces": []}
    namespaces = _namespace_names()
    tables = {}
    for dataset in datasets():
        if not dataset.id.startswith("iceberg."):
            continue
        zone, name = dataset.id.split(".")[1:]
        try:
            metadata = _get(f"{REST}/namespaces/{zone}/tables/{name}")["metadata"]
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                continue
            raise
        tables[f"{zone}.{name}"] = _table_view(metadata)
    listing = _get(f"{REST}/namespaces/silver/tables")
    names = []
    for item in listing.get("identifiers", []):
        name = item.get("name") if isinstance(item, dict) else item[-1]
        if str(name).startswith("it_"):
            names.append(name)
    silver_it = []
    for name in names:
        try:
            metadata = _get(f"{REST}/namespaces/silver/tables/{name}")["metadata"]
        except urllib.error.HTTPError:
            continue
        view = _table_view(metadata)
        silver_it.append({"namespace": "silver", "name": name, **view})
    return {"status": "read", "tables": tables, "silver_it": silver_it, "silver_it_names": names, "namespaces": namespaces}


def _namespace_names() -> list[str]:
    payload = _get(f"{REST}/namespaces")
    names = []
    for item in payload.get("namespaces") or []:
        if isinstance(item, dict):
            raw = item.get("namespace") or item.get("name") or item
            name = raw[-1] if isinstance(raw, list) else raw
        elif isinstance(item, list):
            name = item[-1]
        else:
            name = item
        names.append(str(name))
    return sorted(names)


def _table_view(metadata: dict) -> dict:
    current = metadata.get("current-schema-id")
    schemas = metadata.get("schemas") or []
    chosen = schemas[-1] if schemas else {"fields": []}
    for item in schemas:
        if item.get("schema-id") == current:
            chosen = item
    snapshot_id = metadata.get("current-snapshot-id")
    snapshot = None
    for item in metadata.get("snapshots") or []:
        if item.get("snapshot-id") == snapshot_id:
            snapshot = item
    summary = (snapshot or {}).get("summary") or {}
    total = summary.get("total-records")
    timestamp = (snapshot or {}).get("timestamp-ms")
    committed = None
    if timestamp is not None:
        committed = datetime.fromtimestamp(int(timestamp) / 1000, timezone.utc).isoformat()
    specs = metadata.get("partition-specs") or []
    current_spec = metadata.get("default-spec-id")
    partition_fields = []
    for spec in specs:
        if spec.get("spec-id") == current_spec:
            partition_fields = [field.get("name") for field in spec.get("fields") or []]
    return {
        "snapshot_id": snapshot_id,
        "total_records": None if total is None else int(total),
        "committed_at": committed,
        "fields": [
            {"name": field["name"], "type": field.get("type"), "required": field.get("required")}
            for field in chosen.get("fields") or []
        ],
        "partition_fields": partition_fields,
    }


def _airflow() -> dict:
    try:
        payload = _get(
            f"{AIRFLOW}/api/v1/dags/cinestream_daily_batch_lakehouse/dagRuns?limit=100&order_by=-start_date",
            auth=b"airflow:airflow",
        )
    except Exception as exc:
        summary = summarize_airflow([])
        return {"status": f"unread:{exc.__class__.__name__}", "summary": summary}
    runs = []
    for item in payload.get("dag_runs") or []:
        conf = item.get("conf") or {}
        run = {
            "run_id": item.get("dag_run_id"),
            "state": item.get("state"),
            "start_date": item.get("start_date"),
            "production": is_production_run(conf),
            "namespaces": {
                "bronze": conf.get("bronze_namespace"),
                "silver": conf.get("silver_namespace"),
                "gold": conf.get("gold_namespace"),
            },
            "tasks": [],
        }
        runs.append(run)
    interesting = []
    seen = set()

    def take(run: dict) -> None:
        if id(run) not in seen:
            seen.add(id(run))
            interesting.append(run)

    production_seen = False
    production_failure_seen = False
    failure_seen = False
    for run in runs:
        if run["production"] and not production_seen:
            take(run)
            production_seen = True
        if run["production"] and run["state"] == "failed" and not production_failure_seen:
            take(run)
            production_failure_seen = True
        if run["state"] == "failed" and not failure_seen:
            take(run)
            failure_seen = True
    if runs:
        take(runs[0])
    for run in interesting:
        run["tasks"] = _airflow_tasks(run["run_id"])
    return {"status": "read", "summary": summarize_airflow(runs)}


def _airflow_tasks(run_id: str) -> list[dict]:
    encoded = urllib.parse.quote(run_id, safe="")
    payload = _get(
        f"{AIRFLOW}/api/v1/dags/cinestream_daily_batch_lakehouse/dagRuns/{encoded}/taskInstances",
        auth=b"airflow:airflow",
    )
    return [{"task_id": item.get("task_id"), "state": item.get("state")} for item in payload.get("task_instances") or []]


def _clickhouse() -> dict:
    try:
        parts = _clickhouse_json(
            """
            SELECT database, `table` AS table_name, sum(rows) AS rows
            FROM system.parts
            WHERE active AND (database = 'cinestream_serving' OR database LIKE 'it_%')
            GROUP BY database, table_name
            ORDER BY database, table_name
            """
        )
    except Exception as exc:
        return {"status": f"unread:{exc.__class__.__name__}", "error": str(exc), "production_rows": None, "databases": []}
    production_rows = sum(int(row["rows"]) for row in parts if row["database"] == "cinestream_serving")
    versions = _optional_count("SELECT uniqExact(iceberg_snapshot_id) AS versions FROM cinestream_serving.movie_engagement")
    rating_versions = _optional_count("SELECT uniqExact(iceberg_snapshot_id) AS versions FROM cinestream_serving.movie_ratings")
    batches = _optional_count("SELECT uniqExact(batch_id) AS versions FROM cinestream_serving.movie_engagement")
    rating_batches = _optional_count("SELECT uniqExact(batch_id) AS versions FROM cinestream_serving.movie_ratings")
    same_batch = None
    if batches is not None and rating_batches is not None and versions not in (None, 0):
        same_batch = batches == 1 and rating_batches == 1
    databases = sorted({row["database"] for row in parts})
    return {
        "status": "read",
        "error": None,
        "production_rows": production_rows,
        "engagement_versions": versions,
        "rating_versions": rating_versions,
        "same_batch": same_batch,
        "databases": databases,
    }


def _optional_count(query: str) -> int | None:
    try:
        rows = _clickhouse_json(query)
    except Exception:
        return None
    if not rows:
        return 0
    return int(rows[0].get("versions") or 0)


def _flink(streaming_enabled: bool) -> dict:
    try:
        overview = _get(f"{FLINK}/overview")
    except Exception as exc:
        return {"status": f"unread:{exc.__class__.__name__}", "streaming_enabled": streaming_enabled, "checkpoint": "unread"}
    running = int(overview.get("jobs-running") or 0)
    return {
        "status": "read",
        "streaming_enabled": streaming_enabled,
        "jobs_running": running,
        "jobs_failed": int(overview.get("jobs-failed") or 0),
        "jobs_cancelled": int(overview.get("jobs-cancelled") or 0),
        "checkpoint": "no_active_job" if running == 0 else "active_job_not_inspected",
        "historical_cancelled_is_not_current_health": True,
    }


def _production_marker() -> dict | None:
    try:
        import boto3
    except ImportError:
        return None
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name="us-east-1",
    )
    page = client.list_objects_v2(Bucket="lakehouse", Prefix="_orchestration/bronze/")
    objects = page.get("Contents") or []
    if not objects:
        return None
    newest = max(objects, key=lambda item: item["LastModified"])
    body = client.get_object(Bucket="lakehouse", Key=newest["Key"])["Body"].read()
    payload = json.loads(body.decode())
    payload["marker_key"] = newest["Key"]
    return payload


def _referenced_test_names(names: set[str]) -> set[str]:
    chunks = []
    for relative in ("streaming", "serving", "orchestration", "tests", "transformation"):
        root = REPO / relative
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.suffix not in {".py", ".sql"}:
                continue
            chunks.append(path.read_text(errors="ignore"))
    text = "\n".join(chunks)
    return {name for name in names if name in text}


def _head() -> str:
    completed = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True, check=False)
    return completed.stdout.strip()


def _get(url: str, auth: bytes | None = None) -> dict:
    request = urllib.request.Request(url)
    if auth:
        request.add_header("Authorization", "Basic " + b64encode(auth).decode())
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode())


def _clickhouse_json(query: str) -> list[dict]:
    params = urllib.parse.urlencode(
        {"user": "default", "password": os.environ.get("CINESTREAM_CLICKHOUSE_PASSWORD", "cinestream"), "query": query + " FORMAT JSON"}
    )
    request = urllib.request.Request(f"{CLICKHOUSE}/?{params}", method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode()).get("data") or []


if __name__ == "__main__":
    main()
