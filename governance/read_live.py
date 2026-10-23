"""Đọc metadata hiện tại. Không ghi file proof và không ALTER catalog.

Bảng ``it_*`` chỉ được liệt kê tên. Không tải metadata từng bảng test.
"""

from __future__ import annotations

import os
import subprocess
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

from governance.collect import (
    AIRFLOW,
    REST,
    _airflow,
    _clickhouse,
    _flink,
    _get,
    _production_marker,
    _referenced_test_names,
    _table_view,
)
from governance.inventory import datasets

REPO_ROOT = Path(__file__).resolve().parents[1]


def read_live() -> dict:
    streaming_enabled = os.environ.get("CINESTREAM_ALLOW_STREAMING", "").strip().lower() in {"1", "true", "yes"}
    iceberg = _iceberg()
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_head": _head(),
        "streaming_enabled": streaming_enabled,
        "iceberg": iceberg["source"],
        "airflow": _airflow_view(),
        "clickhouse": _clickhouse_view(),
        "flink": _flink_view(streaming_enabled),
        "marker": _marker(),
        "integration_names": iceberg["integration_names"],
        "integration_error": iceberg["integration_error"],
        "integration_referenced": iceberg["integration_referenced"],
    }


def _iceberg() -> dict:
    try:
        _get(f"{REST}/namespaces")
    except Exception as exc:
        return {
            "source": {"status": "unavailable", "error": f"{exc.__class__.__name__}: {exc}", "tables": None},
            "integration_names": None,
            "integration_error": f"{exc.__class__.__name__}: {exc}",
            "integration_referenced": None,
        }
    tables = {}
    for dataset in datasets():
        if not dataset.id.startswith("iceberg."):
            continue
        zone, name = dataset.id.split(".")[1:]
        key = f"{zone}.{name}"
        try:
            metadata = _get(f"{REST}/namespaces/{zone}/tables/{name}")["metadata"]
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                continue
            tables[key] = {"catalog": "unread", "error": f"HTTP {exc.code}"}
            continue
        except Exception as exc:
            tables[key] = {"catalog": "unread", "error": exc.__class__.__name__}
            continue
        tables[key] = {"catalog": "present", **_table_view(metadata)}
    names, error = _integration_names()
    referenced = None if names is None else sorted(_referenced_test_names(set(names)))
    return {
        "source": {"status": "read", "error": None, "tables": tables},
        "integration_names": names,
        "integration_error": error,
        "integration_referenced": referenced,
    }


def _integration_names() -> tuple[list[str] | None, str | None]:
    try:
        listing = _get(f"{REST}/namespaces/silver/tables")
    except Exception as exc:
        return None, f"{exc.__class__.__name__}: {exc}"
    names = []
    for item in listing.get("identifiers") or []:
        name = item.get("name") if isinstance(item, dict) else item[-1]
        if str(name).startswith("it_"):
            names.append(str(name))
    return sorted(names), None


def _airflow_view() -> dict:
    raw = _airflow()
    if not str(raw.get("status", "")).startswith("read"):
        return {"status": "unavailable", "error": raw.get("status"), "paused": None, "summary": None}
    paused = None
    error = None
    try:
        dag = _get(f"{AIRFLOW}/api/v1/dags/cinestream_daily_batch_lakehouse", auth=b"airflow:airflow")
        paused = dag.get("is_paused")
    except Exception as exc:
        error = f"paused unread: {exc.__class__.__name__}"
    return {"status": "read", "error": error, "paused": paused, "summary": raw.get("summary")}


def _clickhouse_view() -> dict:
    raw = _clickhouse()
    if not str(raw.get("status", "")).startswith("read"):
        return {
            "status": "unavailable",
            "error": raw.get("error") or raw.get("status"),
            "production_rows": None,
            "engagement_versions": None,
            "rating_versions": None,
            "same_batch": None,
        }
    return {
        "status": "read",
        "error": None,
        "production_rows": raw.get("production_rows"),
        "engagement_versions": raw.get("engagement_versions"),
        "rating_versions": raw.get("rating_versions"),
        "same_batch": raw.get("same_batch"),
    }


def _flink_view(streaming_enabled: bool) -> dict:
    raw = _flink(streaming_enabled)
    if not str(raw.get("status", "")).startswith("read"):
        return {
            "status": "unavailable",
            "error": raw.get("status"),
            "jobs_running": None,
            "jobs_failed": None,
            "jobs_cancelled": None,
            "checkpoint": None,
        }
    return {
        "status": "read",
        "error": None,
        "jobs_running": raw.get("jobs_running"),
        "jobs_failed": raw.get("jobs_failed"),
        "jobs_cancelled": raw.get("jobs_cancelled"),
        "checkpoint": raw.get("checkpoint"),
        "historical_cancelled_is_not_current_health": True,
    }


def _marker() -> dict:
    try:
        import boto3  # noqa: F401
    except ImportError:
        return {"status": "unavailable", "error": "boto3 không có", "payload": None}
    try:
        payload = _production_marker()
    except Exception as exc:
        return {"status": "unavailable", "error": f"{exc.__class__.__name__}: {exc}", "payload": None}
    if not payload:
        return {"status": "absent", "error": None, "payload": None}
    return {
        "status": "read",
        "error": None,
        "payload": {
            "marker_key": payload.get("marker_key"),
            "batch_id": payload.get("batch_id"),
            "published": payload.get("published"),
            "run_id": payload.get("run_id"),
            "gold_snapshots": payload.get("gold_snapshots"),
        },
    }


def _head() -> str | None:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return completed.stdout.strip() or None
