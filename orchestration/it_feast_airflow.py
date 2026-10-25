"""DP1 đến DP4 trên namespace ``it_af_dp_b2``.

Không reset ``it_af_dp``. Không ghi bronze, silver, gold. Không ghi
``cinestream_serving``. Feast nằm ở ``/opt/feast-packages`` trong scheduler.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

from ml.offline.feast_export import MOVIE_TABLE, USER_TABLE, export_frames
from orchestration.it_dp_e2e import (
    DP1,
    DP2,
    DP3,
    LEGACY,
    SERVING_COUNTS,
    clickhouse,
    production_fingerprint,
    run_conf,
    set_paused,
    snapshots_of,
    task_map,
    trigger,
    wait_dags,
    xcom,
)
from orchestration.it_phase2 import close_reader, ensure_buckets, local_batch_id, open_reader, seed_happy, wait_http, wait_worker

REPO = Path(__file__).resolve().parents[1]
DP4 = "cinestream_dp4_materialize_online"
NAMESPACE = "it_af_dp_b2"
LOGICAL = "2026-06-01T00:00:00+00:00"
CLICKHOUSE_DB = "it_af_dp_b2_ch"
EXPORT = REPO / "ml" / "feast" / "data" / "airflow-b2"
LOG = REPO / "docs" / "proof" / "logs" / "phase_b2_airflow_materialize.json"
AS_OF = "2026-01-20T00:00:00+00:00"
CREATED = "2026-01-21T03:00:00+00:00"


def main() -> None:
    wait_http("http://127.0.0.1:18082/health")
    wait_worker()
    ensure_buckets()
    before = production_fingerprint()
    serving_before = {sql: clickhouse(sql) for sql in SERVING_COUNTS}
    legacy = _pause_legacy()
    wait_dags()
    report = {"namespace": NAMESPACE, "logical_date": LOGICAL}
    try:
        for dag_id in (DP1, DP2, DP3, DP4):
            set_paused(dag_id, False)
        root = Path(tempfile.mkdtemp(prefix="cinestream-feast-airflow-"))
        spark = open_reader()
        from orchestration.it_phase2 import reset_namespaces

        reset_namespaces(spark, [NAMESPACE])
        seed_happy(spark, root, "b2")
        batch_id = local_batch_id("it-airflow/b2")
        conf = run_conf("it-airflow/b2", NAMESPACE, batch_id=batch_id, clickhouse_database=CLICKHOUSE_DB)
        dp1 = trigger(DP1, conf, LOGICAL, timeout=900)
        dp2 = trigger(DP2, conf, LOGICAL, timeout=1200)
        dp3 = trigger(DP3, {**conf, "as_of": AS_OF, "created_timestamp": CREATED}, LOGICAL, timeout=900)
        current = snapshots_of(NAMESPACE)
        _export(spark, current, batch_id)
        subprocess.run(["docker", "exec", "cinestream-redis-1", "redis-cli", "-n", "1", "FLUSHDB"], check=True)
        dp4_conf = {
            "export_dir": "/opt/cinestream/ml/feast/data/airflow-b2",
            "repo_dir": "/opt/cinestream/ml/feast/data/airflow-b2-repo",
            "state_dir": "/opt/cinestream/ml/feast/data/airflow-b2-state",
            "redis": "redis:6379,db=1",
            "batch_id": batch_id,
            "gold_namespace": NAMESPACE,
            "iceberg_rest": "http://iceberg-rest:8181",
        }
        dp4 = trigger(DP4, dp4_conf, LOGICAL, timeout=600)
        online = xcom(DP4, dp4["dag_run_id"], "materialize_online")
        tasks = task_map(DP4, dp4, ("wait_for_dp3", "materialize_online"))
        report.update(
            {
                "batch_id": batch_id,
                "dp1": dp1.get("state"),
                "dp2": dp2.get("state"),
                "dp3": dp3.get("state"),
                "dp4": dp4.get("state"),
                "dp4_tasks": {name: item.get("state") for name, item in tasks.items()},
                "snapshot_ids": {USER_TABLE: current.get(USER_TABLE), MOVIE_TABLE: current.get(MOVIE_TABLE)},
                "materialize": online,
            }
        )
        ok = (
            dp1.get("state") == "success"
            and dp2.get("state") == "success"
            and dp3.get("state") == "success"
            and dp4.get("state") == "success"
            and online.get("result") == "materialized"
            and online.get("batch_id") == batch_id
        )
        report["status"] = "PASS" if ok else "FAIL"
    except Exception as exc:
        report["status"] = "FAIL"
        report["error"] = f"{exc.__class__.__name__}: {exc}"
    finally:
        for dag_id in (DP1, DP2, DP3, DP4):
            try:
                set_paused(dag_id, True)
            except Exception as exc:
                print(f"không pause được {dag_id}: {exc}", flush=True)
        close_reader()
    after = production_fingerprint()
    serving_after = {sql: clickhouse(sql) for sql in SERVING_COUNTS}
    report["legacy_paused"] = legacy
    report["production_unchanged"] = before == after
    report["serving_unchanged"] = serving_before == serving_after
    if report.get("status") == "PASS" and not (report["production_unchanged"] and report["serving_unchanged"]):
        report["status"] = "FAIL"
        report["error"] = "production hoặc serving đổi"
    LOG.parent.mkdir(parents=True, exist_ok=True)
    LOG.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps({key: report.get(key) for key in ("status", "dp1", "dp2", "dp3", "dp4", "materialize", "error")}, default=str))
    if report.get("status") != "PASS":
        raise SystemExit(report.get("error") or report["status"])


def _export(spark, current: dict, batch_id: str) -> None:
    frames = {}
    for table in (USER_TABLE, MOVIE_TABLE):
        frame = spark.sql(
            f"SELECT * FROM cinestream.{NAMESPACE}.{table} VERSION AS OF {int(current[table])}"
        ).toPandas()
        for column in ("event_timestamp", "created_timestamp"):
            frame[column] = pd.to_datetime(frame[column], utc=True)
        frames[table] = frame
    export_frames(
        EXPORT,
        frames,
        {
            "batch_id": batch_id,
            "sequence": 1,
            "namespace": NAMESPACE,
            "snapshot_ids": {USER_TABLE: current[USER_TABLE], MOVIE_TABLE: current[MOVIE_TABLE]},
        },
    )


def _pause_legacy() -> bool:
    from orchestration.it_phase2 import api

    paused = api("GET", f"/dags/{LEGACY}").get("is_paused")
    if paused is not True:
        set_paused(LEGACY, True)
    return paused is True


if __name__ == "__main__":
    main()
