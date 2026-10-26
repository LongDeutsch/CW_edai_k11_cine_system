"""Chiếu data_flow của DP1–DP3 và kết quả validate_* lên DataHub GMS.

Governance Python, triage và dashboard không đọc module này. Hàm emit chỉ
POST proposal. Lỗi GMS không xóa snapshot Iceberg và không ghi lại batch.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

from governance.inventory import Dataset, Edge, datasets, edges

DATASET_FABRIC = "PROD"
FLOW_FABRIC = "prod"
GMS_PATH = "/aspects?action=ingestProposal"

_PRODUCTION_LAYERS = {"bronze": "bronze", "silver": "silver", "gold": "gold"}


@dataclass(frozen=True)
class Pipeline:
    dag_id: str
    task_id: str
    validate_task: str
    evidence_paths: frozenset[str]
    description: str


PIPELINES: dict[str, Pipeline] = {
    "cinestream_dp1_raw_to_bronze": Pipeline(
        dag_id="cinestream_dp1_raw_to_bronze",
        task_id="spark_raw_to_bronze",
        validate_task="validate_bronze",
        evidence_paths=frozenset(
            {
                "transformation/batch/jobs/ingest_to_bronze.py",
                "transformation/batch/contracts/validators.py",
            }
        ),
        description="DP1 Raw MinIO → Bronze. Inlet là nguồn, outlet là bảng bronze.",
    ),
    "cinestream_dp2_bronze_to_gold": Pipeline(
        dag_id="cinestream_dp2_bronze_to_gold",
        task_id="spark_bronze_to_gold",
        validate_task="validate_gold",
        evidence_paths=frozenset(
            {
                "transformation/batch/jobs/publish_silver_gold.py",
                "transformation/batch/jobs/build_gold_layer.py",
            }
        ),
        description="DP2 Bronze → Silver/Gold. Chỉ cạnh data_flow của job publish.",
    ),
    "cinestream_dp3_offline_features": Pipeline(
        dag_id="cinestream_dp3_offline_features",
        task_id="spark_offline_features",
        validate_task="validate_features",
        evidence_paths=frozenset({"transformation/batch/jobs/build_offline_features.py"}),
        description="DP3 Silver → feat_*. Assertion là khóa user và movie.",
    ),
}


def pipeline_for(dag_id: str) -> Pipeline:
    try:
        return PIPELINES[dag_id]
    except KeyError as exc:
        known = ", ".join(sorted(PIPELINES))
        raise KeyError(f"không chiếu DAG {dag_id}. Chỉ có {known}") from exc


def dataset_urn(platform: str, name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{DATASET_FABRIC})"


def flow_urn(dag_id: str) -> str:
    return f"urn:li:dataFlow:(airflow,{dag_id},{FLOW_FABRIC})"


def job_urn(dag_id: str, task_id: str) -> str:
    return f"urn:li:dataJob:({flow_urn(dag_id)},{task_id})"


def assertion_urn(dag_id: str, check: str, dataset_name: str) -> str:
    token = dataset_name.replace("/", "_")
    return f"urn:li:assertion:{dag_id}.{check}.{token}"


def run_prefix(namespaces: dict[str, str] | None) -> str | None:
    """Namespace IT đứng trước tên dataset, để không đè catalog production."""
    chosen = dict(namespaces or {})
    for layer, default in _PRODUCTION_LAYERS.items():
        value = str(chosen.get(layer) or default)
        if value != default:
            return value
    return None


def qualified_dataset(dataset: Dataset, namespaces: dict[str, str] | None = None) -> tuple[str, str]:
    """Trả (platform, tên) dùng trong URN. Tên IT không trùng tên production."""
    prefix = run_prefix(namespaces)
    if dataset.id.startswith("iceberg."):
        catalog, layer, table = dataset.location.split(".")
        layer_name = str((namespaces or {}).get(layer) or layer)
        return "iceberg", f"{catalog}.{layer_name}.{table}"
    if dataset.id.startswith("minio."):
        path = dataset.location.removeprefix("s3a://").rstrip("/")
        if prefix:
            path = f"{prefix}.{path}"
        return "s3", path
    raise KeyError(f"DataHub chỉ chiếu iceberg và minio, không chiếu {dataset.id}")


def pipeline_edges(pipeline: Pipeline) -> tuple[Edge, ...]:
    """Chỉ data_flow có evidence trỏ tới job của pipeline. Ba kind kia bị bỏ."""
    selected = [
        edge
        for edge in edges()
        if edge.kind == "data_flow" and edge.evidence[0] in pipeline.evidence_paths
    ]
    return tuple(selected)


def quality_checks(pipeline: Pipeline, result: dict, threshold: float) -> tuple[dict, ...]:
    """Assertion lấy từ dict validate đã có. Không thêm rule mới."""
    if pipeline.validate_task == "validate_bronze":
        ready = result.get("ready") is True
        violations = list(result.get("violations") or [])
        over = [
            item
            for item in list(result.get("datasets") or [])
            if float(item.get("error_rate") or 0) > float(threshold)
        ]
        return (
            {
                "name": "bronze_ready",
                "passed": ready,
                "outlets": frozenset({"raw_users", "raw_movies", "raw_ratings", "raw_watch_history"}),
                "native": {"ready": ready, "reason": result.get("reason") or ""},
            },
            {
                "name": "bronze_error_threshold",
                "passed": ready and not violations and not over,
                "outlets": frozenset({"raw_users", "raw_movies", "raw_ratings", "raw_watch_history"}),
                "native": {
                    "error_threshold": threshold,
                    "violations": len(violations),
                    "over_threshold": len(over),
                },
            },
        )
    if pipeline.validate_task == "validate_gold":
        counts = dict(result.get("counts") or {})
        has_counts = {"fact_ratings", "stg_ratings", "fact_watch_sessions", "stg_watch_sessions", "obt_movie_engagement"} <= set(counts)
        matched = has_counts and counts["fact_ratings"] == counts["stg_ratings"] and counts["fact_watch_sessions"] == counts["stg_watch_sessions"] and counts["obt_movie_engagement"] == counts["fact_watch_sessions"]
        has_orphans = "orphan_user_sk" in result and "orphan_movie_sk" in result
        no_orphan = has_orphans and int(result.get("orphan_user_sk") or 0) == 0 and int(result.get("orphan_movie_sk") or 0) == 0
        outlets = frozenset({"fact_ratings", "fact_watch_sessions"})
        return (
            {
                "name": "silver_gold_match",
                "passed": matched,
                "outlets": outlets,
                "native": {"counts": counts, "reason": result.get("reason") or ""},
            },
            {
                "name": "no_orphan_key",
                "passed": no_orphan,
                "outlets": outlets,
                "native": {
                    "orphan_user_sk": result.get("orphan_user_sk"),
                    "orphan_movie_sk": result.get("orphan_movie_sk"),
                },
            },
        )
    if pipeline.validate_task == "validate_features":
        return (
            {
                "name": "user_rows_distinct",
                "passed": "user_rows" in result and result.get("user_rows") == result.get("user_distinct"),
                "outlets": frozenset({"feat_user_movie_stats_daily"}),
                "native": {"user_rows": result.get("user_rows"), "user_distinct": result.get("user_distinct")},
            },
            {
                "name": "movie_rows_distinct",
                "passed": "movie_rows" in result and result.get("movie_rows") == result.get("movie_distinct"),
                "outlets": frozenset({"feat_movie_engagement_stats_daily"}),
                "native": {"movie_rows": result.get("movie_rows"), "movie_distinct": result.get("movie_distinct")},
            },
        )
    raise KeyError(pipeline.validate_task)


def build_proposals(
    pipeline: Pipeline,
    result: dict,
    *,
    namespaces: dict[str, str] | None = None,
    run_id: str,
    threshold: float = 0.05,
    now_ms: int | None = None,
) -> list[dict]:
    """Dựng proposal REST. Chưa gửi. Namespace IT đổi tên dataset."""
    catalog = {item.id: item for item in datasets()}
    selected = pipeline_edges(pipeline)
    involved: dict[str, Dataset] = {}
    inputs: list[str] = []
    outputs: list[str] = []
    for edge in selected:
        for dataset_id, bucket in ((edge.upstream, inputs), (edge.downstream, outputs)):
            dataset = catalog[dataset_id]
            involved[dataset_id] = dataset
            platform, name = qualified_dataset(dataset, namespaces)
            bucket.append(dataset_urn(platform, name))
    stamp = int(time.time() * 1000) if now_ms is None else int(now_ms)
    proposals = [
        _proposal("dataFlow", flow_urn(pipeline.dag_id), "dataFlowInfo", {
            "name": pipeline.dag_id,
            "description": pipeline.description,
        }),
        _proposal("dataFlow", flow_urn(pipeline.dag_id), "status", {"removed": False}),
        _proposal("dataJob", job_urn(pipeline.dag_id, pipeline.task_id), "dataJobInfo", {
            "name": pipeline.task_id,
            "type": {"string": "AIRFLOW"},
            "description": pipeline.description,
            "flowUrn": flow_urn(pipeline.dag_id),
        }),
        _proposal("dataJob", job_urn(pipeline.dag_id, pipeline.task_id), "dataJobInputOutput", {
            "inputDatasets": sorted(set(inputs)),
            "outputDatasets": sorted(set(outputs)),
            "inputDatajobs": [],
        }),
        _proposal("dataJob", job_urn(pipeline.dag_id, pipeline.task_id), "status", {"removed": False}),
    ]
    for dataset in involved.values():
        platform, name = qualified_dataset(dataset, namespaces)
        urn = dataset_urn(platform, name)
        proposals.append(_proposal("dataset", urn, "datasetProperties", {
            "name": dataset.name,
            "description": dataset.grain,
            "customProperties": {
                "inventory_id": dataset.id,
                "qualified_name": name,
                "owner": dataset.owner,
            },
        }))
        proposals.append(_proposal("dataset", urn, "status", {"removed": False}))
    by_name = {item.name: item for item in involved.values()}
    for check in quality_checks(pipeline, result or {}, threshold):
        for table_name in sorted(check["outlets"]):
            dataset = by_name.get(table_name)
            if dataset is None:
                continue
            platform, name = qualified_dataset(dataset, namespaces)
            target = dataset_urn(platform, name)
            urn = assertion_urn(pipeline.dag_id, check["name"], name)
            proposals.append(_proposal("assertion", urn, "assertionInfo", {
                "type": "DATASET",
                "description": check["name"],
                "source": {"type": "NATIVE"},
                "datasetAssertion": {
                    "dataset": target,
                    "scope": "DATASET_ROWS",
                    "operator": "EQUAL_TO",
                    "nativeType": check["name"],
                    "nativeParameters": _strings(check["native"]),
                },
            }))
            proposals.append(_proposal("assertion", urn, "assertionRunEvent", {
                "runId": run_id,
                "timestampMillis": stamp,
                "assertionUrn": urn,
                "asserteeUrn": target,
                "status": "COMPLETE",
                "result": {
                    "type": "SUCCESS" if check["passed"] else "FAILURE",
                    "nativeResults": _strings(check["native"]),
                },
            }))
    return proposals


def emit_pipeline(
    gms_url: str,
    pipeline: Pipeline,
    result: dict,
    *,
    namespaces: dict[str, str] | None = None,
    run_id: str,
    threshold: float = 0.05,
    token: str | None = None,
    transport=None,
) -> dict:
    """Gửi proposal. Không mở Iceberg, MinIO hay ClickHouse."""
    bodies = build_proposals(
        pipeline,
        result,
        namespaces=namespaces,
        run_id=run_id,
        threshold=threshold,
    )
    sender = transport or post_proposal
    for body in bodies:
        sender(gms_url, body, token)
    return {
        "dag_id": pipeline.dag_id,
        "task_id": pipeline.task_id,
        "run_id": run_id,
        "proposals": len(bodies),
        "gms_url": gms_url.rstrip("/"),
    }


def post_proposal(gms_url: str, body: dict, token: str | None = None) -> None:
    url = gms_url.rstrip("/") + GMS_PATH
    request = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST")
    request.add_header("Content-Type", "application/json")
    request.add_header("X-RestLi-Protocol-Version", "2.0.0")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:500]
        raise RuntimeError(f"DataHub GMS HTTP {exc.code} tại {url}: {detail}") from exc


def publish_datahub(**context) -> dict:
    """Task Airflow đứng sau validate. Fail GMS không rollback batch đã ghi."""
    dag_id = context["dag"].dag_id
    pipeline = pipeline_for(dag_id)
    params = _params(context)
    result = dict(context["ti"].xcom_pull(task_ids=pipeline.validate_task) or {})
    state = _task_state(context["ti"], pipeline.validate_task)
    if state not in (None, "success"):
        result["ready"] = False
        result["reason"] = result.get("reason") or f"{pipeline.validate_task} {state}"
    namespaces = {
        "bronze": str(params.get("bronze_namespace") or "bronze"),
        "silver": str(params.get("silver_namespace") or "silver"),
        "gold": str(params.get("gold_namespace") or "gold"),
    }
    threshold = float(params.get("error_threshold") or 0.05)
    gms_url = str(params.get("datahub_gms_url") or os.environ.get("DATAHUB_GMS_URL") or "http://datahub-gms:8080")
    token = os.environ.get("DATAHUB_GMS_TOKEN") or None
    summary = emit_pipeline(
        gms_url,
        pipeline,
        result,
        namespaces=namespaces,
        run_id=str(context["run_id"]),
        threshold=threshold,
        token=token,
    )
    print("DATAHUB_JSON=" + json.dumps(summary))
    return summary


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Chiếu một pipeline DP lên DataHub GMS.")
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--gms-url", default=os.environ.get("DATAHUB_GMS_URL", "http://localhost:18083"))
    parser.add_argument("--result-json", default="{}")
    parser.add_argument("--bronze-namespace", default="bronze")
    parser.add_argument("--silver-namespace", default="silver")
    parser.add_argument("--gold-namespace", default="gold")
    parser.add_argument("--error-threshold", type=float, default=0.05)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    pipeline = pipeline_for(args.dag_id)
    result = json.loads(args.result_json)
    namespaces = {
        "bronze": args.bronze_namespace,
        "silver": args.silver_namespace,
        "gold": args.gold_namespace,
    }
    if args.dry_run:
        bodies = build_proposals(
            pipeline,
            result,
            namespaces=namespaces,
            run_id=args.run_id,
            threshold=args.error_threshold,
        )
        print(json.dumps({"proposals": len(bodies), "dag_id": pipeline.dag_id}))
        return
    summary = emit_pipeline(
        args.gms_url,
        pipeline,
        result,
        namespaces=namespaces,
        run_id=args.run_id,
        threshold=args.error_threshold,
        token=os.environ.get("DATAHUB_GMS_TOKEN") or None,
    )
    print("DATAHUB_JSON=" + json.dumps(summary))


def _params(context) -> dict:
    params = dict(context.get("params") or {})
    dag_run = context.get("dag_run")
    incoming = dict(getattr(dag_run, "conf", None) or {})
    params.update({key: value for key, value in incoming.items() if value is not None})
    return params


def _task_state(ti, task_id: str) -> str | None:
    getter = getattr(ti, "get_dagrun", None)
    if getter is None:
        return None
    dag_run = getter()
    if dag_run is None:
        return None
    other = dag_run.get_task_instance(task_id)
    if other is None:
        return None
    return getattr(other, "state", None)


def _proposal(entity_type: str, urn: str, aspect_name: str, aspect: dict) -> dict:
    return {
        "proposal": {
            "entityType": entity_type,
            "entityUrn": urn,
            "changeType": "UPSERT",
            "aspectName": aspect_name,
            "aspect": {
                "value": json.dumps(aspect, default=str),
                "contentType": "application/json",
            },
        }
    }


def _strings(payload: dict) -> dict[str, str]:
    rendered = {}
    for key, value in payload.items():
        if isinstance(value, str):
            rendered[str(key)] = value
        else:
            rendered[str(key)] = json.dumps(value, default=str)
    return rendered


if __name__ == "__main__":
    main()
