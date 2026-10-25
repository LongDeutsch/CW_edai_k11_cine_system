# 04 — Evidence Inventory: Artifacts, Logs, Tests & Deployment Proof

> **Audit Target:** Verification of all physical artifacts, log files, screenshots, and test suites in the Cinestream codebase.

## 1. Verified Test Suites & Execution Output

The test suite was executed against the repository under Python 3.13:
```bash
PYTHONPATH=. pytest --ignore=tests/unit/test_timestamp_contract.py -v
```
**Result:** **75 Passed, 2 Skipped, 0 Failed** in 0.30 seconds.

| Test File | Test Cases | Status | Scope & Purpose |
| :--- | :---: | :---: | :--- |
| `tests/unit/test_scd2_logic.py` | 6 | `PASSED` | Verifies SCD Type 2 state changes, surrogate keys, and tier transitions (`dim_users`). |
| `tests/streaming/test_dedup_ttl.py` | 4 | `PASSED` | Verifies RocksDB State TTL expiration and duplicate heartbeat elimination. |
| `tests/streaming/test_watermark_and_late_data.py` | 6 | `PASSED` | Verifies BoundedOutOfOrderness watermarking and late data threshold. |
| `tests/streaming/test_window_semantics.py` | 6 | `PASSED` | Verifies 1-minute tumbling window membership and dead-letter expulsion. |
| `tests/governance/test_lineage.py` | 6 | `PASSED` | Verifies automated lineage edge extraction and dataset relationship consistency. |
| `tests/governance/test_observe.py` | 16 | `PASSED` | Verifies schema drift classification, breaking change detection, and health checks. |
| `tests/governance/test_schema_contract.py` | 8 | `PASSED` | Verifies Iceberg schema consistency contracts and partition safety. |
| `tests/governance/test_triage.py` | 11 | `PASSED` | Verifies Governance Triage Engine rules isolating production from drift. |
| `tests/governance/test_governance_api.py` | 8 | `PASSED` (2 skipped) | Verifies FastAPI `/governance` endpoints and view models. |
| `orchestration/test_batch_identity.py` | 4 | `PASSED` | Verifies idempotency and deterministic S3 batch hash generation. |

> *Note on PySpark Unit Test:* `tests/unit/test_timestamp_contract.py` requires `pyspark` inside the runner environment; when executed inside the Spark container, it verifies timestamp schema contracts.

---

## 2. Inventory of Existing Proof Documents & Screenshots

The repository contains 14 detailed proof documents under `docs/proof/`:

| Document | Target Layer | Key Verified Artifacts & Data |
| :--- | :--- | :--- |
| [`01_data_generator_proof.md`](../proof/01_data_generator_proof.md) | Generator | Parquet output scale 0.1, Zipf skew (movie 42 = 79.94%), duplicate 2.5%, late arrivals 6.0%. |
| [`02_lakehouse_storage_proof.md`](../proof/02_lakehouse_storage_proof.md) | Lakehouse Storage | Iceberg catalog DDL, FFD Bin-Packing compaction (16 files -> 3 files), SCD Type 2 logic. |
| [`03_spark_optimizations_proof.md`](../proof/03_spark_optimizations_proof.md) | Spark Batch | Baseline vs Salting Key join metrics, AQE comparison, schema evolution handling. |
| [`04_streaming_flink_proof.md`](../proof/04_streaming_flink_proof.md) | Flink Streaming | Tumbling window 1m, watermark lateness 15s, RocksDB TTL 1h, Kafka DLQ side output. |
| [`05_orchestration_governance_proof.md`](../proof/05_orchestration_governance_proof.md) | Airflow | Batch identity hashing, sensor checks, execution guard `CINESTREAM_ALLOW_PRODUCTION`. |
| [`05_incremental_batch_proof.md`](../proof/05_incremental_batch_proof.md) | Airflow & Iceberg | Incremental snapshot commit behavior and idempotency checks. |
| [`06_consumption_serving_proof.md`](../proof/06_consumption_serving_proof.md) | Serving & ClickHouse | ClickHouse MergeTree DDL, partition pruning by month, staging table isolation. |
| [`07_analytics_dashboard.md`](../proof/07_analytics_dashboard.md) | Analytics UI | Web analytics dashboard at `:8088`, live metric display. |
| [`08_data_governance_lineage.md`](../proof/08_data_governance_lineage.md) | Governance | Lineage extraction from Spark/ClickHouse/FastAPI code; acknowledges DataHub absence. |
| [`09_governance_observability.md`](../proof/09_governance_observability.md) | Governance | Pipeline health checks, drift detection, Iceberg metadata audit. |
| [`10_governance_findings_triage.md`](../proof/10_governance_findings_triage.md) | Governance | Triage classification engine separating breaking schema changes from valid evolutions. |
| [`11_schema_contract_validation.md`](../proof/11_schema_contract_validation.md) | Lakehouse Contracts | Schema consistency rules, nullability checks, constraint validations. |
| [`12_governance_dashboard.md`](../proof/12_governance_dashboard.md) | Governance UI | Read-only `/governance` routes and web lineage interface. |

### Physical Image Artifacts in `docs/proof/images/`
- `spark-ui-stage5-skew.png`: Screenshot of Spark Web UI Stage 5 demonstrating skew before/after salting.
- `lineage-before.png`: Custom dashboard lineage graph before pipeline run.
- `lineage-after.png`: Custom dashboard lineage graph showing populated table dependencies.
- `phase4_batch.png`, `phase4_trending.png`, `phase4_mobile.png`, `phase4b_unified.png`: Screenshots of the ClickHouse serving dashboard.

### Missing Critical Image Artifacts Required by Rubric
- ❌ **Airflow UI Screenshot showing 3 distinct DAGs** (`DP1`, `DP2`, `DP3`).
- ❌ **Flink Web UI Screenshot** displaying watermark progression, window processing, and DLQ drop metrics.
- ❌ **DataHub Web UI Screenshot** (`:9002`) showing pipeline-to-table lineage and dataset assertions.
- ❌ **DBeaver ERD Diagram Export** showing relational constraints.
- ❌ **Locust Load Test HTML Report** showing SLA throughput and latency.
- ❌ **Mutmut Mutation Testing Score Report** showing mutation coverage > 80%.
- ❌ **Kubeflow Pipelines & MLflow Model Registry UI Screenshots**.
- ❌ **Agent Registry UI Screenshot**.

---

## 3. Physical Datasets on Disk

```
data_generator/output/
├── lakehouse/ (44 MB)
│   ├── silver/stg_watch_sessions/
│   ├── silver/stg_movies/
│   └── gold/
│       ├── dim_movies/
│       ├── dim_users/
│       ├── fact_ratings/
│       ├── fact_watch_sessions/
│       └── obt_movie_engagement/
└── scale_0_1/ (27 MB)
    ├── movies/ (partitions 1–100, schema evolution at 45)
    ├── ratings/
    ├── tags/
    ├── users/
    └── watch_history/
```

**Finding:** Total generated offline data on disk is ~71 MB, representing scale factor 0.1 (0.1% volume). Scale factor 100 (~101 GB) has not been populated on disk.