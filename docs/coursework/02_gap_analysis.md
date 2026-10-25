# 02 — Gap Analysis: Cinestream Compliance & Architecture Audit

> **Status:** AUDIT COMPLETE | **Target:** EDAI K11 Coursework Proposal & Rubric `EDAI K11 - DE.xlsx`

## 1. Executive Gap Summary

The Cinestream platform demonstrates engineering rigor in its foundational Data Engineering tier (Module 1), with solid implementations of Apache Iceberg Medallion architecture, Apache Spark skew handling via salting keys, Apache Flink tumbling window state management, and custom automated governance triage.

However, a strict compliance audit reveals critical discrepancies between documentation claims, code reality, and grading rubric expectations:
1. **Discrepancy Between Docs and Implementation:** README and specification guides claim components that do not actually exist in code or runtime deployment (e.g., DataHub at port 9002, Multi-stage Docker optimization reducing images by 88%, and a 100 GB offline dataset).
2. **DataHub Absence:** The repository does not deploy DataHub. All lineage and metadata operations are handled by an in-house Python module (`governance/`) and custom dashboard UI, whereas the rubric explicitly awards points for DataHub UI lineage and assertions.
3. **Airflow Pipeline Consolidation:** The rubric requires 3 distinct, independent pipelines (DP1 Ingestion, DP2 Transformation, DP3 Feature Materialization). The codebase collapses these into a single monolithic DAG (`cinestream_daily_batch_lakehouse.py`).
4. **Module 2 Unimplemented State:** The 2026-10-23 audit left Module 2 at 2.0 verified points. B2 (2026-10-25) verifies the offline-to-online Airflow materialize, so verified points are 4.0 and missing points are 90.0. Streaming push, Kubeflow, MLflow, KServe, vLLM/Ollama, MCP Gateway, Agent Fleet, and Helm charts are still absent.

---

## 2. Deep-Dive Gap Analysis: Module 1 (Data Platform)

### 2.1. Data Generator & Volume
- **Rubric Requirement (R5):** Simulate offline data problems with a minimum volume of 100 GB (`>= 100GB`).
- **Actual Reality:** The disk contains only `data_generator/output/scale_0_1` (27 MB Parquet). While `generator/offline_generator.py` mathematically supports `--scale-factor 100` (~101 GB), the full dataset was never generated or stored.
- **Compliance Impact:** Rated `PARTIAL`. Reviewers verifying disk volume will not find >=100 GB.
- **Remediation:** Execute generator at scale 100 on external storage/MinIO, or generate dedicated proof logs and disk metrics.

### 2.2. Multi-Stage Dockerfile Optimization
- **Rubric Requirement (R4):** Optimize Dockerfile (e.g. multi-stage build), document size comparison.
- **Actual Reality:** All Dockerfiles (`Dockerfile.spark`, `Dockerfile.flink`, `Dockerfile.airflow`, `Dockerfile.lakehouse-ui`) are single-stage (`FROM ...`). The claims in `docs/spec/publish_and_review_guide.md` regarding an 85.5% reduction from 1.28 GB to 185 MB for `data-generator` are fabricated narrative—there is no `Dockerfile.data-generator` in the codebase.
- **Compliance Impact:** Rated `MISSING` (0/3 pts).
- **Remediation:** Refactor Dockerfiles to use genuine multi-stage builds (`AS builder`), discard build dependencies, and capture real `docker images` size comparisons.

### 2.3. Spark Baseline vs Optimized Jobs
- **Rubric Requirements (R11–R14):** Baseline unoptimized job vs optimized job handling skew, schema evolution, and duplicates, with Spark UI screenshots.
- **Actual Reality:** Implemented well. `transformation/batch/baseline/unoptimized_join.py` and `transformation/batch/optimized/handle_skew_salting.py` exist. Screenshot `spark-ui-stage5-skew.png` is provided.
- **Compliance Impact:** Rated `VERIFIED` (11/11 pts).

### 2.4. Flink Streaming Optimization & Proof
- **Rubric Requirements (R16–R19):** Baseline streaming job vs optimized handling of late arrivals (10–35 min), duplicates (1.8%), and 1-minute window processing with Flink UI proof.
- **Actual Reality:** `streaming/flink/jobs/heartbeat_window_job.py` implements watermarking, tumbling window, late data side output, and RocksDB state TTL. Unit tests pass (16/16). However, there is NO baseline unoptimized Flink job, and NO Flink Web UI screenshots exist in `docs/proof/images/`.
- **Compliance Impact:** Rated `PARTIAL` on baseline, `IMPLEMENTED_NOT_PROVEN` on live proof.
- **Remediation:** Provide a baseline Flink job script; run the streaming job live and capture Flink Web UI screenshots showing watermark progression and DLQ side output.

### 2.5. Airflow Pipeline Orchestration
- **Rubric Requirements (R22–R27):** 3 distinct pipelines: DP1 (Raw -> Bronze), DP2 (Bronze -> Silver/Gold), DP3 (Offline Feature Table), each with Ingest and Validate stages.
- **Actual Reality:** Only ONE DAG file exists: `orchestration/dags/cinestream_daily_batch_lakehouse.py`. It sequentially runs raw-to-bronze, validate-bronze, bronze-to-gold, and validate-gold. DP3 is not separated, and offline feature tables (`feat_*`) are never computed or populated with rows.
- **Compliance Impact:** Rated `PARTIAL` (6/12 pts).
- **Remediation:** Split the monolithic DAG into 3 distinct DAGs: `cinestream_dp1_raw_to_bronze`, `cinestream_dp2_medallion_transform`, and `cinestream_dp3_feature_materialization`.

### 2.6. Data Governance: DataHub vs Custom Governance
- **Rubric Requirements (R28–R33):** Pipeline-to-table lineage on DataHub, data validation assertions on DataHub for all 3 pipelines.
- **Actual Reality:** DataHub is NOT deployed. The project built a custom Python module (`governance/`) and a custom dashboard UI (`governance.html`). Reports in `docs/proof/` explicitly admit: *"Không có DataHub"* (`08_data_governance_lineage.md`).
- **Compliance Impact:** Rated `PARTIAL` (6/12 pts). Custom innovation is commendable as a novel idea, but directly fails the explicit rubric requirement for DataHub.
- **Remediation:** Deploy DataHub GMS + Frontend via Docker Compose, configure `datahub-airflow-plugin`, and ingest metadata to display native DataHub lineage and assertions.

---

## 3. Deep-Dive Gap Analysis: Module 2 (MLE & Agent Platform)

Module 2 still has a 90-point missing gap after B2. The following table highlights the critical missing pillars:

| Pillar | Rubric Rows | Points | Status | Gap Description |
| :--- | :---: | :---: | :---: | :--- |
| **LLM Inference Server** | R3–R6 | 7.0 | `MISSING` | No vLLM or Ollama deployment; no benchmark or latency optimization. |
| **Global Model Config & Registry** | R7–R8 | 4.0 | `MISSING` | No centralized model YAML config; no Agent Registry service. |
| **RAG Pipeline & Embeddings** | R9–R10 | 4.0 | `MISSING` | No chunking, vector embedding generation, or Qdrant/Feast integration. |
| **Feast Feature Store** | R11–R12 | 4.0 | `PARTIAL` | R11 is verified: DP4 materializes a snapshot-pinned export into Redis. R12 remains missing: no streaming push into either store. |
| **ML Pipelines (Kubeflow/MLflow)** | R13–R14 | 4.0 | `MISSING` | No training notebook, Kubeflow pipeline DSL, or MLflow model registry integration. |
| **KServe Model Serving** | R15 | 2.0 | `MISSING` | No KServe Custom Predictor manifest or model-inside-image container. |
| **Web API & Multi-Agent Fleet** | R18–R27 | 21.0 | `MISSING` | No MCP tools, Coordinator Agent, Sub-agents, Sandboxing, or Warm-up Fleet. |
| **Advanced Testing (V&V)** | R29–R32 | 8.0 | `MISSING` | No Equivalence Partitioning matrix, no Mutmut mutation tests, no Hypothesis tests, no Locust SLA report. |
| **CI/CD Automation** | R33–R36 | 8.0 | `MISSING` | No GitHub Actions workflows or Jenkinsfiles for pipelines or agents. |
| **Gateway, Auth & HTTPS** | R37–R43 | 13.0 | `MISSING` | No NGINX Ingress Controller, reverse proxy, basic auth, or TLS certificates. |
| **IaC (Terraform & Ansible)** | R44–R45 | 2.0 | `MISSING` | No Terraform `.tf` files; no Ansible playbooks. |
| **Observability Telemetry** | R47–R51 | 7.0 | `MISSING` | No Prometheus/Grafana stack; no LLM token telemetry or Agent failure metrics. |
| **A/B Testing & Vault** | R52–R54 | 3.0 | `MISSING` | No traffic split for models/agents; no HashiCorp Vault. |

---

## 4. Critical Integration Gaps Analysis (Mandatory 7 Questions)

### Q1: Whether offline feature tables have actual populated data?
**Finding:** **NO.**
- `storage/lakehouse/gold/gold_tables.sql` defines schemas for `feat_user_movie_stats_daily` and `feat_movie_engagement_stats_daily` with required timestamps.
- However, `data_generator/output/lakehouse/gold/` contains only `dim_movies`, `dim_users`, `fact_ratings`, `fact_watch_sessions`, and `obt_movie_engagement`. No parquet files exist for `feat_*`.
- Test assertions in `tests/governance/test_observe.py` explicitly check `test_empty_feature_table_has_no_downstream`.

### Q2: Whether streaming outputs can become Feast features?
**Finding:** **ARCHITECTURALLY COMPATIBLE, BUT UNCONNECTED.**
- Flink aggregations (`stg_playback_windows`) output to Iceberg and ClickHouse (`serving/clickhouse/sync_windows.py`).
- They contain event-time window metrics (`play_count`, `total_watch_seconds`).
- However, no Feast stream ingestion or push source is configured.

### Q3: Whether any ML prediction problem has been implemented?
**Finding:** **NO.**
- Zero ML prediction code, training scripts, or evaluated models exist in the repository.

### Q4: Whether training -> registry -> model serving works end-to-end?
**Finding:** **NO.**
- Neither Kubeflow, MLflow, nor KServe are installed or integrated.

### Q5: Whether an LLM/Agent can call ML inference and retrieve features?
**Finding:** **NO.**
- The LLM inference platform, MCP tools, and agent layers are absent.

### Q6: Whether RAG and agents operate in the same Cinestream domain?
**Finding:** **YES IN DESIGN, NO IN IMPLEMENTATION.**
- The conceptual design in `docs/spec/llm_agent_platform.md` specifies "CineBot" operating on MovieLens plot summaries and watch history.
- However, no code exists.

### Q7: Whether the complete system meets the single-domain requirement?
**Finding:** **YES.**
- Despite legacy notes mentioning e-commerce in `docs/domain/ecommerce/`, all implemented code strictly models the **MovieLens Streaming domain** (`movies`, `users`, `ratings`, `playback heartbeats`, `watch sessions`).

---

## 5. Recommended Movie-Domain ML Tasks (Feasible with Current Foundation)

To satisfy Module 2 requirements with minimal friction, we recommend 3 candidate ML problems grounded in Cinestream's existing schemas:

1. **Task 1: Movie Rating Prediction (Classification / Regression)**
   - **Target:** Predict user rating score (1.0–5.0) or binary like (`rating >= 4.0`).
   - **Offline Features:** User historical average rating, user genre affinity count (from `feat_user_movie_stats_daily`), movie overall mean rating.
   - **Streaming Features:** User recent watch duration in the last 15 minutes, movie trending play velocity from `stg_playback_windows`.
   - **Model:** LightGBM Regressor / Classifier.

2. **Task 2: Playback Abandonment / Churn Prediction**
   - **Target:** Predict if an ongoing watch session will drop off before 50% completion.
   - **Offline Features:** User historical session completion rate, subscription tier (`VIP`, `Standard`, `Free` from SCD2 `dim_users`).
   - **Streaming Features:** Heartbeat ping interval regularity, buffer stall count from live stream.
   - **Model:** XGBoost Classifier.

3. **Task 3: Next-Movie Recommendation Candidate Ranking**
   - **Target:** Rank top 10 candidate movies for user home screen.
   - **Offline Features:** User-movie collaborative embedding dot product, genre match vector.
   - **Streaming Features:** Currently active genre velocity, real-time trending score.
   - **Model:** Two-Tower DSSM or LambdaMART Ranker.