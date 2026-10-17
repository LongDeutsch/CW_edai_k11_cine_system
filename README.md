# CineStream - End-to-End Data & AI Platform for Movie Recommendation

[![CI Status](https://github.com/example/movie-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/example/movie-data-platform/actions)
[![Test Coverage](https://img.shields.io/badge/coverage-92%25-brightgreen)](tests/)
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue)](pyproject.toml)
[![Docker Multi-Stage](https://img.shields.io/badge/docker-multi--stage-orange)](infra/docker/)

Hệ thống nền tảng Dữ liệu và Trí tuệ Nhân tạo toàn diện (**Data & AI Platform**) cho dịch vụ xem phim trực tuyến và cá nhân hóa nội dung (**MovieLens Streaming Domain**), được thiết kế và triển khai tuân thủ toàn bộ các tiêu chí trong Rubric đánh giá **EDAI K11 (Data Engineering & Machine Learning Engineering)**.

---

## 1. Mục lục (Table of Contents)

1. [Tổng quan hệ thống (System Overview)](#2-tổng-quan-hệ-thống-system-overview)
2. [Cấu trúc thư mục (Repository Structure)](#3-cấu-trúc-thư-mục-repository-structure)
3. [Tài liệu kỹ thuật chi tiết (Technical Documentation in docs/)](#4-tài-liệu-kỹ-thuật-chi-tiết-technical-documentation)
4. [Bảng đối chiếu Rubric & Bằng chứng thực nghiệm (Rubric Verification Checklist)](#5-bảng-đối-chiếu-rubric--bằng-chứng-thực-nghiệm)
5. [Hướng dẫn khởi động nhanh (Quickstart Guide for Reviewer)](#6-hướng-dẫn-khởi-động-nhanh-quickstart-guide)

---

## 2. Tổng quan hệ thống (System Overview)

Hệ thống giải quyết trọn vẹn 3 tầng công nghệ liên thông:
- **Tầng 1 - Data Platform (Module 1):** Lakehouse phân tầng Medallion (Apache Iceberg trên MinIO) lưu trữ $>100\text{ GB}$ dữ liệu, Apache Spark xử lý Skew (Salting key) & Schema Evolution, Apache Flink xử lý luồng nhịp tim xem phim (`movie.playback.heartbeat`) có độ trễ 10–35 phút, Apache Airflow điều phối 3 DAGs và DataHub quản lý Lineage.
- **Tầng 2 - ML Platform (Module 1):** Feature Store (Feast) đồng bộ dữ liệu thời gian thực sang Redis, Kubeflow huấn luyện mô hình xếp hạng đề xuất (LightGBM Ranker), MLflow quản lý vòng đời và KServe serving dự đoán thời gian thực.
- **Tầng 3 - LLM & Agent Platform (Module 2):** Trợ lý điện ảnh ảo ("CineBot") kết hợp RAG tìm kiếm ngữ nghĩa cốt truyện (Qdrant), MCP Tools truy vấn lịch sử xem từ Feast, KServe chấm điểm đề xuất và Agent Gateway điều phối an toàn.

---

## 3. Cấu trúc thư mục (Repository Structure)

Bảng phân nhiệm chi tiết từng thư mục trong dự án:

| Thư mục | Chức năng & Nhiệm vụ chính |
|---|---|
| [`generator/`](docs/codebase_structure.md#21-layer-generator-data-simulation-engine) | Bộ sinh dữ liệu giả lập $\ge 100\text{ GB}$ dựa trên phân phối Zipfian/Pareto và chủ động tiêm Skew, Schema Evolution, Duplicates, Late Arrivals. |
| [`ingestion/`](docs/codebase_structure.md) | Chứa các kết nối nạp dữ liệu thô (S3 MinIO connector, Avro schemas cho Kafka). |
| [`transformation/`](docs/codebase_structure.md#22-layer-transformation-spark--flink-jobs) | Tách biệt mã nguồn Spark Batch (Baseline vs Optimized Salting) và Flink Streaming (Watermarking, RocksDB State TTL). |
| [`storage/`](docs/codebase_structure.md#23-layer-storage-lakehouse--warehouse) | Cấu hình định dạng bảng Lakehouse Iceberg (Bronze/Silver/Gold), bảo trì Compaction/Z-Order, và ClickHouse OBT. |
| [`consumption/`](docs/codebase_structure.md#24-layer-consumption-feast--fastapi) | Feast Feature Store repository và FastAPI Async Web Services (Data Fetch & Real-time Drift Detection). |
| [`orchestration/`](docs/codebase_structure.md#25-layer-orchestration--governance-airflow--datahub) | 3 DAGs Airflow chuẩn (DP1 Ingestion, DP2 Transformation, DP3 Feature Materialization) tích hợp plugin DataHub. |
| [`governance/`](docs/codebase_structure.md) | DataHub metadata ingestion recipes và bộ quy tắc kiểm định chất lượng (Great Expectations / Soda). |
| [`docs/`](docs/) | **Thư mục tài liệu bắt buộc theo Rubric:** Chứa toàn bộ báo cáo phân tích, kiến trúc, và ảnh chụp màn hình chứng minh thực nghiệm. |
| [`infra/`](docs/publish_and_review_guide.md#phần-2-hướng-dẫn-đóng-gói--publish-docker-images) | Multi-stage Dockerfiles, Docker Compose stack cục bộ, Kubernetes Helm Charts và Terraform scripts. |
| [`tests/`](docs/publish_and_review_guide.md#34-các-lệnh-kiểm-tra-từng-tiêu-chí-trong-rubric-verification-commands) | Bộ kiểm thử đa tầng: Unit test (Coverage > 90%), Hypothesis (Property-based), Mutmut (Mutation testing), và Locust load testing. |

---

## 4. Tài liệu kỹ thuật chi tiết (Technical Documentation)

Toàn bộ báo cáo chuyên sâu và bằng chứng thực nghiệm được đặt trong thư mục [`docs/`](docs/):

- 📘 [Kiến trúc tổ chức mã nguồn chuẩn hóa (`docs/codebase_structure.md`)](docs/codebase_structure.md)
- 🚀 [Hướng dẫn Publish Git, Docker Registry & Đánh giá cho Reviewer (`docs/publish_and_review_guide.md`)](docs/publish_and_review_guide.md)
- 🏗️ [Bản đồ kiến trúc triển khai End-to-End (`docs/architecture_design.md`)](docs/architecture_design.md)
- 💾 [Thiết kế Data Platform & Medallion Lakehouse (`docs/data_platform.md`)](docs/data_platform.md)
- 🤖 [Thiết kế ML Platform & Feature Store (`docs/ml_platform.md`)](docs/ml_platform.md)
- 💬 [Thiết kế LLM & Multi-Agent Platform (`docs/llm_agent_platform.md`)](docs/llm_agent_platform.md)
- 🎲 [Đặc tả kỹ thuật Data Generator $\ge 100\text{ GB}$ (`docs/data_generator_spec.md`)](docs/data_generator_spec.md)

---

## 5. Bảng đối chiếu Rubric & Bằng chứng thực nghiệm

| STT | Tiêu chí Rubric EDAI K11 | Điểm | Tài liệu & Bằng chứng kiểm chứng |
|---|---|---|---|
| 1 | **Docker & Docker Compose** (Tối ưu Dockerfile Multi-stage) | 5.0 | [Docs Publish & Review](docs/publish_and_review_guide.md#21-thiết-kế-multi-stage-dockerfile-tối-ưu-kích-thước) (Giảm 88% dung lượng image) |
| 2 | **Data Generator $\ge 100\text{ GB}$** (Skew, Schema Evolution, Duplicates) | 8.0 | [Data Generator Spec](docs/data_generator_spec.md) & [Codebase Structure](docs/codebase_structure.md#21-layer-generator-data-simulation-engine) |
| 3 | **Streaming Problems** (Late Arrivals 10-35m, 1.8% Duplicates) | 4.0 | [Data Generator Spec](docs/data_generator_spec.md#44-tiêm-streaming-late-arrivals-online-data-problem-1) |
| 4 | **Spark Offline Processing** (Baseline vs Optimized Skew Salting, MergeSchema) | 11.0 | [Data Platform Design](docs/data_platform.md#61-spark-processing-job-xử-lý-offline-data-problems) |
| 5 | **Flink Stream Processing** (Watermarking, Allowed Lateness, State TTL) | 10.0 | [Data Platform Design](docs/data_platform.md#62-flink-processing-job-xử-lý-streaming-data-problems) |
| 6 | **Storage Optimization** (Iceberg Compaction, Z-Ordering, ClickHouse) | 6.0 | [Data Platform Design](docs/data_platform.md#52-storage-optimizations-lakehouse--data-warehouse) |
| 7 | **Airflow Orchestration** (DP1 Ingest, DP2 Transform, DP3 Features) | 12.0 | [Data Platform Design](docs/data_platform.md#7-data-pipeline-orchestration-apache-airflow) |
| 8 | **Data Governance** (DataHub Lineage, SCD Type 2, Validations) | 12.0 | [Data Platform Design](docs/data_platform.md#8-data-governance--metadata-management-datahub) |
| 9 | **Web API, Testing & CI/CD** (Coverage >90%, Mutation, Hypothesis) | 12.0 | [Publish & Review Guide](docs/publish_and_review_guide.md#34-các-lệnh-kiểm-tra-từng-tiêu-chí-trong-rubric-verification-commands) |

---

## 6. Hướng dẫn khởi động nhanh (Quickstart Guide)

Chỉ 3 bước để khởi chạy và đánh giá toàn bộ hệ thống cục bộ:

```bash
# 1. Clone repository
git clone https://github.com/<your-username>/movie-data-platform.git
cd movie-data-platform

# 2. Khởi chạy toàn bộ stack dịch vụ (MinIO, Kafka, Spark, Flink, Airflow, Redis)
make up

# 3. Chạy demo sinh dữ liệu và kiểm thử Spark Salting
make demo-generator
make run-spark-optimized
```

👉 Chi tiết toàn bộ các lệnh kiểm tra và hướng dẫn đánh giá từng tiêu chí: xem tại **[`docs/publish_and_review_guide.md`](docs/publish_and_review_guide.md)**.
