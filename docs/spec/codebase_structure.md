# Movie Recommendation - Optimized Source Code Architecture & Structure

Tài liệu này đặc tả chi tiết kiến trúc tổ chức mã nguồn (Directory & Codebase Architecture) tối ưu hóa dành riêng cho hệ thống **Data Engineering Nền tảng Đề xuất Phim (Movie Recommendation & Streaming Analytics Platform)**, đáp ứng toàn diện các tiêu chí đánh giá trong Rubric **EDAI K11 (DE & MLE)**.

---

## 1. Bản đồ tổng thể cấu trúc mã nguồn (Directory Tree)

```text
movie-data-platform/
│
├── generator/                              # [Rubric 4-9] Sinh dữ liệu >=100GB & Chủ động tiêm lỗi
│   ├── distributions/                      # Các mô hình phân phối thống kê thực tế
│   │   ├── __init__.py
│   │   ├── zipf.py                         # Phân phối phim bom tấn (1% phim chiếm 80% tương tác)
│   │   ├── pareto.py                       # Phân phối người dùng tích cực (5% super-users chiếm 40%)
│   │   └── beta_rating.py                  # Phân phối điểm rating thực tế (lệch về 3.5 - 4.5 sao)
│   ├── injectors/                          # Cơ chế tiêm sự cố dữ liệu có kiểm soát
│   │   ├── __init__.py
│   │   ├── skew_injector.py                # Tiêm Skew trên movie_id và thể loại Action/Comedy
│   │   ├── schema_evolution_injector.py    # Tiêm cột content_rating, hdr_format sau batch 45
│   │   ├── duplicate_injector.py           # Tiêm 2.5% bản ghi trùng lặp offline
│   │   └── late_arrival_injector.py        # Tiêm 6% sự kiện Kafka trễ 10-35 phút
│   ├── offline_generator.py                # Mở rộng ml-latest-small thành >=100GB Parquet nạp MinIO S3
│   └── streaming_producer.py               # Kafka Producer bắn playback heartbeats & clickstreams
│
├── ingestion/                              # [Rubric 21-22] Nạp dữ liệu thô vào Bronze Zone
│   ├── connectors/                         # Kết nối nguồn (MinIO S3, Postgres CDC)
│   │   ├── __init__.py
│   │   └── s3_source_connector.py          # Kéo dữ liệu gia tăng từ MinIO nguồn ngoài
│   ├── schemas/                            # Lược đồ tuần tự hóa Kafka (Avro / Protobuf)
│   │   ├── playback_heartbeat.avsc         # Định dạng nhịp tim phát phim (10s/lần)
│   │   └── user_clickstream.avsc           # Định dạng hành vi lướt UI, search, watchlist
│   └── config/
│       └── ingestion_config.yaml
│
├── transformation/                         # [Rubric 10-18] Xử lý phân tán trên Spark & Flink
│   ├── batch/                              # Apache Spark Jobs (Xử lý dữ liệu Offline)
│   │   ├── baseline/                       # Code nguyên bản chưa tối ưu (chứng minh nghẽn task trên Spark UI)
│   │   │   └── unoptimized_join.py
│   │   ├── optimized/                      # Code đã áp dụng kỹ thuật tối ưu theo Rubric
│   │   │   ├── handle_skew_salting.py      # Kỹ thuật Salting Key + Broadcast Join khử Skew
│   │   │   ├── handle_schema_merge.py      # Tự động đọc và dung hợp schema Parquet mới/cũ
│   │   │   └── handle_deduplication.py     # Window function row_number() loại bỏ 2.5% duplicate
│   │   └── scd/
│   │       └── dim_users_scd2.py           # Quản lý lịch sử nâng cấp gói cước Free/VIP (SCD Type 2)
│   │
│   └── streaming/                          # Apache Flink Jobs (Xử lý dữ liệu thời gian thực)
│       ├── baseline/                       # Flink job cơ bản dùng Processing Time (mất dữ liệu trễ)
│       │   └── unoptimized_window.py
│       └── jobs/                           # Flink job tối ưu theo Rubric
│           ├── playback_stream_job.py      # Entrypoint Flink streaming application
│           ├── operators/
│           │   ├── watermark_assigner.py   # BoundedOutOfOrderness xử lý dữ liệu trễ 10-35 phút
│           │   ├── state_deduplication.py  # Khử lặp 1.8% streaming bằng RocksDB State TTL 2 giờ
│           │   └── sliding_window.py       # Cửa sổ tính số người xem đồng thời & tỷ lệ skip phim
│           └── side_outputs/
│               └── late_data_sink.py       # Định tuyến dữ liệu siêu trễ (>35m) về audit storage
│
├── storage/                                # [Rubric 19-20, 33-37] Lakehouse & Data Warehouse
│   ├── lakehouse/                          # Cấu hình Apache Iceberg / Delta Lake
│   │   ├── bronze/                         # raw_users, raw_movies, raw_ratings, raw_watch_history
│   │   ├── silver/                         # stg_users, stg_movies, stg_ratings, stg_watch_sessions
│   │   ├── gold/                           # dim_users, dim_movies, fact_ratings, fact_watch_sessions
│   │   └── maintenance/
│   │       ├── compaction.py               # Bin-packing gom file nhỏ thành file chuẩn 128MB
│   │       └── z_order_clustering.sql      # Đa chiều Z-Ordering theo (movie_id, user_id)
│   └── warehouse/                          # ClickHouse Analytical Data Warehouse
│       ├── clickhouse_obt.sql              # Bảng hợp nhất phẳng obt_movie_engagement
│       └── projections.sql                 # ClickHouse Projections tăng tốc query báo cáo < 50ms
│
├── consumption/                            # [Rubric MLE, Feature Store & Web APIs]
│   ├── feature_repo/                       # Feast Feature Store Repository
│   │   ├── feature_store.yaml              # Cấu hình Offline (Iceberg) và Online (Redis)
│   │   ├── entities/                       # Thực thể nghiệp vụ
│   │   │   ├── user.py                     # Entity: user_id
│   │   │   └── movie.py                    # Entity: movie_id
│   │   ├── feature_views/                  # Các view đặc trưng
│   │   │   ├── user_ratings_fv.py          # user_avg_rating_90d, user_ratings_count_90d
│   │   │   ├── movie_popularity_fv.py      # movie_completion_rate_30d, movie_avg_rating_90d
│   │   │   └── realtime_skip_rate_fv.py    # user_skip_rate_15m (Streaming Push Source)
│   │   └── feature_services.py             # Gom nhóm features phục vụ model inference
│   │
│   └── api/                                # FastAPI Microservices (K8s Deployable)
│       ├── main.py                         # App entrypoint (Async FastAPI, CORS, OpenTelemetry)
│       ├── routes/
│       │   ├── data_fetch.py               # Web API kéo dữ liệu phân đoạn người dùng
│       │   └── drift_detection.py          # Web API giám sát Real-time Data Drift
│       └── schemas/
│           ├── request_models.py           # Pydantic v2 data validation
│           └── response_models.py
│
├── orchestration/                          # [Rubric 21-26] Điều phối quy trình trên Apache Airflow
│   ├── dags/
│   │   ├── dp1_raw_ingestion_dag.py        # Kéo dữ liệu nguồn vào Bronze + Data Validation
│   │   ├── dp2_transform_lakehouse_dag.py  # Spark Bronze -> Silver -> Gold (SCD2, De-skew)
│   │   └── dp3_feature_materialize_dag.py  # Tính bảng feat_* và đồng bộ sang Redis Online Store
│   ├── plugins/
│   │   └── datahub_lineage_plugin.py       # Tự động gửi phả hệ dữ liệu (lineage) lên DataHub
│   └── config/
│       └── airflow_connections.yaml
│
├── governance/                             # [Rubric 27-32] Data Governance & Data Quality
│   ├── datahub/
│   │   ├── metadata_ingestion.yaml         # Recipe nạp schema và glossary lên DataHub
│   │   └── lineage_emitters.py
│   └── validations/                        # Great Expectations / Soda Checks
│       ├── bronze_assertions.json          # Kiểm tra file không rỗng, schema cơ bản
│       ├── silver_cleansing_rules.json     # Kiểm tra rating_val từ 0.5 đến 5.0
│       └── gold_referential_checks.json    # Kiểm tra toàn vẹn khóa ngoại Fact - Dim
│
├── shared/                                 # Module tiện ích và chuẩn dùng chung
│   ├── contracts/                          # Data Contracts & Schemas dùng chung giữa các services
│   ├── observability/                      # Prometheus Metrics, Structured Logging, Tracer
│   └── exceptions.py                       # Custom Exceptions phân loại lỗi rõ ràng
│
├── docs/                                   # [BẮT BUỘC THEO RUBRIC] Tài liệu báo cáo & Proof Screenshots
│   ├── 01_data_generator_proof.md          # Thống kê phân phối, bảng chứng minh >=100GB
│   ├── 02_spark_optimizations.md           # Ảnh Spark UI chứng minh tối ưu Skew, OOM
│   ├── 03_flink_streaming_proof.md         # Ảnh Flink UI xử lý Late arrival & Watermarks
│   ├── 04_lakehouse_storage_proof.md       # Kết quả Compaction, Z-Ordering, SCD Type 2
│   ├── 05_airflow_pipelines.md             # Ảnh 3 DAGs xanh trên Airflow + DataHub Lineage
│   ├── codebase_structure.md               # Bản sao tài liệu kiến trúc source code
│   ├── publish_and_review_guide.md         # Hướng dẫn publish Git/Docker và cách review
│   └── images/                             # Toàn bộ ảnh chụp bằng chứng (proof screenshots)
│
├── infra/                                  # [Rubric IaC, K8s, Docker]
│   ├── docker/
│   │   ├── Dockerfile.generator            # Multi-stage build cho Data Generator
│   │   ├── Dockerfile.spark                # Image Spark 3.5 kèm Iceberg/MinIO dependencies
│   │   ├── Dockerfile.flink                # Image Flink 1.18 kèm Kafka & RocksDB connectors
│   │   └── Dockerfile.api                  # Multi-stage build cho FastAPI Web API (<150MB)
│   ├── compose/
│   │   ├── docker-compose.yml              # Local testing stack đầy đủ
│   │   └── .env.example
│   ├── k8s/                                # Helm Charts & Manifests cho K8s
│   │   ├── helm/
│   │   │   ├── api-service/
│   │   │   └── spark-jobs/
│   │   └── ingress/
│   │       └── nginx-ingress.yaml
│   └── terraform/                          # Terraform provisioning hạ tầng Cloud/MinIO
│
├── tests/                                  # [Rubric 27-31 MLE] Kiểm thử chuyên sâu
│   ├── unit/                               # Test Coverage > 90% (Pytest + Mocks)
│   ├── property_based/                     # Hypothesis testing kiểm tra tính Idempotent
│   ├── mutation/                           # Mutmut testing (Mutation score > 80%)
│   └── load_test/                          # Locust scripts kiểm tra tải Web API (HTML report)
│
├── pyproject.toml                          # Quản lý dependencies với uv
├── uv.lock                                 # Khóa phiên bản dependency chính xác
├── Makefile                                # Bộ lệnh tiện ích tự động hóa dev & evaluation
└── README.md                               # Cổng thông tin chính dẫn link tới các docs
```

---

## 2. Phân tích chi tiết chức năng từng Layer

### 2.1. Layer `generator/` (Data Simulation Engine)
- **Mục tiêu:** Giải quyết hoàn toàn bài toán tạo ra môi trường thử nghiệm dữ liệu lớn ($\ge 100\text{ GB}$) sát thực tế thay vì phụ thuộc vào việc tải dataset khổng lồ trên mạng.
- **Dữ liệu hạt giống (Seed Data):** Sử dụng các file gốc có sẵn trong `data_generator/data_offline/ml-latest-small/` (`movies.csv`, `ratings.csv`, `tags.csv`, `links.csv`) làm seed, sau đó áp dụng phân phối **Zipfian** và **Pareto** để nhân bản (scale-up) lên hàng trăm triệu bản ghi Parquet.
- **Output Contracts:**
  - `offline_generator.py` ghi trực tiếp các file Parquet phân vùng theo ngày vào bucket MinIO nguồn `s3://external-source-lake/`.
  - `streaming_producer.py` đẩy liên tục dữ liệu nhịp tim xem phim vào Kafka topic `movie.playback.heartbeat` với 6% sự kiện bị lùi thời gian (`event_ts` cũ từ 10–35 phút) và 1.8% sự kiện bị lặp lại.

### 2.2. Layer `transformation/` (Spark & Flink Jobs)
- **Tách rõ Baseline và Optimized:** 
  - Thư mục `baseline/` chứa mã nguồn xử lý tự nhiên chưa can thiệp tối ưu. Khi chạy trên Spark UI hoặc Flink UI, reviewer sẽ nhìn thấy rõ các hiện tượng nghẽn cổ chai:
    - Spark Join giữa `watch_history` và `movies` bị 1 task chạy lâu bất thường (Straggler task do Skew).
    - Flink Tumbling Window bị mất sạch 6% sự kiện trễ.
  - Thư mục `optimized/` chứa các kỹ thuật giải quyết:
    - `handle_skew_salting.py`: Thêm khóa muối `salt = rand(0, 15)` vào bảng lớn, nhân bản bảng nhỏ và dùng Broadcast Join.
    - `handle_schema_merge.py`: Bật `.option("mergeSchema", "true")` đọc an toàn các cột mới `content_rating`.
    - `watermark_assigner.py`: Cấu hình `BoundedOutOfOrderness(Duration.ofMinutes(15))` và `allowedLateness(Time.minutes(30))`.

### 2.3. Layer `storage/` (Lakehouse & Warehouse)
- **Medallion Architecture:**
  - **Bronze (`raw_*`):** Lưu trữ dạng Append-Only giữ nguyên dữ liệu gốc kèm metadata (`_ingest_ts`).
  - **Silver (`stg_*`):** Đã khử duplicate, đã hợp nhất schema, lọc các giá trị không hợp lệ.
  - **Gold (`dim_*`, `fact_*`, `feat_*`, `obt_*`):**
    - `dim_users`: Quản lý biến động gói cước bằng **SCD Type 2** (`valid_from_ts`, `valid_to_ts`, `is_current`).
    - `feat_*`: Tuân thủ chuẩn Feast với 2 cột bắt buộc `event_timestamp` và `created_timestamp`.
    - `obt_movie_engagement`: Bảng một bảng phẳng (One Big Table) đồng bộ sang ClickHouse phục vụ dashboard với độ trễ mili-giây.

### 2.4. Layer `consumption/` (Feast & FastAPI)
- **Feast Feature Store:**
  - `user_ratings_fv`: Trích xuất đặc trưng lịch sử 90 ngày của người dùng từ Lakehouse Gold Zone.
  - `realtime_skip_rate_fv`: Nhận dữ liệu streaming trực tiếp từ Flink qua Push Source để cập nhật tức thời vào Redis.
- **FastAPI Web API:**
  - Viết hoàn toàn bằng Python `async/await`.
  - Pydantic v2 data validation đảm bảo schema contract an toàn.
  - Được đóng gói Docker nhỏ gọn, sẵn sàng deploy lên K8s bằng Helm Chart kèm HPA (Autoscaling).

### 2.5. Layer `orchestration/` & `governance/` (Airflow & DataHub)
- **3 DAGs độc lập trên Airflow:**
  - **DP1:** Nạp dữ liệu thô từ MinIO nguồn ngoài vào Bronze Zone $\rightarrow$ Chạy Great Expectations validation.
  - **DP2:** Kích hoạt Spark Job chuyển đổi Bronze $\rightarrow$ Silver $\rightarrow$ Gold $\rightarrow$ Cập nhật SCD Type 2.
  - **DP3:** Tính toán các bảng đặc trưng $\rightarrow$ Gọi `feast materialize` đồng bộ dữ liệu vào Redis.
- **DataHub Lineage:** Tích hợp `datahub_lineage_plugin.py` giúp DataHub tự động vẽ sơ đồ quan hệ giữa các bảng và 3 pipeline Airflow.
