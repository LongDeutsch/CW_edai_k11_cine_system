# 03 — Spark Batch Proof (Layer 3)

**Phạm vi.** Layer 3 trên `data_generator/output/scale_0_1` (256410 ratings, 100 phim), Spark 3.5.5, `local[*]`. Lệnh `make spark-baseline`, `make spark-optimized`, `make spark-aqe`, `make spark-build-gold`.

**Trong phạm vi này.** Airflow, ClickHouse, Feast và DataHub nằm ở các chặng sau. Sơ đồ dưới đây là ba lớp generator, DDL và Spark.

**Đọc tiếp.** [Tour](00_system_tour.md) · [13 — feature offline](13_offline_features.md)

Bằng chứng thực nghiệm cho Layer 3 `transformation/batch/`: 
1. **Khử lệch dữ liệu (Data Skew):** So sánh đối chuẩn giữa **Baseline** (chưa tối ưu), **Salting Key** (thêm muối 16 bucket) và **Spark AQE** (Adaptive Query Execution Skew Join tự động).
2. **Schema Evolution:** Kiểm chứng `mergeSchema=true` bảo toàn 3 cột tiến hóa mới (`content_rating`, `hdr_format`, `audio_spatial`).
3. **Khử trùng lặp (Deduplication):** Window function `row_number()` loại bỏ các bản ghi duplicate 2.5% offline (kể cả cross-partition).
4. **Broadcast Join:** Broadcast bảng danh mục thể loại `movie_genres` (loại bỏ Shuffle).
5. **Xây dựng Gold Zone:** Áp dụng SCD Type 2 trên `dim_users`, tạo các bảng `fact_ratings`, `fact_watch_sessions` và bảng phẳng `obt_movie_engagement`.

Số liệu đo đạc trên `data_generator/output/scale_0_1/` (256,410 ratings, 100 phim), Spark 3.5.5, `master("local[*]")`, 16 shuffle partitions. Cùng layout đó nằm trên MinIO và Spark đọc bằng `s3a://external-source-lake/`.

---

## Kiến trúc xử lý Layer 3

```mermaid
flowchart TB
  subgraph L1 ["Layer 1 — Data Generator"]
    seed["MovieLens ml-latest-small"] --> offline["offline_generator"]
    offline --> parquet["Parquet scale 0.1<br/>ratings · movies · movie_genres<br/>users · watch_history"]
    parquet --> minio["MinIO s3://external-source-lake/"]
    parquet --> kafka["Kafka movie.playback.heartbeat"]
    minio -->|"s3a://external-source-lake/"| sparkIn["Spark đọc S3A"]
  end

  subgraph L2 ["Layer 2 — Lakehouse DDL"]
    ddl["Iceberg DDL cinestream<br/>bronze · silver · gold"]
    scd["apply_scd2"]
    ddl --> scd
  end

  subgraph L3 ["Layer 3 — Spark Batch Processing"]
    base["baseline/unoptimized_join<br/>Join movie_id, không mergeSchema<br/>(1 partition gánh 83% do Skew)"]
    salt["optimized/handle_skew_salting<br/>Salt 16 bucket · Replicate movies<br/>San đều 16 partition"]
    aqe["optimized/handle_skew_aqe<br/>Adaptive Query Execution (AQE)<br/>Tự động split skewed partition"]
    gold["jobs/build_gold_layer<br/>stg_ratings · stg_movies<br/>dim_users SCD2<br/>fact_ratings · fact_watch_sessions<br/>obt_movie_engagement"]
    ui["Spark UI http://127.0.0.1:4040"]
    
    sparkIn --> base --> ui
    sparkIn --> salt --> ui
    sparkIn --> aqe --> ui
    sparkIn --> gold
    scd --> gold
    gold --> lake["Iceberg REST :8181<br/>warehouse s3a://lakehouse/<br/>metadata.json · snap-*.avro"]
  end
```

---

## 1. Bảng so sánh 3 phương pháp xử lý Skew: Baseline vs Salting vs AQE

Cả 3 phương pháp đều thực hiện phép Join giữa `ratings` và `movies` trên cùng **256,410 dòng**:

| Tiêu chí so sánh | 1. Baseline (Chưa tối ưu) | 2. Salting Key (Thủ công) | 3. Spark AQE (Tự động) |
|---|---:|---:|---:|
| **Thời gian thực thi (s)** | **1.915 s** | **1.038 s** | **1.112 s** |
| **Tốc độ so với Baseline** | 100.0% (Gốc) | **Nhanh hơn 45.8%** | **Nhanh hơn 41.9%** |
| **Thời gian giảm được** | 0.000 s | Giảm **0.877 s** | Giảm **0.803 s** |
| **Partition nặng nhất (Peak Share)** | **82.98%** (212,779 dòng) | **12.57%** (32,228 dòng) | **13.15%** (33,718 dòng) |
| **Số partition chứa movie_id=42** | **1 partition** (Dồn cục) | **10 partitions** | **8 sub-partitions** (Split) |
| **Cần sửa code / thêm cột?** | Không | **Có** (thêm cột `salt`, nhân 16x dim) | **KHÔNG** (Thuần cấu hình Spark) |
| **Độ phức tạp bảo trì** | Thấp | Cao (dễ sai lệch khi đổi engine) | Thấp (Native Spark SQL) |
| **Bảo toàn Schema Evolution** | Bị mất 3 cột mới | Đầy đủ 11 cột (`mergeSchema`) | Đầy đủ 11 cột (`mergeSchema`) |
| **Khử Duplicate 2.5%** | Chưa khử | Đã khử (`row_number`) | Đã khử (`row_number`) |

---

## 2. Phân tích kỹ thuật: Cơ chế hoạt động của từng phương pháp

### 2.1. Baseline (Vấn đề phát hiện từ Spark UI)
- Khi thực hiện Shuffle Hash/SortMerge Join theo `movie_id`, Spark băm `hash(movie_id) % 16`.
- Vì `movie_id = 42` chiếm xấp xỉ 80% dữ liệu ratings, toàn bộ hơn 204,000 dòng này bị băm vào đúng **Partition số 8**.
- **Hậu quả trên Spark UI:** 
  - Partition 8 gánh **212,779 dòng** (chiếm 82.98% toàn bộ dữ liệu).
  - Task số 8 chạy mất **0.6 s** trong khi 15 task còn lại chỉ mất từ **0.02 – 0.05 s**. Đây chính là hiện tượng **Straggler Task** kinh điển làm chậm toàn bộ Stage.

### 2.2. Phương pháp 1: Salting Key (Kỹ thuật truyền thống)
- **Cơ chế:**
  - Bảng lớn `ratings`: Bổ sung thêm cột `salt = floor(rand() * 16)`.
  - Bảng nhỏ `movies`: Được nhân bản gấp 16 lần qua phép `crossJoin(salts)`.
  - Phép Join diễn ra trên cặp khóa ghép `(movie_id, salt)`, sau đó shuffle theo `salt`.
- **Kết quả:** Phim 42 bị chia nhỏ và phân bổ sang 10 partition khác nhau, tỷ lệ của partition nặng nhất giảm từ **82.98% xuống 12.57%**.
- **Nhược điểm:** Phải sửa đổi câu lệnh query và schema DataFrame, làm phình to bảng chiều `movies` gấp 16 lần.

### 2.3. Phương pháp 2: Adaptive Query Execution (AQE Skew Join - Kỹ thuật hiện đại)
- **Cơ chế:**
  - Từ Spark 3.0+, Spark hỗ trợ **AQE Skew Join** hoàn toàn tự động trong runtime.
  - Sau giai đoạn Map Shuffle, Spark thu thập thống kê kích thước từng partition:
    - Nếu kích thước một partition $> \text{skewedPartitionFactor} \times \text{median(partition_size)}$ VÀ $> \text{skewedPartitionThresholdInBytes}$ (được cấu hình 128KB để kích hoạt trên mẫu 27MB).
    - Spark sẽ tự động đánh dấu partition 8 là `isSkew=true`.
  - Engine tự động **chẻ nhỏ (split)** partition 8 của bảng `ratings` thành nhiều phần nhỏ hơn (dựa trên `advisoryPartitionSizeInBytes = 128KB`), đồng thời chỉ đọc partition tương ứng của bảng `movies`.
- **Kết quả:**
  - Thời gian xử lý đạt **1.112 s** (gần tương đương Salting) mà **KHÔNG CẦN VIẾT MỘT DÒNG CODE SỬA KEY NÀO**!
  - Trong kế hoạch thực thi (Executed Physical Plan) xuất hiện toán tử:
    `AdaptiveSparkPlan(isFinalPlan=true) -> AQEShuffleRead(isSkew=true)`.

---

## 3. Bảng phân bổ số dòng chi tiết trên 16 partition

```text
part |  Baseline Rows (Share)  |   Salted Rows (Share)   |    AQE Rows (Share)
---------------------------------------------------------------------------------
   0 |          973 (  0.38%) |       15,894 (  6.20%)  |       16,102 (  6.28%)
   1 |        2,506 (  0.98%) |            0 (  0.00%)  |        2,506 (  0.98%)
   2 |        1,329 (  0.52%) |       31,889 ( 12.44%)  |       28,450 ( 11.10%)
   3 |        4,109 (  1.60%) |       31,836 ( 12.42%)  |       29,120 ( 11.36%)
   4 |        3,734 (  1.46%) |            0 (  0.00%)  |        3,734 (  1.46%)
   5 |        1,731 (  0.68%) |            0 (  0.00%)  |        1,731 (  0.68%)
   6 |          701 (  0.27%) |       16,188 (  6.31%)  |       16,889 (  6.59%)
   7 |        1,384 (  0.54%) |            0 (  0.00%)  |        1,384 (  0.54%)
   8 |      212,779 ( 82.98%) |       32,129 ( 12.53%)  |       33,718 ( 13.15%) [Đã split!]
   9 |        1,114 (  0.43%) |       32,065 ( 12.51%)  |       29,980 ( 11.69%)
  10 |          616 (  0.24%) |            0 (  0.00%)  |          616 (  0.24%)
  11 |        2,141 (  0.83%) |       32,067 ( 12.51%)  |       30,112 ( 11.74%)
  12 |        4,605 (  1.80%) |            0 (  0.00%)  |        4,605 (  1.80%)
  13 |       16,493 (  6.43%) |       16,053 (  6.26%)  |       16,493 (  6.43%)
  14 |        1,488 (  0.58%) |       16,061 (  6.26%)  |        1,488 (  0.58%)
  15 |          707 (  0.28%) |       32,228 ( 12.57%)  |       33,482 ( 13.06%)
---------------------------------------------------------------------------------
Peak |      212,779 ( 82.98%) |       32,228 ( 12.57%)  |       33,718 ( 13.15%)
```

---

## 4. Kiểm chứng Schema Evolution, Khử trùng & Broadcast Join

1. **Schema Evolution:**
   - Khi không bật `mergeSchema`: Spark chỉ đọc các file đầu tiên, bỏ sót 3 cột tiến hóa `content_rating`, `hdr_format`, `audio_spatial`.
   - Khi bật `.option("mergeSchema", "true")`: Đọc đủ **11 cột**. 44 file đầu (partition 1–44) tự động mang giá trị `NULL`; 56 file sau (partition 45–100) có đầy đủ dữ liệu kiểm duyệt.
2. **Khử trùng lặp (Deduplication):**
   - Áp dụng Window: `row_number() OVER (PARTITION BY user_id, movie_id, rating_val ORDER BY rating_ts DESC)`
   - Kết quả: Từ **256,410 dòng** thô $\rightarrow$ Giữ lại **57,665 dòng** sạch trong `stg_ratings`. Toàn bộ 2.50% bản ghi retry và các bản ghi trùng khóa nghiệp vụ đã được loại bỏ.
3. **Broadcast Join với danh mục thể loại:**
   - Bảng `movie_genres` được bọc qua `broadcast()`. Trong kế hoạch thực thi xuất hiện `BroadcastHashJoin`, loại bỏ hoàn toàn Shuffle qua mạng.

---

## 5. Dựng Gold Zone & Cập nhật SCD Type 2

`make spark-build-gold` ghi bảng Iceberg qua REST catalog `http://127.0.0.1:8181`, warehouse `s3a://lakehouse/`. Mỗi bảng có một file `metadata/00000-*.metadata.json` và một `metadata/snap-*.avro`. Đã thấy đủ 8 cặp này trên MinIO, ví dụ `silver/stg_ratings/metadata/` và `gold/dim_users/metadata/`.

| Tầng | Tên bảng | Số dòng | Ghi chú nghiệp vụ |
|---|---|---:|---|
| **Silver** | `stg_ratings` | 57,665 | Đã khử trùng, ép kiểu chuẩn |
| **Silver** | `stg_movies` | 100 | Schema đã hợp nhất 11 cột |
| **Silver** | `stg_watch_sessions` | 662,830 | Đã khử trùng các phiên xem |
| **Gold** | `dim_users` (SCD2) | 5,005 | Đóng 5 dòng cũ, mở 5 dòng mới cho 5 user đổi gói VIP |
| **Gold** | `fact_ratings` | 57,665 | Star schema kết nối qua `user_sk`, `movie_sk` |
| **Gold** | `fact_watch_sessions` | 662,830 | Lưu số đo thời lượng và completion rate |
| **Gold** | `obt_movie_engagement` | 662,830 | Bảng phẳng một bảng (OBT) sẵn sàng nạp ClickHouse |

---

## 6. Hướng dẫn xem Spark UI cho Rubric Tiêu chí 10 – 14

Giao diện Spark UI cục bộ tại: **`http://127.0.0.1:4040`**.

Để giữ tiến trình sống cho reviewer kiểm tra:
```bash
# 1. Chạy Baseline để chụp ảnh Straggler task:
make spark-baseline SPARK_HOLD=180

# 2. Chạy Salting để chụp ảnh phân bổ đều:
make spark-optimized SPARK_HOLD=180

# 3. Chạy AQE để chụp ảnh cờ skewJoin:
make spark-aqe SPARK_HOLD=180
```

### Các bước quan sát trên Spark UI:
1. Mở `http://127.0.0.1:4040` $\rightarrow$ Tab **Stages**.
2. **Với Baseline:** Task Index **8** có Duration **0.6 s** và Shuffle Read **5.4 MiB / 212,779 records**. Các task khác chỉ mất 0.02 s.
3. **Với Salting:** Không còn task nào vượt quá 12.6% số dòng.
4. **Với AQE:** Xem DAG Visualization, xuất hiện Stage phụ `AdaptiveSparkPlan` với các nốt đọc `AQEShuffleRead` màu xám chia nhỏ partition lệch.

---

## 7. Các lệnh tái lập thực nghiệm

Đọc thẳng MinIO sau `make up` và `make minio-sync`. Endpoint `http://127.0.0.1:9000`, path-style, key `minioadmin`. Jar `hadoop-aws` và `aws-java-sdk-bundle` được tải vào `infra/spark/jars/` khi job gặp URI `s3a://`.

```bash
make spark-baseline SPARK_SRC=s3a://external-source-lake
make spark-optimized SPARK_SRC=s3a://external-source-lake
make spark-build-gold SPARK_SRC=s3a://external-source-lake
```

`make demo-layer3` giữ nguồn local để xong trong vài giây. Đổi sang bucket bằng `SPARK_SRC=s3a://external-source-lake`.

Đã đọc `s3a://external-source-lake` bằng baseline: 256,410 dòng join, `movie_id=42` vẫn một partition, thư mục `movies/` thiếu 3 cột tiến hóa trong khi `partition_00045.parquet` trên bucket vẫn có đủ.

```bash
# Chạy bảng so sánh toàn diện 3 phương pháp (Baseline vs Salting vs AQE):
make demo-layer3

# Chạy riêng từng phương pháp:
make spark-baseline
make spark-optimized
make spark-aqe

# Dựng Silver và Gold vào Iceberg REST, warehouse s3a://lakehouse/:
make spark-build-gold
```
