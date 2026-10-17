# Movie Recommendation - Data Platform Architecture & Design

## 1. Domain Overview

Nền tảng **CineStream** là một dịch vụ phát trực tuyến và đề xuất phim điện ảnh quy mô lớn (mô phỏng kiến trúc kết hợp giữa Netflix và tập dữ liệu MovieLens 32M+ mở rộng). Người dùng có thể đăng ký gói xem phim (Free, Standard, VIP/4K), duyệt phim theo thể loại, tìm kiếm phim, chấm điểm (rating từ 0.5 đến 5.0), viết nhận xét ngắn (tags), thêm phim vào danh sách xem sau (watchlist), và xem phim trực tuyến trên đa nền tảng (Web, SmartTV, Mobile App).

Dữ liệu được thu thập từ hai nguồn chính:
1. **Hệ thống dữ liệu giao dịch & danh mục (OLTP Database / Object Storage Batch):** Quản lý hồ sơ người dùng, thông tin bản quyền phim, danh mục thể loại, đạo diễn, diễn viên, các lượt đánh giá và gắn thẻ có cấu trúc.
2. **Hệ thống luồng sự kiện thời gian thực (Real-time Event Streaming - Apache Kafka):** Nhật ký tương tác giao diện người dùng (clickstream, impressions trên trang chủ) và luồng nhịp tim phát video (video playback heartbeats được gửi định kỳ 10 giây/lần khi đang xem).

---

## 2. Offline Data Specification

Dữ liệu offline là dữ liệu lịch sử và dữ liệu tham chiếu danh mục (master data), được trích xuất hàng loạt (batch export) định dạng Parquet nén Snappy và lưu trữ tại tầng lưu trữ đối tượng (MinIO/S3).

| Table | Grain (Một dòng đại diện cho) | Key Columns | Data Volume Dự kiến |
|---|---|---|---|
| `users` | 1 người dùng đăng ký | `user_id`, `signup_ts`, `country`, `subscription_tier`, `preferred_language`, `age_group`, `gender` | ~5 triệu bản ghi (~500MB) |
| `movies` | 1 bộ phim / tựa phim | `movie_id`, `title`, `release_year`, `runtime_minutes`, `original_language`, `budget`, `director_id`, `created_ts` | ~100,000 bản ghi (~50MB) |
| `movie_genres` | 1 thể loại của 1 bộ phim | `movie_id`, `genre_name` (Action, Sci-Fi, Romance, etc.) | ~300,000 bản ghi (~20MB) |
| `ratings` | 1 lượt chấm điểm của người dùng cho 1 phim | `rating_id`, `user_id`, `movie_id`, `rating_val` (0.5 - 5.0), `rating_ts`, `review_text` | ~250 triệu bản ghi (~25GB) |
| `tags` | 1 lượt gắn thẻ từ khoá của user cho phim | `tag_id`, `user_id`, `movie_id`, `tag_name`, `tag_ts` | ~30 triệu bản ghi (~3GB) |
| `watch_history` | 1 phiên xem phim của người dùng | `session_id`, `user_id`, `movie_id`, `start_ts`, `end_ts`, `watch_duration_seconds`, `completion_rate`, `device_type`, `resolution` | ~800 triệu bản ghi (~75GB) |

> **Tổng thể tích dữ liệu offline (Target Volume):** $\ge 100 \text{ GB}$ (được phân mảnh thành hàng nghìn file Parquet kích thước 64MB - 128MB để phục vụ kiểm thử benchmark phân tán trên Apache Spark).

---

## 3. Streaming Data Specification

Dữ liệu streaming được sản sinh liên tục từ ứng dụng client của người dùng, được đẩy vào Apache Kafka topic với định dạng JSON hoặc Protobuf.

| Event / Topic | Grain | Key Columns | Throughput Dự kiến |
|---|---|---|---|
| `movie.playback.heartbeat` | 1 nhịp tim báo cáo tiến độ xem phim (phát mỗi 10 giây khi user đang xem) | `heartbeat_id`, `session_id`, `user_id`, `movie_id`, `event_ts`, `current_position_seconds`, `playback_speed`, `buffer_count`, `bitrate_kbps`, `device_id` | ~5,000 - 15,000 events/giây |
| `movie.user.clickstream` | 1 tương tác UI của user (click card phim, search, add watchlist, hover) | `event_id`, `user_id`, `session_id`, `event_type` (view_detail, add_watchlist, search_query, click_banner), `target_movie_id`, `page_location`, `event_ts`, `client_ip` | ~2,000 - 8,000 events/giây |

---

## 4. Required Data Problems & Domain Mapping

Theo yêu cầu chuẩn của EDAI Rubric, hệ thống được cấu hình chủ đích các sự cố dữ liệu thực tế nhằm kiểm thử năng lực xử lý của các Data Processing Jobs (Spark & Flink):

### 4.1. Data Skew (Độ lệch phân phối dữ liệu)
- **Movie Popularity Skew (Quy luật Zipf / Pareto 80/20):**
  - Trong bảng `ratings` và `watch_history`, khoảng **1% số bộ phim bom tấn** (Blockbusters ví dụ: *Avatar*, *The Avengers*, *Interstellar*) chiếm tới **80% tổng số lượt xem và lượt đánh giá**.
  - **Genre Skew:** 70% lượt xem tập trung vào 2 thể loại chính là *Action* và *Comedy*.
  - **User Activity Skew:** 5% "super-users" (cinephile power-raters) đóng góp hơn 40% tổng số rating và tag.
  - **Hệ quả trên Spark:** Khi thực hiện phép Join hoặc Aggregation theo `movie_id` hoặc `user_id`, một số partition sẽ chứa lượng dữ liệu khổng lồ (vài GB), gây hiện tượng nghẽn cổ chai (Straggler Tasks), tràn bộ nhớ (OOM Spill to Disk), trong khi các task khác hoàn thành trong vài giây.

### 4.2. Schema Evolution (Tiến hóa cấu trúc dữ liệu)
- **Nghiệp vụ thay đổi:** Giữa tập dữ liệu lịch sử (từ ngày thứ 90 trở đi), hệ thống bổ sung thêm 3 trường thông tin mới để phục vụ chính sách bản quyền và chuẩn hiển thị mới:
  - `content_rating`: Chuẩn phân loại độ tuổi (`G`, `PG-13`, `R`, `NC-17`) - kiểu dữ liệu `STRING` (nullable).
  - `hdr_format`: Định dạng hình ảnh cao cấp (`HDR10`, `Dolby_Vision`, `SDR`) - kiểu dữ liệu `STRING` (nullable).
  - `audio_spatial`: Định dạng âm thanh vòm (`Dolby_Atmos`, `Stereo`) - kiểu dữ liệu `BOOLEAN` (nullable).
- **Hệ quả:** Các file Parquet sinh ra trước thời điểm này không chứa 3 cột trên. Spark Reader nếu không cấu hình đọc tự động hợp nhất lược đồ (`mergeSchema`) sẽ gặp lỗi đọc sai vị trí hoặc văng ngoại lệ `AnalysisException`.

### 4.3. Offline Duplicates (Dữ liệu trùng lặp ở tầng Batch)
- **Nguyên nhân kỹ thuật:** Do lỗi cơ chế retry tự động ở client mobile khi rớt mạng ngắt quãng lúc gửi rating hoặc kết thúc phim, dẫn tới khoảng **2% - 3%** bản ghi trong bảng `ratings` và `watch_history` bị lặp lại.
- **Đặc điểm:** Các bản ghi trùng lặp có cùng bộ khóa nghiệp vụ (`user_id`, `movie_id`, `rating_val`), nhưng khác biệt đôi chút về `rating_id` và `created_ts` (chênh lệch vài giây).
- **Hệ quả:** Nếu tính toán trực tiếp mà không khử trùng (deduplication), các chỉ số doanh thu xem phim, điểm trung bình phim và tổng số giờ xem sẽ bị thổi phồng sai lệch.

### 4.4. Streaming Late Arrivals (Dữ liệu sự kiện đến muộn)
- **Nguyên nhân kỹ thuật:** Người dùng sử dụng tính năng tải phim về máy và xem offline trên máy bay, tàu điện ngầm hoặc vùng sóng yếu. Khi thiết bị kết nối lại Wi-Fi, hàng loạt `heartbeat` và `playback_event` được đồng bộ ngược về Kafka trễ từ **10 phút đến 45 phút** so với thời gian phát sinh thực tế (`event_ts`).
- **Hệ quả trên Flink:** Các phép toán tính toán theo cửa sổ thời gian (Tumbling / Sliding Event-time Windows 5 phút) để đếm số người đang xem đồng thời (concurrent viewers) sẽ bị bỏ sót (drop) các sự kiện muộn này nếu không có cơ chế `Watermark` và `Allowed Lateness` phù hợp.

### 4.5. Streaming Duplicates (Dữ liệu trùng lặp thời gian thực)
- **Nguyên nhân kỹ thuật:** Cơ chế đảm bảo phân phát tin cậy At-Least-Once Delivery từ Kafka Producer và mạng chập chờn khiến khoảng **1.5% - 2%** sự kiện `movie.playback.heartbeat` bị gửi lặp lại 2 lần với cùng `heartbeat_id`.
- **Hệ quả:** Tính toán trùng số phút xem thực tế trong phiên của người dùng nếu Flink sink không áp dụng xử lý idempotent hoặc state deduplication.

### 4.6. Vấn đề bổ sung (Additional Problems)
- **High-Cardinality Join:** Join bảng sự kiện nhịp tim phát phim `watch_history` (hàng trăm triệu dòng) với bảng thông tin phim `movies` và bảng thể loại chi tiết.
- **Out-of-Order Events:** Các sự kiện `heartbeat` từ cùng một phiên xem phim gửi qua mạng 4G/5G đến Kafka broker bị xáo trộn thứ tự thời gian (event_ts $t_2$ đến trước $t_1$).

---

## 5. Storage Architecture & Medallion Design

Hệ thống lưu trữ áp dụng kiến trúc **Lakehouse (Apache Iceberg / Delta Lake)** kết hợp **Data Warehouse (ClickHouse / PostgreSQL)**, tuân thủ nghiêm ngặt quy tắc đặt tên và phân tầng chuẩn doanh nghiệp:

```
[Kafka Streams] --------------> [Bronze Zone: raw_]
                                       │
                                       ▼ (Spark Batch / Flink Streaming Cleaning)
[MinIO Source Batch] ---------> [Silver Zone: stg_]
                                       │
                                       ▼ (Dimension Modeling & Aggregations)
                                [Gold Zone: dim_, fact_, obt_, feat_]
                                       │
                                       ▼ (Feature Serving & Analytics)
                           [Feast Store / ClickHouse DW]
```

### 5.1. Naming Convention & Cấu trúc các bảng

#### Bronze Zone (Raw Ingestion Layer)
Lưu trữ dữ liệu thô nguyên bản dưới dạng Append-only, lưu giữ toàn bộ lịch sử lỗi và metadata nạp (`_ingest_ts`, `_source_file`):
- `raw_users`
- `raw_movies`
- `raw_ratings`
- `raw_tags`
- `raw_watch_history`
- `raw_playback_heartbeat`
- `raw_clickstream`

#### Silver Zone (Cleansed & Conformed Layer)
Dữ liệu đã được chuẩn hóa kiểu dữ liệu, khử trùng lặp (deduplicated), giải quyết schema evolution và kiểm tra tính toàn vẹn (data validation):
- `stg_users`
- `stg_movies`
- `stg_ratings`
- `stg_watch_sessions`
- `stg_playback_events`

#### Gold Zone (Business, Dimensional & Feature Layer)
Thiết kế theo mô hình chiều kim tự tháp (Kimball Dimensional Modeling):

1. **Bảng chiều có áp dụng SCD Type 2 (Slowly Changing Dimensions):**
   - `dim_users`: Quản lý lịch sử thay đổi gói thuê bao người dùng (`subscription_tier` từ Free -> VIP -> Premium).
     - Các cột kỹ thuật bắt buộc: `user_sk` (Surrogate Key), `user_id`, `country`, `subscription_tier`, `valid_from_ts`, `valid_to_ts`, `is_current` (BOOLEAN).
   - `dim_movies`: Lưu trữ thông tin phim, hỗ trợ cập nhật danh mục và giấy phép bản quyền.
     - Cột: `movie_sk`, `movie_id`, `title`, `release_year`, `content_rating`, `hdr_format`, `valid_from_ts`, `valid_to_ts`, `is_current`.

2. **Bảng sự thật (Fact Tables):**
   - `fact_ratings`: Một dòng cho mỗi lượt đánh giá hợp lệ (`rating_sk`, `user_sk`, `movie_sk`, `rating_val`, `rating_date_key`, `created_ts`).
   - `fact_watch_sessions`: Một dòng cho mỗi phiên xem hoàn thành (`session_sk`, `user_sk`, `movie_sk`, `watch_duration_seconds`, `completion_rate`, `device_type`, `session_date_key`).

3. **Bảng hợp nhất phẳng (One Big Table - OBT):**
   - `obt_movie_engagement`: Phục vụ truy vấn phân tích tức thời (BI Dashboard) không cần join nhiều bảng: Denormalized từ `fact_watch_sessions` + `dim_movies` + `dim_users`.

4. **Bảng đặc trưng (Feature Tables) phục vụ Feature Store (Feast):**
   Tuân thủ nghiêm ngặt chuẩn định dạng Feast: bắt buộc chứa `entity_id`, `event_timestamp`, và `created_timestamp`:
   - `feat_user_movie_stats_daily`:
     - Keys: `user_id`, `event_timestamp`, `created_timestamp`
     - Features: `user_total_ratings_90d`, `user_avg_rating_90d`, `user_favorite_genre`, `user_total_watch_hours_30d`.
   - `feat_movie_engagement_stats_daily`:
     - Keys: `movie_id`, `event_timestamp`, `created_timestamp`
     - Features: `movie_rating_count_30d`, `movie_avg_rating_30d`, `movie_completion_rate_30d`, `movie_trending_score_7d`.

### 5.2. Storage Optimizations (Lakehouse & Data Warehouse)

#### Tối ưu hóa trên Lakehouse (Iceberg / Delta Lake)
1. **File Compaction (Bin-packing):** Tự động gom hàng ngàn file nhỏ (vài MB do streaming micro-batch tạo ra) thành các file kích thước chuẩn 128MB - 256MB để hạn chế chi phí tra cứu metadata của NameNode/Object Storage.
2. **Partitioning Strategy:** Phân vùng bảng `watch_history` và `ratings` theo `event_date` (yyyy-MM-dd) giúp tối ưu hóa Partition Pruning khi query chỉ quét theo khoảng thời gian.
3. **Z-Ordering / Multi-dimensional Clustering:** Áp dụng Z-Order trên hai cột truy vấn lọc nhiều nhất: `(movie_id, user_id)`. Kỹ thuật này giúp phân bổ dữ liệu lân cận trong không gian đa chiều vào cùng các data blocks, cho phép Parquet DataSkipping nhảy cóc hàng triệu dòng không liên quan.

#### Tối ưu hóa trên Data Warehouse (ClickHouse)
1. **Primary Index & Sorting Key:** Chọn `ORDER BY (movie_id, event_date, user_id)` trong ClickHouse `ReplacingMergeTree` engine.
2. **Projections:** Tạo Projection tính toán trước ma trận tổng hợp số lượt xem theo `(genre, release_year)` để trả lời các truy vấn phân tích báo cáo trong thời gian dưới 50ms.

---

## 6. Processing Jobs Design

### 6.1. Spark Processing Job (Xử lý Offline Data Problems)

```
[Raw Parquet Files in MinIO]
              │
              ▼
   [Spark Ingestion Job]
              │
              ├── 1. Read with .option("mergeSchema", "true")  ---> Giải quyết Schema Evolution
              ├── 2. Window Row_Number() over Partition Key    ---> Khử Duplicates (2-3%)
              ├── 3. Salting Key (movie_id + random_int(0,15)) ---> Khử Data Skew (Blockbuster 80/20)
              ├── 4. Broadcast Hash Join for dim_movies        ---> Tránh Shuffle Skew
              └── 5. Write to Iceberg Silver/Gold Tables       ---> Partition & Z-Order
```

1. **Baseline Job:** Chạy phép biến đổi cơ bản bằng Spark SQL tiêu chuẩn không cấu hình tối ưu. Khi thực hiện join `watch_history` với `ratings` theo `movie_id`, Spark UI sẽ hiển thị một task chạy kéo dài bất thường do 1% phim bom tấn bị dồn hết về 1 Reducer partition.
2. **Handle Skew Optimization:**
   - **Salting Technique:** Thêm một tiền tố ngẫu nhiên `salt = rand(0, 15)` vào `movie_id` của bảng lớn `watch_history` (`salted_movie_id = movie_id_salt`), đồng thời nhân bản bảng nhỏ `movies` gấp 16 lần. Phép join được thực hiện đều trên 16 partition thay vì dồn về 1.
   - **Adaptive Query Execution (AQE):** Bật `spark.sql.adaptive.enabled = true`, `spark.sql.adaptive.skewJoin.enabled = true`. Spark sẽ tự động chia nhỏ các skewed partition khi phát hiện kích thước partition vượt ngưỡng `skewedPartitionFactor`.
   - **Broadcast Join:** Với các bảng danh mục như `dim_genres`, `dim_categories`, ép dùng `broadcast()` để loại bỏ hoàn toàn giai đoạn Shuffle.
3. **Handle Schema Evolution:**
   - Cấu hình Spark Reader: `.option("mergeSchema", "true")`.
   - Spark sẽ tự động quét footer của các file Parquet thế hệ mới và cũ, hợp nhất lược đồ một cách an toàn và điền `NULL` vào các dòng dữ liệu thuộc thế hệ cũ.
4. **Handle Duplicates:**
   - Áp dụng kỹ thuật Window function `row_number() OVER (PARTITION BY user_id, movie_id, rating_val ORDER BY rating_ts DESC)` và lọc lấy `rn = 1`, giữ lại bản ghi mới nhất và loại bỏ 2% bản ghi rác.

### 6.2. Flink Processing Job (Xử lý Streaming Data Problems)

```
[Kafka Topic: movie.playback.heartbeat]
                     │
                     ▼
          [Apache Flink Stream Job]
                     │
                     ├── 1. Keyed State with TTL (RocksDB)       ---> Khử Duplicates (1.5%)
                     ├── 2. BoundedOutOfOrderness Watermark      ---> Chấp nhận Delay 10-15m
                     ├── 3. Sliding / Tumbling Event Windows     ---> Tính toán Real-time Metrics
                     ├── 4. SideOutputLateData                   ---> Hứng dữ liệu siêu trễ (>15m)
                     │
        ┌────────────┴────────────┐
        ▼                         ▼
 [Feast Online Store]     [Dead-Letter / MinIO]
 (Redis Feature Serving)  (Late Audit Table)
```

1. **Baseline Job:** Flink job tiêu chuẩn sử dụng Processing Time window. Kết quả bị sai lệch nghiêm trọng do không tính đến độ trễ mạng và các bản ghi trùng lặp từ Kafka.
2. **Handle Late Arrival (Event-Time & Watermarking):**
   - Định nghĩa Watermark với độ trễ dung sai (BoundedOutOfOrderness):
     `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(15)).withTimestampAssigner(...)`
   - Bổ sung cơ chế `allowedLateness(Time.minutes(30))` cho cửa sổ tính toán số lượt xem phim đồng thời.
   - Các bản ghi đến muộn vượt quá 30 phút sẽ được định tuyến vào **Side Output (Late Data Stream)** để lưu về MinIO phục vụ việc chạy lại batch re-conciliation mà không làm đứt đoạn luồng chính.
3. **Handle Streaming Duplicates (Idempotency with State TTL):**
   - Sử dụng `KeyedProcessFunction` trên khóa `heartbeat_id`.
   - Lưu trữ `ValueState<Boolean>` trong RocksDB StateBackend với cấu hình `StateTtlConfig` trong 2 giờ. Nếu `heartbeat_id` đã tồn tại trong state, sự kiện trùng lặp sẽ bị loại bỏ ngay lập tức.
4. **Window Processing:**
   - **Tumbling Event Window (5 phút):** Tính toán lưu lượng xem phim tức thời theo thể loại `(genre_id, count_active_viewers)`.
   - **Sliding Window (1 giờ, slide 5 phút):** Tính toán tỷ lệ bỏ dở phim (skip/drop rate) thời gian thực của người dùng để cập nhật trực tiếp vào Feast Online Store (Redis).

---

## 7. Data Pipeline Orchestration (Apache Airflow)

Toàn bộ quy trình nạp, xử lý và tạo đặc trưng dữ liệu được điều phối bởi 3 DAGs độc lập trên Apache Airflow:

```
[DP1: Ingestion DAG] ---> [Bronze Lakehouse] 
                               │
[DP2: Transformation DAG] ---> [Silver / Gold Layer]
                                       │
[DP3: Feature Pipeline DAG] ---------> [Feast Offline & Online Store]
```

### 7.1. DP1: Raw Data Ingestion to Bronze Zone
- **Nhiệm vụ:** Kéo dữ liệu từ nguồn phát sinh (OLTP Database / MinIO Raw Landing) nạp vào bảng Bronze `raw_` trên Apache Iceberg.
- **Các Stage:**
  1. `ExtractStage`: Đọc dữ liệu gia tăng (incremental extract) dựa theo cột `updated_at`.
  2. `IngestStage`: Ghi dữ liệu dạng Append-Only vào tầng Bronze với partition theo ngày nạp.
  3. `ValidateStage`: Thực hiện kiểm tra tính toàn vẹn (Great Expectations):
     - Kiểm tra không bị thiếu file (file count > 0).
     - Kiểm tra số lượng bản ghi không giảm đột biến so với trung bình 7 ngày qua.
     - Kiểm tra các trường khóa chính không được `NULL`.

### 7.2. DP2: Transform Bronze to Silver & Gold Zone
- **Nhiệm vụ:** Kích hoạt Spark Job để làm sạch, khử trùng lặp, giải quyết schema evolution, cập nhật bảng SCD Type 2 `dim_users`, tính toán các bảng Fact và OBT.
- **Các Stage:**
  1. `TransformSilverStage`: Khử duplicate, ép kiểu dữ liệu chuẩn, ghi vào `stg_ratings` và `stg_watch_sessions`.
  2. `ValidateSilverStage`: Kiểm tra định dạng trường, tỷ lệ null của cột mới (`content_rating`), kiểm tra phạm vi giá trị `rating_val` trong khoảng $[0.5, 5.0]$.
  3. `TransformGoldStage`: Cập nhật `dim_users` (SCD Type 2), tính toán `fact_ratings`, `fact_watch_sessions`, và bảng tổng hợp `obt_movie_engagement`.
  4. `ValidateGoldStage`: Kiểm tra toàn vẹn khóa ngoại (Referential Integrity Check) giữa Fact và Dim; xác nhận cờ `is_current = true` chỉ xuất hiện duy nhất 1 lần cho mỗi `user_id`.

### 7.3. DP3: Feature Pipeline (Compute Offline Feature Tables)
- **Nhiệm vụ:** Tính toán bảng đặc trưng phục vụ huấn luyện Machine Learning và đồng bộ lên Feature Store (Feast).
- **Các Stage:**
  1. `ComputeFeatureStage`: Tính toán các chỉ số thống kê tích lũy của user và movie (ví dụ: `feat_user_avg_rating_90d`, `feat_movie_completion_rate_30d`) với hai cột bắt buộc `event_timestamp` và `created_timestamp`.
  2. `ValidateFeatureStage`: Kiểm tra phân phối đặc trưng (Data Drift Check), kiểm tra giá trị cực đoan (outliers), đảm bảo không có giá trị NaN/Infinite.
  3. `MaterializeToFeastStage`: Gọi lệnh `feast materialize` để đồng bộ đặc trưng mới nhất vào Redis (Online Feature Store) phục vụ mô hình inference thời gian thực.

---

## 8. Data Governance & Metadata Management (DataHub)

1. **End-to-End Data Lineage:**
   - Tích hợp DataHub Airflow Operator để tự động vẽ bản đồ phả hệ dữ liệu (Lineage Graph):
     `MinIO Source -> DP1 Ingestion -> raw_ratings -> DP2 Transformation -> stg_ratings -> fact_ratings -> DP3 Feature Pipeline -> feat_user_movie_stats -> Feast Feature View`.
   - Mỗi bảng trên DataHub hiển thị rõ DAG nào tạo ra nó, thời gian chạy gần nhất, và người chịu trách nhiệm (Owner).
2. **Data Contracts & Quality Assurances:**
   - Định nghĩa Data Quality Assertions trên DataHub:
     - `ratings.rating_val BETWEEN 0.5 AND 5.0` (Pass rate: 100%).
     - `users.subscription_tier IN ('Free', 'Standard', 'VIP', 'Family')`.
     - `watch_history.watch_duration_seconds >= 0`.
3. **Data Dictionary:**
   - Đính kèm mô tả nghiệp vụ (Glossary Terms) cho từng cột trong Gold Layer để các kỹ sư ML và phân tích dữ liệu dễ dàng tra cứu.
