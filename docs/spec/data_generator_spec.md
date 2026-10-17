# Movie Recommendation - Synthetic Data Generator Specification ($\ge$ 100GB)

## 1. Mục tiêu & Nguyên tắc thiết kế (Objectives & Principles)

Tài liệu này đặc tả phương pháp luận và kỹ thuật sinh dữ liệu giả lập quy mô lớn ($\ge 100\text{ GB}$) cho nền tảng Movie Recommendation & Streaming Analytics (CineStream). 

Bộ sinh dữ liệu (Data Generator) được thiết kế nhằm đáp ứng đầy đủ các tiêu chuẩn khắt khe trong Rubric EDAI K11:
1. **Tính chân thực (Realism):** Mô phỏng sát thực tế hành vi người dùng bằng các phân phối xác suất thống kê nâng cao (Zipfian, Pareto, Beta) thay vì dữ liệu ngẫu nhiên đồng đều (Uniform distribution).
2. **Quy mô lớn ($\ge 100\text{ GB}$):** Đảm bảo đủ khối lượng để bộc lộ rõ rệt sự khác biệt về hiệu năng giữa mã nguồn chưa tối ưu (Baseline) và mã nguồn đã tối ưu (Optimized) trên Apache Spark và Apache Flink.
3. **Cơ chế chủ động tiêm lỗi (Fault & Problem Injection):** Tự động tiêm các bất thường dữ liệu (Data Skew, Schema Evolution, Duplicates, Late Arrivals, Out-of-order) với tỷ lệ và vị trí được kiểm soát chính xác để đo đạc và đánh giá.
4. **Mô phỏng nguồn thực tế (Source Simulation):** Lưu trữ dữ liệu sau khi sinh vào **MinIO** (mô phỏng Data Lake của phòng ban khác) và bắn trực tiếp vào **Apache Kafka** (mô phỏng luồng telemetry từ thiết bị client).

---

## 2. Các phân phối xác suất thống kê (Statistical Distributions)

Dữ liệu xem phim và đánh giá trong thực tế không bao giờ phân bổ ngẫu nhiên đồng đều. Bộ sinh dữ liệu áp dụng các mô hình toán học sau:

### 2.1. Phân phối Zipfian (Power-Law) cho độ phổ biến của phim (Movie Popularity)
- **Công thức xác suất:**
  $$P(k) = \frac{1 / k^s}{\sum_{n=1}^{N} (1 / n^s)}$$
  với $s \approx 1.25$, $N \approx 100,000$ phim, và $k$ là thứ hạng phổ biến của bộ phim.
- **Hành vi sinh ra:** Top 1% các bộ phim bom tấn (Blockbusters) nhận được hơn **80% tổng lượt xem và lượt đánh giá**, tạo ra hiện tượng **Data Skew** cực lớn khi join trên Spark.

### 2.2. Phân phối Pareto (80/20) cho mức độ tích cực của người dùng (User Activity)
- **Hành vi:** Khoảng 5% "super-users" (những người đam mê điện ảnh) đóng góp hơn 40% tổng số lượt đánh giá và phiên xem, trong khi phần lớn người dùng thông thường chỉ xem 1-2 phim mỗi tuần.

### 2.3. Phân phối Beta cho điểm đánh giá (Rating Value Distribution)
- Điểm đánh giá trong thế giới thực thường có xu hướng lệch về phía tích cực (người dùng thường chọn xem phim họ thích, hoặc chấm điểm cao khi hài lòng):
  $$\text{Rating} \sim \text{Beta}(\alpha=6, \beta=2) \times 4.5 + 0.5$$
- Cho ra phân phối tập trung chủ yếu ở mức 3.5 - 4.5 sao, rất ít điểm 1.0 hoặc 2.0 sao.

---

## 3. Quy mô dữ liệu chi tiết (Target Volume $\ge$ 100GB)

| Bảng / Tập tin | Định dạng | Số lượng bản ghi | Dung lượng đĩa (Uncompressed) | Dung lượng Parquet Snappy |
|---|---|---|---|---|
| `users` | Parquet | 5,000,000 dòng | ~1.2 GB | ~450 MB |
| `movies` | Parquet | 100,000 dòng | ~150 MB | ~50 MB |
| `movie_genres` | Parquet | 300,000 dòng | ~50 MB | ~20 MB |
| `ratings` | Parquet | 250,000,000 dòng | ~55 GB | ~22 GB |
| `tags` | Parquet | 30,000,000 dòng | ~7 GB | ~2.5 GB |
| `watch_history` | Parquet | 800,000,000 dòng | ~180 GB | ~76 GB |
| **Tổng cộng Offline** | **Parquet** | **> 1 tỷ bản ghi** | **> 240 GB** | **~ 101 GB** |

> **Quy cách đóng gói file:** Dữ liệu được chia thành các partition theo `event_date` (yyyy-MM-dd), mỗi partition gồm các file Parquet kích thước từ 64MB đến 128MB để Spark có thể phân bổ đều trên các Executor core.

---

## 4. Đặc tả cơ chế tiêm lỗi dữ liệu (Data Problems Injection Spec)

### 4.1. Tiêm Skew (Offline Data Problem 1)
- **Cột bị Skew:**
  1. `movie_id` trong bảng `watch_history` và `ratings`:
     - Phim có `movie_id \in [1, 1000]` (chiếm 1% số phim) nhận **80% tổng số lượt xem và lượt đánh giá**.
     - Phim có `movie_id = 42` (tượng trưng cho phim bom tấn toàn cầu như *Avatar*) chiếm đơn độc **4% toàn bộ dung lượng dữ liệu**.
  2. `genre_name` trong bảng `movie_genres`: 70% các bản ghi liên kết phim mang thể loại *Action* hoặc *Comedy*.
- **High Cardinality Column:** `movie_id` có 100,000 giá trị duy nhất, `user_id` có 5,000,000 giá trị duy nhất.

### 4.2. Tiêm Schema Evolution (Offline Data Problem 2)
- **Thời điểm tiến hóa:** Tại mốc thời gian ngày thứ 90 (hoặc file partition batch số 45 trở đi):
  - **Schema thế hệ cũ (Files 1 $\rightarrow$ 44):**
    ```
    root
     |-- movie_id: long
     |-- title: string
     |-- release_year: integer
     |-- runtime_minutes: integer
     |-- original_language: string
    ```
  - **Schema thế hệ mới (Files 45 $\rightarrow$ 100):**
    ```
    root
     |-- movie_id: long
     |-- title: string
     |-- release_year: integer
     |-- runtime_minutes: integer
     |-- original_language: string
     |-- content_rating: string (nullable: 'G', 'PG-13', 'R', 'NC-17')
     |-- hdr_format: string (nullable: 'HDR10', 'Dolby_Vision', 'SDR')
     |-- audio_spatial: boolean (nullable: true/false)
    ```
- **Hành vi sinh:** Các file cũ hoàn toàn không chứa 3 cột mới này trong Parquet metadata footer.

### 4.3. Tiêm Offline Duplicates (Offline Data Problem 3)
- **Tỷ lệ trùng lặp:** Đúng **2.5%** tổng số bản ghi trong bảng `ratings` và `watch_history`.
- **Cơ chế tiêm:**
  1. Khi sinh được 1 bản ghi hợp lệ: `(user_id=123, movie_id=456, rating_val=4.5, rating_ts='2026-05-10 14:00:00')`.
  2. Với xác suất $p = 0.025$, bộ sinh tạo thêm 1 bản ghi thứ hai trùng toàn bộ khóa nghiệp vụ `(user_id=123, movie_id=456, rating_val=4.5)`, nhưng sinh `rating_id` mới ngẫu nhiên và `rating_ts = rating_ts + 3 seconds`.
  3. Bản ghi trùng lặp được ghi lẫn vào cùng một partition hoặc partition kế tiếp (mô phỏng retry do lỗi mạng).

### 4.4. Tiêm Streaming Late Arrivals (Online Data Problem 1)
- **Topic:** `movie.playback.heartbeat`.
- **Tỷ lệ đến muộn:** **6.0%** tổng số sự kiện streaming.
- **Cơ chế tiêm độ trễ:**
  - Thông thường: Event được đẩy vào Kafka với `event_ts \approx producer_send_ts` (độ trễ $< 2$ giây).
  - Đối với 6% sự kiện mô phỏng chế độ xem offline:
    - `event_ts` được sinh lùi về quá khứ từ **10 phút đến 35 phút** so với đồng hồ hiện tại (`current_wall_clock_time`).
    - Sau đó, một loạt các event này được "xả" ồ ạt vào Kafka cùng một lúc khi client kết nối lại mạng.
- **Thử thách cho Flink:** Nếu Watermark không được cấu hình `BoundedOutOfOrderness` phù hợp, Flink Window 5 phút sẽ làm rơi rớt toàn bộ 6% dữ liệu này.

### 4.5. Tiêm Streaming Duplicates (Online Data Problem 2)
- **Topic:** `movie.playback.heartbeat`.
- **Tỷ lệ trùng lặp:** **1.8%** tổng số sự kiện streaming.
- **Cơ chế tiêm:**
  - Bộ sinh Kafka Producer giữ lại một danh sách hàng đợi các event vừa gửi.
  - Với xác suất $1.8\%$, Producer gửi lại nguyên vẹn event trước đó (giữ nguyên hoàn toàn `heartbeat_id`, `session_id`, `current_position_seconds`, và `event_ts`) sau khoảng 200ms - 1500ms.

### 4.6. Tiêm Out-of-Order Events (Online Problem bổ sung)
- Trong một phiên xem phim kéo dài 100 nhịp tim (`heartbeat_seq = 1, 2, ..., 100`), khoảng 3% các nhịp tim bị tráo đổi thứ tự khi gửi vào Kafka (ví dụ nhịp số 45 đến sau nhịp số 48) do đi qua các đường mạng khác nhau.

---

## 5. Kiến trúc lưu trữ đích của Data Generator

```
                           [Synthetic Data Generator Process]
                                           │
                    ┌──────────────────────┴──────────────────────┐
                    ▼ (Batch Mode)                                ▼ (Stream Mode)
         [MinIO Object Storage]                         [Apache Kafka Cluster]
    Bucket: s3://external-source-lake/              Broker: kafka:9092
                    │                                             │
    ├── /users/partition_*.parquet                  ├── Topic: movie.playback.heartbeat
    ├── /movies/partition_*.parquet                 └── Topic: movie.user.clickstream
    ├── /ratings/dt=YYYY-MM-DD/*.parquet
    └── /watch_history/dt=YYYY-MM-DD/*.parquet
```

1. **Batch Storage Target (MinIO):**
   - Đóng vai trò là hệ thống lưu trữ bên ngoài (mô phỏng nguồn dữ liệu từ hệ thống Production Core của phòng ban khác).
   - Airflow DAG DP1 sẽ kết nối qua S3/MinIO Hook để kéo dữ liệu từ bucket này về Bronze Lakehouse Zone.
2. **Streaming Target (Kafka):**
   - Kafka Producer chạy song song liên tục, phát các sự kiện theo thời gian thực với cấu hình thông lượng kiểm soát được (từ 1,000 đến 10,000 msgs/sec).
