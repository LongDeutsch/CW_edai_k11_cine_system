# Movie Recommendation - ML Platform Architecture & Design

## 1. ML Use Case

**Tên bài toán:** Dự đoán khả năng người dùng sẽ xem và yêu thích một bộ phim được đề xuất trong vòng 7 ngày tới (*Next-7-Day Movie Watch & Like Prediction*).

**Ý nghĩa nghiệp vụ:**
- Trên ứng dụng xem phim trực tuyến (CineStream), không gian hiển thị trên màn hình trang chủ (Home Carousel & Recommendation Rails) rất giới hạn.
- Đề xuất những bộ phim người dùng chắc chắn sẽ tự tìm xem sẽ làm lãng phí vị trí hiển thị quý giá. Ngược lại, đề xuất những phim người dùng bấm vào rồi tắt sau 2 phút sẽ làm giảm tỷ lệ giữ chân (Retention Rate) và tăng nguy cơ hủy gói thuê bao (Churn).
- Dự đoán xác suất xem và thích trong chu kỳ 7 ngày cho phép thuật toán sắp xếp (Ranking Engine) ưu tiên những nội dung phù hợp nhất với khẩu vị dài hạn (sở thích thể loại) kết hợp với tâm trạng tức thời (hành vi xem trong vài giờ gần nhất).

**Thực thể dự đoán (Prediction Entity):** Cặp `(user_id, movie_id)`.

**Thời điểm dự đoán (Inference Time):**
- **Batch Scoring:** Chạy mỗi đêm cho Top-100 phim tiềm năng của từng active user để nạp vào cache trang chủ.
- **Real-time Re-ranking:** Khi người dùng mở app hoặc tải lại trang chủ, microservice Recommendation gọi API KServe, kết hợp với các streaming features vừa phát sinh trong 15-30 phút gần nhất từ Feast Online Store để sắp xếp lại danh sách đề xuất.

---

## 2. Label Definition

**Tên cột nhãn (Label):** `will_watch_and_like_7d` (Nhị phân: 1 hoặc 0).

**Quy tắc xác định nhãn (Ground Truth Logic):**
- Một tương tác giữa `user_id` và `movie_id` được coi là tích cực (`label = 1`) nếu trong vòng 7 ngày kể từ thời điểm quan sát ($T_0$), người dùng thỏa mãn **ít nhất một** trong hai điều kiện sau:
  1. Có bản ghi trong `fact_watch_sessions` với `completion_rate >= 0.70` (đã xem trên 70% thời lượng bộ phim).
  2. Có bản ghi trong `fact_ratings` với `rating_val >= 4.0` (đánh giá phim từ 4.0 sao trở lên).
- Ngược lại, nếu người dùng có thấy hiển thị nhưng không xem, hoặc xem dưới 10% rồi thoát (bỏ dở), hoặc chấm điểm dưới 3.0 thì gán `label = 0`.

**Cấu trúc bảng nhãn (Label Table):**
- Hạt dữ liệu (Grain): 1 dòng cho mỗi cặp `(user_id, movie_id, label_date)`.
- Các cột: `user_id`, `movie_id`, `label_date`, `will_watch_and_like_7d`.
- Khóa kết nối (Join Keys): `user_id`, `movie_id` kết hợp với `label_date = event_timestamp` để thực hiện Point-in-time Feature Join chống rò rỉ dữ liệu (Data Leakage).

---

## 3. Feature Plan & Feature Store (Feast)

Hệ thống phân tách rõ ràng giữa đặc trưng dài hạn (Offline Features) và đặc trưng thời gian thực (Streaming Features):

### 3.1. Offline Features (Tính toán theo lô hàng ngày, lưu trữ trong Data Lakehouse)

Được trích xuất từ dữ liệu lịch sử trong `fact_ratings`, `fact_watch_sessions`, và `dim_movies`:

| Feature Name | Entity | Kiểu dữ liệu | Mô tả chi tiết |
|---|---|---|---|
| `user_avg_rating_90d` | `user_id` | Float | Điểm đánh giá trung bình user chấm trong 90 ngày qua (thể hiện tính cách khó/dễ tính). |
| `user_ratings_count_90d` | `user_id` | Integer | Tổng số lượt chấm điểm trong 90 ngày (mức độ tương tác của user). |
| `user_total_watch_hours_30d` | `user_id` | Float | Tổng số giờ xem phim tích lũy trong 30 ngày gần nhất. |
| `user_genre_affinity_score` | `user_id, genre` | Float | Tỷ lệ phần trăm thời lượng xem dành cho thể loại phim tương ứng trong 60 ngày. |
| `movie_avg_rating_90d` | `movie_id` | Float | Điểm đánh giá trung bình toàn hệ thống của bộ phim trong 90 ngày. |
| `movie_rating_count_90d` | `movie_id` | Integer | Số lượt đánh giá của phim (đo lường độ phổ biến - popularity). |
| `movie_completion_rate_30d` | `movie_id` | Float | Tỷ lệ người xem xem hết trên 70% thời lượng của bộ phim này trong 30 ngày qua. |
| `days_since_release` | `movie_id` | Integer | Số ngày kể từ ngày công chiếu đến ngày quan sát (đo lường độ mới - recency). |

### 3.2. Streaming Features (Tính toán thời gian thực bằng Apache Flink, ghi vào Feast Online Store)

Được cập nhật liên tục từ luồng Kafka `movie.playback.heartbeat` và `movie.user.clickstream` vào Redis:

| Feature Name | Entity | Kiểu dữ liệu | TTL | Mô tả chi tiết |
|---|---|---|---|---|
| `user_watch_duration_24h` | `user_id` | Float | 24 giờ | Số phút người dùng đã xem phim trong 24 giờ qua (nhận biết ngày rảnh rỗi / xem liên tục). |
| `user_recent_genres_viewed_1h`| `user_id` | String | 1 giờ | Danh sách các thể loại phim user vừa click/xem trong 1 giờ qua (nắm bắt tâm trạng tức thời). |
| `user_skip_rate_15m` | `user_id` | Float | 15 phút | Tỷ lệ bấm chuyển phim trong 15 phút gần nhất (dấu hiệu đang phân vân, chưa tìm được phim ưng ý). |
| `movie_active_viewers_now` | `movie_id` | Integer | 5 phút | Số lượng người đang đồng thời xem bộ phim này (đo lường độ trending tức thời). |

---

## 4. Training Data Shape & Split Strategy

1. **Point-in-Time Correct Feature Lookup:**
   - Sử dụng Feast SDK để ghép nối bảng nhãn (Observation Table) với các Feature Views lịch sử tại đúng thời điểm $T_{\text{label\_date}}$:
     $$\text{Dataset} = \text{Feast.get\_historical\_features}(\text{entity\_df}=\text{label\_df}, \text{features}=[\dots])$$
   - Đảm bảo loại bỏ hoàn toàn hiện tượng Future Leakage (không sử dụng thông tin phát sinh sau ngày gán nhãn).
2. **Loại bỏ dữ liệu rác (Data Filtering):**
   - Lọc bỏ các user mới chưa từng xem quá 3 bộ phim (Cold-start users sẽ được xử lý riêng bằng thuật toán Popularity & Onboarding Questionnaire).
3. **Chiến lược phân chia dữ liệu (Time-based Splitting):**
   - Tuyệt đối không dùng Random Split vì dữ liệu mang tính chuỗi thời gian (sở thích và xu hướng phim thay đổi theo thời gian).
   - **Train Set (80% mốc thời gian đầu):** Ví dụ từ ngày $1 \rightarrow 72$.
   - **Validation Set (10% mốc thời gian tiếp theo):** Từ ngày $73 \rightarrow 81$ (dùng để Early Stopping và Tuning Hyperparameters).
   - **Test Set (10% mốc thời gian cuối cùng):** Từ ngày $82 \rightarrow 90$ (dùng để báo cáo đánh giá cuối cùng).

---

## 5. Model Architecture & Selection

Hệ thống triển khai kiến trúc đề xuất hai giai đoạn chuẩn công nghiệp (Two-Stage Recommendation Pipeline):

```
[Candidate Retrieval (Candidate Generation)]
      │ (Chọn ra Top 500 phim từ 100,000 phim)
      ▼
[Feature Enrichment via Feast Online Store]
      │
      ▼
[Ranking Model (KServe Inference)]
      │ (Chấm điểm xác suất Click & Watch)
      ▼
[Top 20 Personalized Recommendations to User UI]
```

1. **Giai đoạn 1: Candidate Retrieval (Thu hẹp không gian):**
   - Sử dụng mô hình **Two-Tower Neural Network** (hoặc Matrix Factorization / Approximate Nearest Neighbor với Qdrant) để lấy nhanh 500 ứng viên tiềm năng cho người dùng từ hàng trăm ngàn bộ phim.
2. **Giai đoạn 2: Precision Ranking (Chấm điểm chi tiết):**
   - **Baseline Model:** Logistic Regression kết hợp các đặc trưng one-hot thể loại cơ bản (đơn giản, tốc độ huấn luyện nhanh, dễ giải thích trọng số).
   - **Main Model:** **Gradient Boosted Decision Trees (LightGBM Ranker / XGBoost)** hoặc Deep & Cross Network (DCN):
     - Xử lý mượt mà sự kết hợp giữa các đặc trưng số thực (tỷ lệ xem, số giờ xem) và đặc trưng phân loại (thể loại phim, quốc gia).
     - Bắt trọn các mối quan hệ phi tuyến (Non-linear Interactions) giữa độ tuổi người dùng và thể loại phim.
3. **Thước đo đánh giá (Evaluation Metrics):**
   - **AUC-ROC / PR-AUC:** Đánh giá khả năng phân loại tổng thể trên tập nhãn mất cân bằng.
   - **NDCG@10 (Normalized Discounted Cumulative Gain tại vị trí 10):** Thước đo quan trọng nhất cho bài toán Ranking, đánh giá xem các bộ phim người dùng thực sự thích có được xếp ở các vị trí đầu tiên của trang chủ hay không.
   - **Hit Rate@10 & Precision@10:** Tỷ lệ người dùng có ít nhất một tương tác tích cực trong danh sách Top-10 đề xuất.

---

## 6. MLOps Platform Components

### 6.1. Feature Store (Feast)
- **Cấu hình Repo (`feature_store.yaml`):**
  - Offline Store: Trỏ tới bảng Iceberg Gold Layer trên MinIO thông qua Spark/Trino.
  - Online Store: Trỏ tới Redis Cluster độ trễ thấp ($< 5\text{ms}$).
- **Materialization Pipeline:** Chạy Airflow DAG định kỳ (DP3) để đẩy các đặc trưng mới nhất vào Redis.
- **Streaming Ingestion:** Flink Job ghi trực tiếp các streaming features thông qua Feast Push Source API.

### 6.2. Training Pipeline (Kubeflow Pipelines)
Một DAG hoàn chỉnh trên Kubeflow Pipelines gồm các containerized steps:
1. `data_extraction_step`: Đọc dữ liệu từ Lakehouse và trích xuất historical features qua Feast.
2. `data_validation_step`: Kiểm tra drift phân phối dữ liệu huấn luyện bằng Evidently AI / Deepchecks.
3. `model_training_step`: Huấn luyện mô hình LightGBM Ranker với Hyperparameter Tuning (Optuna).
4. `model_evaluation_step`: Đánh giá NDCG@10 và AUC-ROC trên tập Test độc lập.
5. `model_gate_step`: Nếu `NDCG@10 > 0.78` và vượt qua baseline, tự động đẩy mô hình và thông số vào MLflow Model Registry với trạng thái `Candidate`.

### 6.3. Experiment Tracking & Registry (MLflow)
- Lưu trữ toàn bộ siêu tham số (Hyperparameters: learning_rate, num_leaves, max_depth).
- Lưu trữ Artifacts: File model (`model.pkl`), Feature Importance Plot, ma trận Confusion Matrix.
- **Model Registry:** Quản lý vòng đời mô hình (`None` $\rightarrow$ `Staging` $\rightarrow$ `Production`).

### 6.4. Model Serving (KServe on Kubernetes)
- Triển khai mô hình dưới dạng Pod KServe InferenceService với chuẩn giao tiếp **v2 Data Plane (Open Inference Protocol)**.
- **Tối ưu hóa hiệu năng:**
  - Hỗ trợ Auto-scaling (HPA) từ 1 lên 10 pods dựa theo tải CPU hoặc số lượng request/giây.
  - Rolling Update đảm bảo Zero-Downtime khi nâng cấp phiên bản mô hình.
  - Hỗ trợ Canary Deployment (định tuyến 10% traffic sang model mới để đánh giá A/B Testing).
