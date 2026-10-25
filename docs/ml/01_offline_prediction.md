# Dự đoán điểm cao sau một mốc thời gian

Bài toán này dùng dữ liệu phim đã có trong lakehouse. Chưa train model. Chưa cài Kubeflow, MLflow hay KServe. Streaming production không được bật. Feast offline và online nằm ở cuối tài liệu này.

## Quần thể và thời điểm

Một dòng dự đoán là một cặp `(user_id, movie_id)` tại mốc `event_timestamp = T`. `T` là đầu một ngày UTC. Feature chỉ được tính từ sự kiện có thời gian nhỏ hơn `T`. Nhãn lấy từ rating trong khoảng `[T, T + 1 ngày)`.

Rating đúng lúc `T` là nhãn, không phải feature.

## Nhãn

- Dương (`1`): rating sớm nhất trong cửa sổ nhãn có `rating_val >= 4.0`.
- Âm (`0`): rating sớm nhất trong cửa sổ đó nhỏ hơn `4.0`.
- Không có rating trong cửa sổ: không tạo dòng nhãn. Không gán âm cho sự vắng mặt.

## Feature offline

Bảng và cột lấy từ `storage/lakehouse/gold/gold_tables.sql`. Cột thời điểm ghi là `created_timestamp`, không phải tên `created`.

`feat_user_movie_stats_daily`, khóa `(user_id, event_timestamp)`:

| Cột | Cửa sổ | Nguồn |
| --- | --- | --- |
| `user_total_ratings_90d`, `user_avg_rating_90d` | `[T - 90 ngày, T)` | `stg_ratings` |
| `user_favorite_genre` | cùng cửa sổ rating | chỉ khi có `stg_movie_genres`. Silver production không có genre, nên cột là null |
| `user_total_watch_hours_30d` | `[T - 30 ngày, T)` | `watch_duration_seconds / 3600` của `stg_watch_sessions` |

`feat_movie_engagement_stats_daily`, khóa `(movie_id, event_timestamp)`:

| Cột | Cửa sổ | Nguồn |
| --- | --- | --- |
| `movie_rating_count_30d`, `movie_avg_rating_30d` | `[T - 30 ngày, T)` | `stg_ratings` |
| `movie_completion_rate_30d` | `[T - 30 ngày, T)` | trung bình `completion_rate` |
| `movie_trending_score_7d` | `[T - 7 ngày, T)` | tổng giờ xem. Không đọc `stg_playback_windows` |

Count bằng 0 khi entity có hoạt động ở nguồn kia nhưng không có sự kiện trong cửa sổ đó. Average null khi count bằng 0. Job từ chối khóa null và khóa trùng.

`fact_ratings` không được dùng làm nguồn vì bảng đó chỉ có `user_sk`, không có `user_id`.

## Feature streaming, chưa tính

Khi streaming được phép chạy, feature online có thể lấy số heartbeat và số người xem khác nhau trong một phút từ `stg_playback_windows`. Phase B3 đã sync bảng cô lập `it_p2_b3_windows`, không phải bảng production. Chi tiết ở `docs/proof/17_feast_streaming.md`.

## Rò dữ liệu và cách chia tập

Rò dữ liệu xảy ra nếu rating hoặc lượt xem tại thời điểm `>= T` đi vào feature của chính mốc `T`. Job dùng điều kiện `timestamp < event_timestamp`.

Không chia ngẫu nhiên theo dòng. Train là các ví dụ có `event_timestamp < cutoff`. Test là `event_timestamp >= cutoff`. Cùng một user có thể nằm ở cả hai phía nếu các mốc khác nhau. Đó là chia theo thời gian, không phải theo người.

## Feast offline

Hai bảng có entity `user_id` hoặc `movie_id`, `event_timestamp` và `created_timestamp`. Repo `ml/feast/` khai báo Feature View cho đúng các cột này. Nguồn là Parquet ghim một snapshot Iceberg, vì Feast 0.67 không nhận snapshot id trên Iceberg REST. Cách lấy lịch sử nằm ở `docs/ml/02_feast_offline.md`.

Online store là Redis. Giá trị online chỉ có sau khi materialize đúng export đã publish. Cách làm và giới hạn nằm ở `docs/proof/16_feast_online.md`. Chưa có Kubeflow, MLflow, KServe, và không có số liệu model.
