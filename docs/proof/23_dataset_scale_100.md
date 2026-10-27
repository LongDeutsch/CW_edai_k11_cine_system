# 23 — Dataset offline scale-factor 100

**Phạm vi.** `python -m generator.offline_generator --scale-factor 100 --allow-large-scale`. Listing MinIO ngày 2026-10-26, bucket `s3://external-source-lake/`.

**Trong phạm vi này.** 27,527,170,951 byte là số đã đo. Ước lượng 101 GB trong file 01 là đặc tả, không phải lần đo này. 47 object `it-airflow` có từ trước, không thuộc lần sinh.

**Đọc tiếp.** [Tour](00_system_tour.md) · [02 — lakehouse](02_lakehouse_storage_proof.md)

```mermaid
flowchart LR
  gen[offline_generator scale 100] --> disk["Parquet 27527170951 byte"]
  disk --> minio[MinIO external-source-lake]
```

Sinh bằng `python -m generator.offline_generator --scale-factor 100 --allow-large-scale`.
Cùng Zipf, Pareto, Beta, duplicate 2.5% và schema evolution từ partition 45.
Đường chunk chỉ đổi cách ghi khi số dòng watch vượt bộ nhớ một mảng.

## Dung lượng

| Bảng | File | Bytes |
|---|---:|---:|
| `movie_genres` | 1 | 1234857 |
| `movies` | 100 | 4178559 |
| `ratings` | 1010 | 3982346537 |
| `tags` | 180 | 595617464 |
| `users` | 5 | 65853103 |
| `watch_history` | 2815 | 22877940431 |
| **Tổng** |  | **27527170951** |

Spec cũ ước lượng khoảng 101 GB. Parquet đo trên đĩa sau lần chạy này là 27,527,170,951 byte. Số dòng vẫn đúng scale-factor 100: 250,000,000 ratings, 30,000,000 tags, 800,000,000 watch.

## Skew

- Cột skew: `movie_id` trên ratings và watch_history. Nhóm đầu `1000` phim (head fraction 0.01) nhận 80.00% dòng, mục tiêu 80%.
- `movie_id=42` chiếm 4.00% (sàn 4%).
- `genre_name` Action+Comedy: 70.00% trên 300000 dòng `movie_genres`, mục tiêu 70%.
- Super-user 5% user → ratings 40.00%, tags 39.99%, watch 40.00%. Mục tiêu 40%.

## High cardinality

- `movie_id`: 100000 giá trị có mặt / 100000 phim trong catalog / 1076923120 dòng ratings+watch. Tỷ lệ unique/dòng = 0.00009286.
- `user_id` ratings: 4999986 / 5000000 user, unique/dòng = 0.01949994.
- `user_id` watch: 5000000 / 5000000 user, unique/dòng = 0.00609375.

## Schema evolution

- Mốc partition `45`.
- Schema cũ: 44 file, ví dụ `partition_00001.parquet`, cột `['movie_id', 'title', 'release_year', 'runtime_minutes', 'original_language', 'budget', 'director_id', 'created_ts']`.
- Schema mới: 56 file, ví dụ `partition_00045.parquet`, cột `['movie_id', 'title', 'release_year', 'runtime_minutes', 'original_language', 'budget', 'director_id', 'created_ts', 'content_rating', 'hdr_format', 'audio_spatial']`.

## Duplicate

- Ratings: 2.5000% (6410320 / 256410320), mục tiêu 2.50%. Sang ngày kế tiếp: 3209104.
- Watch: 2.5000% (20512800 / 820512800). Sang ngày kế tiếp: 10256980.
- Ví dụ ratings: `{'id_column': 'rating_id', 'source_id': 531869, 'duplicate_id': 1017686, 'source_ts': '2026-01-01T00:20:24', 'duplicate_ts': '2026-01-01T00:20:27', 'delta_seconds': 3, 'user_id': 3013656, 'movie_id': 44, 'rating_val': 4.0}`.

## Top 5 movie_id trên ratings + watch

| Hạng | movie_id | Title | Dòng | Share | Cộng dồn |
|---:|---:|---|---:|---:|---:|
| 1 | 42 | Dead Presidents (1995) | 43064301 | 4.00% | 4.00% |
| 2 | 1 | Toy Story (1995) | 21536201 | 2.00% | 6.00% |
| 3 | 1001 | Synthetic Feature 1001 (2004) | 21535446 | 2.00% | 8.00% |
| 4 | 2 | Jumanji (1995) | 14234606 | 1.32% | 9.32% |
| 5 | 3 | Grumpier Old Men (1995) | 11165097 | 1.04% | 10.36% |

## MinIO

`python -m generator.upload_to_source_minio --source-dir data_generator/output/scale_100` đã tải 4111 object Parquet, 27,527,170,951 byte, lên `s3://external-source-lake/`. Key giữ đường dẫn tương đối (`ratings/dt=...`, `watch_history/dt=...`, `movies/partition_*.parquet`).

Listing bucket ngày 2026-10-26 khớp từng prefix: `watch_history` 2815 object / 22,877,940,431 byte, `ratings` 1010 / 3,982,346,537, `tags` 180 / 595,617,464, `movies` 100 / 4,178,559, `users` 5 / 65,853,103, `movie_genres` 1 / 1,234,857. Bucket còn 47 object `it-airflow` có từ trước, không thuộc lần sinh này.

## Rating

- Trung bình 3.875 (kỳ vọng Beta(6, 2) khoảng 3.875).
- Trong [3.5, 4.5]: 77.26%.
- <= 2 sao: 1.61%.

