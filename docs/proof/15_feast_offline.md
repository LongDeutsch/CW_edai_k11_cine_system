# 15 — Feast offline và point-in-time

**Phạm vi.** Chạy lúc 2026-10-25. `make feast-offline-it`, PASS. Feast `0.67.0`. Warehouse `file://`, namespace `it_b1`. Không ghi `bronze` / `silver` / `gold`.

**Trong phạm vi này.** Bài này không materialize và không ghi Redis. Redis db 1 và db 2 là file 16 và 17.

**Đọc tiếp.** [Tour](00_system_tour.md) · [16 — Redis db 1](16_feast_online.md)

```mermaid
flowchart LR
  ice[feat Iceberg file it_b1] --> feast[Feast offline]
  feast --> pit[historical_features]
```

Chạy lúc 2026-10-25. Lệnh `make feast-offline-it` kết thúc PASS. Sáu test đơn vị OK. Log đối chiếu snapshot: `docs/proof/logs/phase_b1_feast_offline.json`. Feast `0.67.0`.

HEAD git vẫn `51b22ba`. Phần B1 nằm trên working tree, chưa commit. Không ghi namespace `bronze`, `silver`, `gold`. Không bật streaming. Bài offline này không materialize và không ghi Redis. Online serving đã chạy sau đó: Redis db 1 ở [16_feast_online.md](16_feast_online.md), Redis db 2 ở [17_feast_streaming.md](17_feast_streaming.md). Không có Kubeflow, MLflow, KServe.

## Catalog production lúc chạy

REST `127.0.0.1:8181` còn đọc được `it_af_dp.raw_users`, snapshot `1062383487765229157`. `gold.fact_ratings`, `gold.fact_watch_sessions`, hai bảng `feat_*` và `silver.stg_ratings` trả HTTP error cả trước và sau bài test. Fingerprint trước và sau giống nhau. Bảng `feat_*` của DP3 trên namespace `it_af_dp` không còn trong catalog, nên bài test không đọc các snapshot lịch sử `1992904055192348575` và `1720343794407991313`.

Dòng logic của DP3 được tính lại bằng `publish_features` trên warehouse `file://`, namespace `it_b1`. `batch_id` của lần export là `batch-b1-file-snapshot`. `batch_id` DP3 `batch-ad6a69a535ee5297ad2c73b10942813a` được giữ trong metadata để đối chiếu, không phải snapshot mà Feast đã đọc.

## Snapshot Feast đã đọc

| Bảng | Snapshot file:// | Dòng logic |
| --- | --- | --- |
| `feat_user_movie_stats_daily` | `2533810920237671057` | 4 |
| `feat_movie_engagement_stats_daily` | `2290706951991004865` | 4 |

Lần ghi đầu `overwritten`. Lần ghi cùng input `unchanged`. Mỗi bảng còn một snapshot. Export lần hai không đổi số dòng.

Dòng user 7, movie 11, `event_timestamp` `2026-01-20`, `created_timestamp` `2026-01-21T03:00:00` khớp DP3: 1 rating, điểm trung bình 4.0, 0.166667 giờ xem, genre null, completion 0.5, trending 0.166667. Nguồn là một rating 4.0 và một phiên 600 giây lúc 2026-01-15 12:00.

## Point-in-time

`historical_features` chỉ lấy dòng có `event_timestamp` và `created_timestamp` không vượt mốc của entity.

| Entity | Mốc hỏi | Kết quả |
| --- | --- | --- |
| user 7, movie 11 | `2026-01-21T03:00:00` | dòng 2026-01-20: 1 rating, movie count 1 |
| user 7, movie 11 | `2026-01-21T02:00:00` | null. Batch chưa được ghi |
| user 7, movie 11 | `2026-02-01T00:00:00` | dòng 2026-02-01: 2 rating, movie count 2, trending 0 |
| user 8, movie 12 | `2026-01-21T03:00:00` | rating của user 8, không nhận số của user 7 |
| user 8, movie 11 | `2026-02-01T00:00:00` | feature user 8 đi với feature movie 11 |
| user 9, movie 99 | `2026-02-01T00:00:00` | null |

Test đơn vị thêm hai bản cùng `event_timestamp` `2026-01-20` nhưng `created_timestamp` 03:00 và 05:00. Hỏi lúc 04:00 được bản 03:00. Hỏi lúc 06:00 được bản 05:00. Bản `event_timestamp` `2026-02-01` có `created_timestamp` `2026-02-02T03:00:00` không lọt vào câu hỏi lúc `2026-02-02T00:00:00`. Gọi Feast với bộ lọc created tắt thì bản 05:00 lọt vào câu hỏi lúc 04:00. Đó là lý do helper luôn bật bộ lọc.

Parquet thiếu `user_avg_rating_90d` bị `export_frames` từ chối, và `get_historical_features` báo đúng tên cột. `feast apply` chạy hai lần, registry vẫn hai Feature View, cả hai `online=False`.

## Việc còn lại

Tại thời điểm B1, Module 2 R11 và R12 còn MISSING. Phase B2 sau đó materialize sang Redis. Trạng thái hiện tại nằm ở `docs/proof/16_feast_online.md`.
