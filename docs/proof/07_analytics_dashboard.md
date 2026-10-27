# 07 — Dashboard analytics

**Phạm vi.** `make serving-it-dashboard` lúc 2026-10-18 13:17 UTC. Batch Gold ở database `it_it_af_real`. Cửa sổ Flink ở `it_e2e`. API `127.0.0.1:18083`, dashboard `127.0.0.1:18084`.

**Trong phạm vi này.** Dashboard chỉ gọi FastAPI. DataHub GMS ở file 20 cũng ghi cổng 18083 trên một lần chạy khác.

**Đọc tiếp.** [Tour](00_system_tour.md) · [15 — Feast offline](15_feast_offline.md)

```mermaid
flowchart LR
  ch[ClickHouse] --> api[FastAPI]
  api --> dash[Dashboard]
```

Dashboard chỉ gọi FastAPI. Không đọc Iceberg, Kafka hay ClickHouse, và không bật streaming production.

Lệnh đã chạy lúc 2026-10-18 13:17 UTC:

```bash
make serving-it-dashboard
```

Kết quả `PASS`. Log: `docs/proof/logs/phase4_dashboard_it.json`.

## Cách chạy và tắt

Batch Gold đã sync nằm ở database `it_it_af_real`. Cửa sổ Flink đã sync nằm ở `it_e2e`. `cinestream_serving` vẫn rỗng vì production chưa được poll.

Một process API đọc cả hai. `CINESTREAM_CLICKHOUSE_DATABASE` là batch. `CINESTREAM_TRENDING_DATABASE` là trending. Nếu biến trending trống, API dùng cùng database batch như trước.

```bash
make serving-up
CINESTREAM_CLICKHOUSE_DATABASE=it_it_af_real CINESTREAM_TRENDING_DATABASE=it_e2e make serving-api
make serving-dashboard
```

Mở http://127.0.0.1:18084. API là http://127.0.0.1:18083. Không cần khởi động lại API để xem cả overview lẫn trending.

Nginx cùng trang, proxy tới API trên host:

```bash
make serving-dashboard-up
make serving-dashboard-down
```

Nginx nghe http://127.0.0.1:18085. `serving-dashboard-down` chỉ tắt container dashboard. Ctrl-C để tắt process `serving-api` và `serving-dashboard`. `make serving-down` dừng ClickHouse và giữ MinIO.

## Những gì trang vẽ

| Phần | Endpoint | Field được hiện |
|---|---|---|
| Overview | `/analytics/overview` | `session_count`, `total_watch_seconds`, `watched_movie_count`, `rated_movie_count`, `rating_count`, `mean_rating`, `batch_id`, `published_at`, hai snapshot |
| Top rated | `/movies/top-rated` | `movie_id`, `title`, `content_rating`, `avg_rating`, `rating_count` |
| Most watched | `/movies/most-watched` | `movie_id`, `title`, `watch_seconds`, `session_count` |
| Trending | `/movies/trending` | `movie_id`, `recent_heartbeats`, `recent_watch_seconds`, `peak_minute_unique_viewers`, `latest_window_end`, `synced_at` |
| Status | `/serving/status` | `consistent`, `log_present`, snapshot batch, `trending_freshness`, `trending_live` |

`mean_rating` là trung bình có trọng số theo `rating_count`. `peak_minute_unique_viewers` là `max(active_users)` của một phút. Trang không thêm genre, điểm trending hay title cho cửa sổ Flink. Nhãn `live` chỉ xuất hiện khi API trả `live=true` và `freshness=live`.

Lỗi 409 và 503 được thử lại hai lần. Hết giờ chờ sau 20 giây cũng được thử lại. Dữ liệu của lần đọc trước vẫn hiện, nhưng chip `live` bị gỡ.

## Đối chiếu với API

| Database | API | Dashboard |
|---|---|---|
| `it_it_af_real` | `session_count` 662830, `total_watch_seconds` 3725663032, 57665 rating, `mean_rating` 3.7678471072574355. Top movie 63, `avg_rating` 4.05488. Movie xem nhiều nhất là 42, `watch_seconds` 2628418172, 504009 phiên. `/movies/trending` HTTP 503 vì chưa có bảng `movie_windows`. Status `consistent=true`, `trending_freshness=empty`, `trending_live=false` | Cùng các số. Snapshot rating `1073738621869077082` giữ đủ chữ số. Không có chip live |
| `it_e2e` | Overview rỗng. Trending movie 42: 7 heartbeat, 70 giây, peak 5. `latest_window_end` `1970-01-01 00:02:00.000`, `synced_at` `2026-10-18 11:34:46.719`, `freshness=stale`, `live=false` | Cùng dòng và cùng nhãn stale |
| `cinestream_serving` | Session 0, không snapshot, trending `empty` | Ô trống, không gắn live |
| ClickHouse `127.0.0.1:9` | Cả năm endpoint HTTP 503 | Không vẽ overview, `live=false` |
| `it_dash_conflict` | Overview 409 `reader table đang trộn nhiều snapshot`. Status HTTP 200, `consistent=false` | Không vẽ metric batch. Banner nói 409. Database này đã bị `DROP` sau test |

Ảnh: `docs/proof/images/phase4_batch.png`, `phase4_batch_lower.png`, `phase4_mobile.png`, `phase4_trending.png`.

Nginx :18085 trả HTML 200 và `/api/analytics/overview` đúng `session_count` 662830 khi API trỏ `it_it_af_real`.

## Phase 4B — hai database trên một API

Lệnh đã chạy lúc 2026-10-18 13:59 UTC:

```bash
make serving-it-unified
```

Kết quả `PASS`. Log: `docs/proof/logs/phase4b_unified_it.json`.

| Tình huống | Kết quả |
|---|---|
| Batch `it_it_af_real`, trending `it_e2e` | Overview `session_count` 662830. Top rated movie 63, `avg_rating` 4.05488, `rating_count` 82. Most watched movie 42, `watch_seconds` 2628418172, `session_count` 504009. Trending movie 42: 7 heartbeat, 70 giây, peak 5. `freshness=stale`, `live=false`. Status `batch_database=it_it_af_real`, `trending_database=it_e2e`, `consistent=true` |
| Trending trỏ `it_dash_absent_stream` | Overview 200, 662830 phiên. `/movies/trending` 503. Status 200, `trending_freshness=sync_failed`, `trending_live=false` |
| Batch trỏ `it_dash_absent_batch` | Overview 503. Trending 200, movie 42, `freshness=stale`. Status 200, `consistent=false`, `trending_database=it_e2e` |
| Batch `it_dash_conflict` trộn snapshot 101 và 202 | Overview, top-rated và most-watched 409. Trending 200. Status 200, `consistent=false`. Database này bị `DROP` sau test |
| ClickHouse `127.0.0.1:9` | Cả năm endpoint 503, gồm `/serving/status` |
| Production | Snapshot `silver.stg_watch_sessions` `1975472564804901513`, `bronze.raw_users` `7954921826504091397`. `stg_playback_heartbeats` và `stg_playback_windows` vẫn HTTP 404. `it_it_af_real.movie_engagement` 662830, `movie_ratings` 100, `sync_log` 1. `it_e2e.movie_windows` 2. `cinestream_serving` không có part active trước và sau test |

Số xếp hạng trong bảng trên được đối chiếu bằng `SELECT` đọc trực tiếp ClickHouse, cùng thứ tự với API.

Ảnh một màn hình: `docs/proof/images/phase4b_unified.png` và `docs/proof/images/phase4b_trending.png`.

## Giới hạn

- Cửa sổ `it_e2e` có `synced_at` hơn 180 giây so với lúc mở trang, nên nhãn đúng là `stale`. Dữ liệu cũ không được gắn `live`.
- `cinestream_serving` chưa có dòng. Production streaming vẫn tắt.
- `/ready` chỉ kiểm tra hai reader table của batch. Trending lỗi không làm `/health` thành lỗi.
