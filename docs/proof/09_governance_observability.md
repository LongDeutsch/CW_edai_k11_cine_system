# 09 — Metadata drift và quan sát pipeline

Phase 5B đọc code tại Git `4e90eda009a4498c198fa399f9c75c47919eee86` và đối chiếu cụm bằng GET/SELECT. Không ALTER, không DROP, không submit Flink, không bật `CINESTREAM_ALLOW_STREAMING`.

```bash
PYTHONPATH=. uv run python -m unittest tests.governance.test_lineage tests.governance.test_observe
PYTHONPATH=. uv run --extra minio python -m governance.collect
```

Unittest: 22 test, OK. Collector ghi `docs/proof/logs/phase5b_observability.json`. Hai target Make tương ứng là `governance-check` và `governance-observe`.

Log `phase5a_governance_audit.json` chỉ là lần đọc trước. Kết luận bên dưới lấy từ JSON 5B.

## Metadata nào là tĩnh, nào là đọc live

| Nguồn | Vai trò trong 5B |
|---|---|
| `governance/inventory.py` và DDL/writer | Schema kỳ vọng, lineage, owner. Tĩnh theo code. |
| Iceberg REST `127.0.0.1:8181` | Schema vật lý, snapshot, `total-records`, thời điểm commit, partition spec, 65 bảng `silver.it_*`, danh sách namespace. |
| Airflow `127.0.0.1:18082` | 100 dag run mới nhất của `cinestream_daily_batch_lakehouse`, task của run production mới nhất, run fail mới nhất, và run fail production mới nhất. |
| MinIO marker mới nhất dưới `_orchestration/bronze/` | `published`, `batch_id`, `run_id`. File này không có `gold_snapshots`. |
| ClickHouse `127.0.0.1:18123` | Part active của `cinestream_serving` và database `it_%`, số snapshot/batch trên reader production. |
| Flink `127.0.0.1:18081` `/overview` | Job đang chạy. Job cancelled lịch sử không được dùng làm sức khỏe hiện tại. |

Catalog REST hiện có `bronze`, `silver`, `gold` và 10 namespace `it_af_backfill`, `it_af_badts`, `it_af_contract`, `it_af_ok`, `it_af_real`, `it_af_real_retry`, `it_af_retry`, `it_api_gold`, `it_audit_gold`, `it_ch_gold`.

## Drift schema

So sánh cột DDL cộng cột writer với schema Iceberg hiện tại. Không sửa cột.

Quy tắc phân loại:

- Thiếu cột kỳ vọng: `breaking`.
- Cột vật lý thừa, kể cả `dt`: `manual_review`. Reader hiện chọn cột theo tên. Lần overwrite sau có thể đổi schema.
- Kiểu không cùng họ, ví dụ string với long: `breaking`.
- `DECIMAL` lệch precision hoặc thành `double`: `manual_review`.
- `TIMESTAMP` trong DDL khớp `timestamptz`. `timestamp` không timezone: `manual_review`.
- Vật lý required trong khi DDL optional: `compatible`. DDL `NOT NULL` nhưng vật lý optional: `manual_review`.
- Phân loại bảng là mức nặng nhất trong các finding.

Không có finding `breaking` và không có cột kỳ vọng nào bị thiếu. Tám bảng lệch đều là `manual_review`. Partition spec của cả tám bảng rỗng, nên `dt` là cột dữ liệu, không phải field partition Iceberg.

| Bảng | Finding chính |
|---|---|
| `silver.stg_ratings` | Thêm `dt DATE` optional. `rating_val` là `double`, DDL là `DECIMAL(2, 1) NOT NULL`. `rating_ts` là `timestamp` không timezone. Các khóa và `event_date` optional trong khi DDL required. Snapshot `8795824390230744395`, 57665 dòng, commit `2026-10-16T15:45:17Z`. |
| `silver.stg_watch_sessions` | Thêm `dt DATE` optional. `start_ts` và `end_ts` là `timestamp` không timezone. Khóa, duration, completion, `event_date` optional. Snapshot `1975472564804901513`, 662830 dòng, commit `2026-10-16T15:45:20Z`. |
| `silver.stg_movies` | `movie_id` optional. `created_ts` là `timestamp` không timezone. Snapshot `8380250790601157578`, commit `2026-10-16T15:45:18Z`. |
| `gold.dim_movies` | `movie_sk`, `movie_id`, `is_current` optional. `valid_from_ts` là `timestamp` không timezone. Snapshot `1217011260721305647`, commit `2026-10-16T15:45:22Z`. |
| `gold.dim_users` | `user_sk`, `user_id`, `subscription_tier`, `valid_from_ts`, `is_current` optional. `valid_from_ts` vẫn là `timestamptz`. |
| `gold.fact_ratings` | `rating_val` giữ `decimal(2, 1)` nhưng optional. `created_ts` là `timestamp` không timezone. Khóa và `rating_date_key` optional. |
| `gold.fact_watch_sessions` | Khóa, duration, completion, `session_date_key` optional. Không lệch kiểu. |
| `gold.obt_movie_engagement` | `session_sk`, `session_date_key`, `user_id`, `movie_id` optional. Không lệch kiểu. |

`stg_tables.sql` và `publish()` hiện không khai báo `dt`. `dt=YYYY-MM-DD` trên MinIO là thư mục file nguồn. Bronze `raw_ratings` và `raw_watch_history` không có cột `dt`, nên drift này nằm ở Silver đã commit ngày 16/10. `_batch_id` và `_file_id` trên Bronze có trong writer nên không vào báo cáo drift.

`gold.fact_ratings.rating_val` đã là `decimal(2, 1)`. Lệch `double` dừng ở Silver. Consumer Gold đang đọc decimal, consumer đọc thẳng `stg_ratings` thì thấy double.

Nullability optional trên hàng loạt cột `NOT NULL` là lệch có hệ thống của schema vật lý so với DDL. Reader theo tên cột vẫn thấy cột. Đây là việc cần review trước lần ghi đè schema, không phải cột mất.

## Trạng thái dataset

46 mục inventory. `failure` chỉ bật với `missing_unexpected` và `populated_stale`. Lần đọc này có 0 failure theo hai trạng thái đó.

| Trạng thái | Số mục | Ý nghĩa lần đọc này |
|---|---|---|
| `populated` | 14 | Có snapshot và `total-records` > 0. |
| `declared` | 26 | Khai báo trong code. Không phải bảng Iceberg: MinIO, Kafka, ClickHouse, API, dashboard, DAG, job Flink. |
| `expected_absent` | 2 | `stg_playback_heartbeats`, `stg_playback_windows`. Writer bị cổng streaming. Streaming đang tắt, REST 404. |
| `declared_empty` | 2 | `feat_user_movie_stats_daily`, `feat_movie_engagement_stats_daily`. Có trong catalog, snapshot `-1`. DDL có, `publish()` không ghi. |
| `empty_committed` | 1 | `bronze.quarantine_records`, snapshot `8494360816768729791`, 0 dòng, commit `2026-10-18T06:35:11Z`. |
| `integration_only` | 1 | Nút `integration.resources`. 65 bảng `it_*` nằm trong báo cáo dọn, không nối vào lineage production. |

`clickhouse.movie_engagement` ở trạng thái `declared` vì inventory không coi nó là bảng Iceberg. Phần serving bên dưới mới là kết luận về `cinestream_serving`.

Snapshot và số dòng của bảng production đã commit:

| Bảng | Snapshot | Dòng | Commit |
|---|---|---|---|
| `raw_users` | `7954921826504091397` | 5000 | `2026-10-18T06:33:51Z` |
| `raw_movies` | `5393514282830386405` | 100 | `2026-10-18T06:34:12Z` |
| `raw_ratings` | `5741429269183032913` | 256410 | `2026-10-18T06:34:20Z` |
| `raw_watch_history` | `3906642207194326372` | 820513 | `2026-10-18T06:34:34Z` |
| `quality_results` | `4647764305794695491` | 8 | `2026-10-18T06:35:12Z` |
| `stg_users` | `5696626651778242204` | 5000 | `2026-10-18T06:35:22Z` |
| `dim_users` | `4373803545286674709` | 5010 | `2026-10-18T06:35:33Z` |
| `fact_ratings` | `1324146680817977793` | 57665 | `2026-10-18T06:35:38Z` |
| `fact_watch_sessions` | `4699535929651689042` | 662830 | `2026-10-18T06:35:51Z` |
| `obt_movie_engagement` | `6121133416268278903` | 662830 | `2026-10-18T06:36:03Z` |

`stg_movies`, `stg_ratings`, `stg_watch_sessions` và `dim_movies` vẫn là snapshot commit `2026-10-16T15:45Z`. Run ngày 18/10 đã commit các bảng ở bảng trên. Bốn snapshot cũ hơn không được ghi lại trong run đó. Trạng thái của chúng vẫn là `populated` vì marker không ghim snapshot id để so.

## Sức khỏe và độ tươi

`success_inferred_from_snapshot` là false. Có snapshot hoặc có bảng không được tính là run thành công.

Airflow, run mới nhất trước:

- Run mới nhất trong cửa sổ 100 run là `success` và là run integration. `latest_is_production` false.
- Run production mới nhất là `manual__2026-10-18T06:26:38+00:00`, state `success`. Task thành công: `check_source_ready`, `spark_raw_to_bronze`, `validate_bronze`, `spark_bronze_to_gold`, `validate_gold`, `publish_success`. Không có `sync_clickhouse`.
- `production_success_implies_current_data` false.
- 29 run failed trong cửa sổ. Run failed mới nhất là `manual__2026-10-18T09:17:33.303012+00:00`, integration. Các task ở `failed` hoặc `upstream_failed` gồm `spark_raw_to_bronze`, `validate_bronze`, `spark_bronze_to_gold`, `validate_gold`, `publish_success`, `sync_clickhouse`.
- Run production failed mới nhất là `manual__2026-10-18T06:24:00+00:00`. Các task cùng hai trạng thái đó: `check_source_ready`, `spark_raw_to_bronze`, `validate_bronze`, `spark_bronze_to_gold`, `validate_gold`, `publish_success`. Báo cáo không chọn một task gốc từ danh sách gộp này.

Marker `_orchestration/bronze/batch-a3f18e1ea671765052443d7ef5608212.json` thuộc đúng run production success, `published` true. Code DAG hiện tại có `_gold_snapshots`, nhưng file marker của run này không có khóa đó. `snapshot_pins` false. Độ tươi batch là `published_without_snapshot_pins`. Ba bảng Gold có dòng chưa đủ để gọi là khớp marker.

Serving production là `not_run`. `cinestream_serving` có 0 dòng active, `uniqExact` snapshot của `movie_engagement` và `movie_ratings` là 0, `same_batch` là null. Run production success không chứa task sync, nên trống này không bị gọi là `sync_failed`.

Database ClickHouse còn part active: `it_api`, `it_audit`, `it_ch`, `it_e2e`, `it_it_af_real`, `it_it_af_real_retry`, `it_ops`, `it_trend`. Đó là dữ liệu integration.

Flink 1.18.1: `jobs-running` 0, `jobs-failed` 0, `jobs-cancelled` 1. Checkpoint hiện tại là `no_active_job`. Bộ đếm cancelled là lịch sử. `CINESTREAM_ALLOW_STREAMING` đang tắt. Độ tươi streaming là `disabled`.

Độ tươi batch không dùng tuổi sự kiện MovieLens. Tuổi event chỉ áp vào quan sát streaming khi có `event_age_seconds`.

## Ứng viên dọn trong namespace Silver

65 bảng tên `it_*` nằm trong namespace `silver`. Không xóa bảng, metadata Iceberg, topic Kafka hay checkpoint.

Cách phân loại: namespace production (`bronze`, `silver`, `gold`) và tên bắt đầu bằng `it_`. `keep_referenced` khi đúng chuỗi tên xuất hiện trong file `.py` hoặc `.sql` dưới `streaming`, `serving`, `orchestration`, `tests`, `transformation`. Còn lại là `cleanup_candidate`. Tìm chuỗi có thể giữ nhầm một tên ngắn nếu tên đó nằm trong một identifier dài hơn.

36 ứng viên, 29 bảng còn được source nhắc. Commit trải từ bảng chưa có snapshot tới `2026-10-18T11:34:23Z`. Tuổi một ngày không đủ để xóa.

| Disposition | Bảng | Dòng | Commit |
|---|---|---|---|
| cleanup_candidate | `it_p05_sessions` | — | snapshot `-1` |
| cleanup_candidate | `it_p05_windows` | — | snapshot `-1` |
| cleanup_candidate | `it_p05b_sessions` | 6 | `2026-10-17T04:42:51Z` |
| cleanup_candidate | `it_p05b_windows` | 0 | `2026-10-17T04:42:31Z` |
| cleanup_candidate | `it_p05c_sessions` | 6 | `2026-10-17T04:44:26Z` |
| cleanup_candidate | `it_p05c_windows` | 4 | `2026-10-17T04:44:26Z` |
| cleanup_candidate | `it_p05d_sessions` | 6 | `2026-10-17T06:01:40Z` |
| cleanup_candidate | `it_p05d_windows` | 4 | `2026-10-17T06:01:40Z` |
| cleanup_candidate | `it_p1b_heartbeats` | 8 | `2026-10-17T06:37:20Z` |
| cleanup_candidate | `it_p1b_windows` | 11 | `2026-10-17T06:37:10Z` |
| cleanup_candidate | `it_p1c_heartbeats` | 8 | `2026-10-17T07:29:58Z` |
| cleanup_candidate | `it_p1c_windows` | 9 | `2026-10-17T07:29:48Z` |
| cleanup_candidate | `it_p25_heartbeats` | 5 | `2026-10-17T08:32:15Z` |
| cleanup_candidate | `it_p25_windows` | 7 | `2026-10-17T08:31:25Z` |
| cleanup_candidate | `it_p25b_heartbeats` | 5 | `2026-10-17T08:44:12Z` |
| cleanup_candidate | `it_p25b_windows` | 7 | `2026-10-17T08:43:19Z` |
| cleanup_candidate | `it_p25c_heartbeats` | — | snapshot `-1` |
| cleanup_candidate | `it_p25c_windows` | — | snapshot `-1` |
| cleanup_candidate | `it_p25d_heartbeats` | 5 | `2026-10-17T11:07:42Z` |
| cleanup_candidate | `it_p25d_windows` | 7 | `2026-10-17T11:06:38Z` |
| cleanup_candidate | `it_p2_recbench1_heartbeats` | 300 | `2026-10-17T15:50:55Z` |
| cleanup_candidate | `it_p2_recbench1_windows` | 4 | `2026-10-17T15:50:55Z` |
| cleanup_candidate | `it_p2_recbench2_heartbeats` | 300 | `2026-10-17T15:51:16Z` |
| cleanup_candidate | `it_p2_recbench2_windows` | 4 | `2026-10-17T15:51:16Z` |
| cleanup_candidate | `it_p2_recgap_heartbeats` | 6 | `2026-10-17T15:49:20Z` |
| cleanup_candidate | `it_p2_recgap_windows` | 1 | `2026-10-17T15:49:20Z` |
| cleanup_candidate | `it_p2_recidle_heartbeats` | 3 | `2026-10-17T15:39:15Z` |
| cleanup_candidate | `it_p2_recidle_windows` | 1 | `2026-10-17T15:38:32Z` |
| cleanup_candidate | `it_p2_recnorm_heartbeats` | 12 | `2026-10-17T15:50:29Z` |
| cleanup_candidate | `it_p2_recnorm_windows` | 3 | `2026-10-17T15:50:19Z` |
| cleanup_candidate | `it_p2_recp1_heartbeats` | 11 | `2026-10-17T15:36:29Z` |
| cleanup_candidate | `it_p2_recp1_windows` | 3 | `2026-10-17T15:33:55Z` |
| cleanup_candidate | `it_p2_recslow_heartbeats` | 6 | `2026-10-17T15:37:44Z` |
| cleanup_candidate | `it_p2_recslow_windows` | 1 | `2026-10-17T15:37:44Z` |
| cleanup_candidate | `it_playback_windows` | — | snapshot `-1` |
| cleanup_candidate | `it_watch_sessions` | — | snapshot `-1` |
| keep_referenced | `it_p1_heartbeats` | 8 | `2026-10-17T06:07:51Z` |
| keep_referenced | `it_p1_windows` | 11 | `2026-10-17T06:07:51Z` |
| keep_referenced | `it_p25e_heartbeats` | 5 | `2026-10-17T11:25:51Z` |
| keep_referenced | `it_p25e_windows` | 7 | `2026-10-17T11:24:55Z` |
| keep_referenced | `it_p2_conflict` | 2001 | `2026-10-17T08:00:49Z` |
| keep_referenced | `it_p2_e2e_heartbeats` | 8 | `2026-10-18T11:34:23Z` |
| keep_referenced | `it_p2_e2e_windows` | 3 | `2026-10-18T11:34:23Z` |
| keep_referenced | `it_p2_heartbeats` | 6 | `2026-10-17T07:51:28Z` |
| keep_referenced | `it_p2_ops_heartbeats` | 6 | `2026-10-18T10:38:03Z` |
| keep_referenced | `it_p2_ops_windows` | 2 | `2026-10-18T10:38:03Z` |
| keep_referenced | `it_p2_replace` | 1 | `2026-10-17T08:00:39Z` |
| keep_referenced | `it_p2_trending_empty` | — | snapshot `-1` |
| keep_referenced | `it_p2_trending_heartbeats` | 10 | `2026-10-18T09:57:47Z` |
| keep_referenced | `it_p2_trending_windows` | 4 | `2026-10-18T09:57:47Z` |
| keep_referenced | `it_p2_tumbling_heartbeats` | 8 | `2026-10-17T13:50:17Z` |
| keep_referenced | `it_p2_tumbling_windows` | 3 | `2026-10-17T13:49:33Z` |
| keep_referenced | `it_p2_valhold_heartbeats` | 3 | `2026-10-17T14:17:01Z` |
| keep_referenced | `it_p2_valhold_windows` | — | snapshot `-1` |
| keep_referenced | `it_p2_validle_heartbeats` | 3 | `2026-10-17T14:18:24Z` |
| keep_referenced | `it_p2_validle_windows` | 1 | `2026-10-17T14:17:48Z` |
| keep_referenced | `it_p2_valp1_heartbeats` | 10 | `2026-10-17T14:16:00Z` |
| keep_referenced | `it_p2_valp1_windows` | 3 | `2026-10-17T14:14:20Z` |
| keep_referenced | `it_p2_windows` | 9 | `2026-10-17T07:51:28Z` |
| keep_referenced | `it_p2_wm1_heartbeats` | 5 | `2026-10-17T07:59:43Z` |
| keep_referenced | `it_p2_wm1_windows` | 4 | `2026-10-17T07:59:43Z` |
| keep_referenced | `it_p2_wm2_heartbeats` | 4 | `2026-10-17T08:00:02Z` |
| keep_referenced | `it_p2_wm2_windows` | 2 | `2026-10-17T08:00:32Z` |
| keep_referenced | `it_p2b_heartbeats` | 5 | `2026-10-17T07:53:09Z` |
| keep_referenced | `it_p2b_windows` | 7 | `2026-10-17T07:51:49Z` |

Namespace `it_*` và database ClickHouse `it_*` đã tách khỏi production. Chỗ còn trộn là bảng test nằm trong chính `silver`.

Chính sách cho lần sau: test Iceberg tạo namespace `it_*` riêng, không `CREATE TABLE` trong `silver`. Một đợt dọn sau này chỉ xét bảng `cleanup_candidate`, có snapshot hoặc snapshot `-1`, sau khi dry-run liệt kê metadata MinIO. Bảng `keep_referenced` chờ test bỏ tên cứng. Phase này không thực hiện đợt đó.

## Tác động lineage

Cạnh lấy từ graph Phase 5A. Không thêm cạnh. `manual_review` và bảng stale hoặc thiếu ngoài ý muốn có effect `downstream_review`. Bảng streaming vắng vì cổng tắt có effect `disabled_path`. Feature không có cạnh nên effect `none`.

`stg_ratings` chạm 13 hậu duệ: `fact_ratings`, marker, stage và reader `movie_ratings` / `movie_engagement`, `sync_log`, overview, top-rated, most-watched, serving status, ready, dashboard. Không có `movie_windows` và không có bảng feature.

`stg_watch_sessions` chạm thêm `fact_watch_sessions` và `obt_movie_engagement`, rồi cùng nhóm serving batch và dashboard. Không có nhánh trending.

`obt_movie_engagement` chạm marker, hai reader batch, `sync_log`, các route batch và dashboard.

`fact_watch_sessions` có drift nullability và 0 hậu duệ. OBT không đọc bảng fact watch. Lệch nằm trên bảng này cho đến khi có consumer mới.

`stg_playback_windows` có 7 hậu duệ trên đường trending: stage cửa sổ, `movie_windows`, worker status, trending sync log, route trending, serving status, dashboard. Effect là `disabled_path`. `stg_playback_heartbeats` không có consumer serving trong graph.

Hai bảng feature có 0 hậu duệ.

## Test

`tests/governance/test_observe.py` cùng `test_lineage.py`: 22 test, OK.

Các ca 5B dùng fixture, không phụ thuộc cụm:

- Cột `dt` thừa là `manual_review`, không phải `breaking`.
- Thiếu cột là `breaking`. Cột thừa khác cũng là `manual_review`.
- String so với long là `breaking`. Decimal thành double là `manual_review`.
- Required chặt hơn DDL là `compatible`.
- `stg_playback_windows` vắng và streaming tắt là `expected_absent`, effect `disabled_path`, hậu duệ có `movie_windows`.
- Feature snapshot `-1` là `declared_empty`, không có hậu duệ, không phải failure.
- Snapshot Gold lệch marker là `populated_stale`. Hậu duệ OBT có `movie_engagement` và không có `movie_windows`.
- Event time quá ngưỡng trên bảng có dòng là `populated_stale`.
- Run integration success mới hơn không che run production failed.
- Serving lệch snapshot hoặc lỗi kết nối là `sync_failed`.
- Task production không có `sync_clickhouse` và 0 dòng là `not_run`.
- Ba bảng Gold populated mà marker không ghim id là `published_without_snapshot_pins`.
- Bảng `it_*` trong `silver` thành ứng viên hoặc `keep_referenced`. Bảng production và namespace `it_*` riêng không vào danh sách.
- Hậu duệ `stg_ratings` gồm `fact_ratings`, top-rated và dashboard, không gồm cửa sổ trending hay feature.

## Giới hạn

Collector không so từng cột ClickHouse. `cinestream_serving` không có part active nên không có schema serving production để lệch. Database `it_*` không được đem ra làm schema production.

Checkpoint của job đã cancel không được đọc. `/jobs/overview` không có job hiện tại.

Marker của run success không chứa snapshot id. Độ tươi batch dừng ở `published_without_snapshot_pins`. Bốn snapshot commit ngày 16/10 được báo riêng bằng `committed_at`, chưa bị gắn `populated_stale`.

`keep_referenced` là tìm chuỗi trong năm cây source. Nó không đọc notebook, XCom hay object MinIO.

Airflow chỉ kéo task của bốn run cần xem. Danh sách task failed gộp `failed` và `upstream_failed`, nên không chỉ ra task gốc của run `manual__2026-10-18T06:24:00+00:00`.

## Khuyến nghị

Giữ nguyên cổng streaming và không copy `it_it_af_real` hay `it_e2e` vào `cinestream_serving`.

Trước lần `publish()` đè Silver hoặc Gold, review cột `dt`, `stg_ratings.rating_val` kiểu double, và các cột `timestamp` không timezone. Chưa ALTER và chưa DROP cột trong phase này.

Run production tiếp theo, nếu dùng DAG hiện tại, sẽ ghi `gold_snapshots` vào marker. Khi đó mới so snapshot id với ba bảng `dim_movies`, `fact_ratings`, `obt_movie_engagement`. `sync_clickhouse` chỉ có nghĩa sau một run production có task đó.

Test mới dùng namespace Iceberg `it_*` và database ClickHouse `it_*`. 36 ứng viên trong `silver` chờ một đợt dọn có dry-run. Chưa cài DataHub, Feast hay Prometheus.

Phase 5B dừng ở báo cáo và test này.
