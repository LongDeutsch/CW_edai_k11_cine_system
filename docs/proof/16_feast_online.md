# 16 — Feast online và materialize theo snapshot

Chạy lúc 2026-10-25. Feast `0.67.0`. Redis `7.4-alpine`, cổng host `16379`, database `1`. HEAD git vẫn `51b22ba`. Phần B2 nằm trên working tree, chưa commit.

Namespace `bronze`, `silver`, `gold` không bị ghi. `cinestream_serving` không đổi. Streaming production không được bật. Không có Kubeflow, MLflow, KServe, và không có số liệu model.

## Catalog của DP3 trên `it_af_dp`

REST Catalog dùng `jdbc:sqlite:file:/tmp/iceberg_catalog.db` và không có volume. Container `cinestream-iceberg-rest-1` được tạo lúc `2026-10-23T17:17:06Z`, sau lần PASS của Phase A2 lúc `2026-10-23T11:09:02Z`. Tạo lại container bỏ metadata trong `/tmp` dù file trên MinIO còn. `make airflow-dp-it` còn gọi `reset_namespaces` và `DROP TABLE ... PURGE` trên `it_af_dp`.

B2 không tạo lại `iceberg-rest` và không reset `it_af_dp`. Lúc kiểm tra, `it_af_dp.raw_users` vẫn snapshot `1062383487765229157`. `gold.feat_user_movie_stats_daily` trả HTTP 404. Snapshot lịch sử `1992904055192348575` và `1720343794407991313` không còn để đọc.

Đường tái lập là một workflow cô lập: DP3 ghi Iceberg, export `VERSION AS OF`, rồi Feast đọc đúng file đó.

## Redis và Feature View

Service Redis nằm ở `infra/compose/docker-compose.feast.yml`. Persistence tắt. Compose không dùng `--remove-orphans`. ClickHouse và FastAPI không đổi.

`ml/feast/feature_store.yaml` trỏ online store tới `localhost:16379,db=1` và `skip_dedup: true`. Trong scheduler, task dùng `redis:6379,db=1`. Entity `user` / `movie` và hai Feature View `user_movie_stats`, `movie_engagement_stats` có `online=True`. `ttl` vẫn là 0 để bài historical của B1 giữ hành vi cũ.

`skip_dedup` bật vì Redis của Feast 0.67 chỉ so `event_timestamp`. Hai bản cùng ngày, khác `created_timestamp`, sẽ không ghi đè nếu dedup còn bật. Thứ tự ghi do sequence của publication quyết định, trước khi có lệnh ghi Redis.

## Materialize

`materialize_incremental` của Feast lấy mốc bắt đầu từ lần chạy trước, hoặc từ `now - ttl` khi ttl là 0. Feature `as_of` nằm trong quá khứ nên cửa sổ đó bỏ sót bản sửa. Workflow này gọi `materialize` trên toàn bộ export đã chấp nhận, `disable_event_timestamp=True`, rồi giữ dòng event mới nhất của từng entity.

`committed.json` chỉ được ghi sau khi `get_online_features` khớp dòng mới nhất của export. `pending.json` được ghi trước lệnh Redis và chỉ xóa sau khi commit.

| Tình huống | Kết quả |
| --- | --- |
| Cùng sequence, cùng snapshot, cùng batch, online đã khớp | `unchanged`, không ghi Redis |
| Cùng publication nhưng online trống sau flush | `reloaded`, identity đã commit giữ nguyên, thêm `reloaded_at` |
| Sequence nhỏ hơn, hoặc cùng sequence nhưng snapshot khác | `StaleExport` trước mọi ghi Redis |
| Lỗi sau khi ghi, trước commit | `MaterializeFailed`, `committed.json` giữ nguyên, `pending.json` còn; chạy lại cùng export thì commit |
| Thiếu `snapshot.json` hoặc Parquet rỗng | thất bại, state đã commit không tiến |

Parquet có `TIMESTAMPTZ_NS` làm DuckDB lỗi khi Feast `CAST` sang `TIMESTAMP`. Trước `feast apply`, bản copy đưa vào online được đổi sang datetime UTC không timezone. Bài historical vẫn đọc file gốc có timezone.

Độ mới của online là dòng có `event_timestamp` mới nhất, rồi `created_timestamp` mới nhất, trong export đã chấp nhận. `get_online_features().to_dict()` của bản này không trả một cột `event_timestamp` riêng. Bằng chứng đổi giá trị là snapshot thứ hai của `it_b2`: user 7 từ 1 rating / 4.0 lên 2 rating / 4.5.

## `file://` namespace `it_b2`

Lệnh là module `ml.online.it_feast_online` với `feast[duckdb,redis]==0.67.0`. Log `docs/proof/logs/phase_b2_feast_online.json`, status PASS, `production_unchanged` true.

| Lần | `feat_user_movie_stats_daily` | `feat_movie_engagement_stats_daily` | Ghi Iceberg | Online |
| --- | --- | --- | --- | --- |
| 1 | `3703638653197485553` | `2086065432481338326` | `overwritten` | `materialized` |
| 2 | `6236898748916454034` | `6473351710346622994` | `overwritten` | `materialized` |
| 3 | cùng snapshot lần 2 | cùng snapshot lần 2 | `unchanged` | `unchanged` |

Export sequence 1 bị từ chối với `sequence 1 cũ hơn 2` sau khi sequence 2 đã lên online. User 7, movie 11 sau snapshot 2: 2 rating, trung bình 4.5, 0.166667 giờ, genre null, movie count 2, completion 0.5, trending 0, movie avg 4.5. User 9 / movie 99 và user 8 / movie 12 trả null.

Test `tests/ml/test_materialize.py` đã chạy trước bài Airflow: nạp lần đầu, chạy lại không đổi, flush Redis rồi nạp lại, sequence mới ghi đè, sequence cũ bị từ chối, lỗi sau ghi không tiến `committed.json`, chạy lại thì commit, thư mục thiếu không tiến state.

Trong lần IT đầu, snapshot `silver.stg_playback_windows` đổi từ `596623653236145611` sang `4656737654278145700` trong lúc job `file://` chạy. Job feature không ghi Silver. Fingerprint PASS bỏ bảng đó. Writer đồng thời là tiến trình đang giữ bảng cửa sổ. B2 không dừng job đó và không ghi bảng đó.

## Airflow DP4

DAG `cinestream_dp4_materialize_online` pause khi tạo, schedule chỉ bật khi production được phép. Sensor `wait_for_dp3` chờ `cinestream_dp3_offline_features.validate_features`, kể cả `upstream_failed`. Task `materialize_online` không nằm trong file monolith.

Scheduler không import Feast vào site-packages của Airflow. Feast `0.67.0` nằm ở `/opt/feast-packages`. Task chạy subprocess `python -m ml.online.run_materialize` với `PYTHONPATH` đó. Tạo lại container scheduler sẽ mất thư mục này vì image Airflow chưa cài Feast.

`assert_publication` đối chiếu `batch_id` của marker DP2, cờ `ready` của DP3, `batch_id` trong conf, và snapshot hiện tại của hai bảng `feat_*`. Namespace `bronze`, `silver`, `gold` bị từ chối.

Lệnh `python -m orchestration.it_feast_airflow` trên namespace `it_af_dp_b2`, logical date `2026-06-01T00:00:00+00:00`, ClickHouse `it_af_dp_b2_ch`. Log `docs/proof/logs/phase_b2_airflow_materialize.json`.

| DAG / task | Trạng thái |
| --- | --- |
| DP1, DP2, DP3 | success |
| `wait_for_dp3` | success |
| `materialize_online` | success, result `materialized` |

`batch_id` là `batch-bc2183e8026ee1ac2ad75370cfb4be58`. Snapshot user `2820089896547319496`, movie `1690044593843706509`. `production_unchanged` và `serving_unchanged` đều true. DAG legacy vẫn pause. DP1–DP4 được pause lại sau bài chạy.

Ảnh: `docs/proof/images/airflow-dp4-run.png`, `docs/proof/images/airflow-dp4-tasks.png`. Lưới Grid đang neo ở ngày chạy `2026-10-25` nên không hiện logical date `2026-06-01`. Hai ảnh Browse cho thấy run và hai task `success`.

Đối chiếu sau run, cùng snapshot Iceberg và `get_online_features` trong scheduler:

| Nguồn | user 7 / movie 11 |
| --- | --- |
| Iceberg `VERSION AS OF` | 1 rating, avg 4.0, 0.166667 giờ, genre null, movie count 1, avg 4.0, completion 0.5, trending 0.166667 |
| Redis | cùng các giá trị |

User 9 / movie 99 và user 8 / movie 12 trả null. Đây là dòng logic một rating 4.0 và một phiên 600 giây, `as_of` `2026-01-20`, `created_timestamp` `2026-01-21T03:00:00`.

## Streaming

`stg_playback_windows` có grain `(movie_id, window_start)`, cửa sổ tumbling một phút, cột `total_watch_seconds`, `heartbeat_count`, `active_users`, chế độ upsert. Nếu đẩy lên Feast, event time nên là `window_end`: một giá trị online cho mỗi `movie_id` là cửa sổ đã đóng mới nhất.

Dedup theo `event_timestamp` của Redis sẽ bỏ một bản sửa cùng cửa sổ vì timestamp bằng nhau. Đó là lý do đường batch bật `skip_dedup` và tự giữ sequence. Phase B3 sau đó sync cửa sổ Flink cô lập vào view riêng. Trạng thái R12 nằm ở `docs/proof/17_feast_streaming.md`.

## Bảo đảm

- Online chỉ nhận export có `snapshot.json`, Parquet đủ cột, và snapshot còn là current của namespace được chỉ định.
- DP4 từ chối khi `batch_id` conf, marker DP2, hoặc export lệch nhau, khi DP3 chưa `ready`, hoặc khi snapshot catalog lệch export.
- Export cũ hơn publication đã commit không ghi đè Redis.
- State chỉ tiến sau khi online khớp export.
- Lần chạy lại cùng publication không ghi Redis khi online đã khớp.

## Giới hạn

- Một writer cho database Redis `1`. Hai materialize song song không được khóa.
- Redis không persist. Restart container làm mất key. Chạy lại cùng export đã commit sẽ `reloaded` nếu file export còn.
- Feast chưa nằm trong image Airflow. `/opt/feast-packages` mất khi tạo lại scheduler.
- `materialize_incremental` không được dùng. Increment ở đây là sequence của publication.
- Chưa có job streaming đẩy feature. Chưa train, chưa Kubeflow, chưa MLflow, chưa KServe.
- Module 2 R11 là VERIFIED, 2.0 điểm. R12 vẫn MISSING. Verified của Module 2 là 4.0 / 100. Module 1 giữ 73%.

Lệnh:

```bash
make feast-redis-up
make feast-online-it
make feast-airflow-it
```

`make feast-online-it` là lệnh gói unit test và `it_feast_online`. Log PASS của file store được ghi bằng module đó trước bài Airflow. Bài Airflow sau đó `FLUSHDB` database 1 rồi nạp export của `it_af_dp_b2`. Dừng tại Phase B2.
