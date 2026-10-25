# Feast offline cho hai bảng feature

Feast `0.67.0`, cài bằng `feast[duckdb,redis]`. Offline store là DuckDB. Online store trong `ml/feast/feature_store.yaml` là Redis `localhost:16379,db=1`. Cả hai Feature View có `online=True` và `ttl` bằng 0. Test lịch sử ghi đè file yaml bằng SQLite để chạy không cần Redis. Giá trị online và DAG DP4 nằm ở `docs/proof/16_feast_online.md`.

## Vì sao không đọc Iceberg REST trực tiếp

`IcebergSource` của bản 0.67 có `endpoint` và `warehouse`, không có `snapshot_id`. DuckDB đọc bảng bằng `table.scan().to_arrow()`, tức snapshot current của catalog. DP3 cần đúng snapshot đã publish. REST Catalog vì vậy không phải nguồn của Feature View.

Export ghi đè một file Parquet từ `VERSION AS OF snapshot_id`, rồi ghi `snapshot.json` kèm snapshot id dạng chuỗi và `batch_id`. Ghi lại cùng nội dung không thêm dòng. Namespace `bronze`, `silver`, `gold` và warehouse `s3` bị từ chối.

## Đối tượng trong repo

| Đối tượng | Khóa / cột thời gian | Owner |
| --- | --- | --- |
| Entity `user` | `user_id` | `transformation.batch.jobs.build_offline_features` |
| Entity `movie` | `movie_id` | cùng job |
| Feature View `user_movie_stats` | `event_timestamp`, `created_timestamp` | cùng job |
| Feature View `movie_engagement_stats` | `event_timestamp`, `created_timestamp` | cùng job |

Cột feature trùng `storage/lakehouse/gold/gold_tables.sql`. `ttl` là 0: bản DuckDB/ibis này bỏ hạn dùng khi ttl là giá trị falsy, nên dòng cũ vẫn được chọn nếu đó là `event_timestamp` mới nhất còn hiệu lực.

`get_historical_features` mặc định `filter_by_created_timestamp=False`. Khi đó `created_timestamp` chỉ phá hòa, và bản ghi chưa được ghi tại thời điểm entity vẫn có thể lọt vào. `historical_features` luôn bật bộ lọc: `created_timestamp <= event_timestamp` của entity, rồi lấy event mới nhất và created mới nhất.

## Lệnh

```bash
make feast-offline-it
```

Lệnh này chạy `tests/ml/test_feast_offline.py` rồi `ml/offline/it_feast_snapshot.py`. Log: `docs/proof/logs/phase_b1_feast_offline.json`.
