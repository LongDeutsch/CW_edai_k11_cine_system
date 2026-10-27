# 18 — DataHub chiếu DP1, DP2, DP3

**Phạm vi.** Lệnh emit payload IT, run `manual__2026-05-01T00:00:00+00:00`, namespace `it_af_dp`. GMS `http://127.0.0.1:18083`. Ảnh lần này là `images/datahub_dp*_*.png`.

**Trong phạm vi này.** File này giữ lệnh emit IT. Catalog gốc, cạnh không được chiếu, task `publish_datahub`, prefix `it_af_dp` và cả hai bộ ảnh nằm ở file 20.

**Đọc tiếp.** [Tour](00_system_tour.md) · [20 — DataHub](20_datahub_lineage.md)

```mermaid
flowchart LR
  validate[validate IT] --> emit[emit it_af_dp]
  emit --> gms[GMS]
```

Mạch đọc nằm ở [20_datahub_lineage.md](20_datahub_lineage.md).

Lệnh chiếu payload của lần DP IT (`ready`, counts khớp, orphan bằng 0, `user_rows == user_distinct`):

```bash
python -m governance.datahub.emit \
  --dag-id cinestream_dp1_raw_to_bronze \
  --run-id manual__2026-05-01T00:00:00+00:00 \
  --gms-url http://127.0.0.1:18083 \
  --result-json '{"ready": true, "datasets": [{"dataset": "users", "error_rate": 0.0}, {"dataset": "movies", "error_rate": 0.0}, {"dataset": "ratings", "error_rate": 0.0}, {"dataset": "watch", "error_rate": 0.0}], "violations": [], "reason": "bronze đạt hợp đồng"}'
```

DP2 và DP3 dùng cùng `--run-id` với dict `validate_gold` và `validate_features`. Namespace `it_af_dp` đổi tên dataset thành `cinestream.it_af_dp.*` để không đè tên production. Ảnh của lần này gắn trong file 20.
