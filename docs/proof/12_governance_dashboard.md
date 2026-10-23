# 12 — Governance dashboard

Phase 5E thêm API chỉ đọc và một trang lineage trên dashboard hiện có. Không ALTER, không ghi marker, không bật streaming, không thêm DataHub hay Feast.

`git rev-parse HEAD` lúc đọc live là `f089b7c9d5de0808396d0597d7e3fb9851415eb2`. Code của phase này nằm trên working tree, chưa nằm trong commit đó. API không đọc `docs/proof/logs/phase5a`–`phase5d` để kết luận sức khỏe.

## Chạy và dừng

```bash
make serving-api
make serving-dashboard
```

API ở `http://127.0.0.1:18083`. Dashboard ở `http://127.0.0.1:18084`. Trang governance là `http://127.0.0.1:18084/governance.html`. Analytics và governance trỏ sang nhau. Dừng bằng Ctrl+C ở hai tiến trình đó. Không `docker compose down`.

`make serving-api` cài `boto3` cùng FastAPI để route governance đọc marker trên MinIO. Route analytics vẫn chỉ đọc ClickHouse.

```bash
make governance-check
make governance-dashboard-it
```

`governance-check` chạy unittest thuần. `governance-dashboard-it` thêm FastAPI và httpx để gọi TestClient.

## API

Mọi route dưới `/governance` là GET. POST, PUT và DELETE trả 405.

| Route | Nội dung |
|---|---|
| `/governance/snapshot` | Catalog, cạnh, drift, health, integration |
| `/governance/datasets` | Lọc `q`, `environment`, `status`, `zone` |
| `/governance/datasets/{id}` | Một dataset, schema kỳ vọng và schema vật lý |
| `/governance/lineage` và `/governance/lineage/{id}` | Cạnh kèm `path` và `token` |
| `/governance/drift` | Finding drift, hoặc `findings: null` khi catalog chưa đọc |
| `/governance/health` | Pipeline, freshness, serving, Flink, marker |
| `/governance/impact/{id}` | Dataset phía dưới theo cạnh đã có |

Cạnh lấy từ inventory code. Không thêm cạnh `fact_watch_sessions` → `obt_movie_engagement`. `fact_watch_sessions` không có cạnh xuống. `stg_ratings` vẫn đi tới `fact_ratings` và `api.movies.top_rated`, không đi tới `movie_windows` hay bảng feature.

`historical_proof_used_as_current_health` luôn `false`. `success_inferred_from_snapshot` luôn `false`. Run integration thành công không bị ghi thành pipeline production đang khỏe. Job Flink cancelled trong quá khứ không được đưa vào health khi Flink chưa đọc được.

## Lần đọc live

Docker daemon không chạy. Iceberg, Airflow, ClickHouse và Flink đều `connection refused`. Tiến trình API của lần đọc này chưa có boto3, nên marker là `unavailable` với lỗi `boto3 không có`, không phải “không có marker”.

`docs/proof/logs/phase5e_governance_live.json` ghi lại lần đó:

- 46 dataset trong inventory. 28 `unknown` (Iceberg và ClickHouse). 18 `declared` (nguồn, API, dashboard, Airflow, Flink, `integration.resources`).
- Drift `unavailable`, `findings` là `null`.
- Danh sách bảng `it_*` `unavailable`, `count` là `null`.
- Freshness batch `unavailable`. Streaming `disabled` vì `CINESTREAM_ALLOW_STREAMING` không bật.
- `stg_playback_windows` có pipeline `disabled` và status `unknown`. Catalog chưa đọc nên không gọi là `expected_absent`.
- Serving `unavailable`, không phải `not_run` hay 0 dòng.

Trang `http://127.0.0.1:18084/governance.html` hiện banner unavailable, vẫn liệt kê 46 dataset, vẽ 46 nút và 64 cạnh. Tìm `fact_watch` còn một dòng. Lọc integration còn `integration.resources`. Schema vật lý của dataset Iceberg hiện `unknown`, cột kỳ vọng vẫn lấy từ DDL. Analytics ở `/` vẫn mở được và báo ClickHouse connection refused. Link Governance nằm trên trang đó.

## Test

10 test trong `tests/governance/test_governance_api.py` đều chạy khi có FastAPI và httpx. Không có FastAPI thì hai test route bị bỏ, tám test payload và UI vẫn chạy. Cả bộ governance là 51 test, 2 test route bị bỏ trên interpreter chưa cài FastAPI.

Các ca khóa: catalog chưa đọc không thành danh sách rỗng; cạnh khớp evidence; lọc production/integration/status; `dt` thừa giữ `kind=extra`, nguồn DDL và `iceberg-rest`, `repartition=false`; playback tắt là `expected_absent` chỉ khi catalog đã đọc và bảng vắng; serving là `unknown` khi chưa đọc task Airflow; module governance không ghi file và không mở log phase trước; lỗi fetch của trang không gán nhãn 0 dataset.

## Lineage explorer

Bản đầu vẽ toàn bộ catalog trên một SVG. Nhãn bị cắt và cạnh của dataset khác chạy ngang dataset đang chọn.

Explorer hiện chỉ vẽ dataset đang chọn cùng các cạnh mà API trả về. Ba chế độ: Upstream (cạnh vào trực tiếp), Downstream (cạnh ra trực tiếp), Full path (đi tiếp theo những cạnh đó). Cột từ trái sang phải là Raw, Bronze, Silver, Gold, Serving, API. Mũi tên luôn kết thúc ở dataset downstream. Tên đầy đủ nằm trong tooltip `title` và danh sách evidence `path: token` dưới đồ thị. Khung có thanh cuộn để kéo và nút phóng to.

Mỗi cạnh có một loại lấy từ evidence, không suy từ tên bảng:

| Loại | Evidence | Cách vẽ |
| --- | --- | --- |
| data flow | Job ghi dòng hoặc cột sang bảng sau, gồm `replace_quarantine` vì bảng đó lưu `raw_payload` | nét liền nâu |
| control | DAG ghi `gold_snapshots` vào marker; `read_marker` chỉ chọn snapshot | nét đứt xám |
| observability | `quality_results`, `sync_log`, `trending_sync_log`, `trending_worker_status`, `/ready`, `/serving/status` | nét chấm xanh |
| api | Endpoint hoặc `app.js` đọc bảng nghiệp vụ | nét liền xanh |

`fact_ratings → movie_ratings_stage` là data flow vì `_rating_rows`. `fact_ratings → orchestration_marker` là control vì marker chỉ giữ snapshot id. `/ready` kiểm tra bảng tồn tại, nên không được vẽ như biến đổi dữ liệu.

Click một dòng catalog chọn dataset đó, tô dòng và nút, rồi vẽ lại lineage cùng panel chi tiết. Ô tìm kiếm không đổi dataset đang chọn. Nếu bộ lọc loại dataset đó, chi tiết và đồ thị bị xóa và có câu «không nằm trong bộ lọc», không giữ schema của lần chọn trước. `fact_watch_sessions` không có cạnh xuống, nên downstream hiện «Không có downstream đã kiểm chứng» và không có cạnh tới `obt_movie_engagement`. `integration.resources` và các bảng feature cũng không có cạnh.

Ảnh trước: `docs/proof/images/lineage-before.png`. Ảnh downstream của `fact_ratings` sau khi tách loại cạnh: `docs/proof/images/lineage-after.png`. Nét liền nâu là `_rating_rows`. Nét đứt xám là pin snapshot trong marker.

Lần đọc lúc kiểm tra này là `2026-10-23T07:30:10Z`: health `read`, batch pipeline `disabled`, freshness `published_without_snapshot_pins`, streaming `disabled`, serving `not_run`, 111 dataset. `fact_ratings` snapshot `1324146680817977793`, 57665 dòng. `fact_watch_sessions` snapshot `4699535929651689042`, 662830 dòng, không có downstream và không có cạnh tới `obt_movie_engagement`. Playback `expected_absent` với pipeline `disabled`. `integration.resources` và feature table không có cạnh. Log `phase5e_governance_live.json` là lần đọc khi Docker tắt, không phải sức khỏe của lần này.

## Giới hạn

Phase dừng ở dashboard. Drift production đã ghi ở phase 5D không được sửa. Bảng `it_*` chỉ được đếm tên, không tải metadata từng bảng. Lần đọc đầu, khi Docker tắt, nằm trong `phase5e_governance_live.json` và không được dùng làm sức khỏe hiện tại.
