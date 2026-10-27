# 20 — Kích thước image trước và sau multi-stage

**Phạm vi.** Một lần `docker image inspect` trường `Size`, cùng cột `docker images` trên máy đó. Bốn image: Spark, Flink, Airflow, Lakehouse UI. Tag `:single-stage` và `:multi-stage`.

**Trong phạm vi này.** Flink sau multi-stage trên lần này là 1,271,375,322 byte, cột `docker images` 3.15GB. Lần inspect sau đo Flink 1,271,104,760 byte. Hai lần inspect. Cách đọc `Size` và layer dùng chung nằm ở file 22.

**Đọc tiếp.** [Tour](00_system_tour.md) · [22 — Size và layer](22_multistage_images.md)

```mermaid
flowchart LR
  builder[Stage builder] --> run[Image chay]
```

Mạch đọc nằm ở [22_multistage_images.md](22_multistage_images.md). Bảng dưới giữ cột `docker images` của lần đo này.

| Image | Trước (`:single-stage`) | Sau (`:multi-stage`) | `docker images` trước | `docker images` sau |
|---|---:|---:|---:|---:|
| Spark | 842,808,888 | 841,453,810 | 2.24GB | 2.24GB |
| Flink | 1,814,918,517 | 1,271,375,322 | 4.55GB | 3.15GB |
| Airflow | 1,195,486,812 | 1,202,369,880 | 3.91GB | 3.95GB |
| Lakehouse UI | 43,551,511 | 43,551,518 | 203MB | 203MB |
