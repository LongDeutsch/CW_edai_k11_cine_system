# 20 — Kích thước image trước và sau multi-stage

Bốn Dockerfile (`infra/docker/Dockerfile.spark`, `Dockerfile.flink`, `Dockerfile.airflow`, `Dockerfile.lakehouse-ui`) tách stage `builder` và image chạy. Stage builder giữ `apt`, `curl`, JDK và gcc. Image chạy chỉ copy JAR, Python và code.

Số byte là `docker image inspect --format '{{.Size}}'`. Cột `docker images` là SIZE mà lệnh in ra trên máy này. Không dùng mức giảm 88% đã viết trong README trước đó, vì Dockerfile lúc ấy không tương ứng.

| Image | Trước (`:single-stage`) | Sau (`:multi-stage`) | `docker images` trước | `docker images` sau |
|---|---:|---:|---:|---:|
| Spark | 842,808,888 | 841,453,810 | 2.24GB | 2.24GB |
| Flink | 1,814,918,517 | 1,271,375,322 | 4.55GB | 3.15GB |
| Airflow | 1,195,486,812 | 1,202,369,880 | 3.91GB | 3.95GB |
| Lakehouse UI | 43,551,511 | 43,551,518 | 203MB | 203MB |

Spark giảm 1,355,078 byte. Base `apache/spark` chiếm gần hết image. Runtime không còn `curl` và cache apt.

Flink giảm 543,543,195 byte. Runtime chỉ cài `python3` và `python3-pkg-resources`. JAR Iceberg/Hadoop, RocksDB và `apache-flink` được copy từ builder bằng `COPY --chown`, không có layer `chown -R` thứ hai. `gcc`, JDK và `curl` ở lại builder.

Airflow tăng 6,883,068 byte. Runtime bỏ `curl` nhưng cài `acryl-datahub==0.14.1` để task `publish_datahub` có package. JRE và bản Spark đã copy từ stage `spark-fetch`.

Lakehouse UI lệch 7 byte. Image gốc đã là `python:3.12-slim` chỉ copy `storage/`, nên stage builder không bỏ thêm gói nào.
