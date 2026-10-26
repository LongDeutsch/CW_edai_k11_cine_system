# 22 — Kích thước image trước và sau multi-stage

Bốn Dockerfile đều tách stage builder và image chạy. Số đo là `docker image inspect` trường `Size` (tổng byte các layer), không phải cột `docker images` đã gộp layer dùng chung.

Image một stage được giữ bằng tag `single-stage` trước khi build lại. Image nhiều stage là tag `multi-stage`. Spark master và spark worker dùng cùng `Dockerfile.spark`; hai tag một stage có cùng số byte.

| Image | Trước (byte) | Sau (byte) | Trước | Sau |
|---|---:|---:|---:|---:|
| Spark (`Dockerfile.spark`) | 842808888 | 841453810 | 803.8 MiB | 802.5 MiB |
| Flink (`Dockerfile.flink`) | 1814918517 | 1271104760 | 1730.8 MiB | 1212.2 MiB |
| Airflow (`Dockerfile.airflow`) | 1195486812 | 1202369880 | 1140.1 MiB | 1146.7 MiB |
| Lakehouse UI (`Dockerfile.lakehouse-ui`) | 43551511 | 43551518 | 41.5 MiB | 41.5 MiB |

Flink giảm 543813757 byte. Builder giữ JDK, gcc, curl và apt cache. Image chạy chỉ thêm `python3`, JAR Iceberg/Hadoop, plugin RocksDB và `apache-flink` đã cài vào `/opt/pydeps`.

Spark giảm 1355078 byte. Builder tải JAR và Python standalone rồi xóa apt list. Phần lớn image là base `apache/spark:3.5.3`.

Lakehouse UI gần như không đổi. Cả hai stage đều từ `python:3.12-slim`, và stage chạy chỉ copy thư mục `storage`.

Airflow tăng 6883068 byte. Stage `spark-fetch` tải Spark và JAR, image chạy chỉ cài JRE và copy `/opt/spark`. Phần tăng thêm là `acryl-datahub==0.14.1`, gói image một stage chưa có.
