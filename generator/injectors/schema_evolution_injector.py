"""Tiêm schema evolution cho bảng ``movies``.

Đặc tả mục 4.2: từ một mốc partition (mặc định file thứ 45),
Parquet bắt đầu mang thêm 3 cột:

* ``content_rating`` string, nullable: G, PG-13, R, NC-17
* ``hdr_format`` string, nullable: HDR10, Dolby_Vision, SDR
* ``audio_spatial`` boolean, nullable

Các partition trước mốc đó **không khai báo** 3 cột này trong footer
Parquet. Cột không được ghi dưới dạng null — chúng vắng mặt hoàn toàn,
đúng lỗi mà Spark gặp khi đọc nếu không bật ``mergeSchema``.

Các cột ổn định (có ở mọi thế hệ) theo sau lược đồ gốc của đặc tả,
cộng thêm thuộc tính danh mục dùng ở tầng sau: ``budget``,
``director_id``, ``created_ts``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

CONTENT_RATINGS = ("G", "PG-13", "R", "NC-17")
HDR_FORMATS = ("HDR10", "Dolby_Vision", "SDR")
EVOLVED_COLUMNS = ("content_rating", "hdr_format", "audio_spatial")

BASE_FIELDS = (
    pa.field("movie_id", pa.int64(), nullable=False),
    pa.field("title", pa.string(), nullable=False),
    pa.field("release_year", pa.int32(), nullable=False),
    pa.field("runtime_minutes", pa.int32(), nullable=False),
    pa.field("original_language", pa.string(), nullable=False),
    pa.field("budget", pa.int64(), nullable=False),
    pa.field("director_id", pa.int64(), nullable=False),
    pa.field("created_ts", pa.timestamp("s"), nullable=False),
)
EVOLVED_FIELDS = (
    pa.field("content_rating", pa.string(), nullable=True),
    pa.field("hdr_format", pa.string(), nullable=True),
    pa.field("audio_spatial", pa.bool_(), nullable=True),
)


def assign_evolved_attributes(n_rows: int, rng: np.random.Generator) -> dict[str, np.ndarray]:
    """Sinh giá trị 3 cột mới. Writer quyết định cột nào được ghi ra file."""
    if n_rows < 0:
        raise ValueError("n_rows phải >= 0")
    return {
        "content_rating": rng.choice(np.array(CONTENT_RATINGS, dtype=object), size=n_rows),
        "hdr_format": rng.choice(np.array(HDR_FORMATS, dtype=object), size=n_rows),
        "audio_spatial": rng.integers(0, 2, size=n_rows).astype(np.bool_),
    }


def schema_for_partition(partition_number: int, evolution_from_partition: int) -> pa.Schema:
    """Lược đồ của một partition 1-based. Mốc evolution thuộc thế hệ mới."""
    if partition_number < 1:
        raise ValueError("partition_number bắt đầu từ 1")
    evolved = partition_number >= evolution_from_partition
    fields = BASE_FIELDS + EVOLVED_FIELDS if evolved else BASE_FIELDS
    return pa.schema(list(fields))


def write_movie_partitions(
    columns: dict[str, np.ndarray],
    output_dir: Path,
    *,
    n_partitions: int,
    evolution_from_partition: int,
) -> dict[str, object]:
    """Ghi ``movies/partition_XXXXX.parquet``, mỗi file một thế hệ schema.

    Dòng được chia liên tiếp theo ``movie_id`` đã sắp tăng dần, nên
    partition 1 chứa các id nhỏ nhất. File rỗng không được ghi.

    Returns:
        Thống kê số file mỗi thế hệ và danh sách tên cột đọc lại từ
        footer của file cũ đầu tiên và file mới đầu tiên.
    """
    if n_partitions < 1:
        raise ValueError("n_partitions phải >= 1")
    if evolution_from_partition < 1:
        raise ValueError("evolution_from_partition phải >= 1")

    n_rows = int(np.asarray(columns["movie_id"]).size)
    if n_rows < 1:
        raise ValueError("không có phim để ghi")

    n_files = min(n_partitions, n_rows)
    output_dir.mkdir(parents=True, exist_ok=True)
    slices = np.array_split(np.arange(n_rows, dtype=np.int64), n_files)

    n_old = 0
    n_new = 0
    first_old: Path | None = None
    first_new: Path | None = None
    for offset, row_index in enumerate(slices):
        if row_index.size == 0:
            continue
        partition_number = offset + 1
        evolved = partition_number >= evolution_from_partition
        schema = schema_for_partition(partition_number, evolution_from_partition)
        path = output_dir / f"partition_{partition_number:05d}.parquet"
        _write_slice(columns, row_index, schema, path)
        if evolved:
            n_new += 1
            if first_new is None:
                first_new = path
        else:
            n_old += 1
            if first_old is None:
                first_old = path

    return {
        "n_partitions_requested": n_partitions,
        "n_files_written": n_old + n_new,
        "n_old_schema_files": n_old,
        "n_new_schema_files": n_new,
        "evolution_from_partition": evolution_from_partition,
        "old_schema_columns": _footer_names(first_old),
        "new_schema_columns": _footer_names(first_new),
        "old_example": None if first_old is None else first_old.name,
        "new_example": None if first_new is None else first_new.name,
    }


def _write_slice(
    columns: dict[str, np.ndarray],
    row_index: np.ndarray,
    schema: pa.Schema,
    path: Path,
) -> None:
    arrays = []
    for field in schema:
        values = np.asarray(columns[field.name])[row_index]
        arrays.append(pa.array(values, type=field.type))
    table = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(table, path, compression="snappy")


def _footer_names(path: Path | None) -> list[str]:
    if path is None:
        return []
    return list(pq.ParquetFile(path).schema_arrow.names)
