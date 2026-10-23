"""So schema DataFrame với schema bảng mà không ghi đè dữ liệu.

``rows_equal`` và ``schema_equal`` là hai kết quả riêng. Lệch cột, kiểu,
nullability hoặc partition không bật overwrite. ``dt`` của thư mục nguồn
không phải field partition Iceberg.
"""

from __future__ import annotations

import re


def compare_fields(frame_fields: list[dict], table_fields: list[dict]) -> list[dict]:
    """``type`` là chuỗi simpleString. Không đọc catalog."""
    frame = {item["name"]: item for item in frame_fields}
    table = {item["name"]: item for item in table_fields}
    differences = []
    for name, field in frame.items():
        physical = table.get(name)
        if physical is None:
            differences.append({"column": name, "kind": "missing_on_table", "frame": field.get("type"), "table": None})
            continue
        if _type_key(field.get("type")) != _type_key(physical.get("type")):
            differences.append(
                {"column": name, "kind": "type", "frame": field.get("type"), "table": physical.get("type")}
            )
        frame_nullable = bool(field.get("nullable", True))
        table_nullable = bool(physical.get("nullable", True))
        if frame_nullable != table_nullable:
            differences.append(
                {
                    "column": name,
                    "kind": "nullability",
                    "frame": "nullable" if frame_nullable else "required",
                    "table": "nullable" if table_nullable else "required",
                }
            )
    for name, field in table.items():
        if name in frame:
            continue
        differences.append({"column": name, "kind": "extra_on_table", "frame": None, "table": field.get("type")})
    return differences


def write_decision(rows_equal: bool | None, differences: list[dict]) -> dict:
    """Chỉ overwrite khi dòng shared khác. Schema lệch một mình giữ snapshot."""
    schema_diffs = [item for item in differences if item.get("kind") != "null_value"]
    if rows_equal is False:
        status, overwrite = "overwritten", True
    else:
        status, overwrite = "unchanged", False
    return {
        "status": status,
        "overwrite": overwrite,
        "rows_equal": rows_equal,
        "schema_equal": not schema_diffs,
        "differences": differences,
        "repartition": False,
    }


def null_constraint_findings(null_counts: dict[str, int], required: list[str]) -> list[dict]:
    """Đếm NULL thật. Cờ Iceberg optional không chứng minh cột có giá trị."""
    return [
        {"column": name, "kind": "null_value", "nulls": int(null_counts.get(name) or 0), "constraint": "not_null"}
        for name in required
        if int(null_counts.get(name) or 0) > 0
    ]


def mark_source_folders(differences: list[dict], partition_fields: list[str], folders: tuple[str, ...] = ("dt",)) -> list[dict]:
    marked = []
    for item in differences:
        copied = dict(item)
        if copied.get("column") in folders and copied.get("kind") == "extra_on_table":
            copied["origin"] = "source_folder"
            copied["iceberg_partition_field"] = copied["column"] in partition_fields
        marked.append(copied)
    return marked


def partition_difference(expected: list[str], actual: list[str], source_folders: tuple[str, ...] = ("dt",)) -> dict:
    """Không repartition. ``dt`` của path nguồn được kê riêng."""
    return {
        "expected_iceberg_fields": list(expected),
        "actual_iceberg_fields": list(actual),
        "missing_partition_fields": [field for field in expected if field not in actual],
        "unexpected_partition_fields": [field for field in actual if field not in expected],
        "source_folder_columns": [field for field in source_folders],
        "source_folder_is_iceberg_partition": [field for field in source_folders if field in actual],
        "repartition": False,
    }


def expected_partitions(sql: str) -> dict[str, list[str]]:
    """Đọc ``PARTITIONED BY`` của từng CREATE TABLE. Bảng không khai báo thì danh sách rỗng."""
    found = {}
    for part in re.split(r"(?=CREATE TABLE IF NOT EXISTS)", sql):
        name = re.search(r"CREATE TABLE IF NOT EXISTS\s+\S+\.(\w+)", part)
        if name is None:
            continue
        clause = _partition_clause(part)
        if clause is None:
            found[name.group(1)] = []
            continue
        fields = []
        for raw in clause.split(","):
            token = " ".join(raw.split())
            if token:
                fields.append(token)
        found[name.group(1)] = fields
    return found


def _partition_clause(text: str) -> str | None:
    marker = text.upper().find("PARTITIONED BY")
    if marker < 0:
        return None
    start = text.find("(", marker)
    if start < 0:
        return None
    depth = 0
    for index in range(start, len(text)):
        if text[index] == "(":
            depth += 1
        elif text[index] == ")":
            depth -= 1
            if depth == 0:
                return text[start + 1 : index]
    return None


def _type_key(value: object) -> str:
    return str(value or "").lower().replace(" ", "")
