"""So sánh schema kỳ vọng từ DDL/writer với schema Iceberg vật lý.

Không sửa cột. ``breaking`` là reader hoặc writer hiện tại không đọc được kiểu.
``manual_review`` là lệch nhưng đường đọc hiện tại vẫn chọn cột theo tên.
``compatible`` là vật lý chặt hơn contract, reader vẫn đọc được.
"""

from __future__ import annotations

_SEVERITY = {"match": 0, "compatible": 1, "manual_review": 2, "breaking": 3}


def classify_schema(expected: list[tuple[str, str]], physical: list[dict]) -> dict:
    """``physical`` là field Iceberg: name, type, required."""
    expected_map = {name: _declared(type_sql) for name, type_sql in expected}
    physical_map = {field["name"]: field for field in physical}
    findings = []
    for name, declared in expected_map.items():
        field = physical_map.get(name)
        if field is None:
            findings.append(
                {
                    "column": name,
                    "kind": "missing",
                    "classification": "breaking",
                    "expected": declared["sql"],
                    "physical": None,
                }
            )
            continue
        findings.extend(_compare_column(name, declared, field))
    for name, field in physical_map.items():
        if name in expected_map:
            continue
        findings.append(
            {
                "column": name,
                "kind": "extra",
                "classification": "manual_review",
                "expected": None,
                "physical": {"type": field.get("type"), "required": field.get("required")},
            }
        )
    classification = "match"
    for item in findings:
        if _SEVERITY[item["classification"]] > _SEVERITY[classification]:
            classification = item["classification"]
    return {"classification": classification, "findings": findings}


def _compare_column(name: str, declared: dict, field: dict) -> list[dict]:
    found = []
    physical_type = str(field.get("type") or "")
    relation = _type_relation(declared, physical_type)
    if relation != "match":
        found.append(
            {
                "column": name,
                "kind": "type",
                "classification": relation,
                "expected": declared["sql"],
                "physical": physical_type,
            }
        )
    physical_required = bool(field.get("required"))
    if declared["required"] and not physical_required:
        found.append(
            {
                "column": name,
                "kind": "nullability",
                "classification": "manual_review",
                "expected": "required",
                "physical": "optional",
            }
        )
    elif physical_required and not declared["required"]:
        found.append(
            {
                "column": name,
                "kind": "nullability",
                "classification": "compatible",
                "expected": "optional",
                "physical": "required",
            }
        )
    return found


def _declared(type_sql: str) -> dict:
    upper = " ".join(str(type_sql).upper().split())
    required = "NOT NULL" in upper
    compact = upper.replace("NOT NULL", "").replace(" ", "")
    if compact.startswith("DECIMAL"):
        family = compact.lower()
    elif compact.startswith("TIMESTAMP"):
        family = "timestamp"
    else:
        family = {
            "BIGINT": "long",
            "INT": "int",
            "STRING": "string",
            "DOUBLE": "double",
            "FLOAT": "float",
            "BOOLEAN": "boolean",
            "DATE": "date",
        }.get(compact, compact.lower())
    return {"sql": type_sql, "family": family, "required": required}


def _type_relation(declared: dict, physical_type: str) -> str:
    physical = physical_type.lower().replace(" ", "")
    family = declared["family"]
    if family == "timestamp":
        if physical == "timestamptz":
            return "match"
        if physical == "timestamp":
            return "manual_review"
        return "breaking"
    if family.startswith("decimal"):
        if physical == family:
            return "match"
        if physical.startswith("decimal") or physical in {"double", "float"}:
            return "manual_review"
        return "breaking"
    if physical == family:
        return "match"
    return "breaking"
