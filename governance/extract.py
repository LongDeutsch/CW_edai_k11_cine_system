"""Đọc schema và tên bảng từ source đang có. Không suy diễn cột."""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

_CREATE_START = re.compile(r"CREATE TABLE(?: IF NOT EXISTS)?\s+([A-Za-z0-9_.{}$]+)\s*\(", re.IGNORECASE)
_COLUMN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+(.+)$")


def spark_tables(relative_sql: str) -> dict[str, list[tuple[str, str]]]:
    return _tables((REPO / relative_sql).read_text())


def clickhouse_tables() -> dict[str, list[tuple[str, str]]]:
    text = _strip_comments((REPO / "serving/clickhouse/ddl.sql").read_text())
    found = _tables(text)
    clones = re.findall(
        r"CREATE TABLE IF NOT EXISTS __DATABASE__\.([A-Za-z0-9_]+) AS __DATABASE__\.([A-Za-z0-9_]+)",
        text,
    )
    for name, source in clones:
        found[name] = list(found[source])
    return found


def flink_tables() -> dict[str, list[tuple[str, str]]]:
    from streaming.flink.config import StreamJobConfig
    from streaming.flink.sinks.iceberg_sink import heartbeat_table_ddl, windows_table_ddl

    config = StreamJobConfig()
    found = {}
    for ddl in (heartbeat_table_ddl(config), windows_table_ddl(config)):
        found.update(_tables(ddl))
    return found


def audit_tables() -> dict[str, list[tuple[str, str]]]:
    text = (REPO / "transformation/batch/contracts/validators.py").read_text()
    markers = {
        "quarantine_records": "CREATE TABLE IF NOT EXISTS {quarantine_table(namespace)} (",
        "quality_results": "CREATE TABLE IF NOT EXISTS {quality_table(namespace)} (",
    }
    found = {}
    for name, marker in markers.items():
        start = text.find(marker)
        if start < 0:
            found[name] = []
            continue
        body = _balanced(text[start + len(marker) - 1 :])
        found[name] = _columns(body[1:-1]) if body else []
    return found


def fastapi_routes() -> list[str]:
    text = (REPO / "serving/api/app.py").read_text()
    return re.findall(r"@app\.get\(\s*\"([^\"]+)\"", text)


def airflow_task_ids() -> list[str]:
    text = (REPO / "orchestration/dags/cinestream_daily_batch_lakehouse.py").read_text()
    return re.findall(r"task_id=\"([^\"]+)\"", text)


def _strip_comments(text: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in text.splitlines())


def _tables(text: str) -> dict[str, list[tuple[str, str]]]:
    source = _strip_comments(text)
    found = {}
    for match in _CREATE_START.finditer(source):
        body = _balanced(source[match.end() - 1 :])
        if body is None:
            continue
        found[match.group(1).split(".")[-1]] = _columns(body[1:-1])
    return found


def _balanced(text: str) -> str | None:
    if not text.startswith("("):
        return None
    depth = 0
    for index, character in enumerate(text):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return text[: index + 1]
    return None


def _columns(body: str) -> list[tuple[str, str]]:
    columns = []
    for raw in body.splitlines():
        line = raw.strip().rstrip(",")
        match = _COLUMN.match(line)
        if not match:
            continue
        name = match.group(1)
        if name.upper() in {"PRIMARY", "CONSTRAINT", "INDEX", "KEY"}:
            continue
        columns.append((name, match.group(2).rstrip(",")))
    return columns
