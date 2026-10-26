"""Xuất sơ đồ dim/fact từ schema Iceberg hoặc ClickHouse ra SVG."""

from __future__ import annotations

import argparse
import json
import re
import urllib.request
from pathlib import Path

RELATIONS = (
    ("dim_users", "user_sk", "fact_ratings", "user_sk", "1", "n"),
    ("dim_movies", "movie_sk", "fact_ratings", "movie_sk", "1", "n"),
    ("dim_users", "user_sk", "fact_watch_sessions", "user_sk", "1", "n"),
    ("dim_movies", "movie_sk", "fact_watch_sessions", "movie_sk", "1", "n"),
    ("fact_watch_sessions", "session_sk", "obt_movie_engagement", "session_sk", "1", "1"),
)

TABLES = (
    "dim_users",
    "dim_movies",
    "fact_ratings",
    "fact_watch_sessions",
    "obt_movie_engagement",
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Xuất ERD dim/fact.")
    parser.add_argument("--iceberg-rest", default="http://127.0.0.1:8181")
    parser.add_argument("--clickhouse-http", default="http://127.0.0.1:18123")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="cinestream")
    parser.add_argument("--ddl", type=Path, default=Path("storage/lakehouse/gold/gold_tables.sql"))
    parser.add_argument("--out", type=Path, default=Path("docs/proof/images/dim_fact_erd.svg"))
    parser.add_argument("--meta-out", type=Path, default=Path("docs/proof/logs/dim_fact_erd.json"))
    args = parser.parse_args(argv)
    columns, source = load_columns(args)
    svg = render_svg(columns, source)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(svg)
    args.meta_out.parent.mkdir(parents=True, exist_ok=True)
    args.meta_out.write_text(json.dumps({"source": source, "columns": columns}, indent=2))
    print(f"đã ghi {args.out} từ {source}")


def load_columns(args) -> tuple[dict[str, list[str]], str]:
    live = _iceberg_columns(args.iceberg_rest)
    if _covers_star(live):
        return live, "iceberg-rest"
    stored = _minio_columns()
    if _covers_star(stored):
        serving = _clickhouse_columns(args.clickhouse_http, args.clickhouse_user, args.clickhouse_password)
        stored.update({f"clickhouse.{name}": columns for name, columns in serving.items()})
        return stored, "iceberg-minio-metadata+clickhouse"
    parsed = _ddl_columns(args.ddl.read_text())
    serving = _clickhouse_columns(args.clickhouse_http, args.clickhouse_user, args.clickhouse_password)
    if serving:
        parsed["clickhouse.movie_ratings"] = serving.get("movie_ratings", [])
        parsed["clickhouse.movie_engagement"] = serving.get("movie_engagement", [])
        return parsed, "iceberg-ddl+clickhouse"
    return parsed, "iceberg-ddl"


def render_svg(columns: dict[str, list[str]], source: str) -> str:
    boxes = [name for name in TABLES if name in columns]
    width = 1100
    box_w = 340
    gap_y = 28
    positions = {}
    y_box = 78
    for name in boxes:
        height = 36 + 18 * max(1, len(columns[name]))
        positions[name] = (40, y_box, box_w, height)
        y_box += height + gap_y
    height_total = y_box + 120
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height_total}" viewBox="0 0 {width} {height_total}">',
        '<rect width="100%" height="100%" fill="#f7f4ee"/>',
        f'<text x="40" y="36" font-family="Georgia, serif" font-size="22" fill="#1c1915">CineStream dim / fact</text>',
        f'<text x="40" y="56" font-family="ui-sans-serif, sans-serif" font-size="12" fill="#5c564c">Nguồn cột: {source}</text>',
    ]
    centers = {}
    for name, (x, y_box, box_width, height) in positions.items():
        parts.append(f'<rect x="{x}" y="{y_box}" width="{box_width}" height="{height}" rx="8" fill="#fffdf8" stroke="#1c1915" stroke-width="1.5"/>')
        parts.append(f'<rect x="{x}" y="{y_box}" width="{box_width}" height="28" rx="8" fill="#1c1915"/>')
        parts.append(f'<rect x="{x}" y="{y_box + 16}" width="{box_width}" height="12" fill="#1c1915"/>')
        parts.append(f'<text x="{x + 12}" y="{y_box + 19}" font-family="ui-sans-serif, sans-serif" font-size="13" fill="#f7f4ee">{name}</text>')
        for row, column in enumerate(columns[name]):
            parts.append(
                f'<text x="{x + 12}" y="{y_box + 46 + row * 18}" font-family="ui-monospace, monospace" font-size="12" fill="#1c1915">{column}</text>'
            )
    parts.append('<text x="430" y="100" font-family="Georgia, serif" font-size="16" fill="#1c1915">Quan hệ khóa</text>')
    label_y = 128
    for left, left_col, right, right_col, left_card, right_card in RELATIONS:
        if left not in positions or right not in positions:
            continue
        parts.append(
            f'<text x="430" y="{label_y}" font-family="ui-monospace, monospace" font-size="13" fill="#8a5a2a">'
            f'{left}.{left_col}  {left_card} — {right_card}  {right}.{right_col}</text>'
        )
        label_y += 28
    if "clickhouse.movie_ratings" in columns:
        parts.append(
            f'<text x="430" y="{label_y + 12}" font-family="ui-sans-serif, sans-serif" font-size="13" fill="#1c1915">'
            "ClickHouse serving không giữ dim/fact. movie_ratings và movie_engagement là bản phẳng.</text>"
        )
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def _covers_star(columns: dict[str, list[str]]) -> bool:
    return {"dim_users", "dim_movies", "fact_ratings", "fact_watch_sessions"} <= set(columns)


def _minio_columns() -> dict[str, list[str]]:
    try:
        import boto3
    except ImportError:
        return {}
    client = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    found = {}
    for name in TABLES:
        latest = _latest_metadata(client, f"gold/{name}/metadata/")
        if latest is None:
            continue
        body = client.get_object(Bucket="lakehouse", Key=latest)["Body"].read()
        meta = json.loads(body)
        schema_id = meta.get("current-schema-id")
        schemas = meta.get("schemas") or []
        chosen = next((item for item in schemas if item.get("schema-id") == schema_id), schemas[-1] if schemas else None)
        if not chosen:
            continue
        found[name] = [
            f"{field['name']} {field.get('type')}"
            for field in chosen.get("fields") or []
        ]
    return found


def _latest_metadata(client, prefix: str) -> str | None:
    token = None
    newest = None
    newest_key = None
    while True:
        kwargs = {"Bucket": "lakehouse", "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for item in page.get("Contents") or []:
            if not item["Key"].endswith(".metadata.json"):
                continue
            if newest is None or item["LastModified"] >= newest:
                newest = item["LastModified"]
                newest_key = item["Key"]
        if not page.get("IsTruncated"):
            return newest_key
        token = page.get("NextContinuationToken")


def _ddl_columns(sql: str) -> dict[str, list[str]]:
    found = {}
    pattern = re.compile(r"CREATE TABLE IF NOT EXISTS cinestream\.gold\.(\w+) \((.*?)\)\s*USING iceberg", re.S)
    for name, body in pattern.findall(sql):
        columns = []
        for line in body.splitlines():
            stripped = line.strip().rstrip(",")
            if not stripped or stripped.startswith("--"):
                continue
            columns.append(stripped.split()[0])
        found[name] = columns
    return found


def _iceberg_columns(rest: str) -> dict[str, list[str]]:
    found = {}
    for namespace in ("gold", "it_af_dp"):
        for name in TABLES:
            url = f"{rest.rstrip('/')}/v1/namespaces/{namespace}/tables/{name}"
            try:
                with urllib.request.urlopen(url, timeout=5) as response:
                    payload = json.loads(response.read().decode())
            except Exception:
                continue
            schema = payload.get("metadata", {}).get("schemas") or []
            if not schema:
                continue
            fields = schema[-1].get("fields") or []
            found[name] = [f"{field['name']} {field.get('type', '')}" for field in fields]
        if found:
            return found
    return {}


def _clickhouse_columns(http: str, user: str, password: str) -> dict[str, list[str]]:
    query = (
        "SELECT table, name, type FROM system.columns "
        "WHERE database = 'cinestream_serving' AND table IN ('movie_ratings', 'movie_engagement') "
        "ORDER BY table, position FORMAT TabSeparated"
    )
    url = f"{http.rstrip('/')}/?user={user}&password={password}"
    request = urllib.request.Request(url, data=query.encode(), method="POST")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            text = response.read().decode()
    except Exception:
        return {}
    found: dict[str, list[str]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        table, name, kind = line.split("\t")
        found.setdefault(table, []).append(f"{name} {kind}")
    return found


if __name__ == "__main__":
    main()
