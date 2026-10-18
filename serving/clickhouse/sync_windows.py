"""Đồng bộ cửa sổ Flink từ Iceberg sang ClickHouse.

Đọc bằng Spark ``VERSION AS OF`` để Iceberg áp dụng equality-delete. Không đọc
file Parquet thô. Mỗi lần poll lấy snapshot hiện tại. Cùng snapshot thì không
ghi lại. Lỗi trước ``RENAME`` giữ reader cũ. Worker không có offset riêng: restart
đọc lại snapshot hiện tại.

Không gọi sync Gold và không đổi hai bảng reader của batch.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from serving.clickhouse.sync_gold import ServingSyncError, apply_ddl, clickhouse, clickhouse_json
from streaming.flink.runtime_guard import require_streaming_approval
from transformation.batch.contracts.validators import check_identifier

_COLUMNS = (
    "movie_id",
    "window_start",
    "window_end",
    "total_watch_seconds",
    "heartbeat_count",
    "active_users",
)


def sync_windows(
    spark,
    *,
    source_table: str,
    http: str = "http://127.0.0.1:18123",
    database: str = "cinestream_serving",
    password: str = "cinestream",
    snapshot_id: int | None = None,
    fail_after_rows: int | None = None,
    fail_before_log: bool = False,
) -> dict:
    """Nạp một snapshot cửa sổ. ``snapshot_id`` trống thì lấy snapshot hiện tại."""
    started = time.perf_counter()

    def finish(payload: dict) -> dict:
        payload["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
        return payload

    source_table = _source_table(source_table)
    database = check_identifier(database, "database")
    require_streaming_approval(windows_table=source_table, database=database)
    clickhouse(http, "SELECT 1", password=password)
    apply_ddl(http, database, password)
    current = _current_snapshot(spark, source_table)
    if current in {"absent", "empty"}:
        return finish({"status": "source_absent" if current == "absent" else "source_empty", "source_table": source_table})
    chosen = int(snapshot_id if snapshot_id is not None else current)
    frame = _read(spark, source_table, chosen)
    rows = _logical_rows(frame, chosen)
    _validate_keys(rows)
    if _checkpoint_matches(http, database, password, source_table, chosen, len(rows)):
        return finish({
            "status": "unchanged",
            "source_table": source_table,
            "snapshot_id": chosen,
            "window_rows": len(rows),
            "serving": _serving_view(http, database, password),
        })
    if _readers_match(http, database, password, chosen, rows):
        _insert_log(http, database, password, source_table, chosen, rows)
        return finish({
            "status": "logged",
            "source_table": source_table,
            "snapshot_id": chosen,
            "window_rows": len(rows),
            "serving": _serving_view(http, database, password),
        })
    _replace(http, database, password, rows, fail_after_rows)
    if fail_before_log:
        raise ServingSyncError("dừng trước trending_sync_log")
    _insert_log(http, database, password, source_table, chosen, rows)
    return finish({
        "status": "published",
        "source_table": source_table,
        "snapshot_id": chosen,
        "window_rows": len(rows),
        "serving": _serving_view(http, database, password),
    })


def poll_windows(spark, *, interval_seconds: float = 30, max_polls: int | None = None, **kwargs) -> dict:
    """Lặp sync. Lỗi kết nối trả ``retry`` và vòng sau đọc lại snapshot."""
    if interval_seconds < 0:
        raise ServingSyncError("interval-seconds phải >= 0")
    last: dict = {"status": "idle"}
    polls = 0
    while max_polls is None or polls < max_polls:
        polls += 1
        started = time.perf_counter()
        try:
            last = sync_windows(spark, **kwargs)
        except ServingSyncError as exc:
            last = {"status": "retry", "error": str(exc), "source_table": kwargs.get("source_table")}
        _record_poll(
            kwargs.get("http", "http://127.0.0.1:18123"),
            kwargs.get("database", "cinestream_serving"),
            kwargs.get("password", "cinestream"),
            str(kwargs.get("source_table") or ""),
            last,
        )
        print("TRENDING_JSON=" + json.dumps(last, default=str), flush=True)
        if max_polls is not None and polls >= max_polls:
            return last
        remaining = interval_seconds - (time.perf_counter() - started)
        if remaining > 0:
            time.sleep(remaining)
    return last


def _record_poll(http: str, database: str, password: str, source_table: str, result: dict) -> None:
    """Ghi một dòng trạng thái poll. Lỗi ClickHouse thì bỏ qua vì không có chỗ ghi."""
    try:
        database = check_identifier(database, "database")
        clickhouse(http, "SELECT 1", password=password)
        apply_ddl(http, database, password)
    except (ServingSyncError, RuntimeError):
        return
    snapshot = result.get("snapshot_id")
    rows = int(result.get("window_rows") or 0)
    latest = None
    try:
        count = int(clickhouse(http, f"SELECT count() FROM {database}.movie_windows", password=password).strip() or "0")
        rows = count
        if count:
            text = clickhouse(http, f"SELECT toString(max(window_end)) FROM {database}.movie_windows", password=password).strip()
            if text and text not in {"\\N", "NULL"}:
                latest = text
    except ServingSyncError:
        latest = None
    detail = str(result.get("error") or "").replace("'", "")[:300]
    status = str(result.get("status") or "unknown").replace("'", "")
    snapshot_sql = "NULL" if snapshot is None else str(int(snapshot))
    latest_sql = "NULL" if latest is None else f"'{latest}'"
    detail_sql = "NULL" if not detail else f"'{detail}'"
    safe_source = source_table.replace("'", "")
    try:
        clickhouse(
            http,
            f"""
            INSERT INTO {database}.trending_worker_status
                (source_table, status, iceberg_snapshot_id, window_rows, latest_window_end, polled_at, detail)
            VALUES ('{safe_source}', '{status}', {snapshot_sql}, {rows}, {latest_sql}, now64(3, 'UTC'), {detail_sql})
            """,
            password=password,
        )
    except ServingSyncError:
        return


def _source_table(name: str) -> str:
    parts = name.split(".")
    if len(parts) != 3:
        raise ServingSyncError("source table phải là catalog.namespace.table")
    return ".".join(check_identifier(part, "table") for part in parts)


def _current_snapshot(spark, table: str):
    try:
        spark.sql(f"REFRESH TABLE {table}")
        rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    except Exception as exc:
        if _missing(exc):
            return "absent"
        raise ServingSyncError(str(exc).splitlines()[-1][:300]) from exc
    if not rows:
        return "empty"
    return int(rows[0].snapshot_id)


def _missing(exc: Exception) -> bool:
    text = str(exc)
    needles = ("TABLE_OR_VIEW_NOT_FOUND", "Table or view not found", "NoSuchTableException", "NotFoundException")
    return any(needle in text for needle in needles)


def _read(spark, table: str, snapshot_id: int):
    known = {int(row.snapshot_id) for row in spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").collect()}
    if snapshot_id not in known:
        raise ServingSyncError(f"{table} không có snapshot {snapshot_id}")
    return spark.sql(
        f"""
        SELECT movie_id, window_start, window_end, total_watch_seconds, heartbeat_count, active_users
        FROM {table} VERSION AS OF {int(snapshot_id)}
        """
    )


def _logical_rows(frame, snapshot_id: int) -> list[dict]:
    missing = [name for name in _COLUMNS if name not in frame.columns]
    if missing:
        raise ServingSyncError(f"snapshot thiếu cột {missing}")
    rows = []
    for record in frame.collect():
        rows.append(
            {
                "movie_id": int(record.movie_id),
                "window_start": _format_ts(record.window_start),
                "window_end": _format_ts(record.window_end),
                "total_watch_seconds": int(record.total_watch_seconds),
                "heartbeat_count": int(record.heartbeat_count),
                "active_users": int(record.active_users),
                "iceberg_snapshot_id": int(snapshot_id),
            }
        )
    return rows


def _validate_keys(rows: list[dict]) -> None:
    keys = [(row["movie_id"], row["window_start"]) for row in rows]
    if len(keys) != len(set(keys)):
        raise ServingSyncError("đọc logic còn trùng (movie_id, window_start)")


def _replace(http: str, database: str, password: str, rows: list[dict], fail_after_rows: int | None) -> None:
    clickhouse(http, f"TRUNCATE TABLE {database}.movie_windows_stage", password=password)
    _insert(http, database, password, rows, fail_after_rows)
    keys = int(
        clickhouse(http, f"SELECT uniqExact(movie_id, window_start) FROM {database}.movie_windows_stage", password=password).strip()
        or "0"
    )
    count = int(clickhouse(http, f"SELECT count() FROM {database}.movie_windows_stage", password=password).strip() or "0")
    if count != len(rows) or keys != len(rows):
        raise ServingSyncError(f"stage cửa sổ {count} khóa {keys}, nguồn {len(rows)}")
    clickhouse(http, f"DROP TABLE IF EXISTS {database}.movie_windows_old", password=password)
    clickhouse(
        http,
        f"""
        RENAME TABLE
            {database}.movie_windows TO {database}.movie_windows_old,
            {database}.movie_windows_stage TO {database}.movie_windows
        """,
        password=password,
    )
    clickhouse(
        http,
        f"RENAME TABLE {database}.movie_windows_old TO {database}.movie_windows_stage",
        password=password,
    )


def _insert(http: str, database: str, password: str, rows: list[dict], fail_after: int | None) -> None:
    if not rows:
        if fail_after is not None:
            raise ServingSyncError("dừng giữa bulk load")
        return
    sent = 0
    chunk: list[dict] = []
    for row in rows:
        if fail_after is not None and sent >= fail_after:
            raise ServingSyncError("dừng giữa bulk load")
        chunk.append(row)
        if len(chunk) == 1000:
            _flush(http, database, password, chunk)
            sent += len(chunk)
            chunk = []
    if not chunk:
        return
    if fail_after is not None and sent + len(chunk) > fail_after:
        head = chunk[: fail_after - sent]
        if head:
            _flush(http, database, password, head)
        raise ServingSyncError("dừng giữa bulk load")
    _flush(http, database, password, chunk)


def _flush(http: str, database: str, password: str, rows: list[dict]) -> None:
    body = "\n".join(json.dumps(row) for row in rows).encode()
    clickhouse(http, f"INSERT INTO {database}.movie_windows_stage FORMAT JSONEachRow", body=body, password=password)


def _insert_log(http, database, password, source_table: str, snapshot_id: int, rows: list[dict]) -> None:
    latest = max((row["window_end"] for row in rows), default=None)
    latest_sql = "NULL" if latest is None else f"'{latest}'"
    clickhouse(
        http,
        f"""
        INSERT INTO {database}.trending_sync_log
            (iceberg_snapshot_id, source_table, window_rows, latest_window_end, synced_at)
        VALUES ({int(snapshot_id)}, '{source_table}', {len(rows)}, {latest_sql}, now64(3, 'UTC'))
        """,
        password=password,
    )


def _checkpoint_matches(http, database, password, source_table: str, snapshot_id: int, window_rows: int) -> bool:
    found = clickhouse_json(
        http,
        f"""
        SELECT window_rows
        FROM {database}.trending_sync_log
        WHERE iceberg_snapshot_id = {int(snapshot_id)}
          AND source_table = '{source_table}'
        """,
        password=password,
    )
    if len(found) != 1 or int(found[0]["window_rows"]) != window_rows:
        return False
    serving = _serving_view(http, database, password)
    return serving["rows"] == window_rows and serving["snapshots"] == [int(snapshot_id)] and serving["keys"] == window_rows


def _readers_match(http, database, password, snapshot_id: int, rows: list[dict]) -> bool:
    serving = _serving_view(http, database, password)
    if serving["rows"] != len(rows) or serving["keys"] != len(rows) or serving["snapshots"] != [int(snapshot_id)]:
        return False
    if not rows:
        return True
    stored = clickhouse_json(
        http,
        f"""
        SELECT movie_id, toString(window_start) AS window_start, heartbeat_count, total_watch_seconds, active_users
        FROM {database}.movie_windows
        ORDER BY movie_id, window_start
        """,
        password=password,
    )
    expected = sorted(
        (
            int(row["movie_id"]),
            row["window_start"],
            int(row["heartbeat_count"]),
            int(row["total_watch_seconds"]),
            int(row["active_users"]),
        )
        for row in rows
    )
    actual = [
        (
            int(row["movie_id"]),
            _norm_ts(row["window_start"]),
            int(row["heartbeat_count"]),
            int(row["total_watch_seconds"]),
            int(row["active_users"]),
        )
        for row in stored
    ]
    return actual == [(item[0], _norm_ts(item[1]), item[2], item[3], item[4]) for item in expected]


def _serving_view(http, database, password) -> dict:
    rows = int(clickhouse(http, f"SELECT count() FROM {database}.movie_windows", password=password).strip() or "0")
    keys = int(
        clickhouse(http, f"SELECT uniqExact(movie_id, window_start) FROM {database}.movie_windows", password=password).strip()
        or "0"
    )
    snapshots = clickhouse_json(
        http,
        f"SELECT DISTINCT iceberg_snapshot_id FROM {database}.movie_windows ORDER BY iceberg_snapshot_id",
        password=password,
    )
    return {
        "rows": rows,
        "keys": keys,
        "snapshots": [int(row["iceberg_snapshot_id"]) for row in snapshots],
        "log_rows": int(clickhouse(http, f"SELECT count() FROM {database}.trending_sync_log", password=password).strip() or "0"),
    }


def _norm_ts(value) -> str:
    text = str(value).replace("T", " ").replace("Z", "")[:23]
    if len(text) == 19:
        return text + ".000"
    return text


def _format_ts(value) -> str:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, str):
        text = value.replace("T", " ").replace("Z", "")
        return text[:23]
    if isinstance(value, datetime) and value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def main(argv: list[str] | None = None) -> None:
    from transformation.batch.common import build_parser, start_spark

    parser = build_parser("Đồng bộ cửa sổ trending Iceberg sang ClickHouse.")
    parser.add_argument("--warehouse", default="s3a://lakehouse")
    parser.add_argument("--source-table", default="cinestream.silver.stg_playback_windows")
    parser.add_argument("--clickhouse-http", default="http://127.0.0.1:18123")
    parser.add_argument("--database", default="cinestream_serving")
    parser.add_argument("--interval-seconds", type=float, default=30)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-polls", type=int, default=None)
    parser.add_argument("--fail-after-rows", type=int, default=None)
    parser.add_argument("--fail-before-log", action="store_true")
    args = parser.parse_args(argv)
    require_streaming_approval(windows_table=args.source_table, database=args.database)
    spark = start_spark("cinestream-trending-sync", args)
    try:
        poll_windows(
            spark,
            interval_seconds=args.interval_seconds,
            max_polls=1 if args.once else args.max_polls,
            source_table=args.source_table,
            http=args.clickhouse_http,
            database=args.database,
            fail_after_rows=args.fail_after_rows,
            fail_before_log=args.fail_before_log,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
