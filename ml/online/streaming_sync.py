"""Đưa snapshot logic của cửa sổ Flink vào Feast offline và Redis.

Đọc dòng đã áp equality-delete. Không đọc file Parquet thô của Iceberg.
``committed.json`` chỉ tiến sau khi ``get_online_features`` khớp cửa sổ mới
nhất của từng phim. Snapshot có ``committed_at`` cũ hơn bị từ chối trước khi
ghi Redis.

Cùng ``window_end`` vẫn ghi đè được vì Redis ``skip_dedup`` bật và
``created_timestamp`` tăng theo sequence. Đây không phải exactly-once xuyên
Kafka, Iceberg và Redis: retry cùng snapshot sẽ ghi lại Redis rồi mới commit.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from ml.feast.streaming_features import FEATURES, VIEW

FEATURE_FILE = Path(__file__).resolve().parents[1] / "feast" / "streaming_features.py"
PRODUCTION_WINDOWS = "cinestream.silver.stg_playback_windows"
STREAM_REDIS = "localhost:16379,db=2"


class StaleSnapshot(RuntimeError):
    """Snapshot cũ hơn bản đã lên online."""


class SyncFailed(RuntimeError):
    """Chưa commit. State thành công giữ nguyên."""


def sync_logical(
    rows: list[dict] | None,
    *,
    snapshot_id: str | int | None,
    committed_at: datetime | str | None,
    repo_dir: Path,
    state_dir: Path,
    redis: str = STREAM_REDIS,
    after_write=None,
) -> dict:
    """Nạp các dòng logic của một snapshot. ``rows is None`` nghĩa là bảng không có."""
    started = time.perf_counter()
    if _redis_db(redis) != "2":
        raise SyncFailed("streaming chỉ ghi Redis db=2")
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    committed_path = state_dir / "committed.json"
    pending_path = state_dir / "pending.json"
    history_path = state_dir / "history.parquet"
    next_path = state_dir / "history.next.parquet"
    committed = _read(committed_path)

    def finish(payload: dict) -> dict:
        payload["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
        return payload

    if rows is None:
        raise SyncFailed("nguồn cửa sổ không có")
    if snapshot_id is None or committed_at is None:
        raise SyncFailed("thiếu snapshot id hoặc committed_at")
    if not rows:
        return finish({"result": "source_empty", "snapshot_id": str(snapshot_id)})
    _reject_duplicates(rows)
    incoming_at = _as_utc(committed_at)
    snapshot_key = str(snapshot_id)
    if committed and committed.get("status") == "success":
        if snapshot_key == str(committed["snapshot_id"]):
            if _online_matches(repo_dir, history_path, redis):
                return finish({**committed, "result": "unchanged"})
            _publish(history_path, repo_dir, redis)
            if after_write:
                after_write()
            if not _online_matches(repo_dir, history_path, redis):
                raise SyncFailed("Redis không khớp history sau khi nạp lại")
            committed["reloaded_at"] = _now()
            _atomic(committed_path, committed)
            return finish({**committed, "result": "reloaded"})
        previous_at = _as_utc(committed["committed_at"])
        if incoming_at <= previous_at:
            raise StaleSnapshot(
                f"snapshot {snapshot_key} cũ hơn {committed['snapshot_id']}"
            )
    pending = _read(pending_path)
    if pending and str(pending.get("snapshot_id")) == snapshot_key and next_path.exists():
        sequence = int(pending["sequence"])
        candidate = pd.read_parquet(next_path)
    else:
        sequence = int(committed["sequence"]) + 1 if committed else 1
        candidate = _append(history_path, _version_frame(rows, sequence))
        candidate.to_parquet(next_path, index=False)
        _atomic(
            pending_path,
            {
                "snapshot_id": snapshot_key,
                "sequence": sequence,
                "committed_at": incoming_at.isoformat(),
                "status": "pending",
                "started_at": _now(),
            },
        )
    try:
        _publish(next_path, repo_dir, redis)
        if after_write:
            after_write()
        if not _online_matches(repo_dir, next_path, redis):
            raise SyncFailed("get_online_features lệch snapshot")
    except Exception as exc:
        if isinstance(exc, SyncFailed):
            raise
        raise SyncFailed(str(exc)) from exc
    shutil.copy(next_path, history_path)
    done = {
        "snapshot_id": snapshot_key,
        "committed_at": incoming_at.isoformat(),
        "sequence": sequence,
        "window_rows": int(len(rows)),
        "status": "success",
        "materialized_at": _now(),
    }
    _atomic(committed_path, done)
    pending_path.unlink(missing_ok=True)
    next_path.unlink(missing_ok=True)
    return finish({**done, "result": "materialized"})


def read_iceberg_windows(spark, table: str) -> dict:
    """Đọc snapshot hiện tại bằng ``VERSION AS OF``. Equality-delete đã được Spark áp."""
    assert_isolated_window_table(table)
    try:
        spark.sql(f"REFRESH TABLE {table}")
        snapshots = spark.sql(
            f"""
            SELECT snapshot_id, parent_id, committed_at, operation, summary
            FROM {table}.snapshots
            ORDER BY committed_at
            """
        ).collect()
    except Exception as exc:
        if _missing_table(exc):
            return {"absent": True, "table": table}
        raise SyncFailed(str(exc).splitlines()[-1][:300]) from exc
    if not snapshots:
        return {"empty": True, "table": table, "rows": []}
    last = None
    for _ in range(8):
        spark.sql(f"REFRESH TABLE {table}")
        try:
            spark.catalog.refreshTable(table)
        except Exception:
            pass
        snapshots = spark.sql(
            f"""
            SELECT snapshot_id, parent_id, committed_at, operation, summary
            FROM {table}.snapshots
            ORDER BY committed_at
            """
        ).collect()
        latest = snapshots[-1]
        snapshot_id = int(latest.snapshot_id)
        frame = spark.sql(
            f"""
            SELECT movie_id, window_start, window_end,
                   heartbeat_count, total_watch_seconds, active_users
            FROM {table} VERSION AS OF {snapshot_id}
            """
        ).collect()
        rows = [_row(record) for record in frame]
        found = {
            "absent": False,
            "empty": not rows,
            "table": table,
            "snapshot_id": str(snapshot_id),
            "parent_id": None if latest.parent_id is None else str(int(latest.parent_id)),
            "committed_at": _as_utc(latest.committed_at).isoformat(),
            "operation": latest.operation,
            "rows": rows,
            "evidence": _delete_evidence(spark, table, snapshots),
        }
        keys = [(row["movie_id"], row["window_start"].isoformat()) for row in rows]
        if len(keys) == len(set(keys)):
            return found
        last = found
        time.sleep(2)
    return last


def assert_isolated_window_table(table: str) -> None:
    if table == PRODUCTION_WINDOWS or table.endswith(".stg_playback_windows"):
        raise SyncFailed("không sync stg_playback_windows")
    if ".it_p2" not in table and ".it_b3" not in table:
        raise SyncFailed("bảng cửa sổ phải thuộc namespace cô lập it_p2 hoặc it_b3")


def read_online(store, movie_ids: list[int]) -> list[dict]:
    payload = store.get_online_features(
        features=FEATURES,
        entity_rows=[{"movie_id": int(movie_id)} for movie_id in movie_ids],
    ).to_dict()
    width = len(next(iter(payload.values()))) if payload else 0
    rows = []
    for index in range(width):
        rows.append({key: _plain(values[index]) for key, values in payload.items()})
    return rows


def _publish(source: Path, repo_dir: Path, redis: str) -> None:
    repo_dir = Path(repo_dir)
    data = repo_dir / "data"
    data.mkdir(parents=True, exist_ok=True)
    destination = data / "movie_minute_playback.parquet"
    _write_naive(source, destination)
    shutil.copy(FEATURE_FILE, repo_dir / "features.py")
    (repo_dir / "feature_store.yaml").write_text(
        "\n".join(
            [
                "project: cinestream_streaming",
                "provider: local",
                "registry: data/registry.db",
                "online_store:",
                "  type: redis",
                "  connection_string: " + redis,
                "  skip_dedup: true",
                "offline_store:",
                "  type: duckdb",
                "",
            ]
        )
    )
    env = {
        **os.environ,
        "FEAST_USAGE": "False",
        "CINESTREAM_FEAST_WINDOW_PARQUET": str(destination),
    }
    completed = subprocess.run(
        ["feast", "apply"],
        cwd=repo_dir,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if completed.returncode != 0:
        raise SyncFailed(completed.stderr or completed.stdout)
    from feast import FeatureStore

    store = FeatureStore(repo_path=str(repo_dir))
    store.materialize(
        start_date=datetime(1970, 1, 1, tzinfo=timezone.utc),
        end_date=datetime.now(timezone.utc) + timedelta(days=1),
        feature_views=[VIEW],
        disable_event_timestamp=True,
    )


def _online_matches(repo_dir: Path, source: Path, redis: str) -> bool:
    if not source.exists() or not (Path(repo_dir) / "feature_store.yaml").exists():
        return False
    try:
        from feast import FeatureStore

        expected = _latest_rows(pd.read_parquet(source))
        store = FeatureStore(repo_path=str(repo_dir))
        found = read_online(store, [int(row.movie_id) for row in expected.itertuples()])
    except Exception:
        return False
    if len(found) != len(expected):
        return False
    for online_row, want in zip(found, expected.itertuples()):
        for column in ("heartbeat_count", "total_watch_seconds", "active_users"):
            if _plain(online_row.get(column)) != int(getattr(want, column)):
                return False
    if _redis_db(redis) != "2":
        return False
    return True


def _latest_rows(frame: pd.DataFrame) -> pd.DataFrame:
    ordered = frame.copy()
    ordered["event_timestamp"] = pd.to_datetime(ordered["event_timestamp"], utc=True)
    ordered["created_timestamp"] = pd.to_datetime(ordered["created_timestamp"], utc=True)
    ordered = ordered.sort_values(["event_timestamp", "created_timestamp"])
    return ordered.groupby("movie_id", as_index=False).tail(1)


def _version_frame(rows: list[dict], sequence: int) -> pd.DataFrame:
    records = []
    for row in rows:
        end = _as_utc(row["window_end"])
        records.append(
            {
                "movie_id": int(row["movie_id"]),
                "window_start": _as_utc(row["window_start"]),
                "event_timestamp": end,
                "created_timestamp": end + timedelta(milliseconds=sequence),
                "heartbeat_count": int(row["heartbeat_count"]),
                "total_watch_seconds": int(row["total_watch_seconds"]),
                "active_users": int(row["active_users"]),
            }
        )
    return pd.DataFrame(records)


def _append(history_path: Path, frame: pd.DataFrame) -> pd.DataFrame:
    if not history_path.exists():
        return frame
    previous = pd.read_parquet(history_path)
    return pd.concat([previous, frame], ignore_index=True)


def _write_naive(source: Path, destination: Path) -> None:
    frame = pd.read_parquet(source)
    for column in ("window_start", "event_timestamp", "created_timestamp"):
        stamps = pd.to_datetime(frame[column], utc=True)
        frame[column] = stamps.dt.tz_convert("UTC").dt.tz_localize(None)
    frame.to_parquet(destination, index=False)


def _reject_duplicates(rows: list[dict]) -> None:
    keys = [(int(row["movie_id"]), _as_utc(row["window_start"]).isoformat()) for row in rows]
    if len(keys) != len(set(keys)):
        raise SyncFailed("snapshot logic còn hai dòng cùng (movie_id, window_start)")


def _row(record) -> dict:
    return {
        "movie_id": int(record.movie_id),
        "window_start": _as_utc(record.window_start),
        "window_end": _as_utc(record.window_end),
        "heartbeat_count": int(record.heartbeat_count),
        "total_watch_seconds": int(record.total_watch_seconds),
        "active_users": int(record.active_users),
    }


def _delete_evidence(spark, table: str, snapshots) -> dict:
    evidence = {"snapshots": [], "delete_files": [], "logical_rows": None}
    for item in snapshots:
        summary = dict(item.summary or {})
        evidence["snapshots"].append(
            {
                "snapshot_id": str(int(item.snapshot_id)),
                "operation": item.operation,
                "added_equality_deletes": int(summary.get("added-equality-deletes") or 0),
                "total_equality_deletes": int(summary.get("total-equality-deletes") or 0),
            }
        )
    try:
        counted = spark.sql(f"SELECT count(*) AS n FROM {table}").collect()[0]
        evidence["logical_rows"] = int(counted.n)
    except Exception as exc:
        evidence["logical_error"] = str(exc).splitlines()[-1][:200]
    try:
        deletes = spark.sql(
            f"SELECT content, coalesce(sum(record_count), 0) AS records FROM {table}.delete_files GROUP BY content"
        ).collect()
        evidence["delete_files"] = [{"content": int(row.content), "records": int(row.records)} for row in deletes]
    except Exception as exc:
        evidence["delete_error"] = str(exc).splitlines()[-1][:200]
    try:
        data = spark.sql(f"SELECT coalesce(sum(record_count), 0) AS records FROM {table}.data_files").collect()[0]
        evidence["data_records"] = int(data.records)
    except Exception as exc:
        evidence["data_error"] = str(exc).splitlines()[-1][:200]
    return evidence


def _missing_table(exc: Exception) -> bool:
    text = str(exc)
    needles = ("TABLE_OR_VIEW_NOT_FOUND", "Table or view not found", "NoSuchTableException", "NotFoundException")
    return any(needle in text for needle in needles)


def _redis_db(redis: str) -> str | None:
    for part in str(redis).split(","):
        item = part.strip()
        if item.startswith("db="):
            return item.split("=", 1)[1]
    return None


def _as_utc(value) -> datetime:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.to_pydatetime()


def _read(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _atomic(path: Path, payload: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, default=str) + "\n")
    temporary.replace(path)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _plain(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if hasattr(value, "item"):
        value = value.item()
    return value
