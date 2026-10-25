"""Materialize một export đã ghim snapshot vào Redis.

``materialize_incremental`` của Feast 0.67 lấy mốc bắt đầu từ lần materialize
trước hoặc từ ``now - ttl``. Feature ``as_of`` nằm trong quá khứ nên cửa sổ đó
bỏ sót bản sửa cùng ngày. Workflow này materialize toàn bộ file export đã
được chấp nhận, và chỉ tiến ``committed.json`` sau khi ``get_online_features``
khớp dòng mới nhất của export.

Export có ``sequence`` nhỏ hơn bản đã commit bị từ chối trước khi ghi Redis.
``skip_dedup`` bật vì Redis của Feast so mỗi ``event_timestamp``: hai bản cùng
ngày khác ``created_timestamp`` sẽ không ghi đè nếu dedup còn bật.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from ml.feast.features import ALL_FEATURES, MOVIE_VIEW, USER_VIEW
from ml.offline.feast_export import MOVIE_TABLE, USER_TABLE

FEAST_REPO = Path(__file__).resolve().parents[1] / "feast"
VIEWS = (USER_VIEW, MOVIE_VIEW)


class StaleExport(RuntimeError):
    """Export cũ hơn publication đã lên online."""


class MaterializeFailed(RuntimeError):
    """Ghi Redis không xong. committed.json giữ nguyên."""


def materialize_export(
    export_dir: Path,
    repo_dir: Path,
    state_dir: Path,
    *,
    redis: str = "localhost:16379,db=1",
    after_write=None,
) -> dict:
    if _redis_db(redis) == "2":
        raise MaterializeFailed("materialize batch không được ghi Redis db=2")
    export_dir = Path(export_dir)
    meta_path = export_dir / "snapshot.json"
    if not meta_path.exists():
        raise MaterializeFailed("thiếu snapshot.json")
    meta = json.loads(meta_path.read_text())
    _require_files(export_dir)
    incoming = _identity(meta)
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    committed_path = state_dir / "committed.json"
    pending_path = state_dir / "pending.json"
    committed = _read(committed_path)
    if committed and committed.get("status") == "success":
        if _same_publication(committed, incoming):
            if _online_matches(repo_dir, export_dir):
                return {**committed, "result": "unchanged"}
            _write_online(export_dir, repo_dir, redis)
            if after_write:
                after_write()
            if not _online_matches(repo_dir, export_dir):
                raise MaterializeFailed("Redis không khớp export sau khi nạp lại")
            committed["reloaded_at"] = _now()
            _atomic(committed_path, committed)
            return {**committed, "result": "reloaded"}
        if int(incoming["sequence"]) < int(committed["sequence"]):
            raise StaleExport(
                f"sequence {incoming['sequence']} cũ hơn {committed['sequence']}"
            )
        if int(incoming["sequence"]) == int(committed["sequence"]):
            raise StaleExport("cùng sequence nhưng snapshot khác")
    pending = {**incoming, "status": "pending", "started_at": _now()}
    _atomic(pending_path, pending)
    try:
        _write_online(export_dir, repo_dir, redis)
        if after_write:
            after_write()
        if not _online_matches(repo_dir, export_dir):
            raise MaterializeFailed("get_online_features lệch export")
    except Exception as exc:
        if isinstance(exc, MaterializeFailed):
            raise
        raise MaterializeFailed(str(exc)) from exc
    done = {**incoming, "status": "success", "materialized_at": _now()}
    _atomic(committed_path, done)
    pending_path.unlink(missing_ok=True)
    return {**done, "result": "materialized"}


def _redis_db(redis: str) -> str | None:
    for part in str(redis).split(","):
        item = part.strip()
        if item.startswith("db="):
            return item.split("=", 1)[1]
    return None


def _write_online(export_dir: Path, repo_dir: Path, redis: str) -> None:
    _prepare_repo(repo_dir, export_dir, redis)
    _apply(repo_dir)
    from feast import FeatureStore

    store = FeatureStore(repo_path=str(repo_dir))
    store.materialize(
        start_date=datetime(1970, 1, 1, tzinfo=timezone.utc),
        end_date=datetime.now(timezone.utc) + timedelta(days=1),
        feature_views=list(VIEWS),
        disable_event_timestamp=True,
    )


def _prepare_repo(repo_dir: Path, export_dir: Path, redis: str) -> None:
    repo_dir = Path(repo_dir)
    data = repo_dir / "data"
    data.mkdir(parents=True, exist_ok=True)
    for name in (USER_TABLE, MOVIE_TABLE):
        _write_naive_timestamps(export_dir / f"{name}.parquet", data / f"{name}.parquet")
    shutil.copy(FEAST_REPO / "features.py", repo_dir / "features.py")
    (repo_dir / "feature_store.yaml").write_text(
        "\n".join(
            [
                "project: cinestream_offline",
                "provider: local",
                "registry: data/registry.db",
                "online_store:",
                "  type: redis",
                f"  connection_string: {redis}",
                "  skip_dedup: true",
                "offline_store:",
                "  type: duckdb",
                "",
            ]
        )
    )


def _write_naive_timestamps(source: Path, destination: Path) -> None:
    """Feast 0.67 materialize cast TIMESTAMPTZ_NS sang TIMESTAMP và lỗi.

    Giữ giờ UTC, bỏ timezone để DuckDB đọc được lúc materialize.
    """
    frame = pd.read_parquet(source)
    for column in ("event_timestamp", "created_timestamp"):
        stamps = pd.to_datetime(frame[column], utc=True)
        frame[column] = stamps.dt.tz_convert("UTC").dt.tz_localize(None)
    frame.to_parquet(destination, index=False)


def _apply(repo_dir: Path) -> None:
    data = repo_dir / "data"
    env = {
        **os.environ,
        "FEAST_USAGE": "False",
        "CINESTREAM_FEAST_USER_PARQUET": str(data / f"{USER_TABLE}.parquet"),
        "CINESTREAM_FEAST_MOVIE_PARQUET": str(data / f"{MOVIE_TABLE}.parquet"),
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
        raise MaterializeFailed(completed.stderr or completed.stdout)


def _online_matches(repo_dir: Path, export_dir: Path) -> bool:
    if not (repo_dir / "feature_store.yaml").exists():
        return False
    try:
        from feast import FeatureStore

        store = FeatureStore(repo_path=str(repo_dir))
        expected = _expected_online(export_dir)
        found = read_online(store, [_entity(row) for row in expected])
    except Exception:
        return False
    if not expected or len(found) != len(expected):
        return False
    for online_row, want in zip(found, expected):
        for column, value in want["values"].items():
            if not _close(online_row.get(column), value):
                return False
    return True


def read_online(store, entity_rows: list[dict]) -> list[dict]:
    payload = store.get_online_features(features=ALL_FEATURES, entity_rows=entity_rows).to_dict()
    width = len(next(iter(payload.values()))) if payload else 0
    rows = []
    for index in range(width):
        rows.append({key: _plain(values[index]) for key, values in payload.items()})
    return rows


def _expected_online(export_dir: Path) -> list[dict]:
    users = {
        int(row["user_id"]): row
        for _, row in _latest(pd.read_parquet(export_dir / f"{USER_TABLE}.parquet"), "user_id").iterrows()
    }
    movies = {
        int(row["movie_id"]): row
        for _, row in _latest(pd.read_parquet(export_dir / f"{MOVIE_TABLE}.parquet"), "movie_id").iterrows()
    }
    if not users or not movies:
        return []
    first_user = next(iter(users))
    first_movie = next(iter(movies))
    rows = []
    for user_id, user in users.items():
        rows.append(_pair(user, movies[first_movie], user_id, first_movie))
    for movie_id, movie in movies.items():
        if movie_id == first_movie:
            continue
        rows.append(_pair(users[first_user], movie, first_user, movie_id))
    return rows


def _pair(user, movie, user_id: int, movie_id: int) -> dict:
    return {
        "user_id": user_id,
        "movie_id": movie_id,
        "values": {
            "user_total_ratings_90d": user["user_total_ratings_90d"],
            "user_avg_rating_90d": user["user_avg_rating_90d"],
            "user_favorite_genre": user["user_favorite_genre"],
            "user_total_watch_hours_30d": user["user_total_watch_hours_30d"],
            "movie_rating_count_30d": movie["movie_rating_count_30d"],
            "movie_avg_rating_30d": movie["movie_avg_rating_30d"],
            "movie_completion_rate_30d": movie["movie_completion_rate_30d"],
            "movie_trending_score_7d": movie["movie_trending_score_7d"],
        },
    }


def _entity(row: dict) -> dict:
    return {"user_id": int(row["user_id"]), "movie_id": int(row["movie_id"])}


def _latest(frame: pd.DataFrame, key: str) -> pd.DataFrame:
    ordered = frame.sort_values(["event_timestamp", "created_timestamp"])
    return ordered.groupby(key, as_index=False).tail(1)


def _identity(meta: dict) -> dict:
    if "sequence" not in meta or "batch_id" not in meta:
        raise MaterializeFailed("export thiếu sequence hoặc batch_id")
    snapshots = meta.get("snapshot_ids") or {}
    if USER_TABLE not in snapshots or MOVIE_TABLE not in snapshots:
        raise MaterializeFailed("export thiếu snapshot id")
    return {
        "batch_id": str(meta["batch_id"]),
        "sequence": int(meta["sequence"]),
        "snapshot_ids": {key: str(value) for key, value in snapshots.items()},
    }


def _same_publication(committed: dict, incoming: dict) -> bool:
    return (
        int(committed["sequence"]) == int(incoming["sequence"])
        and committed["snapshot_ids"] == incoming["snapshot_ids"]
        and committed["batch_id"] == incoming["batch_id"]
    )


def _require_files(export_dir: Path) -> None:
    for name in (USER_TABLE, MOVIE_TABLE):
        path = export_dir / f"{name}.parquet"
        if not path.exists() or path.stat().st_size == 0:
            raise MaterializeFailed(f"thiếu {name}.parquet")


def _read(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _atomic(path: Path, payload: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n")
    temporary.replace(path)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _plain(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if hasattr(value, "item"):
        value = value.item()
    return value


def _close(actual, expected) -> bool:
    actual = _plain(actual)
    expected = _plain(expected)
    if expected is None:
        return actual is None
    if isinstance(expected, float) or isinstance(actual, float):
        return actual is not None and abs(float(actual) - float(expected)) < 1e-6
    return actual == expected or str(actual) == str(expected)
