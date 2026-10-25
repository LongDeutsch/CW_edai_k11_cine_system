"""Đối chiếu Feast với snapshot Iceberg file:// do chính job feature ghi.

Không ghi bronze, silver, gold và không ghi warehouse s3. Chạy::

    make feast-offline-it
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import traceback
import urllib.request
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
from feast import FeatureStore
from pyspark.sql.types import DateType, DecimalType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from ml.feast.features import historical_features
from ml.offline.feast_export import MOVIE_TABLE, USER_TABLE, assert_isolated, export_frames
from transformation.batch.common import ICEBERG_CATALOG, build_parser, start_spark
from transformation.batch.jobs.build_offline_features import publish_features
from transformation.batch.jobs.lakehouse_ddl import execute_zone
from transformation.batch.jobs.table_updates import snapshots

REPO = Path(__file__).resolve().parents[2]
FEAST_REPO = REPO / "ml" / "feast"
LOG = REPO / "docs" / "proof" / "logs" / "phase_b1_feast_offline.json"
NAMESPACE = "it_b1"
BATCH_ID = "batch-b1-file-snapshot"
A2_BATCH_ID = "batch-ad6a69a535ee5297ad2c73b10942813a"
A2_SNAPSHOTS = {
    USER_TABLE: "1992904055192348575",
    MOVIE_TABLE: "1720343794407991313",
}
AS_OF = [datetime(2026, 1, 20), datetime(2026, 2, 1)]
CREATED = datetime(2026, 1, 21, 3, 0, 0)
os.environ.setdefault("FEAST_USAGE", "False")


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="cinestream-feast-it-"))
    warehouse = (root / "warehouse").resolve()
    warehouse.mkdir()
    assert_isolated(NAMESPACE, warehouse.as_uri())
    before = _production_fingerprint()
    parser = build_parser("feast snapshot test")
    args = parser.parse_args([])
    args.warehouse = warehouse.as_uri()
    args.no_ui = True
    args.shuffle_partitions = 2
    spark = start_spark("cinestream-feast-it", args)
    report = {
        "namespace": NAMESPACE,
        "warehouse": warehouse.as_uri(),
        "batch_id": BATCH_ID,
        "historical_dp3_batch_id": A2_BATCH_ID,
        "historical_dp3_snapshots": A2_SNAPSHOTS,
        "feast_version": _feast_version(),
    }
    try:
        _expect(_catalog_is_up(before), "catalog production không đọc được")
        execute_zone(spark, "silver", NAMESPACE)
        _load_fixture(spark)
        blocked = _production_namespace_is_blocked(spark)
        first = publish_features(spark, NAMESPACE, NAMESPACE, AS_OF, CREATED)
        second = publish_features(spark, NAMESPACE, NAMESPACE, AS_OF, CREATED)
        user_table = f"{ICEBERG_CATALOG}.{NAMESPACE}.{USER_TABLE}"
        movie_table = f"{ICEBERG_CATALOG}.{NAMESPACE}.{MOVIE_TABLE}"
        user_snaps = snapshots(spark, user_table)
        movie_snaps = snapshots(spark, movie_table)
        user_snapshot = user_snaps[-1]["snapshot_id"]
        movie_snapshot = movie_snaps[-1]["snapshot_id"]
        users = _read_snapshot(spark, user_table, user_snapshot)
        movies = _read_snapshot(spark, movie_table, movie_snapshot)
        export_dir = root / "export"
        metadata = export_frames(
            export_dir,
            {USER_TABLE: users, MOVIE_TABLE: movies},
            {
                "batch_id": BATCH_ID,
                "namespace": NAMESPACE,
                "warehouse": warehouse.as_uri(),
                "historical_dp3_batch_id": A2_BATCH_ID,
                "historical_dp3_snapshots": A2_SNAPSHOTS,
                "snapshot_ids": {USER_TABLE: user_snapshot, MOVIE_TABLE: movie_snapshot},
            },
        )
        again = export_frames(
            export_dir,
            {USER_TABLE: users, MOVIE_TABLE: movies},
            metadata,
        )
        feast_rows = _retrieve(root, export_dir)
        report.update(
            {
                "status": "PASS",
                "production_namespace_blocked": blocked,
                "first_write": {"user": first["user_write"], "movie": first["movie_write"], "ready": first["ready"]},
                "second_write": {"user": second["user_write"], "movie": second["movie_write"]},
                "user_snapshots": len(user_snaps),
                "movie_snapshots": len(movie_snaps),
                "snapshot_ids": {USER_TABLE: str(user_snapshot), MOVIE_TABLE: str(movie_snapshot)},
                "export_rows": again["rows"],
                "logical_rows": {
                    USER_TABLE: int(users.drop_duplicates(["user_id", "event_timestamp", "created_timestamp"]).shape[0]),
                    MOVIE_TABLE: int(movies.drop_duplicates(["movie_id", "event_timestamp", "created_timestamp"]).shape[0]),
                },
                "feast_rows": feast_rows,
                "iceberg_sample": _sample(users, movies),
            }
        )
        _expect(blocked, "production namespace")
        _expect(first["ready"] is True and second["ready"] is True, "validation")
        _expect(first["user_write"] == "overwritten" and second["user_write"] == "unchanged", "user rerun")
        _expect(first["movie_write"] == "overwritten" and second["movie_write"] == "unchanged", "movie rerun")
        _expect(report["user_snapshots"] == 1 and report["movie_snapshots"] == 1, "snapshot count")
        _expect(again["rows"] == metadata["rows"], "export overwrite")
        _expect(again["rows"][USER_TABLE] == report["logical_rows"][USER_TABLE], "user logical rows")
        _expect_a2_row(users, movies)
        _expect_feast_matches_snapshot(feast_rows, users, movies)
    except Exception as exc:
        report["status"] = "FAIL"
        report["error"] = str(exc)
        report["traceback"] = traceback.format_exc()
    finally:
        spark.stop()
    after = _production_fingerprint()
    report["production_before"] = before
    report["production_after"] = after
    report["production_unchanged"] = before == after
    if report.get("status") == "PASS" and not report["production_unchanged"]:
        report["status"] = "FAIL"
        report["error"] = "production snapshot đổi"
    LOG.parent.mkdir(parents=True, exist_ok=True)
    LOG.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps({key: report.get(key) for key in ("status", "production_unchanged", "snapshot_ids", "first_write", "second_write", "error")}, default=str))
    if report["status"] != "PASS":
        raise SystemExit(report.get("error") or "feast snapshot it thất bại")


def _load_fixture(spark) -> None:
    rating_schema = StructType(
        [
            StructField("rating_id", LongType(), False),
            StructField("user_id", LongType(), False),
            StructField("movie_id", LongType(), False),
            StructField("rating_val", DecimalType(2, 1), False),
            StructField("rating_ts", TimestampType(), False),
            StructField("review_text", StringType(), True),
            StructField("event_date", DateType(), False),
        ]
    )
    watch_schema = StructType(
        [
            StructField("session_id", LongType(), False),
            StructField("user_id", LongType(), False),
            StructField("movie_id", LongType(), False),
            StructField("start_ts", TimestampType(), False),
            StructField("end_ts", TimestampType(), True),
            StructField("watch_duration_seconds", LongType(), False),
            StructField("completion_rate", DoubleType(), False),
            StructField("device_type", StringType(), True),
            StructField("resolution", StringType(), True),
            StructField("event_date", DateType(), False),
        ]
    )
    ratings = [
        _rating(1, 7, 11, "4.0", datetime(2026, 1, 15, 12, 0, 0)),
        _rating(2, 8, 12, "3.0", datetime(2026, 1, 18, 12, 0, 0)),
        _rating(3, 7, 11, "5.0", datetime(2026, 1, 25, 8, 0, 0)),
    ]
    watches = [_watch(1, 7, 11, datetime(2026, 1, 15, 12, 0, 0), 600, 0.5)]
    silver = f"{ICEBERG_CATALOG}.{NAMESPACE}"
    spark.createDataFrame(ratings, rating_schema).writeTo(f"{silver}.stg_ratings").append()
    spark.createDataFrame(watches, watch_schema).writeTo(f"{silver}.stg_watch_sessions").append()


def _rating(rating_id, user_id, movie_id, value, ts):
    return (rating_id, user_id, movie_id, Decimal(value), ts, None, date(ts.year, ts.month, ts.day))


def _watch(session_id, user_id, movie_id, start, seconds, completion):
    return (session_id, user_id, movie_id, start, None, seconds, completion, "tv", None, date(start.year, start.month, start.day))


def _read_snapshot(spark, table: str, snapshot_id: int) -> pd.DataFrame:
    frame = spark.sql(f"SELECT * FROM {table} VERSION AS OF {int(snapshot_id)}").toPandas()
    for column in ("event_timestamp", "created_timestamp"):
        frame[column] = pd.to_datetime(frame[column], utc=True)
    return frame


def _retrieve(root: Path, export_dir: Path) -> list[dict]:
    repo = root / "repo"
    repo.mkdir()
    data = repo / "data"
    data.mkdir()
    user_path = data / f"{USER_TABLE}.parquet"
    movie_path = data / f"{MOVIE_TABLE}.parquet"
    shutil.copy(export_dir / f"{USER_TABLE}.parquet", user_path)
    shutil.copy(export_dir / f"{MOVIE_TABLE}.parquet", movie_path)
    for name in ("feature_store.yaml", "features.py"):
        shutil.copy(FEAST_REPO / name, repo / name)
    applied = subprocess.run(
        ["feast", "apply"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "FEAST_USAGE": "False",
            "CINESTREAM_FEAST_USER_PARQUET": str(user_path),
            "CINESTREAM_FEAST_MOVIE_PARQUET": str(movie_path),
        },
    )
    if applied.returncode != 0:
        raise RuntimeError(applied.stderr)
    entities = pd.DataFrame(
        [
            _entity(7, 11, "2026-01-21 03:00:00"),
            _entity(7, 11, "2026-01-21 02:00:00"),
            _entity(7, 11, "2026-02-01 00:00:00"),
            _entity(8, 12, "2026-01-21 03:00:00"),
            _entity(8, 11, "2026-02-01 00:00:00"),
            _entity(9, 99, "2026-02-01 00:00:00"),
        ]
    )
    found = historical_features(FeatureStore(repo_path=str(repo)), entities)
    rows = []
    for _, row in found.iterrows():
        rows.append({column: _jsonable(row[column]) for column in found.columns})
    return rows


def _entity(user_id, movie_id, event):
    return {"user_id": user_id, "movie_id": movie_id, "event_timestamp": pd.Timestamp(event, tz="UTC")}


def _expect_a2_row(users: pd.DataFrame, movies: pd.DataFrame) -> None:
    user = _feature_row(users, "user_id", 7, "2026-01-20 00:00:00")
    movie = _feature_row(movies, "movie_id", 11, "2026-01-20 00:00:00")
    _expect(int(user["user_total_ratings_90d"]) == 1, "a2 user count")
    _expect(_close(user["user_avg_rating_90d"], 4.0), "a2 user average")
    _expect(pd.isna(user["user_favorite_genre"]), "a2 genre")
    _expect(_close(user["user_total_watch_hours_30d"], 600 / 3600), "a2 hours")
    _expect(int(movie["movie_rating_count_30d"]) == 1, "a2 movie count")
    _expect(_close(movie["movie_avg_rating_30d"], 4.0), "a2 movie average")
    _expect(_close(movie["movie_completion_rate_30d"], 0.5), "a2 completion")
    _expect(_close(movie["movie_trending_score_7d"], 600 / 3600), "a2 trending")


def _expect_feast_matches_snapshot(feast_rows: list[dict], users: pd.DataFrame, movies: pd.DataFrame) -> None:
    by_key = {
        (int(row["user_id"]), int(row["movie_id"]), _utc(row["event_timestamp"])): row
        for row in feast_rows
    }
    checks = (
        (7, 11, "2026-01-21 03:00:00"),
        (7, 11, "2026-01-21 02:00:00"),
        (7, 11, "2026-02-01 00:00:00"),
        (8, 12, "2026-01-21 03:00:00"),
        (8, 11, "2026-02-01 00:00:00"),
        (9, 99, "2026-02-01 00:00:00"),
    )
    for user_id, movie_id, event in checks:
        found = by_key[(user_id, movie_id, _utc(event))]
        user = _latest(users, "user_id", user_id, event)
        movie = _latest(movies, "movie_id", movie_id, event)
        _same(found, "user_total_ratings_90d", None if user is None else user["user_total_ratings_90d"])
        _same(found, "user_avg_rating_90d", None if user is None else user["user_avg_rating_90d"])
        _same(found, "movie_rating_count_30d", None if movie is None else movie["movie_rating_count_30d"])
        _same(found, "movie_trending_score_7d", None if movie is None else movie["movie_trending_score_7d"])


def _utc(value) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize("UTC")
    return stamp.tz_convert("UTC")


def _latest(frame: pd.DataFrame, key: str, value: int, event: str):
    entity_time = pd.Timestamp(event, tz="UTC")
    stamps = pd.to_datetime(frame["event_timestamp"], utc=True)
    created = pd.to_datetime(frame["created_timestamp"], utc=True)
    eligible = frame[(frame[key] == value) & (stamps <= entity_time) & (created <= entity_time)]
    if eligible.empty:
        return None
    return eligible.sort_values(["event_timestamp", "created_timestamp"]).iloc[-1]


def _same(found: dict, column: str, expected) -> None:
    actual = found[column]
    if expected is None or pd.isna(expected):
        _expect(actual is None, column)
        return
    if isinstance(expected, float) or isinstance(actual, float):
        _expect(actual is not None and abs(float(actual) - float(expected)) < 1e-6, column)
        return
    _expect(int(actual) == int(expected), column)


def _feature_row(frame: pd.DataFrame, key: str, value: int, event: str) -> pd.Series:
    stamps = pd.to_datetime(frame["event_timestamp"], utc=True)
    matched = frame[(frame[key] == value) & (stamps == pd.Timestamp(event, tz="UTC"))]
    if len(matched) != 1:
        raise AssertionError(f"{key}={value} {event} có {len(matched)} dòng")
    return matched.iloc[0]


def _sample(users: pd.DataFrame, movies: pd.DataFrame) -> dict:
    return {
        USER_TABLE: [_jsonable_row(row) for _, row in users.iterrows()],
        MOVIE_TABLE: [_jsonable_row(row) for _, row in movies.iterrows()],
    }


def _jsonable_row(row: pd.Series) -> dict:
    return {column: _jsonable(row[column]) for column in row.index}


def _jsonable(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        value = value.item()
    return value


def _production_namespace_is_blocked(spark) -> bool:
    try:
        publish_features(spark, NAMESPACE, "gold", AS_OF, CREATED)
    except RuntimeError as exc:
        return "gold" in str(exc)
    return False


def _expect(condition: bool, name: str) -> None:
    if not condition:
        raise AssertionError(name)


def _close(actual, expected) -> bool:
    return actual is not None and not pd.isna(actual) and abs(float(actual) - expected) < 1e-6


def _feast_version() -> str:
    import feast

    return feast.__version__


def _catalog_is_up(fingerprint: dict) -> bool:
    return not str(fingerprint.get("namespaces/it_af_dp/tables/raw_users", "unavailable")).startswith("unavailable")


def _production_fingerprint() -> dict:
    paths = (
        "namespaces/it_af_dp/tables/raw_users",
        "namespaces/gold/tables/fact_ratings",
        "namespaces/gold/tables/fact_watch_sessions",
        "namespaces/gold/tables/feat_user_movie_stats_daily",
        "namespaces/gold/tables/feat_movie_engagement_stats_daily",
        "namespaces/silver/tables/stg_ratings",
    )
    found = {}
    for path in paths:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/{path}", timeout=2) as response:
                metadata = json.loads(response.read().decode())["metadata"]
                found[path] = {
                    "snapshot": str(metadata.get("current-snapshot-id")),
                    "schema": metadata.get("current-schema-id"),
                }
        except Exception as exc:
            found[path] = f"unavailable:{exc.__class__.__name__}"
    return found


if __name__ == "__main__":
    main()
