"""DP3 trên warehouse file://, export đúng snapshot, materialize Redis.

Không ghi bronze, silver, gold và không ghi warehouse s3. Chạy::

    make feast-online-it
"""

from __future__ import annotations

import json
import os
import tempfile
import traceback
import urllib.request
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import redis
from feast import FeatureStore
from pyspark.sql.types import DateType, DecimalType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from ml.offline.feast_export import MOVIE_TABLE, USER_TABLE, assert_isolated, export_frames
from ml.online.materialize import StaleExport, materialize_export, read_online
from transformation.batch.common import ICEBERG_CATALOG, build_parser, start_spark
from transformation.batch.jobs.build_offline_features import publish_features
from transformation.batch.jobs.lakehouse_ddl import execute_zone
from transformation.batch.jobs.table_updates import snapshots

REPO = Path(__file__).resolve().parents[2]
LOG = REPO / "docs" / "proof" / "logs" / "phase_b2_feast_online.json"
NAMESPACE = "it_b2"
REDIS = "localhost:16379,db=1"
AS_OF_1 = [datetime(2026, 1, 20)]
CREATED_1 = datetime(2026, 1, 21, 3, 0, 0)
AS_OF_2 = [datetime(2026, 2, 1)]
CREATED_2 = datetime(2026, 2, 2, 3, 0, 0)
os.environ.setdefault("FEAST_USAGE", "False")


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="cinestream-feast-online-"))
    warehouse = (root / "warehouse").resolve()
    warehouse.mkdir()
    assert_isolated(NAMESPACE, warehouse.as_uri())
    redis.Redis.from_url("redis://localhost:16379/1").flushdb()
    before = _production_fingerprint()
    parser = build_parser("feast online test")
    args = parser.parse_args([])
    args.warehouse = warehouse.as_uri()
    args.no_ui = True
    args.shuffle_partitions = 2
    spark = start_spark("cinestream-feast-online", args)
    report = {"namespace": NAMESPACE, "warehouse": warehouse.as_uri(), "redis": REDIS}
    try:
        _expect(_catalog_is_up(before), "catalog production không đọc được")
        execute_zone(spark, "silver", NAMESPACE)
        _load_fixture(spark)
        blocked = _production_namespace_is_blocked(spark)
        first = publish_features(spark, NAMESPACE, NAMESPACE, AS_OF_1, CREATED_1)
        user_table, movie_table = _tables()
        first_ids = _current(spark, user_table, movie_table)
        first_dir = root / "export-1"
        _export(spark, first_dir, first_ids, "batch-b2-1", 1)
        online_1 = materialize_export(first_dir, root / "repo", root / "state", redis=REDIS)
        _expect_a2(spark, user_table, movie_table, first_ids)
        _append_later_rating(spark)
        second = publish_features(spark, NAMESPACE, NAMESPACE, AS_OF_2, CREATED_2)
        third = publish_features(spark, NAMESPACE, NAMESPACE, AS_OF_2, CREATED_2)
        second_ids = _current(spark, user_table, movie_table)
        second_dir = root / "export-2"
        _export(spark, second_dir, second_ids, "batch-b2-2", 2)
        online_2 = materialize_export(second_dir, root / "repo", root / "state", redis=REDIS)
        online_same = materialize_export(second_dir, root / "repo", root / "state", redis=REDIS)
        stale_error = None
        try:
            materialize_export(first_dir, root / "repo", root / "state", redis=REDIS)
        except StaleExport as exc:
            stale_error = str(exc)
        rows = read_online(
            FeatureStore(repo_path=str(root / "repo")),
            [
                {"user_id": 7, "movie_id": 11},
                {"user_id": 9, "movie_id": 99},
                {"user_id": 8, "movie_id": 12},
            ],
        )
        report.update(
            {
                "status": "PASS",
                "production_namespace_blocked": blocked,
                "first_write": first["user_write"],
                "second_write": second["user_write"],
                "third_write": third["user_write"],
                "snapshot_ids": {"first": first_ids, "second": second_ids},
                "online_first": online_1["result"],
                "online_second": online_2["result"],
                "online_rerun": online_same["result"],
                "stale_refused": stale_error,
                "online_rows": rows,
                "user_snapshots": len(snapshots(spark, user_table)),
                "movie_snapshots": len(snapshots(spark, movie_table)),
            }
        )
        _expect(blocked, "production namespace")
        _expect(first["user_write"] == "overwritten" and second["user_write"] == "overwritten", "writes")
        _expect(third["user_write"] == "unchanged" and third["movie_write"] == "unchanged", "feature rerun")
        _expect(report["user_snapshots"] == 2 and report["movie_snapshots"] == 2, "snapshot count")
        _expect(online_1["result"] == "materialized" and online_2["result"] == "materialized", "materialize")
        _expect(online_same["result"] == "unchanged", "materialize rerun")
        _expect(stale_error is not None, "stale export")
        _expect(int(rows[0]["user_total_ratings_90d"]) == 2, "online user")
        _expect(_close(rows[0]["user_avg_rating_90d"], 4.5), "online average")
        _expect(int(rows[0]["movie_rating_count_30d"]) == 2, "online movie")
        _expect(rows[1]["user_total_ratings_90d"] is None and rows[1]["movie_rating_count_30d"] is None, "missing entity")
        _expect(rows[2]["user_total_ratings_90d"] is None and rows[2]["movie_rating_count_30d"] is None, "entity absent from snapshot")
        _expect(first_ids != second_ids, "new snapshot")
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
    print(json.dumps({key: report.get(key) for key in ("status", "production_unchanged", "snapshot_ids", "online_rows", "stale_refused", "error")}, default=str))
    if report.get("status") != "PASS":
        raise SystemExit(report.get("error") or "feast online it thất bại")


def _tables() -> tuple[str, str]:
    return (
        f"{ICEBERG_CATALOG}.{NAMESPACE}.{USER_TABLE}",
        f"{ICEBERG_CATALOG}.{NAMESPACE}.{MOVIE_TABLE}",
    )


def _current(spark, user_table: str, movie_table: str) -> dict:
    return {
        USER_TABLE: str(snapshots(spark, user_table)[-1]["snapshot_id"]),
        MOVIE_TABLE: str(snapshots(spark, movie_table)[-1]["snapshot_id"]),
    }


def _export(spark, directory: Path, snapshot_ids: dict, batch_id: str, sequence: int) -> None:
    frames = {}
    for name, table in ((USER_TABLE, _tables()[0]), (MOVIE_TABLE, _tables()[1])):
        frame = spark.sql(f"SELECT * FROM {table} VERSION AS OF {int(snapshot_ids[name])}").toPandas()
        for column in ("event_timestamp", "created_timestamp"):
            frame[column] = pd.to_datetime(frame[column], utc=True)
        frames[name] = frame
    export_frames(
        directory,
        frames,
        {"batch_id": batch_id, "sequence": sequence, "namespace": NAMESPACE, "snapshot_ids": snapshot_ids},
    )


def _expect_a2(spark, user_table: str, movie_table: str, snapshot_ids: dict) -> None:
    users = spark.sql(f"SELECT * FROM {user_table} VERSION AS OF {int(snapshot_ids[USER_TABLE])}").toPandas()
    movies = spark.sql(f"SELECT * FROM {movie_table} VERSION AS OF {int(snapshot_ids[MOVIE_TABLE])}").toPandas()
    user = users[users["user_id"] == 7].iloc[0]
    movie = movies[movies["movie_id"] == 11].iloc[0]
    _expect(int(user["user_total_ratings_90d"]) == 1, "a2 count")
    _expect(_close(user["user_avg_rating_90d"], 4.0), "a2 average")
    _expect(_close(user["user_total_watch_hours_30d"], 600 / 3600), "a2 hours")
    _expect(int(movie["movie_rating_count_30d"]) == 1, "a2 movie count")
    _expect(_close(movie["movie_completion_rate_30d"], 0.5), "a2 completion")


def _load_fixture(spark) -> None:
    silver = f"{ICEBERG_CATALOG}.{NAMESPACE}"
    spark.createDataFrame([_rating(1, 7, 11, "4.0", datetime(2026, 1, 15, 12, 0, 0))], _rating_schema()).writeTo(
        f"{silver}.stg_ratings"
    ).append()
    spark.createDataFrame([_watch(1, 7, 11, datetime(2026, 1, 15, 12, 0, 0), 600, 0.5)], _watch_schema()).writeTo(
        f"{silver}.stg_watch_sessions"
    ).append()


def _append_later_rating(spark) -> None:
    silver = f"{ICEBERG_CATALOG}.{NAMESPACE}"
    spark.createDataFrame([_rating(2, 7, 11, "5.0", datetime(2026, 1, 25, 8, 0, 0))], _rating_schema()).writeTo(
        f"{silver}.stg_ratings"
    ).append()


def _rating_schema():
    return StructType(
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


def _watch_schema():
    return StructType(
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


def _rating(rating_id, user_id, movie_id, value, ts):
    return (rating_id, user_id, movie_id, Decimal(value), ts, None, date(ts.year, ts.month, ts.day))


def _watch(session_id, user_id, movie_id, start, seconds, completion):
    return (session_id, user_id, movie_id, start, None, seconds, completion, "tv", None, date(start.year, start.month, start.day))


def _production_namespace_is_blocked(spark) -> bool:
    try:
        publish_features(spark, NAMESPACE, "gold", AS_OF_1, CREATED_1)
    except RuntimeError as exc:
        return "gold" in str(exc)
    return False


def _expect(condition: bool, name: str) -> None:
    if not condition:
        raise AssertionError(name)


def _close(actual, expected) -> bool:
    return actual is not None and not pd.isna(actual) and abs(float(actual) - expected) < 1e-6


def _catalog_is_up(fingerprint: dict) -> bool:
    return not str(fingerprint.get("namespaces/it_af_dp/tables/raw_users", "unavailable")).startswith("unavailable")


def _production_fingerprint() -> dict:
    paths = (
        "namespaces/it_af_dp/tables/raw_users",
        "namespaces/gold/tables/fact_ratings",
        "namespaces/gold/tables/feat_user_movie_stats_daily",
        "namespaces/silver/tables/stg_ratings",
    )
    found = {}
    for path in paths:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/{path}", timeout=2) as response:
                metadata = json.loads(response.read().decode())["metadata"]
                found[path] = str(metadata.get("current-snapshot-id"))
        except Exception as exc:
            found[path] = f"unavailable:{exc.__class__.__name__}"
    return found


if __name__ == "__main__":
    main()
