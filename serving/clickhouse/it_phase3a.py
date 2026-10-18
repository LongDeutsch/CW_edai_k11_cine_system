"""Integration test ClickHouse trên Iceberg REST và MinIO thật.

Namespace Iceberg ``it_ch_gold`` và database ClickHouse ``it_ch``.
Không ghi bronze, silver, gold.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from transformation.batch.common import ICEBERG_CATALOG, build_parser, start_spark
from transformation.batch.jobs.table_updates import create_or_overwrite, snapshots

from serving.clickhouse.sync_gold import (
    ServingSyncError,
    _read_snapshot,
    clickhouse,
    clickhouse_json,
    sync_published,
)

HTTP = "http://127.0.0.1:18123"
DATABASE = "it_ch"
NAMESPACE = "it_ch_gold"
MARKER_NS = "it_ch_marker"
REPO = Path(__file__).resolve().parents[2]
REPORT = REPO / "docs" / "proof" / "logs" / "phase3a_clickhouse_it.json"
PRODUCTION = (
    "namespaces/silver/tables/stg_watch_sessions",
    "namespaces/bronze/tables/raw_users",
    "namespaces/silver/tables/stg_playback_heartbeats",
    "namespaces/silver/tables/stg_playback_windows",
)
TS = datetime(2026, 1, 15, 0, 0, 0)


def main() -> None:
    _wait_clickhouse()
    before = _fingerprint()
    parser = build_parser("seed serving gold")
    args = parser.parse_args([])
    args.no_ui = True
    args.shuffle_partitions = 2
    args.warehouse = "s3a://lakehouse"
    spark = start_spark("cinestream-serving-it", args)
    cases = []
    try:
        _reset(spark)
        versions = _write_versions(spark)
        cases.append(_case_pin(spark, versions))
        cases.append(_case_initial(spark, versions))
        cases.append(_case_resync(spark, versions))
        cases.append(_case_replace(spark, versions))
        cases.append(_case_fail_then_retry(spark, versions))
        cases.append(_case_bad_markers(spark, versions))
    finally:
        spark.stop()
    after = _fingerprint()
    unchanged = before == after
    cases.append(
        {
            "name": "production untouched",
            "status": "PASS" if unchanged else "FAIL",
            "expected": "snapshot production không đổi",
            "actual": {"before": before, "after": after, "unchanged": unchanged},
        }
    )
    failed = [item["name"] for item in cases if item["status"] != "PASS"]
    report = {"status": "FAIL" if failed else "PASS", "failed": failed, "cases": cases}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    if failed:
        raise SystemExit(f"FAIL {failed}")


def _case_pin(spark, versions: dict) -> dict:
    frame, current = _read_snapshot(spark, NAMESPACE, "obt_movie_engagement", versions["v1"]["obt_movie_engagement"])
    keys = sorted(int(row.session_sk) for row in frame.select("session_sk").collect())
    actual = {
        "current_snapshot": current,
        "requested_snapshot": versions["v1"]["obt_movie_engagement"],
        "v3_snapshot": versions["v3"]["obt_movie_engagement"],
        "rows": keys,
    }
    ok = (
        current == versions["v3"]["obt_movie_engagement"]
        and keys == [1, 2]
        and current != actual["requested_snapshot"]
    )
    return _case("đọc đúng snapshot cũ", "current là v3 nhưng VERSION AS OF v1 ra session 1 và 2", actual, ok)


def _case_initial(spark, versions: dict) -> dict:
    _put_marker("it-ch-v1", versions["v1"])
    result = sync_published(
        spark,
        batch_id="it-ch-v1",
        marker_namespace=MARKER_NS,
        http=HTTP,
        database=DATABASE,
    )
    rows = _engagement()
    ratings = _ratings()
    actual = {"sync": result, "engagement": rows, "ratings": ratings}
    ok = (
        result["status"] == "published"
        and result["current_snapshots"]["obt_movie_engagement"] == versions["v3"]["obt_movie_engagement"]
        and result["requested_snapshots"]["obt_movie_engagement"] == versions["v1"]["obt_movie_engagement"]
        and result["current_snapshots"]["obt_movie_engagement"] != result["requested_snapshots"]["obt_movie_engagement"]
        and rows == [{"session_sk": 1, "movie_id": 10, "completion_rate": 0.5}, {"session_sk": 2, "movie_id": 20, "completion_rate": 1.0}]
        and ratings == [{"movie_id": 10, "rating_count": 1, "avg_rating": 4.0}, {"movie_id": 20, "rating_count": 1, "avg_rating": 5.0}]
        and result["serving"]["log_rows"] == 1
    )
    return _case("nạp snapshot đầu", "serving là v1 dù current Iceberg là v3", actual, ok)


def _case_resync(spark, versions: dict) -> dict:
    before = {"engagement": _engagement(), "ratings": _ratings(), "log": _log_count()}
    result = sync_published(
        spark,
        batch_id="it-ch-v1",
        marker_namespace=MARKER_NS,
        http=HTTP,
        database=DATABASE,
    )
    after = {"engagement": _engagement(), "ratings": _ratings(), "log": _log_count()}
    ok = result["status"] == "unchanged" and before == after and after["log"] == 1
    return _case("sync lại cùng snapshot", "dòng và sync_log không đổi", {"before": before, "after": after, "sync": result}, ok)


def _case_replace(spark, versions: dict) -> dict:
    _put_marker("it-ch-v2", versions["v2"])
    result = sync_published(
        spark,
        batch_id="it-ch-v2",
        marker_namespace=MARKER_NS,
        http=HTTP,
        database=DATABASE,
    )
    rows = _engagement()
    ratings = _ratings()
    ok = (
        result["status"] == "published"
        and rows == [{"session_sk": 1, "movie_id": 10, "completion_rate": 0.9}, {"session_sk": 3, "movie_id": 30, "completion_rate": 0.2}]
        and ratings == [{"movie_id": 10, "rating_count": 1, "avg_rating": 3.0}, {"movie_id": 30, "rating_count": 1, "avg_rating": 2.0}]
        and _log_count() == 2
    )
    return _case(
        "snapshot mới có thêm, sửa, xóa",
        "session 2 và movie 20 biến mất, session 1 đổi completion, session 3 và movie 30 xuất hiện",
        {"sync": result, "engagement": rows, "ratings": ratings},
        ok,
    )


def _case_fail_then_retry(spark, versions: dict) -> dict:
    _put_marker("it-ch-v3", versions["v3"])
    visible_before = {"engagement": _engagement(), "ratings": _ratings(), "log": _log_count()}
    try:
        sync_published(
            spark,
            batch_id="it-ch-v3",
            marker_namespace=MARKER_NS,
            http=HTTP,
            database=DATABASE,
            fail_after_rows=1,
        )
        failed = {"error": "không dừng"}
    except ServingSyncError as exc:
        failed = {"error": str(exc)}
    during = {
        "engagement": _engagement(),
        "ratings": _ratings(),
        "log": _log_count(),
        "stage_rows": int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.movie_engagement_stage").strip()),
    }
    held = during["engagement"] == visible_before["engagement"] and during["ratings"] == visible_before["ratings"] and during["log"] == 2
    result = sync_published(
        spark,
        batch_id="it-ch-v3",
        marker_namespace=MARKER_NS,
        http=HTTP,
        database=DATABASE,
    )
    rows = _engagement()
    ok = (
        "dừng giữa bulk load" in failed.get("error", "")
        and held
        and during["stage_rows"] == 1
        and result["status"] == "published"
        and [item["session_sk"] for item in rows] == [1, 3, 4]
        and _log_count() == 3
    )
    return _case(
        "bulk load lỗi rồi retry",
        "serving giữ v2 khi stage dở, retry xong mới thấy session 4",
        {"failed": failed, "during": during, "retry": result, "engagement": rows},
        ok,
    )


def _case_bad_markers(spark, versions: dict) -> dict:
    before = {"engagement": _engagement(), "ratings": _ratings(), "log": _log_count()}
    _put_marker("it-ch-draft", versions["v3"], published=False)
    _put_marker("it-ch-nosnap", versions["v3"], include_snapshots=False)
    bad = dict(versions["v3"])
    bad["obt_movie_engagement"] = 1
    _put_marker("it-ch-badsnap", bad)
    found = {}
    for batch_id, needle in (
        ("it-ch-missing", "không có marker"),
        ("it-ch-draft", "marker chưa published"),
        ("it-ch-nosnap", "marker thiếu gold_snapshots"),
        ("it-ch-badsnap", "không có snapshot"),
    ):
        try:
            sync_published(spark, batch_id=batch_id, marker_namespace=MARKER_NS, http=HTTP, database=DATABASE)
            found[batch_id] = "published"
        except ServingSyncError as exc:
            found[batch_id] = str(exc)
    after = {"engagement": _engagement(), "ratings": _ratings(), "log": _log_count()}
    ok = before == after and all(needle in found[batch_id] for batch_id, needle in (
        ("it-ch-missing", "không có marker"),
        ("it-ch-draft", "marker chưa published"),
        ("it-ch-nosnap", "marker thiếu gold_snapshots"),
        ("it-ch-badsnap", "không có snapshot"),
    ))
    return _case("marker hỏng hoặc thiếu", "bốn trường hợp bị từ chối, serving vẫn là v3", {"errors": found, "before": before, "after": after}, ok)


def _write_versions(spark) -> dict:
    versions = {}
    for name, builder in (("v1", _v1), ("v2", _v2), ("v3", _v3)):
        frames = builder(spark)
        for table, frame in frames.items():
            create_or_overwrite(spark, frame, f"{ICEBERG_CATALOG}.{NAMESPACE}.{table}")
        versions[name] = {
            table: snapshots(spark, f"{ICEBERG_CATALOG}.{NAMESPACE}.{table}")[-1]["snapshot_id"]
            for table in frames
        }
    return versions


def _v1(spark):
    return _frames(
        spark,
        movies=[(1, 10, "A", "PG"), (2, 20, "B", "R")],
        ratings=[(100, 1, Decimal("4.0")), (101, 2, Decimal("5.0"))],
        sessions=[(1, 10, "A", "PG", 60, 0.5), (2, 20, "B", "R", 30, 1.0)],
    )


def _v2(spark):
    return _frames(
        spark,
        movies=[(1, 10, "A", "PG"), (2, 20, "B", "R"), (3, 30, "C", "PG")],
        ratings=[(100, 1, Decimal("3.0")), (102, 3, Decimal("2.0"))],
        sessions=[(1, 10, "A", "PG", 60, 0.9), (3, 30, "C", "PG", 10, 0.2)],
    )


def _v3(spark):
    return _frames(
        spark,
        movies=[(1, 10, "A", "PG"), (2, 20, "B", "R"), (3, 30, "C", "PG"), (4, 40, "D", "G")],
        ratings=[(100, 1, Decimal("3.0")), (102, 3, Decimal("2.0")), (103, 4, Decimal("4.5"))],
        sessions=[(1, 10, "A", "PG", 60, 0.9), (3, 30, "C", "PG", 10, 0.2), (4, 40, "D", "G", 5, 0.1)],
    )


def _frames(spark, movies, ratings, sessions):
    from pyspark.sql.types import (
        BooleanType,
        DecimalType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    dim = spark.createDataFrame(
        [
            (sk, movie_id, title, 2020, rating, None, TS, None, True)
            for sk, movie_id, title, rating in movies
        ],
        StructType(
            [
                StructField("movie_sk", LongType(), False),
                StructField("movie_id", LongType(), False),
                StructField("title", StringType(), True),
                StructField("release_year", IntegerType(), True),
                StructField("content_rating", StringType(), True),
                StructField("hdr_format", StringType(), True),
                StructField("valid_from_ts", TimestampType(), False),
                StructField("valid_to_ts", TimestampType(), True),
                StructField("is_current", BooleanType(), False),
            ]
        ),
    )
    facts = spark.createDataFrame(
        [(sk, 1, movie_sk, value, 20260115, TS) for sk, movie_sk, value in ratings],
        StructType(
            [
                StructField("rating_sk", LongType(), False),
                StructField("user_sk", LongType(), False),
                StructField("movie_sk", LongType(), False),
                StructField("rating_val", DecimalType(2, 1), False),
                StructField("rating_date_key", IntegerType(), False),
                StructField("created_ts", TimestampType(), False),
            ]
        ),
    )
    obt = spark.createDataFrame(
        [(sk, 20260115, 7, "Free", movie_id, title, rating, duration, rate, "tv") for sk, movie_id, title, rating, duration, rate in sessions],
        StructType(
            [
                StructField("session_sk", LongType(), False),
                StructField("session_date_key", IntegerType(), False),
                StructField("user_id", LongType(), False),
                StructField("subscription_tier", StringType(), True),
                StructField("movie_id", LongType(), False),
                StructField("title", StringType(), True),
                StructField("content_rating", StringType(), True),
                StructField("watch_duration_seconds", IntegerType(), True),
                StructField("completion_rate", DoubleType(), True),
                StructField("device_type", StringType(), True),
            ]
        ),
    )
    return {"dim_movies": dim, "fact_ratings": facts, "obt_movie_engagement": obt}


def _reset(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{NAMESPACE}")
    for table in ("dim_movies", "fact_ratings", "obt_movie_engagement"):
        spark.sql(f"DROP TABLE IF EXISTS {ICEBERG_CATALOG}.{NAMESPACE}.{table}")
    clickhouse(HTTP, f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
    _delete_markers()


def _put_marker(batch_id: str, snaps: dict, *, published: bool = True, include_snapshots: bool = True) -> None:
    payload = {
        "published": published,
        "batch_id": batch_id,
        "namespaces": {"bronze": MARKER_NS, "silver": NAMESPACE, "gold": NAMESPACE},
    }
    if include_snapshots:
        payload["gold_snapshots"] = snaps
    client = _s3()
    client.put_object(
        Bucket="lakehouse",
        Key=f"_orchestration/{MARKER_NS}/{batch_id}.json",
        Body=json.dumps(payload).encode(),
        ContentType="application/json",
    )


def _delete_markers() -> None:
    client = _s3()
    listed = client.list_objects_v2(Bucket="lakehouse", Prefix=f"_orchestration/{MARKER_NS}/")
    keys = [{"Key": item["Key"]} for item in listed.get("Contents") or []]
    if keys:
        client.delete_objects(Bucket="lakehouse", Delete={"Objects": keys})


def _s3():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _engagement() -> list[dict]:
    rows = clickhouse_json(
        HTTP,
        f"SELECT session_sk, movie_id, completion_rate FROM {DATABASE}.movie_engagement ORDER BY session_sk",
    )
    return [
        {
            "session_sk": int(row["session_sk"]),
            "movie_id": int(row["movie_id"]),
            "completion_rate": round(float(row["completion_rate"]), 4),
        }
        for row in rows
    ]


def _ratings() -> list[dict]:
    rows = clickhouse_json(
        HTTP,
        f"SELECT movie_id, rating_count, avg_rating FROM {DATABASE}.movie_ratings ORDER BY movie_id",
    )
    return [
        {
            "movie_id": int(row["movie_id"]),
            "rating_count": int(row["rating_count"]),
            "avg_rating": round(float(row["avg_rating"]), 4),
        }
        for row in rows
    ]


def _log_count() -> int:
    return int(clickhouse(HTTP, f"SELECT count() FROM {DATABASE}.sync_log").strip())


def _fingerprint() -> dict:
    found = {}
    for path in PRODUCTION:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/{path}", timeout=5) as response:
                meta = json.loads(response.read().decode())["metadata"]
                found[path] = meta.get("current-snapshot-id")
        except urllib.error.HTTPError as exc:
            found[path] = f"http:{exc.code}"
        except Exception as exc:
            found[path] = f"unavailable:{exc.__class__.__name__}"
    return found


def _wait_clickhouse() -> None:
    deadline = __import__("time").time() + 60
    while __import__("time").time() < deadline:
        try:
            clickhouse(HTTP, "SELECT 1")
            return
        except Exception:
            __import__("time").sleep(2)
    raise SystemExit("ClickHouse chưa sẵn sàng. Chạy make serving-up trước.")


def _case(name: str, expected: str, actual: dict, ok: bool) -> dict:
    status = "PASS" if ok else "FAIL"
    print(f"CASE {status} {name}", flush=True)
    return {"name": name, "status": status, "expected": expected, "actual": actual}


if __name__ == "__main__":
    main()
