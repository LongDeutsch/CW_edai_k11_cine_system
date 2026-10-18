"""Integration test Raw → Bronze → Silver → Gold trên warehouse file:// cách ly.

Không ghi catalog REST ``s3a://lakehouse``. Mỗi case dùng namespace ``it_*``.

Chạy::

    make spark-batch-it
"""

from __future__ import annotations

import json
import tempfile
import traceback
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

from transformation.batch.common import build_parser, start_spark
from transformation.batch.contracts.validators import ContractError, QualityThresholdExceeded
from transformation.batch.jobs.ingest_to_bronze import SimulatedTaskFailure, ingest
from transformation.batch.jobs.publish_silver_gold import publish
from transformation.batch.jobs.table_updates import snapshots

EFFECTIVE = datetime(2026, 3, 1, tzinfo=timezone.utc)
LATER = datetime(2026, 4, 1, tzinfo=timezone.utc)
EARLIER = datetime(2026, 2, 1, tzinfo=timezone.utc)
SIGNUP = datetime(2026, 1, 1, 0, 0, 0)
CREATED = datetime(2026, 1, 1, 0, 0, 0)
DAY1 = date(2026, 1, 15)
DAY2 = date(2026, 2, 15)
TS1 = datetime(2026, 1, 15, 12, 0, 0)
TS2 = datetime(2026, 2, 15, 12, 0, 0)
TS_AFTER = datetime(2026, 4, 15, 12, 0, 0)


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="cinestream-batch-it-"))
    warehouse = root / "warehouse"
    warehouse.mkdir()
    before = _production_fingerprint()
    parser = build_parser("integration test batch lakehouse")
    args = parser.parse_args([])
    args.warehouse = warehouse.resolve().as_uri()
    args.no_ui = True
    args.shuffle_partitions = 4
    args.enable_aqe = True
    spark = start_spark("cinestream-batch-it", args, enable_aqe=True)
    cases = []
    try:
        shared: dict = {}
        cases.append(_run("1 normal", lambda: case_normal(spark, root, shared)))
        cases.append(_run("2 rerun", lambda: case_rerun(spark, root)))
        cases.append(_run("3 retry", lambda: case_retry(spark, root)))
        cases.append(_run("4 quarantine", lambda: case_quarantine(spark, root)))
        cases.append(_run("5 threshold", lambda: case_threshold(spark, root)))
        cases.append(_run("6 schema", lambda: case_schema(spark, root)))
        cases.append(_run("7 backfill", lambda: case_backfill(spark, root)))
        cases.append(_run("8 scd2", lambda: case_scd(spark, root, shared)))
        cases.append(_run("9 late", lambda: case_late(spark, root)))
        cases.append(_run("10 relationships", lambda: case_relationships(spark, shared)))
    finally:
        spark.stop()
    after = _production_fingerprint()
    failed = [item["name"] for item in cases if item["status"] != "PASS"]
    report = {
        "warehouse": str(warehouse),
        "production_unchanged": before == after,
        "production_before": before,
        "production_after": after,
        "cases": cases,
        "status": "FAIL" if failed else "PASS",
        "failed": failed,
    }
    print(json.dumps(report, indent=2, default=str))
    if failed:
        raise SystemExit(f"FAIL {failed}")


def case_normal(spark, root: Path, shared: dict) -> dict:
    namespace = "it_normal"
    source = _source(root, namespace)
    _dimensions(spark, source)
    _write_ratings(spark, source / "ratings" / "dt=2026-01-15", [_rating(1, 4.0, TS1, DAY1)])
    _write_watch(spark, source / "watch_history" / "dt=2026-01-15", [_watch(1, 600, 0.5, TS1, DAY1)])
    first = ingest(spark, str(source), namespace, batch_id="normal", run_id="airflow-a")
    published = publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="normal-publish", run_id="airflow-a")
    shared["normal"] = namespace
    actual = {
        "bronze_ratings": _count(spark, namespace, "raw_ratings"),
        "silver_ratings": _count(spark, namespace, "stg_ratings"),
        "facts": _count(spark, namespace, "fact_ratings"),
        "watches": _count(spark, namespace, "fact_watch_sessions"),
        "obt": _count(spark, namespace, "obt_movie_engagement"),
        "current_users": _sql_int(spark, f"SELECT count(*) FROM cinestream.{namespace}.dim_users WHERE is_current"),
        "ingest": first["datasets"],
        "writes": published["writes"],
        "snapshots": _snap(spark, namespace, "raw_ratings", "stg_ratings", "fact_ratings", "dim_users"),
    }
    _expect(actual["bronze_ratings"] == 1, actual)
    _expect(actual["silver_ratings"] == 1 and actual["facts"] == 1, actual)
    _expect(actual["watches"] == 1 and actual["obt"] == 1, actual)
    _expect(actual["current_users"] == 2, actual)
    return {"input": "1 rating, 1 watch, 2 users, 2 movies", "expected": "mỗi grain một dòng và đủ fact", "actual": actual}


def case_rerun(spark, root: Path) -> dict:
    namespace = "it_rerun"
    source = _source(root, namespace)
    _dimensions(spark, source)
    _write_ratings(spark, source / "ratings" / "dt=2026-01-15", [_rating(1, 4.0, TS1, DAY1)])
    _write_watch(spark, source / "watch_history" / "dt=2026-01-15", [_watch(1, 600, 0.5, TS1, DAY1)])
    ingest(spark, str(source), namespace, batch_id="rerun", run_id="airflow-a")
    before = _snap(spark, namespace, "raw_ratings", "raw_users")
    users_before = _count(spark, namespace, "raw_users")
    second = ingest(spark, str(source), namespace, batch_id="rerun", run_id="airflow-b")
    after = _snap(spark, namespace, "raw_ratings", "raw_users")
    first_publish = publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="rerun-publish", run_id="airflow-a")
    gold_before = _snap(spark, namespace, "dim_users", "fact_ratings", "stg_ratings")
    second_publish = publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="rerun-publish", run_id="airflow-b")
    gold_after = _snap(spark, namespace, "dim_users", "fact_ratings", "stg_ratings")
    actual = {
        "second_ingest": second["datasets"],
        "users": _count(spark, namespace, "raw_users"),
        "users_before": users_before,
        "bronze_snapshots_unchanged": before == after,
        "publish_writes": second_publish["writes"],
        "gold_snapshots_unchanged": gold_before == gold_after,
        "first_publish_writes": first_publish["writes"],
        "snapshots": gold_after,
    }
    _expect(actual["users"] == users_before, actual)
    _expect(all(item["status"] == "skipped_duplicate" for item in second["datasets"].values()), actual)
    _expect(before == after and gold_before == gold_after, actual)
    _expect(all(status == "unchanged" for status in second_publish["writes"].values()), actual)
    return {
        "input": "cùng file, hai run_id airflow-a rồi airflow-b",
        "expected": "Bronze không thêm snapshot, publish lần hai unchanged",
        "actual": actual,
    }


def case_retry(spark, root: Path) -> dict:
    namespace = "it_retry"
    source = _source(root, namespace)
    _dimensions(spark, source)
    _write_ratings(spark, source / "ratings" / "dt=2026-01-15", [_rating(1, 4.0, TS1, DAY1)])
    _write_watch(spark, source / "watch_history" / "dt=2026-01-15", [_watch(1, 600, 0.5, TS1, DAY1)])
    failed = False
    try:
        ingest(spark, str(source), namespace, batch_id="retry", run_id="attempt-1", fail_after="users")
    except SimulatedTaskFailure:
        failed = True
    after_fail = {
        "users": _count(spark, namespace, "raw_users"),
        "ratings": _count(spark, namespace, "raw_ratings"),
    }
    ingest(spark, str(source), namespace, batch_id="retry", run_id="attempt-2")
    publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="retry-publish", run_id="attempt-2")
    actual = {
        "failed_once": failed,
        "after_fail": after_fail,
        "users": _count(spark, namespace, "raw_users"),
        "ratings": _count(spark, namespace, "raw_ratings"),
        "facts": _count(spark, namespace, "fact_ratings"),
        "snapshots": _snap(spark, namespace, "raw_users", "raw_ratings", "fact_ratings"),
    }
    _expect(failed and after_fail["users"] == 2 and after_fail["ratings"] == 0, actual)
    _expect(actual["users"] == 2 and actual["ratings"] == 1 and actual["facts"] == 1, actual)
    return {
        "input": "fail sau commit users, rồi chạy lại cùng file",
        "expected": "users không nhân đôi, ratings xuất hiện một lần",
        "actual": actual,
    }


def case_quarantine(spark, root: Path) -> dict:
    namespace = "it_quarantine"
    source = _source(root, namespace)
    _dimensions(spark, source)
    ratings = [_rating(index, value, TS1, DAY1, movie_id=movie_id, user_id=user_id) for index, (user_id, movie_id, value) in enumerate(_grains(38), start=1)]
    ratings.append(_rating(39, 9.0, TS1, DAY1))
    ratings.append((40, 7, 999, 4.0, TS1, "orphan", DAY1))
    watches = [_watch(index, 100 + index, 0.5, TS1, DAY1) for index in range(1, 39)]
    watches.append(_watch(39, -5, 0.5, TS1, DAY1))
    watches.append(_watch(40, 600, 1.5, TS1, DAY1))
    _write_ratings(spark, source / "ratings" / "dt=2026-01-15", ratings)
    _write_watch(spark, source / "watch_history" / "dt=2026-01-15", watches)
    summary = ingest(spark, str(source), namespace, batch_id="quarantine", run_id="q-1")
    codes = _pairs(
        spark,
        f"""
        SELECT error_code, count(*) AS c
        FROM cinestream.{namespace}.quarantine_records
        GROUP BY error_code
        ORDER BY error_code
        """,
    )
    quarantine_before = _count(spark, namespace, "quarantine_records")
    bronze_before = _count(spark, namespace, "raw_ratings")
    ingest(spark, str(source), namespace, batch_id="quarantine", run_id="q-2")
    (source / "ratings" / "dt=2026-01-15" / "broken.parquet").write_text("not parquet", encoding="utf-8")
    unreadable = False
    try:
        ingest(spark, str(source), namespace, batch_id="quarantine-bad", run_id="q-3")
    except ContractError:
        unreadable = True
    publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="quarantine-publish", run_id="q-2")
    actual = {
        "ingest": summary["datasets"]["ratings"],
        "watch": summary["datasets"]["watch"],
        "codes": codes,
        "bronze_ratings": _count(spark, namespace, "raw_ratings"),
        "silver_ratings": _count(spark, namespace, "stg_ratings"),
        "silver_watches": _count(spark, namespace, "stg_watch_sessions"),
        "quarantine_stable": _count(spark, namespace, "quarantine_records") >= quarantine_before,
        "quarantine_same_after_rerun": _count(spark, namespace, "quarantine_records"),
        "bronze_unchanged_by_broken_file": _count(spark, namespace, "raw_ratings") == bronze_before,
        "unreadable": unreadable,
        "bad_in_silver": _sql_int(
            spark,
            f"SELECT count(*) FROM cinestream.{namespace}.stg_ratings WHERE rating_val > 5 OR movie_id = 999",
        ),
        "snapshots": _snap(spark, namespace, "raw_ratings", "quarantine_records", "stg_ratings"),
    }
    # The rerun of the good files is skipped, then the broken file adds one quarantine row.
    _expect(actual["bronze_ratings"] == 38 and actual["silver_ratings"] == 38, actual)
    _expect(actual["silver_watches"] == 38, actual)
    _expect(actual["bad_in_silver"] == 0, actual)
    _expect(dict(codes) == {"FK_MOVIE": 1, "RATING_RANGE": 1, "NEGATIVE_DURATION": 1, "COMPLETION_RANGE": 1}, actual)
    _expect(unreadable and actual["bronze_unchanged_by_broken_file"], actual)
    _expect(actual["quarantine_same_after_rerun"] == quarantine_before + 1, actual)
    return {
        "input": "38 rating hợp lệ, 1 rating 9.0, 1 movie lạ, 1 duration âm, 1 completion 1.5, rồi một file hỏng",
        "expected": "dòng lỗi vào quarantine, Silver chỉ nhận 38 rating, file hỏng không vào Bronze",
        "actual": actual,
    }


def case_threshold(spark, root: Path) -> dict:
    namespace = "it_threshold"
    source = _source(root, namespace)
    _dimensions(spark, source)
    ratings = [_rating(index, 4.0, TS1, DAY1) for index in range(1, 9)]
    ratings.extend([_rating(9, 9.0, TS1, DAY1), _rating(10, 0.1, TS1, DAY1)])
    _write_ratings(spark, source / "ratings" / "dt=2026-01-15", ratings)
    _write_watch(spark, source / "watch_history" / "dt=2026-01-15", [_watch(1, 600, 0.5, TS1, DAY1)])
    users_before = None
    rejected = None
    try:
        ingest(spark, str(source), namespace, batch_id="threshold", run_id="t-1")
    except QualityThresholdExceeded as exc:
        rejected = {"rate": exc.error_rate, "invalid": exc.invalid_rows, "total": exc.total_rows}
        users_before = _count(spark, namespace, "raw_users")
    ingest_again = False
    try:
        ingest(spark, str(source), namespace, batch_id="threshold", run_id="t-2")
    except QualityThresholdExceeded:
        ingest_again = True
    _seed_bad_ratings(spark, namespace)
    publish_rejected = False
    try:
        publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="threshold-publish", run_id="t-3")
    except QualityThresholdExceeded:
        publish_rejected = True
    actual = {
        "rejected": rejected,
        "second_reject": ingest_again,
        "users": _count(spark, namespace, "raw_users"),
        "users_before": users_before,
        "bronze_ratings_before_seed": 0,
        "bronze_ratings": _count(spark, namespace, "raw_ratings"),
        "silver_ratings": _count(spark, namespace, "stg_ratings"),
        "facts": _count(spark, namespace, "fact_ratings"),
        "publish_rejected": publish_rejected,
        "quarantine": _count(spark, namespace, "quarantine_records"),
        "snapshots": _snap(spark, namespace, "raw_users", "raw_ratings", "stg_ratings", "fact_ratings"),
    }
    _expect(rejected is not None and rejected["invalid"] == 2 and rejected["total"] == 10, actual)
    _expect(actual["users"] == users_before == 2, actual)
    _expect(ingest_again and publish_rejected, actual)
    _expect(actual["silver_ratings"] == 0 and actual["facts"] == 0, actual)
    return {
        "input": "8 rating hợp lệ và 2 rating ngoài [0.5, 5.0], rồi seed thẳng vào Bronze",
        "expected": "Bronze ratings không nhận batch, Silver và fact vẫn trống",
        "actual": actual,
    }


def case_schema(spark, root: Path) -> dict:
    namespace = "it_schema"
    source = _source(root, namespace)
    _dimensions(spark, source, evolved=False)
    _movie(spark, source / "movies" / "v2", movie_id=12, title="With HDR", extra=True)
    _write_ratings(spark, source / "ratings" / "dt=2026-01-15", [_rating(1, 4.0, TS1, DAY1, movie_id=11)])
    _write_watch(spark, source / "watch_history" / "dt=2026-01-15", [_watch(1, 600, 0.5, TS1, DAY1)])
    summary = ingest(spark, str(source), namespace, batch_id="schema", run_id="s-1")
    publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="schema-publish", run_id="s-1")
    evolved = _pairs(
        spark,
        f"""
        SELECT movie_id, content_rating, hdr_format, CAST(audio_spatial AS STRING)
        FROM cinestream.{namespace}.raw_movies
        ORDER BY movie_id
        """,
    )
    bad_ns = "it_schema_bad"
    bad = _source(root, bad_ns)
    _dimensions(spark, bad)
    bad_frame = spark.createDataFrame(
        [(7, 11, 4.0, TS1, "x", DAY1)],
        "user_id long, movie_id long, rating_val double, rating_ts timestamp, review_text string, event_date date",
    )
    bad_frame.coalesce(1).write.mode("overwrite").parquet(str(bad / "ratings" / "dt=2026-01-15"))
    schema_failed = False
    message = ""
    try:
        ingest(spark, str(bad), bad_ns, batch_id="schema-bad", run_id="s-2")
    except ContractError as exc:
        schema_failed = True
        message = str(exc)
    actual = {
        "status": summary["datasets"]["movies"]["status"],
        "movies": evolved,
        "bronze_columns": spark.table(f"cinestream.{namespace}.raw_movies").columns,
        "silver_nulls": _sql_int(spark, f"SELECT count(*) FROM cinestream.{namespace}.stg_movies WHERE movie_id = 11 AND content_rating IS NULL"),
        "schema_failed": schema_failed,
        "schema_message": message,
        "bad_ratings": _count(spark, bad_ns, "raw_ratings"),
        "snapshots": _snap(spark, namespace, "raw_movies", "stg_movies"),
    }
    _expect(actual["status"].startswith("accepted_ignored") and "studio" in actual["status"], actual)
    _expect("studio" not in actual["bronze_columns"], actual)
    _expect(evolved[0][0] == 11 and evolved[0][1] is None, actual)
    _expect(evolved[1][0] == 12 and evolved[1][1] == "PG-13" and evolved[1][2] == "HDR10", actual)
    _expect(actual["silver_nulls"] == 1, actual)
    _expect(schema_failed and "rating_id" in message and actual["bad_ratings"] == 0, actual)
    return {
        "input": "movie cũ thiếu 3 cột, movie mới có thêm studio, và một file ratings thiếu rating_id",
        "expected": "cột cũ thành NULL, studio bị bỏ, file thiếu rating_id làm fail batch",
        "actual": actual,
    }


def case_backfill(spark, root: Path) -> dict:
    namespace = "it_backfill"
    source = _source(root, namespace)
    _dimensions(spark, source)
    _write_ratings(spark, source / "ratings" / "dt=2026-02-15", [_rating(2, 3.5, TS2, DAY2)])
    _write_watch(spark, source / "watch_history" / "dt=2026-02-15", [_watch(2, 300, 0.25, TS2, DAY2)])
    ingest(spark, str(source), namespace, batch_id="backfill-new", run_id="b-1")
    publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="backfill-publish-1", run_id="b-1")
    before = {
        "day1": _sql_int(spark, f"SELECT count(*) FROM cinestream.{namespace}.fact_ratings WHERE rating_date_key = 20260115"),
        "day2": _sql_int(spark, f"SELECT count(*) FROM cinestream.{namespace}.fact_ratings WHERE rating_date_key = 20260215"),
    }
    _write_ratings(spark, source / "ratings" / "dt=2026-01-15", [_rating(1, 4.5, TS1, DAY1)])
    ingest(spark, str(source), namespace, batch_id="backfill-old", run_id="b-2")
    publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="backfill-publish-2", run_id="b-2")
    actual = {
        "before": before,
        "day1": _sql_int(spark, f"SELECT count(*) FROM cinestream.{namespace}.fact_ratings WHERE rating_date_key = 20260115"),
        "day2": _sql_int(spark, f"SELECT count(*) FROM cinestream.{namespace}.fact_ratings WHERE rating_date_key = 20260215"),
        "rating_ids": _sql_ints(spark, f"SELECT rating_sk FROM cinestream.{namespace}.fact_ratings ORDER BY rating_sk"),
        "snapshots": _snap(spark, namespace, "raw_ratings", "fact_ratings"),
    }
    _expect(before == {"day1": 0, "day2": 1}, actual)
    _expect(actual["day1"] == 1 and actual["day2"] == 1, actual)
    _expect(actual["rating_ids"] == [1, 2], actual)
    return {
        "input": "ngày 2026-02-15 trước, sau đó bổ sung file dt=2026-01-15",
        "expected": "fact của ngày mới giữ nguyên một dòng, ngày cũ xuất hiện thêm",
        "actual": actual,
    }


def case_scd(spark, root: Path, shared: dict) -> dict:
    namespace = "it_scd"
    source = _source(root, namespace)
    _dimensions(spark, source)
    _write_ratings(
        spark,
        source / "ratings" / "dt=2026-01-15",
        [_rating(1, 4.0, TS1, DAY1), _rating(2, 5.0, TS_AFTER, date(2026, 4, 15))],
    )
    _write_watch(spark, source / "watch_history" / "dt=2026-01-15", [_watch(1, 600, 0.5, TS1, DAY1)])
    ingest(spark, str(source), namespace, batch_id="scd-1", run_id="c-1")
    publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="scd-publish-1", run_id="c-1")
    _user_file(spark, source / "users" / "tier_vip", 7, "VIP")
    _movie(spark, source / "movies" / "retitle", movie_id=11, title="Renamed", extra=True)
    ingest(spark, str(source), namespace, batch_id="scd-2", run_id="c-2")
    publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=LATER, batch_id="scd-publish-2", run_id="c-2")
    snap_before = _snap(spark, namespace, "dim_users", "dim_movies", "stg_users")
    rows_before = _count(spark, namespace, "dim_users")
    publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=LATER, batch_id="scd-publish-2", run_id="c-3")
    snap_rerun = _snap(spark, namespace, "dim_users", "dim_movies", "stg_users")
    _user_file(spark, source / "users" / "tier_standard", 7, "Standard")
    ingest(spark, str(source), namespace, batch_id="scd-3", run_id="c-4")
    bronze_tiers = _pairs(
        spark,
        f"""
        SELECT subscription_tier, CAST(_ingest_ts AS STRING)
        FROM cinestream.{namespace}.raw_users
        WHERE user_id = 7
        ORDER BY _ingest_ts
        """,
    )
    rejected = False
    try:
        publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EARLIER, batch_id="scd-publish-3", run_id="c-4")
    except ValueError:
        rejected = True
    history = _pairs(
        spark,
        f"""
        SELECT user_sk, subscription_tier, CAST(valid_from_ts AS STRING), CAST(valid_to_ts AS STRING), is_current
        FROM cinestream.{namespace}.dim_users
        WHERE user_id = 7
        ORDER BY valid_from_ts
        """,
    )
    actual = {
        "history": history,
        "movie_versions": _count(spark, namespace, "dim_movies"),
        "current_movie": _sql_int(spark, f"SELECT count(*) FROM cinestream.{namespace}.dim_movies WHERE movie_id = 11 AND is_current"),
        "rerun_same_snapshot": snap_before == snap_rerun,
        "rows_before": rows_before,
        "rows_after_reject": _count(spark, namespace, "dim_users"),
        "rejected": rejected,
        "bronze_tiers": bronze_tiers,
        "silver_tier": _sql_one(spark, f"SELECT subscription_tier FROM cinestream.{namespace}.stg_users WHERE user_id = 7"),
        "snapshots": _snap(spark, namespace, "dim_users", "dim_movies", "fact_ratings"),
    }
    shared["scd"] = namespace
    _expect(len(history) == 2 and history[0][4] is False and history[1][4] is True, actual)
    _expect(history[0][1] == "Free" and history[1][1] == "VIP", actual)
    _expect(history[0][3] == history[1][2], actual)
    _expect(history[0][0] == 7_000_001 and history[1][0] == 7_000_002, actual)
    _expect(actual["rerun_same_snapshot"] and actual["rows_after_reject"] == rows_before, actual)
    _expect(str(history[1][2]).startswith("2026-04-01"), actual)
    _expect(rejected and actual["silver_tier"] == "VIP", actual)
    _expect(actual["current_movie"] == 1, actual)
    return {
        "input": "user 7 Free, rồi VIP tại 2026-04-01, rerun, rồi Standard với mốc 2026-02-01",
        "expected": "hai phiên bản không chồng, rerun không thêm dòng, mốc lùi bị từ chối",
        "actual": actual,
    }


def case_late(spark, root: Path) -> dict:
    namespace = "it_late"
    source = _source(root, namespace)
    _dimensions(spark, source)
    _write_ratings(
        spark,
        source / "ratings" / "dt=2026-01-15",
        [_rating(1, 4.0, TS1, DAY1), _rating(9, 3.0, TS1, DAY1)],
    )
    _write_watch(spark, source / "watch_history" / "dt=2026-01-15", [_watch(1, 600, 0.5, TS1, DAY1)])
    ingest(spark, str(source), namespace, batch_id="late-1", run_id="l-1")
    publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="late-publish-1", run_id="l-1")
    _write_ratings(spark, source / "ratings" / "dt=2026-02-15", [_rating(2, 4.0, TS2, DAY2)])
    ingest(spark, str(source), namespace, batch_id="late-2", run_id="l-2")
    publish(spark, bronze_namespace=namespace, silver_namespace=namespace, gold_namespace=namespace, effective_ts=EFFECTIVE, batch_id="late-publish-2", run_id="l-2")
    actual = {
        "bronze": _count(spark, namespace, "raw_ratings"),
        "silver": _count(spark, namespace, "stg_ratings"),
        "kept": _pairs(
            spark,
            f"""
            SELECT rating_id, CAST(event_date AS STRING)
            FROM cinestream.{namespace}.stg_ratings
            WHERE user_id = 7 AND movie_id = 11 AND rating_val = 4.0
            """,
        ),
        "day1": _sql_int(spark, f"SELECT count(*) FROM cinestream.{namespace}.fact_ratings WHERE rating_date_key = 20260115"),
        "day2": _sql_int(spark, f"SELECT count(*) FROM cinestream.{namespace}.fact_ratings WHERE rating_date_key = 20260215"),
        "snapshots": _snap(spark, namespace, "raw_ratings", "stg_ratings", "fact_ratings"),
    }
    _expect(actual["bronze"] == 3 and actual["silver"] == 2, actual)
    _expect(actual["kept"] == [(2, "2026-02-15")], actual)
    _expect(actual["day1"] == 1 and actual["day2"] == 1, actual)
    return {
        "input": "rating_id 1 val 4.0 ngày 01-15, rồi rating_id 2 cùng khóa và timestamp mới hơn ngày 02-15",
        "expected": "Bronze giữ cả hai, Silver và fact chỉ giữ bản mới, rating 3.0 ở lại ngày cũ",
        "actual": actual,
    }


def case_relationships(spark, shared: dict) -> dict:
    namespace = shared["scd"]
    orphans = _sql_int(
        spark,
        f"""
        SELECT count(*)
        FROM cinestream.{namespace}.fact_ratings f
        LEFT JOIN cinestream.{namespace}.dim_users u ON f.user_sk = u.user_sk
        LEFT JOIN cinestream.{namespace}.dim_movies m ON f.movie_sk = m.movie_sk
        WHERE u.user_sk IS NULL OR m.movie_sk IS NULL
        """,
    )
    missed = _sql_int(
        spark,
        f"""
        SELECT count(*) FROM (
            SELECT r.rating_id
            FROM cinestream.{namespace}.stg_ratings r
            JOIN cinestream.{namespace}.dim_users u
              ON r.user_id = u.user_id
             AND r.rating_ts >= u.valid_from_ts
             AND (u.valid_to_ts IS NULL OR r.rating_ts < u.valid_to_ts)
            GROUP BY r.rating_id
            HAVING count(*) <> 1
        )
        """,
    )
    pointed = _pairs(
        spark,
        f"""
        SELECT f.rating_sk, f.user_sk, u.subscription_tier
        FROM cinestream.{namespace}.fact_ratings f
        JOIN cinestream.{namespace}.dim_users u ON f.user_sk = u.user_sk
        ORDER BY f.rating_sk
        """,
    )
    obt_gap = _sql_int(
        spark,
        f"""
        SELECT count(*)
        FROM cinestream.{namespace}.fact_watch_sessions f
        FULL OUTER JOIN cinestream.{namespace}.obt_movie_engagement o ON f.session_sk = o.session_sk
        WHERE f.session_sk IS NULL OR o.session_sk IS NULL
        """,
    )
    actual = {
        "orphans": orphans,
        "ratings_without_one_version": missed,
        "pointed": pointed,
        "obt_gap": obt_gap,
        "snapshots": _snap(spark, namespace, "fact_ratings", "dim_users", "obt_movie_engagement"),
    }
    _expect(orphans == 0 and missed == 0 and obt_gap == 0, actual)
    _expect(pointed == [(1, 7_000_001, "Free"), (2, 7_000_002, "VIP")], actual)
    return {
        "input": "fact và dim của case SCD2",
        "expected": "rating 01-15 trỏ Free, rating 04-15 trỏ VIP, không fact mồ côi",
        "actual": actual,
    }


def _run(name: str, fn) -> dict:
    try:
        body = fn()
    except Exception as exc:
        traceback.print_exc()
        return {"name": name, "status": "FAIL", "error": f"{exc.__class__.__name__}: {exc}"}
    body["name"] = name
    body["status"] = "PASS"
    return body


def _source(root: Path, namespace: str) -> Path:
    path = root / namespace
    path.mkdir(parents=True, exist_ok=True)
    return path


def _dimensions(spark, source: Path, evolved: bool = True) -> None:
    _user_file(spark, source / "users" / "base", 7, "Free")
    _user_file(spark, source / "users" / "second", 8, "Standard")
    _movie(spark, source / "movies" / "base", movie_id=11, title="Base", extra=evolved)
    if evolved:
        _movie(spark, source / "movies" / "other", movie_id=12, title="Other", extra=True)


def _user_file(spark, path: Path, user_id: int, tier: str) -> None:
    frame = spark.createDataFrame(
        [(user_id, SIGNUP, "VN", tier, "vi", "18-24", "F")],
        "user_id long, signup_ts timestamp, country string, subscription_tier string, preferred_language string, age_group string, gender string",
    )
    frame.coalesce(1).write.mode("overwrite").parquet(str(path))


def _movie(spark, path: Path, movie_id: int, title: str, extra: bool) -> None:
    base = [(movie_id, title, 2024, 100, "en", 1_000, 3, CREATED)]
    if not extra:
        frame = spark.createDataFrame(
            base,
            "movie_id long, title string, release_year int, runtime_minutes int, original_language string, budget long, director_id long, created_ts timestamp",
        )
    else:
        frame = spark.createDataFrame(
            [(movie_id, title, 2024, 100, "en", 1_000, 3, CREATED, "PG-13", "HDR10", True, "Cinestream")],
            "movie_id long, title string, release_year int, runtime_minutes int, original_language string, budget long, director_id long, created_ts timestamp, content_rating string, hdr_format string, audio_spatial boolean, studio string",
        )
    frame.coalesce(1).write.mode("overwrite").parquet(str(path))


def _grains(n: int) -> list[tuple[int, int, float]]:
    values = [index / 2 for index in range(1, 11)]
    combos = [(user_id, movie_id, value) for user_id in (7, 8) for movie_id in (11, 12) for value in values]
    return combos[:n]


def _rating(rating_id: int, value: float, when: datetime, day: date, movie_id: int = 11, user_id: int = 7):
    return (rating_id, user_id, movie_id, float(value), when, "ok", day)


def _watch(session_id: int, duration: int, completion: float, when: datetime, day: date):
    return (session_id, 7, 11, when, when, int(duration), float(completion), "tv", "1080p", day)


def _write_ratings(spark, path: Path, rows: list) -> None:
    frame = spark.createDataFrame(
        rows,
        "rating_id long, user_id long, movie_id long, rating_val double, rating_ts timestamp, review_text string, event_date date",
    )
    frame.coalesce(1).write.mode("overwrite").parquet(str(path))


def _write_watch(spark, path: Path, rows: list) -> None:
    frame = spark.createDataFrame(
        rows,
        "session_id long, user_id long, movie_id long, start_ts timestamp, end_ts timestamp, watch_duration_seconds int, completion_rate double, device_type string, resolution string, event_date date",
    )
    frame.coalesce(1).write.mode("overwrite").parquet(str(path))


def _seed_bad_ratings(spark, namespace: str) -> None:
    table = f"cinestream.{namespace}.raw_ratings"
    rows = []
    for index in range(1, 9):
        rows.append((index, 7, 11, 4.0, TS1, "ok", DAY1, TS1, "seed", "seed", f"seed-{index}"))
    rows.append((9, 7, 11, 9.0, TS1, "bad", DAY1, TS1, "seed", "seed", "seed-9"))
    rows.append((10, 7, 11, 0.1, TS1, "bad", DAY1, TS1, "seed", "seed", "seed-10"))
    frame = spark.createDataFrame(
        rows,
        "rating_id long, user_id long, movie_id long, rating_val double, rating_ts timestamp, review_text string, event_date date, _ingest_ts timestamp, _source_file string, _batch_id string, _file_id string",
    )
    frame.writeTo(table).append()


def _count(spark, namespace: str, table: str) -> int:
    name = f"cinestream.{namespace}.{table}"
    if not spark.catalog.tableExists(name):
        return 0
    spark.sql(f"REFRESH TABLE {name}")
    return spark.table(name).count()


def _snap(spark, namespace: str, *tables: str) -> dict:
    found = {}
    for table in tables:
        name = f"cinestream.{namespace}.{table}"
        found[table] = snapshots(spark, name) if spark.catalog.tableExists(name) else []
    return found


def _sql_int(spark, query: str) -> int:
    return int(spark.sql(query).collect()[0][0])


def _sql_ints(spark, query: str) -> list[int]:
    return [int(row[0]) for row in spark.sql(query).collect()]


def _sql_one(spark, query: str):
    return spark.sql(query).collect()[0][0]


def _pairs(spark, query: str) -> list:
    return [tuple(row) for row in spark.sql(query).collect()]


def _expect(condition: bool, actual: dict) -> None:
    if not condition:
        raise AssertionError(json.dumps(actual, default=str))


def _production_fingerprint() -> dict:
    paths = (
        "namespaces/silver/tables/stg_watch_sessions",
        "namespaces/bronze/tables/raw_users",
    )
    found = {}
    for path in paths:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/{path}", timeout=2) as response:
                found[path] = response.read().decode()
        except Exception as exc:
            found[path] = f"unavailable:{exc.__class__.__name__}"
    return found


if __name__ == "__main__":
    main()
