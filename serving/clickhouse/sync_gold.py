"""Đồng bộ Gold Iceberg đã publish sang ClickHouse.

Chỉ đọc snapshot ghi trong marker Airflow. Không lấy snapshot mới nhất của bảng
khi marker không nêu id đó. Dữ liệu được nạp vào bảng ``_stage``. Người đọc
chỉ thấy bản mới sau một câu ``RENAME`` đổi cả hai bảng serving. Insert và
``RENAME`` không nằm trong một transaction. ``sync_log`` chỉ ghi sau ``RENAME``.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from transformation.batch.common import ICEBERG_CATALOG
from transformation.batch.contracts.validators import check_batch_id, check_identifier

SNAPSHOT_TABLES = ("dim_movies", "fact_ratings", "obt_movie_engagement")
READER_TABLES = ("movie_engagement", "movie_ratings")
_DDL = Path(__file__).with_name("ddl.sql")


class ServingSyncError(RuntimeError):
    """Marker, snapshot hoặc bước nạp không đủ để phát hành serving."""


def apply_ddl(http: str, database: str, password: str = "cinestream") -> None:
    name = check_identifier(database, "database")
    sql = _DDL.read_text().replace("__DATABASE__", name)
    for statement in sql.split(";"):
        lines = [line for line in statement.splitlines() if not line.strip().startswith("--")]
        text = "\n".join(lines).strip()
        if text:
            clickhouse(http, text, password=password)


def sync_published(
    spark,
    *,
    batch_id: str,
    marker_namespace: str,
    http: str = "http://127.0.0.1:18123",
    database: str = "cinestream_serving",
    password: str = "cinestream",
    bucket: str = "lakehouse",
    s3_endpoint: str = "http://127.0.0.1:9000",
    fail_after_rows: int | None = None,
    fail_before_log: bool = False,
) -> dict:
    """Nạp snapshot trong marker. Cùng snapshot đã phục vụ thì không ghi lại."""
    batch_id = check_batch_id(batch_id)
    marker_namespace = check_identifier(marker_namespace, "marker-namespace")
    database = check_identifier(database, "database")
    marker = read_marker(batch_id, marker_namespace, bucket=bucket, s3_endpoint=s3_endpoint)
    gold = _require_marker(marker, batch_id)
    requested = {name: int(marker["gold_snapshots"][name]) for name in SNAPSHOT_TABLES}
    apply_ddl(http, database, password)
    if _checkpoint_matches(http, database, password, batch_id, gold, requested):
        serving = _serving_view(http, database, password)
        return {
            "status": "unchanged",
            "batch_id": batch_id,
            "gold_namespace": gold,
            "requested_snapshots": requested,
            "serving": serving,
        }
    current = {}
    frames = {}
    for name in SNAPSHOT_TABLES:
        frames[name], current[name] = _read_snapshot(spark, gold, name, requested[name])
    engagement = _engagement_rows(frames["obt_movie_engagement"], requested["obt_movie_engagement"], batch_id)
    ratings = _rating_rows(spark, frames["fact_ratings"], frames["dim_movies"], requested["fact_ratings"], batch_id)
    _validate_source(frames["fact_ratings"], frames["obt_movie_engagement"], engagement, ratings)
    if _readers_already_published(http, database, password, batch_id, requested, len(engagement), len(ratings)):
        _insert_log(http, database, password, batch_id, gold, marker_namespace, requested, len(engagement), len(ratings))
        return {
            "status": "logged",
            "batch_id": batch_id,
            "gold_namespace": gold,
            "requested_snapshots": requested,
            "current_snapshots": current,
            "engagement_rows": len(engagement),
            "rating_rows": len(ratings),
            "serving": _serving_view(http, database, password),
        }
    _truncate_stage(http, database, password)
    _insert(http, database, password, "movie_engagement_stage", engagement, fail_after_rows)
    _insert(http, database, password, "movie_ratings_stage", ratings, None)
    _validate_stage(http, database, password, engagement, ratings)
    _publish(http, database, password)
    if fail_before_log:
        raise ServingSyncError("dừng trước sync_log")
    _insert_log(http, database, password, batch_id, gold, marker_namespace, requested, len(engagement), len(ratings))
    return {
        "status": "published",
        "batch_id": batch_id,
        "gold_namespace": gold,
        "requested_snapshots": requested,
        "current_snapshots": current,
        "engagement_rows": len(engagement),
        "rating_rows": len(ratings),
        "serving": _serving_view(http, database, password),
    }


def read_marker(batch_id: str, namespace: str, *, bucket: str, s3_endpoint: str) -> dict:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    key = f"_orchestration/{namespace}/{batch_id}.json"
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"NoSuchKey", "404"}:
            raise ServingSyncError(f"không có marker s3://{bucket}/{key}") from exc
        raise
    return json.loads(body.decode())


def _require_marker(marker: dict, batch_id: str) -> str:
    if marker.get("published") is not True:
        raise ServingSyncError("marker chưa published")
    if marker.get("batch_id") not in (None, batch_id):
        raise ServingSyncError("batch_id của marker không khớp")
    snapshots = marker.get("gold_snapshots")
    if not isinstance(snapshots, dict) or any(name not in snapshots for name in SNAPSHOT_TABLES):
        raise ServingSyncError("marker thiếu gold_snapshots")
    namespaces = marker.get("namespaces") or {}
    gold = namespaces.get("gold")
    if not gold:
        raise ServingSyncError("marker thiếu namespace gold")
    return check_identifier(str(gold), "gold")


def _read_snapshot(spark, namespace: str, table: str, snapshot_id: int):
    full = f"{ICEBERG_CATALOG}.{namespace}.{table}"
    spark.sql(f"REFRESH TABLE {full}")
    known = {int(row.snapshot_id) for row in spark.sql(f"SELECT snapshot_id FROM {full}.snapshots").collect()}
    if int(snapshot_id) not in known:
        raise ServingSyncError(f"{full} không có snapshot {snapshot_id}")
    current = int(
        spark.sql(f"SELECT snapshot_id FROM {full}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0].snapshot_id
    )
    frame = spark.sql(f"SELECT * FROM {full} VERSION AS OF {int(snapshot_id)}")
    return frame, current


def _engagement_rows(frame, snapshot_id: int, batch_id: str) -> list[dict]:
    columns = (
        "session_sk",
        "session_date_key",
        "user_id",
        "subscription_tier",
        "movie_id",
        "title",
        "content_rating",
        "watch_duration_seconds",
        "completion_rate",
        "device_type",
    )
    missing = [name for name in columns if name not in frame.columns]
    if missing:
        raise ServingSyncError(f"obt_movie_engagement thiếu cột {missing}")
    rows = []
    for item in frame.select(*columns).collect():
        row = {name: item[name] for name in columns}
        row["session_sk"] = int(row["session_sk"])
        row["session_date_key"] = int(row["session_date_key"])
        row["user_id"] = int(row["user_id"])
        row["movie_id"] = int(row["movie_id"])
        if row["watch_duration_seconds"] is not None:
            row["watch_duration_seconds"] = int(row["watch_duration_seconds"])
        if row["completion_rate"] is not None:
            row["completion_rate"] = float(row["completion_rate"])
        row["iceberg_snapshot_id"] = int(snapshot_id)
        row["batch_id"] = batch_id
        rows.append(row)
    return rows


def _rating_rows(spark, facts, movies, snapshot_id: int, batch_id: str) -> list[dict]:
    for name in ("movie_sk", "rating_val"):
        if name not in facts.columns:
            raise ServingSyncError(f"fact_ratings thiếu cột {name}")
    for name in ("movie_sk", "movie_id", "title", "content_rating"):
        if name not in movies.columns:
            raise ServingSyncError(f"dim_movies thiếu cột {name}")
    facts.createOrReplaceTempView("_serving_fact_ratings")
    movies.createOrReplaceTempView("_serving_dim_movies")
    summary = spark.sql(
        """
        SELECT
            d.movie_id AS movie_id,
            d.title AS title,
            d.content_rating AS content_rating,
            count(*) AS rating_count,
            avg(f.rating_val) AS avg_rating
        FROM _serving_fact_ratings f
        JOIN _serving_dim_movies d ON f.movie_sk = d.movie_sk
        GROUP BY d.movie_id, d.title, d.content_rating
        """
    )
    rows = []
    for item in summary.collect():
        rows.append(
            {
                "movie_id": int(item["movie_id"]),
                "title": item["title"],
                "content_rating": item["content_rating"],
                "rating_count": int(item["rating_count"]),
                "avg_rating": float(item["avg_rating"]),
                "iceberg_snapshot_id": int(snapshot_id),
                "batch_id": batch_id,
            }
        )
    return rows


def _validate_source(facts, obt, engagement: list[dict], ratings: list[dict]) -> None:
    fact_count = facts.count()
    obt_count = obt.count()
    if len(engagement) != obt_count:
        raise ServingSyncError(f"engagement {len(engagement)} lệch obt {obt_count}")
    if len({row["session_sk"] for row in engagement}) != len(engagement):
        raise ServingSyncError("session_sk bị trùng trong snapshot")
    joined = sum(row["rating_count"] for row in ratings)
    if joined != fact_count:
        raise ServingSyncError(f"tổng rating {joined} lệch fact_ratings {fact_count}")
    if len({row["movie_id"] for row in ratings}) != len(ratings):
        raise ServingSyncError("một movie_id có nhiều title trong snapshot")


def _validate_stage(http: str, database: str, password: str, engagement: list[dict], ratings: list[dict]) -> None:
    eng = int(_scalar(http, database, password, "SELECT count() FROM movie_engagement_stage"))
    rat = int(_scalar(http, database, password, "SELECT count() FROM movie_ratings_stage"))
    eng_keys = int(_scalar(http, database, password, "SELECT count(DISTINCT session_sk) FROM movie_engagement_stage"))
    rat_keys = int(_scalar(http, database, password, "SELECT count(DISTINCT movie_id) FROM movie_ratings_stage"))
    if eng != len(engagement) or eng_keys != len(engagement):
        raise ServingSyncError(f"stage engagement {eng} khóa {eng_keys}, nguồn {len(engagement)}")
    if rat != len(ratings) or rat_keys != len(ratings):
        raise ServingSyncError(f"stage ratings {rat} khóa {rat_keys}, nguồn {len(ratings)}")


def _truncate_stage(http: str, database: str, password: str) -> None:
    clickhouse(http, f"TRUNCATE TABLE {database}.movie_engagement_stage", password=password)
    clickhouse(http, f"TRUNCATE TABLE {database}.movie_ratings_stage", password=password)


def _insert(http: str, database: str, password: str, table: str, rows: list[dict], fail_after: int | None) -> None:
    sent = 0
    chunk: list[dict] = []
    for row in rows:
        if fail_after is not None and sent >= fail_after:
            raise ServingSyncError("dừng giữa bulk load")
        chunk.append(row)
        if len(chunk) == 1000:
            _flush(http, database, password, table, chunk)
            sent += len(chunk)
            chunk = []
    if chunk:
        if fail_after is not None and sent + len(chunk) > fail_after:
            head = chunk[: fail_after - sent]
            if head:
                _flush(http, database, password, table, head)
            raise ServingSyncError("dừng giữa bulk load")
        _flush(http, database, password, table, chunk)


def _flush(http: str, database: str, password: str, table: str, rows: list[dict]) -> None:
    body = "\n".join(json.dumps(row, default=_json) for row in rows).encode()
    clickhouse(http, f"INSERT INTO {database}.{table} FORMAT JSONEachRow", body=body, password=password)


def _publish(http: str, database: str, password: str) -> None:
    """Một câu RENAME trên database Atomic. Hai bảng reader đổi cùng lúc hoặc không đổi."""
    clickhouse(http, f"DROP TABLE IF EXISTS {database}.movie_engagement_old", password=password)
    clickhouse(http, f"DROP TABLE IF EXISTS {database}.movie_ratings_old", password=password)
    clickhouse(
        http,
        f"""
        RENAME TABLE
            {database}.movie_engagement TO {database}.movie_engagement_old,
            {database}.movie_engagement_stage TO {database}.movie_engagement,
            {database}.movie_ratings TO {database}.movie_ratings_old,
            {database}.movie_ratings_stage TO {database}.movie_ratings
        """,
        password=password,
    )
    clickhouse(
        http,
        f"""
        RENAME TABLE
            {database}.movie_engagement_old TO {database}.movie_engagement_stage,
            {database}.movie_ratings_old TO {database}.movie_ratings_stage
        """,
        password=password,
    )


def _insert_log(http, database, password, batch_id, gold, marker_namespace, requested, engagement_rows, rating_rows) -> None:
    clickhouse(
        http,
        f"""
        INSERT INTO {database}.sync_log
            (batch_id, gold_namespace, marker_namespace, dim_movies_snapshot,
             fact_ratings_snapshot, obt_snapshot, engagement_rows, rating_rows, published_at)
        VALUES (
            '{batch_id}', '{gold}', '{marker_namespace}',
            {requested['dim_movies']}, {requested['fact_ratings']}, {requested['obt_movie_engagement']},
            {engagement_rows}, {rating_rows}, now64(3, 'UTC')
        )
        """,
        password=password,
    )


def _readers_already_published(http, database, password, batch_id, requested, engagement_rows: int, rating_rows: int) -> bool:
    """Reader đã là snapshot của marker nhưng sync_log chưa ghi. Không RENAME lần nữa."""
    serving = _serving_view(http, database, password)
    if serving["engagement_rows"] != engagement_rows or serving["rating_rows"] != rating_rows:
        return False
    if serving["engagement_snapshots"] != [requested["obt_movie_engagement"]]:
        return False
    if serving["rating_snapshots"] != [requested["fact_ratings"]]:
        return False
    engagement_batches = _text_ids(http, database, password, "movie_engagement")
    rating_batches = _text_ids(http, database, password, "movie_ratings")
    return engagement_batches == [batch_id] and rating_batches == [batch_id]


def _text_ids(http, database, password, table: str) -> list[str]:
    rows = clickhouse_json(
        http,
        f"SELECT DISTINCT batch_id FROM {database}.{table} ORDER BY batch_id",
        password=password,
    )
    return [str(row["batch_id"]) for row in rows]


def _checkpoint_matches(http, database, password, batch_id, gold, requested) -> bool:
    rows = clickhouse_json(
        http,
        f"""
        SELECT engagement_rows, rating_rows
        FROM {database}.sync_log
        WHERE batch_id = '{batch_id}'
          AND gold_namespace = '{gold}'
          AND dim_movies_snapshot = {requested['dim_movies']}
          AND fact_ratings_snapshot = {requested['fact_ratings']}
          AND obt_snapshot = {requested['obt_movie_engagement']}
        """,
        password=password,
    )
    if len(rows) != 1:
        return False
    expected = rows[0]
    serving = _serving_view(http, database, password)
    return (
        serving["engagement_rows"] == int(expected["engagement_rows"])
        and serving["rating_rows"] == int(expected["rating_rows"])
        and serving["engagement_snapshots"] == [requested["obt_movie_engagement"]]
        and serving["rating_snapshots"] == [requested["fact_ratings"]]
    )


def _serving_view(http, database, password) -> dict:
    return {
        "engagement_rows": int(_scalar(http, database, password, "SELECT count() FROM movie_engagement")),
        "rating_rows": int(_scalar(http, database, password, "SELECT count() FROM movie_ratings")),
        "engagement_snapshots": _snapshot_ids(http, database, password, "movie_engagement"),
        "rating_snapshots": _snapshot_ids(http, database, password, "movie_ratings"),
        "stage_engagement_rows": int(_scalar(http, database, password, "SELECT count() FROM movie_engagement_stage")),
        "log_rows": int(_scalar(http, database, password, "SELECT count() FROM sync_log")),
    }


def _snapshot_ids(http, database, password, table: str) -> list[int]:
    rows = clickhouse_json(
        http,
        f"SELECT DISTINCT iceberg_snapshot_id FROM {database}.{table} ORDER BY iceberg_snapshot_id",
        password=password,
    )
    return [int(row["iceberg_snapshot_id"]) for row in rows]


def _scalar(http, database, password, query: str) -> str:
    return clickhouse(http, _qualify(database, query), password=password).strip()


def _qualify(database: str, query: str) -> str:
    return (
        query.replace("FROM movie_", f"FROM {database}.movie_")
        .replace("FROM sync_log", f"FROM {database}.sync_log")
    )


def clickhouse(http: str, query: str, body: bytes | None = None, password: str = "cinestream") -> str:
    params = urllib.parse.urlencode({"user": "default", "password": password, "query": query})
    request = urllib.request.Request(f"{http.rstrip('/')}/?{params}", data=body, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read().decode()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()
        raise ServingSyncError(f"ClickHouse HTTP {exc.code}: {detail[:500]}") from exc
    except urllib.error.URLError as exc:
        raise ServingSyncError(f"ClickHouse không tới được: {exc.reason}") from exc


def clickhouse_json(http: str, query: str, password: str = "cinestream") -> list[dict]:
    raw = clickhouse(http, query + " FORMAT JSON", password=password)
    return json.loads(raw).get("data") or []


def _json(value):
    if hasattr(value, "as_tuple"):
        return float(value)
    return value


def main(argv: list[str] | None = None) -> None:
    from transformation.batch.common import build_parser, start_spark

    parser = build_parser("Đồng bộ một marker Gold sang ClickHouse.")
    parser.add_argument("--warehouse", default="s3a://lakehouse")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--marker-namespace", required=True)
    parser.add_argument("--clickhouse-http", default="http://127.0.0.1:18123")
    parser.add_argument("--database", default="cinestream_serving")
    parser.add_argument("--fail-after-rows", type=int, default=None)
    args = parser.parse_args(argv)
    spark = start_spark("cinestream-serving-sync", args)
    try:
        result = sync_published(
            spark,
            batch_id=args.batch_id,
            marker_namespace=args.marker_namespace,
            http=args.clickhouse_http,
            database=args.database,
            s3_endpoint=args.s3_endpoint,
            fail_after_rows=args.fail_after_rows,
        )
        print("SERVING_JSON=" + json.dumps(result, default=str), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
