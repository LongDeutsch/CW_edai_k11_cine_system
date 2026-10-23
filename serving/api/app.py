"""Đọc Gold đã phát hành trên ClickHouse.

Chỉ truy vấn ``movie_engagement``, ``movie_ratings`` và ``sync_log``.
Không đọc bảng ``_stage`` và không mở Iceberg. Hai subquery trong overview
không phải một transaction: nếu snapshot hai bảng lệch, API trả 409 thay vì
trộn hai phiên bản.

Route ``/governance`` đọc inventory và metadata. Không ALTER và không ghi marker.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from serving.api.governance_routes import router as governance_router
from serving.clickhouse.sync_gold import ServingSyncError, clickhouse, clickhouse_json
from transformation.batch.contracts.validators import check_identifier

app = FastAPI(title="Cinestream serving", version="3c")
app.include_router(governance_router)


class Overview(BaseModel):
    engagement_snapshot_id: int | None
    ratings_snapshot_id: int | None
    batch_id: str | None
    published_at: str | None
    session_count: int
    total_watch_seconds: int
    watched_movie_count: int
    rated_movie_count: int
    rating_count: int
    mean_rating: float | None = Field(description="Trung bình có trọng số theo rating_count, không phải trung bình của các avg_rating.")


class RatedMovie(BaseModel):
    movie_id: int
    title: str | None
    content_rating: str | None
    rating_count: int
    avg_rating: float
    iceberg_snapshot_id: int
    batch_id: str


class TopRated(BaseModel):
    ratings_snapshot_id: int | None
    items: list[RatedMovie]


class WatchedMovie(BaseModel):
    movie_id: int
    title: str | None
    watch_seconds: int
    session_count: int
    iceberg_snapshot_id: int


class MostWatched(BaseModel):
    engagement_snapshot_id: int | None
    items: list[WatchedMovie]


class ServingStatus(BaseModel):
    database: str
    engagement_snapshot_id: int | None
    ratings_snapshot_id: int | None
    engagement_rows: int
    rating_rows: int
    batch_id: str | None
    published_at: str | None
    log_present: bool
    consistent: bool
    detail: str | None
    trending_snapshot_id: int | None = None
    trending_rows: int = 0
    trending_synced_at: str | None = None
    trending_latest_window_end: str | None = None
    trending_detail: str | None = None
    trending_freshness: str = "empty"
    trending_live: bool = False
    trending_poll_status: str | None = None
    trending_polled_at: str | None = None
    trending_event_age_seconds: float | None = None
    trending_sync_age_seconds: float | None = None
    trending_stale_after_seconds: float = 180
    batch_database: str = ""
    trending_database: str = ""


class TrendingMovie(BaseModel):
    movie_id: int
    recent_heartbeats: int
    recent_watch_seconds: int
    peak_minute_unique_viewers: int = Field(description="max(active_users) của từng phút trong cửa sổ. Không phải concurrent users.")


class Trending(BaseModel):
    window_minutes: int
    database: str | None = None
    iceberg_snapshot_id: int | None
    latest_window_end: str | None
    synced_at: str | None
    freshness: str
    live: bool
    poll_status: str | None = None
    polled_at: str | None = None
    event_age_seconds: float | None = None
    sync_age_seconds: float | None = None
    stale_after_seconds: float
    items: list[TrendingMovie]


def _database() -> str:
    """Database batch Gold. Giữ tên cũ để các endpoint phiên và rating không đổi."""
    return check_identifier(os.environ.get("CINESTREAM_CLICKHOUSE_DATABASE", "cinestream_serving"), "database")


def _trending_database() -> str:
    """Database cửa sổ Flink. Trống thì dùng cùng database batch."""
    raw = os.environ.get("CINESTREAM_TRENDING_DATABASE", "").strip()
    if not raw:
        return _database()
    return check_identifier(raw, "database")


def _unreachable(detail: str | None) -> bool:
    return bool(detail) and "không tới được" in detail


def _batch_status() -> dict:
    database = _database()
    base = {
        "database": database,
        "batch_database": database,
        "engagement_snapshot_id": None,
        "ratings_snapshot_id": None,
        "engagement_rows": 0,
        "rating_rows": 0,
        "batch_id": None,
        "published_at": None,
        "log_present": False,
        "consistent": False,
        "detail": None,
        "_unavailable": False,
    }
    try:
        body = overview()
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
        if exc.status_code == 409:
            try:
                engagement_rows, rating_rows = _reader_counts(database)
            except HTTPException:
                engagement_rows, rating_rows = 0, 0
            base.update(engagement_rows=engagement_rows, rating_rows=rating_rows, detail=detail)
            return base
        if exc.status_code == 503:
            base.update(detail=detail, _unavailable=True)
            return base
        raise
    base.update(
        engagement_snapshot_id=body.engagement_snapshot_id,
        ratings_snapshot_id=body.ratings_snapshot_id,
        engagement_rows=body.session_count,
        rating_rows=body.rated_movie_count,
        batch_id=body.batch_id,
        published_at=body.published_at,
        log_present=body.published_at is not None,
        consistent=True,
        detail=None if body.published_at is not None or body.session_count == 0 else "reader đã có snapshot nhưng sync_log chưa ghi",
    )
    return base


def _http() -> str:
    return os.environ.get("CINESTREAM_CLICKHOUSE_HTTP", "http://127.0.0.1:18123")


def _password() -> str:
    return os.environ.get("CINESTREAM_CLICKHOUSE_PASSWORD", "cinestream")


def _rows(query: str) -> list[dict[str, Any]]:
    try:
        return clickhouse_json(_http(), query, password=_password())
    except ServingSyncError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _one_snapshot(table: str, database: str) -> int | None:
    rows = _rows(f"SELECT uniqExact(iceberg_snapshot_id) AS versions, any(iceberg_snapshot_id) AS snapshot_id FROM {database}.{table}")
    versions = int(rows[0]["versions"] or 0)
    if versions > 1:
        raise HTTPException(status_code=409, detail=f"{table} đang trộn nhiều snapshot")
    if versions == 0:
        return None
    return int(rows[0]["snapshot_id"])


@app.get("/analytics/overview", response_model=Overview)
def overview() -> Overview:
    """Tóm tắt phiên xem và rating của một lần publish. Grain là session và movie."""
    database = _database()
    rows = _rows(
        f"""
        SELECT
            e.snapshot_id AS engagement_snapshot_id,
            r.snapshot_id AS ratings_snapshot_id,
            e.batch_id AS engagement_batch_id,
            r.batch_id AS ratings_batch_id,
            e.versions AS engagement_versions,
            r.versions AS ratings_versions,
            e.sessions AS session_count,
            e.watch_seconds AS total_watch_seconds,
            e.movies AS watched_movie_count,
            r.movies AS rated_movie_count,
            r.ratings AS rating_count,
            r.weighted AS rating_sum
        FROM (
            SELECT
                any(iceberg_snapshot_id) AS snapshot_id,
                any(batch_id) AS batch_id,
                uniqExact(iceberg_snapshot_id) AS versions,
                count() AS sessions,
                toInt64(ifNull(sum(watch_duration_seconds), 0)) AS watch_seconds,
                toInt64(uniqExact(movie_id)) AS movies
            FROM {database}.movie_engagement
        ) AS e
        CROSS JOIN (
            SELECT
                any(iceberg_snapshot_id) AS snapshot_id,
                any(batch_id) AS batch_id,
                uniqExact(iceberg_snapshot_id) AS versions,
                count() AS movies,
                toInt64(ifNull(sum(rating_count), 0)) AS ratings,
                ifNull(sum(avg_rating * rating_count), 0) AS weighted
            FROM {database}.movie_ratings
        ) AS r
        """
    )
    row = rows[0]
    engagement_versions = int(row["engagement_versions"] or 0)
    ratings_versions = int(row["ratings_versions"] or 0)
    if engagement_versions > 1 or ratings_versions > 1:
        raise HTTPException(status_code=409, detail="reader table đang trộn nhiều snapshot")
    engagement = _optional_int(row["engagement_snapshot_id"]) if engagement_versions else None
    ratings = _optional_int(row["ratings_snapshot_id"]) if ratings_versions else None
    engagement_batch = row["engagement_batch_id"]
    ratings_batch = row["ratings_batch_id"]
    published_at = None
    batch_id = None
    if engagement is None and ratings is None:
        pass
    elif engagement is None or ratings is None or engagement_batch != ratings_batch:
        raise HTTPException(status_code=409, detail="movie_engagement và movie_ratings không cùng lần publish")
    else:
        meta = _rows(
            f"""
            SELECT batch_id, toString(published_at) AS published_at
            FROM {database}.sync_log
            WHERE obt_snapshot = {engagement}
              AND fact_ratings_snapshot = {ratings}
              AND batch_id = '{engagement_batch}'
            ORDER BY published_at DESC
            LIMIT 1
            """
        )
        if meta:
            batch_id = meta[0]["batch_id"]
            published_at = meta[0]["published_at"]
        else:
            batch_id = engagement_batch
            published_at = None
    rating_count = int(row["rating_count"] or 0)
    mean = None if rating_count == 0 else float(row["rating_sum"]) / rating_count
    return Overview(
        engagement_snapshot_id=engagement,
        ratings_snapshot_id=ratings,
        batch_id=batch_id,
        published_at=published_at,
        session_count=int(row["session_count"] or 0),
        total_watch_seconds=int(row["total_watch_seconds"] or 0),
        watched_movie_count=int(row["watched_movie_count"] or 0),
        rated_movie_count=int(row["rated_movie_count"] or 0),
        rating_count=rating_count,
        mean_rating=mean,
    )


@app.get("/movies/top-rated", response_model=TopRated)
def top_rated(
    limit: int = Query(default=10, ge=1, le=100),
    min_rating_count: int = Query(default=1, ge=1, le=1_000_000),
) -> TopRated:
    """Xếp theo avg_rating của fact đã gộp theo movie_id. Không đọc stage."""
    database = _database()
    snapshot = _one_snapshot("movie_ratings", database)
    rows = _rows(
        f"""
        SELECT movie_id, title, content_rating, rating_count, avg_rating, iceberg_snapshot_id, batch_id
        FROM {database}.movie_ratings
        WHERE rating_count >= {int(min_rating_count)}
        ORDER BY avg_rating DESC, rating_count DESC, movie_id ASC
        LIMIT {int(limit)}
        """
    )
    return TopRated(
        ratings_snapshot_id=snapshot,
        items=[
            RatedMovie(
                movie_id=int(item["movie_id"]),
                title=item["title"],
                content_rating=item["content_rating"],
                rating_count=int(item["rating_count"]),
                avg_rating=float(item["avg_rating"]),
                iceberg_snapshot_id=int(item["iceberg_snapshot_id"]),
                batch_id=item["batch_id"],
            )
            for item in rows
        ],
    )


@app.get("/movies/most-watched", response_model=MostWatched)
def most_watched(limit: int = Query(default=10, ge=1, le=100)) -> MostWatched:
    """Tổng watch_duration_seconds theo movie_id. Một dòng OBT là một phiên."""
    database = _database()
    snapshot = _one_snapshot("movie_engagement", database)
    rows = _rows(
        f"""
        SELECT
            movie_id,
            any(title) AS title,
            toInt64(ifNull(sum(watch_duration_seconds), 0)) AS watch_seconds,
            count() AS session_count,
            any(iceberg_snapshot_id) AS iceberg_snapshot_id
        FROM {database}.movie_engagement
        GROUP BY movie_id
        ORDER BY watch_seconds DESC, session_count DESC, movie_id ASC
        LIMIT {int(limit)}
        """
    )
    return MostWatched(
        engagement_snapshot_id=snapshot,
        items=[
            WatchedMovie(
                movie_id=int(item["movie_id"]),
                title=item["title"],
                watch_seconds=int(item["watch_seconds"]),
                session_count=int(item["session_count"]),
                iceberg_snapshot_id=int(item["iceberg_snapshot_id"]),
            )
            for item in rows
        ],
    )


@app.get("/health")
def health() -> dict:
    """Process sống. Không kiểm tra ClickHouse."""
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict:
    """ClickHouse trả lời và đủ hai reader table. Bảng rỗng vẫn ready."""
    database = _database()
    try:
        clickhouse(_http(), "SELECT 1", password=_password())
        found = clickhouse_json(
            _http(),
            f"""
            SELECT name
            FROM system.tables
            WHERE database = '{database}'
              AND name IN ('movie_engagement', 'movie_ratings')
            """,
            password=_password(),
        )
    except ServingSyncError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    names = {row["name"] for row in found}
    if names != {"movie_engagement", "movie_ratings"}:
        raise HTTPException(status_code=503, detail="thiếu reader table")
    return {"status": "ready", "database": database}


@app.get("/serving/status", response_model=ServingStatus)
def serving_status() -> ServingStatus:
    """Batch và trending độc lập. Lệch snapshot batch vẫn là 200 với consistent=false."""
    batch = _batch_status()
    unavailable = batch.pop("_unavailable")
    trend = _trending_view(_trending_database())
    if unavailable and _unreachable(batch.get("detail")) and _unreachable(trend.get("trending_detail")):
        raise HTTPException(status_code=503, detail=batch.get("detail") or "ClickHouse không tới được")
    return ServingStatus(**batch, **trend, trending_database=_trending_database())


@app.get("/movies/trending", response_model=Trending)
def trending(
    window_minutes: int = Query(default=15, ge=1, le=24 * 60),
    limit: int = Query(default=10, ge=1, le=100),
) -> Trending:
    """Cộng các phút đã đóng, tính từ ``max(window_end)`` của snapshot đang phục vụ.

    Mốc là event time của cửa sổ mới nhất trong ClickHouse, không phải đồng hồ tường.
    ``peak_minute_unique_viewers`` là số user phân biệt cao nhất của một phút.
    """
    database = _trending_database()
    head = _rows(
        f"""
        SELECT
            count() AS rows,
            uniqExact(iceberg_snapshot_id) AS versions,
            any(iceberg_snapshot_id) AS snapshot_id,
            toString(max(window_end)) AS latest_window_end
        FROM {database}.movie_windows
        """
    )[0]
    versions = int(head["versions"] or 0)
    if versions > 1:
        raise HTTPException(status_code=409, detail="movie_windows đang trộn nhiều snapshot")
    if versions == 0:
        state = _classify(database, versions=0, latest_window_end=None, synced_at=None)
        return Trending(
            window_minutes=window_minutes,
            database=database,
            iceberg_snapshot_id=None,
            latest_window_end=None,
            synced_at=None,
            items=[],
            **state,
        )
    snapshot = int(head["snapshot_id"])
    latest = head["latest_window_end"]
    rows = _rows(
        f"""
        SELECT
            movie_id,
            toInt64(sum(heartbeat_count)) AS recent_heartbeats,
            toInt64(sum(total_watch_seconds)) AS recent_watch_seconds,
            toInt64(max(active_users)) AS peak_minute_unique_viewers
        FROM {database}.movie_windows
        WHERE window_end > (SELECT max(window_end) FROM {database}.movie_windows) - toIntervalMinute({int(window_minutes)})
          AND window_end <= (SELECT max(window_end) FROM {database}.movie_windows)
        GROUP BY movie_id
        ORDER BY recent_heartbeats DESC, recent_watch_seconds DESC, movie_id ASC
        LIMIT {int(limit)}
        """
    )
    meta = _rows(
        f"""
        SELECT toString(synced_at) AS synced_at
        FROM {database}.trending_sync_log
        WHERE iceberg_snapshot_id = {snapshot}
        ORDER BY synced_at DESC
        LIMIT 1
        """
    )
    synced_at = meta[0]["synced_at"] if meta else None
    latest_text = None if latest in (None, "") else str(latest)
    return Trending(
        window_minutes=window_minutes,
        database=database,
        iceberg_snapshot_id=snapshot,
        latest_window_end=latest_text,
        synced_at=synced_at,
        items=[
            TrendingMovie(
                movie_id=int(item["movie_id"]),
                recent_heartbeats=int(item["recent_heartbeats"]),
                recent_watch_seconds=int(item["recent_watch_seconds"]),
                peak_minute_unique_viewers=int(item["peak_minute_unique_viewers"]),
            )
            for item in rows
        ],
        **_classify(database, versions=versions, latest_window_end=latest_text, synced_at=synced_at),
    )


def _classify(database: str, *, versions: int, latest_window_end: str | None, synced_at: str | None) -> dict:
    """`live` chỉ khi poll và event time đều trong ngưỡng. Dữ liệu cũ không được gọi là trending sống."""
    poll = _latest_poll(database)
    threshold = _stale_after()
    now = datetime.now(timezone.utc)
    poll_status = None if poll is None else poll.get("status")
    polled_at = None if poll is None else poll.get("polled_at")
    event_age = _age_seconds(latest_window_end, now) if versions else None
    sync_age = _age_seconds(synced_at, now)
    poll_age = _age_seconds(polled_at, now)
    if versions == 0 and poll_status not in {"retry", "source_absent"}:
        freshness = "empty"
    elif poll_status in {"retry", "source_absent"}:
        freshness = "sync_failed"
    elif poll_age is None or poll_age > threshold:
        freshness = "stale"
    elif event_age is None or event_age > threshold:
        freshness = "idle"
    else:
        freshness = "live"
    return {
        "freshness": freshness,
        "live": freshness == "live",
        "poll_status": poll_status,
        "polled_at": polled_at,
        "event_age_seconds": event_age,
        "sync_age_seconds": sync_age,
        "stale_after_seconds": threshold,
    }


def _latest_poll(database: str) -> dict | None:
    try:
        rows = clickhouse_json(
            _http(),
            f"""
            SELECT status, toString(polled_at) AS polled_at
            FROM {database}.trending_worker_status
            ORDER BY polled_at DESC
            LIMIT 1
            """,
            password=_password(),
        )
    except ServingSyncError:
        return None
    return rows[0] if rows else None


def _stale_after() -> float:
    raw = os.environ.get("CINESTREAM_TRENDING_STALE_SECONDS", "180")
    try:
        return float(raw)
    except ValueError:
        return 180.0


def _age_seconds(value: str | None, now: datetime) -> float | None:
    parsed = _parse_utc(value)
    if parsed is None:
        return None
    return round((now - parsed).total_seconds(), 3)


def _parse_utc(value: str | None) -> datetime | None:
    if not value or value in {"\\N", "NULL"}:
        return None
    text = str(value).replace("T", " ").replace("Z", "")
    for size, fmt in ((26, "%Y-%m-%d %H:%M:%S.%f"), (19, "%Y-%m-%d %H:%M:%S")):
        try:
            return datetime.strptime(text[:size], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _trending_view(database: str) -> dict:
    empty_state = _classify(database, versions=0, latest_window_end=None, synced_at=None)
    empty = {
        "trending_snapshot_id": None,
        "trending_rows": 0,
        "trending_synced_at": None,
        "trending_latest_window_end": None,
        "trending_detail": None,
        **_status_freshness(empty_state),
    }
    try:
        rows = clickhouse_json(
            _http(),
            f"""
            SELECT
                count() AS rows,
                uniqExact(iceberg_snapshot_id) AS versions,
                any(iceberg_snapshot_id) AS snapshot_id,
                toString(max(window_end)) AS latest_window_end
            FROM {database}.movie_windows
            """,
            password=_password(),
        )
    except ServingSyncError as exc:
        empty["trending_detail"] = str(exc)
        empty["trending_freshness"] = "sync_failed"
        empty["trending_live"] = False
        return empty
    if not rows:
        return empty
    versions = int(rows[0]["versions"] or 0)
    if versions == 0:
        empty["trending_detail"] = "chưa có cửa sổ"
        return empty
    if versions > 1:
        empty["trending_rows"] = int(rows[0]["rows"] or 0)
        empty["trending_detail"] = "movie_windows đang trộn nhiều snapshot"
        empty["trending_freshness"] = "stale"
        empty["trending_live"] = False
        return empty
    snapshot = int(rows[0]["snapshot_id"])
    meta = []
    try:
        meta = clickhouse_json(
            _http(),
            f"""
            SELECT toString(synced_at) AS synced_at
            FROM {database}.trending_sync_log
            WHERE iceberg_snapshot_id = {snapshot}
            ORDER BY synced_at DESC
            LIMIT 1
            """,
            password=_password(),
        )
    except ServingSyncError as exc:
        empty["trending_detail"] = str(exc)
    latest = rows[0]["latest_window_end"]
    latest_text = None if latest in (None, "") else str(latest)
    synced_at = meta[0]["synced_at"] if meta else None
    state = _classify(database, versions=versions, latest_window_end=latest_text, synced_at=synced_at)
    return {
        "trending_snapshot_id": snapshot,
        "trending_rows": int(rows[0]["rows"] or 0),
        "trending_synced_at": synced_at,
        "trending_latest_window_end": latest_text,
        "trending_detail": None if meta else "snapshot cửa sổ chưa có trong trending_sync_log",
        **_status_freshness(state),
    }


def _status_freshness(state: dict) -> dict:
    return {
        "trending_freshness": state["freshness"],
        "trending_live": state["live"],
        "trending_poll_status": state["poll_status"],
        "trending_polled_at": state["polled_at"],
        "trending_event_age_seconds": state["event_age_seconds"],
        "trending_sync_age_seconds": state["sync_age_seconds"],
        "trending_stale_after_seconds": state["stale_after_seconds"],
    }


def _reader_counts(database: str) -> tuple[int, int]:
    rows = _rows(
        f"""
        SELECT
            (SELECT count() FROM {database}.movie_engagement) AS engagement_rows,
            (SELECT count() FROM {database}.movie_ratings) AS rating_rows
        """
    )
    return int(rows[0]["engagement_rows"]), int(rows[0]["rating_rows"])


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
