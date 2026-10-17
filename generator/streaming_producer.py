"""Kafka producer heartbeat xem phim, có dry-run không cần broker.

Mỗi message là một JSON ``movie.playback.heartbeat``. Mặc định phát
1.000 message rồi dừng, trong đó đúng ``--late-ratio`` (6%) có
``event_ts`` lùi 10–35 phút và đúng ``--dup-ratio`` (1,8%) là bản
phát lại nguyên vẹn heartbeat vừa gửi.

Dry-run in JSON ra stdout và bảng tỷ lệ ra stderr. Không mở socket Kafka.

Ví dụ::

    python -m generator.streaming_producer --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generator.injectors.late_arrival_injector import (
    event_timestamp,
    message_quota,
    sample_late_delay_seconds,
    sample_on_time_lag_seconds,
)

logger = logging.getLogger("generator.streaming")

PLAYBACK_SPEEDS = np.array([0.5, 1.0, 1.0, 1.25, 1.5, 2.0])
BITRATES_KBPS = np.array([800, 1500, 3000, 4500, 8000])
HEARTBEAT_STEP_SECONDS = 10


@dataclass
class Outbound:
    """Một message sắp phát. ``payload`` là đúng body ghi vào Kafka."""

    payload: dict[str, object]
    kind: str
    lag_seconds: int
    replay_delay_ms: int | None = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phát heartbeat xem phim ra console hoặc Kafka.")
    parser.add_argument("--dry-run", action="store_true", help="In JSON ra console, không nối Kafka.")
    parser.add_argument("--late-ratio", type=float, default=0.06, help="Tỷ lệ message có event_ts lùi 10–35 phút.")
    parser.add_argument("--dup-ratio", type=float, default=0.018, help="Tỷ lệ message là bản phát lại nguyên vẹn.")
    parser.add_argument(
        "--kafka-bootstrap",
        default="localhost:19092",
        help="Bootstrap server. Cổng 19092 vì 9092 thường đã bị Kafka khác chiếm.",
    )
    parser.add_argument("--topic", default="movie.playback.heartbeat")
    parser.add_argument("--max-events", type=int, default=1000, help="Tổng số message phát ra, gồm cả bản replay.")
    parser.add_argument("--preview", type=int, default=12, help="Số JSON in ra khi dry-run. 0 = in tất cả.")
    parser.add_argument("--catalog-dir", type=Path, default=Path("data_generator/output/scale_0_1"))
    parser.add_argument("--session-beats", type=int, default=10, help="Số heartbeat liên tiếp trong một phiên.")
    parser.add_argument("--late-flush-every", type=int, default=40, help="Sau bấy nhiêu event đúng giờ thì xả burst late.")
    parser.add_argument("--late-min-minutes", type=int, default=10)
    parser.add_argument("--late-max-minutes", type=int, default=35)
    parser.add_argument("--dup-delay-min-ms", type=int, default=200)
    parser.add_argument("--dup-delay-max-ms", type=int, default=1500)
    parser.add_argument("--random-seed", type=int, default=42)
    return parser


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = build_parser().parse_args(argv)
    _validate(args)
    rng = np.random.default_rng(args.random_seed)
    produced_at = datetime.now(timezone.utc).replace(microsecond=0)

    n_dup = message_quota(args.max_events, args.dup_ratio)
    n_late = message_quota(args.max_events, args.late_ratio)
    n_original = args.max_events - n_dup
    replay_at, late_at = _assign_roles(n_original, n_dup, n_late, rng)

    user_ids, user_weights, movie_ids, movie_weights = _load_catalog(args.catalog_dir)
    originals, lags = _build_originals(
        n_original,
        user_ids,
        user_weights,
        movie_ids,
        movie_weights,
        late_at,
        produced_at,
        rng,
        session_beats=args.session_beats,
        late_min_minutes=args.late_min_minutes,
        late_max_minutes=args.late_max_minutes,
    )
    outbound = _arrange(originals, lags, replay_at, late_at, rng, args)
    _assert_contracts(outbound, args, n_late, n_dup)

    payloads = [json.dumps(item.payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True) for item in outbound]
    if args.dry_run:
        _print_dry_run(outbound, payloads, args)
        sys.stdout.flush()
    else:
        _produce_kafka(payloads, args)
    _log_summary(outbound, payloads, args, produced_at)


def _validate(args: argparse.Namespace) -> None:
    parser = build_parser()
    if args.max_events < 1:
        parser.error("--max-events phải >= 1")
    if not 0 <= args.late_ratio <= 1 or not 0 <= args.dup_ratio < 1:
        parser.error("late-ratio nằm trong [0, 1], dup-ratio nằm trong [0, 1)")
    if args.late_ratio + args.dup_ratio > 1:
        parser.error("tổng late-ratio và dup-ratio không được vượt 1")
    if args.session_beats < 1 or args.late_flush_every < 1:
        parser.error("session-beats và late-flush-every phải >= 1")
    if args.late_min_minutes < 0 or args.late_max_minutes < args.late_min_minutes:
        parser.error("khoảng phút late arrival không hợp lệ")
    if args.dup_delay_min_ms < 0 or args.dup_delay_max_ms < args.dup_delay_min_ms:
        parser.error("khoảng delay duplicate không hợp lệ")
    if args.preview < 0:
        parser.error("--preview phải >= 0")


def _assign_roles(
    n_original: int,
    n_dup: int,
    n_late: int,
    rng: np.random.Generator,
) -> tuple[set[int], set[int]]:
    """Chọn event gốc được replay và event gốc đến muộn.

    Bản replay không gắn vào event muộn, nên số message muộn đúng bằng
    số event gốc muộn và không bị đếm trùng.
    """
    if n_original < 1:
        raise SystemExit("dup-ratio để lại 0 event gốc. Hãy giảm --dup-ratio hoặc tăng --max-events.")
    replay_at: set[int] = set()
    if n_dup:
        if n_dup > n_original:
            raise SystemExit("số bản replay nhiều hơn số event gốc")
        replay_at = set(int(i) for i in rng.choice(n_original, size=n_dup, replace=False))
    candidates = [i for i in range(n_original) if i not in replay_at]
    if n_late > len(candidates):
        raise SystemExit(
            "không đủ event gốc để vừa đúng late-ratio và dup-ratio. "
            "Hãy tăng --max-events hoặc giảm một trong hai tỷ lệ."
        )
    late_at: set[int] = set()
    if n_late:
        late_at = set(int(i) for i in rng.choice(np.array(candidates, dtype=np.int64), size=n_late, replace=False))
    return replay_at, late_at


def _load_catalog(catalog_dir: Path) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Lấy phân phối user/movie từ ratings offline nếu thư mục đã được sinh."""
    ratings_dir = catalog_dir / "ratings"
    if not ratings_dir.exists():
        logger.info("không thấy %s — stream dùng user 1..500 và movie 1..100 đồng đều", ratings_dir)
        users = np.arange(1, 501, dtype=np.int64)
        movies = np.arange(1, 101, dtype=np.int64)
        return users, np.full(users.size, 1 / users.size), movies, np.full(movies.size, 1 / movies.size)

    import pyarrow.parquet as pq

    table = pq.read_table(ratings_dir, columns=["user_id", "movie_id"])
    user_ids, user_counts = np.unique(table.column("user_id").to_numpy(), return_counts=True)
    movie_ids, movie_counts = np.unique(table.column("movie_id").to_numpy(), return_counts=True)
    logger.info(
        "catalog %s | users=%d movies=%d | movie nặng nhất trong ratings offline chiếm %.2f%%",
        catalog_dir,
        user_ids.size,
        movie_ids.size,
        100.0 * float(movie_counts.max() / movie_counts.sum()),
    )
    return (
        user_ids.astype(np.int64, copy=False),
        user_counts / user_counts.sum(),
        movie_ids.astype(np.int64, copy=False),
        movie_counts / movie_counts.sum(),
    )


def _build_originals(
    n_original: int,
    user_ids: np.ndarray,
    user_weights: np.ndarray,
    movie_ids: np.ndarray,
    movie_weights: np.ndarray,
    late_at: set[int],
    produced_at: datetime,
    rng: np.random.Generator,
    *,
    session_beats: int,
    late_min_minutes: int,
    late_max_minutes: int,
) -> tuple[list[dict[str, object]], list[int]]:
    events: list[dict[str, object]] = []
    lags: list[int] = []
    cursor = 0
    session_no = 0
    while cursor < n_original:
        length = min(session_beats, n_original - cursor)
        session_no += 1
        user_id = int(rng.choice(user_ids, p=user_weights))
        movie_id = int(rng.choice(movie_ids, p=movie_weights))
        speed = float(rng.choice(PLAYBACK_SPEEDS))
        bitrate = int(rng.choice(BITRATES_KBPS))
        session_id = f"ses-{session_no:06d}"
        for beat in range(length):
            index = cursor + beat
            lag = (
                sample_late_delay_seconds(rng, min_minutes=late_min_minutes, max_minutes=late_max_minutes)
                if index in late_at
                else sample_on_time_lag_seconds(rng)
            )
            events.append(
                {
                    "bitrate_kbps": bitrate,
                    "current_position_seconds": beat * HEARTBEAT_STEP_SECONDS,
                    "event_ts": event_timestamp(produced_at, lag),
                    "heartbeat_id": index + 1,
                    "movie_id": movie_id,
                    "playback_speed": speed,
                    "session_id": session_id,
                    "user_id": user_id,
                }
            )
            lags.append(lag)
        cursor += length
    return events, lags


def _arrange(
    originals: list[dict[str, object]],
    lags: list[int],
    replay_at: set[int],
    late_at: set[int],
    rng: np.random.Generator,
    args: argparse.Namespace,
) -> list[Outbound]:
    """Event đúng giờ đi ngay. Event muộn được giữ và xả thành burst."""
    outbound: list[Outbound] = []
    late_buffer: list[Outbound] = []
    on_time_since_flush = 0
    for index, payload in enumerate(originals):
        item = Outbound(payload=payload, kind="late" if index in late_at else "on_time", lag_seconds=lags[index])
        if item.kind == "late":
            late_buffer.append(item)
            continue
        outbound.append(item)
        if index in replay_at:
            outbound.append(_replay(item, rng, args))
        on_time_since_flush += 1
        if on_time_since_flush >= args.late_flush_every and late_buffer:
            outbound.extend(late_buffer)
            late_buffer = []
            on_time_since_flush = 0
    if late_buffer:
        outbound.extend(late_buffer)
    return outbound


def _replay(source: Outbound, rng: np.random.Generator, args: argparse.Namespace) -> Outbound:
    delay_ms = int(rng.integers(args.dup_delay_min_ms, args.dup_delay_max_ms + 1))
    return Outbound(
        payload=dict(source.payload),
        kind="replay",
        lag_seconds=source.lag_seconds,
        replay_delay_ms=delay_ms,
    )


def _assert_contracts(outbound: list[Outbound], args: argparse.Namespace, n_late: int, n_dup: int) -> None:
    late_items = [item for item in outbound if item.kind == "late"]
    replays = [item for item in outbound if item.kind == "replay"]
    if len(late_items) != n_late or len(replays) != n_dup:
        raise SystemExit(
            f"sai hợp đồng tiêm lỗi: late={len(late_items)}/{n_late}, replay={len(replays)}/{n_dup}"
        )
    min_lag = args.late_min_minutes * 60
    max_lag = args.late_max_minutes * 60
    for item in outbound:
        if item.kind == "late" and not min_lag <= item.lag_seconds <= max_lag:
            raise SystemExit(f"late lag {item.lag_seconds}s nằm ngoài [{min_lag}, {max_lag}]")
        if item.kind == "on_time" and item.lag_seconds >= 2:
            raise SystemExit(f"event đúng giờ lệch {item.lag_seconds}s")
        if item.kind == "replay" and not args.dup_delay_min_ms <= int(item.replay_delay_ms) <= args.dup_delay_max_ms:
            raise SystemExit("delay bản replay nằm ngoài khoảng đã cấu hình")


def _print_dry_run(outbound: list[Outbound], payloads: list[str], args: argparse.Namespace) -> None:
    limit = len(outbound) if args.preview == 0 else min(args.preview, len(outbound))
    previous_kind = ""
    for item, line in list(zip(outbound, payloads))[:limit]:
        marker = _marker(previous_kind, item)
        if marker:
            print(marker)
        print(line)
        previous_kind = item.kind
    if limit < len(outbound):
        print(f"... đã ẩn {len(outbound) - limit} message tiếp theo. Dùng --preview 0 để in hết.")
    _print_proof_pair(outbound, payloads)


def _print_proof_pair(outbound: list[Outbound], payloads: list[str]) -> None:
    late = next((i for i, item in enumerate(outbound) if item.kind == "late"), None)
    replay = next((i for i, item in enumerate(outbound) if item.kind == "replay"), None)
    print("--- mẫu kiểm chứng ---")
    if late is not None:
        item = outbound[late]
        print(f"[late-burst] delay={item.lag_seconds / 60:.2f} phút | {payloads[late]}")
    if replay is not None and replay > 0:
        print(f"[duplicate delay_ms={outbound[replay].replay_delay_ms}] giống hệt message ngay trước:")
        print(payloads[replay - 1])
        print(payloads[replay])
        same = payloads[replay] == payloads[replay - 1]
        print(f"json_identical={str(same).lower()} heartbeat_id={outbound[replay].payload['heartbeat_id']}")


def _marker(previous_kind: str, item: Outbound) -> str | None:
    if item.kind == "replay":
        return f"[duplicate delay_ms={item.replay_delay_ms}]"
    if item.kind == "late" and previous_kind != "late":
        return "[late-burst]"
    return None


def _produce_kafka(payloads: list[str], args: argparse.Namespace) -> None:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise SystemExit(
            "chưa cài kafka-python. Dry-run không cần thư viện này. "
            "Để phát thật: pip install kafka-python"
        ) from exc
    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap,
        value_serializer=lambda raw: raw,
        linger_ms=20,
    )
    try:
        for line in payloads:
            producer.send(args.topic, line.encode("utf-8"))
        producer.flush()
    finally:
        producer.close()
    logger.info("đã gửi %d message tới topic %s qua %s", len(payloads), args.topic, args.kafka_bootstrap)


def _log_summary(
    outbound: list[Outbound],
    payloads: list[str],
    args: argparse.Namespace,
    produced_at: datetime,
) -> None:
    total = len(outbound)
    n_late = sum(item.kind == "late" for item in outbound)
    n_replay = sum(item.kind == "replay" for item in outbound)
    identical = all(
        payloads[index] == payloads[index - 1]
        for index, item in enumerate(outbound)
        if item.kind == "replay"
    )
    originals = [item for item in outbound if item.kind != "replay"]
    movie_ids = [int(item.payload["movie_id"]) for item in originals]
    n_sessions = len({item.payload["session_id"] for item in originals})
    top_movie, top_count = Counter(movie_ids).most_common(1)[0]
    late_lags = [item.lag_seconds for item in outbound if item.kind == "late"]
    replay_delays = [int(item.replay_delay_ms) for item in outbound if item.kind == "replay"]
    mode = "dry-run stdout" if args.dry_run else f"kafka {args.kafka_bootstrap} topic={args.topic}"
    lines = [
        "========== streaming producer ==========",
        f"mode                : {mode}",
        f"produced_at_utc     : {produced_at.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"messages            : {total:,}",
        (
            f"late arrival        : {100 * n_late / total:.4f}% "
            f"({n_late:,}/{total:,}) target {100 * args.late_ratio:.4f}% | "
            f"delay {min(late_lags) / 60:.2f}–{max(late_lags) / 60:.2f} phút"
            if late_lags
            else "late arrival        : 0"
        ),
        (
            f"duplicate           : {100 * n_replay / total:.4f}% "
            f"({n_replay:,}/{total:,}) target {100 * args.dup_ratio:.4f}% | "
            f"replay delay {min(replay_delays)}–{max(replay_delays)} ms | "
            f"json_identical={str(identical).lower()}"
            if replay_delays
            else "duplicate           : 0"
        ),
        (
            f"movie_id gốc        : distinct={len(set(movie_ids)):,} | "
            f"top movie_id={top_movie} chiếm {100 * top_count / len(movie_ids):.2f}% "
            f"trên {n_sessions} phiên (mỗi phiên một phim, nên dao động quanh 80% của catalog)"
        ),
        "========================================",
    ]
    logger.info("\n".join(lines))


if __name__ == "__main__":
    main()
