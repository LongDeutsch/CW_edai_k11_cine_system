"""Sinh dữ liệu offline Parquet từ seed MovieLens ml-latest-small.

``--scale-factor`` là hệ số phóng so với khối lượng đặc tả:

* ``100.0`` → đúng bảng volume trong ``docs/data_generator_spec.md`` (~101 GB Parquet)
* ``1.0``   → 1% khối lượng đó (test cục bộ)
* ``0.1``   → 0.1% khối lượng (smoke test, vài giây)

Ví dụ smoke test từ thư mục gốc repo::

    python -m generator.offline_generator --scale-factor 0.1 \\
        --output-dir data_generator/output/scale_0_1

Script đọc ``movies.csv``, ``ratings.csv``, ``tags.csv``, dựng danh mục
``movie_id`` liền mạch ``1..N`` (id có trong seed thì giữ title/genres),
rồi sinh ratings / tags / watch_history theo Zipf + Pareto + Beta và
các injector. Không ghi MinIO và không bắn Kafka — đó là bước nối sau.
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import shutil
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from generator.distributions.beta_rating import sample_star_ratings, summarize_star_ratings
from generator.distributions.pareto import activity_weights, super_user_event_share
from generator.distributions.zipf import popularity_weights
from generator.injectors.duplicate_injector import DuplicateInjectionResult, inject_duplicates
from generator.injectors.schema_evolution_injector import assign_evolved_attributes, write_movie_partitions
from generator.injectors.skew_injector import MovieSkewPlan, apply_genre_skew, plan_movie_skew, sample_from_weights

logger = logging.getLogger("generator.offline")

# Khối lượng tại scale-factor = 100 (đặc tả mục 3).
FULL_SCALE = 100.0
SPEC_ROW_COUNTS = {
    "users": 5_000_000,
    "movies": 100_000,
    "movie_genres": 300_000,
    "ratings": 250_000_000,
    "tags": 30_000_000,
    "watch_history": 800_000_000,
}
DATASETS = ("users", "movies", "movie_genres", "ratings", "tags", "watch_history")
_YEAR_IN_TITLE = re.compile(r"\((\d{4})\)\s*$")
_REVIEW_TEMPLATES = np.array(
    [
        "Worth the runtime",
        "Rewatched with friends",
        "Strong performances",
        "Pacing dragged in the middle",
        "Would recommend",
        "Not my genre, still solid",
        "Great on a big screen",
        "Ending stayed with me",
    ],
    dtype=object,
)
_LANGUAGES = np.array(["en", "fr", "ja", "ko", "es", "hi"], dtype=object)
_LANGUAGE_P = np.array([0.72, 0.05, 0.08, 0.05, 0.06, 0.04])
_COUNTRIES = np.array(["US", "VN", "KR", "JP", "FR", "GB", "DE", "BR"], dtype=object)
_TIERS = np.array(["Free", "Standard", "VIP"], dtype=object)
_TIER_P = np.array([0.50, 0.35, 0.15])
_AGE_GROUPS = np.array(["18-24", "25-34", "35-44", "45-54", "55+"], dtype=object)
_GENDERS = np.array(["female", "male", "other", "unspecified"], dtype=object)
_DEVICES = np.array(["web", "mobile", "smart_tv", "tablet"], dtype=object)
_RESOLUTIONS = np.array(["480p", "720p", "1080p", "4k"], dtype=object)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sinh Parquet offline từ seed MovieLens, có tham số phóng to và tiêm lỗi.",
    )
    parser.add_argument("--scale-factor", type=float, default=1.0, help="100 = đúng volume đặc tả (~101GB). 0.1 = smoke test.")
    parser.add_argument("--skew-ratio", type=float, default=0.8, help="Phần dòng ratings/watch mà nhóm 1%% phim đầu nhận.")
    parser.add_argument("--duplicate-rate", type=float, default=0.025, help="Tỷ lệ bản sao trên tổng dòng sau khi tiêm.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_generator/output/offline"),
        help="Thư mục gốc chứa users/, movies/, ratings/, ...",
    )
    parser.add_argument("--seed-dir", type=Path, default=None, help="Thư mục ml-latest-small. Mặc định tự tìm trong repo.")
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--head-fraction", type=float, default=0.01, help="Tỷ lệ phim thuộc nhóm bom tấn.")
    parser.add_argument("--anchor-movie-id", type=int, default=42, help="movie_id bị ghim là phim nặng nhất.")
    parser.add_argument("--anchor-share", type=float, default=0.04, help="Sàn tỷ lệ dòng của phim neo khi nhóm đầu có nhiều phim.")
    parser.add_argument("--zipf-exponent", type=float, default=1.25, help="s của Zipf bên trong nhóm đầu và nhóm đuôi.")
    parser.add_argument("--super-user-fraction", type=float, default=0.05)
    parser.add_argument("--super-user-mass", type=float, default=0.40)
    parser.add_argument("--pareto-shape", type=float, default=1.5)
    parser.add_argument("--beta-alpha", type=float, default=6.0)
    parser.add_argument("--beta-beta", type=float, default=2.0)
    parser.add_argument("--genre-skew-ratio", type=float, default=0.70)
    parser.add_argument("--schema-evolution-from-partition", type=int, default=45)
    parser.add_argument("--movie-partitions", type=int, default=100)
    parser.add_argument("--duplicate-ts-offset-seconds", type=int, default=3)
    parser.add_argument("--duplicate-next-partition-rate", type=float, default=0.5, help="Phần bản sao bị đẩy sang ngày kế tiếp.")
    parser.add_argument("--num-event-days", type=int, default=0, help="Số ngày partition dt=. 0 = tự chọn theo scale.")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--max-rows-per-file", type=int, default=500_000)
    parser.add_argument(
        "--allow-large-scale",
        action="store_true",
        help="Bắt buộc khi --scale-factor > 2 (hàng chục triệu dòng trở lên).",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
    args = build_parser().parse_args(argv)
    _validate_args(args)

    started = time.perf_counter()
    rng = np.random.default_rng(args.random_seed)
    seed_dir = _resolve_seed_dir(args.seed_dir)
    seed = _load_seed(seed_dir)
    counts = {name: _scaled_rows(name, args.scale_factor) for name in SPEC_ROW_COUNTS}
    counts["movies"] = max(counts["movies"], args.anchor_movie_id)
    _reject_accidental_full_scale(args, counts["watch_history"])

    logger.info(
        "seed %s | movies=%d ratings=%d tags=%d users=%d | phim đông nhất trong seed chiếm %.2f%%",
        seed_dir,
        seed["n_movies"],
        seed["n_ratings"],
        seed["n_tags"],
        seed["n_users"],
        100.0 * seed["top_movie_share"],
    )
    logger.info(
        "scale-factor=%.4g = %.3f%% volume đặc tả | users=%d movies=%d genres=%d ratings=%d tags=%d watch=%d",
        args.scale_factor,
        100.0 * args.scale_factor / FULL_SCALE,
        counts["users"],
        counts["movies"],
        counts["movie_genres"],
        counts["ratings"],
        counts["tags"],
        counts["watch_history"],
    )

    theoretical, solved_s = popularity_weights(
        counts["movies"],
        head_fraction=args.head_fraction,
        head_mass=args.skew_ratio,
    )
    head_n = max(1, int(round(args.head_fraction * counts["movies"])))
    logger.info(
        "Zipf thuần trên %d phim: s hiệu chỉnh=%.3f để top %d hạng đạt %.2f%% (spec gợi ý s≈1.25; sampling dùng s=%.3f bên trong từng nhóm)",
        counts["movies"],
        solved_s,
        head_n,
        100.0 * float(theoretical[:head_n].sum()),
        args.zipf_exponent,
    )

    movies, genre_lists = _build_movies(seed["movies_by_id"], counts["movies"], rng)
    users = _build_users(counts["users"], rng)
    user_weights, super_user_ids = activity_weights(
        users["user_id"],
        head_fraction=args.super_user_fraction,
        head_mass=args.super_user_mass,
        pareto_shape=args.pareto_shape,
        rng=rng,
    )
    skew = plan_movie_skew(
        movies["movie_id"],
        skew_ratio=args.skew_ratio,
        head_fraction=args.head_fraction,
        anchor_movie_id=args.anchor_movie_id,
        anchor_share=args.anchor_share,
        zipf_exponent=args.zipf_exponent,
    )
    num_days = args.num_event_days if args.num_event_days > 0 else _auto_event_days(args.scale_factor)
    start_date = np.datetime64(args.start_date, "D")

    ratings = _build_ratings(counts["ratings"], users["user_id"], user_weights, skew, movies, start_date, num_days, rng, args)
    tags = _build_tags(counts["tags"], users["user_id"], user_weights, skew, seed["tag_vocab"], start_date, num_days, rng)
    watch = _build_watch_history(counts["watch_history"], users["user_id"], user_weights, skew, movies, start_date, num_days, rng)
    genres = _build_genres(movies["movie_id"], genre_lists, counts["movie_genres"], args.genre_skew_ratio, rng)

    ratings_dup = inject_duplicates(
        ratings,
        rate=args.duplicate_rate,
        id_column="rating_id",
        timestamp_columns=("rating_ts",),
        rng=rng,
        timestamp_offset_seconds=args.duplicate_ts_offset_seconds,
        next_partition_rate=args.duplicate_next_partition_rate,
    )
    watch_dup = inject_duplicates(
        watch,
        rate=args.duplicate_rate,
        id_column="session_id",
        timestamp_columns=("start_ts", "end_ts"),
        rng=rng,
        timestamp_offset_seconds=args.duplicate_ts_offset_seconds,
        next_partition_rate=args.duplicate_next_partition_rate,
    )

    output_dir = args.output_dir.resolve()
    _reset_output(output_dir)
    _write_users(users, output_dir / "users", args.max_rows_per_file)
    evolution = write_movie_partitions(
        movies,
        output_dir / "movies",
        n_partitions=args.movie_partitions,
        evolution_from_partition=args.schema_evolution_from_partition,
    )
    _write_frame(
        {"movie_id": genres["movie_id"], "genre_name": genres["genre_name"]},
        output_dir / "movie_genres",
        _GENRE_SCHEMA,
        args.max_rows_per_file,
    )
    _write_dated(ratings_dup.columns, output_dir / "ratings", _RATINGS_SCHEMA, "event_date", args.max_rows_per_file)
    _write_dated(tags, output_dir / "tags", _TAGS_SCHEMA, "event_date", args.max_rows_per_file)
    _write_dated(watch_dup.columns, output_dir / "watch_history", _WATCH_SCHEMA, "event_date", args.max_rows_per_file)

    elapsed = time.perf_counter() - started
    _log_stats(
        args=args,
        output_dir=output_dir,
        movies=movies,
        skew=skew,
        ratings_dup=ratings_dup,
        watch_dup=watch_dup,
        tags=tags,
        genres=genres,
        super_user_ids=super_user_ids,
        evolution=evolution,
        elapsed=elapsed,
    )


def _validate_args(args: argparse.Namespace) -> None:
    parser = build_parser()
    if args.scale_factor <= 0:
        parser.error("--scale-factor phải > 0")
    if not 0 < args.skew_ratio <= 1:
        parser.error("--skew-ratio phải nằm trong (0, 1]")
    if not 0 <= args.duplicate_rate < 1:
        parser.error("--duplicate-rate phải nằm trong [0, 1)")
    if args.duplicate_rate >= 0.5:
        parser.error("--duplicate-rate phải < 0.5 vì mỗi dòng gốc chỉ nhân một lần")
    if args.schema_evolution_from_partition < 1 or args.movie_partitions < 1:
        parser.error("số partition phải >= 1")
    if args.max_rows_per_file < 1:
        parser.error("--max-rows-per-file phải >= 1")
    if args.anchor_movie_id < 1:
        parser.error("--anchor-movie-id phải >= 1")


def _reject_accidental_full_scale(args: argparse.Namespace, watch_rows: int) -> None:
    if args.scale_factor > 2 and not args.allow_large_scale:
        build_parser().error(
            f"--scale-factor {args.scale_factor} sẽ sinh khoảng {watch_rows:,} dòng watch_history. "
            "Thêm --allow-large-scale nếu đây là lần chạy lớn bạn chủ đích."
        )


def _scaled_rows(name: str, scale_factor: float) -> int:
    return max(1, int(round(SPEC_ROW_COUNTS[name] * (scale_factor / FULL_SCALE))))


def _auto_event_days(scale_factor: float) -> int:
    if scale_factor <= 0.2:
        return 4
    if scale_factor <= 1:
        return 14
    if scale_factor <= 10:
        return 60
    return 180


def _resolve_seed_dir(explicit: Path | None) -> Path:
    if explicit is not None:
        path = explicit
    else:
        candidates = [
            Path("data_generator/data_offline/ml-latest-small"),
            Path(__file__).resolve().parent.parent / "data_generator/data_offline/ml-latest-small",
        ]
        path = next((item for item in candidates if (item / "movies.csv").exists()), candidates[0])
    if not (path / "movies.csv").exists():
        raise SystemExit(f"không thấy movies.csv trong seed dir: {path}")
    return path


def _load_seed(seed_dir: Path) -> dict[str, object]:
    movies_by_id: dict[int, dict[str, str]] = {}
    with (seed_dir / "movies.csv").open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            movies_by_id[int(row["movieId"])] = row

    rating_movies: list[int] = []
    rating_users: set[int] = set()
    with (seed_dir / "ratings.csv").open(encoding="utf-8") as handle:
        header = handle.readline()
        if "movieId" not in header:
            raise SystemExit("ratings.csv thiếu header movieId")
        for line in handle:
            user_raw, movie_raw, _rating, _ts = line.rstrip("\n").split(",")
            rating_users.add(int(user_raw))
            rating_movies.append(int(movie_raw))
    rating_movie_ids = np.asarray(rating_movies, dtype=np.int64)
    _unique, counts = np.unique(rating_movie_ids, return_counts=True)
    top_movie_share = float(counts.max() / counts.sum()) if counts.size else 0.0

    tag_values: list[str] = []
    with (seed_dir / "tags.csv").open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            tag_values.append(row["tag"])
    tag_vocab = np.array(sorted(set(tag_values)) or ["classic"], dtype=object)

    return {
        "movies_by_id": movies_by_id,
        "n_movies": len(movies_by_id),
        "n_ratings": int(rating_movie_ids.size),
        "n_tags": len(tag_values),
        "n_users": len(rating_users),
        "top_movie_share": top_movie_share,
        "tag_vocab": tag_vocab,
    }


def _build_movies(
    movies_by_id: dict[int, dict[str, str]],
    n_movies: int,
    rng: np.random.Generator,
) -> tuple[dict[str, np.ndarray], list[list[str]]]:
    movie_id = np.arange(1, n_movies + 1, dtype=np.int64)
    titles = np.empty(n_movies, dtype=object)
    years = np.empty(n_movies, dtype=np.int32)
    genre_lists: list[list[str]] = []
    for index, mid in enumerate(movie_id.tolist()):
        seed_row = movies_by_id.get(mid)
        if seed_row is None:
            year = int(rng.integers(1970, 2025))
            titles[index] = f"Synthetic Feature {mid} ({year})"
            years[index] = year
            genre_lists.append(["Drama"])
        else:
            titles[index] = seed_row["title"]
            years[index] = _parse_year(seed_row["title"])
            genre_lists.append(_parse_genres(seed_row["genres"]))

    runtime = rng.integers(80, 181, size=n_movies).astype(np.int32)
    language = rng.choice(_LANGUAGES, size=n_movies, p=_LANGUAGE_P)
    budget = np.exp(rng.normal(16.0, 1.1, size=n_movies)).astype(np.int64)
    anchor_slot = np.flatnonzero(movie_id == 42)
    if anchor_slot.size:
        budget[anchor_slot] = int(budget[anchor_slot[0]] * 4 + 150_000_000)
    created = np.datetime64("2010-01-01", "s") + rng.integers(0, 16 * 365 * 24 * 3600, size=n_movies).astype("timedelta64[s]")
    evolved = assign_evolved_attributes(n_movies, rng)
    columns = {
        "movie_id": movie_id,
        "title": titles,
        "release_year": years,
        "runtime_minutes": runtime,
        "original_language": language,
        "budget": budget,
        "director_id": rng.integers(1, 20_001, size=n_movies, dtype=np.int64),
        "created_ts": created,
        **evolved,
    }
    return columns, genre_lists


def _build_users(n_users: int, rng: np.random.Generator) -> dict[str, np.ndarray]:
    signup = np.datetime64("2018-01-01", "s") + rng.integers(0, 8 * 365 * 24 * 3600, size=n_users).astype("timedelta64[s]")
    return {
        "user_id": np.arange(1, n_users + 1, dtype=np.int64),
        "signup_ts": signup,
        "country": rng.choice(_COUNTRIES, size=n_users),
        "subscription_tier": rng.choice(_TIERS, size=n_users, p=_TIER_P),
        "preferred_language": rng.choice(_LANGUAGES, size=n_users, p=_LANGUAGE_P),
        "age_group": rng.choice(_AGE_GROUPS, size=n_users),
        "gender": rng.choice(_GENDERS, size=n_users),
    }


def _build_ratings(n_rows, user_ids, user_weights, skew, movies, start_date, num_days, rng, args) -> dict[str, np.ndarray]:
    user_id = sample_from_weights(user_ids, user_weights, n_rows, rng)
    movie_id = sample_from_weights(skew.movie_ids, skew.weights, n_rows, rng)
    rating_val = sample_star_ratings(n_rows, rng, alpha=args.beta_alpha, beta=args.beta_beta)
    event_date, rating_ts = _event_timestamps(n_rows, start_date, num_days, rng)
    return {
        "rating_id": np.arange(1, n_rows + 1, dtype=np.int64),
        "user_id": user_id,
        "movie_id": movie_id,
        "rating_val": rating_val,
        "rating_ts": rating_ts,
        "review_text": rng.choice(_REVIEW_TEMPLATES, size=n_rows),
        "event_date": event_date,
    }


def _build_tags(n_rows, user_ids, user_weights, skew, tag_vocab, start_date, num_days, rng) -> dict[str, np.ndarray]:
    event_date, tag_ts = _event_timestamps(n_rows, start_date, num_days, rng)
    return {
        "tag_id": np.arange(1, n_rows + 1, dtype=np.int64),
        "user_id": sample_from_weights(user_ids, user_weights, n_rows, rng),
        "movie_id": sample_from_weights(skew.movie_ids, skew.weights, n_rows, rng),
        "tag_name": rng.choice(tag_vocab, size=n_rows),
        "tag_ts": tag_ts,
        "event_date": event_date,
    }


def _build_watch_history(n_rows, user_ids, user_weights, skew, movies, start_date, num_days, rng) -> dict[str, np.ndarray]:
    user_id = sample_from_weights(user_ids, user_weights, n_rows, rng)
    movie_id = sample_from_weights(skew.movie_ids, skew.weights, n_rows, rng)
    event_date, start_ts = _event_timestamps(n_rows, start_date, num_days, rng)
    runtime_minutes = movies["runtime_minutes"][np.searchsorted(movies["movie_id"], movie_id)]
    completion = np.clip(rng.beta(5.0, 1.6, size=n_rows), 0.02, 1.0)
    duration = np.maximum(1, (runtime_minutes.astype(np.int64) * 60.0 * completion).astype(np.int32))
    end_ts = start_ts + duration.astype("timedelta64[s]")
    return {
        "session_id": np.arange(1, n_rows + 1, dtype=np.int64),
        "user_id": user_id,
        "movie_id": movie_id,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "watch_duration_seconds": duration,
        "completion_rate": completion.astype(np.float64),
        "device_type": rng.choice(_DEVICES, size=n_rows),
        "resolution": rng.choice(_RESOLUTIONS, size=n_rows),
        "event_date": event_date,
    }


def _build_genres(movie_ids, genre_lists, n_target, genre_skew_ratio, rng) -> dict[str, np.ndarray]:
    owners: list[int] = []
    names: list[str] = []
    for movie_id, genres in zip(movie_ids.tolist(), genre_lists):
        for name in genres:
            owners.append(movie_id)
            names.append(name)
    owner_arr = np.asarray(owners, dtype=np.int64)
    name_arr = np.asarray(names, dtype=object)
    if owner_arr.size == 0:
        raise SystemExit("không dựng được movie_genres từ seed")
    if owner_arr.size >= n_target:
        picked = rng.choice(owner_arr.size, size=n_target, replace=False)
    else:
        picked = rng.choice(owner_arr.size, size=n_target, replace=True)
    skewed = apply_genre_skew(name_arr[picked], skew_ratio=genre_skew_ratio, rng=rng)
    return {"movie_id": owner_arr[picked], "genre_name": skewed}


def _event_timestamps(n_rows: int, start_date: np.datetime64, num_days: int, rng: np.random.Generator):
    day_offset = rng.integers(0, num_days, size=n_rows)
    event_date = start_date + day_offset.astype("timedelta64[D]")
    second_of_day = rng.integers(0, 86_400, size=n_rows)
    timestamp = event_date.astype("datetime64[s]") + second_of_day.astype("timedelta64[s]")
    return event_date, timestamp


def _parse_year(title: str) -> int:
    match = _YEAR_IN_TITLE.search(title)
    return int(match.group(1)) if match else 2000


def _parse_genres(raw: str) -> list[str]:
    if not raw or raw == "(no genres listed)":
        return ["Drama"]
    parsed = [part for part in raw.split("|") if part]
    return parsed or ["Drama"]


def _reset_output(output_dir: Path) -> None:
    if output_dir in {Path("/"), Path.home()}:
        raise SystemExit(f"từ chối ghi đè thư mục: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in DATASETS:
        target = output_dir / name
        if target.exists():
            shutil.rmtree(target)


def _write_users(columns: dict[str, np.ndarray], output_dir: Path, max_rows: int) -> None:
    _write_frame(columns, output_dir, _USERS_SCHEMA, max_rows, prefix="partition")


def _write_frame(columns, output_dir: Path, schema: pa.Schema, max_rows: int, prefix: str = "partition") -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    n_rows = int(np.asarray(next(iter(columns.values()))).size)
    if n_rows == 0:
        return
    part = 0
    for start in range(0, n_rows, max_rows):
        index = np.arange(start, min(start + max_rows, n_rows))
        path = output_dir / f"{prefix}_{part:05d}.parquet"
        _write_indexed(columns, index, schema, path)
        part += 1


def _write_dated(columns, output_dir: Path, schema: pa.Schema, date_column: str, max_rows: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    dates = np.asarray(columns[date_column])
    order = np.argsort(dates, kind="mergesort")
    sorted_dates = dates[order]
    unique_dates, starts = np.unique(sorted_dates, return_index=True)
    ends = np.append(starts[1:], sorted_dates.size)
    for day, start, end in zip(unique_dates, starts, ends):
        day_index = order[start:end]
        folder = output_dir / f"dt={str(day)}"
        folder.mkdir(parents=True, exist_ok=True)
        for part, chunk in enumerate(_chunks(day_index, max_rows)):
            _write_indexed(columns, chunk, schema, folder / f"part-{part:05d}.parquet")


def _chunks(index: np.ndarray, max_rows: int):
    for start in range(0, int(index.size), max_rows):
        yield index[start : start + max_rows]


def _write_indexed(columns: dict[str, np.ndarray], index: np.ndarray, schema: pa.Schema, path: Path) -> None:
    arrays = [pa.array(np.asarray(columns[field.name])[index], type=field.type) for field in schema]
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), path, compression="snappy")


def _log_stats(
    *,
    args: argparse.Namespace,
    output_dir: Path,
    movies: dict[str, np.ndarray],
    skew: MovieSkewPlan,
    ratings_dup: DuplicateInjectionResult,
    watch_dup: DuplicateInjectionResult,
    tags: dict[str, np.ndarray],
    genres: dict[str, np.ndarray],
    super_user_ids: np.ndarray,
    evolution: dict[str, object],
    elapsed: float,
) -> None:
    titles = {int(movie_id): str(title) for movie_id, title in zip(movies["movie_id"], movies["title"])}
    rating_ids = ratings_dup.columns["movie_id"]
    watch_ids = watch_dup.columns["movie_id"]
    combined_ids = np.concatenate([rating_ids, watch_ids])
    rating_summary = summarize_star_ratings(ratings_dup.columns["rating_val"])
    genre_hot = np.isin(genres["genre_name"], ["Action", "Comedy"])
    bytes_written = sum(path.stat().st_size for path in output_dir.rglob("*.parquet"))

    lines = [
        "========== thống kê generator ==========",
        f"output_dir          : {output_dir}",
        f"elapsed_seconds     : {elapsed:.2f}",
        f"parquet_bytes       : {bytes_written:,}",
        (
            f"ratings duplicate   : {100 * ratings_dup.actual_rate:.4f}% "
            f"(target {100 * args.duplicate_rate:.4f}%) | "
            f"extra={ratings_dup.n_extra:,} / total={ratings_dup.n_total:,} | "
            f"sang ngày kế tiếp={ratings_dup.n_next_partition:,}"
        ),
        (
            f"watch duplicate     : {100 * watch_dup.actual_rate:.4f}% "
            f"(target {100 * args.duplicate_rate:.4f}%) | "
            f"extra={watch_dup.n_extra:,} / total={watch_dup.n_total:,} | "
            f"sang ngày kế tiếp={watch_dup.n_next_partition:,}"
        ),
        _format_example("ratings ví dụ retry", ratings_dup.example),
        _format_example("watch ví dụ retry", watch_dup.example),
        (
            f"anchor movie_id={skew.anchor_movie_id} trọng số mục tiêu={100 * skew.anchor_weight:.2f}% | "
            f"nhóm đầu n={skew.head_count} | zipf s={skew.zipf_exponent:.2f} | "
            f"đã hạ s trong head={skew.tempered_head} | đã hạ s trong tail={skew.tempered_tail}"
        ),
        _anchor_note(args, skew, int(movies["movie_id"].size)),
        _format_top("ratings top 5", rating_ids, titles),
        _format_top("watch top 5", watch_ids, titles),
        _format_top("ratings+watch top 5", combined_ids, titles),
        (
            f"nhóm bom tấn        : ratings {100 * _head_share(rating_ids, movies['movie_id'], skew.head_mask):.2f}% | "
            f"watch {100 * _head_share(watch_ids, movies['movie_id'], skew.head_mask):.2f}% | "
            f"target {100 * args.skew_ratio:.2f}%"
        ),
        (
            f"super-user {100 * args.super_user_fraction:.1f}% user → "
            f"ratings {100 * super_user_event_share(ratings_dup.columns['user_id'], super_user_ids):.2f}% | "
            f"tags {100 * super_user_event_share(tags['user_id'], super_user_ids):.2f}% | "
            f"watch {100 * super_user_event_share(watch_dup.columns['user_id'], super_user_ids):.2f}% | "
            f"target {100 * args.super_user_mass:.2f}%"
        ),
        (
            f"rating_val          : mean={rating_summary['mean']:.3f} | "
            f"trong [3.5, 4.5]={100 * rating_summary['share_3_5_to_4_5']:.2f}% | "
            f"<=2 sao={100 * rating_summary['share_at_most_2']:.2f}%"
        ),
        (
            f"genre Action+Comedy : {100 * float(genre_hot.mean()):.2f}% "
            f"(target {100 * args.genre_skew_ratio:.2f}%) trên {genres['genre_name'].size:,} dòng"
        ),
        (
            f"schema evolution    : mốc partition {evolution['evolution_from_partition']} | "
            f"file schema cũ={evolution['n_old_schema_files']} ({evolution['old_example']}) "
            f"cột={evolution['old_schema_columns']} | "
            f"file schema mới={evolution['n_new_schema_files']} ({evolution['new_example']}) "
            f"cột={evolution['new_schema_columns']}"
        ),
        "========================================",
    ]
    logger.info("\n".join(lines))


def _head_share(fact_movie_ids: np.ndarray, catalog_ids: np.ndarray, head_mask: np.ndarray) -> float:
    if fact_movie_ids.size == 0:
        return 0.0
    return float(np.isin(fact_movie_ids, catalog_ids[head_mask]).mean())


def _format_top(label: str, movie_ids: np.ndarray, titles: dict[int, str]) -> str:
    unique, counts = np.unique(movie_ids, return_counts=True)
    order = np.argsort(counts)[::-1][:5]
    total = int(movie_ids.size)
    rows = [f"{label} trên {total:,} dòng:"]
    cumulative = 0.0
    for rank, idx in enumerate(order, start=1):
        movie_id = int(unique[idx])
        share = float(counts[idx] / total)
        cumulative += share
        title = titles.get(movie_id, "?")[:42]
        rows.append(
            f"  {rank}. movie_id={movie_id:<6} {title:<42} rows={int(counts[idx]):>8,}  "
            f"share={100 * share:6.2f}%  cumulative={100 * cumulative:6.2f}%"
        )
    return "\n".join(rows)


def _anchor_note(args: argparse.Namespace, skew: MovieSkewPlan, n_movies: int) -> str:
    if skew.head_count == 1:
        return (
            f"anchor giải thích    : 1% của {n_movies} phim = 1 phim, nên movie_id={skew.anchor_movie_id} "
            f"nhận trọn skew-ratio {100 * args.skew_ratio:.1f}% "
            f"(sàn anchor-share {100 * args.anchor_share:.1f}% chỉ chia phần còn lại khi nhóm đầu có từ 2 phim)."
        )
    return (
        f"anchor giải thích    : movie_id={skew.anchor_movie_id} là khóa nặng nhất, "
        f"tối thiểu {100 * args.anchor_share:.1f}% dòng; phần còn lại của nhóm đầu chia theo Zipf."
    )


def _format_example(label: str, example: dict[str, object]) -> str:
    if not example:
        return f"{label}: (không tiêm)"
    business = [f"user_id={example['user_id']}", f"movie_id={example['movie_id']}"]
    if "rating_val" in example:
        business.append(f"rating_val={example['rating_val']}")
    return (
        f"{label}: {', '.join(business)} | "
        f"{example['id_column']} {example['source_id']} → {example['duplicate_id']} | "
        f"{example['source_ts']} → {example['duplicate_ts']} (delta={example['delta_seconds']}s)"
    )


_USERS_SCHEMA = pa.schema(
    [
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("signup_ts", pa.timestamp("s"), nullable=False),
        pa.field("country", pa.string(), nullable=False),
        pa.field("subscription_tier", pa.string(), nullable=False),
        pa.field("preferred_language", pa.string(), nullable=False),
        pa.field("age_group", pa.string(), nullable=False),
        pa.field("gender", pa.string(), nullable=False),
    ]
)
_GENRE_SCHEMA = pa.schema(
    [
        pa.field("movie_id", pa.int64(), nullable=False),
        pa.field("genre_name", pa.string(), nullable=False),
    ]
)
_RATINGS_SCHEMA = pa.schema(
    [
        pa.field("rating_id", pa.int64(), nullable=False),
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("movie_id", pa.int64(), nullable=False),
        pa.field("rating_val", pa.float64(), nullable=False),
        pa.field("rating_ts", pa.timestamp("s"), nullable=False),
        pa.field("review_text", pa.string(), nullable=True),
        pa.field("event_date", pa.date32(), nullable=False),
    ]
)
_TAGS_SCHEMA = pa.schema(
    [
        pa.field("tag_id", pa.int64(), nullable=False),
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("movie_id", pa.int64(), nullable=False),
        pa.field("tag_name", pa.string(), nullable=False),
        pa.field("tag_ts", pa.timestamp("s"), nullable=False),
        pa.field("event_date", pa.date32(), nullable=False),
    ]
)
_WATCH_SCHEMA = pa.schema(
    [
        pa.field("session_id", pa.int64(), nullable=False),
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("movie_id", pa.int64(), nullable=False),
        pa.field("start_ts", pa.timestamp("s"), nullable=False),
        pa.field("end_ts", pa.timestamp("s"), nullable=False),
        pa.field("watch_duration_seconds", pa.int32(), nullable=False),
        pa.field("completion_rate", pa.float64(), nullable=False),
        pa.field("device_type", pa.string(), nullable=False),
        pa.field("resolution", pa.string(), nullable=False),
        pa.field("event_date", pa.date32(), nullable=False),
    ]
)


if __name__ == "__main__":
    main()
