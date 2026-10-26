"""Ghi scale lớn theo chunk để không giữ cả 800 triệu dòng trong RAM.

Cùng phân phối, injector và schema với ``offline_generator``. CLI gọi module
này khi ``watch_history`` vượt quá hai triệu dòng gốc. Scale 0.1 vẫn đi
đường một mảng như trước.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np

from generator.injectors.duplicate_injector import inject_duplicates

logger = logging.getLogger("generator.offline")

MEMORY_ROW_LIMIT = 2_000_000


def exceeds_memory_budget(watch_rows: int) -> bool:
    return watch_rows > MEMORY_ROW_LIMIT


def run_chunked(args, rng: np.random.Generator, seed: dict, counts: dict[str, int]) -> dict:
    from generator import offline_generator as og

    movies, genre_lists = og._build_movies(seed["movies_by_id"], counts["movies"], rng)
    users = og._build_users(counts["users"], rng)
    user_weights, super_user_ids = og.activity_weights(
        users["user_id"],
        head_fraction=args.super_user_fraction,
        head_mass=args.super_user_mass,
        pareto_shape=args.pareto_shape,
        rng=rng,
    )
    skew = og.plan_movie_skew(
        movies["movie_id"],
        skew_ratio=args.skew_ratio,
        head_fraction=args.head_fraction,
        anchor_movie_id=args.anchor_movie_id,
        anchor_share=args.anchor_share,
        zipf_exponent=args.zipf_exponent,
    )
    num_days = args.num_event_days if args.num_event_days > 0 else og._auto_event_days(args.scale_factor)
    start_date = np.datetime64(args.start_date, "D")
    output_dir = args.output_dir.resolve()
    og._reset_output(output_dir)
    og._write_users(users, output_dir / "users", args.max_rows_per_file)
    evolution = og.write_movie_partitions(
        movies,
        output_dir / "movies",
        n_partitions=args.movie_partitions,
        evolution_from_partition=args.schema_evolution_from_partition,
    )
    genres = og._build_genres(movies["movie_id"], genre_lists, counts["movie_genres"], args.genre_skew_ratio, rng)
    og._write_frame(
        {"movie_id": genres["movie_id"], "genre_name": genres["genre_name"]},
        output_dir / "movie_genres",
        og._GENRE_SCHEMA,
        args.max_rows_per_file,
    )
    genre_hot = float(np.isin(genres["genre_name"], ["Action", "Comedy"]).mean()) if genres["genre_name"].size else 0.0

    titles = {int(movie_id): str(title) for movie_id, title in zip(movies["movie_id"], movies["title"])}
    head_ids = movies["movie_id"][skew.head_mask]
    stats = _Stats(n_movies=int(movies["movie_id"].size), n_users=int(users["user_id"].size))
    chunk = max(1, int(args.max_rows_per_file))

    _stream_fact(
        stats,
        label="ratings",
        n_rows=counts["ratings"],
        chunk=chunk,
        start_date=start_date,
        num_days=num_days,
        build=lambda n, day: og._build_ratings(n, users["user_id"], user_weights, skew, movies, day, 1, rng, args),
        id_column="rating_id",
        timestamp_columns=("rating_ts",),
        duplicate_rate=args.duplicate_rate,
        schema=og._RATINGS_SCHEMA,
        output_dir=output_dir / "ratings",
        args=args,
        rng=rng,
        head_ids=head_ids,
        super_user_ids=super_user_ids,
        on_rows=lambda columns: stats.add_ratings(columns["rating_val"]),
    )
    _stream_fact(
        stats,
        label="tags",
        n_rows=counts["tags"],
        chunk=chunk,
        start_date=start_date,
        num_days=num_days,
        build=lambda n, day: og._build_tags(n, users["user_id"], user_weights, skew, seed["tag_vocab"], day, 1, rng),
        id_column="tag_id",
        timestamp_columns=("tag_ts",),
        duplicate_rate=0.0,
        schema=og._TAGS_SCHEMA,
        output_dir=output_dir / "tags",
        args=args,
        rng=rng,
        head_ids=head_ids,
        super_user_ids=super_user_ids,
        on_rows=None,
    )
    _stream_fact(
        stats,
        label="watch_history",
        n_rows=counts["watch_history"],
        chunk=chunk,
        start_date=start_date,
        num_days=num_days,
        build=lambda n, day: og._build_watch_history(n, users["user_id"], user_weights, skew, movies, day, 1, rng),
        id_column="session_id",
        timestamp_columns=("start_ts", "end_ts"),
        duplicate_rate=args.duplicate_rate,
        schema=og._WATCH_SCHEMA,
        output_dir=output_dir / "watch_history",
        args=args,
        rng=rng,
        head_ids=head_ids,
        super_user_ids=super_user_ids,
        on_rows=None,
    )

    payload = stats.payload(
        args=args,
        output_dir=output_dir,
        titles=titles,
        anchor_movie_id=int(skew.anchor_movie_id),
        head_count=int(skew.head_count),
        head_ids=head_ids,
        genre_hot=genre_hot,
        genre_rows=int(genres["genre_name"].size),
        evolution=evolution,
        counts=counts,
    )
    (output_dir / "characteristics.json").write_text(json.dumps(payload, indent=2, default=str))
    if args.scale_factor >= 100:
        doc = Path("docs/proof/23_dataset_scale_100.md")
        doc.parent.mkdir(parents=True, exist_ok=True)
        doc.write_text(_markdown(payload))
        logger.info("đã ghi %s", doc)
    logger.info(
        "chunked xong | parquet_bytes=%s | ratings_dup=%.4f | watch_dup=%.4f | anchor_share=%.2f%%",
        f"{payload['parquet_bytes']:,}",
        100 * payload["ratings"]["duplicate_rate"],
        100 * payload["watch_history"]["duplicate_rate"],
        100 * payload["combined"]["anchor_share"],
    )
    return payload


class _Stats:
    def __init__(self, n_movies: int, n_users: int) -> None:
        self.n_movies = n_movies
        self.n_users = n_users
        self.movie_counts = {name: np.zeros(n_movies + 1, dtype=np.int64) for name in ("ratings", "watch_history", "tags")}
        self.tables: dict[str, dict] = {}
        self.rating_sum = 0.0
        self.rating_n = 0
        self.rating_band = 0
        self.rating_low = 0
        self.parts: dict[str, dict[str, int]] = {}

    def add_ratings(self, values: np.ndarray) -> None:
        ratings = np.asarray(values, dtype=np.float64)
        self.rating_n += int(ratings.size)
        self.rating_sum += float(ratings.sum())
        self.rating_band += int(((ratings >= 3.5) & (ratings <= 4.5)).sum())
        self.rating_low += int((ratings <= 2.0).sum())

    def payload(self, **extra) -> dict:
        args = extra["args"]
        output_dir: Path = extra["output_dir"]
        volumes = _volumes(output_dir)
        combined = self.movie_counts["ratings"] + self.movie_counts["watch_history"]
        return {
            "scale_factor": args.scale_factor,
            "output_dir": str(output_dir),
            "parquet_bytes": volumes["total_bytes"],
            "volumes": volumes["by_dataset"],
            "target_rows": extra["counts"],
            "skew_ratio_target": args.skew_ratio,
            "anchor_movie_id": extra["anchor_movie_id"],
            "anchor_share_floor": args.anchor_share,
            "head_fraction": args.head_fraction,
            "head_count": extra["head_count"],
            "duplicate_rate_target": args.duplicate_rate,
            "super_user_fraction": args.super_user_fraction,
            "super_user_mass_target": args.super_user_mass,
            "genre_skew_target": args.genre_skew_ratio,
            "genre_action_comedy_share": extra["genre_hot"],
            "genre_rows": extra["genre_rows"],
            "schema_evolution": extra["evolution"],
            "ratings": self.tables["ratings"],
            "tags": self.tables["tags"],
            "watch_history": self.tables["watch_history"],
            "rating_value": {
                "mean": (self.rating_sum / self.rating_n) if self.rating_n else 0.0,
                "share_3_5_to_4_5": (self.rating_band / self.rating_n) if self.rating_n else 0.0,
                "share_at_most_2": (self.rating_low / self.rating_n) if self.rating_n else 0.0,
            },
            "combined": _movie_report(combined, extra["titles"], extra["anchor_movie_id"], extra["head_ids"]),
            "cardinality": {
                "movie_id": {
                    "unique": int(np.count_nonzero(combined)),
                    "catalog": self.n_movies,
                    "rows": int(combined.sum()),
                },
                "user_id_ratings": self.tables["ratings"]["user_cardinality"],
                "user_id_watch": self.tables["watch_history"]["user_cardinality"],
            },
        }


def _portions(total: int, parts: int) -> list[int]:
    """Chia số dòng theo ngày. Ngày đầu nhận phần dư để tổng không lệch."""
    parts = max(1, int(parts))
    base, remainder = divmod(int(total), parts)
    return [base + (1 if index < remainder else 0) for index in range(parts)]


def _stream_fact(
    stats: _Stats,
    *,
    label: str,
    n_rows: int,
    chunk: int,
    start_date,
    num_days: int,
    build,
    id_column: str,
    timestamp_columns: tuple[str, ...],
    duplicate_rate: float,
    schema,
    output_dir: Path,
    args,
    rng,
    head_ids: np.ndarray,
    super_user_ids: np.ndarray,
    on_rows,
) -> None:
    from generator.distributions.pareto import super_user_event_share

    next_id = 1
    base_rows = 0
    extra_rows = 0
    next_partition = 0
    head_hits = 0
    super_hits = 0.0
    seen_users = np.zeros(stats.n_users + 1, dtype=np.bool_)
    example = {}
    written = 0
    for offset, portion in enumerate(_portions(n_rows, num_days)):
        if portion <= 0:
            continue
        day = start_date + np.timedelta64(offset, "D")
        produced = 0
        while produced < portion:
            size = min(chunk, portion - produced)
            columns = build(size, day)
            columns[id_column] = np.arange(next_id, next_id + size, dtype=np.int64)
            duplicated = inject_duplicates(
                columns,
                rate=duplicate_rate,
                id_column=id_column,
                timestamp_columns=timestamp_columns,
                rng=rng,
                timestamp_offset_seconds=args.duplicate_ts_offset_seconds,
                next_partition_rate=args.duplicate_next_partition_rate,
            )
            frame = duplicated.columns
            next_id = int(np.max(frame[id_column])) + 1
            movie_ids = np.asarray(frame["movie_id"], dtype=np.int64)
            user_ids = np.asarray(frame["user_id"], dtype=np.int64)
            np.add.at(stats.movie_counts[label], movie_ids, 1)
            head_hits += int(np.isin(movie_ids, head_ids).sum())
            super_hits += super_user_event_share(user_ids, super_user_ids) * int(user_ids.size)
            seen_users[user_ids] = True
            if on_rows is not None:
                on_rows(frame)
            if not example and duplicated.example:
                example = dict(duplicated.example)
            _append_dated(frame, output_dir, schema, "event_date", chunk, stats.parts.setdefault(label, {}))
            base_rows += duplicated.n_base
            extra_rows += duplicated.n_extra
            next_partition += duplicated.n_next_partition
            produced += size
            written += size
            if written == size or written % (chunk * 20) == 0 or written == n_rows:
                logger.info("%s %s / %s dòng gốc | ngày %s", label, f"{written:,}", f"{n_rows:,}", day)
    total = base_rows + extra_rows
    stats.tables[label] = {
        "base_rows": base_rows,
        "extra_rows": extra_rows,
        "rows": total,
        "duplicate_rate": (extra_rows / total) if total else 0.0,
        "next_partition_rows": next_partition,
        "head_share": (head_hits / total) if total else 0.0,
        "super_user_share": (super_hits / total) if total else 0.0,
        "example": example,
        "user_cardinality": {
            "unique": int(np.count_nonzero(seen_users)),
            "catalog": stats.n_users,
            "rows": total,
            "unique_over_rows": (int(np.count_nonzero(seen_users)) / total) if total else 0.0,
        },
    }


def _append_dated(columns, output_dir: Path, schema, date_column: str, max_rows: int, parts: dict[str, int]) -> None:
    from generator import offline_generator as og

    output_dir.mkdir(parents=True, exist_ok=True)
    dates = np.asarray(columns[date_column])
    order = np.argsort(dates, kind="mergesort")
    sorted_dates = dates[order]
    unique_dates, starts = np.unique(sorted_dates, return_index=True)
    ends = np.append(starts[1:], sorted_dates.size)
    for day, start, end in zip(unique_dates, starts, ends):
        day_index = order[int(start) : int(end)]
        folder = output_dir / f"dt={day}"
        folder.mkdir(parents=True, exist_ok=True)
        key = str(folder)
        for chunk in og._chunks(day_index, max_rows):
            part = parts.get(key, 0)
            og._write_indexed(columns, chunk, schema, folder / f"part-{part:05d}.parquet")
            parts[key] = part + 1


def _movie_report(counts: np.ndarray, titles: dict[int, str], anchor_movie_id: int, head_ids: np.ndarray) -> dict:
    total = int(counts.sum())
    if total == 0:
        return {"rows": 0, "anchor_share": 0.0, "head_share": 0.0, "top5": []}
    head_rows = int(sum(int(counts[int(movie_id)]) for movie_id in np.asarray(head_ids).tolist()))
    order = np.argsort(counts)[::-1]
    top = []
    cumulative = 0
    for movie_id in order:
        if counts[movie_id] == 0 or len(top) == 5:
            break
        share = int(counts[movie_id]) / total
        cumulative += share
        top.append(
            {
                "movie_id": int(movie_id),
                "title": titles.get(int(movie_id), ""),
                "rows": int(counts[movie_id]),
                "share": share,
                "cumulative": cumulative,
            }
        )
    return {
        "rows": total,
        "anchor_share": int(counts[anchor_movie_id]) / total,
        "head_share": head_rows / total,
        "top5": top,
    }


def _volumes(output_dir: Path) -> dict:
    by_dataset = {}
    total = 0
    for path in output_dir.rglob("*.parquet"):
        size = path.stat().st_size
        total += size
        dataset = path.relative_to(output_dir).parts[0]
        slot = by_dataset.setdefault(dataset, {"files": 0, "bytes": 0})
        slot["files"] += 1
        slot["bytes"] += size
    return {"total_bytes": total, "by_dataset": by_dataset}


def _cardinality_line(payload: dict) -> str:
    movie = payload["cardinality"]["movie_id"]
    if not movie["rows"]:
        return "- `movie_id`: không có dòng."
    ratio = movie["unique"] / movie["rows"]
    return (
        f"- `movie_id`: {movie['unique']} giá trị có mặt / {movie['catalog']} phim trong catalog / "
        f"{movie['rows']} dòng ratings+watch. Tỷ lệ unique/dòng = {ratio:.8f}."
    )


def _markdown(payload: dict) -> str:
    volumes = payload["volumes"]
    evolution = payload["schema_evolution"]
    combined = payload["combined"]
    lines = [
        "# 23 — Dataset offline scale-factor 100",
        "",
        "Sinh bằng `python -m generator.offline_generator --scale-factor 100 --allow-large-scale`.",
        "Cùng Zipf, Pareto, Beta, duplicate 2.5% và schema evolution từ partition 45.",
        "Đường chunk chỉ đổi cách ghi khi số dòng watch vượt bộ nhớ một mảng.",
        "",
        "## Dung lượng",
        "",
        "| Bảng | File | Bytes |",
        "|---|---:|---:|",
    ]
    for name, slot in sorted(volumes.items()):
        lines.append(f"| `{name}` | {slot['files']} | {slot['bytes']} |")
    lines.append(f"| **Tổng** |  | **{payload['parquet_bytes']}** |")
    lines.extend(
        [
            "",
            "## Skew",
            "",
            f"- Cột skew: `movie_id` trên ratings và watch_history. Nhóm đầu `{payload['head_count']}` phim "
            f"(head fraction {payload['head_fraction']}) nhận {100 * combined['head_share']:.2f}% dòng, mục tiêu "
            f"{100 * payload['skew_ratio_target']:.0f}%.",
            f"- `movie_id={payload['anchor_movie_id']}` chiếm {100 * combined['anchor_share']:.2f}% "
            f"(sàn {100 * payload['anchor_share_floor']:.0f}%).",
            f"- `genre_name` Action+Comedy: {100 * payload['genre_action_comedy_share']:.2f}% "
            f"trên {payload['genre_rows']} dòng `movie_genres`, mục tiêu {100 * payload['genre_skew_target']:.0f}%.",
            f"- Super-user {100 * payload['super_user_fraction']:.0f}% user → ratings "
            f"{100 * payload['ratings']['super_user_share']:.2f}%, tags "
            f"{100 * payload['tags']['super_user_share']:.2f}%, watch "
            f"{100 * payload['watch_history']['super_user_share']:.2f}%. Mục tiêu {100 * payload['super_user_mass_target']:.0f}%.",
            "",
            "## High cardinality",
            "",
            _cardinality_line(payload),
            f"- `user_id` ratings: {payload['cardinality']['user_id_ratings']['unique']} / "
            f"{payload['cardinality']['user_id_ratings']['catalog']} user, "
            f"unique/dòng = {payload['cardinality']['user_id_ratings']['unique_over_rows']:.8f}.",
            f"- `user_id` watch: {payload['cardinality']['user_id_watch']['unique']} / "
            f"{payload['cardinality']['user_id_watch']['catalog']} user, "
            f"unique/dòng = {payload['cardinality']['user_id_watch']['unique_over_rows']:.8f}.",
            "",
            "## Schema evolution",
            "",
            f"- Mốc partition `{evolution['evolution_from_partition']}`.",
            f"- Schema cũ: {evolution['n_old_schema_files']} file, ví dụ `{evolution['old_example']}`, cột `{evolution['old_schema_columns']}`.",
            f"- Schema mới: {evolution['n_new_schema_files']} file, ví dụ `{evolution['new_example']}`, cột `{evolution['new_schema_columns']}`.",
            "",
            "## Duplicate",
            "",
            f"- Ratings: {100 * payload['ratings']['duplicate_rate']:.4f}% "
            f"({payload['ratings']['extra_rows']} / {payload['ratings']['rows']}), mục tiêu "
            f"{100 * payload['duplicate_rate_target']:.2f}%. Sang ngày kế tiếp: {payload['ratings']['next_partition_rows']}.",
            f"- Watch: {100 * payload['watch_history']['duplicate_rate']:.4f}% "
            f"({payload['watch_history']['extra_rows']} / {payload['watch_history']['rows']}). "
            f"Sang ngày kế tiếp: {payload['watch_history']['next_partition_rows']}.",
            f"- Ví dụ ratings: `{payload['ratings']['example']}`.",
            "",
            "## Top 5 movie_id trên ratings + watch",
            "",
            "| Hạng | movie_id | Title | Dòng | Share | Cộng dồn |",
            "|---:|---:|---|---:|---:|---:|",
        ]
    )
    for rank, row in enumerate(combined["top5"], start=1):
        lines.append(
            f"| {rank} | {row['movie_id']} | {row['title']} | {row['rows']} | "
            f"{100 * row['share']:.2f}% | {100 * row['cumulative']:.2f}% |"
        )
    lines.extend(
        [
            "",
            "## Rating",
            "",
            f"- Trung bình {payload['rating_value']['mean']:.3f} (kỳ vọng Beta(6, 2) khoảng 3.875).",
            f"- Trong [3.5, 4.5]: {100 * payload['rating_value']['share_3_5_to_4_5']:.2f}%.",
            f"- <= 2 sao: {100 * payload['rating_value']['share_at_most_2']:.2f}%.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"
