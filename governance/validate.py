"""Kiểm cạnh lineage và đối chiếu inventory với source."""

from __future__ import annotations

from pathlib import Path

from governance.extract import REPO
from governance.inventory import Dataset, datasets, edges, production_table_names, schema_of

_STATUSES = {"verified", "declared_only", "writer_gated", "integration"}
_MODES = {
    "append",
    "overwrite",
    "upsert",
    "replace_slice",
    "snapshot_replacement",
    "read",
    "scheduled_manual",
    "streaming",
    "isolated",
    "declared_only",
}


def problems() -> list[str]:
    found = []
    rows = datasets()
    links = edges()
    by_id = {item.id: item for item in rows}
    if len(by_id) != len(rows):
        found.append("id dataset bị trùng")
    found.extend(_dataset_problems(rows))
    found.extend(_edge_problems(by_id, links))
    found.extend(_coverage_problems(rows))
    from governance.inventory import assert_catalog_matches_sources

    found.extend(assert_catalog_matches_sources())
    return found


def _dataset_problems(rows: tuple[Dataset, ...]) -> list[str]:
    found = []
    for item in rows:
        if item.lineage_status not in _STATUSES:
            found.append(f"{item.id} status lạ {item.lineage_status}")
        if item.update_mode not in _MODES:
            found.append(f"{item.id} update mode lạ {item.update_mode}")
        if not item.owner:
            found.append(f"{item.id} thiếu owner")
        if item.lineage_status == "declared_only" and item.update_mode != "declared_only":
            found.append(f"{item.id} declared_only nhưng mode {item.update_mode}")
        for path, token in item.evidence:
            file_path = REPO / path
            if not file_path.is_file():
                found.append(f"{item.id} thiếu file {path}")
                continue
            if token not in file_path.read_text():
                found.append(f"{item.id} không thấy {token!r} trong {path}")
        if item.schema_ref is not None:
            try:
                columns = schema_of(item)
            except KeyError as exc:
                found.append(f"{item.id} không parse được schema {exc}")
                continue
            if not columns:
                found.append(f"{item.id} schema rỗng")
            names = [name for name, _ in columns]
            missing = [key for key in item.business_keys if key not in names and item.schema_ref is not None]
            if missing and item.id.startswith(("iceberg.", "clickhouse.")):
                found.append(f"{item.id} khóa {missing} không có trong schema")
    return found


def _edge_problems(by_id: dict[str, Dataset], links) -> list[str]:
    found = []
    incoming = {item_id: 0 for item_id in by_id}
    seen = set()
    for edge in links:
        key = (edge.upstream, edge.downstream)
        if key in seen:
            found.append(f"cạnh trùng {key}")
        seen.add(key)
        if edge.upstream == edge.downstream:
            found.append(f"cạnh tự thân {edge.upstream}")
        for item_id in (edge.upstream, edge.downstream):
            if item_id not in by_id:
                found.append(f"cạnh trỏ tới id không có {item_id}")
            elif by_id[item_id].lineage_status == "integration":
                found.append(f"cạnh đụng tài nguyên integration {item_id}")
            elif by_id[item_id].lineage_status == "declared_only":
                found.append(f"cạnh đụng bảng chỉ có DDL {item_id}")
        path, token = edge.evidence
        file_path = Path(REPO / path)
        if not file_path.is_file() or token not in file_path.read_text():
            found.append(f"cạnh {edge.upstream}->{edge.downstream} thiếu bằng chứng {path}:{token}")
        if edge.upstream in incoming and edge.downstream in incoming:
            incoming[edge.downstream] += 1
    for item_id, item in by_id.items():
        if item.lineage_status == "declared_only":
            continue
        if item.lineage_status == "integration":
            continue
        if not item.root and incoming[item_id] == 0:
            found.append(f"mồ côi, không có upstream: {item_id}")
    if ("iceberg.gold.fact_watch_sessions", "iceberg.gold.obt_movie_engagement") in seen:
        found.append("obt không được đọc từ bảng fact_watch_sessions")
    if ("iceberg.gold.dim_users", "clickhouse.movie_ratings_stage") in seen:
        found.append("sync ratings không đọc dim_users")
    if ("iceberg.silver.stg_playback_heartbeats", "clickhouse.movie_windows_stage") in seen:
        found.append("poll trending không đọc bảng heartbeat")
    return found


def _coverage_problems(rows: tuple[Dataset, ...]) -> list[str]:
    found = []
    names = production_table_names()
    inventory = {
        "bronze": set(),
        "silver": set(),
        "gold": set(),
        "clickhouse": set(),
    }
    for item in rows:
        if item.schema_ref is None:
            continue
        kind, _source, name = item.schema_ref
        if kind == "spark":
            zone = item.id.split(".")[1]
            inventory[zone].add(name)
        elif kind == "flink":
            inventory["silver"].add(name)
        elif kind == "audit":
            inventory["bronze"].add(name)
        elif kind == "clickhouse":
            inventory["clickhouse"].add(name)
    for zone, expected in names.items():
        missing = expected - inventory[zone]
        extra = inventory[zone] - expected
        if missing:
            found.append(f"DDL {zone} chưa vào inventory: {sorted(missing)}")
        if extra:
            found.append(f"inventory {zone} thừa so với DDL: {sorted(extra)}")
    return found
