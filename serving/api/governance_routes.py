"""Route governance chỉ đọc. Không có POST, PUT hay DELETE."""

from __future__ import annotations

import time

from fastapi import APIRouter, Depends, HTTPException, Query

from governance.read_live import read_live
from governance.serve import build_view, dataset_row, filter_datasets, impact_of, lineage_of

router = APIRouter()
_cache = {"at": 0.0, "view": None}


def clear_cache() -> None:
    _cache["at"] = 0.0
    _cache["view"] = None


def cached_live_view(loader=None, now=None, ttl: float = 15):
    loader = loader or read_live
    clock = time.time() if now is None else now
    if _cache["view"] is not None and clock - _cache["at"] < ttl:
        return _cache["view"]
    view = build_view(loader())
    _cache["at"] = clock
    _cache["view"] = view
    return view


def get_view(refresh: int = Query(0)) -> dict:
    if refresh:
        clear_cache()
    return cached_live_view()


@router.get("/governance/snapshot")
def snapshot(view: dict = Depends(get_view)) -> dict:
    return view


@router.get("/governance/datasets")
def list_datasets(
    q: str = "",
    environment: str = "",
    status: str = "",
    zone: str = "",
    view: dict = Depends(get_view),
) -> dict:
    rows = filter_datasets(view["datasets"], q=q, environment=environment, status=status, zone=zone)
    return {
        "datasets": rows,
        "count": len(rows),
        "catalog_status": view["sources"]["iceberg"]["status"],
        "historical_proof_used_as_current_health": False,
    }


@router.get("/governance/datasets/{dataset_id:path}")
def dataset_detail(dataset_id: str, view: dict = Depends(get_view)) -> dict:
    try:
        return dataset_row(view, dataset_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="không có trong inventory") from None


@router.get("/governance/lineage")
def lineage(view: dict = Depends(get_view)) -> dict:
    return {
        "edges": view["edges"],
        "lineage_source": "code",
        "historical_proof_used_as_current_health": False,
    }


@router.get("/governance/lineage/{dataset_id:path}")
def lineage_one(dataset_id: str, view: dict = Depends(get_view)) -> dict:
    try:
        return lineage_of(view, dataset_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="không có trong inventory") from None


@router.get("/governance/drift")
def drift(view: dict = Depends(get_view)) -> dict:
    return view["drift"]


@router.get("/governance/health")
def health(view: dict = Depends(get_view)) -> dict:
    return view["health"]


@router.get("/governance/impact/{dataset_id:path}")
def impact(dataset_id: str, view: dict = Depends(get_view)) -> dict:
    try:
        return impact_of(view, dataset_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="không có trong inventory") from None
