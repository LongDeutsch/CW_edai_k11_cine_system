"""Chặn DAG cũ và ba DAG mới cùng ghi namespace production.

Namespace ``it_*`` vẫn chạy được cho cả hai phía. Namespace ``bronze``,
``silver``, ``gold`` chỉ thuộc entry đang được chọn. Mặc định là DAG
``cinestream_daily_batch_lakehouse``. Đặt ``CINESTREAM_BATCH_ENTRY=split``
khi muốn ba DAG DP ghi production và DAG cũ phải dừng.
"""

from __future__ import annotations

import os

PROTECTED = frozenset({"bronze", "silver", "gold"})
LEGACY_DAG = "cinestream_daily_batch_lakehouse"
SPLIT_DAGS = frozenset(
    {
        "cinestream_dp1_raw_to_bronze",
        "cinestream_dp2_bronze_to_gold",
        "cinestream_dp3_offline_features",
    }
)


def writer_family(dag_id: str) -> str:
    if dag_id == LEGACY_DAG:
        return "legacy"
    if dag_id in SPLIT_DAGS:
        return "split"
    raise RuntimeError(f"DAG {dag_id} không thuộc entry batch nào")


def batch_entry() -> str:
    value = os.environ.get("CINESTREAM_BATCH_ENTRY", "legacy").strip().lower() or "legacy"
    if value not in {"legacy", "split"}:
        raise RuntimeError(f"CINESTREAM_BATCH_ENTRY không hợp lệ: {value}")
    return value


def assert_exclusive_writer(dag_id: str, namespaces: tuple[str, ...]) -> None:
    """Không làm gì với namespace test. Namespace production phải đúng entry."""
    if not any(name in PROTECTED for name in namespaces):
        return
    entry = batch_entry()
    family = writer_family(dag_id)
    if family != entry:
        raise RuntimeError(
            f"namespace production đang dành cho entry {entry}; "
            f"{dag_id} không được ghi cùng bảng"
        )
