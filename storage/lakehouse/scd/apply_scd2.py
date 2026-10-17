"""Đóng phiên bản gói cước cũ và mở phiên bản mới trên ``dim_users``.

SCD Type 2 không sửa đè dòng hiện tại. Khi ``subscription_tier`` đổi,
dòng đang mở được đóng (``is_current = False``, ``valid_to_ts`` = mốc đổi)
và một dòng mới được thêm với ``user_sk`` mới.

Khoảng hiệu lực là nửa mở: ``valid_from_ts <= t < valid_to_ts``.
Dòng hiện tại có ``valid_to_ts = None``.

Chạy thử 5 user::

    python -m storage.lakehouse.scd.apply_scd2
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import datetime, timezone

OPEN_ENDED = None


@dataclass(frozen=True)
class DimUser:
    """Một phiên bản của user trong ``dim_users``."""

    user_sk: int
    user_id: int
    subscription_tier: str
    valid_from_ts: datetime
    valid_to_ts: datetime | None
    is_current: bool
    country: str


@dataclass(frozen=True)
class TierChange:
    """Một lần đổi gói. ``change_ts`` là lúc gói mới bắt đầu có hiệu lực."""

    user_id: int
    subscription_tier: str
    change_ts: datetime


def apply_scd2(current_rows: list[DimUser], changes: list[TierChange]) -> list[DimUser]:
    """Trả về bảng chiều mới. Không sửa list đầu vào.

    Nhiều thay đổi của cùng một user được áp theo thứ tự thời gian.
    Nếu gói mới trùng gói đang mở, dòng đó được giữ nguyên.
    """
    rows = list(current_rows)
    next_sk = max((row.user_sk for row in rows), default=0) + 1
    ordered = sorted(changes, key=lambda change: (change.change_ts, change.user_id))
    for change in ordered:
        active = _current_row(rows, change.user_id)
        if active.subscription_tier == change.subscription_tier:
            continue
        if change.change_ts < active.valid_from_ts:
            raise ValueError(
                f"user_id={change.user_id}: change_ts {change.change_ts} sớm hơn valid_from_ts"
            )
        closed = replace(active, is_current=False, valid_to_ts=change.change_ts)
        opened = DimUser(
            user_sk=next_sk,
            user_id=active.user_id,
            subscription_tier=change.subscription_tier,
            valid_from_ts=change.change_ts,
            valid_to_ts=OPEN_ENDED,
            is_current=True,
            country=active.country,
        )
        next_sk += 1
        rows = [closed if row.user_sk == active.user_sk else row for row in rows]
        rows.append(opened)
    return rows


def demo_snapshot() -> list[DimUser]:
    """Năm user, mỗi người một phiên bản đang mở từ 2026-01-01."""
    opened = datetime(2026, 1, 1, tzinfo=timezone.utc)
    seed = (
        (1, 101, "Free", "VN"),
        (2, 102, "Standard", "US"),
        (3, 103, "VIP", "KR"),
        (4, 104, "Free", "JP"),
        (5, 105, "Standard", "FR"),
    )
    return [
        DimUser(
            user_sk=user_sk,
            user_id=user_id,
            subscription_tier=tier,
            valid_from_ts=opened,
            valid_to_ts=OPEN_ENDED,
            is_current=True,
            country=country,
        )
        for user_sk, user_id, tier, country in seed
    ]


def demo_changes() -> list[TierChange]:
    """Đúng 5 dòng đổi gói, cùng mốc 2026-06-01."""
    changed = datetime(2026, 6, 1, tzinfo=timezone.utc)
    return [
        TierChange(101, "VIP", changed),
        TierChange(102, "VIP", changed),
        TierChange(103, "Free", changed),
        TierChange(104, "Standard", changed),
        TierChange(105, "Premium", changed),
    ]


def format_table(rows: list[DimUser]) -> str:
    """Bảng chữ để đọc trên terminal."""
    ordered = sorted(rows, key=lambda row: (row.user_id, row.valid_from_ts, row.user_sk))
    header = f"{'user_sk':>7}  {'user_id':>7}  {'country':<6}  {'tier':<10}  {'valid_from':<20}  {'valid_to':<20}  current"
    lines = [header, "-" * len(header)]
    for row in ordered:
        lines.append(
            f"{row.user_sk:>7}  {row.user_id:>7}  {row.country:<6}  {row.subscription_tier:<10}  "
            f"{_fmt(row.valid_from_ts):<20}  {_fmt(row.valid_to_ts):<20}  {str(row.is_current).lower()}"
        )
    return "\n".join(lines)


def _current_row(rows: list[DimUser], user_id: int) -> DimUser:
    matches = [row for row in rows if row.user_id == user_id and row.is_current]
    if len(matches) != 1:
        raise ValueError(f"user_id={user_id} cần đúng 1 dòng is_current, thấy {len(matches)}")
    return matches[0]


def _fmt(value: datetime | None) -> str:
    if value is None:
        return "NULL"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    before = demo_snapshot()
    after = apply_scd2(before, demo_changes())
    print("TRƯỚC KHI ĐỔI GÓI")
    print(format_table(before))
    print()
    print("SAU KHI ĐỔI GÓI (2026-06-01)")
    print(format_table(after))
    print()
    print(f"dòng trước={len(before)}  dòng sau={len(after)}  dòng hiện tại={sum(row.is_current for row in after)}")


if __name__ == "__main__":
    argparse.ArgumentParser(description="In ví dụ SCD Type 2 với 5 user đổi gói.").parse_args()
    main()
