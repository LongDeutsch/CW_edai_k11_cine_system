"""Kiểm tra SCD Type 2 trên 5 user đổi gói, không cần Spark hay pytest.

Chạy::

    python tests/unit/test_scd2_logic.py
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storage.lakehouse.scd.apply_scd2 import (  # noqa: E402
    TierChange,
    apply_scd2,
    demo_changes,
    demo_snapshot,
    format_table,
)

CHANGE_TS = datetime(2026, 6, 1, tzinfo=timezone.utc)


def test_five_changes_keep_history_and_one_current_row() -> None:
    before = demo_snapshot()
    after = apply_scd2(before, demo_changes())
    assert len(before) == 5
    assert len(after) == 10
    assert sum(row.is_current for row in after) == 5

    expected = {101: "VIP", 102: "VIP", 103: "Free", 104: "Standard", 105: "Premium"}
    for user_id, tier in expected.items():
        versions = [row for row in after if row.user_id == user_id]
        assert len(versions) == 2
        closed = next(row for row in versions if not row.is_current)
        opened = next(row for row in versions if row.is_current)
        assert closed.valid_to_ts == CHANGE_TS
        assert closed.subscription_tier != tier
        assert opened.subscription_tier == tier
        assert opened.valid_from_ts == CHANGE_TS
        assert opened.valid_to_ts is None
        assert opened.user_sk != closed.user_sk
        assert opened.country == closed.country

    assert len({row.user_sk for row in after}) == 10


def test_same_tier_does_not_open_a_new_version() -> None:
    before = demo_snapshot()
    repeated = [TierChange(101, "Free", CHANGE_TS)]
    after = apply_scd2(before, repeated)
    assert after == before


def test_second_change_chains_a_third_version() -> None:
    before = demo_snapshot()
    later = CHANGE_TS + timedelta(days=30)
    after = apply_scd2(
        before,
        [TierChange(101, "VIP", CHANGE_TS), TierChange(101, "Premium", later)],
    )
    versions = sorted((row for row in after if row.user_id == 101), key=lambda row: row.valid_from_ts)
    assert [row.subscription_tier for row in versions] == ["Free", "VIP", "Premium"]
    assert versions[0].valid_to_ts == CHANGE_TS
    assert versions[1].valid_to_ts == later
    assert versions[2].is_current is True
    assert versions[2].valid_to_ts is None


def test_unknown_user_is_rejected() -> None:
    try:
        apply_scd2(demo_snapshot(), [TierChange(999, "VIP", CHANGE_TS)])
    except ValueError as exc:
        assert "999" in str(exc)
    else:
        raise AssertionError("user lạ phải bị từ chối")


def test_input_list_is_not_mutated() -> None:
    before = demo_snapshot()
    snapshot = list(before)
    apply_scd2(before, demo_changes())
    assert before == snapshot
    assert all(row.is_current for row in before)


def _print_demo() -> None:
    before = demo_snapshot()
    after = apply_scd2(before, demo_changes())
    print("TRƯỚC KHI ĐỔI GÓI")
    print(format_table(before))
    print()
    print("SAU KHI ĐỔI GÓI")
    print(format_table(after))
    print()


def main() -> None:
    tests = [
        test_five_changes_keep_history_and_one_current_row,
        test_same_tier_does_not_open_a_new_version,
        test_second_change_chains_a_third_version,
        test_unknown_user_is_rejected,
        test_input_list_is_not_mutated,
    ]
    _print_demo()
    for test in tests:
        test()
        print(f"PASS  {test.__name__}")
    print(f"\n{len(tests)} test SCD2 đã qua.")


if __name__ == "__main__":
    main()
