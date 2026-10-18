"""Fingerprint batch nguồn, không cần Airflow hay Spark."""

from __future__ import annotations

from orchestration.batch_identity import assess_listing


def test_missing_folder_is_not_ready() -> None:
    found = assess_listing(
        [{"key": "it/users/a.parquet", "size": 10, "etag": "aaa"}],
        "it",
    )
    assert found["ready"] is False
    assert "movies" in found["reason"]
    assert found["batch_id"] == ""


def test_same_files_keep_the_same_batch_id() -> None:
    objects = _complete()
    first = assess_listing(objects, "it")
    second = assess_listing(list(reversed(objects)), "it")
    assert first["ready"] is True
    assert first["batch_id"] == second["batch_id"]
    assert first["batch_id"].startswith("batch-")
    assert first["files"] == 4


def test_old_partition_does_not_invent_a_new_id() -> None:
    """Hai ngày event nằm chung một listing. Ngày lịch không tạo batch mới."""
    base = _complete()
    with_old_day = base + [
        {"key": "it/ratings/dt=2026-01-01/old.parquet", "size": 4, "etag": "old"},
    ]
    once = assess_listing(with_old_day, "it")
    again = assess_listing(with_old_day, "it")
    assert once["batch_id"] == again["batch_id"]
    assert once["datasets"]["ratings"] == 2
    changed = with_old_day + [{"key": "it/ratings/dt=2026-02-01/new.parquet", "size": 5, "etag": "new"}]
    assert assess_listing(changed, "it")["batch_id"] != once["batch_id"]


def test_unrelated_prefix_is_ignored() -> None:
    objects = _complete() + [{"key": "it/tags/a.parquet", "size": 1, "etag": "t"}]
    found = assess_listing(objects, "it")
    assert found["files"] == 4


def _complete() -> list[dict]:
    return [
        {"key": "it/users/a.parquet", "size": 10, "etag": '"u"'},
        {"key": "it/movies/a.parquet", "size": 11, "etag": "m"},
        {"key": "it/ratings/dt=2026-01-15/a.parquet", "size": 12, "etag": "r"},
        {"key": "it/watch_history/dt=2026-01-15/a.parquet", "size": 13, "etag": "w"},
    ]


def main() -> None:
    tests = [
        test_missing_folder_is_not_ready,
        test_same_files_keep_the_same_batch_id,
        test_old_partition_does_not_invent_a_new_id,
        test_unrelated_prefix_is_ignored,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"PASS {len(tests)} batch identity")


if __name__ == "__main__":
    main()
