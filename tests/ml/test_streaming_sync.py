"""Sync cửa sổ sang Redis db=2. Không đụng key batch trên db=1."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import redis

from ml.feast.streaming_features import historical_windows
from ml.online.materialize import MaterializeFailed, materialize_export
from ml.online.streaming_sync import (
    STREAM_REDIS,
    StaleSnapshot,
    SyncFailed,
    assert_isolated_window_table,
    read_online,
    sync_logical,
)

REDIS = STREAM_REDIS
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _window(movie_id: int, start: datetime, beats: int, watched: int, users: int) -> dict:
    return {
        "movie_id": movie_id,
        "window_start": start,
        "window_end": start + timedelta(minutes=1),
        "heartbeat_count": beats,
        "total_watch_seconds": watched,
        "active_users": users,
    }


class StreamingSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        redis.Redis.from_url("redis://localhost:16379/2").flushdb()
        self.root = Path(tempfile.mkdtemp(prefix="cinestream-stream-feast-"))
        self.repo = self.root / "repo"
        self.state = self.root / "state"
        self.repo.mkdir()
        self.state.mkdir()
        redis.Redis.from_url("redis://localhost:16379/1").set("b3-batch-sentinel", "keep")

    def test_refuses_batch_database_and_production_table(self) -> None:
        with self.assertRaises(SyncFailed):
            sync_logical([], snapshot_id="1", committed_at=EPOCH, repo_dir=self.repo, state_dir=self.state, redis="localhost:16379,db=1")
        with self.assertRaises(MaterializeFailed):
            materialize_export(self.root / "missing", self.repo, self.state, redis="localhost:16379,db=2")
        with self.assertRaises(SyncFailed):
            assert_isolated_window_table("cinestream.silver.stg_playback_windows")

    def test_empty_duplicate_and_missing_do_not_commit(self) -> None:
        empty = sync_logical(
            [],
            snapshot_id="0",
            committed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            repo_dir=self.repo,
            state_dir=self.state,
        )
        self.assertEqual(empty["result"], "source_empty")
        self.assertFalse((self.state / "committed.json").exists())
        start = EPOCH
        duplicated = [_window(42, start, 1, 10, 1), _window(42, start, 2, 20, 2)]
        with self.assertRaises(SyncFailed):
            sync_logical(
                duplicated,
                snapshot_id="1",
                committed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                repo_dir=self.repo,
                state_dir=self.state,
            )
        with self.assertRaises(SyncFailed):
            sync_logical(None, snapshot_id="1", committed_at=datetime(2026, 1, 1, tzinfo=timezone.utc), repo_dir=self.repo, state_dir=self.state)
        self.assertFalse((self.state / "committed.json").exists())

    def test_late_correction_second_window_and_retry(self) -> None:
        start = EPOCH
        later = EPOCH + timedelta(minutes=1)
        first_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        opened = self._sync("snap-1", first_at, [_window(42, start, 4, 40, 4)])
        self.assertEqual(opened["result"], "materialized")
        again = self._sync("snap-1", first_at, [_window(42, start, 4, 40, 4)])
        self.assertEqual(again["result"], "unchanged")
        redis.Redis.from_url("redis://localhost:16379/2").flushdb()
        reloaded = self._sync("snap-1", first_at, [_window(42, start, 4, 40, 4)])
        self.assertEqual(reloaded["result"], "reloaded")
        self.assertEqual(int(reloaded["sequence"]), 1)

        corrected = self._sync("snap-2", first_at + timedelta(minutes=1), [_window(42, start, 5, 50, 5)])
        self.assertEqual(corrected["result"], "materialized")
        self._assert_online(42, 5, 50, 5)
        self._assert_history(start + timedelta(minutes=1, milliseconds=1), 4, 40, 4)
        self._assert_history(start + timedelta(minutes=1, milliseconds=2), 5, 50, 5)
        with self.assertRaises(StaleSnapshot):
            self._sync("snap-1", first_at, [_window(42, start, 4, 40, 4)])
        self._assert_online(42, 5, 50, 5)

        both = [
            _window(42, start, 5, 50, 5),
            _window(42, later, 1, 10, 1),
        ]
        second = self._sync("snap-3", first_at + timedelta(minutes=2), both)
        self.assertEqual(second["result"], "materialized")
        self._assert_online(42, 1, 10, 1)
        self._assert_online(99, None, None, None)
        self._assert_history(later + timedelta(minutes=1, milliseconds=3), 1, 10, 1)

        committed_before = (self.state / "committed.json").read_bytes()
        with self.assertRaises(SyncFailed):
            self._sync(
                "snap-4",
                first_at + timedelta(minutes=3),
                [_window(42, start, 5, 50, 5), _window(42, later, 2, 20, 2)],
                after_write=lambda: (_ for _ in ()).throw(RuntimeError("worker dừng")),
            )
        self.assertEqual((self.state / "committed.json").read_bytes(), committed_before)
        self.assertTrue((self.state / "pending.json").exists())
        retried = self._sync(
            "snap-4",
            first_at + timedelta(minutes=3),
            [_window(42, start, 5, 50, 5), _window(42, later, 2, 20, 2)],
        )
        self.assertEqual(retried["result"], "materialized")
        self.assertFalse((self.state / "pending.json").exists())
        self._assert_online(42, 2, 20, 2)
        self.assertEqual(redis.Redis.from_url("redis://localhost:16379/1").get("b3-batch-sentinel"), b"keep")

    def _sync(self, snapshot_id: str, committed_at: datetime, rows: list[dict], after_write=None) -> dict:
        return sync_logical(
            rows,
            snapshot_id=snapshot_id,
            committed_at=committed_at,
            repo_dir=self.repo,
            state_dir=self.state,
            after_write=after_write,
        )

    def _assert_online(self, movie_id: int, beats, watched, users) -> None:
        from feast import FeatureStore

        found = read_online(FeatureStore(repo_path=str(self.repo)), [movie_id])[0]
        self.assertEqual(found["movie_id"], movie_id)
        self.assertEqual(found["heartbeat_count"], beats)
        self.assertEqual(found["total_watch_seconds"], watched)
        self.assertEqual(found["active_users"], users)

    def _assert_history(self, event_time: datetime, beats: int, watched: int, users: int) -> None:
        from feast import FeatureStore

        frame = historical_windows(
            FeatureStore(repo_path=str(self.repo)),
            pd.DataFrame({"movie_id": [42], "event_timestamp": [pd.Timestamp(event_time)]}),
        )
        row = frame.iloc[0]
        self.assertEqual(int(row["heartbeat_count"]), beats)
        self.assertEqual(int(row["total_watch_seconds"]), watched)
        self.assertEqual(int(row["active_users"]), users)


if __name__ == "__main__":
    unittest.main()
