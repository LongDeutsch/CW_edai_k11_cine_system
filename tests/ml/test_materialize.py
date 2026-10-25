"""Materialize Redis trên Parquet tạm. Cần Redis localhost:16379 db 1."""

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import redis

from ml.offline.feast_export import MOVIE_TABLE, USER_TABLE, export_frames
from ml.online.materialize import MaterializeFailed, StaleExport, materialize_export, read_online

REDIS = "localhost:16379,db=1"


def _ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC")


def _frames(ratings: int, average: float, genre, hours: float, count: int, completion):
    users = pd.DataFrame(
        [
            {
                "user_id": 7,
                "event_timestamp": _ts("2026-01-20 00:00:00"),
                "created_timestamp": _ts("2026-01-21 03:00:00"),
                "user_total_ratings_90d": ratings,
                "user_avg_rating_90d": average,
                "user_favorite_genre": genre,
                "user_total_watch_hours_30d": hours,
            },
            {
                "user_id": 8,
                "event_timestamp": _ts("2026-01-20 00:00:00"),
                "created_timestamp": _ts("2026-01-21 03:00:00"),
                "user_total_ratings_90d": 0,
                "user_avg_rating_90d": None,
                "user_favorite_genre": None,
                "user_total_watch_hours_30d": 0.0,
            },
        ]
    )
    movies = pd.DataFrame(
        [
            {
                "movie_id": 11,
                "event_timestamp": _ts("2026-01-20 00:00:00"),
                "created_timestamp": _ts("2026-01-21 03:00:00"),
                "movie_rating_count_30d": count,
                "movie_avg_rating_30d": average,
                "movie_completion_rate_30d": completion,
                "movie_trending_score_7d": hours,
            },
            {
                "movie_id": 12,
                "event_timestamp": _ts("2026-01-20 00:00:00"),
                "created_timestamp": _ts("2026-01-21 03:00:00"),
                "movie_rating_count_30d": 1,
                "movie_avg_rating_30d": 3.0,
                "movie_completion_rate_30d": None,
                "movie_trending_score_7d": 0.0,
            },
        ]
    )
    return users, movies


class MaterializeTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="cinestream-materialize-"))
        self.addCleanup(shutil.rmtree, self.root, True)
        self.client = redis.Redis.from_url("redis://localhost:16379/1")
        self.client.ping()
        self.client.flushdb()
        self.repo = self.root / "repo"
        self.state = self.root / "state"
        self.first = self._export(1, 1, 4.0, None, 600 / 3600, 1, 0.5)
        self.second = self._export(2, 2, 4.5, "Action", 1.0, 2, 0.25)

    def test_materialize_reload_retry_and_stale(self):
        first = materialize_export(self.first, self.repo, self.state, redis=REDIS)
        self.assertEqual(first["result"], "materialized")
        self.assertEqual(self._ratings(7, 11), 1)
        self.assertIsNone(self._value(8, 11, "user_avg_rating_90d"))
        self.assertIsNone(self._value(7, 12, "movie_completion_rate_30d"))
        again = materialize_export(self.first, self.repo, self.state, redis=REDIS)
        self.assertEqual(again["result"], "unchanged")
        self.client.flushdb()
        reloaded = materialize_export(self.first, self.repo, self.state, redis=REDIS)
        self.assertEqual(reloaded["result"], "reloaded")
        self.assertEqual(self._ratings(7, 11), 1)
        second = materialize_export(self.second, self.repo, self.state, redis=REDIS)
        self.assertEqual(second["result"], "materialized")
        self.assertEqual(self._ratings(7, 11), 2)
        self.assertEqual(self._value(7, 11, "user_favorite_genre"), "Action")
        with self.assertRaises(StaleExport):
            materialize_export(self.first, self.repo, self.state, redis=REDIS)
        self.assertEqual(self._ratings(7, 11), 2)
        committed = (self.state / "committed.json").read_text()

        def boom():
            raise RuntimeError("worker dừng")

        with self.assertRaises(MaterializeFailed):
            materialize_export(self._export(3, 9, 5.0, "Drama", 2.0, 6, 0.9), self.repo, self.state, redis=REDIS, after_write=boom)
        self.assertEqual((self.state / "committed.json").read_text(), committed)
        self.assertTrue((self.state / "pending.json").exists())
        retried = materialize_export(self.root / "export-3", self.repo, self.state, redis=REDIS)
        self.assertEqual(retried["result"], "materialized")
        self.assertEqual(retried["sequence"], 3)
        self.assertFalse((self.state / "pending.json").exists())
        self.assertEqual(self._ratings(7, 11), 9)

    def test_missing_source_does_not_advance(self):
        materialize_export(self.first, self.repo, self.state, redis=REDIS)
        before = (self.state / "committed.json").read_text()
        with self.assertRaises(MaterializeFailed):
            materialize_export(self.root / "absent", self.repo, self.state, redis=REDIS)
        self.assertEqual((self.state / "committed.json").read_text(), before)
        self.assertEqual(self._ratings(7, 11), 1)

    def _export(self, sequence, ratings, average, genre, hours, count, completion) -> Path:
        destination = self.root / f"export-{sequence}"
        users, movies = _frames(ratings, average, genre, hours, count, completion)
        export_frames(
            destination,
            {USER_TABLE: users, MOVIE_TABLE: movies},
            {
                "batch_id": f"batch-{sequence}",
                "sequence": sequence,
                "snapshot_ids": {USER_TABLE: 1000 + sequence, MOVIE_TABLE: 2000 + sequence},
            },
        )
        return destination

    def _ratings(self, user_id: int, movie_id: int) -> int:
        return int(self._value(user_id, movie_id, "user_total_ratings_90d"))

    def _value(self, user_id: int, movie_id: int, column: str):
        from feast import FeatureStore

        rows = read_online(FeatureStore(repo_path=str(self.repo)), [{"user_id": user_id, "movie_id": movie_id}])
        self.assertEqual(len(rows), 1, rows)
        return rows[0][column]


if __name__ == "__main__":
    unittest.main()
