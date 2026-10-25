"""Feast 0.67 offline trên Parquet tạm. Không Spark, không catalog production."""

from __future__ import annotations

import inspect
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd
from feast import FeatureStore
from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import IcebergSource

from ml.feast.features import ALL_FEATURES, historical_features
from ml.offline.feast_export import MOVIE_TABLE, USER_TABLE, assert_isolated, export_frames

REPO = Path(__file__).resolve().parents[2] / "ml" / "feast"
os.environ.setdefault("FEAST_USAGE", "False")


def _ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC")


def _users() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _user(7, "2026-01-20 00:00:00", "2026-01-21 03:00:00", 1, 4.0, None, 1 / 6),
            _user(7, "2026-01-20 00:00:00", "2026-01-21 05:00:00", 3, 4.5, "Action", 0.5),
            _user(7, "2026-02-01 00:00:00", "2026-02-02 03:00:00", 9, 5.0, "Drama", 2.0),
            _user(8, "2026-01-20 00:00:00", "2026-01-21 03:00:00", 2, None, None, 0.0),
        ]
    )


def _movies() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _movie(11, "2026-01-20 00:00:00", "2026-01-21 03:00:00", 1, 4.0, 0.5, 1 / 6),
            _movie(11, "2026-01-20 00:00:00", "2026-01-21 05:00:00", 4, 4.2, 0.4, 0.2),
            _movie(11, "2026-02-01 00:00:00", "2026-02-02 03:00:00", 6, 4.8, 0.9, 1.0),
            _movie(12, "2026-01-20 00:00:00", "2026-01-21 03:00:00", 2, 3.0, None, 0.0),
        ]
    )


def _user(user_id, event, created, ratings, average, genre, hours):
    return {
        "user_id": user_id,
        "event_timestamp": _ts(event),
        "created_timestamp": _ts(created),
        "user_total_ratings_90d": ratings,
        "user_avg_rating_90d": average,
        "user_favorite_genre": genre,
        "user_total_watch_hours_30d": hours,
    }


def _movie(movie_id, event, created, count, average, completion, trending):
    return {
        "movie_id": movie_id,
        "event_timestamp": _ts(event),
        "created_timestamp": _ts(created),
        "movie_rating_count_30d": count,
        "movie_avg_rating_30d": average,
        "movie_completion_rate_30d": completion,
        "movie_trending_score_7d": trending,
    }


def _entities(*rows) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"user_id": user_id, "movie_id": movie_id, "event_timestamp": _ts(event)}
            for user_id, movie_id, event in rows
        ]
    )


class FeastOfflineTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="cinestream-feast-"))
        self.addCleanup(shutil.rmtree, self.root, True)
        self._install(_users(), _movies())
        applied = self._apply()
        if applied.returncode != 0:
            self.fail(applied.stderr + applied.stdout)

    def test_iceberg_source_has_no_snapshot_pin(self):
        names = set(inspect.signature(IcebergSource.__init__).parameters)
        self.assertNotIn("snapshot_id", names)
        with self.assertRaises(RuntimeError):
            assert_isolated("gold", "file:///tmp/warehouse")
        with self.assertRaises(RuntimeError):
            assert_isolated("it_b1", "s3a://lakehouse")

    def test_apply_is_idempotent(self):
        first = self._apply()
        second = self._apply()
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        store = FeatureStore(repo_path=str(self.root))
        self.assertEqual(sorted(view.name for view in store.list_feature_views()), ["movie_engagement_stats", "user_movie_stats"])
        self.assertTrue(all(view.online is True for view in store.list_feature_views()))

    def test_point_in_time_versions_isolation_and_nulls(self):
        found = historical_features(
            FeatureStore(repo_path=str(self.root)),
            _entities(
                (7, 11, "2026-01-21 04:00:00"),
                (7, 11, "2026-01-21 06:00:00"),
                (7, 11, "2026-02-02 00:00:00"),
                (7, 11, "2026-02-02 04:00:00"),
                (7, 11, "2026-01-19 00:00:00"),
                (8, 12, "2026-01-21 06:00:00"),
                (8, 11, "2026-01-21 06:00:00"),
                (9, 99, "2026-02-02 00:00:00"),
            ),
        )
        early = self._row(found, 7, 11, "2026-01-21 04:00:00")
        late = self._row(found, 7, 11, "2026-01-21 06:00:00")
        not_yet_written = self._row(found, 7, 11, "2026-02-02 00:00:00")
        future = self._row(found, 7, 11, "2026-02-02 04:00:00")
        before = self._row(found, 7, 11, "2026-01-19 00:00:00")
        other = self._row(found, 8, 12, "2026-01-21 06:00:00")
        mixed = self._row(found, 8, 11, "2026-01-21 06:00:00")
        missing = self._row(found, 9, 99, "2026-02-02 00:00:00")

        self.assertEqual(int(early["user_total_ratings_90d"]), 1)
        self.assertEqual(int(early["movie_rating_count_30d"]), 1)
        self.assertEqual(int(late["user_total_ratings_90d"]), 3)
        self.assertEqual(int(late["movie_rating_count_30d"]), 4)
        self.assertEqual(int(not_yet_written["user_total_ratings_90d"]), 3)
        self.assertEqual(int(not_yet_written["movie_rating_count_30d"]), 4)
        self.assertEqual(int(future["user_total_ratings_90d"]), 9)
        self.assertEqual(int(future["movie_rating_count_30d"]), 6)
        self.assertTrue(pd.isna(before["user_total_ratings_90d"]))
        self.assertTrue(pd.isna(before["movie_rating_count_30d"]))
        self.assertEqual(int(other["user_total_ratings_90d"]), 2)
        self.assertTrue(pd.isna(other["user_avg_rating_90d"]))
        self.assertTrue(pd.isna(other["movie_completion_rate_30d"]))
        self.assertEqual(int(mixed["user_total_ratings_90d"]), 2)
        self.assertEqual(int(mixed["movie_rating_count_30d"]), 4)
        self.assertTrue(pd.isna(missing["user_total_ratings_90d"]))
        self.assertTrue(pd.isna(missing["movie_avg_rating_30d"]))

    def test_default_created_timestamp_filter_is_off(self):
        store = FeatureStore(repo_path=str(self.root))
        leaked = store.get_historical_features(
            entity_df=_entities((7, 11, "2026-01-21 04:00:00")),
            features=ALL_FEATURES,
        ).to_df()
        self.assertEqual(int(leaked.iloc[0]["user_total_ratings_90d"]), 3)

    def test_schema_mismatch_is_rejected(self):
        broken = _users().drop(columns=["user_avg_rating_90d"])
        with self.assertRaises(ValueError):
            export_frames(self.root / "broken", {USER_TABLE: broken, MOVIE_TABLE: _movies()}, {})
        broken.to_parquet(self.user_path, index=False)
        with self.assertRaises(Exception) as caught:
            historical_features(FeatureStore(repo_path=str(self.root)), _entities((7, 11, "2026-01-21 06:00:00")))
        self.assertIn("user_avg_rating_90d", str(caught.exception))

    def test_export_overwrite_replaces_rows(self):
        destination = self.root / "export"
        meta = {"batch_id": "batch-b1-unit", "snapshot_ids": {USER_TABLE: 1992904055192348575}}
        first = export_frames(destination, {USER_TABLE: _users().iloc[:1], MOVIE_TABLE: _movies().iloc[:1]}, meta)
        second_users = _users()
        second = export_frames(destination, {USER_TABLE: second_users, MOVIE_TABLE: _movies()}, meta)
        self.assertEqual(first["rows"][USER_TABLE], 1)
        self.assertEqual(second["rows"][USER_TABLE], len(second_users))
        self.assertEqual(second["snapshot_ids"][USER_TABLE], "1992904055192348575")
        reread = pd.read_parquet(destination / f"{USER_TABLE}.parquet")
        self.assertEqual(len(reread), len(second_users))
        self._install(second_users, _movies())
        self._apply()
        found = historical_features(
            FeatureStore(repo_path=str(self.root)),
            _entities((8, 12, "2026-01-21 06:00:00")),
        )
        self.assertEqual(int(self._row(found, 8, 12, "2026-01-21 06:00:00")["user_total_ratings_90d"]), 2)

    def _install(self, users: pd.DataFrame, movies: pd.DataFrame) -> None:
        data = self.root / "data"
        data.mkdir(exist_ok=True)
        self.user_path = data / f"{USER_TABLE}.parquet"
        self.movie_path = data / f"{MOVIE_TABLE}.parquet"
        users.to_parquet(self.user_path, index=False)
        movies.to_parquet(self.movie_path, index=False)
        shutil.copy(REPO / "features.py", self.root / "features.py")
        (self.root / "feature_store.yaml").write_text(
            "\n".join(
                [
                    "project: cinestream_offline",
                    "provider: local",
                    "registry: data/registry.db",
                    "online_store:",
                    "  type: sqlite",
                    "  path: data/online_store.db",
                    "offline_store:",
                    "  type: duckdb",
                    "",
                ]
            )
        )

    def _apply(self) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["feast", "apply"],
            cwd=self.root,
            check=False,
            capture_output=True,
            text=True,
            env=self._env(),
        )

    def _env(self) -> dict:
        return {
            **os.environ,
            "FEAST_USAGE": "False",
            "CINESTREAM_FEAST_USER_PARQUET": str(self.user_path),
            "CINESTREAM_FEAST_MOVIE_PARQUET": str(self.movie_path),
        }

    def _row(self, frame: pd.DataFrame, user_id: int, movie_id: int, event: str) -> pd.Series:
        event_ts = _ts(event)
        stamps = pd.to_datetime(frame["event_timestamp"], utc=True)
        matched = frame[(frame["user_id"] == user_id) & (frame["movie_id"] == movie_id) & (stamps == event_ts)]
        self.assertEqual(len(matched), 1)
        return matched.iloc[0]


if __name__ == "__main__":
    unittest.main()
