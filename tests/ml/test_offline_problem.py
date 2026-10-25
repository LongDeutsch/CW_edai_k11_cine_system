"""Nhãn và cách chia thời gian, không cần Spark."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from ml.offline.problem import label_high_ratings, split_by_time


T0 = datetime(2026, 6, 1, 0, 0, 0)


class LabelTests(unittest.TestCase):
    def test_boundary_rating_is_a_label_and_earlier_rating_is_not(self):
        events = [
            {"user_id": 1, "movie_id": 10, "rating_val": 5.0, "rating_ts": T0 - timedelta(hours=1)},
            {"user_id": 1, "movie_id": 12, "rating_val": 4.0, "rating_ts": T0},
            {"user_id": 1, "movie_id": 10, "rating_val": 1.0, "rating_ts": T0 + timedelta(hours=8)},
            {"user_id": 1, "movie_id": 10, "rating_val": 5.0, "rating_ts": T0 + timedelta(days=1)},
        ]
        labels = label_high_ratings(events, T0)
        self.assertEqual([(row["movie_id"], row["label"]) for row in labels], [(10, 0), (12, 1)])

    def test_split_keeps_the_cutoff_in_test(self):
        rows = [
            {"event_timestamp": T0 - timedelta(days=1), "label": 1},
            {"event_timestamp": T0, "label": 0},
        ]
        train, test = split_by_time(rows, T0)
        self.assertEqual(len(train), 1)
        self.assertEqual(len(test), 1)
        self.assertLess(train[0]["event_timestamp"], T0)
        self.assertGreaterEqual(test[0]["event_timestamp"], T0)


if __name__ == "__main__":
    unittest.main()
