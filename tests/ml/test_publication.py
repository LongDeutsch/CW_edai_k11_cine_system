"""Marker DP2/DP3 phải trùng export trước khi materialize."""

from __future__ import annotations

import unittest

from ml.online.publication import PublicationMismatch, assert_publication

MARKER = {"batch_id": "batch-a", "dag_id": "cinestream_dp2_bronze_to_gold"}
REPORT = {"ready": True, "user_rows": 1, "user_distinct": 1}
EXPORT = {
    "batch_id": "batch-a",
    "sequence": 1,
    "snapshot_ids": {"feat_user_movie_stats_daily": "11", "feat_movie_engagement_stats_daily": "22"},
}


class PublicationTests(unittest.TestCase):
    def test_matching_marker_and_snapshots(self):
        batch = assert_publication(MARKER, REPORT, EXPORT, "batch-a", EXPORT["snapshot_ids"])
        self.assertEqual(batch, "batch-a")

    def test_conf_batch_must_match_marker(self):
        with self.assertRaises(PublicationMismatch):
            assert_publication(MARKER, REPORT, EXPORT, "batch-b", EXPORT["snapshot_ids"])

    def test_export_from_another_batch_is_refused(self):
        other = {**EXPORT, "batch_id": "batch-b"}
        with self.assertRaises(PublicationMismatch):
            assert_publication(MARKER, REPORT, other, None, other["snapshot_ids"])

    def test_dp3_not_ready_is_refused(self):
        with self.assertRaises(PublicationMismatch):
            assert_publication(MARKER, {"ready": False}, EXPORT)

    def test_live_snapshot_drift_is_refused(self):
        with self.assertRaises(PublicationMismatch):
            assert_publication(MARKER, REPORT, EXPORT, None, {"feat_user_movie_stats_daily": "99", "feat_movie_engagement_stats_daily": "22"})


if __name__ == "__main__":
    unittest.main()
