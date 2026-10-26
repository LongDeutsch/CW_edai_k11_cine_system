"""Chunked generator giữ duplicate rate và không giữ cả bảng trong RAM."""

from __future__ import annotations

import unittest
from argparse import Namespace
from pathlib import Path

import numpy as np

from generator.large_scale import exceeds_memory_budget, run_chunked


class LargeScaleTests(unittest.TestCase):
    def test_budget_switches_only_above_two_million_watch_rows(self):
        self.assertFalse(exceeds_memory_budget(800_000))
        self.assertTrue(exceeds_memory_budget(2_000_001))

    def test_chunked_run_writes_parquet_and_duplicate_rate(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            args = Namespace(
                scale_factor=0.01,
                skew_ratio=0.8,
                duplicate_rate=0.025,
                output_dir=Path(tmp),
                random_seed=7,
                head_fraction=0.01,
                anchor_movie_id=42,
                anchor_share=0.04,
                zipf_exponent=1.25,
                super_user_fraction=0.05,
                super_user_mass=0.40,
                pareto_shape=1.5,
                beta_alpha=6.0,
                beta_beta=2.0,
                genre_skew_ratio=0.70,
                schema_evolution_from_partition=3,
                movie_partitions=4,
                duplicate_ts_offset_seconds=3,
                duplicate_next_partition_rate=0.5,
                num_event_days=2,
                start_date="2026-01-01",
                max_rows_per_file=400,
            )
            rng = np.random.default_rng(7)
            seed = {"movies_by_id": {}, "tag_vocab": np.array(["classic", "rewatch"], dtype=object)}
            counts = {
                "users": 80,
                "movies": 50,
                "movie_genres": 60,
                "ratings": 200,
                "tags": 40,
                "watch_history": 180,
            }
            payload = run_chunked(args, rng, seed, counts)
            self.assertGreater(payload["parquet_bytes"], 0)
            self.assertAlmostEqual(payload["ratings"]["duplicate_rate"], 0.025, places=2)
            self.assertAlmostEqual(payload["watch_history"]["duplicate_rate"], 0.025, places=2)
            self.assertEqual(payload["tags"]["extra_rows"], 0)
            self.assertGreater(payload["combined"]["head_share"], 0.5)
            self.assertTrue((Path(tmp) / "ratings").exists())
            self.assertIn("content_rating", payload["schema_evolution"]["new_schema_columns"])
            self.assertNotIn("content_rating", payload["schema_evolution"]["old_schema_columns"])


if __name__ == "__main__":
    unittest.main()
