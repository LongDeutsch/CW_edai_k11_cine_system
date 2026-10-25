"""Entry production là một giá trị. DAG còn lại không ghi cùng bảng."""

from __future__ import annotations

import os
import unittest

from orchestration.batch_guard import assert_exclusive_writer


class ExclusiveWriterTests(unittest.TestCase):
    def tearDown(self):
        os.environ.pop("CINESTREAM_BATCH_ENTRY", None)

    def test_isolated_namespace_is_open_to_both_families(self):
        assert_exclusive_writer("cinestream_daily_batch_lakehouse", ("it_af_dp",) * 3)
        assert_exclusive_writer("cinestream_dp1_raw_to_bronze", ("it_af_dp",) * 3)

    def test_split_cannot_write_production_while_legacy_is_the_entry(self):
        with self.assertRaises(RuntimeError):
            assert_exclusive_writer("cinestream_dp2_bronze_to_gold", ("bronze", "silver", "gold"))

    def test_legacy_cannot_write_production_after_the_entry_switches(self):
        os.environ["CINESTREAM_BATCH_ENTRY"] = "split"
        with self.assertRaises(RuntimeError):
            assert_exclusive_writer("cinestream_daily_batch_lakehouse", ("gold", "gold", "gold"))
        assert_exclusive_writer("cinestream_dp3_offline_features", ("gold", "gold", "gold"))


if __name__ == "__main__":
    unittest.main()
