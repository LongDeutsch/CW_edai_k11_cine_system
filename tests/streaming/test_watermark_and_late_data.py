"""Watermark Flink và membership của chuỗi proof."""

from __future__ import annotations

import unittest

from streaming.flink.config import StreamJobConfig
from streaming.flink.jobs.heartbeat_window_job import proof_events
from streaming.flink.operators.watermark_assigner import (
    WatermarkClock,
    dlq_watermark_ms,
    extract_event_time_ms,
    flink_watermark_ms,
    is_window_late,
    tumbling_start_ms,
)
from streaming.flink.operators.window_engine import HeartbeatWindowEngine


class WatermarkAndLateDataTest(unittest.TestCase):
    def test_bounded_out_of_orderness_matches_flink_periodic_watermark(self) -> None:
        clock = WatermarkClock(out_of_orderness_ms=15_000)
        self.assertIsNone(clock.observe(1_000))
        self.assertEqual(clock.watermark_ms, flink_watermark_ms(1_000, 15_000))
        previous = clock.observe(40_000)
        self.assertEqual(previous, 1_000 - 15_000 - 1)
        self.assertEqual(clock.watermark_ms, 40_000 - 15_000 - 1)

    def test_recovery_uses_checkpointed_max_only_while_live_watermark_is_absent(self) -> None:
        self.assertEqual(dlq_watermark_ms(314_999, 330_000, 15_000), 314_999)
        self.assertEqual(dlq_watermark_ms(-15_001, 330_000, 15_000), -15_001)
        self.assertEqual(dlq_watermark_ms(None, 330_000, 15_000), 314_999)
        self.assertEqual(dlq_watermark_ms(-1 << 63, 330_000, 15_000), 314_999)
        self.assertIsNone(dlq_watermark_ms(None, None, 15_000))
        self.assertIsNone(dlq_watermark_ms(-1 << 63, None, 15_000))

    def test_lateness_is_per_window_not_a_single_watermark_cut(self) -> None:
        self.assertFalse(is_window_late(60_000, None, 15_000))
        self.assertFalse(is_window_late(60_000, 74_998, 15_000))
        self.assertTrue(is_window_late(60_000, 74_999, 15_000))
        self.assertFalse(is_window_late(300_000, 74_999, 15_000))

    def test_iso_event_ts_and_tumbling_alignment(self) -> None:
        event_ms = extract_event_time_ms({"event_ts": "1970-01-01T00:01:30+00:00"})
        self.assertEqual(event_ms, 90_000)
        self.assertEqual(tumbling_start_ms(90_000, 60_000), 60_000)

    def test_disordered_stream_keeps_partially_late_heartbeat(self) -> None:
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(proof_events())
        self.assertEqual(result.accepted, 6)
        self.assertEqual(result.duplicates, 1)
        self.assertEqual(result.dead_letters, [])
        self.assertEqual(result.final_watermark_ms, 164_999)
        self.assertEqual(len(result.session_rows), 6)
        first = next(row for row in result.closed_windows if row.window_type == "tumbling_1m" and row.window_start_ms == 0)
        self.assertEqual(first.heartbeat_count, 2)
        self.assertEqual(first.total_watch_seconds, 20)
        second = next(row for row in result.closed_windows if row.window_type == "tumbling_1m" and row.window_start_ms == 60_000)
        self.assertEqual(second.heartbeat_count, 2)
        self.assertEqual(second.total_watch_seconds, 20)


if __name__ == "__main__":
    unittest.main()
