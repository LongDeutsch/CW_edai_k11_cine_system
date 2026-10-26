"""Baseline không có watermark, dedup hay DLQ. Job tối ưu giữ các thứ đó."""

from __future__ import annotations

import unittest
from pathlib import Path

from streaming.flink.jobs.heartbeat_baseline_job import run_local
from streaming.flink.jobs.heartbeat_window_job import proof_events
from streaming.flink.operators.window_engine import HeartbeatWindowEngine
from streaming.flink.config import StreamJobConfig

REPO = Path(__file__).resolve().parents[2]
SOURCE = (REPO / "streaming/flink/jobs/heartbeat_baseline_job.py").read_text()


class BaselineJobTests(unittest.TestCase):
    def test_graph_omits_watermark_dedup_and_dlq(self):
        self.assertIn("TumblingProcessingTimeWindows", SOURCE)
        self.assertIn("baseline-processing-time-1m", SOURCE)
        self.assertNotIn("for_bounded_out_of_orderness", SOURCE)
        self.assertNotIn(".allowed_lateness(", SOURCE)
        self.assertNotIn("side_output_late_data", SOURCE)
        self.assertNotIn("dedup-heartbeat-id", SOURCE)
        self.assertNotIn("StateTtlConfig", SOURCE)
        self.assertNotIn("kafka_dlq", SOURCE)

    def test_local_keeps_duplicates_and_late_events(self):
        payloads = proof_events()
        baseline = run_local(payloads)
        optimized = HeartbeatWindowEngine(StreamJobConfig()).process_all(payloads)
        self.assertEqual(baseline["dead_letters"], 0)
        self.assertGreater(baseline["duplicates_kept"], 0)
        self.assertEqual(baseline["accepted"], len(payloads))
        self.assertFalse(baseline["watermark"])
        self.assertFalse(baseline["dedup"])
        self.assertLess(optimized.accepted, baseline["accepted"])
        self.assertGreater(len(optimized.dead_letters) + optimized.duplicates, 0)


if __name__ == "__main__":
    unittest.main()
