"""TTL dedup theo processing time. Không đổi luật cửa sổ."""

from __future__ import annotations

import unittest

from datetime import datetime, timezone

from streaming.flink.config import StreamJobConfig
from streaming.flink.jobs.heartbeat_window_job import proof_events
from streaming.flink.operators.window_engine import HeartbeatWindowEngine


def _row(heartbeat_id: int, user_id: int, event_ms: int) -> dict:
    stamp = datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc).isoformat()
    return {
        "heartbeat_id": heartbeat_id,
        "session_id": f"ses-{heartbeat_id:06d}",
        "user_id": user_id,
        "movie_id": 42,
        "event_ts": stamp,
        "current_position_seconds": 10,
        "playback_speed": 1.0,
        "bitrate_kbps": 2400,
    }


def _tumbling(result, start_ms: int):
    rows = [
        row
        for row in result.closed_windows + result.open_windows
        if row.window_type == "tumbling_1m" and row.window_start_ms == start_ms
    ]
    if len(rows) != 1:
        raise AssertionError(rows)
    return rows[0]


class DedupTtlTest(unittest.TestCase):
    def test_default_ttl_is_an_hour_and_keeps_the_proof_duplicate(self) -> None:
        config = StreamJobConfig()
        self.assertEqual(config.dedup_ttl_ms, 3_600_000)
        self.assertEqual(config.parallelism, 1)
        self.assertEqual(config.idle_timeout_s, 30)
        result = HeartbeatWindowEngine(config).process_all(proof_events())
        self.assertEqual(result.duplicates, 1)
        self.assertEqual(len(result.session_rows), 6)

    def test_duplicate_inside_ttl_is_dropped_and_survives_a_short_restart(self) -> None:
        engine = HeartbeatWindowEngine(StreamJobConfig(dedup_ttl_ms=1_000))
        engine.process_all([_row(1, 10, 0)])
        engine.advance_processing_time(999)
        again = engine.process_all([_row(1, 10, 0)])
        self.assertEqual(again.duplicates, 1)
        self.assertEqual(again.accepted, 0)
        self.assertEqual(_tumbling(again, 0).heartbeat_count, 1)

    def test_duplicate_after_processing_time_ttl_is_counted_again(self) -> None:
        engine = HeartbeatWindowEngine(StreamJobConfig(dedup_ttl_ms=1_000))
        engine.process_all([_row(1, 10, 0)])
        engine.advance_processing_time(1_000)
        again = engine.process_all([_row(1, 10, 0)])
        self.assertEqual(again.duplicates, 0)
        self.assertEqual(again.accepted, 1)
        row = _tumbling(again, 0)
        self.assertEqual(row.heartbeat_count, 2)
        self.assertEqual(row.heartbeat_ids, (1, 1))

    def test_disabled_ttl_does_not_expire(self) -> None:
        engine = HeartbeatWindowEngine(StreamJobConfig(dedup_ttl_ms=0))
        engine.process_all([_row(7, 1, 0)])
        engine.advance_processing_time(10**12)
        again = engine.process_all([_row(7, 1, 0)])
        self.assertEqual(again.duplicates, 1)


if __name__ == "__main__":
    unittest.main()
