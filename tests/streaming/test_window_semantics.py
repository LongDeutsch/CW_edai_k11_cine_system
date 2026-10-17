"""Tumbling 1 phút: đúng giờ, trễ được phép, và dead letter sau cleanup."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from streaming.flink.config import StreamJobConfig
from streaming.flink.jobs.heartbeat_window_job import proof_events
from streaming.flink.operators.watermark_assigner import cleanup_time_ms, is_window_late
from streaming.flink.operators.window_engine import HeartbeatWindowEngine


def _row(heartbeat_id: int, user_id: int, event_ms: int, position: int = 10, **extra) -> dict:
    stamp = datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc).isoformat()
    payload = {
        "heartbeat_id": heartbeat_id,
        "session_id": f"ses-{heartbeat_id:06d}",
        "user_id": user_id,
        "movie_id": 42,
        "event_ts": stamp,
        "current_position_seconds": position,
        "playback_speed": 1.0,
        "bitrate_kbps": 2400,
    }
    payload.update(extra)
    return payload


def _ids(result, start_ms: int) -> set[int]:
    rows = result.closed_windows + result.open_windows
    match = [row for row in rows if row.window_start_ms == start_ms]
    if len(match) != 1:
        raise AssertionError(f"tumbling@{start_ms}: {len(match)} rows")
    return set(match[0].heartbeat_ids)


class WindowMembershipTest(unittest.TestCase):
    def test_proof_sequence_matches_flink_window_membership(self) -> None:
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(proof_events())
        self.assertEqual(result.duplicates, 1)
        self.assertEqual(result.final_watermark_ms, 180_000 - 15_000 - 1)
        self.assertEqual([item["heartbeat_id"] for item in result.dead_letters], [4])
        self.assertEqual(result.dead_letters[0]["reason"], "window_past_cleanup")
        self.assertEqual(len(result.session_rows), 6)
        self.assertEqual(_ids(result, 0), {1, 2})
        self.assertEqual(_ids(result, 60_000), {3, 5})
        self.assertNotIn(4, _ids(result, 0))

    def test_event_earlier_than_watermark_stays_when_window_is_open(self) -> None:
        self.assertFalse(is_window_late(60_000, 64_999, 15_000))
        self.assertEqual(cleanup_time_ms(60_000, 15_000), 74_999)
        payloads = [
            _row(1, 10, 0),
            _row(2, 11, 20_000),
            _row(3, 12, 80_000),
            _row(4, 13, 5_000),
        ]
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(payloads)
        self.assertEqual(result.dead_letters, [])
        self.assertEqual(_ids(result, 0), {1, 2, 4})
        kinds = [item.kind for item in result.firings if item.row.window_start_ms == 0]
        self.assertEqual(kinds, ["main", "late"])
        main = next(item for item in result.firings if item.kind == "main" and item.row.window_start_ms == 0)
        self.assertEqual(main.row.heartbeat_count, 2)
        late = next(item for item in result.firings if item.kind == "late" and item.row.window_start_ms == 0)
        self.assertEqual(late.row.heartbeat_count, 3)
        self.assertEqual(late.row.total_watch_seconds, 30)


class DeadLetterCasesTest(unittest.TestCase):
    def test_event_after_cleanup_is_one_dead_letter(self) -> None:
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(proof_events())
        self.assertEqual([item["heartbeat_id"] for item in result.dead_letters], [4])
        self.assertEqual(result.dead_letters[0]["window_start_ms"], 0)
        self.assertEqual(result.dead_letters[0]["window_end_ms"], 60_000)

    def test_out_of_order_event_stays_inside_open_window(self) -> None:
        payloads = [
            _row(1, 10, 0),
            _row(2, 11, 50_000),
            _row(3, 12, 10_000),
        ]
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(payloads)
        self.assertEqual(result.dead_letters, [])
        self.assertEqual(_ids(result, 0), {1, 2, 3})

    def test_too_late_event_is_absent_from_the_window(self) -> None:
        payloads = [
            _row(1, 10, 330_000),
            _row(2, 11, 0),
        ]
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(payloads)
        self.assertEqual([item["heartbeat_id"] for item in result.dead_letters], [2])
        self.assertEqual(result.dead_letters[0]["record_kind"], "dead_letter")
        rows = [row for row in result.closed_windows + result.open_windows if row.window_start_ms == 0]
        self.assertEqual(rows, [])


class WatchDurationTest(unittest.TestCase):
    def test_seek_does_not_inflate_watch_duration(self) -> None:
        payloads = [
            _row(1, 10, 0, position=10),
            _row(2, 11, 20_000, position=500),
            _row(3, 12, 90_000, position=40, watched_delta_seconds=4),
        ]
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(payloads)
        window = next(row for row in result.closed_windows if row.window_start_ms == 0)
        self.assertEqual(set(window.heartbeat_ids), {1, 2})
        self.assertEqual(window.total_watch_seconds, 20)
        positions = {row["heartbeat_id"]: row["playback_position_seconds"] for row in result.session_rows}
        watched = {row["heartbeat_id"]: row["watch_duration_seconds"] for row in result.session_rows}
        self.assertEqual(positions[2], 500)
        self.assertEqual(watched[2], 10)
        self.assertEqual(watched[3], 4)


if __name__ == "__main__":
    unittest.main()
