"""Cửa sổ Flink, DLQ theo đường xử lý, và thời lượng xem không tính seek."""

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


def _ids(result, window_type: str, start_ms: int) -> set[int]:
    rows = result.closed_windows + result.open_windows
    match = [row for row in rows if row.window_type == window_type and row.window_start_ms == start_ms]
    if len(match) != 1:
        raise AssertionError(f"{window_type}@{start_ms}: {len(match)} rows")
    return set(match[0].heartbeat_ids)


class WindowMembershipTest(unittest.TestCase):
    def test_proof_sequence_matches_flink_window_membership(self) -> None:
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(proof_events())
        self.assertEqual(result.duplicates, 1)
        self.assertEqual(result.final_watermark_ms, 180_000 - 15_000 - 1)
        self.assertEqual(result.dead_letters, [])
        self.assertEqual(len(result.session_rows), 6)
        self.assertEqual(_ids(result, "tumbling_1m", 0), {1, 2})
        self.assertEqual(_ids(result, "tumbling_1m", 60_000), {3, 5})
        self.assertEqual(_ids(result, "sliding_5m", -240_000), {1, 2})
        self.assertEqual(_ids(result, "sliding_5m", -180_000), {1, 2, 3, 4, 5})
        self.assertIn(4, _ids(result, "sliding_5m", 0))
        self.assertNotIn(4, _ids(result, "tumbling_1m", 0))
        observations = [item for item in result.late_observations if item["heartbeat_id"] == 4]
        self.assertEqual(
            {(item["window_type"], item["window_start_ms"]) for item in observations},
            {("tumbling_1m", 0), ("sliding_5m", -240_000)},
        )

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
        self.assertEqual(_ids(result, "tumbling_1m", 0), {1, 2, 4})
        kinds = [
            item.kind
            for item in result.firings
            if item.row.window_type == "tumbling_1m" and item.row.window_start_ms == 0
        ]
        self.assertEqual(kinds, ["main", "late"])
        main = next(item for item in result.firings if item.kind == "main" and item.row.window_type == "tumbling_1m")
        self.assertEqual(main.row.heartbeat_count, 2)
        late = next(
            item
            for item in result.firings
            if item.kind == "late" and item.row.window_type == "tumbling_1m" and item.row.window_start_ms == 0
        )
        self.assertEqual(late.row.heartbeat_count, 3)
        self.assertEqual(late.row.total_watch_seconds, 30)


class DeadLetterCasesTest(unittest.TestCase):
    def test_tumbling_only_is_observation_not_global_dlq(self) -> None:
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(proof_events())
        self.assertEqual(result.dead_letters, [])
        self.assertTrue(any(item["heartbeat_id"] == 4 and item["window_type"] == "tumbling_1m" for item in result.late_observations))

    def test_sliding_only_when_tumbling_window_is_still_open(self) -> None:
        config = StreamJobConfig(tumbling_ms=300_000, sliding_size_ms=60_000, sliding_slide_ms=60_000)
        payloads = [
            _row(1, 10, 0),
            _row(2, 11, 95_000),
            _row(3, 12, 10_000),
        ]
        result = HeartbeatWindowEngine(config).process_all(payloads)
        self.assertEqual(result.dead_letters, [])
        rejected = [item for item in result.late_observations if item["heartbeat_id"] == 3]
        self.assertEqual([item["window_type"] for item in rejected], ["sliding_5m"])
        self.assertEqual(rejected[0]["window_end_ms"], 60_000)
        self.assertIn(3, _ids(result, "tumbling_1m", 0))

    def test_both_families_emit_one_dead_letter(self) -> None:
        payloads = [
            _row(1, 10, 330_000),
            _row(2, 11, 0),
        ]
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(payloads)
        self.assertEqual([item["heartbeat_id"] for item in result.dead_letters], [2])
        self.assertEqual(result.dead_letters[0]["reason"], "late_for_all_windows")
        self.assertEqual(result.dead_letters[0]["record_kind"], "dead_letter")
        rejected = {item["window_type"] for item in result.dead_letters[0]["rejected_windows"]}
        self.assertEqual(rejected, {"tumbling_1m", "sliding_5m"})
        observations = [item for item in result.late_observations if item["heartbeat_id"] == 2]
        self.assertEqual(len(observations), 6)
        rows = [
            row
            for row in result.closed_windows + result.open_windows
            if row.window_type == "tumbling_1m" and row.window_start_ms == 0
        ]
        self.assertEqual(rows, [])


class WatchDurationTest(unittest.TestCase):
    def test_seek_does_not_inflate_watch_duration(self) -> None:
        payloads = [
            _row(1, 10, 0, position=10),
            _row(2, 11, 20_000, position=500),
            _row(3, 12, 90_000, position=40, watched_delta_seconds=4),
        ]
        result = HeartbeatWindowEngine(StreamJobConfig()).process_all(payloads)
        window = next(row for row in result.closed_windows if row.window_type == "tumbling_1m" and row.window_start_ms == 0)
        self.assertEqual(set(window.heartbeat_ids), {1, 2})
        self.assertEqual(window.total_watch_seconds, 20)
        positions = {row["heartbeat_id"]: row["playback_position_seconds"] for row in result.session_rows}
        watched = {row["heartbeat_id"]: row["watch_duration_seconds"] for row in result.session_rows}
        self.assertEqual(positions[2], 500)
        self.assertEqual(watched[2], 10)
        self.assertEqual(watched[3], 4)


if __name__ == "__main__":
    unittest.main()
