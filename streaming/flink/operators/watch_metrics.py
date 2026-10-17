"""Thời lượng xem của một heartbeat, tách khỏi vị trí seek.

``current_position_seconds`` là vị trí phát trong file phim. Người dùng tua
thì số này nhảy, không phải thời gian đã xem.

Nếu payload có ``watched_delta_seconds`` thì dùng đúng giá trị đó (không âm).
Không có thì ước lượng một nhịp bằng ``HEARTBEAT_STEP_SECONDS`` (mặc định 10).
Batch ``stg_watch_sessions.watch_duration_seconds`` cũng là thời gian đã xem
của cả phiên, không phải vị trí tua. Hai bên khác grain nhưng cùng nghĩa
"thời gian xem", không dùng vị trí seek.
"""

from __future__ import annotations


def watched_delta_seconds(payload: dict, heartbeat_seconds: int) -> int:
    raw = payload.get("watched_delta_seconds")
    if raw is not None and raw != "":
        return max(0, int(raw))
    return max(0, int(heartbeat_seconds))
