"""Dừng job heartbeat đang chạy.

Không xóa checkpoint và không đổi offset Kafka. Job test dùng cùng tên script
``heartbeat_window_job`` nên lệnh này cũng hủy job đó nếu nó đang chạy.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request

FLINK = "http://127.0.0.1:18081"


def running_heartbeat_jobs() -> list[dict]:
    with urllib.request.urlopen(f"{FLINK}/jobs/overview", timeout=5) as response:
        overview = json.load(response)
    jobs = []
    for job in overview.get("jobs") or []:
        name = job.get("name") or ""
        if job.get("state") in {"RUNNING", "RESTARTING", "CREATED"} and "heartbeat_window" in name:
            jobs.append(job)
    return jobs


def stop_heartbeat_jobs() -> list[str]:
    cancelled = []
    for job in running_heartbeat_jobs():
        job_id = job["jid"]
        request = urllib.request.Request(f"{FLINK}/jobs/{job_id}?mode=cancel", method="PATCH")
        with urllib.request.urlopen(request, timeout=10):
            cancelled.append(job_id)
    return cancelled


def main() -> None:
    try:
        cancelled = stop_heartbeat_jobs()
    except urllib.error.URLError as exc:
        print(f"JobManager không chạy: {exc.reason}")
        return
    if cancelled:
        print("đã hủy " + ", ".join(cancelled))
        return
    print("không có job heartbeat đang chạy")


if __name__ == "__main__":
    main()
