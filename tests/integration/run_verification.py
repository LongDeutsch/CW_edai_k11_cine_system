"""Script chạy toàn bộ quy trình Kiểm thử và Xác thực toàn diện (Verification & Validation).

Kiểm tra:
1. Unit Tests Suite (FastMCP tools, Cache-Aside logic, Dynamic Model Loader, RAG, PIT Join)
2. Integration / E2E Trace Suite (Prompt -> SSE Stream -> FastMCP -> Qdrant -> KServe -> Response)
"""

from __future__ import annotations

import io
import json
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path


def run_test_module(module_name: str) -> tuple[int, int, int, float]:
    """Chạy một test module và trả về (total, passed, failed, duration)."""
    suite = unittest.defaultTestLoader.loadTestsFromName(module_name)
    stream = io.StringIO()
    runner = unittest.TextTestRunner(stream=stream, verbosity=0)

    start = time.time()
    result = runner.run(suite)
    duration = time.time() - start

    total = result.testsRun
    failed = len(result.failures) + len(result.errors)
    passed = total - failed
    return total, passed, failed, duration


def main() -> None:
    print("=================================================================")
    print("   CINESTREAM DATA & ML PLATFORM - VERIFICATION & VALIDATION     ")
    print("=================================================================\n")

    modules = [
        ("FastMCP Tools Suite", "tests.llm.test_mcp_tools"),
        ("CineBot Agent ReAct Suite", "tests.llm.test_agent"),
        ("SSE Streaming & Server Suite", "tests.llm.test_sse_stream"),
        ("KServe v2 & Cache-Aside Suite", "tests.ml.test_kserve_serving"),
        ("RAG Vector Indexing Suite", "tests.ml.test_rag_embeddings"),
        ("ML Training & PIT Join Suite", "tests.ml.test_training_pipeline"),
        ("End-to-End Concierge Trace", "tests.integration.test_e2e_concierge_trace"),
    ]

    total_all = 0
    passed_all = 0
    failed_all = 0
    start_total = time.time()

    print(f"{'Module Kiểm Thử':<35} | {'Tổng':<5} | {'Pass':<5} | {'Fail':<5} | {'Thời Gian':<10}")
    print("-" * 72)

    results_table = []
    for label, mod in modules:
        try:
            total, passed, failed, dur = run_test_module(mod)
            total_all += total
            passed_all += passed
            failed_all += failed
            results_table.append({"module": label, "total": total, "passed": passed, "failed": failed, "duration": dur})
            print(f"{label:<35} | {total:<5} | {passed:<5} | {failed:<5} | {dur:.3f}s")
        except Exception as e:
            print(f"{label:<35} | ERROR: {e}")

    total_duration = time.time() - start_total
    print("-" * 72)
    print(f"{'TỔNG CỘNG TOÀN HỆ THỐNG':<35} | {total_all:<5} | {passed_all:<5} | {failed_all:<5} | {total_duration:.3f}s")
    print("=================================================================\n")

    if failed_all == 0:
        print("✓ XÁC THỰC HOÀN TOÀN THÀNH CÔNG: 100% test cases đạt chuẩn chất lượng!")
    else:
        print(f"✗ CÓ {failed_all} TEST CASES BỊ LỖI!")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
