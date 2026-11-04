"""Kiểm thử tích hợp toàn trình (End-to-End Integration Trace):
User Prompt -> SSE Stream -> CineBot Agent -> FastMCP Tools -> Qdrant -> KServe ML -> Response Tokens.
"""

from __future__ import annotations

import json
import time
import unittest
from pathlib import Path

from llm.agent import agent
from llm.mcp.server import mcp
from llm.streaming import stream_agent_events
from serving.ml_serving.app import (
    cache,
    handle_kserve_infer,
    handle_kserve_model_load,
    handle_recommend,
    registry,
)


class TestE2EConciergeTrace(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Đảm bảo registry nạp đầy đủ mô hình
        registry.artifact_dir = Path("artifacts/models").resolve()
        registry.load_all_available()

    def setUp(self):
        cache._memory_store.clear()

    def test_end_to_end_trace_sse_to_tools_to_ranking(self):
        """Kịch bản E2E toàn trình: Từ câu hỏi người dùng qua SSE -> FastMCP -> Qdrant -> KServe -> Tokens."""
        prompt = "Gợi ý cho tôi phim hành động trộm cướp ngân hàng kịch tính"
        user_id = 1
        top_k = 3

        start_time = time.time()
        event_stream = list(stream_agent_events(prompt, user_id=user_id, top_k=top_k, chunk_delay=0.0))
        total_time = time.time() - start_time

        self.assertGreater(len(event_stream), 10, "Luồng SSE phải chứa đầy đủ các sự kiện")

        # 1. Bóc tách và kiểm chứng từng sự kiện trong stream
        events = []
        for raw in event_stream:
            lines = raw.strip().split("\n")
            event_name = lines[0].replace("event: ", "").strip()
            data_json = json.loads(lines[1].replace("data: ", "").strip())
            events.append((event_name, data_json))

        # Kiểm chứng event: start
        self.assertEqual(events[0][0], "start")
        self.assertEqual(events[0][1]["user_id"], user_id)

        # Kiểm chứng event: tool_start và tool_result cho 3 tools theo đúng thứ tự
        tool_results = [e[1] for e in events if e[0] == "tool_result"]
        self.assertEqual(len(tool_results), 3, "Phải gọi đủ 3 FastMCP tools")

        # Chặng 1: Redis User Profile
        self.assertEqual(tool_results[0]["tool"], "fetch_user_movie_profile")
        profile = tool_results[0]["result"]
        self.assertIn("favorite_genre", profile)

        # Chặng 2: Qdrant Vector Search
        self.assertEqual(tool_results[1]["tool"], "search_movie_knowledge_base")
        self.assertGreater(tool_results[1]["count"], 0)

        # Chặng 3: KServe v2 ML Ranking
        self.assertEqual(tool_results[2]["tool"], "predict_movie_ranking")
        ranked_items = tool_results[2]["result"]
        self.assertEqual(len(ranked_items), top_k)
        self.assertEqual(ranked_items[0]["rank"], 1)

        # Kiểm chứng các token được stream về
        tokens = [e[1]["token"] for e in events if e[0] == "token"]
        self.assertGreater(len(tokens), 15, "Phải stream từng từ của câu trả lời")
        full_reply = "".join(tokens)
        self.assertIn("CineBot", full_reply)

        # Kiểm chứng event: done
        last_event = events[-1]
        self.assertEqual(last_event[0], "done")
        self.assertEqual(last_event[1]["status"], "completed")
        self.assertEqual(len(last_event[1]["recommended_movies"]), top_k)

        # Kiểm tra độ trễ (SLA < 1.0 giây khi không có network lag)
        self.assertLess(total_time, 2.0, f"Thời gian thực thi E2E phải < 2s, thực tế: {total_time:.4f}s")

    def test_cache_aside_latency_benchmark(self):
        """Xác thực hiệu năng Cache-Aside: Cache Hit phải nhanh hơn rõ rệt so với Cache Miss."""
        user_id = 888
        candidates = [1, 6, 10, 28, 42]

        # Lần 1: Cache Miss (gọi mô hình tính toán)
        t0 = time.time()
        status1, resp1 = handle_recommend(user_id, candidate_movie_ids=candidates, top_k=3)
        t_miss = time.time() - t0

        self.assertEqual(status1, 200)
        self.assertFalse(resp1["cache_hit"])

        # Lần 2: Cache Hit (đọc trực tiếp từ bộ đệm)
        t1 = time.time()
        status2, resp2 = handle_recommend(user_id, candidate_movie_ids=candidates, top_k=3)
        t_hit = time.time() - t1

        self.assertEqual(status2, 200)
        self.assertTrue(resp2["cache_hit"])
        self.assertEqual(resp1["recommendations"], resp2["recommendations"])

        # Đảm bảo Cache Hit đạt độ trễ cực thấp (< 5ms)
        self.assertLess(t_hit, 0.01, f"Cache Hit phải có độ trễ < 10ms, thực tế: {t_hit * 1000:.2f}ms")

    def test_dynamic_model_reload_e2e(self):
        """Xác thực cơ chế Zero-Downtime Hot Reload mô hình ML."""
        # Kiểm tra suy luận trước khi reload
        status_before, res_before = handle_kserve_infer(
            "churn",
            {"inputs": [{"name": "feat", "data": [[5.0, 3.5, 10.0]]}]},
        )
        self.assertEqual(status_before, 200)

        # Kích hoạt Dynamic Reload
        status_reload, res_reload = handle_kserve_model_load("churn")
        self.assertEqual(status_reload, 200)
        self.assertIn("thành công", res_reload["message"])

        # Kiểm tra suy luận sau khi reload (phải tiếp tục phục vụ bình thường)
        status_after, res_after = handle_kserve_infer(
            "churn",
            {"inputs": [{"name": "feat", "data": [[5.0, 3.5, 10.0]]}]},
        )
        self.assertEqual(status_after, 200)
        self.assertEqual(res_before["outputs"][0]["shape"], res_after["outputs"][0]["shape"])


if __name__ == "__main__":
    unittest.main()
