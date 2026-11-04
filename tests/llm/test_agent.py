"""Kiểm thử CineBot Agent điều phối hội thoại và gọi MCP Tools."""

from __future__ import annotations

import unittest

from llm.agent import CineBotAgent, agent


class TestCineBotAgent(unittest.TestCase):
    def test_intent_detection(self):
        self.assertEqual(agent.detect_intent("Gợi ý cho tôi phim hành động hay"), "recommendation")
        self.assertEqual(agent.detect_intent("Tìm phim về du hành không gian"), "search")
        self.assertEqual(agent.detect_intent("Xin chào bạn là ai"), "general_qa")

    def test_run_recommendation_flow(self):
        res = agent.run_flow(message="Gợi ý phim trộm cắp ly kỳ", user_id=7, top_k=2)
        self.assertEqual(res["intent"], "recommendation")
        self.assertEqual(res["user_id"], 7)
        self.assertEqual(len(res["tool_calls"]), 3)

        tool_names = [call["tool"] for call in res["tool_calls"]]
        self.assertEqual(
            tool_names,
            ["fetch_user_movie_profile", "search_movie_knowledge_base", "predict_movie_ranking"],
        )
        self.assertEqual(len(res["recommended_movies"]), 2)
        self.assertIn("reply", res)
        self.assertTrue(len(res["reply"]) > 50)

    def test_run_general_qa_flow(self):
        res = agent.run_flow(message="Xin chào", user_id=1)
        self.assertEqual(res["intent"], "general_qa")
        self.assertEqual(len(res["tool_calls"]), 0)
        self.assertIn("CineBot", res["reply"])


if __name__ == "__main__":
    unittest.main()
