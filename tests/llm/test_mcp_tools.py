"""Kiểm thử FastMCP Server và 3 tools nghiệp vụ."""

from __future__ import annotations

import unittest

from llm.mcp import (
    fetch_user_movie_profile,
    mcp,
    predict_movie_ranking,
    search_movie_knowledge_base,
)


class TestFastMCPTools(unittest.TestCase):
    def test_mcp_server_tools_registered(self):
        tools = mcp.list_tools()
        self.assertEqual(len(tools), 3)
        tool_names = [t["name"] for t in tools]
        self.assertIn("fetch_user_movie_profile", tool_names)
        self.assertIn("search_movie_knowledge_base", tool_names)
        self.assertIn("predict_movie_ranking", tool_names)

    def test_fetch_user_movie_profile(self):
        profile = fetch_user_movie_profile(user_id=42)
        self.assertEqual(profile["user_id"], 42)
        self.assertIn("favorite_genre", profile)
        self.assertIn("recent_movie_ids", profile)

    def test_search_movie_knowledge_base(self):
        results = search_movie_knowledge_base(query="heist bank", top_k=2)
        self.assertGreaterEqual(len(results), 1)
        first = results[0]
        self.assertIn("movie_id", first)
        self.assertIn("title", first)
        self.assertIn("overview", first)
        self.assertIn("similarity_score", first)

    def test_predict_movie_ranking(self):
        candidates = [10, 20, 30]
        ranked = predict_movie_ranking(user_id=1, candidate_ids=candidates)
        self.assertEqual(len(ranked), 3)
        self.assertEqual(ranked[0]["rank"], 1)
        self.assertIn("ranking_score", ranked[0])


if __name__ == "__main__":
    unittest.main()
