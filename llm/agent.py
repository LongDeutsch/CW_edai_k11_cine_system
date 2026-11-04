"""CineBot Agent: Điều phối hội thoại, phân tích intent và gọi FastMCP Tools.

Quy trình ReAct Agent:
1. Phân tích Intent của người dùng (Gợi ý phim, Tìm kiếm nội dung, Chào hỏi/Hỏi đáp).
2. Lập kế hoạch và thực thi FastMCP tools:
   - fetch_user_movie_profile -> search_movie_knowledge_base -> predict_movie_ranking.
3. Tổng hợp ngữ cảnh và tạo câu trả lời giải thích tự nhiên qua LLM (Ollama).
"""

from __future__ import annotations

import json
import re
from typing import Any, Generator

from llm.mcp.server import (
    fetch_user_movie_profile,
    mcp,
    predict_movie_ranking,
    search_movie_knowledge_base,
)

SYSTEM_PROMPT = """Bạn là CineBot - trợ lý ảo gợi ý phim thông minh của nền tảng CineStream.
Nhiệm vụ của bạn là tư vấn phim dựa trên sở thích cá nhân, kết quả tìm kiếm ngữ nghĩa cốt truyện và điểm số mô hình Machine Learning.
Hãy giải thích rõ ràng, hấp dẫn lý do vì sao người dùng nên xem từng bộ phim được đề xuất."""


class CineBotAgent:
    """Agent điều phối quy trình hội thoại và gọi MCP Tools cho CineStream."""

    def __init__(self, system_prompt: str = SYSTEM_PROMPT) -> None:
        self.system_prompt = system_prompt
        self.mcp = mcp

    def detect_intent(self, user_message: str) -> str:
        """Phân tích ý định người dùng."""
        msg = (user_message or "").lower()
        if any(w in msg for w in ("gợi ý", "recommend", "xem gì", "thích", "phim hay", "tư vấn")):
            return "recommendation"
        if any(w in msg for w in ("tìm", "search", "cốt truyện", "nội dung", "về", "thể loại")):
            return "search"
        return "general_qa"

    def run_flow(
        self,
        message: str,
        user_id: int = 1,
        top_k: int = 3,
    ) -> dict[str, Any]:
        """Thực thi luồng suy luận và gọi tools đầy đủ."""
        intent = self.detect_intent(message)
        tool_events: list[dict[str, Any]] = []

        if intent in ("recommendation", "search"):
            # 1. Gọi Tool 1: Lấy hồ sơ người dùng
            profile = self.mcp.call_tool("fetch_user_movie_profile", {"user_id": user_id})
            tool_events.append(
                {
                    "tool": "fetch_user_movie_profile",
                    "args": {"user_id": user_id},
                    "result": profile,
                }
            )

            # 2. Gọi Tool 2: Tìm kiếm ngữ nghĩa cốt truyện từ Qdrant
            query_clean = re.sub(r"(gợi ý|cho tôi|phim|hãy|tìm|xem|về)", "", message, flags=re.IGNORECASE).strip()
            if not query_clean:
                query_clean = profile.get("favorite_genre", "Sci-Fi Action")

            search_results = self.mcp.call_tool(
                "search_movie_knowledge_base",
                {"query": query_clean, "top_k": 5, "genres": profile.get("favorite_genre")},
            )
            tool_events.append(
                {
                    "tool": "search_movie_knowledge_base",
                    "args": {"query": query_clean, "top_k": 5},
                    "result": search_results,
                }
            )

            # 3. Gọi Tool 3: Xếp hạng qua Machine Learning Model Serving
            candidate_ids = [m["movie_id"] for m in search_results]
            ranked = self.mcp.call_tool(
                "predict_movie_ranking",
                {"user_id": user_id, "candidate_ids": candidate_ids},
            )
            tool_events.append(
                {
                    "tool": "predict_movie_ranking",
                    "args": {"user_id": user_id, "candidate_ids": candidate_ids},
                    "result": ranked,
                }
            )

            # Ghép thông tin metadata phim với điểm số ML
            movie_map = {m["movie_id"]: m for m in search_results}
            top_movies = []
            for item in ranked[:top_k]:
                mid = item["movie_id"]
                if mid in movie_map:
                    entry = dict(movie_map[mid])
                    entry["ranking_score"] = item["ranking_score"]
                    entry["rank"] = item["rank"]
                    top_movies.append(entry)

            # 4. Sinh câu trả lời tổng hợp
            reply = self._synthesize_response(user_id, message, profile, top_movies)

            return {
                "user_id": user_id,
                "intent": intent,
                "tool_calls": tool_events,
                "recommended_movies": top_movies,
                "reply": reply,
            }

        # General QA
        reply = (
            "Xin chào! Tôi là CineBot - trợ lý xem phim của bạn tại CineStream. "
            "Bạn có thể yêu cầu tôi gợi ý phim theo cốt truyện, thể loại hoặc diễn viên yêu thích nhé!"
        )
        return {
            "user_id": user_id,
            "intent": "general_qa",
            "tool_calls": [],
            "recommended_movies": [],
            "reply": reply,
        }

    def _synthesize_response(
        self,
        user_id: int,
        user_message: str,
        profile: dict[str, Any],
        top_movies: list[dict[str, Any]],
    ) -> str:
        """Tổng hợp câu trả lời tự nhiên cho người dùng."""
        # Thử gọi Ollama chat nếu khả dụng
        try:
            from llm.client import chat

            prompt = (
                f"{self.system_prompt}\n\n"
                f"Người dùng #{user_id} (thể loại yêu thích: {profile.get('favorite_genre')}) hỏi: '{user_message}'.\n"
                f"Mô hình đã chọn lọc và xếp hạng các bộ phim phù hợp nhất sau đây:\n"
            )
            for m in top_movies:
                prompt += f"- {m['title']} (Thể loại: {m['genres']}, Điểm ML: {m.get('ranking_score')}): {m['overview']}\n"
            prompt += "\nHãy trả lời người dùng một cách hào hứng, tóm tắt lý do gợi ý từng phim và chúc họ xem phim vui vẻ."

            res = chat([{"role": "user", "content": prompt}], timeout=5.0)
            msg = res.get("message", {}).get("content", "").strip()
            if msg:
                return msg
        except Exception:
            pass

        # Fallback tạo câu trả lời mạch lạc chuẩn xác
        fav_genre = profile.get("favorite_genre", "yêu thích")
        lines = [
            f"Dựa trên gu xem phim của bạn ({fav_genre}) và yêu cầu tìm kiếm, CineBot đã chọn lọc và xếp hạng {len(top_movies)} bộ phim tuyệt vời nhất dành riêng cho bạn:\n"
        ]
        for idx, m in enumerate(top_movies, 1):
            score_percent = int(m.get("ranking_score", 0.8) * 100)
            lines.append(
                f"{idx}. **{m['title']}** (Thể loại: {m['genres']}) — *Độ phù hợp: {score_percent}%*\n"
                f"   📖 Cốt truyện: {m['overview']}\n"
            )
        lines.append("Chúc bạn có những giờ phút xem phim thư giãn trên CineStream! 🍿🎬")
        return "\n".join(lines)


agent = CineBotAgent()
