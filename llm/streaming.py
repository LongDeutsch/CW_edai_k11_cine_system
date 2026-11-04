"""Server-Sent Events (SSE) Streaming generator cho CineBot Concierge.

Chuẩn hóa các sự kiện streaming:
- start: Bắt đầu phiên hội thoại
- tool_start: Bắt đầu gọi một MCP tool
- tool_result: Trả kết quả của tool
- token: Truyền từng từ (token) của câu trả lời đến UI
- done: Hoàn tất hội thoại kèm danh sách phim đã gợi ý
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Generator

from llm.agent import agent


def format_sse_event(event_type: str, data: dict[str, Any]) -> str:
    """Format một sự kiện theo chuẩn Server-Sent Events (text/event-stream)."""
    payload = json.dumps(data, ensure_ascii=False)
    return f"event: {event_type}\ndata: {payload}\n\n"


def stream_agent_events(
    message: str,
    user_id: int = 1,
    top_k: int = 3,
    chunk_delay: float = 0.01,
) -> Generator[str, None, None]:
    """Generator sinh chuỗi SSE events phản hồi thời gian thực."""
    # 1. Bắt đầu phiên
    yield format_sse_event("start", {"user_id": user_id, "query": message})

    intent = agent.detect_intent(message)

    if intent in ("recommendation", "search"):
        # Gọi Tool 1
        yield format_sse_event("tool_start", {"tool": "fetch_user_movie_profile", "args": {"user_id": user_id}})
        profile = agent.mcp.call_tool("fetch_user_movie_profile", {"user_id": user_id})
        yield format_sse_event("tool_result", {"tool": "fetch_user_movie_profile", "result": profile})

        # Gọi Tool 2
        query_clean = re.sub(r"(gợi ý|cho tôi|phim|hãy|tìm|xem|về)", "", message, flags=re.IGNORECASE).strip()
        if not query_clean:
            query_clean = profile.get("favorite_genre", "Sci-Fi Action")

        yield format_sse_event("tool_start", {"tool": "search_movie_knowledge_base", "args": {"query": query_clean, "top_k": 5}})
        search_results = agent.mcp.call_tool(
            "search_movie_knowledge_base",
            {"query": query_clean, "top_k": 5, "genres": profile.get("favorite_genre")},
        )
        yield format_sse_event("tool_result", {"tool": "search_movie_knowledge_base", "count": len(search_results)})

        # Gọi Tool 3
        candidate_ids = [m["movie_id"] for m in search_results]
        yield format_sse_event("tool_start", {"tool": "predict_movie_ranking", "args": {"user_id": user_id, "candidate_count": len(candidate_ids)}})
        ranked = agent.mcp.call_tool("predict_movie_ranking", {"user_id": user_id, "candidate_ids": candidate_ids})
        yield format_sse_event("tool_result", {"tool": "predict_movie_ranking", "result": ranked[:top_k]})

        # Ghép kết quả
        movie_map = {m["movie_id"]: m for m in search_results}
        top_movies = []
        for item in ranked[:top_k]:
            mid = item["movie_id"]
            if mid in movie_map:
                entry = dict(movie_map[mid])
                entry["ranking_score"] = item["ranking_score"]
                entry["rank"] = item["rank"]
                top_movies.append(entry)

        reply_full = agent._synthesize_response(user_id, message, profile, top_movies)
    else:
        top_movies = []
        reply_full = (
            "Xin chào! Tôi là CineBot - trợ lý xem phim của bạn tại CineStream. "
            "Bạn có thể yêu cầu tôi gợi ý phim theo cốt truyện, thể loại hoặc diễn viên yêu thích nhé!"
        )

    # 2. Truyền stream tokens đến UI
    words = reply_full.split(" ")
    for idx, word in enumerate(words):
        token = word if idx == 0 else " " + word
        yield format_sse_event("token", {"token": token})
        if chunk_delay > 0:
            time.sleep(chunk_delay)

    # 3. Hoàn tất
    yield format_sse_event(
        "done",
        {
            "status": "completed",
            "user_id": user_id,
            "total_words": len(words),
            "recommended_movies": top_movies,
        },
    )
