"""FastMCP package for CineStream LLM Concierge."""

from llm.mcp.server import (
    fetch_user_movie_profile,
    mcp,
    predict_movie_ranking,
    search_movie_knowledge_base,
)

__all__ = [
    "mcp",
    "fetch_user_movie_profile",
    "search_movie_knowledge_base",
    "predict_movie_ranking",
]
