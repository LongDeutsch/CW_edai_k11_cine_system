"""Kiểm thử Server-Sent Events (SSE) Streaming và Concierge HTTP Server."""

from __future__ import annotations

import io
import json
import unittest
from unittest.mock import MagicMock

from llm.server import ConciergeHandler
from llm.streaming import stream_agent_events


class TestSSEStreaming(unittest.TestCase):
    def test_stream_agent_events_structure(self):
        chunks = list(stream_agent_events("Gợi ý phim trinh thám", user_id=1, top_k=2, chunk_delay=0.0))
        full_text = "".join(chunks)

        self.assertIn("event: start", full_text)
        self.assertIn("event: tool_start", full_text)
        self.assertIn("event: tool_result", full_text)
        self.assertIn("event: token", full_text)
        self.assertIn("event: done", full_text)

    def test_concierge_handler_health_and_tools(self):
        handler = ConciergeHandler.__new__(ConciergeHandler)
        handler.headers = {}
        handler.wfile = io.BytesIO()
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        # 1. Health check
        handler.path = "/health"
        handler.do_GET()
        data = json.loads(handler.wfile.getvalue().decode())
        self.assertEqual(data["status"], "ok")

        # 2. MCP Tools schema
        handler.path = "/mcp/tools"
        handler.wfile = io.BytesIO()
        handler.do_GET()
        data = json.loads(handler.wfile.getvalue().decode())
        self.assertIn("tools", data)
        self.assertEqual(len(data["tools"]), 3)

    def test_concierge_handler_sse_streaming(self):
        handler = ConciergeHandler.__new__(ConciergeHandler)
        handler.headers = {}
        handler.wfile = io.BytesIO()
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.path = "/api/chat/stream?message=G%E1%BB%A3i+%C3%BD+phim&user_id=1"
        handler.do_GET()

        stream_output = handler.wfile.getvalue().decode()
        self.assertIn("event: start", stream_output)
        self.assertIn("event: token", stream_output)
        self.assertIn("event: done", stream_output)


if __name__ == "__main__":
    unittest.main()
