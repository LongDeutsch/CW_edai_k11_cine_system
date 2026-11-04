"""HTTP Server cho CineBot Concierge với FastMCP Tools và SSE Streaming.

Cổng mặc định: 18088 (biến môi trường CINESTREAM_CONCIERGE_PORT).
"""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from llm.agent import agent
from llm.mcp.server import mcp
from llm.streaming import stream_agent_events


class ConciergeHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass  # Giảm noise log

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path.rstrip("/")
        query = parse_qs(parsed.query)

        if path == "/health":
            self._send_json(200, {"status": "ok", "service": "cinestream-llm-concierge"})
        elif path == "/mcp/tools":
            self._send_json(200, {"tools": mcp.list_tools()})
        elif path == "/api/chat/stream":
            message = query.get("message", ["Gợi ý phim hay"])[0]
            user_id = int(query.get("user_id", [1])[0])
            self._handle_sse_stream(message, user_id)
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path.rstrip("/")

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            payload = {}

        if path == "/api/chat":
            message = str(payload.get("message", "Gợi ý phim"))
            user_id = int(payload.get("user_id", 1))
            res = agent.run_flow(message=message, user_id=user_id)
            self._send_json(200, res)
        elif path == "/api/chat/stream":
            message = str(payload.get("message", "Gợi ý phim"))
            user_id = int(payload.get("user_id", 1))
            self._handle_sse_stream(message, user_id)
        else:
            self._send_json(404, {"error": "Not found"})

    def _handle_sse_stream(self, message: str, user_id: int) -> None:
        """Gửi luồng phản hồi Server-Sent Events (SSE)."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        for chunk in stream_agent_events(message=message, user_id=user_id, chunk_delay=0.0):
            try:
                self.wfile.write(chunk.encode("utf-8"))
                self.wfile.flush()
            except Exception:
                break

    def _send_json(self, status: int, data: dict[str, Any]) -> None:
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(raw)


def run_concierge_server(port: int = 18088, host: str = "127.0.0.1") -> None:
    server = ThreadingHTTPServer((host, port), ConciergeHandler)
    print(f"CineBot Concierge Server listening on http://{host}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    port = int(os.environ.get("CINESTREAM_CONCIERGE_PORT", "18088"))
    run_concierge_server(port=port)
