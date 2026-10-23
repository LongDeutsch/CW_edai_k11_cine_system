"""Phục vụ dashboard và proxy cùng origin tới FastAPI.

Không mở ClickHouse. Upstream mặc định là http://127.0.0.1:18083.
"""

from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent / "static"
ALLOWED = ("analytics/", "movies/", "serving/", "health", "ready", "governance/")


def main() -> None:
    upstream = os.environ.get("CINESTREAM_API_URL", "http://127.0.0.1:18083").rstrip("/")
    port = int(os.environ.get("CINESTREAM_DASHBOARD_PORT", "18084"))
    handler = lambda *args, **kwargs: DashboardHandler(*args, upstream=upstream, **kwargs)
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    print(f"dashboard http://127.0.0.1:{port} -> {upstream}", flush=True)
    server.serve_forever()


class DashboardHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, upstream: str, **kwargs):
        self.upstream = upstream
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path.startswith("/api/"):
            self._proxy()
            return
        self._file("/index.html" if path == "/" else path)

    def log_message(self, fmt: str, *args) -> None:
        print(f"dashboard {self.address_string()} {fmt % args}", flush=True)

    def _file(self, path: str) -> None:
        relative = path.lstrip("/")
        if not relative or ".." in relative.split("/"):
            self.send_error(404)
            return
        target = (ROOT / relative).resolve()
        if ROOT.resolve() not in target.parents and target != ROOT.resolve():
            self.send_error(404)
            return
        if not target.is_file():
            self.send_error(404)
            return
        kind = "text/html; charset=utf-8"
        if target.suffix == ".css":
            kind = "text/css; charset=utf-8"
        elif target.suffix == ".js":
            kind = "text/javascript; charset=utf-8"
        body = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _proxy(self) -> None:
        split = urlsplit(self.path)
        upstream_path = split.path[len("/api/") :]
        if not upstream_path.startswith(ALLOWED):
            self.send_error(404)
            return
        query = f"?{split.query}" if split.query else ""
        request = Request(f"{self.upstream}/{upstream_path}{query}", headers={"accept": "application/json"})
        try:
            with urlopen(request, timeout=25) as response:
                body = response.read()
                self.send_response(response.status)
                self.send_header("Content-Type", response.headers.get("Content-Type", "application/json"))
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        except HTTPError as exc:
            body = exc.read()
            self.send_response(exc.code)
            self.send_header("Content-Type", exc.headers.get("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except URLError as exc:
            message = f'{{"detail":"không tới được FastAPI: {exc.reason}"}}'.encode()
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(message)))
            self.end_headers()
            self.wfile.write(message)


if __name__ == "__main__":
    main()
