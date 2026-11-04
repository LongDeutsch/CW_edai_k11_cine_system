"""Chỗ duy nhất gửi chat tới Ollama. Tên model lấy từ llm.config, không nhận tham số."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from llm import config


def chat(messages: list[dict[str, str]], *, timeout: float = 120) -> dict:
    """POST /api/chat. Không stream. Trả JSON Ollama đã parse."""
    if not messages:
        raise ValueError("messages rỗng")
    body = json.dumps(
        {"model": config.MODEL, "messages": messages, "stream": False},
        ensure_ascii=False,
    ).encode()
    request = urllib.request.Request(
        f"{config.BASE_URL}/api/chat",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Ollama HTTP {exc.code}: {detail}") from exc


def main() -> None:
    reply = chat([{"role": "user", "content": "ok"}])
    returned = str(reply.get("model") or "")
    print(f"requested={config.MODEL}")
    print(f"returned={returned}")
    if returned != config.MODEL:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
