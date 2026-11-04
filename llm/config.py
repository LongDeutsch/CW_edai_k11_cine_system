"""Đọc cấu hình Ollama lúc import. Thiếu biến thì dừng, không đoán tên model."""

from __future__ import annotations

import os


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"thiếu {name}")
    return value


def load() -> None:
    global BASE_URL, MODEL, EMBED_MODEL
    BASE_URL = _required("OLLAMA_BASE_URL").rstrip("/")
    MODEL = _required("OLLAMA_MODEL")
    embed = os.environ.get("OLLAMA_EMBED_MODEL", "").strip()
    EMBED_MODEL = embed or None


BASE_URL: str
MODEL: str
EMBED_MODEL: str | None

load()
