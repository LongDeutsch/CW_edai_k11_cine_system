"""Client chat gửi đúng tên model từ env và không nhận tên model làm tham số."""

from __future__ import annotations

import importlib
import json
import os
import re
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("OLLAMA_BASE_URL", "http://127.0.0.1:9")
os.environ.setdefault("OLLAMA_MODEL", "env-model")

from llm import config
from llm.client import chat

REPO = Path(__file__).resolve().parents[2]
LIBRARY_NAMES = ("qwen", "mistral", "nomic-embed")


class _Response:
    def __init__(self, payload: dict) -> None:
        self._raw = json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._raw

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_) -> None:
        return None


class ChatClientTest(unittest.TestCase):
    def test_chat_posts_model_from_env(self) -> None:
        os.environ["OLLAMA_BASE_URL"] = "http://127.0.0.1:9"
        os.environ["OLLAMA_MODEL"] = "env-model"
        os.environ["OLLAMA_EMBED_MODEL"] = "env-embed"
        importlib.reload(config)
        seen: dict = {}

        def capture(request, timeout):
            seen["url"] = request.full_url
            seen["body"] = json.loads(request.data.decode())
            seen["timeout"] = timeout
            return _Response({"model": "env-model", "message": {"role": "assistant", "content": "ok"}})

        with patch.object(urllib.request, "urlopen", capture):
            reply = chat([{"role": "user", "content": "ping"}], timeout=3)

        self.assertEqual(seen["url"], "http://127.0.0.1:9/api/chat")
        self.assertEqual(seen["body"]["model"], "env-model")
        self.assertNotIn("env-embed", json.dumps(seen["body"]))
        self.assertFalse(seen["body"]["stream"])
        self.assertEqual(seen["timeout"], 3)
        self.assertEqual(reply["model"], "env-model")

    def test_chat_rejects_model_argument(self) -> None:
        with self.assertRaises(TypeError):
            chat([{"role": "user", "content": "ping"}], model="other")  # type: ignore[call-arg]

    def test_missing_model_stops(self) -> None:
        os.environ["OLLAMA_BASE_URL"] = "http://127.0.0.1:9"
        os.environ.pop("OLLAMA_MODEL", None)
        with self.assertRaises(RuntimeError):
            importlib.reload(config)
        os.environ["OLLAMA_MODEL"] = "env-model"
        importlib.reload(config)

    def test_sources_do_not_name_a_library_model(self) -> None:
        paths = [
            REPO / "llm" / "config.py",
            REPO / "llm" / "client.py",
            REPO / "infra" / "compose" / "docker-compose.ollama.yml",
            REPO / "make" / "llm.mk",
        ]
        blob = "\n".join(path.read_text().lower() for path in paths)
        for name in LIBRARY_NAMES:
            self.assertNotIn(name, blob)
        self.assertIsNone(re.search(r"(?<!o)llama", blob))
        self.assertNotIn("11434", (REPO / "llm" / "client.py").read_text())
        self.assertNotIn("11434", (REPO / "llm" / "config.py").read_text())


if __name__ == "__main__":
    unittest.main()
