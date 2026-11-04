"""Tạo vector embeddings cho tóm tắt phim và lập chỉ mục HNSW trên Qdrant.

Sử dụng Ollama (nomic-embed-text) để tạo vector embedding 768 chiều.
Gửi trực tiếp qua HTTP REST API tới Qdrant (cổng 6333) để thiết lập chỉ mục
HNSW với Cosine similarity metric.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
DEFAULT_EMBED_MODEL = os.environ.get("OLLAMA_EMBED_MODEL", "nomic-embed-text")
DEFAULT_QDRANT_URL = os.environ.get("QDRANT_URL", "http://127.0.0.1:16333")
DEFAULT_COLLECTION_NAME = os.environ.get("QDRANT_COLLECTION", "movie_knowledge_base")
EMBEDDING_DIM = 768


def deterministic_embedding(text: str, dim: int = EMBEDDING_DIM) -> list[float]:
    """Sinh vector embedding xác định từ hash chuỗi khi Ollama offline.

    Chuẩn hóa L2 norm = 1.0 đảm bảo tương thích với độ đo Cosine.
    """
    if not text:
        return [0.0] * dim
    vec: list[float] = []
    text_bytes = text.encode("utf-8")
    for i in range(dim):
        h = hashlib.sha256(text_bytes + str(i).encode()).digest()
        # Biến đổi 4 byte thành float trong khoảng [-1.0, 1.0]
        val = (int.from_bytes(h[:4], "little") / 0xFFFFFFFF) * 2.0 - 1.0
        vec.append(val)
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [round(x / norm, 6) for x in vec]


def generate_embedding(
    text: str,
    *,
    base_url: str = DEFAULT_OLLAMA_BASE_URL,
    model: str = DEFAULT_EMBED_MODEL,
    fallback_deterministic: bool = True,
    timeout: float = 30.0,
) -> list[float]:
    """Gọi Ollama /api/embeddings để sinh vector cho văn bản."""
    clean_text = (text or "").strip()
    if not clean_text:
        return [0.0] * EMBEDDING_DIM

    payload = json.dumps({"model": model, "prompt": clean_text}).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/embeddings",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
            embedding = data.get("embedding", [])
            if embedding and len(embedding) == EMBEDDING_DIM:
                return embedding
            if embedding:
                # Nếu model trả về chiều khác, điều chỉnh về EMBEDDING_DIM
                if len(embedding) < EMBEDDING_DIM:
                    return embedding + [0.0] * (EMBEDDING_DIM - len(embedding))
                return embedding[:EMBEDDING_DIM]
    except Exception:
        if fallback_deterministic:
            return deterministic_embedding(clean_text, dim=EMBEDDING_DIM)
        raise

    if fallback_deterministic:
        return deterministic_embedding(clean_text, dim=EMBEDDING_DIM)
    return [0.0] * EMBEDDING_DIM


def create_qdrant_collection(
    collection_name: str = DEFAULT_COLLECTION_NAME,
    *,
    vector_size: int = EMBEDDING_DIM,
    qdrant_url: str = DEFAULT_QDRANT_URL,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """Tạo hoặc tạo lại collection trên Qdrant với chỉ mục HNSW Cosine."""
    payload = {
        "vectors": {
            "size": vector_size,
            "distance": "Cosine",
        },
        "hnsw_config": {
            "m": 16,
            "ef_construct": 100,
        },
    }
    url = f"{qdrant_url.rstrip('/')}/collections/{collection_name}"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def upsert_points(
    points: list[dict[str, Any]],
    collection_name: str = DEFAULT_COLLECTION_NAME,
    *,
    qdrant_url: str = DEFAULT_QDRANT_URL,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Upsert danh sách điểm vector và payload vào collection."""
    if not points:
        return {"status": "ok", "count": 0}

    url = f"{qdrant_url.rstrip('/')}/collections/{collection_name}/points?wait=true"
    payload = {"points": points}
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def search_similar_movies(
    query_text: str,
    top_k: int = 5,
    *,
    collection_name: str = DEFAULT_COLLECTION_NAME,
    qdrant_url: str = DEFAULT_QDRANT_URL,
    ollama_url: str = DEFAULT_OLLAMA_BASE_URL,
    embed_model: str = DEFAULT_EMBED_MODEL,
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    """Tìm kiếm phim tương đồng ngữ nghĩa từ Qdrant HNSW."""
    query_vec = generate_embedding(query_text, base_url=ollama_url, model=embed_model)
    url = f"{qdrant_url.rstrip('/')}/collections/{collection_name}/points/search"
    payload = {
        "vector": query_vec,
        "limit": top_k,
        "with_payload": True,
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        res = json.loads(resp.read().decode())
        return res.get("result", [])


def load_seed_movies(seed_path: str | Path | None = None) -> list[dict[str, Any]]:
    """Đọc danh sách phim seed có tóm tắt nội dung."""
    if seed_path is None:
        seed_path = Path(__file__).resolve().parents[2] / "data_generator" / "seeds" / "movie_synopses_seed.json"
    p = Path(seed_path)
    if not p.is_file():
        raise FileNotFoundError(f"Không tìm thấy seed movie synopses: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def build_and_index_movies(
    movies: list[dict[str, Any]] | None = None,
    *,
    collection_name: str = DEFAULT_COLLECTION_NAME,
    qdrant_url: str = DEFAULT_QDRANT_URL,
    ollama_url: str = DEFAULT_OLLAMA_BASE_URL,
    embed_model: str = DEFAULT_EMBED_MODEL,
    batch_size: int = 64,
) -> int:
    """Xây dựng embeddings và nạp toàn bộ vào Qdrant."""
    if movies is None:
        movies = load_seed_movies()

    # Lọc các phim có overview
    valid_movies = [m for m in movies if m.get("overview")]
    if not valid_movies:
        return 0

    create_qdrant_collection(collection_name, vector_size=EMBEDDING_DIM, qdrant_url=qdrant_url)

    points: list[dict[str, Any]] = []
    indexed_count = 0
    for movie in valid_movies:
        movie_id = int(movie["movie_id"])
        genres = movie.get("genres") or ""
        overview = movie.get("overview") or ""
        title = movie.get("title") or f"Movie #{movie_id}"

        # Ghép text giàu ngữ nghĩa cho embedding
        text_for_embedding = f"Title: {title}. Genres: {genres}. Plot: {overview}"
        vector = generate_embedding(text_for_embedding, base_url=ollama_url, model=embed_model)

        points.append(
            {
                "id": movie_id,
                "vector": vector,
                "payload": {
                    "movie_id": movie_id,
                    "title": title,
                    "genres": genres,
                    "overview": overview,
                },
            }
        )

        if len(points) >= batch_size:
            upsert_points(points, collection_name=collection_name, qdrant_url=qdrant_url)
            indexed_count += len(points)
            points = []

    if points:
        upsert_points(points, collection_name=collection_name, qdrant_url=qdrant_url)
        indexed_count += len(points)

    return indexed_count


if __name__ == "__main__":
    count = build_and_index_movies()
    print(f"Đã lập chỉ mục {count} phim vào Qdrant collection {DEFAULT_COLLECTION_NAME}")
