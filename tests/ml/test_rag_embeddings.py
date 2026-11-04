"""Kiểm thử pipeline RAG Vector Embeddings và Qdrant indexing."""

from __future__ import annotations

import json
import math
import unittest
from unittest.mock import MagicMock, patch

from transformation.rag.build_movie_embeddings import (
    EMBEDDING_DIM,
    build_and_index_movies,
    create_qdrant_collection,
    deterministic_embedding,
    generate_embedding,
    load_seed_movies,
    search_similar_movies,
    upsert_points,
)


class TestRAGEmbeddings(unittest.TestCase):
    def test_deterministic_embedding_dimension_and_norm(self):
        vec = deterministic_embedding("A mind-bending sci-fi heist movie", dim=EMBEDDING_DIM)
        self.assertEqual(len(vec), EMBEDDING_DIM)
        norm = math.sqrt(sum(x * x for x in vec))
        self.assertAlmostEqual(norm, 1.0, places=4)

    def test_deterministic_embedding_consistency(self):
        vec1 = deterministic_embedding("Inception Christopher Nolan")
        vec2 = deterministic_embedding("Inception Christopher Nolan")
        vec3 = deterministic_embedding("Toy Story Pixar Animation")
        self.assertEqual(vec1, vec2)
        self.assertNotEqual(vec1, vec3)

    def test_load_seed_movies(self):
        movies = load_seed_movies()
        self.assertGreaterEqual(len(movies), 200)
        # Kiểm tra movie_id 42 hoặc phim đầu tiên có overview và genres
        first_movie = movies[0]
        self.assertIn("movie_id", first_movie)
        self.assertIn("overview", first_movie)
        self.assertIn("genres", first_movie)
        self.assertTrue(len(first_movie["overview"]) > 10)

    @patch("urllib.request.urlopen")
    def test_create_qdrant_collection(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"result": True, "status": "ok"}).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        res = create_qdrant_collection("test_movies", qdrant_url="http://127.0.0.1:6333")
        self.assertEqual(res.get("status"), "ok")

    @patch("urllib.request.urlopen")
    def test_upsert_and_search_points(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(
            {
                "result": [
                    {
                        "id": 42,
                        "score": 0.89,
                        "payload": {"title": "Inception", "genres": "Action|Sci-Fi"},
                    }
                ],
                "status": "ok",
            }
        ).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        points = [
            {
                "id": 42,
                "vector": deterministic_embedding("Inception"),
                "payload": {"title": "Inception", "movie_id": 42},
            }
        ]
        upsert_res = upsert_points(points, collection_name="test_movies")
        self.assertEqual(upsert_res.get("status"), "ok")

        search_res = search_similar_movies("dream heist", top_k=1, collection_name="test_movies")
        self.assertEqual(len(search_res), 1)
        self.assertEqual(search_res[0]["id"], 42)


if __name__ == "__main__":
    unittest.main()
