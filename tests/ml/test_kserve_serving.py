"""Kiểm thử KServe v2 Data Plane Serving Service và Redis Cache-Aside."""

from __future__ import annotations

import json
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

from serving.ml_serving.app import (
    MLServingHandler,
    cache,
    handle_kserve_infer,
    handle_kserve_live,
    handle_kserve_model_load,
    handle_kserve_model_metadata,
    handle_kserve_ready,
    handle_kserve_server_metadata,
    handle_recommend,
    registry,
)


class TestKServeServing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        registry.artifact_dir = Path("artifacts/models").resolve()
        registry.load_all_available()

        # Đảm bảo có sẵn model trong registry để test độc lập
        from ml.training.estimator import LightweightEstimator
        import numpy as np

        if "churn" not in registry.models:
            m = LightweightEstimator("churn", ["user_total_ratings_90d", "user_avg_rating_90d", "user_total_watch_hours_30d"])
            m.fit(np.zeros((10, 3)), np.zeros(10))
            registry.models["churn"] = m

        if "recommendation" not in registry.models:
            m = LightweightEstimator("recommendation", ["f1", "f2", "f3", "f4", "f5", "f6", "f7"])
            m.fit(np.zeros((10, 7)), np.zeros(10))
            registry.models["recommendation"] = m

    def setUp(self):
        # Dọn dẹp cache trước mỗi test
        cache._memory_store.clear()

    def test_health_and_metadata(self):
        status, live = handle_kserve_live()
        self.assertEqual(status, 200)
        self.assertTrue(live["live"])

        status, ready = handle_kserve_ready()
        self.assertEqual(status, 200)
        self.assertTrue(ready["ready"])

        status, meta = handle_kserve_server_metadata()
        self.assertEqual(status, 200)
        self.assertEqual(meta["name"], "cinestream-ml-serving")

    def test_model_metadata(self):
        status, churn_meta = handle_kserve_model_metadata("churn")
        self.assertEqual(status, 200)
        self.assertEqual(churn_meta["name"], "churn")
        self.assertIn("inputs", churn_meta)

        status, rec_meta = handle_kserve_model_metadata("recommendation")
        self.assertEqual(status, 200)
        self.assertEqual(rec_meta["name"], "recommendation")

    def test_churn_inference(self):
        req = {
            "id": "req-101",
            "inputs": [
                {
                    "name": "features",
                    "shape": [2, 3],
                    "datatype": "FP32",
                    "data": [
                        [10.0, 4.0, 25.0],
                        [0.0, 0.0, 0.1],
                    ],
                }
            ],
        }
        status, resp = handle_kserve_infer("churn", req)
        self.assertEqual(status, 200)
        self.assertEqual(resp["model_name"], "churn")
        self.assertEqual(resp["id"], "req-101")
        self.assertEqual(len(resp["outputs"]), 2)  # probabilities and predictions
        self.assertEqual(len(resp["outputs"][0]["data"]), 2)

    def test_recommendation_cache_aside_flow(self):
        user_id = 999

        # Lần 1: Cache Miss
        status1, resp1 = handle_recommend(user_id, candidate_movie_ids=[10, 20, 30], top_k=2)
        self.assertEqual(status1, 200)
        self.assertFalse(resp1["cache_hit"])
        self.assertEqual(len(resp1["recommendations"]), 2)

        # Lần 2: Cache Hit
        status2, resp2 = handle_recommend(user_id, candidate_movie_ids=[10, 20, 30], top_k=2)
        self.assertEqual(status2, 200)
        self.assertTrue(resp2["cache_hit"])
        self.assertEqual(resp1["recommendations"], resp2["recommendations"])

    def test_dynamic_model_reload(self):
        churn_file = registry.artifact_dir / "churn_model.joblib"
        if not churn_file.exists():
            import pickle
            registry.artifact_dir.mkdir(parents=True, exist_ok=True)
            with open(churn_file, "wb") as f:
                pickle.dump(registry.models["churn"], f)

        status, res = handle_kserve_model_load("churn")
        self.assertEqual(status, 200)
        self.assertIn("thành công", res["message"])

    def test_http_server_handler_simulation(self):
        """Kiểm thử toàn diện HTTP handler (GET/POST) qua BytesIO in-memory không cần socket."""
        import io
        from unittest.mock import MagicMock

        # 1. Test GET /v2/health/live
        handler = MLServingHandler.__new__(MLServingHandler)
        handler.path = "/v2/health/live"
        handler.headers = {}
        handler.wfile = io.BytesIO()
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        handler.do_GET()
        handler.send_response.assert_called_with(200)
        resp_data = json.loads(handler.wfile.getvalue().decode())
        self.assertTrue(resp_data["live"])

        # 2. Test GET /recommend/77?top_k=3
        handler.path = "/recommend/77?top_k=3"
        handler.wfile = io.BytesIO()
        handler.do_GET()
        resp_data = json.loads(handler.wfile.getvalue().decode())
        self.assertEqual(resp_data["user_id"], 77)
        self.assertEqual(len(resp_data["recommendations"]), 3)
        self.assertFalse(resp_data["cache_hit"])

        # 3. Test POST /v2/models/churn/infer
        payload = json.dumps({"inputs": [{"name": "feat", "data": [[5.0, 3.5, 12.0]]}]}).encode()
        handler.path = "/v2/models/churn/infer"
        handler.headers = {"Content-Length": str(len(payload))}
        handler.rfile = io.BytesIO(payload)
        handler.wfile = io.BytesIO()
        handler.do_POST()
        resp_data = json.loads(handler.wfile.getvalue().decode())
        self.assertEqual(resp_data["model_name"], "churn")
        self.assertIn("outputs", resp_data)


if __name__ == "__main__":
    unittest.main()
