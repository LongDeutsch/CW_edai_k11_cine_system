"""KServe v2 Data Plane ML Model Serving Service với Dynamic Model Loading & Cache-Aside Redis.

Hỗ trợ chuẩn:
- KServe v2 Open Inference Protocol:
  - GET /v2/health/live
  - GET /v2/health/ready
  - GET /v2
  - GET /v2/models/{model_name}
  - GET /v2/models/{model_name}/ready
  - POST /v2/models/{model_name}/infer
  - POST /v2/repository/models/{model_name}/load (Dynamic Model Loading từ MinIO)
- Recommendation Service với Cache-Aside pattern (Redis port 16379, TTL 300s):
  - POST /v2/models/recommendation/recommend hoặc GET /recommend/{user_id}
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import numpy as np

# Fallback serializer tương thích joblib nếu joblib chưa cài đặt
import pickle


class _CustomUnpickler(pickle.Unpickler):
    def find_class(self, module: str, name: str) -> Any:
        if name == "LightweightEstimator":
            from ml.training.estimator import LightweightEstimator
            return LightweightEstimator
        return super().find_class(module, name)


try:
    import joblib
except ImportError:
    class _JoblibFallback:
        @staticmethod
        def load(filename: str | Path) -> Any:
            with open(filename, "rb") as f:
                return _CustomUnpickler(f).load()

    joblib = _JoblibFallback()  # type: ignore[assignment]


# ==============================================================================
# 1. Trình quản lý Cache (Redis Client & In-Memory Fallback)
# ==============================================================================

class CacheClient:
    """Quản lý kết nối Redis hỗ trợ TTL và Fallback bộ nhớ nếu Redis offline."""

    def __init__(self, host: str = "127.0.0.1", port: int = 16379, db: int = 1) -> None:
        self.host = host
        self.port = port
        self.db = db
        self._redis = None
        self._memory_store: dict[str, tuple[str, float]] = {}  # key -> (value, expire_at)
        self._init_client()

    def _init_client(self) -> None:
        try:
            import redis

            r = redis.Redis(host=self.host, port=self.port, db=self.db, socket_connect_timeout=1.0)
            r.ping()
            self._redis = r
        except Exception:
            self._redis = None

    def get(self, key: str) -> str | None:
        if self._redis is not None:
            try:
                val = self._redis.get(key)
                return val.decode("utf-8") if isinstance(val, bytes) else val
            except Exception:
                pass
        # Fallback in-memory
        if key in self._memory_store:
            val, expire_at = self._memory_store[key]
            if expire_at == 0 or time.time() < expire_at:
                return val
            del self._memory_store[key]
        return None

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        if self._redis is not None:
            try:
                self._redis.set(key, value, ex=ex)
                return
            except Exception:
                pass
        # Fallback in-memory
        expire_at = (time.time() + ex) if ex else 0
        self._memory_store[key] = (value, expire_at)

    def delete(self, key: str) -> None:
        if self._redis is not None:
            try:
                self._redis.delete(key)
            except Exception:
                pass
        self._memory_store.pop(key, None)

    def is_connected_to_real_redis(self) -> bool:
        return self._redis is not None


# ==============================================================================
# 2. Dynamic Model Registry & MinIO Downloader
# ==============================================================================

class ModelRegistry:
    """Quản lý vòng đời mô hình động từ MinIO S3 hoặc đĩa local."""

    def __init__(self, artifact_dir: str | Path = "artifacts/models") -> None:
        self.artifact_dir = Path(artifact_dir)
        self.models: dict[str, Any] = {}
        self.metadata: dict[str, Any] = {}

    def download_from_minio(self, s3_key: str, local_path: Path, bucket: str = "lakehouse") -> bool:
        venv_site = Path(__file__).resolve().parents[2] / ".venv" / "lib" / "python3.13" / "site-packages"
        if venv_site.exists() and str(venv_site) not in sys.path:
            sys.path.insert(0, str(venv_site))

        endpoint = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

        try:
            import boto3
            from botocore.client import Config

            client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, connect_timeout=1, read_timeout=1, retries={"max_attempts": 0}),
            )
            local_path.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, s3_key, str(local_path))
            return True
        except Exception:
            return False

    def load_model(self, model_name: str, force_reload: bool = False) -> Any:
        if not force_reload and model_name in self.models:
            return self.models[model_name]

        filename = f"{model_name}_model.joblib"
        local_path = self.artifact_dir / filename

        # Thử tải từ MinIO trước
        self.download_from_minio(f"models/{filename}", local_path)

        if not local_path.exists():
            raise FileNotFoundError(f"Không tìm thấy artifact cho mô hình {model_name} tại {local_path}")

        model = joblib.load(local_path)
        self.models[model_name] = model

        # Nạp metadata nếu có
        meta_path = self.artifact_dir / "metadata.json"
        if meta_path.exists():
            try:
                self.metadata = json.loads(meta_path.read_text())
            except Exception:
                pass

        return model

    def load_all_available(self) -> None:
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        for name in ("churn", "recommendation"):
            try:
                self.load_model(name)
            except Exception:
                pass


registry = ModelRegistry()
cache = CacheClient(
    host=os.environ.get("REDIS_HOST", "127.0.0.1"),
    port=int(os.environ.get("REDIS_PORT", "16379")),
    db=int(os.environ.get("REDIS_DB", "1")),
)


# ==============================================================================
# 3. Logic nghiệp vụ Xử lý Request KServe v2 & Recommendation
# ==============================================================================

def handle_kserve_live() -> tuple[int, dict[str, Any]]:
    return 200, {"live": True}


def handle_kserve_ready() -> tuple[int, dict[str, Any]]:
    return 200, {"ready": len(registry.models) > 0}


def handle_kserve_server_metadata() -> tuple[int, dict[str, Any]]:
    return 200, {
        "name": "cinestream-ml-serving",
        "version": "1.0.0",
        "extensions": ["kserve_v2", "cache_aside"],
    }


def handle_kserve_model_metadata(model_name: str) -> tuple[int, dict[str, Any]]:
    if model_name not in registry.models:
        try:
            registry.load_model(model_name)
        except Exception:
            return 404, {"error": f"Mô hình '{model_name}' chưa được nạp."}

    model = registry.models[model_name]
    feature_names = getattr(model, "feature_names_in_", [])

    return 200, {
        "name": model_name,
        "versions": ["v1"],
        "platform": type(model).__name__,
        "inputs": [
            {
                "name": "features",
                "datatype": "FP32",
                "shape": [-1, len(feature_names)],
                "feature_names": list(feature_names),
            }
        ],
        "outputs": [
            {"name": "probabilities", "datatype": "FP32", "shape": [-1, 2]},
            {"name": "predictions", "datatype": "INT32", "shape": [-1]},
        ],
    }


def handle_kserve_model_ready(model_name: str) -> tuple[int, dict[str, Any]]:
    return 200, {"ready": model_name in registry.models}


def handle_kserve_model_load(model_name: str) -> tuple[int, dict[str, Any]]:
    try:
        registry.load_model(model_name, force_reload=True)
        return 200, {"status": "ok", "message": f"Mô hình {model_name} đã được tải động thành công."}
    except Exception as exc:
        return 500, {"error": str(exc)}


def handle_kserve_infer(model_name: str, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    if model_name not in registry.models:
        try:
            registry.load_model(model_name)
        except Exception:
            return 404, {"error": f"Mô hình '{model_name}' không tồn tại hoặc chưa sẵn sàng."}

    model = registry.models[model_name]
    inputs = payload.get("inputs", [])
    if not inputs:
        return 400, {"error": "Thiếu tensor inputs."}

    input_tensor = inputs[0]
    data = np.asarray(input_tensor.get("data", []), dtype=np.float64)
    if data.ndim == 1:
        data = data.reshape(1, -1)

    if hasattr(model, "predict_proba"):
        probs = model.predict_proba(data)
    else:
        probs = np.column_stack([np.zeros(len(data)), np.ones(len(data))])

    if hasattr(model, "predict"):
        preds = model.predict(data)
    else:
        preds = (probs[:, 1] >= 0.5).astype(int)

    outputs = [
        {
            "name": "probabilities",
            "shape": list(probs.shape),
            "datatype": "FP32",
            "data": [[round(float(p), 4) for p in row] for row in probs],
        },
        {
            "name": "predictions",
            "shape": [len(preds)],
            "datatype": "INT32",
            "data": [int(p) for p in preds],
        },
    ]

    return 200, {
        "model_name": model_name,
        "model_version": "v1",
        "id": payload.get("id"),
        "outputs": outputs,
    }


def handle_recommend(
    user_id: int,
    candidate_movie_ids: list[int] | None = None,
    top_k: int = 5,
) -> tuple[int, dict[str, Any]]:
    """Gợi ý phim cho người dùng với Cache-Aside Redis."""
    cache_key = f"rec:user:{user_id}"

    # 1. Cache Hit
    cached_payload = cache.get(cache_key)
    if cached_payload:
        try:
            cached_data = json.loads(cached_payload)
            items = cached_data.get("items", [])
            return 200, {
                "user_id": user_id,
                "recommendations": items[:top_k],
                "cache_hit": True,
                "model_version": "v1",
            }
        except Exception:
            pass

    # 2. Cache Miss: Tính toán từ mô hình
    if "recommendation" not in registry.models:
        try:
            registry.load_model("recommendation")
        except Exception:
            return 503, {"error": "Mô hình recommendation chưa được nạp."}

    rec_model = registry.models["recommendation"]
    candidates = candidate_movie_ids or list(range(1, 21))

    features_list = []
    for m in candidates:
        row = [5.0, 4.0, 10.0, 20.0, 4.2, 0.75, float(m % 10)]
        features_list.append(row)

    feature_matrix = np.asarray(features_list, dtype=np.float64)
    probs = rec_model.predict_proba(feature_matrix)

    scored_items = []
    for idx, m in enumerate(candidates):
        score = round(float(probs[idx, 1]), 4)
        scored_items.append({"movie_id": m, "score": score})

    scored_items.sort(key=lambda it: it["score"], reverse=True)
    top_items = scored_items[:top_k]

    # 3. Lưu vào Cache (TTL 300s)
    cache_to_store = {
        "user_id": user_id,
        "items": top_items,
        "cached_at": datetime.now(timezone.utc).isoformat(),
    }
    cache.set(cache_key, json.dumps(cache_to_store), ex=300)

    return 200, {
        "user_id": user_id,
        "recommendations": top_items,
        "cache_hit": False,
        "model_version": "v1",
    }


# ==============================================================================
# 4. HTTP Server Handler (Tương thích mọi môi trường không cần dependency ngoài)
# ==============================================================================

class MLServingHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass  # Giảm noise log

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path.rstrip("/")
        query = parse_qs(parsed.query)

        if path == "/v2/health/live":
            status, resp = handle_kserve_live()
        elif path == "/v2/health/ready":
            status, resp = handle_kserve_ready()
        elif path == "/v2":
            status, resp = handle_kserve_server_metadata()
        elif path.startswith("/v2/models/") and path.endswith("/ready"):
            model_name = path.split("/")[3]
            status, resp = handle_kserve_model_ready(model_name)
        elif path.startswith("/v2/models/"):
            model_name = path.split("/")[3]
            status, resp = handle_kserve_model_metadata(model_name)
        elif path.startswith("/recommend/"):
            try:
                user_id = int(path.split("/")[2])
                top_k = int(query.get("top_k", [5])[0])
                cands = [int(x) for x in query.get("candidates", [""])[0].split(",") if x] or None
                status, resp = handle_recommend(user_id, candidate_movie_ids=cands, top_k=top_k)
            except Exception as e:
                status, resp = 400, {"error": str(e)}
        else:
            status, resp = 404, {"error": "Not found"}

        self._send_json(status, resp)

    def do_POST(self) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path.rstrip("/")

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            payload = {}

        if path.startswith("/v2/models/") and path.endswith("/infer"):
            model_name = path.split("/")[3]
            status, resp = handle_kserve_infer(model_name, payload)
        elif path.startswith("/v2/repository/models/") and path.endswith("/load"):
            model_name = path.split("/")[4]
            status, resp = handle_kserve_model_load(model_name)
        elif path in ("/v2/models/recommendation/recommend", "/recommend"):
            user_id = int(payload.get("user_id", 1))
            top_k = int(payload.get("top_k", 5))
            cands = payload.get("candidate_movie_ids")
            status, resp = handle_recommend(user_id, candidate_movie_ids=cands, top_k=top_k)
        else:
            status, resp = 404, {"error": "Not found"}

        self._send_json(status, resp)

    def _send_json(self, status: int, data: dict[str, Any]) -> None:
        raw = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def run_server(port: int = 18086, host: str = "127.0.0.1") -> None:
    registry.load_all_available()
    server = ThreadingHTTPServer((host, port), MLServingHandler)
    print(f"ML Serving KServe v2 listening on http://{host}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    port = int(os.environ.get("ML_SERVING_PORT", "18086"))
    run_server(port=port)
