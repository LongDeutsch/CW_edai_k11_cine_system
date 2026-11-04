"""Huấn luyện mô hình cơ sở cho Recommendation và Churn Prediction.

- Recommendation: LightGBM Ranker / Classifier dự đoán xác suất tương tác tích cực.
- Churn Prediction: XGBoost Classifier dự đoán xác suất rời bỏ dịch vụ sau 14 ngày.
- Xuất artifacts định dạng .joblib và tải lên MinIO bucket s3://lakehouse/models/.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# Fallback serializer tương thích joblib nếu joblib chưa cài đặt
try:
    import joblib
except ImportError:
    import pickle

    class _JoblibFallback:
        @staticmethod
        def dump(value: Any, filename: str | Path) -> None:
            with open(filename, "wb") as f:
                pickle.dump(value, f)

        @staticmethod
        def load(filename: str | Path) -> Any:
            with open(filename, "rb") as f:
                return pickle.load(f)

    joblib = _JoblibFallback()  # type: ignore[assignment]


from ml.training.estimator import LightweightEstimator


def train_churn_model(
    train_df: pd.DataFrame,
    features: list[str] | None = None,
) -> tuple[Any, dict[str, float]]:
    """Huấn luyện mô hình dự đoán Churn (XGBoost hoặc fallback)."""
    if features is None:
        features = [
            "user_total_ratings_90d",
            "user_avg_rating_90d",
            "user_total_watch_hours_30d",
        ]

    X = train_df[features].fillna(0.0).values
    y = train_df["is_churned"].values

    try:
        import xgboost as xgb

        model = xgb.XGBClassifier(
            n_estimators=50,
            max_depth=4,
            learning_rate=0.08,
            random_state=42,
            eval_metric="logloss",
        )
        model.fit(X, y)
        model.feature_names_in_ = features
        framework = "xgboost"
    except ImportError:
        model = LightweightEstimator(name="churn_xgboost_fallback", feature_names=features)
        model.fit(X, y)
        framework = "lightweight_numpy"

    # Đánh giá metric cơ bản
    preds = model.predict(X)
    acc = float(np.mean(preds == y))
    metrics = {"accuracy": round(acc, 4), "samples": int(len(y)), "framework": framework}
    return model, metrics


def train_rec_model(
    train_df: pd.DataFrame,
    features: list[str] | None = None,
) -> tuple[Any, dict[str, float]]:
    """Huấn luyện mô hình xếp hạng / gợi ý (LightGBM hoặc fallback)."""
    if features is None:
        features = [
            "user_total_ratings_90d",
            "user_avg_rating_90d",
            "user_total_watch_hours_30d",
            "movie_rating_count_30d",
            "movie_avg_rating_30d",
            "movie_completion_rate_30d",
            "movie_trending_score_7d",
        ]

    X = train_df[features].fillna(0.0).values
    y = train_df["is_positive"].values

    try:
        import lightgbm as lgb

        model = lgb.LGBMClassifier(
            n_estimators=50,
            num_leaves=15,
            learning_rate=0.08,
            random_state=42,
        )
        model.fit(X, y)
        model.feature_names_in_ = features
        framework = "lightgbm"
    except ImportError:
        model = LightweightEstimator(name="rec_lightgbm_fallback", feature_names=features)
        model.fit(X, y)
        framework = "lightweight_numpy"

    preds = model.predict(X)
    acc = float(np.mean(preds == y))
    metrics = {"accuracy": round(acc, 4), "samples": int(len(y)), "framework": framework}
    return model, metrics


def upload_to_minio(
    local_path: Path,
    s3_key: str,
    bucket: str = "lakehouse",
    endpoint_url: str | None = None,
) -> bool:
    """Tải artifact lên MinIO bucket s3://lakehouse/models/."""
    # Hỗ trợ nạp boto3 từ venv nếu có
    venv_site = Path(__file__).resolve().parents[2] / ".venv" / "lib" / "python3.13" / "site-packages"
    if venv_site.exists() and str(venv_site) not in sys.path:
        sys.path.insert(0, str(venv_site))

    endpoint = endpoint_url or os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    try:
        import boto3
        from botocore.client import Config
        from botocore.exceptions import ClientError

        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, connect_timeout=2, read_timeout=2, retries={"max_attempts": 0}),
        )
        # Đảm bảo bucket tồn tại
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            try:
                client.create_bucket(Bucket=bucket)
            except Exception:
                pass

        client.upload_file(str(local_path), bucket, s3_key)
        return True
    except Exception as exc:
        print(f"[MinIO Upload Info] Chưa tải lên MinIO ({exc}), file đã lưu tại local {local_path}")
        return False


def run_training_pipeline(
    data_dir: str | Path = "data",
    artifact_dir: str | Path = "artifacts/models",
    upload_s3: bool = True,
) -> dict[str, Any]:
    """Quy trình toàn diện: đọc dataset, train 2 models, lưu .joblib và đẩy lên MinIO."""
    data_path = Path(data_dir)
    art_path = Path(artifact_dir)
    art_path.mkdir(parents=True, exist_ok=True)

    churn_file = data_path / "train_churn.parquet"
    rec_file = data_path / "train_rec.parquet"

    if not churn_file.exists() or not rec_file.exists():
        from ml.offline.build_training_sets import extract_and_export_training_sets
        extract_and_export_training_sets(output_dir=data_path)

    churn_df = pd.read_parquet(churn_file)
    rec_df = pd.read_parquet(rec_file)

    # 1. Train Churn
    churn_model, churn_metrics = train_churn_model(churn_df)
    churn_artifact = art_path / "churn_model.joblib"
    joblib.dump(churn_model, churn_artifact)

    # 2. Train Recommendation
    rec_model, rec_metrics = train_rec_model(rec_df)
    rec_artifact = art_path / "recommendation_model.joblib"
    joblib.dump(rec_model, rec_artifact)

    metadata = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "models": {
            "churn": {
                "artifact": "churn_model.joblib",
                "features": getattr(churn_model, "feature_names_in_", []),
                "metrics": churn_metrics,
            },
            "recommendation": {
                "artifact": "recommendation_model.joblib",
                "features": getattr(rec_model, "feature_names_in_", []),
                "metrics": rec_metrics,
            },
        },
    }
    meta_file = art_path / "metadata.json"
    meta_file.write_text(json.dumps(metadata, indent=2))

    if upload_s3:
        upload_to_minio(churn_artifact, "models/churn_model.joblib")
        upload_to_minio(rec_artifact, "models/recommendation_model.joblib")
        upload_to_minio(meta_file, "models/metadata.json")

    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description="Huấn luyện mô hình ML và lưu artifact")
    parser.add_argument("--data-dir", default="data", help="Thư mục chứa train parquet")
    parser.add_argument("--artifact-dir", default="artifacts/models", help="Thư mục lưu artifact")
    parser.add_argument("--no-upload", action="store_true", help="Không upload lên MinIO")
    args = parser.parse_args()

    meta = run_training_pipeline(
        data_dir=args.data_dir,
        artifact_dir=args.artifact_dir,
        upload_s3=not args.no_upload,
    )
    print("✓ Huấn luyện mô hình hoàn tất:")
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
