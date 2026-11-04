"""Kiểm thử quy trình Point-in-Time Join và huấn luyện mô hình ML."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

try:
    import pandas as pd
    from ml.offline.build_training_sets import (
        build_churn_training_set,
        build_rec_training_set,
        extract_and_export_training_sets,
        point_in_time_join,
    )
    from ml.training.estimator import LightweightEstimator
    from ml.training.train_models import run_training_pipeline, train_churn_model, train_rec_model
    HAS_PANDAS = True
except ImportError:
    pd = None  # type: ignore[assignment]
    HAS_PANDAS = False


class TestMLTrainingPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not HAS_PANDAS:
            raise unittest.SkipTest("Cần cài đặt pandas (chạy `uv sync` hoặc `pip install pandas`)")
    def test_point_in_time_join_prevents_future_leakage(self):
        """Đảm bảo không rò rỉ dữ liệu tương lai: chỉ ghép đặc trưng có timestamp <= T."""
        t1 = pd.Timestamp("2026-06-01 00:00:00", tz="UTC")
        t2 = pd.Timestamp("2026-06-05 00:00:00", tz="UTC")
        t3 = pd.Timestamp("2026-06-10 00:00:00", tz="UTC")

        # Labels tại mốc t2
        labels = pd.DataFrame(
            [
                {"user_id": 1, "event_timestamp": t2, "is_churned": 0},
            ]
        )

        # Features có 2 mốc: t1 (quá khứ) và t3 (tương lai)
        features = pd.DataFrame(
            [
                {"user_id": 1, "event_timestamp": t1, "user_total_watch_hours_30d": 10.0},
                {"user_id": 1, "event_timestamp": t3, "user_total_watch_hours_30d": 99.0},
            ]
        )

        joined = point_in_time_join(
            labels,
            features,
            entity_col="user_id",
            timestamp_col="event_timestamp",
            feature_cols=["user_total_watch_hours_30d"],
        )

        self.assertEqual(len(joined), 1)
        # Phải lấy giá trị 10.0 ở t1, KHÔNG ĐƯỢC lấy giá trị 99.0 ở tương lai t3
        self.assertEqual(joined.iloc[0]["user_total_watch_hours_30d"], 10.0)

    def test_lightweight_estimator_fit_and_predict(self):
        rng = np.random.default_rng(42)
        X = rng.normal(size=(50, 4))
        y = (X[:, 0] + X[:, 1] > 0).astype(int)

        clf = LightweightEstimator(name="test_model", feature_names=["f1", "f2", "f3", "f4"])
        clf.fit(X, y, epochs=100)

        probs = clf.predict_proba(X)
        preds = clf.predict(X)

        self.assertEqual(probs.shape, (50, 2))
        self.assertEqual(preds.shape, (50,))
        self.assertTrue(np.all((probs >= 0.0) & (probs <= 1.0)))

    def test_extract_and_train_pipeline_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            art_dir = Path(tmpdir) / "artifacts"

            # 1. Trích xuất training datasets
            c_file, r_file = extract_and_export_training_sets(output_dir=data_dir)
            self.assertTrue(c_file.exists())
            self.assertTrue(r_file.exists())

            # 2. Huấn luyện mô hình và lưu artifacts
            meta = run_training_pipeline(data_dir=data_dir, artifact_dir=art_dir, upload_s3=False)

            self.assertIn("models", meta)
            self.assertIn("churn", meta["models"])
            self.assertIn("recommendation", meta["models"])

            churn_art = art_dir / "churn_model.joblib"
            rec_art = art_dir / "recommendation_model.joblib"
            self.assertTrue(churn_art.exists())
            self.assertTrue(rec_art.exists())


if __name__ == "__main__":
    unittest.main()
