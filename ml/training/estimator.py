"""Mô hình dự đoán thuần NumPy cho Recommendation và Churn Prediction."""

from __future__ import annotations

from typing import Any
import numpy as np

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore[assignment]


class LightweightEstimator:
    """Mô hình Logistic/Boosting thuần NumPy khi môi trường chưa cài LightGBM/XGBoost."""

    def __init__(self, name: str, feature_names: list[str]) -> None:
        self.name = name
        self.feature_names_in_ = feature_names
        self.weights_: np.ndarray | None = None
        self.bias_: float = 0.0
        self.mean_: np.ndarray | None = None
        self.std_: np.ndarray | None = None

    def fit(self, X: np.ndarray, y: np.ndarray, epochs: int = 150, lr: float = 0.05) -> LightweightEstimator:
        X = np.nan_to_num(np.asarray(X, dtype=np.float64), 0.0)
        y = np.asarray(y, dtype=np.float64)

        # Chuẩn hóa z-score
        mean = np.mean(X, axis=0)
        std = np.std(X, axis=0)
        std[std == 0.0] = 1.0
        self.mean_ = mean
        self.std_ = std
        X_norm = (X - mean) / std

        n_samples, n_features = X_norm.shape
        w = np.zeros(n_features)
        b = 0.0

        for _ in range(epochs):
            linear = np.clip(np.dot(X_norm, w) + b, -20.0, 20.0)
            preds = 1.0 / (1.0 + np.exp(-linear))
            err = preds - y
            dw = np.dot(X_norm.T, err) / n_samples
            db = np.sum(err) / n_samples
            w -= lr * dw
            b -= lr * db

        self.weights_ = w
        self.bias_ = b
        return self

    def predict_proba(self, X: Any) -> np.ndarray:
        if pd is not None and isinstance(X, pd.DataFrame):
            X = X[self.feature_names_in_].values
        elif hasattr(X, "columns") and hasattr(X, "values"):
            X = X[self.feature_names_in_].values
        X = np.nan_to_num(np.asarray(X, dtype=np.float64), 0.0)
        if self.mean_ is not None and self.std_ is not None:
            X_norm = (X - self.mean_) / self.std_
        else:
            X_norm = X
        w = self.weights_ if self.weights_ is not None else np.zeros(X_norm.shape[1])
        linear = np.clip(np.dot(X_norm, w) + self.bias_, -20.0, 20.0)
        p1 = 1.0 / (1.0 + np.exp(-linear))
        p0 = 1.0 - p1
        return np.column_stack([p0, p1])

    def predict(self, X: Any) -> np.ndarray:
        probs = self.predict_proba(X)
        return (probs[:, 1] >= 0.5).astype(int)
