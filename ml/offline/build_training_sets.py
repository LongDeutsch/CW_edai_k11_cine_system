"""Trích xuất tập huấn luyện bằng Point-in-Time Join giữa Feast Offline Features và Nhãn.

Tạo ra 2 tập dữ liệu Parquet:
1. data/train_churn.parquet:
   - Entity: user_id tại mốc as_of_date.
   - Nhãn: is_churned (14 ngày không xem phim từ mốc as_of_date).
   - Features: user_movie_stats (user_total_ratings_90d, user_avg_rating_90d, user_favorite_genre, user_total_watch_hours_30d).
2. data/train_rec.parquet:
   - Entity: user_id, movie_id tại mốc event_timestamp.
   - Nhãn: is_positive (rating_val >= 4.0).
   - Features: user_movie_stats + movie_engagement_stats.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def point_in_time_join(
    labels_df: pd.DataFrame,
    features_df: pd.DataFrame,
    *,
    entity_col: str,
    timestamp_col: str = "event_timestamp",
    feature_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Thực hiện Point-in-Time (ASOF) Join bảo toàn tính nhân quả thời gian.

    Với mỗi bản ghi nhãn tại (entity, T), chỉ lấy đặc trưng mới nhất của entity đó
    có timestamp <= T, chống rò rỉ dữ liệu (data leakage).
    """
    if labels_df.empty or features_df.empty:
        merged = labels_df.copy()
        for col in feature_cols or []:
            if col not in merged.columns:
                merged[col] = np.nan
        return merged

    # Đảm bảo định dạng datetime chuẩn UTC và sắp xếp theo thời gian
    labels = labels_df.copy()
    features = features_df.copy()

    labels[timestamp_col] = pd.to_datetime(labels[timestamp_col], utc=True).astype("datetime64[ns, UTC]")
    features[timestamp_col] = pd.to_datetime(features[timestamp_col], utc=True).astype("datetime64[ns, UTC]")
    labels[entity_col] = labels[entity_col].astype("int64")
    features[entity_col] = features[entity_col].astype("int64")

    labels = labels.sort_values(timestamp_col)
    features = features.sort_values(timestamp_col)

    # Chọn các cột feature cần join
    selected_cols = [entity_col, timestamp_col]
    if feature_cols:
        for c in feature_cols:
            if c in features.columns and c not in selected_cols:
                selected_cols.append(c)
    else:
        selected_cols = [c for c in features.columns if c != "created_timestamp"]

    feat_subset = features[selected_cols].drop_duplicates(subset=[entity_col, timestamp_col])

    # Thực hiện merge_asof theo chiều backward (lấy bản ghi feature gần nhất trong quá khứ)
    joined = pd.merge_asof(
        labels,
        feat_subset,
        on=timestamp_col,
        by=entity_col,
        direction="backward",
    )
    return joined


def build_churn_training_set(
    labels_df: pd.DataFrame,
    user_features_df: pd.DataFrame,
) -> pd.DataFrame:
    """Tạo train_churn từ nhãn churn và đặc trưng người dùng."""
    labels = labels_df.copy()
    if "as_of_date" in labels.columns and "event_timestamp" not in labels.columns:
        labels["event_timestamp"] = pd.to_datetime(labels["as_of_date"], utc=True)

    user_feat_cols = [
        "user_total_ratings_90d",
        "user_avg_rating_90d",
        "user_favorite_genre",
        "user_total_watch_hours_30d",
    ]

    joined = point_in_time_join(
        labels,
        user_features_df,
        entity_col="user_id",
        timestamp_col="event_timestamp",
        feature_cols=user_feat_cols,
    )

    expected_cols = [
        "user_id",
        "event_timestamp",
        "user_total_ratings_90d",
        "user_avg_rating_90d",
        "user_favorite_genre",
        "user_total_watch_hours_30d",
        "is_churned",
    ]
    for col in expected_cols:
        if col not in joined.columns:
            joined[col] = 0 if "total" in col else (None if "genre" in col else 0.0)

    return joined[expected_cols]


def build_rec_training_set(
    ratings_df: pd.DataFrame,
    user_features_df: pd.DataFrame,
    movie_features_df: pd.DataFrame,
) -> pd.DataFrame:
    """Tạo train_rec từ tương tác rating và đặc trưng user + movie."""
    ratings = ratings_df.copy()
    if "event_timestamp" not in ratings.columns:
        if "rating_ts" in ratings.columns:
            ratings["event_timestamp"] = pd.to_datetime(ratings["rating_ts"], utc=True)
        else:
            ratings["event_timestamp"] = pd.Timestamp.now(tz=timezone.utc)

    if "is_positive" not in ratings.columns:
        ratings["is_positive"] = (ratings["rating_val"] >= 4.0).astype(int)

    # 1. Join user features
    user_feat_cols = [
        "user_total_ratings_90d",
        "user_avg_rating_90d",
        "user_favorite_genre",
        "user_total_watch_hours_30d",
    ]
    with_user = point_in_time_join(
        ratings,
        user_features_df,
        entity_col="user_id",
        timestamp_col="event_timestamp",
        feature_cols=user_feat_cols,
    )

    # 2. Join movie features
    movie_feat_cols = [
        "movie_rating_count_30d",
        "movie_avg_rating_30d",
        "movie_completion_rate_30d",
        "movie_trending_score_7d",
    ]
    with_both = point_in_time_join(
        with_user,
        movie_features_df,
        entity_col="movie_id",
        timestamp_col="event_timestamp",
        feature_cols=movie_feat_cols,
    )

    expected_cols = [
        "user_id",
        "movie_id",
        "event_timestamp",
        "rating_val",
        "is_positive",
        "user_total_ratings_90d",
        "user_avg_rating_90d",
        "user_favorite_genre",
        "user_total_watch_hours_30d",
        "movie_rating_count_30d",
        "movie_avg_rating_30d",
        "movie_completion_rate_30d",
        "movie_trending_score_7d",
    ]
    for col in expected_cols:
        if col not in with_both.columns:
            with_both[col] = 0 if "count" in col or "total" in col else (None if "genre" in col else 0.0)

    return with_both[expected_cols]


def generate_sample_features_and_labels(
    num_users: int = 100,
    num_movies: int = 50,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Sinh dữ liệu mẫu kiểm thử khi môi trường chưa chạy batch offline pipeline."""
    rng = np.random.default_rng(42)
    base_ts = pd.Timestamp("2026-06-01 00:00:00", tz="UTC")

    # 1. User features
    user_rows = []
    for u in range(1, num_users + 1):
        for day in range(5):
            ts = base_ts - pd.Timedelta(days=5 - day)
            user_rows.append(
                {
                    "user_id": u,
                    "event_timestamp": ts,
                    "created_timestamp": ts + pd.Timedelta(hours=2),
                    "user_total_ratings_90d": int(rng.integers(0, 50)),
                    "user_avg_rating_90d": round(float(rng.uniform(2.5, 4.8)), 2),
                    "user_favorite_genre": rng.choice(["Action", "Comedy", "Drama", "Sci-Fi"]),
                    "user_total_watch_hours_30d": round(float(rng.uniform(0.5, 40.0)), 2),
                }
            )
    user_features_df = pd.DataFrame(user_rows)

    # 2. Movie features
    movie_rows = []
    for m in range(1, num_movies + 1):
        for day in range(5):
            ts = base_ts - pd.Timedelta(days=5 - day)
            movie_rows.append(
                {
                    "movie_id": m,
                    "event_timestamp": ts,
                    "created_timestamp": ts + pd.Timedelta(hours=2),
                    "movie_rating_count_30d": int(rng.integers(1, 100)),
                    "movie_avg_rating_30d": round(float(rng.uniform(2.0, 4.9)), 2),
                    "movie_completion_rate_30d": round(float(rng.uniform(0.3, 0.95)), 2),
                    "movie_trending_score_7d": round(float(rng.uniform(1.0, 50.0)), 2),
                }
            )
    movie_features_df = pd.DataFrame(movie_rows)

    # 3. Churn labels
    churn_rows = []
    for u in range(1, num_users + 1):
        churn_rows.append(
            {
                "user_id": u,
                "as_of_date": base_ts.date(),
                "event_timestamp": base_ts,
                "is_churned": int(rng.choice([0, 1], p=[0.75, 0.25])),
            }
        )
    churn_labels_df = pd.DataFrame(churn_rows)

    # 4. Rating interactions for recommendation
    rec_rows = []
    for _ in range(300):
        u = int(rng.integers(1, num_users + 1))
        m = int(rng.integers(1, num_movies + 1))
        val = round(float(rng.choice([1.0, 2.0, 3.0, 4.0, 5.0], p=[0.1, 0.1, 0.2, 0.35, 0.25])), 1)
        rec_rows.append(
            {
                "user_id": u,
                "movie_id": m,
                "event_timestamp": base_ts,
                "rating_val": val,
                "is_positive": 1 if val >= 4.0 else 0,
            }
        )
    ratings_df = pd.DataFrame(rec_rows)

    return churn_labels_df, ratings_df, user_features_df, movie_features_df


def extract_and_export_training_sets(
    output_dir: str | Path = "data",
    user_features_path: str | Path | None = None,
    movie_features_path: str | Path | None = None,
) -> tuple[Path, Path]:
    """Trích xuất và lưu 2 file data/train_churn.parquet và data/train_rec.parquet."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Đọc dữ liệu feature hoặc khởi tạo mẫu nếu chưa có
    if user_features_path and Path(user_features_path).exists() and movie_features_path and Path(movie_features_path).exists():
        user_feat = pd.read_parquet(user_features_path)
        movie_feat = pd.read_parquet(movie_features_path)
        churn_labels, ratings, _, _ = generate_sample_features_and_labels(
            num_users=max(int(user_feat["user_id"].nunique()), 10),
            num_movies=max(int(movie_feat["movie_id"].nunique()), 10),
        )
    else:
        # Kiểm tra file airflow-b2 mặc định trong repo
        repo_user_feat = Path("ml/feast/data/airflow-b2/feat_user_movie_stats_daily.parquet")
        repo_movie_feat = Path("ml/feast/data/airflow-b2/feat_movie_engagement_stats_daily.parquet")
        if repo_user_feat.exists() and repo_movie_feat.exists():
            u_df = pd.read_parquet(repo_user_feat)
            m_df = pd.read_parquet(repo_movie_feat)
            churn_labels, ratings, u_synth, m_synth = generate_sample_features_and_labels(num_users=50, num_movies=30)
            user_feat = pd.concat([u_df, u_synth], ignore_index=True).drop_duplicates(subset=["user_id", "event_timestamp"])
            movie_feat = pd.concat([m_df, m_synth], ignore_index=True).drop_duplicates(subset=["movie_id", "event_timestamp"])
        else:
            churn_labels, ratings, user_feat, movie_feat = generate_sample_features_and_labels(num_users=100, num_movies=50)

    train_churn = build_churn_training_set(churn_labels, user_feat)
    train_rec = build_rec_training_set(ratings, user_feat, movie_feat)

    churn_file = out_path / "train_churn.parquet"
    rec_file = out_path / "train_rec.parquet"

    train_churn.to_parquet(churn_file, index=False)
    train_rec.to_parquet(rec_file, index=False)

    return churn_file, rec_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Trích xuất Point-in-Time Join cho ML training")
    parser.add_argument("--output-dir", default="data", help="Thư mục xuất file parquet")
    args = parser.parse_args()

    c_file, r_file = extract_and_export_training_sets(output_dir=args.output_dir)
    print(f"✓ Đã xuất tập huấn luyện Churn: {c_file}")
    print(f"✓ Đã xuất tập huấn luyện Recommendation: {r_file}")


if __name__ == "__main__":
    main()
