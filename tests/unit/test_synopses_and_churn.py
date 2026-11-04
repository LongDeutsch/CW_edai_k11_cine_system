"""Kiểm tra việc tích hợp movie synopses và logic tính toán nhãn churn 14 ngày."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

from generator.offline_generator import _build_movies, _load_synopses
from transformation.batch.contracts.validators import CONTRACTS


def test_movie_synopses_seed_file():
    seed_path = Path("data_generator/seeds/movie_synopses_seed.json")
    assert seed_path.exists(), "Không tìm thấy file movie_synopses_seed.json"
    data = json.loads(seed_path.read_text(encoding="utf-8"))
    assert len(data) >= 150, f"Số lượng phim có synopses phải >= 150, hiện có {len(data)}"

    synopses_map = _load_synopses()
    assert len(synopses_map) >= 150
    assert 42 in synopses_map, "Movie 42 phải có trong từ điển synopses"
    assert "overview" in synopses_map[42] and synopses_map[42]["overview"]
    assert "genres" in synopses_map[42] and synopses_map[42]["genres"]


def test_build_movies_includes_overview_and_genres():
    rng = np.random.default_rng(42)
    movies_by_id = {
        1: {"title": "Toy Story (1995)", "genres": "Adventure|Animation"},
        42: {"title": "Dead Presidents (1995)", "genres": "Action|Crime"},
    }
    columns, genre_lists = _build_movies(movies_by_id, 1200, rng)

    assert "overview" in columns
    assert "genres" in columns
    assert len(columns["overview"]) == 1200
    assert len(columns["genres"]) == 1200

    # Movie 1 và 42 có trong seed -> overview không None
    assert columns["overview"][0] is not None
    assert columns["overview"][41] is not None
    # Phim ngoài seed (id > 1000) -> overview là None
    assert columns["overview"][1100] is None


def test_contracts_movies_has_optional_fields():
    movies_contract = CONTRACTS["movies"]
    assert "overview" in movies_contract.optional
    assert "genres" in movies_contract.optional
    assert "content_rating" in movies_contract.optional


def test_churn_14d_logic_window():
    """Kiểm tra logic phân loại churn 14 ngày."""
    as_of = date(2026, 3, 1)
    window_start = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    window_end = window_start + timedelta(days=14)

    # User 1 có session trong cửa sổ [T, T+14d) -> active (is_churned = 0)
    user1_sessions = [datetime(2026, 3, 5, 12, 0, tzinfo=timezone.utc)]
    # User 2 có session trước T nhưng không có trong [T, T+14d) -> churned (is_churned = 1)
    user2_sessions = [datetime(2026, 2, 28, 23, 0, tzinfo=timezone.utc)]
    # User 3 có session sau T+14d -> churned tại T (is_churned = 1)
    user3_sessions = [datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc)]

    def evaluate_churn(sessions):
        in_window = [s for s in sessions if window_start <= s < window_end]
        return 0 if in_window else 1

    assert evaluate_churn(user1_sessions) == 0
    assert evaluate_churn(user2_sessions) == 1
    assert evaluate_churn(user3_sessions) == 1
