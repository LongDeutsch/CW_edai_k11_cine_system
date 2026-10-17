"""Điểm rating theo phân phối Beta, lệch về vùng 3.5–4.5 sao.

Đặc tả:

    rating = Beta(alpha=6, beta=2) * 4.5 + 0.5

Kỳ vọng của Beta(6, 2) là 6/8 = 0.75, nên điểm kỳ vọng trên thang sao là
``0.75 * 4.5 + 0.5 = 3.875``. Mẫu được làm tròn về bước 0.5 sao
(0.5, 1.0, ..., 5.0) cho khớp MovieLens.
"""

from __future__ import annotations

import numpy as np


def sample_star_ratings(
    n: int,
    rng: np.random.Generator,
    *,
    alpha: float = 6.0,
    beta: float = 2.0,
    floor: float = 0.5,
    span: float = 4.5,
) -> np.ndarray:
    """Sinh ``n`` điểm sao nửa bậc trong [0.5, 5.0].

    Args:
        n: Số điểm cần sinh.
        rng: Bộ sinh số ngẫu nhiên.
        alpha: Tham số α của Beta. α > β kéo phân phối về phía điểm cao.
        beta: Tham số β của Beta.
        floor: Điểm thấp nhất sau khi co giãn (0.5 sao).
        span: Độ rộng thang sau floor. ``floor + span`` phải bằng 5.0
            với mặc định 0.5 + 4.5.
    """
    if n < 0:
        raise ValueError("n phải >= 0")
    if alpha <= 0 or beta <= 0:
        raise ValueError("alpha và beta phải > 0")
    if n == 0:
        return np.empty(0, dtype=np.float64)

    raw = rng.beta(alpha, beta, size=n) * span + floor
    half_stars = np.round(np.clip(raw, floor, floor + span) * 2.0) / 2.0
    return np.clip(half_stars, floor, floor + span).astype(np.float64, copy=False)


def summarize_star_ratings(ratings: np.ndarray) -> dict[str, float]:
    """Tóm tắt để kiểm chứng độ lệch về dải 3.5–4.5 sao."""
    values = np.asarray(ratings, dtype=np.float64)
    if values.size == 0:
        return {"mean": 0.0, "share_3_5_to_4_5": 0.0, "share_at_most_2": 0.0}
    in_band = (values >= 3.5) & (values <= 4.5)
    return {
        "mean": float(values.mean()),
        "share_3_5_to_4_5": float(in_band.mean()),
        "share_at_most_2": float((values <= 2.0).mean()),
    }
