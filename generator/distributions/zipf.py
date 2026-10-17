"""Phân phối Zipf cho độ phổ biến của phim.

Công thức (đặc tả data generator, mục 2.1):

    P(k) = (1 / k^s) / sum_{n=1}^{N} (1 / n^s)

``k = 1`` là phim phổ biến nhất. Với ``s ≈ 1.25`` và ``N ≈ 100_000``,
khoảng 1% phim đứng đầu tích lũy hơn 80% xác suất.
"""

from __future__ import annotations

import numpy as np


def zipf_probabilities(n_items: int, exponent: float) -> np.ndarray:
    """Xác suất theo thứ hạng, index 0 là hạng 1 (nặng nhất).

    Args:
        n_items: Số hạng ``N``. Phải >= 1.
        exponent: Hệ số ``s``. ``s = 0`` cho phân phối đều;
            ``s`` càng lớn thì khối lượng càng dồn về hạng đầu.

    Returns:
        Vector float64 độ dài ``n_items``, tổng bằng 1.
    """
    if n_items < 1:
        raise ValueError("n_items phải >= 1")
    if exponent < 0:
        raise ValueError("exponent (s) phải >= 0")
    ranks = np.arange(1, n_items + 1, dtype=np.float64)
    weights = np.ones(n_items, dtype=np.float64) if exponent == 0 else ranks ** (-exponent)
    total = float(weights.sum())
    if not np.isfinite(total) or total <= 0:
        raise ValueError(f"tổng trọng số Zipf không hợp lệ với s={exponent}, N={n_items}")
    return weights / total


def solve_zipf_exponent(
    n_items: int,
    head_count: int,
    target_head_mass: float,
) -> float:
    """Tìm ``s`` sao cho ``head_count`` hạng đầu chiếm đúng ``target_head_mass``.

    Tìm kiếm nhị phân trên ``s``. ``s = 0`` là phân phối đều
    (khối lượng đầu = head_count / n_items). ``s`` lớn dồn gần như
    toàn bộ xác suất vào hạng 1.
    """
    if n_items < 1:
        raise ValueError("n_items phải >= 1")
    if not 1 <= head_count <= n_items:
        raise ValueError("head_count phải nằm trong [1, n_items]")
    if not 0 < target_head_mass <= 1:
        raise ValueError("target_head_mass phải nằm trong (0, 1]")

    uniform_mass = head_count / n_items
    if target_head_mass <= uniform_mass + 1e-12:
        return 0.0
    if head_count == n_items:
        return 0.0

    def head_mass(exponent: float) -> float:
        return float(zipf_probabilities(n_items, exponent)[:head_count].sum())

    lo = 0.0
    hi = 32.0
    if head_mass(hi) < target_head_mass:
        return hi
    for _ in range(48):
        mid = (lo + hi) / 2.0
        if head_mass(mid) < target_head_mass:
            lo = mid
        else:
            hi = mid
    return float(hi)


def popularity_weights(
    n_items: int,
    *,
    exponent: float | None = None,
    head_fraction: float = 0.01,
    head_mass: float = 0.80,
) -> tuple[np.ndarray, float]:
    """Trọng số phổ biến phim: 1% hạng đầu nhận khoảng ``head_mass``.

    Args:
        n_items: Số phim trong danh mục.
        exponent: Nếu truyền vào thì dùng đúng ``s`` này. Nếu ``None``,
            giải ``s`` để top ``head_fraction`` đạt ``head_mass``.
        head_fraction: Tỷ lệ phim bom tấn, mặc định 1%.
        head_mass: Phần xác suất mà nhóm bom tấn phải nhận, mặc định 80%.

    Returns:
        ``(probabilities, exponent_used)``. Index 0 là phim nặng nhất.
    """
    if n_items < 1:
        raise ValueError("n_items phải >= 1")
    if not 0 < head_fraction <= 1:
        raise ValueError("head_fraction phải nằm trong (0, 1]")
    if not 0 < head_mass <= 1:
        raise ValueError("head_mass phải nằm trong (0, 1]")

    head_count = min(n_items, max(1, int(round(head_fraction * n_items))))
    used = float(exponent) if exponent is not None else solve_zipf_exponent(n_items, head_count, head_mass)
    return zipf_probabilities(n_items, used), used
