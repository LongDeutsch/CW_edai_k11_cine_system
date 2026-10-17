"""Phân phối mức độ tích cực của user theo luật Pareto.

Đặc tả: khoảng 5% super-user đóng góp hơn 40% tương tác, phần còn lại
chỉ phát sinh rất ít rating / tag / phiên xem.

Cách làm để tỷ lệ này **chỉnh được** và kiểm chứng được:

1. Chọn đúng ``head_fraction`` user có ``user_id`` nhỏ nhất làm super-user.
2. Sinh trọng số dương bên trong mỗi nhóm bằng Pareto (numpy Lomax + 1).
3. Co giãn hai nhóm sao cho tổng trọng số super-user bằng đúng ``head_mass``.
"""

from __future__ import annotations

import numpy as np


def activity_weights(
    user_ids: np.ndarray,
    *,
    head_fraction: float = 0.05,
    head_mass: float = 0.40,
    pareto_shape: float = 1.5,
    rng: np.random.Generator,
) -> tuple[np.ndarray, np.ndarray]:
    """Trọng số lấy mẫu user, căn chỉnh đúng khối lượng super-user.

    Args:
        user_ids: Mảng id duy nhất. Super-user là các id nhỏ nhất.
        head_fraction: Tỷ lệ user thuộc nhóm đầu, mặc định 5%.
        head_mass: Phần tương tác mà nhóm đó đóng góp, mặc định 40%.
        pareto_shape: Shape của Pareto bên trong mỗi nhóm. Chỉ làm các user
            trong cùng nhóm không đều nhau; không đổi tổng 5% / 40%.
        rng: Bộ sinh số ngẫu nhiên đã được seed.

    Returns:
        ``(weights, super_user_ids)``. ``weights[i]`` ứng với ``user_ids[i]``,
        tổng bằng 1.
    """
    ids = np.asarray(user_ids)
    n_users = int(ids.size)
    if n_users < 1:
        raise ValueError("user_ids rỗng")
    if not 0 < head_fraction <= 1:
        raise ValueError("head_fraction phải nằm trong (0, 1]")
    if not 0 < head_mass <= 1:
        raise ValueError("head_mass phải nằm trong (0, 1]")
    if pareto_shape <= 0:
        raise ValueError("pareto_shape phải > 0")

    head_count = min(n_users, max(1, int(round(head_fraction * n_users))))
    order = np.argsort(ids, kind="mergesort")
    super_positions = order[:head_count]
    regular_positions = order[head_count:]

    weights = np.empty(n_users, dtype=np.float64)
    weights[super_positions] = _positive_pareto(head_count, pareto_shape, rng)
    if regular_positions.size:
        weights[regular_positions] = _positive_pareto(int(regular_positions.size), pareto_shape, rng)
        regular_mass = 1.0 - float(head_mass)
        weights[regular_positions] *= regular_mass / float(weights[regular_positions].sum())
        weights[super_positions] *= float(head_mass) / float(weights[super_positions].sum())
    else:
        weights[super_positions] /= float(weights[super_positions].sum())

    weights /= float(weights.sum())
    return weights, np.asarray(ids[super_positions])


def super_user_event_share(sampled_user_ids: np.ndarray, super_user_ids: np.ndarray) -> float:
    """Tỷ lệ dòng sự kiện thực sự thuộc tập super-user."""
    sampled = np.asarray(sampled_user_ids)
    supers = np.asarray(super_user_ids)
    if sampled.size == 0 or supers.size == 0:
        return 0.0
    flag = np.zeros(int(max(sampled.max(), supers.max())) + 1, dtype=bool)
    flag[supers.astype(np.int64, copy=False)] = True
    return float(flag[sampled.astype(np.int64, copy=False)].mean())


def _positive_pareto(n: int, shape: float, rng: np.random.Generator) -> np.ndarray:
    """Mẫu Pareto > 0. ``Generator.pareto`` là Lomax trên [0, ∞)."""
    if n <= 0:
        return np.empty(0, dtype=np.float64)
    return rng.pareto(shape, size=n) + 1.0
