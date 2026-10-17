"""Tiêm data skew lên ``movie_id`` và ``genre_name``.

Hợp đồng (đặc tả mục 4.1), có thể chỉnh bằng tham số:

* Nhóm bom tấn = ``head_fraction`` phim có ``movie_id`` nhỏ nhất
  (với danh mục ``1..100_000`` thì đó là ``movie_id ∈ [1, 1000]``).
  Nhóm này nhận đúng ``skew_ratio`` số dòng (mặc định 80%).
* ``movie_id`` neo (mặc định 42, vai trò phim bom tấn toàn cầu) luôn
  nằm trong nhóm đầu và là khóa nặng nhất.
* ``genre_name``: đúng ``genre_skew_ratio`` dòng ``movie_genres``
  là Action hoặc Comedy (mặc định 70%).

Zipf (``s``) chỉ tạo hình dạng *bên trong* nhóm đầu và nhóm đuôi.
Phần khối lượng 80/20 do tham số ``skew_ratio`` quyết định, nên khi
đổi ``--skew-ratio`` tỷ lệ đo được đổi theo, không phụ thuộc việc
giải lại hệ số ``s``. Nếu ``s`` làm một phim khác nặng hơn một nửa
phim neo, ``s`` của nhóm đó được hạ xuống vừa đủ.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from generator.distributions.zipf import zipf_probabilities

SKEWED_GENRES = ("Action", "Comedy")
OTHER_GENRES = (
    "Adventure",
    "Animation",
    "Children",
    "Crime",
    "Documentary",
    "Drama",
    "Fantasy",
    "Film-Noir",
    "Horror",
    "Musical",
    "Mystery",
    "Romance",
    "Sci-Fi",
    "Thriller",
    "War",
    "Western",
)


@dataclass(frozen=True)
class MovieSkewPlan:
    """Kế hoạch lấy mẫu ``movie_id`` đã căn chỉnh khối lượng."""

    movie_ids: np.ndarray
    weights: np.ndarray
    head_mask: np.ndarray
    anchor_movie_id: int
    anchor_weight: float
    head_count: int
    zipf_exponent: float
    tempered_head: bool
    tempered_tail: bool


def plan_movie_skew(
    movie_ids: np.ndarray,
    *,
    skew_ratio: float,
    head_fraction: float = 0.01,
    anchor_movie_id: int = 42,
    anchor_share: float = 0.04,
    zipf_exponent: float = 1.25,
) -> MovieSkewPlan:
    """Xây trọng số lấy mẫu để nhóm đầu chiếm đúng ``skew_ratio``.

    Khi nhóm đầu chỉ có 1 phim (danh mục rất nhỏ), phim neo nhận
    toàn bộ ``skew_ratio`` vì không còn phim bom tấn nào khác để chia.
    ``anchor_share`` là sàn khối lượng của phim neo. Zipf bên trong
    nhóm đầu và nhóm đuôi bị hạ ``s`` sao cho không phim nào khác
    nặng quá một nửa phim neo — 42 vẫn là khóa nóng duy nhất.
    """
    ids = np.asarray(movie_ids, dtype=np.int64)
    n_items = int(ids.size)
    if n_items < 1:
        raise ValueError("movie_ids rỗng")
    if not 0 < skew_ratio <= 1:
        raise ValueError("skew_ratio phải nằm trong (0, 1]")
    if not 0 < head_fraction <= 1:
        raise ValueError("head_fraction phải nằm trong (0, 1]")
    if not 0 <= anchor_share <= 1:
        raise ValueError("anchor_share phải nằm trong [0, 1]")
    if zipf_exponent < 0:
        raise ValueError("zipf_exponent phải >= 0")

    head_count = min(n_items, max(1, int(round(head_fraction * n_items))))
    order = np.argsort(ids, kind="mergesort")
    head_idx = order[:head_count].copy()

    anchor_hits = np.flatnonzero(ids == anchor_movie_id)
    if anchor_hits.size == 0:
        raise ValueError(
            f"không tìm thấy anchor movie_id={anchor_movie_id} trong danh mục "
            f"(đang có {n_items} phim). Tăng --scale-factor hoặc chọn neo nhỏ hơn."
        )
    anchor_idx = int(anchor_hits[0])
    if not np.any(head_idx == anchor_idx):
        head_idx[-1] = anchor_idx

    anchor_weight = float(skew_ratio if head_count == 1 else min(anchor_share, skew_ratio))
    # Giữ phim neo nặng ít nhất gấp đôi phim đứng ngay sau.
    per_movie_cap = anchor_weight / 2.0 if anchor_weight > 0 else None
    weights = np.zeros(n_items, dtype=np.float64)
    weights[anchor_idx] = anchor_weight

    others = head_idx[head_idx != anchor_idx]
    remaining_head = float(skew_ratio) - anchor_weight
    tempered_head = False
    if others.size and remaining_head > 0:
        others_sorted = others[np.argsort(ids[others], kind="mergesort")]
        proposed, tempered_head = _zipf_mass(int(others_sorted.size), zipf_exponent, remaining_head, per_movie_cap)
        weights[others_sorted] = proposed
    elif remaining_head > 0:
        weights[anchor_idx] += remaining_head

    tail_idx = np.ones(n_items, dtype=bool)
    tail_idx[head_idx] = False
    tail_positions = np.flatnonzero(tail_idx)
    tail_mass = 1.0 - float(skew_ratio)
    tempered_tail = False
    if tail_positions.size and tail_mass > 0:
        tail_sorted = tail_positions[np.argsort(ids[tail_positions], kind="mergesort")]
        proposed, tempered_tail = _zipf_mass(int(tail_sorted.size), zipf_exponent, tail_mass, per_movie_cap)
        weights[tail_sorted] = proposed
    elif tail_mass > 0:
        weights[anchor_idx] += tail_mass

    weight_sum = float(weights.sum())
    if weight_sum <= 0 or not np.isfinite(weight_sum):
        raise ValueError("trọng số skew không hợp lệ")
    weights /= weight_sum

    head_mask = np.zeros(n_items, dtype=bool)
    head_mask[head_idx] = True
    return MovieSkewPlan(
        movie_ids=ids,
        weights=weights,
        head_mask=head_mask,
        anchor_movie_id=int(anchor_movie_id),
        anchor_weight=float(weights[anchor_idx]),
        head_count=head_count,
        zipf_exponent=float(zipf_exponent),
        tempered_head=tempered_head,
        tempered_tail=tempered_tail,
    )


def _zipf_mass(
    n_items: int,
    exponent: float,
    mass: float,
    cap: float | None,
) -> tuple[np.ndarray, bool]:
    """Chia ``mass`` theo Zipf, hạ ``s`` nếu hạng 1 vượt ``cap``.

    Returns:
        Vector độ dài ``n_items`` tổng bằng ``mass``, và cờ đã phải hạ ``s``.
    """
    if n_items <= 0 or mass <= 0:
        return np.zeros(max(n_items, 0), dtype=np.float64), False
    if cap is None or cap <= 0 or float(zipf_probabilities(n_items, exponent)[0] * mass) <= cap:
        return zipf_probabilities(n_items, exponent) * mass, False
    if mass / n_items > cap:
        return np.full(n_items, mass / n_items, dtype=np.float64), True

    lo = 0.0
    hi = float(exponent)
    for _ in range(48):
        mid = (lo + hi) / 2.0
        if float(zipf_probabilities(n_items, mid)[0] * mass) > cap:
            hi = mid
        else:
            lo = mid
    return zipf_probabilities(n_items, lo) * mass, True


def sample_from_weights(
    ids: np.ndarray,
    weights: np.ndarray,
    n_rows: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Lấy mẫu có hoàn lại theo trọng số đã căn."""
    if n_rows < 0:
        raise ValueError("n_rows phải >= 0")
    if n_rows == 0:
        return np.empty(0, dtype=np.int64)
    probabilities = np.asarray(weights, dtype=np.float64)
    probabilities = probabilities / float(probabilities.sum())
    return rng.choice(np.asarray(ids), size=n_rows, replace=True, p=probabilities)


def apply_genre_skew(
    genre_names: np.ndarray,
    *,
    skew_ratio: float,
    rng: np.random.Generator,
) -> np.ndarray:
    """Ép đúng ``skew_ratio`` dòng thể loại thành Action hoặc Comedy.

    Phần còn lại được gán lại từ các thể loại MovieLens khác, nên tỷ lệ
    Action+Comedy đo được khớp tham số chứ không chỉ "ít nhất 70%".
    """
    if not 0 <= skew_ratio <= 1:
        raise ValueError("genre skew_ratio phải nằm trong [0, 1]")
    names = np.array(genre_names, dtype=object, copy=True)
    n_rows = int(names.size)
    if n_rows == 0:
        return names

    n_hot = int(round(skew_ratio * n_rows))
    hot_labels = np.array(SKEWED_GENRES, dtype=object)
    other_labels = np.array(OTHER_GENRES, dtype=object)
    if n_hot:
        hot_idx = rng.choice(n_rows, size=n_hot, replace=False)
        names[hot_idx] = rng.choice(hot_labels, size=n_hot)
        cold_mask = np.ones(n_rows, dtype=bool)
        cold_mask[hot_idx] = False
    else:
        cold_mask = np.ones(n_rows, dtype=bool)
    n_cold = int(cold_mask.sum())
    if n_cold:
        names[cold_mask] = rng.choice(other_labels, size=n_cold)
    return names
