"""Bộ sinh dữ liệu giả lập cho CineStream (Layer 1).

Seed MovieLens nhỏ được phóng theo phân phối Zipf / Pareto / Beta,
rồi chủ động tiêm skew, schema evolution và duplicate trước khi ghi Parquet.
"""

__all__ = ["offline_generator"]
