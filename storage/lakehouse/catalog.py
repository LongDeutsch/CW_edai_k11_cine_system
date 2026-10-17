"""Danh mục bảng Lakehouse, khớp các file SQL trong bronze/silver/gold.

DDL chạy trên Spark là các file ``*.sql``. Module này chỉ liệt kê tên và
vai trò để UI và Makefile kiểm tra đủ bảng, không thay thế DDL.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LakehouseTable:
    zone: str
    name: str
    grain: str
    sql_file: str


TABLES: tuple[LakehouseTable, ...] = (
    LakehouseTable("bronze", "raw_users", "Một dòng mỗi lần nạp hồ sơ user, append-only", "bronze/raw_tables.sql"),
    LakehouseTable("bronze", "raw_movies", "Một dòng mỗi lần nạp phim, giữ schema cũ và mới", "bronze/raw_tables.sql"),
    LakehouseTable("bronze", "raw_ratings", "Một dòng mỗi rating thô, kể cả bản trùng", "bronze/raw_tables.sql"),
    LakehouseTable("bronze", "raw_watch_history", "Một dòng mỗi phiên xem thô, kể cả bản trùng", "bronze/raw_tables.sql"),
    LakehouseTable("silver", "stg_users", "Một dòng hiện tại cho mỗi user_id sau khi khử trùng", "silver/stg_tables.sql"),
    LakehouseTable("silver", "stg_movies", "Một dòng cho mỗi movie_id, schema đã hợp nhất", "silver/stg_tables.sql"),
    LakehouseTable("silver", "stg_ratings", "Một rating hợp lệ cho mỗi khóa nghiệp vụ", "silver/stg_tables.sql"),
    LakehouseTable("silver", "stg_watch_sessions", "Một phiên xem hợp lệ sau khi khử trùng", "silver/stg_tables.sql"),
    LakehouseTable("gold", "dim_users", "Lịch sử gói cước SCD Type 2", "gold/gold_tables.sql"),
    LakehouseTable("gold", "dim_movies", "Lịch sử danh mục phim SCD Type 2", "gold/gold_tables.sql"),
    LakehouseTable("gold", "fact_ratings", "Một dòng cho mỗi rating hợp lệ", "gold/gold_tables.sql"),
    LakehouseTable("gold", "fact_watch_sessions", "Một dòng cho mỗi phiên xem hoàn thành", "gold/gold_tables.sql"),
    LakehouseTable("gold", "feat_user_movie_stats_daily", "Đặc trưng user theo ngày, chuẩn Feast", "gold/gold_tables.sql"),
    LakehouseTable("gold", "feat_movie_engagement_stats_daily", "Đặc trưng phim theo ngày, chuẩn Feast", "gold/gold_tables.sql"),
    LakehouseTable("gold", "obt_movie_engagement", "Bảng phẳng phục vụ dashboard", "gold/gold_tables.sql"),
)


def tables_in(zone: str) -> tuple[LakehouseTable, ...]:
    return tuple(table for table in TABLES if table.zone == zone)
