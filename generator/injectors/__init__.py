"""Cơ chế tiêm sự cố dữ liệu có kiểm soát (skew, schema, duplicate, late arrival)."""

from generator.injectors.duplicate_injector import inject_duplicates
from generator.injectors.late_arrival_injector import event_timestamp, message_quota, sample_late_delay_seconds
from generator.injectors.schema_evolution_injector import assign_evolved_attributes, write_movie_partitions
from generator.injectors.skew_injector import apply_genre_skew, plan_movie_skew, sample_from_weights

__all__ = [
    "apply_genre_skew",
    "assign_evolved_attributes",
    "event_timestamp",
    "inject_duplicates",
    "message_quota",
    "plan_movie_skew",
    "sample_from_weights",
    "sample_late_delay_seconds",
    "write_movie_partitions",
]
