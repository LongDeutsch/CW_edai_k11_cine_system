"""Đối chiếu marker DP2/DP3 với export Parquet trước khi ghi Redis.

Module này không import Feast. DAG Airflow nạp được kể cả khi image chưa có Feast.
"""

from __future__ import annotations


class PublicationMismatch(RuntimeError):
    """Export không thuộc batch hoặc snapshot đang được publish."""


def assert_publication(
    marker: dict | None,
    feature_report: dict | None,
    export_meta: dict,
    conf_batch: str | None = None,
    live_snapshots: dict | None = None,
) -> str:
    """Trả batch_id khi marker, báo cáo DP3 và export là cùng một publication."""
    if not marker or not marker.get("batch_id"):
        raise PublicationMismatch("marker DP2 không có batch_id")
    batch_id = str(marker["batch_id"])
    if conf_batch not in (None, "", batch_id):
        raise PublicationMismatch(f"batch_id conf {conf_batch} khác marker {batch_id}")
    if not feature_report or feature_report.get("ready") is not True:
        raise PublicationMismatch("DP3 chưa ready")
    if str(export_meta.get("batch_id")) != batch_id:
        raise PublicationMismatch("batch_id export khác marker DP2")
    exported = {key: str(value) for key, value in (export_meta.get("snapshot_ids") or {}).items()}
    if live_snapshots is not None:
        live = {key: str(value) for key, value in live_snapshots.items()}
        if exported != live:
            raise PublicationMismatch(f"snapshot export {exported} khác catalog {live}")
    if "sequence" not in export_meta:
        raise PublicationMismatch("export không có sequence")
    return batch_id
