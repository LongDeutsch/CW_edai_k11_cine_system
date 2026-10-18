"""Nhận diện batch nguồn từ listing MinIO.

Tên thư mục không được giả định trùng ``{{ ds }}``. Batch id là fingerprint
của các file parquet hiện có. File cũ không đổi thì fingerprint không đổi,
nên Airflow không gắn nhãn dữ liệu cũ thành một batch mới.
"""

from __future__ import annotations

import hashlib

from transformation.batch.contracts.validators import CONTRACTS

REQUIRED_FOLDERS = tuple(CONTRACTS[name].folder for name in ("users", "movies", "ratings", "watch"))


def parse_s3_uri(uri: str) -> tuple[str, str]:
    raw = uri.strip().replace("s3a://", "", 1).replace("s3://", "", 1)
    bucket, _, prefix = raw.partition("/")
    if not bucket:
        raise ValueError(f"URI nguồn không hợp lệ: {uri!r}")
    return bucket, prefix.strip("/")


def assess_listing(objects: list[dict], prefix: str) -> dict:
    """``objects`` là ``{key, size, etag}`` tuyệt đối trong bucket.

    Một dataset thiếu parquet thì batch chưa sẵn sàng. File nằm ngoài bốn
    folder hợp đồng bị bỏ qua, kể cả prefix thử nghiệm.
    """
    base = prefix.strip("/")
    grouped = {folder: [] for folder in REQUIRED_FOLDERS}
    for item in objects:
        key = str(item["key"])
        relative = key[len(base) :].lstrip("/") if base and key.startswith(base) else key
        if base and not key.startswith(base.rstrip("/") + "/") and key != base:
            continue
        if not relative.endswith(".parquet"):
            continue
        folder = relative.split("/", 1)[0]
        if folder not in grouped:
            continue
        grouped[folder].append(item)
    counts = {folder: len(files) for folder, files in grouped.items()}
    missing = [folder for folder, files in grouped.items() if not files]
    if missing:
        return {
            "ready": False,
            "reason": "thiếu parquet cho " + ", ".join(missing),
            "batch_id": "",
            "files": 0,
            "datasets": counts,
        }
    lines = []
    for folder in REQUIRED_FOLDERS:
        for item in sorted(grouped[folder], key=lambda row: str(row["key"])):
            etag = str(item.get("etag", "")).strip('"')
            lines.append(f"{item['key']}|{int(item['size'])}|{etag}")
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    return {
        "ready": True,
        "reason": "ready",
        "batch_id": f"batch-{digest[:32]}",
        "files": len(lines),
        "datasets": counts,
    }
