"""Đồng bộ Parquet offline lên bucket nguồn MinIO ``external-source-lake``.

Đường dẫn tương đối trong ``--source-dir`` được giữ nguyên làm object key,
đúng layout ``s3://external-source-lake/ratings/dt=YYYY-MM-DD/...``.

``--dry-run`` chỉ liệt kê file và số byte. Không mở kết nối MinIO.

Ví dụ::

    python -m generator.upload_to_source_minio --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from pathlib import Path

logger = logging.getLogger("generator.minio")

DEFAULT_SOURCE = Path("data_generator/output/scale_0_1")
DEFAULT_BUCKET = "external-source-lake"
DEFAULT_ENDPOINT = "http://localhost:9000"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tải Parquet offline lên bucket nguồn MinIO.")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="URL S3/MinIO, ví dụ http://localhost:9000.")
    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--dry-run", action="store_true", help="In kế hoạch upload, không kết nối MinIO.")
    parser.add_argument("--preview", type=int, default=8, help="Số object key in chi tiết khi dry-run.")
    return parser


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = build_parser().parse_args(argv)
    if args.preview < 0:
        build_parser().error("--preview phải >= 0")
    source = args.source_dir.resolve()
    if not source.is_dir():
        raise SystemExit(f"không thấy thư mục nguồn: {source}")

    objects = _collect_parquet(source)
    if not objects:
        raise SystemExit(f"không có file Parquet trong {source}")

    total_bytes = sum(size for _path, _key, size in objects)
    by_prefix = _bytes_by_prefix(objects)
    _log_plan(args, source, objects, total_bytes, by_prefix)
    if args.dry_run:
        logger.info("dry-run: không mở kết nối tới %s", args.endpoint)
        return
    _upload(args, objects)
    logger.info("đã tải %d object (%s bytes) lên s3://%s/", len(objects), f"{total_bytes:,}", args.bucket)


def _collect_parquet(source: Path) -> list[tuple[Path, str, int]]:
    found: list[tuple[Path, str, int]] = []
    for path in sorted(source.rglob("*")):
        if path.is_file() and path.suffix == ".parquet":
            key = path.relative_to(source).as_posix()
            found.append((path, key, path.stat().st_size))
    return found


def _bytes_by_prefix(objects: list[tuple[Path, str, int]]) -> dict[str, tuple[int, int]]:
    counts: dict[str, int] = defaultdict(int)
    sizes: dict[str, int] = defaultdict(int)
    for _path, key, size in objects:
        prefix = key.split("/", 1)[0]
        counts[prefix] += 1
        sizes[prefix] += size
    return {prefix: (counts[prefix], sizes[prefix]) for prefix in sorted(counts)}


def _log_plan(
    args: argparse.Namespace,
    source: Path,
    objects: list[tuple[Path, str, int]],
    total_bytes: int,
    by_prefix: dict[str, tuple[int, int]],
) -> None:
    lines = [
        "========== upload external-source-lake ==========",
        f"source_dir          : {source}",
        f"destination         : s3://{args.bucket}/",
        f"endpoint            : {args.endpoint}",
        f"objects             : {len(objects):,}",
        f"bytes               : {total_bytes:,}",
        "theo thư mục:",
    ]
    for prefix, (count, size) in by_prefix.items():
        lines.append(f"  {prefix:<16} {count:>4} file   {size:>12,} bytes")
    shown = objects if args.preview == 0 else objects[: args.preview]
    lines.append("object key mẫu:")
    for _path, key, size in shown:
        lines.append(f"  s3://{args.bucket}/{key}   ({size:,} bytes)")
    if len(shown) < len(objects):
        lines.append(f"  ... {len(objects) - len(shown)} object nữa. Dùng --preview 0 để liệt kê hết.")
    lines.append("================================================")
    logger.info("\n".join(lines))


def _upload(args: argparse.Namespace, objects: list[tuple[Path, str, int]]) -> None:
    try:
        import boto3
        from botocore.client import Config
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise SystemExit(
            "chưa cài boto3. Dry-run không cần thư viện này. "
            "Để tải thật: pip install boto3"
        ) from exc

    client = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    _ensure_bucket(client, args.bucket, ClientError)
    for path, key, _size in objects:
        client.upload_file(str(path), args.bucket, key)


def _ensure_bucket(client: object, bucket: str, client_error: type[Exception]) -> None:
    try:
        client.head_bucket(Bucket=bucket)  # type: ignore[attr-defined]
        return
    except client_error:
        pass
    try:
        client.create_bucket(Bucket=bucket)  # type: ignore[attr-defined]
    except client_error as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise


if __name__ == "__main__":
    main()
