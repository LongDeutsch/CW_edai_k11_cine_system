"""Mô phỏng bin-packing: gom file nhỏ thành file mục tiêu 128MB.

Thuật toán First Fit Decreasing:

1. Sắp file giảm dần theo kích thước.
2. Nhét file vào bin đã mở đầu tiên còn đủ chỗ.
3. Không đủ chỗ thì mở bin mới.
4. File lớn hơn mục tiêu đứng một mình. Parquet không bị cắt đôi.

Script không ghi lại Parquet. Nó in kế hoạch gom file.

Chạy thử::

    python -m storage.lakehouse.maintenance.compaction --target-mb 128
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

TARGET_BYTES = 128 * 1024 * 1024

# Bộ file giảng dạy: sau khi xếp giảm dần, ba bin đầu chạm đúng 128MB.
DEMO_FILES_MB: tuple[tuple[str, int], ...] = (
    ("part-000.parquet", 60),
    ("part-001.parquet", 50),
    ("part-002.parquet", 40),
    ("part-003.parquet", 40),
    ("part-004.parquet", 30),
    ("part-005.parquet", 20),
    ("part-006.parquet", 20),
    ("part-007.parquet", 15),
    ("part-008.parquet", 10),
    ("part-009.parquet", 8),
    ("part-010.parquet", 5),
    ("part-011.parquet", 4),
    ("part-012.parquet", 3),
    ("part-013.parquet", 2),
    ("part-014.parquet", 1),
    ("part-015.parquet", 1),
)


@dataclass(frozen=True)
class FileStat:
    name: str
    size_bytes: int


@dataclass(frozen=True)
class Bin:
    files: tuple[FileStat, ...]

    @property
    def size_bytes(self) -> int:
        return sum(item.size_bytes for item in self.files)


def pack_bins(files: list[FileStat], target_bytes: int) -> list[Bin]:
    """Xếp file vào các bin không vượt ``target_bytes``, trừ file quá khổ."""
    if target_bytes < 1:
        raise ValueError("target_bytes phải >= 1")
    ordered = sorted(files, key=lambda item: (-item.size_bytes, item.name))
    open_bins: list[list[FileStat]] = []
    for item in ordered:
        placed = False
        if item.size_bytes <= target_bytes:
            for bucket in open_bins:
                used = sum(current.size_bytes for current in bucket)
                if used + item.size_bytes <= target_bytes:
                    bucket.append(item)
                    placed = True
                    break
        if not placed:
            open_bins.append([item])
    return [Bin(tuple(bucket)) for bucket in open_bins]


def demo_files() -> list[FileStat]:
    return [FileStat(name, size_mb * 1024 * 1024) for name, size_mb in DEMO_FILES_MB]


def scan_directory(path: Path) -> list[FileStat]:
    """Đọc kích thước file thật trong một thư mục output."""
    if not path.is_dir():
        raise SystemExit(f"không thấy thư mục: {path}")
    found = [
        FileStat(item.relative_to(path).as_posix(), item.stat().st_size)
        for item in sorted(path.rglob("*"))
        if item.is_file()
    ]
    if not found:
        raise SystemExit(f"thư mục không có file: {path}")
    return found


def render_plan(files: list[FileStat], bins: list[Bin], target_bytes: int) -> str:
    lines = [
        f"file đầu vào : {len(files)}",
        f"tổng dung lượng: {_fmt_bytes(sum(item.size_bytes for item in files))}",
        f"mục tiêu mỗi file: {_fmt_bytes(target_bytes)}",
        f"file sau gom  : {len(bins)}",
        "",
    ]
    for index, bucket in enumerate(bins):
        names = ", ".join(item.name for item in bucket.files)
        mark = "quá khổ, giữ nguyên" if len(bucket.files) == 1 and bucket.size_bytes > target_bytes else "đã gom"
        lines.append(
            f"bin {index:02d}  {_fmt_bytes(bucket.size_bytes):>10}  {len(bucket.files):>3} file  {mark}"
        )
        lines.append(f"         {names}")
    return "\n".join(lines)


def _fmt_bytes(size: int) -> str:
    if size >= 1024 * 1024 and size % (1024 * 1024) == 0:
        return f"{size // (1024 * 1024)}MB"
    if size >= 1024 * 1024:
        return f"{size / (1024 * 1024):.1f}MB"
    if size >= 1024:
        return f"{size / 1024:.1f}KB"
    return f"{size}B"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mô phỏng gom file nhỏ thành file 128MB.")
    parser.add_argument("--target-mb", type=float, default=128.0, help="Dung lượng mục tiêu của một file sau gom.")
    parser.add_argument("--input-dir", type=Path, default=None, help="Quét file thật thay cho bộ file giảng dạy.")
    parser.add_argument("--include-oversize", action="store_true", help="Thêm một file 200MB để thấy bin không cắt file.")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    if args.target_mb <= 0:
        build_parser().error("--target-mb phải > 0")
    target = int(args.target_mb * 1024 * 1024)
    files = scan_directory(args.input_dir) if args.input_dir else demo_files()
    if args.include_oversize and args.input_dir is None:
        files.append(FileStat("part-oversize.parquet", 200 * 1024 * 1024))
    bins = pack_bins(files, target)
    print(render_plan(files, bins, target))


if __name__ == "__main__":
    main()
