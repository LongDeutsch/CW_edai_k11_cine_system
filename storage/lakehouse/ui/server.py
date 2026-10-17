"""UI đọc luồng Lakehouse: ba zone, SCD2 và kế hoạch gom file.

Không cần Docker::

    python -m storage.lakehouse.ui.server

Rồi mở http://127.0.0.1:8088
"""

from __future__ import annotations

import argparse
import html
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from storage.lakehouse.catalog import tables_in
from storage.lakehouse.maintenance.compaction import demo_files, pack_bins
from storage.lakehouse.scd.apply_scd2 import DimUser, apply_scd2, demo_changes, demo_snapshot

TARGET_BYTES = 128 * 1024 * 1024


def build_page() -> str:
    before = demo_snapshot()
    after = apply_scd2(before, demo_changes())
    bins = pack_bins(demo_files(), TARGET_BYTES)
    return _page(before, after, bins)


def _page(before: list[DimUser], after: list[DimUser], bins: list) -> str:
    zones = "".join(_zone(name) for name in ("bronze", "silver", "gold"))
    bin_rows = "".join(_bin_row(index, bucket.size_bytes, len(bucket.files)) for index, bucket in enumerate(bins))
    return f"""<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <title>CineStream Lakehouse</title>
  <style>
    body {{ margin: 0; font-family: Georgia, "Iowan Old Style", serif; background: #f4f0e6; color: #1c1915; }}
    main {{ max-width: 1080px; margin: 0 auto; padding: 32px 20px 64px; }}
    h1 {{ font-weight: 500; font-size: 2rem; margin-bottom: 0.2rem; }}
    p {{ line-height: 1.45; }}
    .zones {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }}
    .zone {{ background: #fffdf8; border: 1px solid #d9d0c1; padding: 12px 14px; }}
    .zone h2 {{ margin: 0 0 8px; font-size: 1rem; letter-spacing: 0.04em; text-transform: uppercase; }}
    .bronze h2 {{ color: #8a4b08; }}
    .silver h2 {{ color: #4d5560; }}
    .gold h2 {{ color: #8a6a12; }}
    table {{ width: 100%; border-collapse: collapse; background: #fffdf8; font-family: ui-monospace, monospace; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #e4dccb; text-align: left; padding: 6px 8px; }}
    th {{ font-family: Georgia, serif; font-weight: 600; }}
    .bar {{ height: 18px; background: #c4a15a; }}
    .bin {{ display: grid; grid-template-columns: 72px 1fr 120px; gap: 8px; align-items: center; margin: 6px 0; }}
    @media (max-width: 800px) {{ .zones {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
<main>
  <h1>Luồng Lakehouse</h1>
  <p>Nguồn ngoài đi vào Bronze dưới dạng thêm-đuôi. Silver khử trùng và hợp nhất schema. Gold giữ lịch sử gói cước bằng SCD Type 2, cùng fact, feature và bảng phẳng.</p>
  <section class="zones">{zones}</section>
  <h2>SCD Type 2 — 5 user đổi gói ngày 2026-06-01</h2>
  <p>Dòng cũ được đóng. Dòng mới giữ <code>user_id</code> và quốc gia, nhận <code>user_sk</code> mới.</p>
  <h3>Trước</h3>
  {_scd_table(before)}
  <h3>Sau</h3>
  {_scd_table(after)}
  <h2>Compaction — 16 file nhỏ thành {len(bins)} file mục tiêu 128MB</h2>
  {bin_rows}
</main>
</body>
</html>
"""


def _zone(name: str) -> str:
    items = "".join(f"<li><code>{html.escape(table.name)}</code> — {html.escape(table.grain)}</li>" for table in tables_in(name))
    return f'<article class="zone {html.escape(name)}"><h2>{html.escape(name)}</h2><ul>{items}</ul></article>'


def _scd_table(rows: list[DimUser]) -> str:
    body = []
    ordered = sorted(rows, key=lambda row: (row.user_id, row.valid_from_ts, row.user_sk))
    for row in ordered:
        valid_to = "NULL" if row.valid_to_ts is None else row.valid_to_ts.strftime("%Y-%m-%d")
        body.append(
            "<tr>"
            f"<td>{row.user_sk}</td><td>{row.user_id}</td><td>{html.escape(row.country)}</td>"
            f"<td>{html.escape(row.subscription_tier)}</td>"
            f"<td>{row.valid_from_ts.strftime('%Y-%m-%d')}</td><td>{valid_to}</td>"
            f"<td>{str(row.is_current).lower()}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>user_sk</th><th>user_id</th><th>country</th><th>tier</th>"
        "<th>valid_from</th><th>valid_to</th><th>is_current</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def _bin_row(index: int, size_bytes: int, n_files: int) -> str:
    width = min(100, round(100 * size_bytes / TARGET_BYTES))
    label = f"{size_bytes // (1024 * 1024)}MB"
    return (
        f'<div class="bin"><span>bin {index:02d}</span>'
        f'<div class="bar" style="width:{width}%"></div>'
        f"<span>{n_files} file · {label}</span></div>"
    )


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path in {"/", "/index.html"}:
            body = build_page().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/health":
            payload = b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[lakehouse-ui] {self.address_string()} {fmt % args}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Mở UI luồng Lakehouse.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8088)
    args = parser.parse_args(argv)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Lakehouse UI: http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nĐã dừng UI.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
