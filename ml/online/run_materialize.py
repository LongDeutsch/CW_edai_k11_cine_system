"""Chạy materialize trong process đã có Feast trên PYTHONPATH.

Airflow scheduler gọi module này bằng Python của image và ``/opt/feast-packages``,
không cài Feast vào site-packages của Airflow.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ml.online.materialize import materialize_export


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-dir", required=True)
    parser.add_argument("--repo-dir", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--redis", required=True)
    args = parser.parse_args(argv)
    result = materialize_export(Path(args.export_dir), Path(args.repo_dir), Path(args.state_dir), redis=args.redis)
    print("RESULT_JSON=" + json.dumps(result, default=str))


if __name__ == "__main__":
    main()
