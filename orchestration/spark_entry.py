"""Điểm vào spark-submit. Không viết lại phép biến đổi của Phase 1."""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        raise SystemExit("cần ingest, publish, validate-bronze, validate-gold hoặc features")
    command, rest = args[0], args[1:]
    if command == "ingest":
        from transformation.batch.jobs.ingest_to_bronze import main as run

        run(rest)
        return
    if command == "publish":
        from transformation.batch.jobs.publish_silver_gold import main as run

        run(rest)
        return
    if command == "validate-bronze":
        from transformation.batch.jobs.validate_lakehouse import validate_bronze

        validate_bronze(rest)
        return
    if command == "validate-gold":
        from transformation.batch.jobs.validate_lakehouse import validate_gold

        validate_gold(rest)
        return
    if command == "features":
        from transformation.batch.jobs.build_offline_features import main as run

        run(rest)
        return
    raise SystemExit(f"lệnh không hỗ trợ: {command}")


if __name__ == "__main__":
    main()
