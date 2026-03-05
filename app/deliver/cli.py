from __future__ import annotations

import argparse
import json
import logging
import sys

from .runner import FatalDeliverError, run_deliver


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 8 deliver runner")
    parser.add_argument("--pipeline", required=True, help="Path to pipeline YAML.")
    parser.add_argument("--db", help="Override paths.sqlite_db.")
    parser.add_argument("--max-items", help="Override deliver.max_items_per_run.")
    parser.add_argument("--dry-run", action="store_true", help="Force dry run mode.")
    parser.add_argument("--report", help="Output stage_8_report JSON path.")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        result = run_deliver(
            pipeline_path=args.pipeline,
            db_path_override=args.db,
            max_items_override=args.max_items,
            dry_run_override=bool(args.dry_run),
            report_path=args.report,
        )
    except FatalDeliverError as exc:
        logging.error("fatal stage_8_deliver error: %s", exc)
        return 2

    print(json.dumps(result.to_dict(), ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

