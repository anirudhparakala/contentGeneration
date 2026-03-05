from __future__ import annotations

import argparse
import json
import logging
import sys

from .runner import FatalPersistError, run_persist


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 7 persist-to-sheet runner")
    parser.add_argument("--pipeline", required=True, help="Path to pipeline.yaml")
    parser.add_argument("--db", help="Override sqlite DB path.")
    parser.add_argument("--sheet-id", help="Override sheets.spreadsheet_id")
    parser.add_argument("--worksheet", help="Override sheets.worksheet_name")
    parser.add_argument("--max-rows", help="Override stage_7_persist.max_rows_default")
    parser.add_argument("--report", help="Output stage_7_report JSON path.")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        result = run_persist(
            pipeline_path=args.pipeline,
            db_path_override=args.db,
            sheet_id_override=args.sheet_id,
            worksheet_override=args.worksheet,
            max_rows_override=args.max_rows,
            report_path=args.report,
        )
    except FatalPersistError as exc:
        logging.error("fatal stage_7_persist error: %s", exc)
        return 2

    print(json.dumps(result.to_dict(), ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

