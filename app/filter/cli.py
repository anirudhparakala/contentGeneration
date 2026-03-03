from __future__ import annotations

import argparse
import json
import logging
import sys

from .runner import FatalFilterError, run_filter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 3 filter + dedup runner")
    parser.add_argument("--pipeline", required=True, help="Path to pipeline.yaml")
    parser.add_argument("--out", help="Output CandidateItem JSONL path. Overwritten each run.")
    parser.add_argument("--report", help="Output stage_3_report JSON path.")
    parser.add_argument("--db", help="Override sqlite DB path.")
    parser.add_argument("--max-candidates", type=int, help="Override stage_3_filter.max_candidates_default")
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
        result = run_filter(
            pipeline_path=args.pipeline,
            out_path=args.out,
            report_path=args.report,
            db_path_override=args.db,
            max_candidates_override=args.max_candidates,
        )
    except FatalFilterError as exc:
        logging.error("fatal filter error: %s", exc)
        return 2

    print(json.dumps(result.to_dict(), ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
