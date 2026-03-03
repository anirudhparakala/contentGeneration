from __future__ import annotations

import argparse
import json
import logging
import sys

from .runner import FatalIngestionError, run_ingestion


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 1 ingestion runner")
    parser.add_argument("--config", required=True, help="Path to sources.yaml")
    parser.add_argument("--pipeline", required=True, help="Path to pipeline.yaml")
    parser.add_argument("--out", help="Output JSONL path. Overwritten each run.")
    parser.add_argument("--report", help="Output run report JSON path.")
    parser.add_argument("--db", help="Override sqlite DB path.")
    parser.add_argument("--max-per-source", type=int, help="Override cap per source.")
    parser.add_argument("--recency-days", type=int, help="Override recency days.")
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
        result = run_ingestion(
            sources_path=args.config,
            pipeline_path=args.pipeline,
            out_path=args.out,
            report_path=args.report,
            db_path_override=args.db,
            max_per_source_override=args.max_per_source,
            recency_days_override=args.recency_days,
        )
    except FatalIngestionError as exc:
        logging.error("fatal ingestion error: %s", exc)
        return 2

    print(json.dumps(result.to_dict(), ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
