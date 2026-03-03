from __future__ import annotations

import argparse
import json
import logging
import sys

from app.ingest.runner import FatalIngestionError, run_ingestion


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily batch runner")
    parser.add_argument("--run", choices=["daily"], required=True)
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.run != "daily":
        logging.error("unsupported run mode: %s", args.run)
        return 2

    try:
        stage_1 = run_ingestion(
            sources_path="config/sources.yaml",
            pipeline_path="config/pipeline.yaml",
        )
    except FatalIngestionError as exc:
        logging.error("stage 1 fatal error: %s", exc)
        return 2

    payload = {"stage_1": stage_1.to_dict()}
    print(json.dumps(payload, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
