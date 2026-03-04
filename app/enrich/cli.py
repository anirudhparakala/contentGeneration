from __future__ import annotations

import argparse
import json
import logging
import sys

from .runner import FatalEnrichError, run_enrich


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 4 enrich runner")
    parser.add_argument("--pipeline", required=True, help="Path to pipeline.yaml")
    parser.add_argument("--out", help="Output EnrichedItem JSONL path. Overwritten each run.")
    parser.add_argument("--report", help="Output stage_4_report JSON path.")
    parser.add_argument("--db", help="Override sqlite DB path.")
    parser.add_argument("--max-items", help="Max candidates selected for this run.")
    parser.add_argument("--max-transcripts", help="Override caps.max_transcripts_per_run")
    parser.add_argument("--max-asr", help="Override caps.max_asr_fallbacks_per_run")
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
        max_items = _parse_optional_non_negative_int(args.max_items, "--max-items")
        max_transcripts = _parse_optional_non_negative_int(
            args.max_transcripts, "--max-transcripts"
        )
        max_asr = _parse_optional_non_negative_int(args.max_asr, "--max-asr")
    except ValueError as exc:
        logging.error("fatal enrich error: %s", exc)
        return 2

    try:
        result = run_enrich(
            pipeline_path=args.pipeline,
            out_path=args.out,
            report_path=args.report,
            db_path_override=args.db,
            max_items_override=max_items,
            max_transcripts_override=max_transcripts,
            max_asr_override=max_asr,
        )
    except FatalEnrichError as exc:
        logging.error("fatal enrich error: %s", exc)
        return 2

    print(json.dumps(result.to_dict(), ensure_ascii=True))
    return 0


def _parse_optional_non_negative_int(value: str | None, flag: str) -> int | None:
    if value is None:
        return None
    if value.strip().lower() in {"true", "false"}:
        raise ValueError(f"{flag} must be a non-boolean integer >= 0")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{flag} must be a non-boolean integer >= 0") from exc
    if parsed < 0:
        raise ValueError(f"{flag} must be a non-boolean integer >= 0")
    return parsed


if __name__ == "__main__":
    sys.exit(main())
