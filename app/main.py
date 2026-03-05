from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


LOGGER = logging.getLogger(__name__)
STAGE_KEYS: tuple[str, ...] = (
    "stage_1",
    "stage_2",
    "stage_3",
    "stage_4",
    "stage_5",
    "stage_6",
    "stage_7",
    "stage_8",
)
STAGES_WITH_RUN_STATUS: frozenset[str] = frozenset(
    {"stage_4", "stage_5", "stage_6", "stage_7", "stage_8"}
)
VALID_STAGE_RUN_STATUS: frozenset[str] = frozenset({"completed", "fatal"})
VALID_STAGE_STATUS: frozenset[str] = frozenset({"completed", "failed", "skipped"})
VALID_PIPELINE_STATUS: frozenset[str] = frozenset({"completed", "stopped", "failed"})
STAGE_REQUIRED_STRING_FIELDS: dict[str, tuple[str, ...]] = {
    "stage_1": ("raw_items_path", "report_path"),
    "stage_2": ("report_path",),
    "stage_3": ("report_path",),
    "stage_4": ("report_path",),
    "stage_5": ("report_path",),
    "stage_6": ("report_path",),
    "stage_7": ("report_path",),
    "stage_8": ("report_path",),
}
STAGE_REQUIRED_INT_FIELDS: dict[str, tuple[str, ...]] = {
    "stage_1": ("total_new_items_emitted", "sources_failed"),
    "stage_2": ("items_inserted_db", "items_skipped_invalid"),
    "stage_3": ("inserted_db",),
    "stage_4": ("inserted_db", "failed_count"),
    "stage_5": ("inserted_db", "failed_count"),
    "stage_6": ("inserted_db", "failed_count"),
    "stage_7": ("rows_inserted", "rows_updated", "errors_count"),
    "stage_8": ("items_sent", "errors_count"),
}


class FatalOpsError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily batch runner (stage 9 ops)")
    parser.add_argument("--run")
    parser.add_argument("--pipeline", default="config/pipeline.yaml")
    parser.add_argument("--sources", default="config/sources.yaml")
    parser.add_argument("--report")
    parser.add_argument("--report-md")
    parser.add_argument("--stop-after")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    level_name = str(args.log_level).strip().upper() if args.log_level is not None else "INFO"
    level = getattr(logging, level_name, logging.INFO)
    if not isinstance(level, int):
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    started_dt = _utc_now()
    run_id = str(uuid.uuid4())
    started_at = _to_utc_z(started_dt)

    stage_reports: dict[str, str | None] = {stage_key: None for stage_key in STAGE_KEYS}
    stage_status: dict[str, str] = {stage_key: "skipped" for stage_key in STAGE_KEYS}
    stage_errors: dict[str, str | None] = {stage_key: None for stage_key in STAGE_KEYS}
    stage_results: dict[str, Any] = {}

    pipeline_status = "failed"
    fatal_stage: str | None = None
    fatal_error: str | None = None
    stop_after: int | None = None
    pipeline_path = ""
    sources_path = ""
    db_path = ""
    report_md_path: Path | None = None
    final_report_path: Path | None = None

    try:
        run_mode = _parse_required_non_empty_string(args.run, "--run")
        if run_mode != "daily":
            raise FatalOpsError("--run must be exactly 'daily'")

        pipeline_path = _parse_required_non_empty_string(args.pipeline, "--pipeline")
        sources_path = _parse_required_non_empty_string(args.sources, "--sources")

        report_override, report_override_error = _parse_optional_path_override(args.report, "--report")
        if report_override_error is not None:
            raise FatalOpsError(report_override_error)

        pipeline_outputs_dir, db_path_candidate = _try_load_pipeline_paths(pipeline_path)
        if db_path_candidate is not None:
            db_path = db_path_candidate
        final_report_path = _resolve_final_report_path(
            started_dt=started_dt,
            report_override=report_override,
            pipeline_outputs_dir=pipeline_outputs_dir,
        )

        report_md_override, report_md_error = _parse_optional_path_override(
            args.report_md,
            "--report-md",
        )
        if report_md_error is not None:
            raise FatalOpsError(report_md_error)
        report_md_path = Path(report_md_override) if report_md_override is not None else None

        stop_after = _parse_optional_stop_after(args.stop_after)

        for stage_index, stage_key in enumerate(STAGE_KEYS, start=1):
            if stop_after is not None and stage_index > stop_after:
                break

            stage_result: Any
            try:
                stage_result = _run_stage(
                    stage_key=stage_key,
                    pipeline_path=pipeline_path,
                    sources_path=sources_path,
                    stage_results=stage_results,
                )
            except Exception as exc:
                fatal_stage, fatal_error = _mark_stage_failed(
                    stage_key=stage_key,
                    error=_fatal_message(exc),
                    stage_status=stage_status,
                    stage_errors=stage_errors,
                )
                pipeline_status = "failed"
                break

            _validate_stage_result_contract(stage_key=stage_key, stage_result=stage_result)
            stage_results[stage_key] = stage_result
            stage_reports[stage_key] = _get_required_string_field(
                stage_result,
                "report_path",
                context=f"{stage_key}.report_path",
            )

            if stage_key in STAGES_WITH_RUN_STATUS:
                run_status = _get_required_string_field(
                    stage_result,
                    "run_status",
                    context=f"{stage_key}.run_status",
                )
                if run_status not in VALID_STAGE_RUN_STATUS:
                    raise FatalOpsError(f"{stage_key}.run_status must be completed|fatal")
                if run_status == "fatal":
                    stage_fatal_error = _get_required_string_field(
                        stage_result,
                        "fatal_error",
                        context=f"{stage_key}.fatal_error",
                    )
                    fatal_stage, fatal_error = _mark_stage_failed(
                        stage_key=stage_key,
                        error=stage_fatal_error,
                        stage_status=stage_status,
                        stage_errors=stage_errors,
                    )
                    pipeline_status = "failed"
                    break

            stage_status[stage_key] = "completed"

        if pipeline_status != "failed":
            if stop_after is not None:
                pipeline_status = "stopped"
            else:
                pipeline_status = "completed"
            fatal_stage = None
            fatal_error = None

    except FatalOpsError as exc:
        failure_stage = _first_not_completed_stage(stage_status)
        fatal_stage, fatal_error = _mark_stage_failed(
            stage_key=failure_stage,
            error=str(exc),
            stage_status=stage_status,
            stage_errors=stage_errors,
        )
        pipeline_status = "failed"
    except Exception as exc:
        failure_stage = _first_not_completed_stage(stage_status)
        fatal_stage, fatal_error = _mark_stage_failed(
            stage_key=failure_stage,
            error=_fatal_message(exc),
            stage_status=stage_status,
            stage_errors=stage_errors,
        )
        pipeline_status = "failed"

    key_metrics = _build_key_metrics(stage_results)
    non_fatal_by_stage = _build_non_fatal_by_stage(stage_results)
    non_fatal_errors_count = sum(non_fatal_by_stage.values())

    finished_at = _to_utc_z(_utc_now())
    final_payload = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "pipeline_status": pipeline_status,
        "fatal_stage": fatal_stage,
        "fatal_error": fatal_error,
        "stop_after": stop_after,
        "pipeline_path": pipeline_path,
        "sources_path": sources_path,
        "db_path": db_path,
        "final_report_path": str(final_report_path) if final_report_path is not None else "",
        "stage_reports": stage_reports,
        "stage_status": stage_status,
        "stage_errors": stage_errors,
        "key_metrics": key_metrics,
        "errors_summary": {
            "non_fatal_by_stage": non_fatal_by_stage,
            "non_fatal_errors_count": non_fatal_errors_count,
        },
    }

    markdown_write_error: str | None = None
    if final_report_path is not None and report_md_path is not None:
        try:
            _write_markdown_report(path=report_md_path, payload=final_payload)
        except OSError as exc:
            markdown_write_error = f"failed writing markdown report: {exc}"
            LOGGER.error("stage_9 markdown report write failed: %s", exc)
            _apply_output_write_failure(payload=final_payload, error=markdown_write_error)

    try:
        _validate_final_report_invariants(final_payload)
    except FatalOpsError as exc:
        LOGGER.error("stage_9 invariant failure: %s", exc)
        print(json.dumps(final_payload, ensure_ascii=True))
        return 2

    if final_report_path is not None:
        try:
            _write_json(path=final_report_path, payload=final_payload)
        except OSError as exc:
            LOGGER.error("stage_9 final report write failed: %s", exc)
            print(json.dumps(final_payload, ensure_ascii=True))
            return 2
    else:
        LOGGER.error("stage_9 final report path unresolved; skipped final report write")

    print(json.dumps(final_payload, ensure_ascii=True))
    if markdown_write_error is not None:
        return 2
    if pipeline_status in {"completed", "stopped"}:
        return 0
    return 2


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _to_utc_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_required_non_empty_string(value: Any, option_name: str) -> str:
    if not isinstance(value, str):
        raise FatalOpsError(f"{option_name} must be a non-empty string")
    trimmed = value.strip()
    if not trimmed:
        raise FatalOpsError(f"{option_name} must be a non-empty string")
    return trimmed


def _parse_optional_path_override(value: Any, option_name: str) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    if not isinstance(value, str):
        return None, f"{option_name} must be a non-empty string"
    trimmed = value.strip()
    if not trimmed:
        return None, f"{option_name} must be a non-empty string"
    return trimmed, None


def _parse_optional_stop_after(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise FatalOpsError("--stop-after must be an integer in [1, 8]")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            raise FatalOpsError("--stop-after must be an integer in [1, 8]")
        try:
            parsed = int(trimmed)
        except ValueError as exc:
            raise FatalOpsError("--stop-after must be an integer in [1, 8]") from exc
    else:
        raise FatalOpsError("--stop-after must be an integer in [1, 8]")

    if not _is_non_bool_int(parsed) or parsed < 1 or parsed > 8:
        raise FatalOpsError("--stop-after must be an integer in [1, 8]")
    return parsed


def _try_load_pipeline_paths(pipeline_path: str) -> tuple[str | None, str | None]:
    try:
        with Path(pipeline_path).open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError):
        return None, None

    if not isinstance(raw, dict):
        return None, None
    paths = raw.get("paths")
    if not isinstance(paths, dict):
        return None, None

    outputs_dir = _normalize_optional_non_empty_string(paths.get("outputs_dir"))
    db_path = _normalize_optional_non_empty_string(paths.get("sqlite_db"))
    return outputs_dir, db_path


def _normalize_optional_non_empty_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed


def _resolve_final_report_path(
    *, started_dt: datetime, report_override: str | None, pipeline_outputs_dir: str | None
) -> Path:
    if report_override is not None:
        return Path(report_override)
    outputs_dir = pipeline_outputs_dir if pipeline_outputs_dir is not None else "data/outputs"
    date_token = started_dt.strftime("%Y-%m-%d")
    return Path(outputs_dir) / f"final_report_{date_token}.json"


def _run_stage(
    *,
    stage_key: str,
    pipeline_path: str,
    sources_path: str,
    stage_results: dict[str, Any],
) -> Any:
    if stage_key == "stage_1":
        from app.ingest.runner import run_ingestion

        return run_ingestion(sources_path=sources_path, pipeline_path=pipeline_path)
    if stage_key == "stage_2":
        from app.normalize.runner import run_normalize

        stage_1_result = stage_results.get("stage_1")
        if stage_1_result is None:
            raise FatalOpsError("stage_1 result missing for stage_2 in_path binding")
        raw_items_path = _get_required_string_field(
            stage_1_result,
            "raw_items_path",
            context="stage_1.raw_items_path",
        )
        return run_normalize(pipeline_path=pipeline_path, in_path=raw_items_path)
    if stage_key == "stage_3":
        from app.filter.runner import run_filter

        return run_filter(pipeline_path=pipeline_path)
    if stage_key == "stage_4":
        from app.enrich.runner import run_enrich

        return run_enrich(pipeline_path=pipeline_path)
    if stage_key == "stage_5":
        from app.intelligence.runner import run_intelligence

        return run_intelligence(pipeline_path=pipeline_path)
    if stage_key == "stage_6":
        from app.generate.runner import run_generate

        return run_generate(pipeline_path=pipeline_path)
    if stage_key == "stage_7":
        from app.sheets.runner import run_persist

        return run_persist(pipeline_path=pipeline_path)
    if stage_key == "stage_8":
        from app.deliver.runner import run_deliver

        return run_deliver(pipeline_path=pipeline_path)
    raise FatalOpsError(f"unknown stage key: {stage_key}")


def _validate_stage_result_contract(*, stage_key: str, stage_result: Any) -> None:
    for field_name in STAGE_REQUIRED_STRING_FIELDS.get(stage_key, ()):
        _get_required_string_field(stage_result, field_name, context=f"{stage_key}.{field_name}")
    for field_name in STAGE_REQUIRED_INT_FIELDS.get(stage_key, ()):
        _get_required_non_negative_int_field(stage_result, field_name, context=f"{stage_key}.{field_name}")

    if stage_key in STAGES_WITH_RUN_STATUS:
        run_status = _get_required_string_field(
            stage_result,
            "run_status",
            context=f"{stage_key}.run_status",
        )
        if run_status not in VALID_STAGE_RUN_STATUS:
            raise FatalOpsError(f"{stage_key}.run_status must be completed|fatal")
        if run_status == "fatal":
            _get_required_string_field(stage_result, "fatal_error", context=f"{stage_key}.fatal_error")


def _get_required_string_field(stage_result: Any, field_name: str, *, context: str) -> str:
    if not hasattr(stage_result, field_name):
        raise FatalOpsError(f"{context} missing from stage result")
    return _parse_required_non_empty_string(getattr(stage_result, field_name), context)


def _get_required_non_negative_int_field(stage_result: Any, field_name: str, *, context: str) -> int:
    if not hasattr(stage_result, field_name):
        raise FatalOpsError(f"{context} missing from stage result")
    value = getattr(stage_result, field_name)
    if not _is_non_bool_int(value) or int(value) < 0:
        raise FatalOpsError(f"{context} must be a non-boolean integer >= 0")
    return int(value)


def _is_non_bool_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _mark_stage_failed(
    *,
    stage_key: str,
    error: str,
    stage_status: dict[str, str],
    stage_errors: dict[str, str | None],
) -> tuple[str, str]:
    message = error.strip() if isinstance(error, str) else ""
    if not message:
        message = f"{stage_key} failed"
    stage_status[stage_key] = "failed"
    stage_errors[stage_key] = message
    return stage_key, message


def _apply_output_write_failure(*, payload: dict[str, Any], error: str) -> None:
    message = error.strip() if isinstance(error, str) else ""
    if not message:
        message = "stage_9 output write failed"

    stage_status = payload["stage_status"]
    stage_errors = payload["stage_errors"]
    if payload.get("pipeline_status") == "failed":
        stage_key = payload.get("fatal_stage")
        if not isinstance(stage_key, str) or stage_key not in STAGE_KEYS:
            stage_key = _first_not_completed_stage(stage_status)
            stage_status[stage_key] = "failed"
        existing_fatal_error = payload.get("fatal_error")
        if isinstance(existing_fatal_error, str) and existing_fatal_error.strip():
            message = f"{existing_fatal_error}; {message}"
    else:
        stage_key = _first_not_completed_stage(stage_status)
        stage_status[stage_key] = "failed"
        payload["pipeline_status"] = "failed"

    stage_errors[stage_key] = message
    payload["fatal_stage"] = stage_key
    payload["fatal_error"] = message


def _first_not_completed_stage(stage_status: dict[str, str]) -> str:
    for stage_key in STAGE_KEYS:
        if stage_status.get(stage_key) != "completed":
            return stage_key
    return STAGE_KEYS[-1]


def _build_key_metrics(stage_results: dict[str, Any]) -> dict[str, int]:
    return {
        "raw_items_emitted": _metric_from_stage(stage_results, "stage_1", "total_new_items_emitted"),
        "canonical_items_inserted": _metric_from_stage(stage_results, "stage_2", "items_inserted_db"),
        "candidates_inserted": _metric_from_stage(stage_results, "stage_3", "inserted_db"),
        "enriched_inserted": _metric_from_stage(stage_results, "stage_4", "inserted_db"),
        "ideas_inserted": _metric_from_stage(stage_results, "stage_5", "inserted_db"),
        "scripts_inserted": _metric_from_stage(stage_results, "stage_6", "inserted_db"),
        "sheet_rows_inserted": _metric_from_stage(stage_results, "stage_7", "rows_inserted"),
        "sheet_rows_updated": _metric_from_stage(stage_results, "stage_7", "rows_updated"),
        "slack_sent": _metric_from_stage(stage_results, "stage_8", "items_sent"),
    }


def _build_non_fatal_by_stage(stage_results: dict[str, Any]) -> dict[str, int]:
    return {
        "stage_1": _metric_from_stage(stage_results, "stage_1", "sources_failed"),
        "stage_2": _metric_from_stage(stage_results, "stage_2", "items_skipped_invalid"),
        "stage_3": 0,
        "stage_4": _metric_from_stage(stage_results, "stage_4", "failed_count"),
        "stage_5": _metric_from_stage(stage_results, "stage_5", "failed_count"),
        "stage_6": _metric_from_stage(stage_results, "stage_6", "failed_count"),
        "stage_7": _metric_from_stage(stage_results, "stage_7", "errors_count"),
        "stage_8": _metric_from_stage(stage_results, "stage_8", "errors_count"),
    }


def _metric_from_stage(stage_results: dict[str, Any], stage_key: str, field_name: str) -> int:
    stage_result = stage_results.get(stage_key)
    if stage_result is None:
        return 0
    return _get_required_non_negative_int_field(
        stage_result,
        field_name,
        context=f"{stage_key}.{field_name}",
    )


def _write_json(*, path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def _write_markdown_report(*, path: Path, payload: dict[str, Any]) -> None:
    stage_lines = ["| stage | status | report_path | error |", "|---|---|---|---|"]
    for stage_key in STAGE_KEYS:
        status = payload["stage_status"][stage_key]
        report_path = payload["stage_reports"][stage_key] or ""
        error = payload["stage_errors"][stage_key] or ""
        stage_lines.append(f"| {stage_key} | {status} | {report_path} | {error} |")

    metric_lines = ["| metric | value |", "|---|---|"]
    for metric_key, metric_value in payload["key_metrics"].items():
        metric_lines.append(f"| {metric_key} | {metric_value} |")

    non_fatal_lines = ["| stage | non_fatal_errors |", "|---|---|"]
    for stage_key in STAGE_KEYS:
        non_fatal_value = payload["errors_summary"]["non_fatal_by_stage"][stage_key]
        non_fatal_lines.append(f"| {stage_key} | {non_fatal_value} |")
    non_fatal_lines.append(
        f"| total | {payload['errors_summary']['non_fatal_errors_count']} |"
    )

    markdown = "\n".join(
        [
            "# Final Run Summary",
            "",
            f"- run_id: {payload['run_id']}",
            f"- started_at: {payload['started_at']}",
            f"- finished_at: {payload['finished_at']}",
            f"- pipeline_status: {payload['pipeline_status']}",
            "",
            "## Stages",
            *stage_lines,
            "",
            "## Key Metrics",
            *metric_lines,
            "",
            "## Non-Fatal Errors",
            *non_fatal_lines,
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown, encoding="utf-8")


def _validate_final_report_invariants(payload: dict[str, Any]) -> None:
    pipeline_status = payload.get("pipeline_status")
    if pipeline_status not in VALID_PIPELINE_STATUS:
        raise FatalOpsError("pipeline_status must be completed|stopped|failed")

    stage_reports = payload.get("stage_reports")
    stage_status = payload.get("stage_status")
    stage_errors = payload.get("stage_errors")
    if not isinstance(stage_reports, dict) or not isinstance(stage_status, dict) or not isinstance(stage_errors, dict):
        raise FatalOpsError("stage_reports, stage_status, stage_errors must be objects")

    for stage_key in STAGE_KEYS:
        if stage_key not in stage_reports or stage_key not in stage_status or stage_key not in stage_errors:
            raise FatalOpsError(f"missing required stage key: {stage_key}")
        stage_state = stage_status[stage_key]
        if stage_state not in VALID_STAGE_STATUS:
            raise FatalOpsError(f"{stage_key} status must be completed|failed|skipped")
        stage_report_path = stage_reports[stage_key]
        if stage_report_path is not None and not isinstance(stage_report_path, str):
            raise FatalOpsError(f"{stage_key} report path must be string|null")
        stage_error = stage_errors[stage_key]
        if stage_state == "failed":
            if not isinstance(stage_error, str) or not stage_error.strip():
                raise FatalOpsError(f"{stage_key} failed stage requires non-empty stage error")
        else:
            if stage_error is not None:
                raise FatalOpsError(f"{stage_key} non-failed stage must have null stage error")

    key_metrics = payload.get("key_metrics")
    if not isinstance(key_metrics, dict):
        raise FatalOpsError("key_metrics must be an object")
    for key, value in key_metrics.items():
        if not _is_non_bool_int(value) or int(value) < 0:
            raise FatalOpsError(f"key_metrics.{key} must be non-boolean integer >= 0")

    errors_summary = payload.get("errors_summary")
    if not isinstance(errors_summary, dict):
        raise FatalOpsError("errors_summary must be an object")
    non_fatal_by_stage = errors_summary.get("non_fatal_by_stage")
    if not isinstance(non_fatal_by_stage, dict):
        raise FatalOpsError("errors_summary.non_fatal_by_stage must be an object")
    for stage_key in STAGE_KEYS:
        value = non_fatal_by_stage.get(stage_key)
        if not _is_non_bool_int(value) or int(value) < 0:
            raise FatalOpsError(
                f"errors_summary.non_fatal_by_stage.{stage_key} must be non-boolean integer >= 0"
            )
    non_fatal_errors_count = errors_summary.get("non_fatal_errors_count")
    if not _is_non_bool_int(non_fatal_errors_count) or int(non_fatal_errors_count) < 0:
        raise FatalOpsError("errors_summary.non_fatal_errors_count must be non-boolean integer >= 0")
    if int(non_fatal_errors_count) != sum(int(non_fatal_by_stage[stage_key]) for stage_key in STAGE_KEYS):
        raise FatalOpsError("errors_summary.non_fatal_errors_count must equal sum(non_fatal_by_stage)")

    fatal_stage = payload.get("fatal_stage")
    fatal_error = payload.get("fatal_error")
    stop_after = payload.get("stop_after")

    if pipeline_status == "completed":
        if any(stage_status[stage_key] != "completed" for stage_key in STAGE_KEYS):
            raise FatalOpsError("completed pipeline requires all stages completed")
        if fatal_stage is not None or fatal_error is not None:
            raise FatalOpsError("completed pipeline requires fatal_stage and fatal_error to be null")
    elif pipeline_status == "stopped":
        if not _is_non_bool_int(stop_after) or int(stop_after) < 1 or int(stop_after) > 8:
            raise FatalOpsError("stopped pipeline requires stop_after integer in [1, 8]")
        stop_after_int = int(stop_after)
        for index, stage_key in enumerate(STAGE_KEYS, start=1):
            expected = "completed" if index <= stop_after_int else "skipped"
            if stage_status[stage_key] != expected:
                raise FatalOpsError("stopped pipeline has invalid stage_status layout")
        if fatal_stage is not None or fatal_error is not None:
            raise FatalOpsError("stopped pipeline requires fatal_stage and fatal_error to be null")
    else:
        failed_stage_keys = [stage_key for stage_key in STAGE_KEYS if stage_status[stage_key] == "failed"]
        if len(failed_stage_keys) != 1:
            raise FatalOpsError("failed pipeline requires exactly one failed stage")
        failed_stage = failed_stage_keys[0]
        failed_index = STAGE_KEYS.index(failed_stage)
        for stage_key in STAGE_KEYS[failed_index + 1 :]:
            if stage_status[stage_key] != "skipped":
                raise FatalOpsError("failed pipeline requires downstream stages skipped")
        if fatal_stage != failed_stage:
            raise FatalOpsError("failed pipeline requires fatal_stage to match failed stage")
        if not isinstance(fatal_error, str) or not fatal_error.strip():
            raise FatalOpsError("failed pipeline requires non-empty fatal_error")


def _fatal_message(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return message
    return exc.__class__.__name__


if __name__ == "__main__":
    sys.exit(main())
