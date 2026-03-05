from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from .llm import (
    FatalLLMError,
    LLMClient,
    LLMRuntimeConfig,
    LLMSetupError,
    NonRetryableLLMError,
    RetryableLLMError,
    build_llm_client,
)
from .models import (
    FAIL_BREAKDOWN_KEYS,
    RowValidationError,
    ScriptValidationError,
    build_script_record,
    is_non_bool_int,
    is_non_bool_number,
    parse_json_text,
    parse_selected_row,
    to_utc_z,
    utc_now,
    validate_script_payload,
)
from .prompts import PromptLoadError, PromptRenderError, load_prompt_template, render_prompt
from .state import GenerateStore, StateError


LOGGER = logging.getLogger(__name__)


class FatalGenerateError(RuntimeError):
    pass


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class PathsConfig:
    sqlite_db: str
    outputs_dir: str


@dataclass(frozen=True)
class LLMConfig:
    provider: str
    model: str
    temperature: float
    max_output_tokens: int
    requests_per_minute_soft: int
    request_timeout_s: int
    retry_max_attempts: int
    retry_backoff_initial_s: float
    retry_backoff_multiplier: float
    retry_backoff_max_s: float
    api_key_env_var: str
    api_key: str


@dataclass(frozen=True)
class Stage6GenerateConfig:
    max_items_default: int


@dataclass(frozen=True)
class PipelineConfig:
    paths: PathsConfig
    llm: LLMConfig
    stage_6_generate: Stage6GenerateConfig


@dataclass
class GenerateResult:
    run_id: str
    run_status: str
    fatal_error: str | None
    started_at: str
    finished_at: str
    db_path: str
    output_path: str
    report_path: str
    items_available_total: int
    selected_rows_total: int
    items_selected: int
    success_count: int
    failed_count: int
    inserted_db: int
    skipped_already_present: int
    max_items: int
    llm_provider: str
    llm_model: str
    fail_breakdown: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_generate(
    *,
    pipeline_path: str,
    out_path: str | None = None,
    report_path: str | None = None,
    db_path_override: str | None = None,
    max_items_override: str | None = None,
    model_override: str | None = None,
    llm_client: LLMClient | None = None,
    prompt_template: str | None = None,
) -> GenerateResult:
    started_dt = utc_now()
    started_at = to_utc_z(started_dt)
    run_id = str(uuid.uuid4())

    report_file: Path | None = None
    output_file: Path | None = None
    db_path: Path | None = None

    fail_breakdown = _new_fail_breakdown()
    counters = _new_counters()

    max_items = 0
    resolved_provider = "openai"
    resolved_model = ""

    max_items_override_value: int | None = None
    model_override_value: str | None = None
    report_override_error: Exception | None = None

    try:
        if report_path is not None:
            try:
                report_file = _parse_path_override(report_path, "--report")
            except ConfigError as exc:
                report_override_error = exc

        max_items_override_value = _parse_optional_non_negative_int(max_items_override, "--max-items")
        if max_items_override_value is not None:
            max_items = max_items_override_value

        model_override_value = _parse_optional_model_override(model_override)
        if model_override_value is not None:
            resolved_model = model_override_value

        db_override_path = _parse_optional_path_override(db_path_override, "--db")
        output_override_path = _parse_optional_path_override(out_path, "--out")

        if report_override_error is not None:
            raise report_override_error

        pipeline = _load_pipeline_config(pipeline_path)
        resolved_provider = pipeline.llm.provider

        db_path = db_override_path or Path(pipeline.paths.sqlite_db)
        output_file = output_override_path or _resolve_output_path(
            outputs_dir=Path(pipeline.paths.outputs_dir), date=started_dt
        )
        report_file = report_file or _resolve_report_path(
            outputs_dir=Path(pipeline.paths.outputs_dir), date=started_dt
        )

        max_items = (
            max_items_override_value
            if max_items_override_value is not None
            else pipeline.stage_6_generate.max_items_default
        )
        resolved_model = model_override_value or pipeline.llm.model

        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("w", encoding="utf-8", newline="\n") as out_handle:
            api_key, api_key_source = _resolve_api_key(pipeline.llm)
            LOGGER.info(
                "stage_6_generate start run_id=%s db_path=%s output_path=%s report_path=%s max_items=%s llm_provider=%s llm_model=%s api_key_source=%s",
                run_id,
                db_path,
                output_file,
                report_file,
                max_items,
                resolved_provider,
                resolved_model,
                api_key_source,
            )

            if llm_client is None:
                llm_client = build_llm_client(
                    config=LLMRuntimeConfig(
                        provider=resolved_provider,
                        model=resolved_model,
                        temperature=pipeline.llm.temperature,
                        max_output_tokens=pipeline.llm.max_output_tokens,
                        requests_per_minute_soft=pipeline.llm.requests_per_minute_soft,
                        request_timeout_s=pipeline.llm.request_timeout_s,
                        retry_max_attempts=pipeline.llm.retry_max_attempts,
                        retry_backoff_initial_s=pipeline.llm.retry_backoff_initial_s,
                        retry_backoff_multiplier=pipeline.llm.retry_backoff_multiplier,
                        retry_backoff_max_s=pipeline.llm.retry_backoff_max_s,
                    ),
                    api_key=api_key,
                )

            resolved_prompt_template = (
                prompt_template if prompt_template is not None else load_prompt_template()
            )

            store = GenerateStore(db_path=db_path)
            try:
                store.validate_ideas_table()
                store.ensure_scripts_table()
                store.validate_scripts_compatibility()

                counters["items_available_total"] = store.count_items_available_total()
                selected_rows = store.select_rows(max_items=max_items)
                counters["selected_rows_total"] = len(selected_rows)

                for raw in selected_rows:
                    _process_selected_row(
                        raw=raw,
                        store=store,
                        out_handle=out_handle,
                        fail_breakdown=fail_breakdown,
                        counters=counters,
                        prompt_template=resolved_prompt_template,
                        llm_client=llm_client,
                        llm_provider=resolved_provider,
                        llm_model=resolved_model,
                    )
            finally:
                store.close()

        finished_at = to_utc_z(utc_now())
        result = GenerateResult(
            run_id=run_id,
            run_status="completed",
            fatal_error=None,
            started_at=started_at,
            finished_at=finished_at,
            db_path=str(db_path),
            output_path=str(output_file),
            report_path=str(report_file),
            items_available_total=counters["items_available_total"],
            selected_rows_total=counters["selected_rows_total"],
            items_selected=counters["items_selected"],
            success_count=counters["success_count"],
            failed_count=counters["failed_count"],
            inserted_db=counters["inserted_db"],
            skipped_already_present=counters["skipped_already_present"],
            max_items=max_items,
            llm_provider=resolved_provider,
            llm_model=resolved_model,
            fail_breakdown=fail_breakdown,
        )
        payload = result.to_dict()
        _validate_report_invariants(payload)
        _write_json(report_file, payload)

        if counters["failed_count"] > 0:
            LOGGER.warning(
                "stage_6_generate fail_breakdown=%s",
                json.dumps(fail_breakdown, ensure_ascii=True),
            )
        LOGGER.info(
            "stage_6_generate complete run_id=%s items_available_total=%s selected_rows_total=%s items_selected=%s success_count=%s failed_count=%s inserted_db=%s skipped_already_present=%s",
            run_id,
            counters["items_available_total"],
            counters["selected_rows_total"],
            counters["items_selected"],
            counters["success_count"],
            counters["failed_count"],
            counters["inserted_db"],
            counters["skipped_already_present"],
        )
        return result
    except Exception as exc:
        fatal_error = _fatal_message(exc)
        finished_at = to_utc_z(utc_now())
        if report_file is not None:
            payload = GenerateResult(
                run_id=run_id,
                run_status="fatal",
                fatal_error=fatal_error,
                started_at=started_at,
                finished_at=finished_at,
                db_path=str(db_path) if db_path is not None else "",
                output_path=str(output_file) if output_file is not None else "",
                report_path=str(report_file),
                items_available_total=counters["items_available_total"],
                selected_rows_total=counters["selected_rows_total"],
                items_selected=counters["items_selected"],
                success_count=counters["success_count"],
                failed_count=counters["failed_count"],
                inserted_db=counters["inserted_db"],
                skipped_already_present=counters["skipped_already_present"],
                max_items=max_items,
                llm_provider=resolved_provider,
                llm_model=resolved_model,
                fail_breakdown=fail_breakdown,
            ).to_dict()
            _validate_report_invariants(payload)
            try:
                _write_json(report_file, payload)
            except OSError as report_exc:
                raise FatalGenerateError(
                    f"{fatal_error}; failed writing fatal report: {report_exc}"
                ) from report_exc

        raise FatalGenerateError(fatal_error) from exc


def _process_selected_row(
    *,
    raw: dict[str, Any],
    store: GenerateStore,
    out_handle: Any,
    fail_breakdown: dict[str, int],
    counters: dict[str, int],
    prompt_template: str,
    llm_client: LLMClient,
    llm_provider: str,
    llm_model: str,
) -> None:
    phase = "pre_provider_attempt"
    try:
        row = parse_selected_row(raw)
        prompt = render_prompt(
            prompt_template,
            {
                "PLATFORM": row.platform,
                "RECOMMENDED_FORMAT": row.recommended_format,
                "TITLE": row.title,
                "URL": row.url,
                "TOPIC": row.topic,
                "CORE_CLAIM": row.core_claim,
                "WORKFLOW_STEPS": json.dumps(row.workflow_steps, ensure_ascii=True),
                "TOOLS_MENTIONED": json.dumps(row.tools_mentioned, ensure_ascii=True),
                "MONETIZATION_ANGLE": row.monetization_angle,
                "METRICS_CLAIMS": json.dumps(row.metrics_claims, ensure_ascii=True),
                "ASSUMPTIONS": json.dumps(row.assumptions, ensure_ascii=True),
                "PRIOR_HOOKS": json.dumps(row.hooks, ensure_ascii=True),
            },
        )

        phase = "provider_attempt_started"
        llm_response = llm_client.call_json(
            prompt=prompt,
            schema_name="stage_6_script",
            schema=_script_schema(),
            call_label="script_generate",
        )
        try:
            parsed = parse_json_text(llm_response)
        except ValueError:
            _record_failure(counters, fail_breakdown, "script_invalid_json")
            return

        try:
            payload = validate_script_payload(parsed, recommended_format=row.recommended_format)
        except ScriptValidationError:
            _record_failure(counters, fail_breakdown, "script_validation_failed")
            return

        created_at = to_utc_z(utc_now())
        record = build_script_record(
            row=row,
            payload=payload,
            llm_provider=llm_provider,
            llm_model=llm_model,
            created_at=created_at,
        )
        inserted = store.insert_script(record)
        counters["items_selected"] += 1
        counters["success_count"] += 1
        if inserted:
            counters["inserted_db"] += 1
            out_handle.write(json.dumps(record.to_dict(), ensure_ascii=True))
            out_handle.write("\n")
        else:
            counters["skipped_already_present"] += 1
    except (PromptLoadError, PromptRenderError, StateError, OSError, LLMSetupError):
        raise
    except FatalLLMError:
        raise
    except NonRetryableLLMError:
        _record_failure(counters, fail_breakdown, "script_llm_failed")
    except RetryableLLMError:
        _record_failure(counters, fail_breakdown, "script_llm_failed")
    except RowValidationError:
        _record_failure(counters, fail_breakdown, "script_validation_failed")
    except Exception:
        reason = "script_validation_failed" if phase == "pre_provider_attempt" else "script_llm_failed"
        _record_failure(counters, fail_breakdown, reason)


def _script_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["primary_hook", "alt_hooks", "script", "cta", "disclaimer"],
        "properties": {
            "primary_hook": {"type": "string", "maxLength": 140},
            "alt_hooks": {
                "type": "array",
                "items": {"type": "string", "maxLength": 140},
                "minItems": 2,
                "maxItems": 2,
            },
            "script": {
                "type": "object",
                "additionalProperties": False,
                "required": ["sections", "word_count", "estimated_seconds"],
                "properties": {
                    "sections": {
                        "type": "array",
                        "minItems": 4,
                        "maxItems": 4,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["label", "text"],
                            "properties": {
                                "label": {"type": "string"},
                                "text": {"type": "string"},
                            },
                        },
                    },
                    "word_count": {"type": "integer"},
                    "estimated_seconds": {"type": "integer"},
                },
            },
            "cta": {"type": "string"},
            "disclaimer": {"type": "string"},
        },
    }


def _new_fail_breakdown() -> dict[str, int]:
    return {key: 0 for key in FAIL_BREAKDOWN_KEYS}


def _new_counters() -> dict[str, int]:
    return {
        "items_available_total": 0,
        "selected_rows_total": 0,
        "items_selected": 0,
        "success_count": 0,
        "failed_count": 0,
        "inserted_db": 0,
        "skipped_already_present": 0,
    }


def _record_failure(counters: dict[str, int], fail_breakdown: dict[str, int], reason: str) -> None:
    counters["items_selected"] += 1
    counters["failed_count"] += 1
    fail_breakdown[reason] += 1


def _fatal_message(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return message
    return exc.__class__.__name__


def _resolve_output_path(*, outputs_dir: Path, date: Any) -> Path:
    return outputs_dir / f"scripts_{date.strftime('%Y-%m-%d')}.jsonl"


def _resolve_report_path(*, outputs_dir: Path, date: Any) -> Path:
    return outputs_dir / f"stage_6_report_{date.strftime('%Y-%m-%d')}.json"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def _resolve_api_key(config: LLMConfig) -> tuple[str, str]:
    env_name = config.api_key_env_var
    env_value = os.getenv(env_name)
    if isinstance(env_value, str) and env_value.strip():
        return env_value.strip(), "env"
    if config.api_key.strip():
        return config.api_key.strip(), "config"
    raise ConfigError("missing api key: neither env var nor llm.api_key provided")


def _load_pipeline_config(path: str | Path) -> PipelineConfig:
    raw = _load_yaml(path)
    paths_raw = raw.get("paths")
    llm_raw = raw.get("llm")
    stage_raw = raw.get("stage_6_generate")

    if not isinstance(paths_raw, dict):
        raise ConfigError("paths must be a mapping")
    if not isinstance(llm_raw, dict):
        raise ConfigError("llm must be a mapping")
    if not isinstance(stage_raw, dict):
        raise ConfigError("stage_6_generate must be a mapping")

    paths = PathsConfig(
        sqlite_db=_parse_non_empty_string(paths_raw.get("sqlite_db"), "paths.sqlite_db"),
        outputs_dir=_parse_non_empty_string(paths_raw.get("outputs_dir"), "paths.outputs_dir"),
    )
    llm = _parse_llm_config(llm_raw)
    stage = Stage6GenerateConfig(
        max_items_default=_parse_non_negative_int(
            stage_raw.get("max_items_default"),
            "stage_6_generate.max_items_default",
        )
    )
    return PipelineConfig(paths=paths, llm=llm, stage_6_generate=stage)


def _parse_llm_config(raw: dict[str, Any]) -> LLMConfig:
    provider = _parse_non_empty_string(raw.get("provider"), "llm.provider").lower()
    if provider != "openai":
        raise ConfigError("llm.provider must be openai")

    model = _parse_non_empty_string(raw.get("model"), "llm.model")
    temperature = _parse_number_range(
        raw.get("temperature"),
        "llm.temperature",
        minimum=0.0,
        maximum=2.0,
    )
    max_output_tokens = _parse_positive_int(raw.get("max_output_tokens"), "llm.max_output_tokens")
    requests_per_minute_soft = _parse_positive_int(
        raw.get("requests_per_minute_soft"),
        "llm.requests_per_minute_soft",
    )
    request_timeout_s = _parse_positive_int(raw.get("request_timeout_s"), "llm.request_timeout_s")
    retry_max_attempts = _parse_positive_int(raw.get("retry_max_attempts"), "llm.retry_max_attempts")
    retry_backoff_initial_s = _parse_number_gt_zero(
        raw.get("retry_backoff_initial_s"),
        "llm.retry_backoff_initial_s",
    )
    retry_backoff_multiplier = _parse_number_min(
        raw.get("retry_backoff_multiplier"),
        "llm.retry_backoff_multiplier",
        minimum=1.0,
    )
    retry_backoff_max_s = _parse_number_min(
        raw.get("retry_backoff_max_s"),
        "llm.retry_backoff_max_s",
        minimum=retry_backoff_initial_s,
    )
    api_key_env_var = _parse_non_empty_string(raw.get("api_key_env_var"), "llm.api_key_env_var")
    api_key_raw = raw.get("api_key")
    if not isinstance(api_key_raw, str):
        raise ConfigError("llm.api_key must be a string")

    return LLMConfig(
        provider=provider,
        model=model,
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        requests_per_minute_soft=requests_per_minute_soft,
        request_timeout_s=request_timeout_s,
        retry_max_attempts=retry_max_attempts,
        retry_backoff_initial_s=retry_backoff_initial_s,
        retry_backoff_multiplier=retry_backoff_multiplier,
        retry_backoff_max_s=retry_backoff_max_s,
        api_key_env_var=api_key_env_var,
        api_key=api_key_raw,
    )


def _load_yaml(path: str | Path) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
    except FileNotFoundError as exc:
        raise ConfigError(f"missing config file: {path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"top-level config must be a mapping: {path}")
    return raw


def _parse_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{field_name} must be a non-empty string")
    return value.strip()


def _parse_non_negative_int(value: Any, field_name: str) -> int:
    if not is_non_bool_int(value) or int(value) < 0:
        raise ConfigError(f"{field_name} must be a non-boolean integer >= 0")
    return int(value)


def _parse_positive_int(value: Any, field_name: str) -> int:
    if not is_non_bool_int(value) or int(value) < 1:
        raise ConfigError(f"{field_name} must be a non-boolean integer >= 1")
    return int(value)


def _parse_number_range(value: Any, field_name: str, *, minimum: float, maximum: float) -> float:
    if not is_non_bool_number(value):
        raise ConfigError(f"{field_name} must be a non-boolean number between {minimum} and {maximum}")
    normalized = float(value)
    if normalized < minimum or normalized > maximum:
        raise ConfigError(f"{field_name} must be a non-boolean number between {minimum} and {maximum}")
    return normalized


def _parse_number_min(value: Any, field_name: str, *, minimum: float) -> float:
    if not is_non_bool_number(value):
        raise ConfigError(f"{field_name} must be a non-boolean number >= {minimum}")
    normalized = float(value)
    if normalized < minimum:
        raise ConfigError(f"{field_name} must be a non-boolean number >= {minimum}")
    return normalized


def _parse_number_gt_zero(value: Any, field_name: str) -> float:
    if not is_non_bool_number(value):
        raise ConfigError(f"{field_name} must be a non-boolean number > 0")
    normalized = float(value)
    if normalized <= 0:
        raise ConfigError(f"{field_name} must be a non-boolean number > 0")
    return normalized


def _parse_path_override(value: str, flag: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{flag} must be a non-empty string")
    return Path(value.strip())


def _parse_optional_path_override(value: str | None, flag: str) -> Path | None:
    if value is None:
        return None
    return _parse_path_override(value, flag)


def _parse_optional_non_negative_int(value: str | None, flag: str) -> int | None:
    if value is None:
        return None
    normalized = value.strip()
    if normalized.lower() in {"true", "false"}:
        raise ConfigError(f"{flag} must be a non-boolean integer >= 0")
    try:
        parsed = int(normalized)
    except ValueError as exc:
        raise ConfigError(f"{flag} must be a non-boolean integer >= 0") from exc
    if parsed < 0:
        raise ConfigError(f"{flag} must be a non-boolean integer >= 0")
    return parsed


def _parse_optional_model_override(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise ConfigError("--model must be a non-empty string")
    return normalized


def _validate_report_invariants(payload: dict[str, Any]) -> None:
    run_status = payload.get("run_status")
    fatal_error = payload.get("fatal_error")
    if run_status not in {"completed", "fatal"}:
        raise FatalGenerateError("report invariant failed: run_status")
    if run_status == "completed" and fatal_error is not None:
        raise FatalGenerateError("report invariant failed: completed run must have fatal_error null")
    if run_status == "fatal" and (not isinstance(fatal_error, str) or not fatal_error.strip()):
        raise FatalGenerateError("report invariant failed: fatal run must have non-empty fatal_error")

    fail_breakdown = payload.get("fail_breakdown")
    if not isinstance(fail_breakdown, dict):
        raise FatalGenerateError("report invariant failed: fail_breakdown must be a map")
    if set(fail_breakdown.keys()) != set(FAIL_BREAKDOWN_KEYS):
        raise FatalGenerateError("report invariant failed: fail_breakdown keys mismatch")

    int_fields = (
        "items_available_total",
        "selected_rows_total",
        "items_selected",
        "success_count",
        "failed_count",
        "inserted_db",
        "skipped_already_present",
        "max_items",
    )
    for field in int_fields:
        value = payload.get(field)
        if not is_non_bool_int(value) or int(value) < 0:
            raise FatalGenerateError(f"report invariant failed: {field} must be integer >= 0")
    for key in FAIL_BREAKDOWN_KEYS:
        value = fail_breakdown.get(key)
        if not is_non_bool_int(value) or int(value) < 0:
            raise FatalGenerateError(f"report invariant failed: fail_breakdown[{key}] invalid")

    for field in (
        "run_id",
        "started_at",
        "finished_at",
        "db_path",
        "output_path",
        "report_path",
        "llm_provider",
        "llm_model",
    ):
        if not isinstance(payload.get(field), str):
            raise FatalGenerateError(f"report invariant failed: {field} must be a string")

    items_available_total = int(payload["items_available_total"])
    selected_rows_total = int(payload["selected_rows_total"])
    items_selected = int(payload["items_selected"])
    success_count = int(payload["success_count"])
    failed_count = int(payload["failed_count"])
    inserted_db = int(payload["inserted_db"])
    skipped = int(payload["skipped_already_present"])
    max_items = int(payload["max_items"])
    fail_total = sum(int(v) for v in fail_breakdown.values())

    if items_selected != success_count + failed_count:
        raise FatalGenerateError("report invariant failed: items_selected mismatch")
    if success_count != inserted_db + skipped:
        raise FatalGenerateError("report invariant failed: success_count mismatch")
    if failed_count != fail_total:
        raise FatalGenerateError("report invariant failed: failed_count mismatch")
    if items_selected > selected_rows_total:
        raise FatalGenerateError("report invariant failed: items_selected > selected_rows_total")
    if selected_rows_total > max_items:
        raise FatalGenerateError("report invariant failed: selected_rows_total > max_items")
    if items_selected > items_available_total:
        raise FatalGenerateError("report invariant failed: items_selected > items_available_total")
    if run_status == "completed" and items_selected != selected_rows_total:
        raise FatalGenerateError("report invariant failed: completed run items_selected mismatch")

