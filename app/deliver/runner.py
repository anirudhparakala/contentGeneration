from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import yaml

from .models import (
    RowMappingError,
    build_delivery_message,
    is_non_bool_int,
    to_utc_z,
    utc_now,
)
from .slack import SlackSendError, send_slack_message
from .state import DeliverStore


LOGGER = logging.getLogger(__name__)


class FatalDeliverError(RuntimeError):
    pass


class ConfigError(ValueError):
    pass


PostMessageFn = Callable[[str, str], None]


@dataclass(frozen=True)
class PathsConfig:
    sqlite_db: str
    outputs_dir: str


@dataclass(frozen=True)
class DeliverConfig:
    enabled: bool
    channel: str
    slack_webhook_url: str
    max_items_per_run: int
    max_script_chars: int
    min_viral_rating: int | None
    include_only_status: list[str]
    dry_run: bool


@dataclass(frozen=True)
class PipelineConfig:
    paths: PathsConfig
    deliver: DeliverConfig


@dataclass
class DeliverResult:
    run_id: str
    run_status: str
    fatal_error: str | None
    started_at: str
    finished_at: str
    db_path: str
    report_path: str
    enabled: bool
    dry_run: bool
    channel: str
    max_items: int
    min_viral_rating: int | None
    items_available_total: int
    items_selected: int
    items_sent: int
    items_skipped_already_sent: int
    items_skipped_missing_script: int
    errors_count: int
    first_error: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_deliver(
    *,
    pipeline_path: str,
    db_path_override: str | None = None,
    max_items_override: str | None = None,
    dry_run_override: bool = False,
    report_path: str | None = None,
    post_message: PostMessageFn | None = None,
) -> DeliverResult:
    started_dt = utc_now()
    started_at = to_utc_z(started_dt)
    run_id = str(uuid.uuid4())

    report_file: Path | None = None
    db_path: Path | None = None
    counters = _new_counters()
    first_error: str | None = None

    enabled = False
    channel = "slack"
    dry_run = bool(dry_run_override)
    max_items = 0
    min_viral_rating: int | None = None
    max_script_chars = 80
    slack_webhook_url = ""

    max_items_override_value: int | None = None
    report_override_error: Exception | None = None

    try:
        if report_path is not None:
            try:
                report_file = _parse_path_override(report_path, "--report")
            except ConfigError as exc:
                report_override_error = exc

        max_items_override_value = _parse_optional_non_negative_int(
            max_items_override,
            "--max-items",
        )
        if max_items_override_value is not None:
            max_items = max_items_override_value

        db_override_path = _parse_optional_path_override(db_path_override, "--db")

        if report_override_error is not None:
            raise report_override_error

        pipeline = _load_pipeline_config(pipeline_path)

        enabled = pipeline.deliver.enabled
        channel = pipeline.deliver.channel
        dry_run = bool(dry_run_override or pipeline.deliver.dry_run)
        min_viral_rating = pipeline.deliver.min_viral_rating
        max_script_chars = pipeline.deliver.max_script_chars
        slack_webhook_url = pipeline.deliver.slack_webhook_url

        db_path = db_override_path or Path(pipeline.paths.sqlite_db)
        report_file = report_file or _resolve_report_path(
            outputs_dir=Path(pipeline.paths.outputs_dir),
            date=started_dt,
        )
        max_items = (
            max_items_override_value
            if max_items_override_value is not None
            else pipeline.deliver.max_items_per_run
        )

        LOGGER.info(
            "stage_8_deliver start run_id=%s db_path=%s report_path=%s enabled=%s dry_run=%s max_items=%s min_viral_rating=%s",
            run_id,
            db_path,
            report_file,
            enabled,
            dry_run,
            max_items,
            min_viral_rating,
        )

        if not enabled:
            finished_at = to_utc_z(utc_now())
            result = DeliverResult(
                run_id=run_id,
                run_status="completed",
                fatal_error=None,
                started_at=started_at,
                finished_at=finished_at,
                db_path=str(db_path),
                report_path=str(report_file),
                enabled=False,
                dry_run=dry_run,
                channel=channel,
                max_items=max_items,
                min_viral_rating=min_viral_rating,
                items_available_total=0,
                items_selected=0,
                items_sent=0,
                items_skipped_already_sent=0,
                items_skipped_missing_script=0,
                errors_count=0,
                first_error=None,
            )
            payload = result.to_dict()
            _validate_report_invariants(payload)
            _write_json(report_file, payload)
            LOGGER.info("stage_8_deliver skipped deliver.enabled=false run_id=%s", run_id)
            return result

        webhook_hash = _compute_webhook_hash(slack_webhook_url)
        sender = post_message or _send_message

        store = DeliverStore(db_path=db_path)
        try:
            store.validate_dependencies()
            store.ensure_deliveries_table()
            store.validate_deliveries_compatibility()
            counters["items_available_total"] = store.count_items_available_total(
                min_viral_rating=min_viral_rating
            )
            rows = store.select_candidates(min_viral_rating=min_viral_rating)

            for raw in rows:
                if counters["items_selected"] >= max_items:
                    break

                if raw.get("script_item_id") is None:
                    counters["items_skipped_missing_script"] += 1
                    continue

                item_id = _normalize_optional_string(raw.get("item_id"))
                if not item_id:
                    counters["items_selected"] += 1
                    first_error = _record_non_fatal_error(
                        counters=counters,
                        first_error=first_error,
                        message="ideas.item_id must be non-empty after strip()",
                    )
                    continue

                if store.was_already_sent(
                    item_id=item_id,
                    channel=channel,
                    webhook_hash=webhook_hash,
                ):
                    counters["items_skipped_already_sent"] += 1
                    continue

                counters["items_selected"] += 1
                try:
                    message = build_delivery_message(
                        raw,
                        timestamp=started_at,
                        max_script_chars=max_script_chars,
                    )
                except RowMappingError as exc:
                    first_error = _record_non_fatal_error(
                        counters=counters,
                        first_error=first_error,
                        message=_fatal_message(exc),
                    )
                    continue

                if dry_run:
                    continue

                try:
                    sender(slack_webhook_url, message.text)
                except SlackSendError as exc:
                    first_error = _record_non_fatal_error(
                        counters=counters,
                        first_error=first_error,
                        message=_fatal_message(exc),
                    )
                    continue
                except Exception as exc:
                    first_error = _record_non_fatal_error(
                        counters=counters,
                        first_error=first_error,
                        message=f"slack send failed: {_fatal_message(exc)}",
                    )
                    continue

                store.insert_delivery(
                    item_id=message.item_id,
                    channel=channel,
                    webhook_hash=webhook_hash,
                    sent_at=to_utc_z(utc_now()),
                )
                counters["items_sent"] += 1
        finally:
            store.close()

        finished_at = to_utc_z(utc_now())
        result = DeliverResult(
            run_id=run_id,
            run_status="completed",
            fatal_error=None,
            started_at=started_at,
            finished_at=finished_at,
            db_path=str(db_path),
            report_path=str(report_file),
            enabled=enabled,
            dry_run=dry_run,
            channel=channel,
            max_items=max_items,
            min_viral_rating=min_viral_rating,
            items_available_total=counters["items_available_total"],
            items_selected=counters["items_selected"],
            items_sent=counters["items_sent"],
            items_skipped_already_sent=counters["items_skipped_already_sent"],
            items_skipped_missing_script=counters["items_skipped_missing_script"],
            errors_count=counters["errors_count"],
            first_error=first_error,
        )
        payload = result.to_dict()
        _validate_report_invariants(payload)
        _write_json(report_file, payload)
        LOGGER.info(
            "stage_8_deliver complete run_id=%s items_available_total=%s items_selected=%s items_sent=%s items_skipped_already_sent=%s items_skipped_missing_script=%s errors_count=%s",
            run_id,
            counters["items_available_total"],
            counters["items_selected"],
            counters["items_sent"],
            counters["items_skipped_already_sent"],
            counters["items_skipped_missing_script"],
            counters["errors_count"],
        )
        return result
    except Exception as exc:
        fatal_error = _fatal_message(exc)
        finished_at = to_utc_z(utc_now())
        if first_error is None:
            first_error = fatal_error

        if report_file is not None:
            payload = DeliverResult(
                run_id=run_id,
                run_status="fatal",
                fatal_error=fatal_error,
                started_at=started_at,
                finished_at=finished_at,
                db_path=str(db_path) if db_path is not None else "",
                report_path=str(report_file),
                enabled=enabled,
                dry_run=dry_run,
                channel=channel,
                max_items=max_items,
                min_viral_rating=min_viral_rating,
                items_available_total=counters["items_available_total"],
                items_selected=counters["items_selected"],
                items_sent=counters["items_sent"],
                items_skipped_already_sent=counters["items_skipped_already_sent"],
                items_skipped_missing_script=counters["items_skipped_missing_script"],
                errors_count=counters["errors_count"],
                first_error=first_error,
            ).to_dict()
            _validate_report_invariants(payload)
            try:
                _write_json(report_file, payload)
            except OSError as report_exc:
                raise FatalDeliverError(
                    f"{fatal_error}; failed writing fatal report: {report_exc}"
                ) from report_exc

        raise FatalDeliverError(fatal_error) from exc


def _send_message(webhook_url: str, text: str) -> None:
    send_slack_message(webhook_url=webhook_url, text=text, timeout_s=20)


def _new_counters() -> dict[str, int]:
    return {
        "items_available_total": 0,
        "items_selected": 0,
        "items_sent": 0,
        "items_skipped_already_sent": 0,
        "items_skipped_missing_script": 0,
        "errors_count": 0,
    }


def _compute_webhook_hash(webhook_url: str) -> str:
    return hashlib.sha256(webhook_url.encode("utf-8")).hexdigest()


def _record_non_fatal_error(
    *,
    counters: dict[str, int],
    first_error: str | None,
    message: str,
) -> str:
    counters["errors_count"] += 1
    return first_error if first_error is not None else message


def _fatal_message(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return message
    return exc.__class__.__name__


def _resolve_report_path(*, outputs_dir: Path, date: Any) -> Path:
    return outputs_dir / f"stage_8_report_{date.strftime('%Y-%m-%d')}.json"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def _load_pipeline_config(path: str | Path) -> PipelineConfig:
    raw = _load_yaml(path)
    paths_raw = raw.get("paths")
    deliver_raw = raw.get("deliver")

    if not isinstance(paths_raw, dict):
        raise ConfigError("paths must be a mapping")
    if not isinstance(deliver_raw, dict):
        raise ConfigError("deliver must be a mapping")

    paths = PathsConfig(
        sqlite_db=_parse_non_empty_string(paths_raw.get("sqlite_db"), "paths.sqlite_db"),
        outputs_dir=_parse_non_empty_string(paths_raw.get("outputs_dir"), "paths.outputs_dir"),
    )
    deliver = _parse_deliver_config(deliver_raw)
    return PipelineConfig(paths=paths, deliver=deliver)


def _parse_deliver_config(raw: dict[str, Any]) -> DeliverConfig:
    enabled = raw.get("enabled")
    if not isinstance(enabled, bool):
        raise ConfigError("deliver.enabled must be a boolean")

    channel = _parse_non_empty_string(raw.get("channel"), "deliver.channel").lower()
    if channel != "slack":
        raise ConfigError('deliver.channel must be "slack"')

    webhook_raw = raw.get("slack_webhook_url")
    if not isinstance(webhook_raw, str):
        raise ConfigError("deliver.slack_webhook_url must be a string")
    webhook_url = webhook_raw.strip()
    if enabled:
        if not webhook_url:
            raise ConfigError("deliver.slack_webhook_url must be a non-empty string")
        if not (
            webhook_url.startswith("https://hooks.slack.com/services/")
            or webhook_url.startswith("https://hooks.slack-gov.com/services/")
        ):
            raise ConfigError(
                "deliver.slack_webhook_url must start with a Slack webhook prefix"
            )

    max_items_per_run = _parse_non_negative_int(
        raw.get("max_items_per_run"),
        "deliver.max_items_per_run",
    )
    max_script_chars = _parse_min_int(
        raw.get("max_script_chars"),
        "deliver.max_script_chars",
        minimum=80,
    )
    min_viral_rating = _parse_nullable_viral_rating(raw.get("min_viral_rating"))
    include_only_status = _parse_include_only_status(raw.get("include_only_status"))

    dry_run = raw.get("dry_run")
    if not isinstance(dry_run, bool):
        raise ConfigError("deliver.dry_run must be a boolean")

    return DeliverConfig(
        enabled=enabled,
        channel=channel,
        slack_webhook_url=webhook_url,
        max_items_per_run=max_items_per_run,
        max_script_chars=max_script_chars,
        min_viral_rating=min_viral_rating,
        include_only_status=include_only_status,
        dry_run=dry_run,
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


def _parse_min_int(value: Any, field_name: str, *, minimum: int) -> int:
    if not is_non_bool_int(value) or int(value) < minimum:
        raise ConfigError(f"{field_name} must be a non-boolean integer >= {minimum}")
    return int(value)


def _parse_nullable_viral_rating(value: Any) -> int | None:
    if value is None:
        return None
    if not is_non_bool_int(value):
        raise ConfigError("deliver.min_viral_rating must be null or a non-boolean integer in [1, 10]")
    rating = int(value)
    if rating < 1 or rating > 10:
        raise ConfigError("deliver.min_viral_rating must be null or a non-boolean integer in [1, 10]")
    return rating


def _parse_include_only_status(value: Any) -> list[str]:
    if not isinstance(value, list):
        raise ConfigError("deliver.include_only_status must be a list of strings")
    for entry in value:
        if not isinstance(entry, str):
            raise ConfigError("deliver.include_only_status must be a list of strings")
    if value:
        raise ConfigError("deliver.include_only_status must be [] for stage 8")
    return []


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
    if not isinstance(value, str):
        raise ConfigError(f"{flag} must be a non-boolean integer >= 0")
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


def _validate_report_invariants(payload: dict[str, Any]) -> None:
    run_status = payload.get("run_status")
    fatal_error = payload.get("fatal_error")
    if run_status not in {"completed", "fatal"}:
        raise FatalDeliverError("report invariant failed: run_status")
    if run_status == "completed" and fatal_error is not None:
        raise FatalDeliverError("report invariant failed: completed run must have fatal_error null")
    if run_status == "fatal" and (not isinstance(fatal_error, str) or not fatal_error.strip()):
        raise FatalDeliverError("report invariant failed: fatal run must have non-empty fatal_error")

    for field in ("run_id", "started_at", "finished_at", "db_path", "report_path", "channel"):
        if not isinstance(payload.get(field), str):
            raise FatalDeliverError(f"report invariant failed: {field} must be a string")

    channel = str(payload["channel"]).strip().lower()
    if channel != "slack":
        raise FatalDeliverError('report invariant failed: channel must be "slack"')

    for field in ("enabled", "dry_run"):
        if not isinstance(payload.get(field), bool):
            raise FatalDeliverError(f"report invariant failed: {field} must be boolean")

    int_fields = (
        "max_items",
        "items_available_total",
        "items_selected",
        "items_sent",
        "items_skipped_already_sent",
        "items_skipped_missing_script",
        "errors_count",
    )
    for field in int_fields:
        value = payload.get(field)
        if not is_non_bool_int(value) or int(value) < 0:
            raise FatalDeliverError(f"report invariant failed: {field} must be integer >= 0")

    min_viral_rating = payload.get("min_viral_rating")
    if min_viral_rating is not None:
        if (
            not is_non_bool_int(min_viral_rating)
            or int(min_viral_rating) < 1
            or int(min_viral_rating) > 10
        ):
            raise FatalDeliverError(
                "report invariant failed: min_viral_rating must be null or integer in [1, 10]"
            )

    first_error = payload.get("first_error")
    if first_error is not None and (not isinstance(first_error, str) or not first_error.strip()):
        raise FatalDeliverError("report invariant failed: first_error must be null or non-empty string")

    max_items = int(payload["max_items"])
    items_selected = int(payload["items_selected"])
    items_sent = int(payload["items_sent"])
    errors_count = int(payload["errors_count"])
    enabled = bool(payload["enabled"])
    dry_run = bool(payload["dry_run"])

    if items_selected > max_items:
        raise FatalDeliverError("report invariant failed: items_selected > max_items")
    if items_sent > items_selected:
        raise FatalDeliverError("report invariant failed: items_sent > items_selected")
    if dry_run and items_sent != 0:
        raise FatalDeliverError("report invariant failed: dry_run requires items_sent == 0")

    if errors_count == 0 and run_status == "completed" and first_error is not None:
        raise FatalDeliverError(
            "report invariant failed: completed run with no row errors must have first_error null"
        )
    if errors_count > 0 and (not isinstance(first_error, str) or not first_error.strip()):
        raise FatalDeliverError("report invariant failed: first_error missing for row errors")
    if run_status == "fatal" and errors_count == 0 and first_error != fatal_error:
        raise FatalDeliverError(
            "report invariant failed: fatal first_error must equal fatal_error when no row errors"
        )

    if run_status == "completed" and not enabled:
        for field in (
            "items_available_total",
            "items_selected",
            "items_sent",
            "items_skipped_already_sent",
            "items_skipped_missing_script",
            "errors_count",
        ):
            if int(payload[field]) != 0:
                raise FatalDeliverError(
                    "report invariant failed: completed disabled run must have zero counters"
                )


def _normalize_optional_string(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()
