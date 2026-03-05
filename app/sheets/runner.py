from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from .client import GoogleSheetsClient, SheetsClient
from .models import (
    HeaderLayout,
    HeaderValidationError,
    PersistSheetRow,
    RowMappingError,
    build_header_layout,
    build_sheet_row,
    is_non_bool_int,
    to_utc_z,
    utc_now,
)


LOGGER = logging.getLogger(__name__)

REQUIRED_IDEAS_COLUMNS: dict[str, str] = {
    "item_id": "TEXT",
    "url": "TEXT",
    "title": "TEXT",
    "topic": "TEXT",
    "viral_rating": "INTEGER",
    "hooks": "TEXT",
    "platform": "TEXT",
    "monetization_angle": "TEXT",
    "tools_mentioned": "TEXT",
    "published_at": "TEXT",
}

REQUIRED_SCRIPTS_COLUMNS: dict[str, str] = {
    "item_id": "TEXT",
    "primary_hook": "TEXT",
    "script_sections": "TEXT",
}

REQUIRED_ITEMS_COLUMNS: dict[str, str] = {
    "item_id": "TEXT",
    "creator": "TEXT",
    "source_name": "TEXT",
}


class FatalPersistError(RuntimeError):
    pass


class ConfigError(ValueError):
    pass


class StateError(RuntimeError):
    pass


class WorksheetContractError(RuntimeError):
    pass


@dataclass(frozen=True)
class PathsConfig:
    sqlite_db: str
    outputs_dir: str


@dataclass(frozen=True)
class SheetsConfig:
    enabled: bool
    spreadsheet_id: str
    worksheet_name: str
    key_column: str
    header_row: int


@dataclass(frozen=True)
class Stage7PersistConfig:
    max_rows_default: int


@dataclass(frozen=True)
class PipelineConfig:
    paths: PathsConfig
    sheets: SheetsConfig
    stage_7_persist: Stage7PersistConfig


@dataclass
class PersistResult:
    run_id: str
    run_status: str
    fatal_error: str | None
    started_at: str
    finished_at: str
    db_path: str
    report_path: str
    sheets_enabled: bool
    spreadsheet_id: str
    worksheet_name: str
    max_rows: int
    rows_available_total: int
    rows_considered: int
    rows_inserted: int
    rows_updated: int
    rows_skipped_missing_script: int
    rows_skipped_invalid_payload: int
    errors_count: int
    first_error: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExistingSheetRow:
    row_number: int
    values: list[str]


def run_persist(
    *,
    pipeline_path: str,
    db_path_override: str | None = None,
    sheet_id_override: str | None = None,
    worksheet_override: str | None = None,
    max_rows_override: str | None = None,
    report_path: str | None = None,
    sheets_client: SheetsClient | None = None,
) -> PersistResult:
    started_dt = utc_now()
    started_at = to_utc_z(started_dt)
    run_id = str(uuid.uuid4())

    report_file: Path | None = None
    db_path: Path | None = None
    counters = _new_counters()
    first_error: str | None = None

    sheets_enabled = False
    spreadsheet_id = ""
    worksheet_name = ""
    max_rows = 0

    report_override_error: Exception | None = None
    max_rows_override_value: int | None = None

    try:
        if report_path is not None:
            try:
                report_file = _parse_path_override(report_path, "--report")
            except ConfigError as exc:
                report_override_error = exc

        max_rows_override_value = _parse_optional_non_negative_int(max_rows_override, "--max-rows")
        if max_rows_override_value is not None:
            max_rows = max_rows_override_value

        db_override_path = _parse_optional_path_override(db_path_override, "--db")
        sheet_id_override_value = _parse_optional_non_empty_string(sheet_id_override, "--sheet-id")
        worksheet_override_value = _parse_optional_non_empty_string(
            worksheet_override,
            "--worksheet",
        )

        if report_override_error is not None:
            raise report_override_error

        pipeline = _load_pipeline_config(pipeline_path)

        db_path = db_override_path or Path(pipeline.paths.sqlite_db)
        report_file = report_file or _resolve_report_path(outputs_dir=Path(pipeline.paths.outputs_dir), date=started_dt)
        max_rows = (
            max_rows_override_value
            if max_rows_override_value is not None
            else pipeline.stage_7_persist.max_rows_default
        )

        sheets_enabled = pipeline.sheets.enabled
        spreadsheet_id = pipeline.sheets.spreadsheet_id
        worksheet_name = pipeline.sheets.worksheet_name
        key_column = pipeline.sheets.key_column
        header_row = pipeline.sheets.header_row

        LOGGER.info(
            "stage_7_persist start run_id=%s db_path=%s report_path=%s sheets_enabled=%s max_rows=%s",
            run_id,
            db_path,
            report_file,
            sheets_enabled,
            max_rows,
        )

        if not sheets_enabled:
            finished_at = to_utc_z(utc_now())
            result = PersistResult(
                run_id=run_id,
                run_status="completed",
                fatal_error=None,
                started_at=started_at,
                finished_at=finished_at,
                db_path=str(db_path),
                report_path=str(report_file),
                sheets_enabled=False,
                spreadsheet_id="",
                worksheet_name="",
                max_rows=max_rows,
                rows_available_total=0,
                rows_considered=0,
                rows_inserted=0,
                rows_updated=0,
                rows_skipped_missing_script=0,
                rows_skipped_invalid_payload=0,
                errors_count=0,
                first_error=None,
            )
            payload = result.to_dict()
            _validate_report_invariants(payload)
            _write_json(report_file, payload)
            LOGGER.info("stage_7_persist skipped sheets_enabled=false run_id=%s", run_id)
            return result

        if sheet_id_override_value is not None:
            spreadsheet_id = sheet_id_override_value
        if worksheet_override_value is not None:
            worksheet_name = worksheet_override_value

        store = _PersistStore(db_path=db_path)
        try:
            store.validate_dependencies()
            counters["rows_available_total"] = store.count_rows_available_total()
            selected_rows = store.select_rows(max_rows=max_rows)
        finally:
            store.close()
        counters["rows_considered"] = len(selected_rows)

        resolved_client = sheets_client or GoogleSheetsClient.from_env()
        worksheet = resolved_client.open_worksheet(
            spreadsheet_id=spreadsheet_id,
            worksheet_name=worksheet_name,
        )

        all_rows = worksheet.fetch_all_values()
        header_cells, data_rows = _split_sheet_rows(all_rows, header_row=header_row)
        layout = build_header_layout(header_cells, key_column=key_column)
        existing_by_key = _build_existing_key_map(
            data_rows=data_rows,
            layout=layout,
            header_row=header_row,
        )

        updated_at_value = to_utc_z(utc_now())

        for raw in selected_rows:
            if raw.get("script_item_id") is None:
                counters["rows_skipped_missing_script"] += 1
                continue

            try:
                payload = build_sheet_row(raw, updated_at=updated_at_value)
            except RowMappingError as exc:
                counters["rows_skipped_invalid_payload"] += 1
                if first_error is None:
                    first_error = _fatal_message(exc)
                continue

            existing = existing_by_key.get(payload.item_id)
            if existing is None:
                values = _build_insert_values(layout=layout, payload=payload)
                worksheet.append_row(values=values)
                counters["rows_inserted"] += 1
            else:
                values = _build_update_values(
                    layout=layout,
                    payload=payload,
                    existing_values=existing.values,
                )
                worksheet.update_row(row_number=existing.row_number, values=values)
                counters["rows_updated"] += 1
                existing_by_key[payload.item_id] = ExistingSheetRow(
                    row_number=existing.row_number,
                    values=values,
                )

        finished_at = to_utc_z(utc_now())
        result = PersistResult(
            run_id=run_id,
            run_status="completed",
            fatal_error=None,
            started_at=started_at,
            finished_at=finished_at,
            db_path=str(db_path),
            report_path=str(report_file),
            sheets_enabled=True,
            spreadsheet_id=spreadsheet_id,
            worksheet_name=worksheet_name,
            max_rows=max_rows,
            rows_available_total=counters["rows_available_total"],
            rows_considered=counters["rows_considered"],
            rows_inserted=counters["rows_inserted"],
            rows_updated=counters["rows_updated"],
            rows_skipped_missing_script=counters["rows_skipped_missing_script"],
            rows_skipped_invalid_payload=counters["rows_skipped_invalid_payload"],
            errors_count=counters["rows_skipped_invalid_payload"],
            first_error=first_error,
        )
        payload = result.to_dict()
        _validate_report_invariants(payload)
        _write_json(report_file, payload)
        LOGGER.info(
            "stage_7_persist complete run_id=%s rows_available_total=%s rows_considered=%s rows_inserted=%s rows_updated=%s rows_skipped_missing_script=%s rows_skipped_invalid_payload=%s",
            run_id,
            counters["rows_available_total"],
            counters["rows_considered"],
            counters["rows_inserted"],
            counters["rows_updated"],
            counters["rows_skipped_missing_script"],
            counters["rows_skipped_invalid_payload"],
        )
        return result
    except Exception as exc:
        fatal_error = _fatal_message(exc)
        finished_at = to_utc_z(utc_now())
        if first_error is None:
            first_error = fatal_error

        processed_total = (
            counters["rows_inserted"]
            + counters["rows_updated"]
            + counters["rows_skipped_missing_script"]
            + counters["rows_skipped_invalid_payload"]
        )
        if counters["rows_considered"] > processed_total:
            counters["rows_considered"] = processed_total

        if report_file is not None:
            payload = PersistResult(
                run_id=run_id,
                run_status="fatal",
                fatal_error=fatal_error,
                started_at=started_at,
                finished_at=finished_at,
                db_path=str(db_path) if db_path is not None else "",
                report_path=str(report_file),
                sheets_enabled=sheets_enabled,
                spreadsheet_id=spreadsheet_id if sheets_enabled else "",
                worksheet_name=worksheet_name if sheets_enabled else "",
                max_rows=max_rows,
                rows_available_total=counters["rows_available_total"],
                rows_considered=counters["rows_considered"],
                rows_inserted=counters["rows_inserted"],
                rows_updated=counters["rows_updated"],
                rows_skipped_missing_script=counters["rows_skipped_missing_script"],
                rows_skipped_invalid_payload=counters["rows_skipped_invalid_payload"],
                errors_count=counters["rows_skipped_invalid_payload"],
                first_error=first_error,
            ).to_dict()
            _validate_report_invariants(payload)
            try:
                _write_json(report_file, payload)
            except OSError as report_exc:
                raise FatalPersistError(
                    f"{fatal_error}; failed writing fatal report: {report_exc}"
                ) from report_exc

        raise FatalPersistError(fatal_error) from exc


def _new_counters() -> dict[str, int]:
    return {
        "rows_available_total": 0,
        "rows_considered": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_skipped_missing_script": 0,
        "rows_skipped_invalid_payload": 0,
    }


def _split_sheet_rows(
    rows: list[list[str]],
    *,
    header_row: int,
) -> tuple[list[str], list[list[str]]]:
    header_index = header_row - 1
    if header_index < len(rows):
        header = list(rows[header_index])
    else:
        header = []
    if header_row < len(rows):
        body = [list(row) for row in rows[header_row:]]
    else:
        body = []
    return header, body


def _build_existing_key_map(
    *,
    data_rows: list[list[str]],
    layout: HeaderLayout,
    header_row: int,
) -> dict[str, ExistingSheetRow]:
    by_key: dict[str, ExistingSheetRow] = {}
    expected_len = len(layout.headers)
    for offset, row in enumerate(data_rows):
        normalized_values = _pad_row(row, expected_len)
        key = normalized_values[layout.key_index].strip()
        if not key:
            continue
        if key in by_key:
            raise WorksheetContractError(f"duplicate worksheet key: {key}")
        row_number = header_row + 1 + offset
        by_key[key] = ExistingSheetRow(row_number=row_number, values=normalized_values)
    return by_key


def _build_insert_values(*, layout: HeaderLayout, payload: PersistSheetRow) -> list[str]:
    values = [""] * len(layout.headers)
    _set_payload_cells(values=values, layout=layout, payload=payload)
    _set_if_present(values=values, layout=layout, name="status", value="New")
    if layout.has_notes:
        _set_if_present(values=values, layout=layout, name="notes", value="")
    return values


def _build_update_values(
    *,
    layout: HeaderLayout,
    payload: PersistSheetRow,
    existing_values: list[str],
) -> list[str]:
    values = _pad_row(existing_values, len(layout.headers))
    _set_payload_cells(values=values, layout=layout, payload=payload)
    return values


def _set_payload_cells(*, values: list[str], layout: HeaderLayout, payload: PersistSheetRow) -> None:
    _set_if_present(values=values, layout=layout, name="item_id", value=payload.item_id)
    _set_if_present(values=values, layout=layout, name="creator", value=payload.creator)
    _set_if_present(values=values, layout=layout, name="post_link", value=payload.post_link)
    _set_if_present(values=values, layout=layout, name="topic", value=payload.topic)
    _set_if_present(values=values, layout=layout, name="viral_rating", value=str(payload.viral_rating))
    _set_if_present(values=values, layout=layout, name="hook", value=payload.hook)
    _set_if_present(values=values, layout=layout, name="platform", value=payload.platform)
    _set_if_present(values=values, layout=layout, name="draft_script", value=payload.draft_script)
    _set_if_present(
        values=values,
        layout=layout,
        name="monetization_angle",
        value=payload.monetization_angle,
    )
    _set_if_present(
        values=values,
        layout=layout,
        name="tools_mentioned",
        value=payload.tools_mentioned,
    )
    _set_if_present(values=values, layout=layout, name="published_at", value=payload.published_at)
    _set_if_present(values=values, layout=layout, name="updated_at", value=payload.updated_at)


def _set_if_present(*, values: list[str], layout: HeaderLayout, name: str, value: str) -> None:
    index = layout.index_by_name.get(name)
    if index is None:
        return
    values[index] = value


def _pad_row(row: list[str], target_len: int) -> list[str]:
    normalized = list(row[:target_len])
    if len(normalized) < target_len:
        normalized.extend([""] * (target_len - len(normalized)))
    return [cell if isinstance(cell, str) else ("" if cell is None else str(cell)) for cell in normalized]


def _fatal_message(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return message
    return exc.__class__.__name__


def _resolve_report_path(*, outputs_dir: Path, date: Any) -> Path:
    return outputs_dir / f"stage_7_report_{date.strftime('%Y-%m-%d')}.json"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def _load_pipeline_config(path: str | Path) -> PipelineConfig:
    raw = _load_yaml(path)
    paths_raw = raw.get("paths")
    sheets_raw = raw.get("sheets")
    stage_raw = raw.get("stage_7_persist")

    if not isinstance(paths_raw, dict):
        raise ConfigError("paths must be a mapping")
    if not isinstance(sheets_raw, dict):
        raise ConfigError("sheets must be a mapping")
    if not isinstance(stage_raw, dict):
        raise ConfigError("stage_7_persist must be a mapping")

    paths = PathsConfig(
        sqlite_db=_parse_non_empty_string(paths_raw.get("sqlite_db"), "paths.sqlite_db"),
        outputs_dir=_parse_non_empty_string(paths_raw.get("outputs_dir"), "paths.outputs_dir"),
    )
    sheets = _parse_sheets_config(sheets_raw)
    stage_7 = Stage7PersistConfig(
        max_rows_default=_parse_non_negative_int(
            stage_raw.get("max_rows_default"),
            "stage_7_persist.max_rows_default",
        )
    )
    return PipelineConfig(paths=paths, sheets=sheets, stage_7_persist=stage_7)


def _parse_sheets_config(raw: dict[str, Any]) -> SheetsConfig:
    enabled = raw.get("enabled")
    if not isinstance(enabled, bool):
        raise ConfigError("sheets.enabled must be a boolean")

    if enabled:
        return SheetsConfig(
            enabled=True,
            spreadsheet_id=_parse_non_empty_string(raw.get("spreadsheet_id"), "sheets.spreadsheet_id"),
            worksheet_name=_parse_non_empty_string(raw.get("worksheet_name"), "sheets.worksheet_name"),
            key_column=_parse_non_empty_string(raw.get("key_column"), "sheets.key_column"),
            header_row=_parse_header_row(raw.get("header_row"), "sheets.header_row"),
        )

    return SheetsConfig(
        enabled=False,
        spreadsheet_id=_normalize_optional_string(raw.get("spreadsheet_id")),
        worksheet_name=_normalize_optional_string(raw.get("worksheet_name")),
        key_column=_normalize_optional_string(raw.get("key_column")) or "item_id",
        header_row=_normalize_optional_header_row(raw.get("header_row")),
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


def _normalize_optional_string(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def _parse_header_row(value: Any, field_name: str) -> int:
    if not is_non_bool_int(value) or int(value) < 1:
        raise ConfigError(f"{field_name} must be a non-boolean integer >= 1")
    return int(value)


def _normalize_optional_header_row(value: Any) -> int:
    if is_non_bool_int(value) and int(value) >= 1:
        return int(value)
    return 1


def _parse_non_negative_int(value: Any, field_name: str) -> int:
    if not is_non_bool_int(value) or int(value) < 0:
        raise ConfigError(f"{field_name} must be a non-boolean integer >= 0")
    return int(value)


def _parse_path_override(value: str, flag: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{flag} must be a non-empty string")
    return Path(value.strip())


def _parse_optional_path_override(value: str | None, flag: str) -> Path | None:
    if value is None:
        return None
    return _parse_path_override(value, flag)


def _parse_optional_non_empty_string(value: str | None, flag: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{flag} must be a non-empty string")
    return value.strip()


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
        raise FatalPersistError("report invariant failed: run_status")
    if run_status == "completed" and fatal_error is not None:
        raise FatalPersistError("report invariant failed: completed run must have fatal_error null")
    if run_status == "fatal" and (not isinstance(fatal_error, str) or not fatal_error.strip()):
        raise FatalPersistError("report invariant failed: fatal run must have non-empty fatal_error")

    if not isinstance(payload.get("sheets_enabled"), bool):
        raise FatalPersistError("report invariant failed: sheets_enabled must be boolean")

    for field in (
        "run_id",
        "started_at",
        "finished_at",
        "db_path",
        "report_path",
        "spreadsheet_id",
        "worksheet_name",
    ):
        if not isinstance(payload.get(field), str):
            raise FatalPersistError(f"report invariant failed: {field} must be a string")

    int_fields = (
        "max_rows",
        "rows_available_total",
        "rows_considered",
        "rows_inserted",
        "rows_updated",
        "rows_skipped_missing_script",
        "rows_skipped_invalid_payload",
        "errors_count",
    )
    for field in int_fields:
        value = payload.get(field)
        if not is_non_bool_int(value) or int(value) < 0:
            raise FatalPersistError(f"report invariant failed: {field} must be integer >= 0")

    max_rows = int(payload["max_rows"])
    rows_considered = int(payload["rows_considered"])
    rows_inserted = int(payload["rows_inserted"])
    rows_updated = int(payload["rows_updated"])
    rows_skipped_missing_script = int(payload["rows_skipped_missing_script"])
    rows_skipped_invalid_payload = int(payload["rows_skipped_invalid_payload"])
    errors_count = int(payload["errors_count"])
    sheets_enabled = bool(payload["sheets_enabled"])
    spreadsheet_id = str(payload["spreadsheet_id"])
    worksheet_name = str(payload["worksheet_name"])
    first_error = payload.get("first_error")

    if errors_count != rows_skipped_invalid_payload:
        raise FatalPersistError("report invariant failed: errors_count mismatch")
    if rows_considered > max_rows:
        raise FatalPersistError("report invariant failed: rows_considered > max_rows")
    if (
        rows_inserted
        + rows_updated
        + rows_skipped_missing_script
        + rows_skipped_invalid_payload
        != rows_considered
    ):
        raise FatalPersistError("report invariant failed: row counters mismatch")

    if first_error is not None and (not isinstance(first_error, str) or not first_error.strip()):
        raise FatalPersistError("report invariant failed: first_error must be null or non-empty string")
    if errors_count == 0 and run_status == "completed" and first_error is not None:
        raise FatalPersistError("report invariant failed: completed run with no errors must have first_error null")
    if errors_count > 0 and (not isinstance(first_error, str) or not first_error.strip()):
        raise FatalPersistError("report invariant failed: first_error missing for row-level errors")
    if run_status == "fatal" and errors_count == 0 and first_error != fatal_error:
        raise FatalPersistError("report invariant failed: fatal first_error must equal fatal_error when no row errors")

    if run_status == "completed":
        if sheets_enabled:
            if not spreadsheet_id.strip() or not worksheet_name.strip():
                raise FatalPersistError(
                    "report invariant failed: completed sheets-enabled run requires sheet identifiers"
                )
        else:
            if spreadsheet_id or worksheet_name:
                raise FatalPersistError(
                    "report invariant failed: completed sheets-disabled run must emit empty sheet identifiers"
                )
            for field in (
                "rows_available_total",
                "rows_considered",
                "rows_inserted",
                "rows_updated",
                "rows_skipped_missing_script",
                "rows_skipped_invalid_payload",
                "errors_count",
            ):
                if int(payload[field]) != 0:
                    raise FatalPersistError(
                        "report invariant failed: completed sheets-disabled run must emit zero counters"
                    )


@dataclass
class _PersistStore:
    db_path: Path

    def __post_init__(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.Error as exc:
            raise StateError(f"failed to initialize sqlite db at {self.db_path}") from exc

    def close(self) -> None:
        self._conn.close()

    def validate_dependencies(self) -> None:
        self._validate_table_compatibility(
            table_name="ideas",
            required_columns=REQUIRED_IDEAS_COLUMNS,
        )
        self._validate_table_compatibility(
            table_name="scripts",
            required_columns=REQUIRED_SCRIPTS_COLUMNS,
        )
        self._validate_table_compatibility(
            table_name="items",
            required_columns=REQUIRED_ITEMS_COLUMNS,
        )

    def count_rows_available_total(self) -> int:
        try:
            row = self._conn.execute("SELECT COUNT(*) FROM ideas").fetchone()
            return int(row[0]) if row else 0
        except sqlite3.Error as exc:
            raise StateError("failed counting stage_7 available rows") from exc

    def select_rows(self, *, max_rows: int) -> list[dict[str, Any]]:
        try:
            rows = self._conn.execute(
                """
                SELECT
                  i.item_id,
                  i.url,
                  i.title,
                  i.topic,
                  i.viral_rating,
                  i.hooks,
                  i.platform,
                  i.monetization_angle,
                  i.tools_mentioned,
                  i.published_at,
                  s.item_id AS script_item_id,
                  s.primary_hook,
                  s.script_sections,
                  it.creator,
                  it.source_name
                FROM ideas i
                LEFT JOIN scripts s ON s.item_id = i.item_id
                LEFT JOIN items it ON it.item_id = i.item_id
                ORDER BY i.viral_rating DESC, i.published_at DESC, i.item_id ASC
                LIMIT :max_rows
                """,
                {"max_rows": max_rows},
            ).fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as exc:
            raise StateError("failed selecting stage_7 rows") from exc

    def _validate_table_compatibility(self, *, table_name: str, required_columns: dict[str, str]) -> None:
        try:
            exists = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table_name,),
            ).fetchone()
            if exists is None:
                raise StateError(f"{table_name} table is missing")
            rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        except sqlite3.Error as exc:
            raise StateError(f"failed validating {table_name} table compatibility") from exc

        if not rows:
            raise StateError(f"{table_name} table compatibility check failed")

        by_name = {str(row["name"]): row for row in rows}
        for name, expected_type in required_columns.items():
            row = by_name.get(name)
            if row is None:
                raise StateError(f"{table_name} table missing required column: {name}")
            declared_type = str(row["type"]).upper()
            if declared_type != expected_type:
                raise StateError(
                    f"{table_name} table incompatible type for {name}: expected {expected_type}, got {declared_type}"
                )

            is_pk = int(row["pk"]) == 1
            is_not_null = int(row["notnull"]) == 1
            if name == "item_id":
                if not is_pk:
                    raise StateError(f"{table_name}.item_id must be primary key")
            else:
                if not is_not_null:
                    raise StateError(f"{table_name}.{name} must be NOT NULL")
