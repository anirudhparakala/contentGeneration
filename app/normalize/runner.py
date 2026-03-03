from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .models import CanonicalItem, normalize_raw_item, utc_now_z
from .state import ItemsStore, StateError


LOGGER = logging.getLogger(__name__)
RAW_ITEMS_DATED_RE = re.compile(r"^raw_items_(\d{4}-\d{2}-\d{2})\.jsonl$")


class FatalNormalizeError(RuntimeError):
    pass


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class PathsConfig:
    sqlite_db: str
    outputs_dir: str


@dataclass(frozen=True)
class PipelineConfig:
    paths: PathsConfig


@dataclass
class NormalizeResult:
    run_id: str
    input_path: str
    output_path: str
    db_path: str
    report_path: str
    total_lines_read: int
    total_raw_items_parsed: int
    canonical_items_emitted: int
    items_inserted_db: int
    items_skipped_already_present: int
    items_skipped_invalid: int
    invalid_json_lines: int
    invalid_json_objects: int
    missing_required_fields: int
    invalid_field_types: int
    invalid_timestamps: int
    started_at: str
    finished_at: str

    def to_dict(self) -> dict:
        return asdict(self)


def run_normalize(
    *,
    pipeline_path: str,
    in_path: str | None = None,
    out_path: str | None = None,
    report_path: str | None = None,
    db_path_override: str | None = None,
) -> NormalizeResult:
    started_dt = _utc_now()
    started_at = _to_utc_z(started_dt)
    run_id = str(uuid.uuid4())

    try:
        pipeline = _load_pipeline_config(pipeline_path)
    except ConfigError as exc:
        raise FatalNormalizeError(str(exc)) from exc

    input_file = _resolve_input_path(in_path=in_path, outputs_dir=Path(pipeline.paths.outputs_dir))
    out_file = _resolve_out_path(out_path=out_path, outputs_dir=Path(pipeline.paths.outputs_dir), date=started_dt)
    report_file = _resolve_report_path(
        report_path=report_path,
        outputs_dir=Path(pipeline.paths.outputs_dir),
        date=started_dt,
    )
    db_path = Path(db_path_override or pipeline.paths.sqlite_db)

    LOGGER.info("stage_2_normalize start run_id=%s input_path=%s", run_id, input_file)

    try:
        valid_items, counters = _load_and_normalize(input_file)
    except OSError as exc:
        raise FatalNormalizeError(f"failed reading input: {exc}") from exc

    try:
        store = ItemsStore(db_path=db_path)
    except StateError as exc:
        raise FatalNormalizeError(str(exc)) from exc

    inserted = 0
    skipped_existing = 0
    try:
        for item in valid_items:
            if store.insert_if_new(item, inserted_at=utc_now_z()):
                inserted += 1
            else:
                skipped_existing += 1
    except StateError as exc:
        raise FatalNormalizeError(str(exc)) from exc
    finally:
        store.close()

    try:
        _write_jsonl(out_file, valid_items)
    except OSError as exc:
        raise FatalNormalizeError(f"failed writing canonical output: {exc}") from exc

    finished_at = _to_utc_z(_utc_now())
    items_skipped_invalid = (
        counters["invalid_json_lines"]
        + counters["invalid_json_objects"]
        + counters["missing_required_fields"]
        + counters["invalid_field_types"]
        + counters["invalid_timestamps"]
    )
    payload = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "input_path": str(input_file),
        "output_path": str(out_file),
        "db_path": str(db_path),
        "report_path": str(report_file),
        "total_lines_read": counters["total_lines_read"],
        "total_raw_items_parsed": counters["total_raw_items_parsed"],
        "canonical_items_emitted": len(valid_items),
        "items_inserted_db": inserted,
        "items_skipped_already_present": skipped_existing,
        "items_skipped_invalid": items_skipped_invalid,
        "invalid_json_lines": counters["invalid_json_lines"],
        "invalid_json_objects": counters["invalid_json_objects"],
        "missing_required_fields": counters["missing_required_fields"],
        "invalid_field_types": counters["invalid_field_types"],
        "invalid_timestamps": counters["invalid_timestamps"],
    }

    try:
        _write_json(report_file, payload)
    except OSError as exc:
        raise FatalNormalizeError(f"failed writing report: {exc}") from exc

    if items_skipped_invalid:
        LOGGER.warning(
            "stage_2_normalize invalid counts invalid_json_lines=%s invalid_json_objects=%s invalid_field_types=%s missing_required_fields=%s invalid_timestamps=%s",
            counters["invalid_json_lines"],
            counters["invalid_json_objects"],
            counters["invalid_field_types"],
            counters["missing_required_fields"],
            counters["invalid_timestamps"],
        )

    LOGGER.info(
        "stage_2_normalize complete run_id=%s canonical_items_emitted=%s items_inserted_db=%s items_skipped_already_present=%s items_skipped_invalid=%s",
        run_id,
        len(valid_items),
        inserted,
        skipped_existing,
        items_skipped_invalid,
    )

    return NormalizeResult(
        run_id=run_id,
        input_path=str(input_file),
        output_path=str(out_file),
        db_path=str(db_path),
        report_path=str(report_file),
        total_lines_read=counters["total_lines_read"],
        total_raw_items_parsed=counters["total_raw_items_parsed"],
        canonical_items_emitted=len(valid_items),
        items_inserted_db=inserted,
        items_skipped_already_present=skipped_existing,
        items_skipped_invalid=items_skipped_invalid,
        invalid_json_lines=counters["invalid_json_lines"],
        invalid_json_objects=counters["invalid_json_objects"],
        missing_required_fields=counters["missing_required_fields"],
        invalid_field_types=counters["invalid_field_types"],
        invalid_timestamps=counters["invalid_timestamps"],
        started_at=started_at,
        finished_at=finished_at,
    )


def _resolve_input_path(*, in_path: str | None, outputs_dir: Path) -> Path:
    if in_path:
        return Path(in_path)

    dated_candidates: list[tuple[datetime, Path]] = []
    if outputs_dir.exists():
        for child in outputs_dir.iterdir():
            if not child.is_file():
                continue
            match = RAW_ITEMS_DATED_RE.match(child.name)
            if not match:
                continue
            try:
                parsed = datetime.strptime(match.group(1), "%Y-%m-%d")
            except ValueError:
                continue
            dated_candidates.append((parsed, child))

    if dated_candidates:
        dated_candidates.sort(key=lambda row: row[0])
        return dated_candidates[-1][1]

    fallback = outputs_dir / "raw_items.jsonl"
    if fallback.is_file():
        return fallback

    raise FatalNormalizeError("no input file found from defaults in outputs_dir")


def _resolve_out_path(*, out_path: str | None, outputs_dir: Path, date: datetime) -> Path:
    if out_path:
        return Path(out_path)
    return outputs_dir / f"canonical_items_{date.strftime('%Y-%m-%d')}.jsonl"


def _resolve_report_path(*, report_path: str | None, outputs_dir: Path, date: datetime) -> Path:
    if report_path:
        return Path(report_path)
    return outputs_dir / f"stage_2_report_{date.strftime('%Y-%m-%d')}.json"


def _load_and_normalize(input_file: Path) -> tuple[list[CanonicalItem], dict[str, int]]:
    counters = {
        "total_lines_read": 0,
        "total_raw_items_parsed": 0,
        "invalid_json_lines": 0,
        "invalid_json_objects": 0,
        "missing_required_fields": 0,
        "invalid_field_types": 0,
        "invalid_timestamps": 0,
    }
    items: list[CanonicalItem] = []

    with input_file.open("r", encoding="utf-8") as f:
        for line in f:
            counters["total_lines_read"] += 1
            raw_line = line.strip()
            if not raw_line:
                continue

            try:
                parsed = json.loads(raw_line)
            except json.JSONDecodeError:
                counters["invalid_json_lines"] += 1
                continue

            if not isinstance(parsed, dict):
                counters["invalid_json_objects"] += 1
                continue

            counters["total_raw_items_parsed"] += 1
            outcome = normalize_raw_item(parsed)
            if outcome.item is not None:
                items.append(outcome.item)
            else:
                counters[outcome.invalid_reason or "missing_required_fields"] += 1

    return items, counters


def _write_jsonl(path: Path, items: list[CanonicalItem]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for item in items:
            f.write(json.dumps(item.to_dict(), ensure_ascii=True))
            f.write("\n")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, indent=2, ensure_ascii=True)
        f.write("\n")


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _to_utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_pipeline_config(path: str | Path) -> PipelineConfig:
    data = _load_yaml(path)
    paths = data.get("paths")
    if not isinstance(paths, dict):
        raise ConfigError("paths must be a mapping")
    sqlite_db = paths.get("sqlite_db")
    outputs_dir = paths.get("outputs_dir")
    if not isinstance(sqlite_db, str) or not sqlite_db.strip():
        raise ConfigError("paths.sqlite_db must be a non-empty string")
    if not isinstance(outputs_dir, str) or not outputs_dir.strip():
        raise ConfigError("paths.outputs_dir must be a non-empty string")
    return PipelineConfig(
        paths=PathsConfig(
            sqlite_db=sqlite_db.strip(),
            outputs_dir=outputs_dir.strip(),
        )
    )


def _load_yaml(path: str | Path) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except FileNotFoundError as exc:
        raise ConfigError(f"missing config file: {path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"top-level config must be a mapping: {path}")
    return raw
