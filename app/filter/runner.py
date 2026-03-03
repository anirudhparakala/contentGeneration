from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .models import (
    CandidateItem,
    KeywordGroup,
    compile_keyword_groups,
    normalize_required_fields,
    score_relevance,
    select_body_text,
    utc_now_z,
)
from .state import CandidatesStore, StateError


LOGGER = logging.getLogger(__name__)
FAIL_REASONS = ("missing_required_fields", "content_too_short", "low_relevance_score")


class FatalFilterError(RuntimeError):
    pass


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class PathsConfig:
    sqlite_db: str
    outputs_dir: str


@dataclass(frozen=True)
class Stage3FilterConfig:
    min_content_chars: int
    min_relevance_score: int
    max_candidates_default: int
    keyword_groups: dict[str, KeywordGroup]


@dataclass(frozen=True)
class PipelineConfig:
    paths: PathsConfig
    stage_3_filter: Stage3FilterConfig


@dataclass
class FilterResult:
    run_id: str
    started_at: str
    finished_at: str
    db_path: str
    output_path: str
    report_path: str
    items_available_total: int
    items_considered: int
    passed_count: int
    failed_count: int
    inserted_db: int
    candidate_items_emitted: int
    candidates_skipped_already_present: int
    max_candidates: int
    reached_max_candidates: bool
    fail_breakdown: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_filter(
    *,
    pipeline_path: str,
    out_path: str | None = None,
    report_path: str | None = None,
    db_path_override: str | None = None,
    max_candidates_override: int | None = None,
) -> FilterResult:
    started_dt = _utc_now()
    started_at = _to_utc_z(started_dt)
    run_id = str(uuid.uuid4())

    try:
        pipeline = _load_pipeline_config(pipeline_path)
    except ConfigError as exc:
        raise FatalFilterError(str(exc)) from exc

    max_candidates = (
        max_candidates_override
        if max_candidates_override is not None
        else pipeline.stage_3_filter.max_candidates_default
    )
    if not _is_non_bool_int(max_candidates) or int(max_candidates) < 0:
        raise FatalFilterError("max_candidates must be an integer >= 0")
    max_candidates = int(max_candidates)

    outputs_dir = Path(pipeline.paths.outputs_dir)
    out_file = _resolve_out_path(out_path=out_path, outputs_dir=outputs_dir, date=started_dt)
    report_file = _resolve_report_path(report_path=report_path, outputs_dir=outputs_dir, date=started_dt)
    db_path = Path(db_path_override or pipeline.paths.sqlite_db)

    fail_breakdown = {reason: 0 for reason in FAIL_REASONS}
    items_considered = 0
    passed_count = 0
    failed_count = 0
    inserted_db = 0
    candidate_items_emitted = 0
    candidates_skipped_already_present = 0

    LOGGER.info(
        "stage_3_filter start run_id=%s db_path=%s output_path=%s max_candidates=%s",
        run_id,
        db_path,
        out_file,
        max_candidates,
    )

    store: CandidatesStore | None = None
    items_available_total = 0
    try:
        store = CandidatesStore(db_path=db_path)
        items_available_total = store.count_unprocessed_items()
        unprocessed_rows = store.iter_unprocessed_items()

        out_file.parent.mkdir(parents=True, exist_ok=True)
        with out_file.open("w", encoding="utf-8", newline="\n") as out_handle:
            if max_candidates > 0:
                for row in unprocessed_rows:
                    if passed_count == max_candidates:
                        break

                    items_considered += 1
                    required = normalize_required_fields(row)
                    if required is None:
                        failed_count += 1
                        fail_breakdown["missing_required_fields"] += 1
                        continue

                    body_text = select_body_text(
                        summary=row.get("summary"),
                        content_text=row.get("content_text"),
                    )
                    if len(body_text) < pipeline.stage_3_filter.min_content_chars:
                        failed_count += 1
                        fail_breakdown["content_too_short"] += 1
                        continue

                    relevance_score, matched_keywords = score_relevance(
                        title=required["title"],
                        body_text=body_text,
                        keyword_groups=pipeline.stage_3_filter.keyword_groups,
                    )
                    if relevance_score < pipeline.stage_3_filter.min_relevance_score:
                        failed_count += 1
                        fail_breakdown["low_relevance_score"] += 1
                        continue

                    passed_count += 1
                    scored_at = utc_now_z()
                    candidate = CandidateItem(
                        item_id=required["item_id"],
                        source_type=required["source_type"],
                        source_id=required["source_id"],
                        source_name=required["source_name"],
                        creator=required["creator"],
                        title=required["title"],
                        url=required["url"],
                        published_at=required["published_at"],
                        fetched_at=required["fetched_at"],
                        content_text=body_text,
                        relevance_score=relevance_score,
                        matched_keywords=matched_keywords,
                        scored_at=scored_at,
                    )

                    inserted = store.insert_candidate(candidate, created_at=scored_at)
                    if not inserted:
                        candidates_skipped_already_present += 1
                        continue

                    inserted_db += 1
                    candidate_items_emitted += 1
                    out_handle.write(json.dumps(candidate.to_dict(), ensure_ascii=True))
                    out_handle.write("\n")
    except StateError as exc:
        raise FatalFilterError(str(exc)) from exc
    except OSError as exc:
        raise FatalFilterError(f"failed writing output: {exc}") from exc
    finally:
        if store is not None:
            store.close()

    reached_max_candidates = passed_count == max_candidates
    finished_at = _to_utc_z(_utc_now())
    result = FilterResult(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        db_path=str(db_path),
        output_path=str(out_file),
        report_path=str(report_file),
        items_available_total=items_available_total,
        items_considered=items_considered,
        passed_count=passed_count,
        failed_count=failed_count,
        inserted_db=inserted_db,
        candidate_items_emitted=candidate_items_emitted,
        candidates_skipped_already_present=candidates_skipped_already_present,
        max_candidates=max_candidates,
        reached_max_candidates=reached_max_candidates,
        fail_breakdown=fail_breakdown,
    )
    _validate_result_invariants(result)

    try:
        _write_json(report_file, result.to_dict())
    except OSError as exc:
        raise FatalFilterError(f"failed writing report: {exc}") from exc

    if failed_count:
        LOGGER.warning(
            "stage_3_filter fail breakdown missing_required_fields=%s content_too_short=%s low_relevance_score=%s",
            fail_breakdown["missing_required_fields"],
            fail_breakdown["content_too_short"],
            fail_breakdown["low_relevance_score"],
        )

    LOGGER.info(
        "stage_3_filter complete run_id=%s items_available_total=%s items_considered=%s passed_count=%s failed_count=%s inserted_db=%s candidate_items_emitted=%s candidates_skipped_already_present=%s reached_max_candidates=%s",
        run_id,
        items_available_total,
        items_considered,
        passed_count,
        failed_count,
        inserted_db,
        candidate_items_emitted,
        candidates_skipped_already_present,
        reached_max_candidates,
    )
    return result


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

    stage = data.get("stage_3_filter")
    if not isinstance(stage, dict):
        raise ConfigError("stage_3_filter must be a mapping")

    min_content_chars = _parse_non_negative_int(
        stage.get("min_content_chars"), "stage_3_filter.min_content_chars"
    )
    min_relevance_score = _parse_non_negative_int(
        stage.get("min_relevance_score"), "stage_3_filter.min_relevance_score"
    )
    max_candidates_default = _parse_non_negative_int(
        stage.get("max_candidates_default"), "stage_3_filter.max_candidates_default"
    )
    try:
        keyword_groups = compile_keyword_groups(stage.get("keyword_groups"))
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc

    return PipelineConfig(
        paths=PathsConfig(sqlite_db=sqlite_db.strip(), outputs_dir=outputs_dir.strip()),
        stage_3_filter=Stage3FilterConfig(
            min_content_chars=min_content_chars,
            min_relevance_score=min_relevance_score,
            max_candidates_default=max_candidates_default,
            keyword_groups=keyword_groups,
        ),
    )


def _parse_non_negative_int(value: Any, field_name: str) -> int:
    if not _is_non_bool_int(value) or int(value) < 0:
        raise ConfigError(f"{field_name} must be an integer >= 0")
    return int(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, indent=2, ensure_ascii=True)
        f.write("\n")


def _resolve_out_path(*, out_path: str | None, outputs_dir: Path, date: datetime) -> Path:
    if out_path:
        return Path(out_path)
    return outputs_dir / f"candidate_items_{date.strftime('%Y-%m-%d')}.jsonl"


def _resolve_report_path(*, report_path: str | None, outputs_dir: Path, date: datetime) -> Path:
    if report_path:
        return Path(report_path)
    return outputs_dir / f"stage_3_report_{date.strftime('%Y-%m-%d')}.json"


def _validate_result_invariants(result: FilterResult) -> None:
    fail_breakdown_total = sum(result.fail_breakdown.values())
    if result.items_considered != (result.passed_count + result.failed_count):
        raise FatalFilterError("counter invariant failed: items_considered != passed_count + failed_count")
    if result.inserted_db != result.candidate_items_emitted:
        raise FatalFilterError("counter invariant failed: inserted_db != candidate_items_emitted")
    if result.items_considered > result.items_available_total:
        raise FatalFilterError("counter invariant failed: items_considered > items_available_total")
    if result.candidate_items_emitted > result.max_candidates:
        raise FatalFilterError("counter invariant failed: candidate_items_emitted > max_candidates")
    if result.failed_count != fail_breakdown_total:
        raise FatalFilterError("counter invariant failed: failed_count != fail_breakdown total")
    if result.passed_count != (result.inserted_db + result.candidates_skipped_already_present):
        raise FatalFilterError(
            "counter invariant failed: passed_count != inserted_db + candidates_skipped_already_present"
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


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _to_utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_non_bool_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)
