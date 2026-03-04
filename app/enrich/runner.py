from __future__ import annotations

import importlib
import importlib.metadata as importlib_metadata
import json
import logging
import re
import subprocess
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from .fetch import HttpPolicy
from .models import (
    FAIL_BREAKDOWN_KEYS,
    CandidateRow,
    EnrichedItem,
    is_non_bool_int,
    parse_candidate_row,
    to_utc_z,
    utc_now,
    utc_now_z,
)
from .newsletter import (
    NewsletterExtractError,
    NewsletterFetchError,
    NewsletterTextTooShortError,
    enrich_newsletter,
)
from .state import EnrichStore
from .youtube import (
    ASRError,
    ASRAudioConfig,
    ASRDecodeConfig,
    ASRRuntime,
    ASRStartupError,
    VideoIdParseError,
    build_asr_evidence,
    initialize_asr_runtime,
    parse_video_id,
    resolve_executable,
)


LOGGER = logging.getLogger(__name__)
UTC_Z_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PHASE_KEYS: tuple[str, ...] = (
    "floor_newsletter",
    "floor_youtube",
    "source_diversity",
    "remainder",
)
COOLDOWN_REASON_ALLOWED: frozenset[str] = frozenset(
    key for key in FAIL_BREAKDOWN_KEYS if key != "invalid_candidate_row"
)
REQUIREMENTS_PIN_RE = re.compile(
    r"^(?P<name>[A-Za-z0-9_.-]+)\s*==\s*(?P<version>[^;\s]+)$"
)


class FatalEnrichError(RuntimeError):
    pass


class ConfigError(ValueError):
    pass


class FullSuccessTerminationError(RuntimeError):
    pass


@dataclass(frozen=True)
class PathsConfig:
    sqlite_db: str
    outputs_dir: str


@dataclass(frozen=True)
class CapsConfig:
    max_transcripts_per_run: int
    max_asr_fallbacks_per_run: int


@dataclass(frozen=True)
class HttpRetriesConfig:
    max_attempts: int


@dataclass(frozen=True)
class HttpConfig:
    user_agent: str
    connect_timeout_s: int
    read_timeout_s: int
    max_response_mb: int
    retries: HttpRetriesConfig


@dataclass(frozen=True)
class SelectionPolicyConfig:
    min_newsletters_per_run: int
    min_youtube_per_run: int
    max_items_per_source: int
    source_diversity_first_pass: bool


@dataclass(frozen=True)
class CooldownPolicyConfig:
    enabled: bool
    after_consecutive_failures: int
    skip_for_hours: int
    reasons: frozenset[str]


@dataclass(frozen=True)
class YouTubeAudioConfig:
    format_selector: str
    extract_format: str
    download_timeout_s: int
    download_retries: int
    retry_backoff_s: float


@dataclass(frozen=True)
class YouTubeASRConfig:
    model: str
    device: str
    compute_type: str
    language: str
    beam_size: int
    temperature: float
    condition_on_previous_text: bool
    vad_filter: bool
    max_audio_seconds: int
    min_chars: int
    allow_cpu_fallback: bool


@dataclass(frozen=True)
class YouTubeEnrichmentConfig:
    mode: str
    require_full_success: bool
    audio: YouTubeAudioConfig
    asr: YouTubeASRConfig


@dataclass(frozen=True)
class Stage4EnrichConfig:
    max_items_default: int
    selection_policy: SelectionPolicyConfig
    cooldown_policy: CooldownPolicyConfig
    youtube_enrichment: YouTubeEnrichmentConfig


@dataclass(frozen=True)
class PipelineConfig:
    paths: PathsConfig
    caps: CapsConfig
    http: HttpConfig
    stage_4_enrich: Stage4EnrichConfig


@dataclass
class EnrichResult:
    run_id: str
    run_status: str
    fatal_error: str | None
    started_at: str
    finished_at: str
    db_path: str
    output_path: str
    report_path: str
    candidates_available_total: int
    selected_rows_total: int
    invalid_pool_total: int
    eligible_pool_total: int
    eligible_newsletters_total: int
    eligible_youtube_total: int
    cooldown_blocked_total: int
    items_selected: int
    success_count: int
    failed_count: int
    inserted_db: int
    skipped_already_enriched: int
    youtube_transcripts_attempted: int
    youtube_transcripts_succeeded: int
    asr_fallbacks_used: int
    youtube_asr_attempted: int
    youtube_asr_succeeded: int
    youtube_asr_model: str
    youtube_asr_device_effective: str
    youtube_asr_compute_type: str
    youtube_preflight_executed: bool
    full_success_required: bool
    max_items: int
    max_transcripts: int
    max_asr: int
    selected_newsletter_count: int
    selected_youtube_count: int
    selected_invalid_count: int
    selected_unique_sources: int
    newsletter_floor_target: int
    youtube_floor_target: int
    newsletter_floor_met: bool
    youtube_floor_met: bool
    source_diversity_first_pass_applied: bool
    selected_phase_breakdown: dict[str, int]
    fail_breakdown: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _SelectionRow:
    raw: dict[str, Any]
    candidate: CandidateRow | None
    base_index: int


@dataclass(frozen=True)
class _SelectionPlan:
    selected_rows: list[_SelectionRow]
    selected_rows_total: int
    invalid_pool_total: int
    eligible_pool_total: int
    eligible_newsletters_total: int
    eligible_youtube_total: int
    cooldown_blocked_total: int
    selected_newsletter_count: int
    selected_youtube_count: int
    selected_invalid_count: int
    selected_unique_sources: int
    newsletter_floor_target: int
    youtube_floor_target: int
    newsletter_floor_met: bool
    youtube_floor_met: bool
    source_diversity_first_pass_applied: bool
    selected_phase_breakdown: dict[str, int]


def run_enrich(
    *,
    pipeline_path: str,
    out_path: str | None = None,
    report_path: str | None = None,
    db_path_override: str | None = None,
    max_items_override: int | None = None,
    max_transcripts_override: int | None = None,
    max_asr_override: int | None = None,
) -> EnrichResult:
    started_dt = utc_now()
    started_at = to_utc_z(started_dt)
    run_id = str(uuid.uuid4())

    report_file: Path | None = Path(report_path) if report_path else None
    output_file: Path | None = Path(out_path) if out_path else None
    db_path: Path | None = Path(db_path_override) if db_path_override else None

    fail_breakdown = _new_fail_breakdown()
    counters = _new_counters()
    selection_stats = _new_selection_stats()

    max_items = 0
    max_transcripts = 0
    max_asr = 0
    selected_rows: list[_SelectionRow] = []
    cooldown_policy: CooldownPolicyConfig | None = None
    full_success_required = False
    youtube_asr_model = ""
    youtube_asr_device_effective = ""
    youtube_asr_compute_type = ""
    youtube_preflight_executed = False
    youtube_asr_min_chars = 800
    youtube_runtime: ASRRuntime | None = None

    try:
        max_items_override_valid = _validate_override(max_items_override, "max_items")
        max_transcripts_override_valid = _validate_override(
            max_transcripts_override, "max_transcripts"
        )
        max_asr_override_valid = _validate_override(max_asr_override, "max_asr")

        if max_items_override_valid is not None:
            max_items = max_items_override_valid
        if max_transcripts_override_valid is not None:
            max_transcripts = max_transcripts_override_valid
        if max_asr_override_valid is not None:
            max_asr = max_asr_override_valid

        _ensure_required_dependencies()
        pipeline = _load_pipeline_config(pipeline_path)
        cooldown_policy = pipeline.stage_4_enrich.cooldown_policy
        full_success_required = pipeline.stage_4_enrich.youtube_enrichment.require_full_success
        youtube_asr_model = pipeline.stage_4_enrich.youtube_enrichment.asr.model
        youtube_asr_device_effective = pipeline.stage_4_enrich.youtube_enrichment.asr.device
        youtube_asr_compute_type = pipeline.stage_4_enrich.youtube_enrichment.asr.compute_type
        youtube_asr_min_chars = pipeline.stage_4_enrich.youtube_enrichment.asr.min_chars

        if db_path is None:
            db_path = Path(pipeline.paths.sqlite_db)
        if output_file is None:
            output_file = _resolve_output_path(outputs_dir=Path(pipeline.paths.outputs_dir), date=started_dt)
        if report_file is None:
            report_file = _resolve_report_path(outputs_dir=Path(pipeline.paths.outputs_dir), date=started_dt)

        max_items = (
            max_items_override_valid
            if max_items_override_valid is not None
            else pipeline.stage_4_enrich.max_items_default
        )
        max_transcripts = (
            max_transcripts_override_valid
            if max_transcripts_override_valid is not None
            else pipeline.caps.max_transcripts_per_run
        )
        max_asr = (
            max_asr_override_valid
            if max_asr_override_valid is not None
            else pipeline.caps.max_asr_fallbacks_per_run
        )

        if pipeline.stage_4_enrich.youtube_enrichment.mode == "asr_only":
            if max_transcripts_override_valid is not None:
                LOGGER.warning(
                    "stage_4_enrich ignoring --max-transcripts override in asr_only mode: %s",
                    max_transcripts_override_valid,
                )
            if max_asr_override_valid is not None:
                LOGGER.warning(
                    "stage_4_enrich ignoring --max-asr override in asr_only mode: %s",
                    max_asr_override_valid,
                )

        http_policy = HttpPolicy(
            user_agent=pipeline.http.user_agent,
            connect_timeout_s=pipeline.http.connect_timeout_s,
            read_timeout_s=pipeline.http.read_timeout_s,
            max_response_bytes=pipeline.http.max_response_mb * 1024 * 1024,
            max_attempts=pipeline.http.retries.max_attempts,
        )

        LOGGER.info(
            "stage_4_enrich start run_id=%s db_path=%s output_path=%s report_path=%s max_items=%s max_transcripts=%s max_asr=%s",
            run_id,
            db_path,
            output_file,
            report_file,
            max_items,
            max_transcripts,
            max_asr,
        )

        def record_item_failure(
            *,
            candidate: CandidateRow | None,
            raw: dict[str, Any],
            reason: str,
        ) -> None:
            if candidate is None:
                _record_failure(counters, fail_breakdown, reason)
                item_id = _extract_item_id(raw)
            else:
                _record_failure_with_retry(
                    store=store,
                    counters=counters,
                    fail_breakdown=fail_breakdown,
                    candidate=candidate,
                    reason=reason,
                    cooldown_policy=cooldown_policy,
                )
                item_id = candidate.item_id
            if full_success_required:
                raise FullSuccessTerminationError(
                    f"full_success_required violation: reason={reason} item_id={item_id}"
                )

        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("w", encoding="utf-8", newline="\n") as out_handle:
            store = EnrichStore(db_path=db_path)
            try:
                store.ensure_enriched_items_table()
                store.ensure_retry_state_table()
                store.validate_candidates_table()

                raw_candidates = store.select_unenriched_candidates(max_items=None)
                counters["candidates_available_total"] = len(raw_candidates)

                selection = _build_selection_plan(
                    raw_rows=raw_candidates,
                    max_items=max_items,
                    started_dt=started_dt,
                    selection_policy=pipeline.stage_4_enrich.selection_policy,
                    cooldown_policy=pipeline.stage_4_enrich.cooldown_policy,
                    include_invalid_in_selection=not full_success_required,
                    store=store,
                )
                selected_rows = selection.selected_rows
                _apply_selection_stats(selection_stats, selection)

                if selection.selected_youtube_count > 0:
                    youtube_preflight_executed = True
                    youtube_runtime = _prepare_youtube_asr_runtime(
                        youtube_config=pipeline.stage_4_enrich.youtube_enrichment,
                    )
                    youtube_asr_device_effective = youtube_runtime.device_effective
                    youtube_asr_compute_type = youtube_runtime.compute_type_effective

                for selected in selected_rows:
                    candidate = selected.candidate
                    if candidate is None:
                        record_item_failure(
                            candidate=None,
                            raw=selected.raw,
                            reason="invalid_candidate_row",
                        )
                        continue

                    if candidate.source_type == "newsletter":
                        try:
                            enriched_text, evidence_snippets = enrich_newsletter(
                                url=candidate.url,
                                http_policy=http_policy,
                            )
                            enrichment_method = "trafilatura"
                        except NewsletterFetchError:
                            record_item_failure(
                                candidate=candidate,
                                raw=selected.raw,
                                reason="newsletter_fetch_failed",
                            )
                            continue
                        except NewsletterTextTooShortError:
                            record_item_failure(
                                candidate=candidate,
                                raw=selected.raw,
                                reason="newsletter_text_too_short",
                            )
                            continue
                        except NewsletterExtractError:
                            record_item_failure(
                                candidate=candidate,
                                raw=selected.raw,
                                reason="newsletter_extract_failed",
                            )
                            continue
                        except Exception:
                            record_item_failure(
                                candidate=candidate,
                                raw=selected.raw,
                                reason="newsletter_extract_failed",
                            )
                            continue
                    else:
                        try:
                            parse_video_id(candidate.url)
                            counters["youtube_asr_attempted"] += 1
                            if youtube_runtime is None:
                                raise ASRError("youtube asr runtime unavailable")
                            enriched_text = youtube_runtime.transcribe_url(candidate.url)
                            counters["youtube_asr_succeeded"] += 1
                            evidence_snippets = build_asr_evidence(enriched_text)
                            enrichment_method = "asr_faster_whisper"
                        except VideoIdParseError:
                            record_item_failure(
                                candidate=candidate,
                                raw=selected.raw,
                                reason="youtube_video_id_parse_failed",
                            )
                            continue
                        except ASRError:
                            record_item_failure(
                                candidate=candidate,
                                raw=selected.raw,
                                reason="youtube_asr_failed",
                            )
                            continue
                        except Exception:
                            record_item_failure(
                                candidate=candidate,
                                raw=selected.raw,
                                reason="youtube_asr_failed",
                            )
                            continue

                        if len(enriched_text) < youtube_asr_min_chars:
                            record_item_failure(
                                candidate=candidate,
                                raw=selected.raw,
                                reason="youtube_text_too_short",
                            )
                            continue

                    enriched_at = utc_now_z()
                    enriched_item = EnrichedItem(
                        item_id=candidate.item_id,
                        source_type=candidate.source_type,
                        url=candidate.url,
                        title=candidate.title,
                        published_at=candidate.published_at,
                        enriched_text=enriched_text,
                        evidence_snippets=evidence_snippets,
                        enrichment_method=enrichment_method,
                        enriched_at=enriched_at,
                    )

                    inserted = store.insert_enriched_item(enriched_item, inserted_at=enriched_at)
                    counters["items_selected"] += 1
                    counters["success_count"] += 1
                    if inserted:
                        counters["inserted_db"] += 1
                        out_handle.write(json.dumps(enriched_item.to_dict(), ensure_ascii=True))
                        out_handle.write("\n")
                    else:
                        counters["skipped_already_enriched"] += 1

                    _record_retry_outcome(
                        store=store,
                        candidate=candidate,
                        outcome="success",
                        failure_reason=None,
                        cooldown_policy=cooldown_policy,
                    )
            finally:
                store.close()

        finished_at = to_utc_z(utc_now())
        result = EnrichResult(
            run_id=run_id,
            run_status="completed",
            fatal_error=None,
            started_at=started_at,
            finished_at=finished_at,
            db_path=str(db_path),
            output_path=str(output_file),
            report_path=str(report_file),
            candidates_available_total=counters["candidates_available_total"],
            selected_rows_total=selection_stats["selected_rows_total"],
            invalid_pool_total=selection_stats["invalid_pool_total"],
            eligible_pool_total=selection_stats["eligible_pool_total"],
            eligible_newsletters_total=selection_stats["eligible_newsletters_total"],
            eligible_youtube_total=selection_stats["eligible_youtube_total"],
            cooldown_blocked_total=selection_stats["cooldown_blocked_total"],
            items_selected=counters["items_selected"],
            success_count=counters["success_count"],
            failed_count=counters["failed_count"],
            inserted_db=counters["inserted_db"],
            skipped_already_enriched=counters["skipped_already_enriched"],
            youtube_transcripts_attempted=counters["youtube_transcripts_attempted"],
            youtube_transcripts_succeeded=counters["youtube_transcripts_succeeded"],
            asr_fallbacks_used=counters["asr_fallbacks_used"],
            youtube_asr_attempted=counters["youtube_asr_attempted"],
            youtube_asr_succeeded=counters["youtube_asr_succeeded"],
            youtube_asr_model=youtube_asr_model,
            youtube_asr_device_effective=youtube_asr_device_effective,
            youtube_asr_compute_type=youtube_asr_compute_type,
            youtube_preflight_executed=youtube_preflight_executed,
            full_success_required=full_success_required,
            max_items=max_items,
            max_transcripts=max_transcripts,
            max_asr=max_asr,
            selected_newsletter_count=selection_stats["selected_newsletter_count"],
            selected_youtube_count=selection_stats["selected_youtube_count"],
            selected_invalid_count=selection_stats["selected_invalid_count"],
            selected_unique_sources=selection_stats["selected_unique_sources"],
            newsletter_floor_target=selection_stats["newsletter_floor_target"],
            youtube_floor_target=selection_stats["youtube_floor_target"],
            newsletter_floor_met=selection_stats["newsletter_floor_met"],
            youtube_floor_met=selection_stats["youtube_floor_met"],
            source_diversity_first_pass_applied=selection_stats[
                "source_diversity_first_pass_applied"
            ],
            selected_phase_breakdown=selection_stats["selected_phase_breakdown"],
            fail_breakdown=fail_breakdown,
        )
        payload = result.to_dict()
        _validate_report_invariants(payload)
        _write_json(report_file, payload)

        if counters["failed_count"] > 0:
            LOGGER.warning("stage_4_enrich fail_breakdown=%s", json.dumps(fail_breakdown, ensure_ascii=True))
        LOGGER.info(
            "stage_4_enrich complete run_id=%s candidates_available_total=%s selected_rows_total=%s items_selected=%s success_count=%s failed_count=%s inserted_db=%s skipped_already_enriched=%s eligible_newsletters_total=%s eligible_youtube_total=%s selected_newsletter_count=%s selected_youtube_count=%s selected_invalid_count=%s",
            run_id,
            counters["candidates_available_total"],
            selection_stats["selected_rows_total"],
            counters["items_selected"],
            counters["success_count"],
            counters["failed_count"],
            counters["inserted_db"],
            counters["skipped_already_enriched"],
            selection_stats["eligible_newsletters_total"],
            selection_stats["eligible_youtube_total"],
            selection_stats["selected_newsletter_count"],
            selection_stats["selected_youtube_count"],
            selection_stats["selected_invalid_count"],
        )
        return result
    except Exception as exc:
        fatal_error = _fatal_message(exc)
        finished_at = to_utc_z(utc_now())
        if report_file is not None:
            payload = EnrichResult(
                run_id=run_id,
                run_status="fatal",
                fatal_error=fatal_error,
                started_at=started_at,
                finished_at=finished_at,
                db_path=str(db_path) if db_path is not None else "",
                output_path=str(output_file) if output_file is not None else "",
                report_path=str(report_file),
                candidates_available_total=counters["candidates_available_total"],
                selected_rows_total=selection_stats["selected_rows_total"],
                invalid_pool_total=selection_stats["invalid_pool_total"],
                eligible_pool_total=selection_stats["eligible_pool_total"],
                eligible_newsletters_total=selection_stats["eligible_newsletters_total"],
                eligible_youtube_total=selection_stats["eligible_youtube_total"],
                cooldown_blocked_total=selection_stats["cooldown_blocked_total"],
                items_selected=counters["items_selected"],
                success_count=counters["success_count"],
                failed_count=counters["failed_count"],
                inserted_db=counters["inserted_db"],
                skipped_already_enriched=counters["skipped_already_enriched"],
                youtube_transcripts_attempted=counters["youtube_transcripts_attempted"],
                youtube_transcripts_succeeded=counters["youtube_transcripts_succeeded"],
                asr_fallbacks_used=counters["asr_fallbacks_used"],
                youtube_asr_attempted=counters["youtube_asr_attempted"],
                youtube_asr_succeeded=counters["youtube_asr_succeeded"],
                youtube_asr_model=youtube_asr_model,
                youtube_asr_device_effective=youtube_asr_device_effective,
                youtube_asr_compute_type=youtube_asr_compute_type,
                youtube_preflight_executed=youtube_preflight_executed,
                full_success_required=full_success_required,
                max_items=max_items,
                max_transcripts=max_transcripts,
                max_asr=max_asr,
                selected_newsletter_count=selection_stats["selected_newsletter_count"],
                selected_youtube_count=selection_stats["selected_youtube_count"],
                selected_invalid_count=selection_stats["selected_invalid_count"],
                selected_unique_sources=selection_stats["selected_unique_sources"],
                newsletter_floor_target=selection_stats["newsletter_floor_target"],
                youtube_floor_target=selection_stats["youtube_floor_target"],
                newsletter_floor_met=selection_stats["newsletter_floor_met"],
                youtube_floor_met=selection_stats["youtube_floor_met"],
                source_diversity_first_pass_applied=selection_stats[
                    "source_diversity_first_pass_applied"
                ],
                selected_phase_breakdown=selection_stats["selected_phase_breakdown"],
                fail_breakdown=fail_breakdown,
            ).to_dict()
            _validate_report_invariants(payload)
            try:
                _write_json(report_file, payload)
            except OSError as report_exc:
                raise FatalEnrichError(
                    f"{fatal_error}; failed writing fatal report: {report_exc}"
                ) from report_exc

        raise FatalEnrichError(fatal_error) from exc


def _record_failure(
    counters: dict[str, int], fail_breakdown: dict[str, int], reason: str
) -> None:
    fail_breakdown[reason] += 1
    counters["failed_count"] += 1
    counters["items_selected"] += 1
    LOGGER.error("stage_4_enrich item_failed reason=%s", reason)


def _record_failure_with_retry(
    *,
    store: EnrichStore,
    counters: dict[str, int],
    fail_breakdown: dict[str, int],
    candidate: CandidateRow,
    reason: str,
    cooldown_policy: CooldownPolicyConfig | None,
) -> None:
    _record_failure(counters, fail_breakdown, reason)
    if cooldown_policy is None:
        return
    _record_retry_outcome(
        store=store,
        candidate=candidate,
        outcome="failed",
        failure_reason=reason,
        cooldown_policy=cooldown_policy,
    )


def _record_retry_outcome(
    *,
    store: EnrichStore,
    candidate: CandidateRow,
    outcome: str,
    failure_reason: str | None,
    cooldown_policy: CooldownPolicyConfig,
) -> None:
    existing = store.get_retry_state(item_id=candidate.item_id)
    attempts_before = int(existing["attempts_total"]) if existing is not None else 0
    consecutive_before = int(existing["consecutive_failures"]) if existing is not None else 0
    attempts_total = attempts_before + 1

    now_dt = utc_now()
    now_z = to_utc_z(now_dt)

    if outcome == "success":
        consecutive_failures = 0
        last_outcome = "success"
        last_fail_reason = None
        next_eligible_at = None
    else:
        consecutive_failures = consecutive_before + 1
        last_outcome = "failed"
        last_fail_reason = failure_reason
        next_eligible_at = None
        if (
            cooldown_policy.enabled
            and failure_reason is not None
            and failure_reason in cooldown_policy.reasons
            and consecutive_failures >= cooldown_policy.after_consecutive_failures
            and cooldown_policy.skip_for_hours > 0
        ):
            next_eligible_at = to_utc_z(now_dt + timedelta(hours=cooldown_policy.skip_for_hours))

    store.upsert_retry_state(
        item_id=candidate.item_id,
        source_type=candidate.source_type,
        source_id=candidate.source_id,
        attempts_total=attempts_total,
        consecutive_failures=consecutive_failures,
        last_outcome=last_outcome,
        last_fail_reason=last_fail_reason,
        last_attempt_at=now_z,
        next_eligible_at=next_eligible_at,
        updated_at=now_z,
    )


def _new_counters() -> dict[str, int]:
    return {
        "candidates_available_total": 0,
        "items_selected": 0,
        "success_count": 0,
        "failed_count": 0,
        "inserted_db": 0,
        "skipped_already_enriched": 0,
        "youtube_transcripts_attempted": 0,
        "youtube_transcripts_succeeded": 0,
        "asr_fallbacks_used": 0,
        "youtube_asr_attempted": 0,
        "youtube_asr_succeeded": 0,
    }


def _new_selection_stats() -> dict[str, Any]:
    return {
        "selected_rows_total": 0,
        "invalid_pool_total": 0,
        "eligible_pool_total": 0,
        "eligible_newsletters_total": 0,
        "eligible_youtube_total": 0,
        "cooldown_blocked_total": 0,
        "selected_newsletter_count": 0,
        "selected_youtube_count": 0,
        "selected_invalid_count": 0,
        "selected_unique_sources": 0,
        "newsletter_floor_target": 0,
        "youtube_floor_target": 0,
        "newsletter_floor_met": False,
        "youtube_floor_met": False,
        "source_diversity_first_pass_applied": False,
        "selected_phase_breakdown": _new_phase_breakdown(),
    }


def _new_phase_breakdown() -> dict[str, int]:
    return {phase: 0 for phase in PHASE_KEYS}


def _new_fail_breakdown() -> dict[str, int]:
    return {key: 0 for key in FAIL_BREAKDOWN_KEYS}


def _apply_selection_stats(target: dict[str, Any], selection: _SelectionPlan) -> None:
    target["selected_rows_total"] = selection.selected_rows_total
    target["invalid_pool_total"] = selection.invalid_pool_total
    target["eligible_pool_total"] = selection.eligible_pool_total
    target["eligible_newsletters_total"] = selection.eligible_newsletters_total
    target["eligible_youtube_total"] = selection.eligible_youtube_total
    target["cooldown_blocked_total"] = selection.cooldown_blocked_total
    target["selected_newsletter_count"] = selection.selected_newsletter_count
    target["selected_youtube_count"] = selection.selected_youtube_count
    target["selected_invalid_count"] = selection.selected_invalid_count
    target["selected_unique_sources"] = selection.selected_unique_sources
    target["newsletter_floor_target"] = selection.newsletter_floor_target
    target["youtube_floor_target"] = selection.youtube_floor_target
    target["newsletter_floor_met"] = selection.newsletter_floor_met
    target["youtube_floor_met"] = selection.youtube_floor_met
    target["source_diversity_first_pass_applied"] = selection.source_diversity_first_pass_applied
    target["selected_phase_breakdown"] = selection.selected_phase_breakdown


def _build_selection_plan(
    *,
    raw_rows: list[dict[str, Any]],
    max_items: int,
    started_dt: datetime,
    selection_policy: SelectionPolicyConfig,
    cooldown_policy: CooldownPolicyConfig,
    include_invalid_in_selection: bool,
    store: EnrichStore,
) -> _SelectionPlan:
    newsletter_floor_target = min(selection_policy.min_newsletters_per_run, max_items)
    youtube_floor_target = min(
        selection_policy.min_youtube_per_run,
        max_items - newsletter_floor_target,
    )

    valid_pool: list[_SelectionRow] = []
    invalid_pool: list[_SelectionRow] = []
    for index, raw in enumerate(raw_rows):
        try:
            candidate = parse_candidate_row(raw)
        except Exception:
            invalid_pool.append(_SelectionRow(raw=raw, candidate=None, base_index=index))
            continue
        valid_pool.append(_SelectionRow(raw=raw, candidate=candidate, base_index=index))

    eligible_pool: list[_SelectionRow] = []
    cooldown_blocked_total = 0
    if cooldown_policy.enabled:
        retry_states = store.get_retry_states(
            item_ids=[row.candidate.item_id for row in valid_pool if row.candidate is not None]
        )
        for row in valid_pool:
            candidate = row.candidate
            if candidate is None:
                continue
            retry_state = retry_states.get(candidate.item_id)
            if _is_cooldown_blocked(started_dt=started_dt, retry_state=retry_state):
                cooldown_blocked_total += 1
                continue
            eligible_pool.append(row)
    else:
        eligible_pool = list(valid_pool)

    phase_breakdown = _new_phase_breakdown()
    selected_rows: list[_SelectionRow] = []
    selected_item_ids: set[str] = set()
    selected_per_source: dict[tuple[str, str], int] = {}

    def can_take_valid(row: _SelectionRow) -> bool:
        candidate = row.candidate
        if candidate is None:
            return False
        if len(selected_rows) >= max_items:
            return False
        if candidate.item_id in selected_item_ids:
            return False
        return selected_per_source.get(candidate.source_key, 0) < selection_policy.max_items_per_source

    def take_valid(row: _SelectionRow, phase: str) -> bool:
        if not can_take_valid(row):
            return False
        candidate = row.candidate
        if candidate is None:
            return False
        selected_rows.append(row)
        selected_item_ids.add(candidate.item_id)
        selected_per_source[candidate.source_key] = selected_per_source.get(candidate.source_key, 0) + 1
        phase_breakdown[phase] += 1
        return True

    def take_invalid(row: _SelectionRow, phase: str) -> bool:
        if len(selected_rows) >= max_items:
            return False
        selected_rows.append(row)
        phase_breakdown[phase] += 1
        return True

    if max_items > 0:
        floor_newsletter_selected = 0
        for row in eligible_pool:
            if floor_newsletter_selected >= newsletter_floor_target:
                break
            candidate = row.candidate
            if candidate is None or candidate.source_type != "newsletter":
                continue
            if take_valid(row, "floor_newsletter"):
                floor_newsletter_selected += 1

        floor_youtube_selected = 0
        for row in eligible_pool:
            if floor_youtube_selected >= youtube_floor_target:
                break
            candidate = row.candidate
            if candidate is None or candidate.source_type != "youtube":
                continue
            if take_valid(row, "floor_youtube"):
                floor_youtube_selected += 1

        if selection_policy.source_diversity_first_pass:
            queues_by_source: dict[tuple[str, str], list[_SelectionRow]] = {}
            for row in eligible_pool:
                candidate = row.candidate
                if candidate is None:
                    continue
                queues_by_source.setdefault(candidate.source_key, []).append(row)

            while len(selected_rows) < max_items:
                heads: list[_SelectionRow] = []
                for source_key, rows_for_source in queues_by_source.items():
                    if selected_per_source.get(source_key, 0) != 0:
                        continue
                    for row in rows_for_source:
                        candidate = row.candidate
                        if candidate is None:
                            continue
                        if candidate.item_id in selected_item_ids:
                            continue
                        heads.append(row)
                        break

                if not heads:
                    break
                next_row = min(heads, key=lambda row: row.base_index)
                if not take_valid(next_row, "source_diversity"):
                    break

        eligible_by_index = {row.base_index: row for row in eligible_pool}
        invalid_by_index = {row.base_index: row for row in invalid_pool}
        for index in range(len(raw_rows)):
            if len(selected_rows) >= max_items:
                break
            eligible_row = eligible_by_index.get(index)
            if eligible_row is not None:
                take_valid(eligible_row, "remainder")
                continue
            invalid_row = invalid_by_index.get(index)
            if include_invalid_in_selection and invalid_row is not None:
                take_invalid(invalid_row, "remainder")

    selected_newsletter_count = sum(
        1
        for row in selected_rows
        if row.candidate is not None and row.candidate.source_type == "newsletter"
    )
    selected_youtube_count = sum(
        1
        for row in selected_rows
        if row.candidate is not None and row.candidate.source_type == "youtube"
    )
    selected_invalid_count = sum(1 for row in selected_rows if row.candidate is None)
    selected_unique_sources = len(
        {
            row.candidate.source_key
            for row in selected_rows
            if row.candidate is not None
        }
    )

    eligible_newsletters_total = sum(
        1
        for row in eligible_pool
        if row.candidate is not None and row.candidate.source_type == "newsletter"
    )
    eligible_youtube_total = sum(
        1
        for row in eligible_pool
        if row.candidate is not None and row.candidate.source_type == "youtube"
    )

    return _SelectionPlan(
        selected_rows=selected_rows,
        selected_rows_total=len(selected_rows),
        invalid_pool_total=len(invalid_pool),
        eligible_pool_total=len(eligible_pool),
        eligible_newsletters_total=eligible_newsletters_total,
        eligible_youtube_total=eligible_youtube_total,
        cooldown_blocked_total=cooldown_blocked_total,
        selected_newsletter_count=selected_newsletter_count,
        selected_youtube_count=selected_youtube_count,
        selected_invalid_count=selected_invalid_count,
        selected_unique_sources=selected_unique_sources,
        newsletter_floor_target=newsletter_floor_target,
        youtube_floor_target=youtube_floor_target,
        newsletter_floor_met=selected_newsletter_count >= newsletter_floor_target,
        youtube_floor_met=selected_youtube_count >= youtube_floor_target,
        source_diversity_first_pass_applied=(
            selection_policy.source_diversity_first_pass and phase_breakdown["source_diversity"] > 0
        ),
        selected_phase_breakdown=phase_breakdown,
    )


def _is_cooldown_blocked(*, started_dt: datetime, retry_state: dict[str, Any] | None) -> bool:
    if retry_state is None:
        return False
    next_eligible_at_raw = retry_state.get("next_eligible_at")
    next_eligible_at = _parse_utc_z(next_eligible_at_raw)
    if next_eligible_at is None:
        return False
    return started_dt < next_eligible_at


def _parse_utc_z(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, UTC_Z_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _fatal_message(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return message
    return exc.__class__.__name__


def _validate_override(value: int | None, field_name: str) -> int | None:
    if value is None:
        return None
    if not is_non_bool_int(value) or int(value) < 0:
        raise ConfigError(f"{field_name} must be a non-boolean integer >= 0")
    return int(value)


def _extract_item_id(raw: dict[str, Any]) -> str:
    value = raw.get("item_id")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "<unknown>"


def _prepare_youtube_asr_runtime(
    *,
    youtube_config: YouTubeEnrichmentConfig,
) -> ASRRuntime:
    _ensure_youtube_python_dependencies()
    expected_versions = _load_expected_package_versions(
        requirements_path=Path(__file__).resolve().parents[2] / "requirements.txt",
        required_packages=("faster-whisper", "ctranslate2"),
    )
    faster_whisper_runtime = _resolve_runtime_package_version("faster-whisper")
    ctranslate2_runtime = _resolve_runtime_package_version("ctranslate2")

    expected_fw = expected_versions["faster-whisper"]
    expected_ct2 = expected_versions["ctranslate2"]
    if faster_whisper_runtime != expected_fw:
        raise ConfigError(
            "faster-whisper runtime version mismatch: "
            f"expected={expected_fw} runtime={faster_whisper_runtime}"
        )
    if ctranslate2_runtime != expected_ct2:
        raise ConfigError(
            "ctranslate2 runtime version mismatch: "
            f"expected={expected_ct2} runtime={ctranslate2_runtime}"
        )

    ytdlp_bin = resolve_executable(
        env_name="YOUTUBE_YTDLP_BIN",
        candidates=("yt-dlp", "yt-dlp.exe"),
    )
    ffmpeg_bin = resolve_executable(
        env_name="YOUTUBE_FFMPEG_BIN",
        candidates=("ffmpeg", "ffmpeg.exe"),
    )
    if ytdlp_bin is None:
        raise ConfigError("missing required executable: yt-dlp")
    if ffmpeg_bin is None:
        raise ConfigError("missing required executable: ffmpeg")

    ytdlp_version = _resolve_executable_version(
        command=[ytdlp_bin, "--version"],
        executable_name="yt-dlp",
    )
    ffmpeg_version = _resolve_executable_version(
        command=[ffmpeg_bin, "-version"],
        executable_name="ffmpeg",
    )

    LOGGER.info(
        "stage_4_enrich youtube_preflight versions faster-whisper=%s match=%s ctranslate2=%s match=%s yt-dlp=%s ffmpeg=%s",
        faster_whisper_runtime,
        True,
        ctranslate2_runtime,
        True,
        ytdlp_version,
        ffmpeg_version,
    )

    decode = ASRDecodeConfig(
        language=youtube_config.asr.language,
        beam_size=youtube_config.asr.beam_size,
        temperature=youtube_config.asr.temperature,
        condition_on_previous_text=youtube_config.asr.condition_on_previous_text,
        vad_filter=youtube_config.asr.vad_filter,
    )
    audio = ASRAudioConfig(
        format_selector=youtube_config.audio.format_selector,
        extract_format=youtube_config.audio.extract_format,
        download_timeout_s=youtube_config.audio.download_timeout_s,
        download_retries=youtube_config.audio.download_retries,
        retry_backoff_s=youtube_config.audio.retry_backoff_s,
    )
    try:
        return initialize_asr_runtime(
            model_name=youtube_config.asr.model,
            device=youtube_config.asr.device,
            compute_type=youtube_config.asr.compute_type,
            allow_cpu_fallback=youtube_config.asr.allow_cpu_fallback,
            ytdlp_bin=ytdlp_bin,
            ffmpeg_bin=ffmpeg_bin,
            decode=decode,
            audio=audio,
            max_audio_seconds=youtube_config.asr.max_audio_seconds,
        )
    except ASRStartupError as exc:
        raise ConfigError(str(exc)) from exc


def _ensure_youtube_python_dependencies() -> None:
    for module_name in ("faster_whisper", "ctranslate2"):
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            raise ConfigError(f"missing required dependency: {module_name}") from exc


def _resolve_runtime_package_version(distribution_name: str) -> str:
    try:
        version = importlib_metadata.version(distribution_name)
    except Exception as exc:
        raise ConfigError(f"failed to resolve runtime package version: {distribution_name}") from exc
    normalized = str(version).strip()
    if not normalized:
        raise ConfigError(f"failed to resolve runtime package version: {distribution_name}")
    return normalized


def _resolve_executable_version(*, command: list[str], executable_name: str) -> str:
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception as exc:
        raise ConfigError(f"failed to resolve {executable_name} version") from exc

    stdout_lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    stderr_lines = [line.strip() for line in result.stderr.splitlines() if line.strip()]
    lines = stdout_lines if stdout_lines else stderr_lines
    if executable_name == "ffmpeg":
        if not lines:
            raise ConfigError(f"failed to resolve {executable_name} version")
        return lines[0]
    if not lines:
        raise ConfigError(f"failed to resolve {executable_name} version")
    return lines[-1]


def _load_expected_package_versions(
    *,
    requirements_path: Path,
    required_packages: tuple[str, ...],
) -> dict[str, str]:
    required_by_canonical = {
        _canonicalize_package_name(name): name for name in required_packages
    }
    resolved: dict[str, str] = {}

    try:
        lines = requirements_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ConfigError(f"failed to read requirements file: {requirements_path}") from exc

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = REQUIREMENTS_PIN_RE.fullmatch(line)
        if match is None:
            maybe_name = line.split("=", 1)[0].strip().split("[", 1)[0]
            canonical_maybe = _canonicalize_package_name(maybe_name)
            if canonical_maybe in required_by_canonical:
                original_name = required_by_canonical[canonical_maybe]
                raise ConfigError(
                    f"{original_name} must be exact-pinned with == in requirements.txt"
                )
            continue

        canonical = _canonicalize_package_name(match.group("name"))
        if canonical not in required_by_canonical:
            continue
        package_name = required_by_canonical[canonical]
        resolved[package_name] = match.group("version").strip()

    for package_name in required_packages:
        if package_name not in resolved:
            raise ConfigError(
                f"missing exact pin in requirements.txt for package: {package_name}"
            )
    return resolved


def _canonicalize_package_name(name: str) -> str:
    return name.strip().lower().replace("_", "-")


def _ensure_required_dependencies() -> None:
    for module_name in ("trafilatura",):
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            raise ConfigError(f"missing required dependency: {module_name}") from exc


def _load_pipeline_config(path: str | Path) -> PipelineConfig:
    raw = _load_yaml(path)

    paths = raw.get("paths")
    if not isinstance(paths, dict):
        raise ConfigError("paths must be a mapping")
    caps = raw.get("caps")
    if not isinstance(caps, dict):
        raise ConfigError("caps must be a mapping")
    http = raw.get("http")
    if not isinstance(http, dict):
        raise ConfigError("http must be a mapping")
    retries = http.get("retries")
    if not isinstance(retries, dict):
        raise ConfigError("http.retries must be a mapping")

    stage_4_enrich_raw = raw.get("stage_4_enrich")
    if not isinstance(stage_4_enrich_raw, dict):
        raise ConfigError("stage_4_enrich must be a mapping")

    sqlite_db = _parse_non_empty_string(paths.get("sqlite_db"), "paths.sqlite_db")
    outputs_dir = _parse_non_empty_string(paths.get("outputs_dir"), "paths.outputs_dir")

    max_transcripts_per_run = _parse_non_negative_int(
        caps.get("max_transcripts_per_run"), "caps.max_transcripts_per_run"
    )
    max_asr_fallbacks_per_run = _parse_non_negative_int(
        caps.get("max_asr_fallbacks_per_run"), "caps.max_asr_fallbacks_per_run"
    )

    user_agent = _parse_non_empty_string(http.get("user_agent"), "http.user_agent")
    connect_timeout_s = _parse_positive_int(http.get("connect_timeout_s"), "http.connect_timeout_s")
    read_timeout_s = _parse_positive_int(http.get("read_timeout_s"), "http.read_timeout_s")
    max_response_mb = _parse_positive_int(http.get("max_response_mb"), "http.max_response_mb")
    max_attempts = _parse_positive_int(retries.get("max_attempts"), "http.retries.max_attempts")

    max_items_default = _parse_non_negative_int(
        stage_4_enrich_raw.get("max_items_default"),
        "stage_4_enrich.max_items_default",
    )
    selection_policy = _parse_selection_policy(
        stage_4_enrich_raw.get("selection_policy"),
        stage_4_max_items_default=max_items_default,
    )
    cooldown_policy = _parse_cooldown_policy(stage_4_enrich_raw.get("cooldown_policy"))
    youtube_enrichment = _parse_youtube_enrichment(stage_4_enrich_raw.get("youtube_enrichment"))

    if selection_policy.min_newsletters_per_run + selection_policy.min_youtube_per_run > max_items_default:
        raise ConfigError(
            "stage_4_enrich.selection_policy.min_newsletters_per_run + "
            "stage_4_enrich.selection_policy.min_youtube_per_run must be <= "
            "stage_4_enrich.max_items_default"
        )
    if max_items_default > 0 and selection_policy.max_items_per_source > max_items_default:
        raise ConfigError(
            "stage_4_enrich.selection_policy.max_items_per_source must be <= "
            "stage_4_enrich.max_items_default when max_items_default > 0"
        )

    return PipelineConfig(
        paths=PathsConfig(sqlite_db=sqlite_db, outputs_dir=outputs_dir),
        caps=CapsConfig(
            max_transcripts_per_run=max_transcripts_per_run,
            max_asr_fallbacks_per_run=max_asr_fallbacks_per_run,
        ),
        http=HttpConfig(
            user_agent=user_agent,
            connect_timeout_s=connect_timeout_s,
            read_timeout_s=read_timeout_s,
            max_response_mb=max_response_mb,
            retries=HttpRetriesConfig(max_attempts=max_attempts),
        ),
        stage_4_enrich=Stage4EnrichConfig(
            max_items_default=max_items_default,
            selection_policy=selection_policy,
            cooldown_policy=cooldown_policy,
            youtube_enrichment=youtube_enrichment,
        ),
    )


def _parse_selection_policy(
    value: Any,
    *,
    stage_4_max_items_default: int,
) -> SelectionPolicyConfig:
    if not isinstance(value, dict):
        raise ConfigError("stage_4_enrich.selection_policy must be a mapping")

    min_newsletters_per_run = _parse_non_negative_int(
        value.get("min_newsletters_per_run"),
        "stage_4_enrich.selection_policy.min_newsletters_per_run",
    )
    min_youtube_per_run = _parse_non_negative_int(
        value.get("min_youtube_per_run"),
        "stage_4_enrich.selection_policy.min_youtube_per_run",
    )
    max_items_per_source = _parse_positive_int(
        value.get("max_items_per_source"),
        "stage_4_enrich.selection_policy.max_items_per_source",
    )
    source_diversity_first_pass = value.get("source_diversity_first_pass")
    if not isinstance(source_diversity_first_pass, bool):
        raise ConfigError("stage_4_enrich.selection_policy.source_diversity_first_pass must be a boolean")

    if min_newsletters_per_run + min_youtube_per_run > stage_4_max_items_default:
        raise ConfigError(
            "stage_4_enrich.selection_policy.min_newsletters_per_run + "
            "stage_4_enrich.selection_policy.min_youtube_per_run must be <= "
            "stage_4_enrich.max_items_default"
        )

    return SelectionPolicyConfig(
        min_newsletters_per_run=min_newsletters_per_run,
        min_youtube_per_run=min_youtube_per_run,
        max_items_per_source=max_items_per_source,
        source_diversity_first_pass=source_diversity_first_pass,
    )


def _parse_cooldown_policy(value: Any) -> CooldownPolicyConfig:
    if not isinstance(value, dict):
        raise ConfigError("stage_4_enrich.cooldown_policy must be a mapping")

    enabled = value.get("enabled")
    if not isinstance(enabled, bool):
        raise ConfigError("stage_4_enrich.cooldown_policy.enabled must be a boolean")

    after_consecutive_failures = _parse_positive_int(
        value.get("after_consecutive_failures"),
        "stage_4_enrich.cooldown_policy.after_consecutive_failures",
    )
    skip_for_hours = _parse_non_negative_int(
        value.get("skip_for_hours"),
        "stage_4_enrich.cooldown_policy.skip_for_hours",
    )

    reasons_raw = value.get("reasons")
    if not isinstance(reasons_raw, list) or not reasons_raw:
        raise ConfigError("stage_4_enrich.cooldown_policy.reasons must be a non-empty list of strings")

    reasons: list[str] = []
    seen: set[str] = set()
    for reason in reasons_raw:
        if not isinstance(reason, str):
            raise ConfigError("stage_4_enrich.cooldown_policy.reasons entries must be strings")
        normalized = reason.strip()
        if not normalized:
            raise ConfigError(
                "stage_4_enrich.cooldown_policy.reasons entries must be non-empty after strip()"
            )
        if normalized in seen:
            raise ConfigError(
                "stage_4_enrich.cooldown_policy.reasons entries must be unique after strip()"
            )
        seen.add(normalized)
        if normalized == "invalid_candidate_row":
            raise ConfigError("stage_4_enrich.cooldown_policy.reasons must not include invalid_candidate_row")
        if normalized not in COOLDOWN_REASON_ALLOWED:
            raise ConfigError(
                "stage_4_enrich.cooldown_policy.reasons contains unsupported reason: "
                f"{normalized}"
            )
        reasons.append(normalized)

    return CooldownPolicyConfig(
        enabled=enabled,
        after_consecutive_failures=after_consecutive_failures,
        skip_for_hours=skip_for_hours,
        reasons=frozenset(reasons),
    )


def _parse_youtube_enrichment(value: Any) -> YouTubeEnrichmentConfig:
    if not isinstance(value, dict):
        raise ConfigError("stage_4_enrich.youtube_enrichment must be a mapping")

    mode = _parse_non_empty_string(
        value.get("mode"),
        "stage_4_enrich.youtube_enrichment.mode",
    )
    if mode != "asr_only":
        raise ConfigError("stage_4_enrich.youtube_enrichment.mode must be 'asr_only'")

    require_full_success = value.get("require_full_success")
    if not isinstance(require_full_success, bool):
        raise ConfigError(
            "stage_4_enrich.youtube_enrichment.require_full_success must be a boolean"
        )

    audio = _parse_youtube_audio_config(value.get("audio"), mode=mode)
    asr = _parse_youtube_asr_config(value.get("asr"))

    return YouTubeEnrichmentConfig(
        mode=mode,
        require_full_success=require_full_success,
        audio=audio,
        asr=asr,
    )


def _parse_youtube_audio_config(value: Any, *, mode: str) -> YouTubeAudioConfig:
    if not isinstance(value, dict):
        raise ConfigError("stage_4_enrich.youtube_enrichment.audio must be a mapping")

    format_selector = _parse_non_empty_string(
        value.get("format_selector"),
        "stage_4_enrich.youtube_enrichment.audio.format_selector",
    )
    if mode == "asr_only" and format_selector != "bestaudio":
        raise ConfigError(
            "stage_4_enrich.youtube_enrichment.audio.format_selector must be 'bestaudio' in asr_only mode"
        )

    extract_format = _parse_non_empty_string(
        value.get("extract_format"),
        "stage_4_enrich.youtube_enrichment.audio.extract_format",
    )
    download_timeout_s = _parse_int_min(
        value.get("download_timeout_s"),
        "stage_4_enrich.youtube_enrichment.audio.download_timeout_s",
        minimum=30,
    )
    download_retries = _parse_non_negative_int(
        value.get("download_retries"),
        "stage_4_enrich.youtube_enrichment.audio.download_retries",
    )
    retry_backoff_s = _parse_number_gt_zero(
        value.get("retry_backoff_s"),
        "stage_4_enrich.youtube_enrichment.audio.retry_backoff_s",
    )

    return YouTubeAudioConfig(
        format_selector=format_selector,
        extract_format=extract_format,
        download_timeout_s=download_timeout_s,
        download_retries=download_retries,
        retry_backoff_s=retry_backoff_s,
    )


def _parse_youtube_asr_config(value: Any) -> YouTubeASRConfig:
    if not isinstance(value, dict):
        raise ConfigError("stage_4_enrich.youtube_enrichment.asr must be a mapping")

    model = _parse_non_empty_string(
        value.get("model"),
        "stage_4_enrich.youtube_enrichment.asr.model",
    )
    device = _parse_non_empty_string(
        value.get("device"),
        "stage_4_enrich.youtube_enrichment.asr.device",
    )
    if device not in {"cuda", "cpu"}:
        raise ConfigError("stage_4_enrich.youtube_enrichment.asr.device must be one of: cuda, cpu")

    compute_type = _parse_non_empty_string(
        value.get("compute_type"),
        "stage_4_enrich.youtube_enrichment.asr.compute_type",
    )
    language = _parse_non_empty_string(
        value.get("language"),
        "stage_4_enrich.youtube_enrichment.asr.language",
    )
    beam_size = _parse_int_min(
        value.get("beam_size"),
        "stage_4_enrich.youtube_enrichment.asr.beam_size",
        minimum=1,
    )
    temperature = _parse_non_negative_number(
        value.get("temperature"),
        "stage_4_enrich.youtube_enrichment.asr.temperature",
    )

    condition_on_previous_text = value.get("condition_on_previous_text")
    if not isinstance(condition_on_previous_text, bool):
        raise ConfigError(
            "stage_4_enrich.youtube_enrichment.asr.condition_on_previous_text must be a boolean"
        )

    vad_filter = value.get("vad_filter")
    if not isinstance(vad_filter, bool):
        raise ConfigError("stage_4_enrich.youtube_enrichment.asr.vad_filter must be a boolean")

    max_audio_seconds = _parse_int_min(
        value.get("max_audio_seconds"),
        "stage_4_enrich.youtube_enrichment.asr.max_audio_seconds",
        minimum=60,
    )
    min_chars = _parse_int_min(
        value.get("min_chars"),
        "stage_4_enrich.youtube_enrichment.asr.min_chars",
        minimum=1,
    )

    allow_cpu_fallback = value.get("allow_cpu_fallback")
    if not isinstance(allow_cpu_fallback, bool):
        raise ConfigError(
            "stage_4_enrich.youtube_enrichment.asr.allow_cpu_fallback must be a boolean"
        )

    return YouTubeASRConfig(
        model=model,
        device=device,
        compute_type=compute_type,
        language=language,
        beam_size=beam_size,
        temperature=temperature,
        condition_on_previous_text=condition_on_previous_text,
        vad_filter=vad_filter,
        max_audio_seconds=max_audio_seconds,
        min_chars=min_chars,
        allow_cpu_fallback=allow_cpu_fallback,
    )


def _resolve_output_path(*, outputs_dir: Path, date: datetime) -> Path:
    return outputs_dir / f"enriched_items_{date.strftime('%Y-%m-%d')}.jsonl"


def _resolve_report_path(*, outputs_dir: Path, date: datetime) -> Path:
    return outputs_dir / f"stage_4_report_{date.strftime('%Y-%m-%d')}.json"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


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


def _parse_int_min(value: Any, field_name: str, *, minimum: int) -> int:
    if not is_non_bool_int(value) or int(value) < minimum:
        raise ConfigError(f"{field_name} must be a non-boolean integer >= {minimum}")
    return int(value)


def _is_non_bool_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    return isinstance(value, (int, float))


def _parse_non_negative_number(value: Any, field_name: str) -> float:
    if not _is_non_bool_number(value) or float(value) < 0:
        raise ConfigError(f"{field_name} must be a number >= 0")
    return float(value)


def _parse_number_gt_zero(value: Any, field_name: str) -> float:
    if not _is_non_bool_number(value) or float(value) <= 0:
        raise ConfigError(f"{field_name} must be a number > 0")
    return float(value)


def _validate_report_invariants(payload: dict[str, Any]) -> None:
    run_status = payload.get("run_status")
    fatal_error = payload.get("fatal_error")
    if run_status not in {"completed", "fatal"}:
        raise FatalEnrichError("report invariant failed: run_status")
    if run_status == "completed" and fatal_error is not None:
        raise FatalEnrichError("report invariant failed: completed run must have fatal_error null")
    if run_status == "fatal" and (not isinstance(fatal_error, str) or not fatal_error.strip()):
        raise FatalEnrichError("report invariant failed: fatal run must have non-empty fatal_error")

    fail_breakdown = payload.get("fail_breakdown")
    if not isinstance(fail_breakdown, dict):
        raise FatalEnrichError("report invariant failed: fail_breakdown must be a map")
    if set(fail_breakdown.keys()) != set(FAIL_BREAKDOWN_KEYS):
        raise FatalEnrichError("report invariant failed: fail_breakdown keys mismatch")

    selected_phase_breakdown = payload.get("selected_phase_breakdown")
    if not isinstance(selected_phase_breakdown, dict):
        raise FatalEnrichError("report invariant failed: selected_phase_breakdown must be a map")
    if set(selected_phase_breakdown.keys()) != set(PHASE_KEYS):
        raise FatalEnrichError("report invariant failed: selected_phase_breakdown keys mismatch")

    int_fields = (
        "candidates_available_total",
        "selected_rows_total",
        "invalid_pool_total",
        "eligible_pool_total",
        "eligible_newsletters_total",
        "eligible_youtube_total",
        "cooldown_blocked_total",
        "items_selected",
        "success_count",
        "failed_count",
        "inserted_db",
        "skipped_already_enriched",
        "youtube_transcripts_attempted",
        "youtube_transcripts_succeeded",
        "asr_fallbacks_used",
        "youtube_asr_attempted",
        "youtube_asr_succeeded",
        "max_items",
        "max_transcripts",
        "max_asr",
        "selected_newsletter_count",
        "selected_youtube_count",
        "selected_invalid_count",
        "selected_unique_sources",
        "newsletter_floor_target",
        "youtube_floor_target",
    )
    for field in int_fields:
        value = payload.get(field)
        if not is_non_bool_int(value) or int(value) < 0:
            raise FatalEnrichError(f"report invariant failed: {field} must be an integer >= 0")

    for key in FAIL_BREAKDOWN_KEYS:
        value = fail_breakdown.get(key)
        if not is_non_bool_int(value) or int(value) < 0:
            raise FatalEnrichError(f"report invariant failed: fail_breakdown[{key}] invalid")

    for key in PHASE_KEYS:
        value = selected_phase_breakdown.get(key)
        if not is_non_bool_int(value) or int(value) < 0:
            raise FatalEnrichError(
                f"report invariant failed: selected_phase_breakdown[{key}] invalid"
            )

    bool_fields = (
        "newsletter_floor_met",
        "youtube_floor_met",
        "source_diversity_first_pass_applied",
        "youtube_preflight_executed",
        "full_success_required",
    )
    for field in bool_fields:
        if not isinstance(payload.get(field), bool):
            raise FatalEnrichError(f"report invariant failed: {field} must be a boolean")

    string_fields = (
        "youtube_asr_model",
        "youtube_asr_device_effective",
        "youtube_asr_compute_type",
    )
    for field in string_fields:
        if not isinstance(payload.get(field), str):
            raise FatalEnrichError(f"report invariant failed: {field} must be a string")

    items_selected = int(payload["items_selected"])
    selected_rows_total = int(payload["selected_rows_total"])
    success_count = int(payload["success_count"])
    failed_count = int(payload["failed_count"])
    inserted_db = int(payload["inserted_db"])
    skipped = int(payload["skipped_already_enriched"])
    candidates_available_total = int(payload["candidates_available_total"])
    max_items = int(payload["max_items"])
    youtube_transcripts_attempted = int(payload["youtube_transcripts_attempted"])
    youtube_transcripts_succeeded = int(payload["youtube_transcripts_succeeded"])
    youtube_asr_attempted = int(payload["youtube_asr_attempted"])
    youtube_asr_succeeded = int(payload["youtube_asr_succeeded"])
    max_transcripts = int(payload["max_transcripts"])
    asr_fallbacks_used = int(payload["asr_fallbacks_used"])
    max_asr = int(payload["max_asr"])
    invalid_pool_total = int(payload["invalid_pool_total"])
    eligible_pool_total = int(payload["eligible_pool_total"])
    eligible_newsletters_total = int(payload["eligible_newsletters_total"])
    eligible_youtube_total = int(payload["eligible_youtube_total"])
    cooldown_blocked_total = int(payload["cooldown_blocked_total"])
    selected_newsletter_count = int(payload["selected_newsletter_count"])
    selected_youtube_count = int(payload["selected_youtube_count"])
    selected_invalid_count = int(payload["selected_invalid_count"])
    newsletter_floor_target = int(payload["newsletter_floor_target"])
    youtube_floor_target = int(payload["youtube_floor_target"])
    newsletter_floor_met = payload["newsletter_floor_met"]
    youtube_floor_met = payload["youtube_floor_met"]
    youtube_preflight_executed = payload["youtube_preflight_executed"]
    full_success_required = payload["full_success_required"]

    fail_total = sum(int(v) for v in fail_breakdown.values())
    if items_selected != success_count + failed_count:
        raise FatalEnrichError("report invariant failed: items_selected mismatch")
    if success_count != inserted_db + skipped:
        raise FatalEnrichError("report invariant failed: success_count mismatch")
    if failed_count != fail_total:
        raise FatalEnrichError("report invariant failed: failed_count mismatch")
    if items_selected > max_items:
        raise FatalEnrichError("report invariant failed: items_selected > max_items")
    if items_selected > candidates_available_total:
        raise FatalEnrichError("report invariant failed: items_selected > candidates_available_total")
    if items_selected > selected_rows_total:
        raise FatalEnrichError("report invariant failed: items_selected > selected_rows_total")
    if run_status == "completed" and items_selected != selected_rows_total:
        raise FatalEnrichError("report invariant failed: completed run items_selected mismatch")
    if youtube_transcripts_succeeded > youtube_transcripts_attempted:
        raise FatalEnrichError(
            "report invariant failed: youtube_transcripts_succeeded > youtube_transcripts_attempted"
        )
    if youtube_transcripts_attempted > max_transcripts:
        raise FatalEnrichError("report invariant failed: youtube_transcripts_attempted > max_transcripts")
    if asr_fallbacks_used > max_asr:
        raise FatalEnrichError("report invariant failed: asr_fallbacks_used > max_asr")
    if youtube_asr_succeeded > youtube_asr_attempted:
        raise FatalEnrichError(
            "report invariant failed: youtube_asr_succeeded > youtube_asr_attempted"
        )
    if fail_breakdown["youtube_text_too_short"] > youtube_asr_succeeded:
        raise FatalEnrichError(
            "report invariant failed: youtube_text_too_short > youtube_asr_succeeded"
        )

    if selected_rows_total > max_items:
        raise FatalEnrichError("report invariant failed: selected_rows_total > max_items")
    if selected_rows_total > (eligible_pool_total + invalid_pool_total):
        raise FatalEnrichError(
            "report invariant failed: selected_rows_total > eligible_pool_total + invalid_pool_total"
        )
    if selected_newsletter_count + selected_youtube_count + selected_invalid_count != selected_rows_total:
        raise FatalEnrichError(
            "report invariant failed: selected type counts do not sum to selected_rows_total"
        )
    if sum(int(selected_phase_breakdown[k]) for k in PHASE_KEYS) != selected_rows_total:
        raise FatalEnrichError(
            "report invariant failed: selected_phase_breakdown total mismatch"
        )
    if eligible_newsletters_total + eligible_youtube_total != eligible_pool_total:
        raise FatalEnrichError(
            "report invariant failed: eligible type totals do not match eligible_pool_total"
        )
    if selected_invalid_count > invalid_pool_total:
        raise FatalEnrichError("report invariant failed: selected_invalid_count > invalid_pool_total")
    if cooldown_blocked_total + eligible_pool_total + invalid_pool_total != candidates_available_total:
        raise FatalEnrichError("report invariant failed: pool partition total mismatch")
    if youtube_preflight_executed != (selected_youtube_count > 0):
        raise FatalEnrichError("report invariant failed: youtube_preflight_executed mismatch")
    if youtube_transcripts_attempted != 0:
        raise FatalEnrichError("report invariant failed: youtube_transcripts_attempted must be 0 in asr_only")
    if youtube_transcripts_succeeded != 0:
        raise FatalEnrichError("report invariant failed: youtube_transcripts_succeeded must be 0 in asr_only")
    if asr_fallbacks_used != 0:
        raise FatalEnrichError("report invariant failed: asr_fallbacks_used must be 0 in asr_only")
    if fail_breakdown["youtube_transcript_unavailable"] != 0:
        raise FatalEnrichError(
            "report invariant failed: youtube_transcript_unavailable must be 0 in asr_only"
        )
    if fail_breakdown["youtube_transcript_failed"] != 0:
        raise FatalEnrichError(
            "report invariant failed: youtube_transcript_failed must be 0 in asr_only"
        )
    if fail_breakdown["transcript_cap_reached"] != 0:
        raise FatalEnrichError("report invariant failed: transcript_cap_reached must be 0 in asr_only")
    if fail_breakdown["asr_cap_reached"] != 0:
        raise FatalEnrichError("report invariant failed: asr_cap_reached must be 0 in asr_only")
    if full_success_required and selected_invalid_count != 0:
        raise FatalEnrichError(
            "report invariant failed: selected_invalid_count must be 0 when full_success_required"
        )
    if run_status == "completed":
        if newsletter_floor_met != (selected_newsletter_count >= newsletter_floor_target):
            raise FatalEnrichError("report invariant failed: newsletter_floor_met mismatch")
        if youtube_floor_met != (selected_youtube_count >= youtube_floor_target):
            raise FatalEnrichError("report invariant failed: youtube_floor_met mismatch")
        if full_success_required and failed_count != 0:
            raise FatalEnrichError(
                "report invariant failed: failed_count must be 0 when full_success_required completed"
            )
