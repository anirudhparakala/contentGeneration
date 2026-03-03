from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import ConfigError, PipelineConfig, Source, load_pipeline_config, load_sources_config
from .feeds import parse_feed_entries, to_utc_z
from .fetch import fetch_feed
from .models import RawItem
from .state import SeenItemsStore, StateError


LOGGER = logging.getLogger(__name__)


class FatalIngestionError(RuntimeError):
    pass


@dataclass
class SourceReport:
    source_id: str
    source_type: str
    entries_parsed: int = 0
    entries_skipped_recency: int = 0
    entries_skipped_missing_url: int = 0
    entries_skipped_missing_title: int = 0
    entries_skipped_seen: int = 0
    new_items_emitted: int = 0
    error: str | None = None


@dataclass
class IngestionResult:
    run_id: str
    raw_items_path: str
    report_path: str
    total_entries_parsed: int
    total_new_items_emitted: int
    sources_failed: int

    def to_dict(self) -> dict:
        return asdict(self)


def run_ingestion(
    *,
    sources_path: str,
    pipeline_path: str,
    out_path: str | None = None,
    report_path: str | None = None,
    db_path_override: str | None = None,
    max_per_source_override: int | None = None,
    recency_days_override: int | None = None,
) -> IngestionResult:
    started_at = _utc_now()
    run_id = str(uuid.uuid4())

    try:
        sources = load_sources_config(sources_path)
        pipeline = load_pipeline_config(pipeline_path)
    except ConfigError as exc:
        raise FatalIngestionError(str(exc)) from exc

    max_per_source = (
        int(max_per_source_override)
        if max_per_source_override is not None
        else pipeline.max_entries_per_source
    )
    recency_days = (
        int(recency_days_override)
        if recency_days_override is not None
        else pipeline.recency_days
    )
    if max_per_source <= 0:
        raise FatalIngestionError("max entries per source must be > 0")
    if recency_days is not None and recency_days < 0:
        raise FatalIngestionError("recency_days must be >= 0")

    out_file = _resolve_out_path(out_path=out_path, pipeline=pipeline, date=started_at)
    report_file = _resolve_report_path(
        report_path=report_path,
        out_path=out_path,
        out_file=out_file,
        pipeline=pipeline,
        date=started_at,
    )
    db_path = Path(db_path_override or pipeline.paths.sqlite_db)

    LOGGER.info("stage_1_ingest start run_id=%s sources=%s", run_id, len(sources))

    try:
        store = SeenItemsStore(db_path=db_path)
    except StateError as exc:
        raise FatalIngestionError(str(exc)) from exc

    source_reports: list[SourceReport] = []
    emitted_items: list[RawItem] = []
    cutoff = (
        started_at - timedelta(days=recency_days)
        if recency_days is not None
        else None
    )

    try:
        for source in sources:
            report = _process_source(
                source=source,
                pipeline=pipeline,
                cutoff=cutoff,
                max_per_source=max_per_source,
                store=store,
            )
            source_reports.append(report.report)
            emitted_items.extend(report.items)
    finally:
        store.close()

    total_entries_parsed = sum(s.entries_parsed for s in source_reports)
    total_new_items_emitted = sum(s.new_items_emitted for s in source_reports)
    sources_failed = sum(1 for s in source_reports if s.error)
    sources_succeeded = len(source_reports) - sources_failed

    finished_at = _utc_now()
    report_payload = {
        "run_id": run_id,
        "started_at": to_utc_z(started_at),
        "finished_at": to_utc_z(finished_at),
        "total_sources": len(source_reports),
        "sources_succeeded": sources_succeeded,
        "sources_failed": sources_failed,
        "total_entries_parsed": total_entries_parsed,
        "total_new_items_emitted": total_new_items_emitted,
        "per_source": [asdict(s) for s in source_reports],
    }

    try:
        _write_jsonl(out_file, emitted_items)
        _write_json(report_file, report_payload)
    except OSError as exc:
        raise FatalIngestionError(f"failed writing outputs: {exc}") from exc

    LOGGER.info(
        "stage_1_ingest complete run_id=%s total_entries_parsed=%s total_new_items_emitted=%s sources_failed=%s",
        run_id,
        total_entries_parsed,
        total_new_items_emitted,
        sources_failed,
    )
    return IngestionResult(
        run_id=run_id,
        raw_items_path=str(out_file),
        report_path=str(report_file),
        total_entries_parsed=total_entries_parsed,
        total_new_items_emitted=total_new_items_emitted,
        sources_failed=sources_failed,
    )


@dataclass
class _SourceProcessResult:
    report: SourceReport
    items: list[RawItem]


def _process_source(
    *,
    source: Source,
    pipeline: PipelineConfig,
    cutoff: datetime | None,
    max_per_source: int,
    store: SeenItemsStore,
) -> _SourceProcessResult:
    LOGGER.info("ingest source start source_id=%s source_type=%s", source.id, source.source_type)
    report = SourceReport(source_id=source.id, source_type=source.source_type)
    fetched_at = _utc_now()
    emitted: list[RawItem] = []

    try:
        fetched = fetch_feed(source.feed_url, pipeline.http)
        parsed_entries = parse_feed_entries(
            source_type=source.source_type,
            source_id=source.id,
            source_name=source.name,
            content=fetched.body,
            fetched_at=fetched_at,
        )
        report.entries_parsed = len(parsed_entries)

        candidates: list[RawItem] = []
        for parsed in parsed_entries:
            if cutoff is not None and parsed.published_at < cutoff:
                report.entries_skipped_recency += 1
                continue
            if not parsed.url:
                report.entries_skipped_missing_url += 1
                continue
            if not parsed.title:
                report.entries_skipped_missing_title += 1
                continue

            candidates.append(
                RawItem(
                    source_type=parsed.source_type,
                    source_id=parsed.source_id,
                    source_name=parsed.source_name,
                    creator=parsed.creator or source.name,
                    title=parsed.title,
                    url=parsed.url,
                    published_at=to_utc_z(parsed.published_at),
                    external_id=parsed.external_id,
                    summary=parsed.summary or "",
                    fetched_at=to_utc_z(fetched_at),
                )
            )

        # Deterministic ordering: published_at desc, then external_id asc, then url asc.
        candidates.sort(key=lambda item: item.url)
        candidates.sort(key=lambda item: item.external_id)
        candidates.sort(key=lambda item: item.published_at, reverse=True)
        candidates = candidates[:max_per_source]

        for item in candidates:
            is_new = store.register_if_new(
                source_id=item.source_id,
                source_type=item.source_type,
                external_id=item.external_id,
                url=item.url,
                published_at=item.published_at,
                first_seen_at=item.fetched_at,
            )
            if not is_new:
                report.entries_skipped_seen += 1
                continue
            emitted.append(item)

        report.new_items_emitted = len(emitted)
        if (
            report.entries_skipped_recency
            or report.entries_skipped_missing_url
            or report.entries_skipped_missing_title
            or report.entries_skipped_seen
        ):
            LOGGER.warning(
                "source=%s skips recency=%s missing_url=%s missing_title=%s seen=%s",
                source.id,
                report.entries_skipped_recency,
                report.entries_skipped_missing_url,
                report.entries_skipped_missing_title,
                report.entries_skipped_seen,
            )
    except Exception as exc:
        report.error = str(exc)
        LOGGER.error(
            "ingest source failed source_id=%s source_type=%s error=%s",
            source.id,
            source.source_type,
            exc,
        )
        emitted = []

    LOGGER.info(
        "ingest source end source_id=%s parsed=%s emitted=%s error=%s",
        source.id,
        report.entries_parsed,
        report.new_items_emitted,
        bool(report.error),
    )
    return _SourceProcessResult(report=report, items=emitted)


def _resolve_out_path(*, out_path: str | None, pipeline: PipelineConfig, date: datetime) -> Path:
    if out_path:
        return Path(out_path)
    return Path(pipeline.paths.outputs_dir) / f"raw_items_{date.strftime('%Y-%m-%d')}.jsonl"


def _resolve_report_path(
    *,
    report_path: str | None,
    out_path: str | None,
    out_file: Path,
    pipeline: PipelineConfig,
    date: datetime,
) -> Path:
    if report_path:
        return Path(report_path)
    report_name = f"run_report_{date.strftime('%Y-%m-%d')}.json"
    if out_path:
        return out_file.parent / report_name
    return Path(pipeline.paths.outputs_dir) / report_name


def _write_jsonl(path: Path, items: list[RawItem]) -> None:
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
