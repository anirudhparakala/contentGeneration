from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


FAIL_BREAKDOWN_KEYS: tuple[str, ...] = (
    "invalid_candidate_row",
    "newsletter_fetch_failed",
    "newsletter_extract_failed",
    "newsletter_text_too_short",
    "youtube_video_id_parse_failed",
    "youtube_transcript_unavailable",
    "youtube_transcript_failed",
    "youtube_asr_failed",
    "youtube_text_too_short",
    "transcript_cap_reached",
    "asr_cap_reached",
)

TRANSCRIPT_LANGUAGES: tuple[str, ...] = ("en", "en-US", "en-GB")
UTC_SECONDS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
WHITESPACE_RE = re.compile(r"\s+")


class CandidateValidationError(ValueError):
    pass


@dataclass(frozen=True)
class CandidateRow:
    item_id: str
    source_type: str
    source_id: str
    url: str
    title: str
    published_at: str
    relevance_score: int

    @property
    def source_key(self) -> tuple[str, str]:
        return (self.source_type, self.source_id)


@dataclass(frozen=True)
class EvidenceMeta:
    type: str
    offset: int | None
    timestamp: str | None


@dataclass(frozen=True)
class EvidenceSnippet:
    text: str
    meta: EvidenceMeta

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        return payload


@dataclass(frozen=True)
class EnrichedItem:
    item_id: str
    source_type: str
    url: str
    title: str
    published_at: str
    enriched_text: str
    evidence_snippets: list[EvidenceSnippet]
    enrichment_method: str
    enriched_at: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence_snippets"] = [snippet.to_dict() for snippet in self.evidence_snippets]
        return payload


def parse_candidate_row(row: Mapping[str, Any]) -> CandidateRow:
    required_strings = ("item_id", "source_type", "source_id", "url", "title", "published_at")
    values: dict[str, str] = {}

    for key in required_strings:
        raw = row.get(key)
        if not isinstance(raw, str):
            raise CandidateValidationError(f"{key} must be a non-empty string")
        trimmed = raw.strip()
        if not trimmed:
            raise CandidateValidationError(f"{key} must be a non-empty string")
        values[key] = trimmed

    source_type = values["source_type"].lower()
    if source_type not in ("newsletter", "youtube"):
        raise CandidateValidationError("source_type must be newsletter or youtube")

    source_id = values["source_id"].lower()
    if not source_id:
        raise CandidateValidationError("source_id must be a non-empty string")

    published_at = values["published_at"]
    if not UTC_SECONDS_RE.fullmatch(published_at):
        raise CandidateValidationError("published_at must be YYYY-MM-DDTHH:MM:SSZ")

    relevance_score = row.get("relevance_score")
    if not is_non_bool_int(relevance_score) or int(relevance_score) < 0:
        raise CandidateValidationError("relevance_score must be an integer >= 0")

    return CandidateRow(
        item_id=values["item_id"],
        source_type=source_type,
        source_id=source_id,
        url=values["url"],
        title=values["title"],
        published_at=published_at,
        relevance_score=int(relevance_score),
    )


def normalize_text(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def to_utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_now_z() -> str:
    return to_utc_z(utc_now())


def is_non_bool_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)
