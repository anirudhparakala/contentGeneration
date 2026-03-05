from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


FAIL_BREAKDOWN_KEYS: tuple[str, ...] = (
    "extract_llm_failed",
    "extract_invalid_json",
    "extract_validation_failed",
    "score_llm_failed",
    "score_invalid_json",
    "score_validation_failed",
)

EXTRACT_CONTENT_TYPES: frozenset[str] = frozenset(
    {"howto", "case_study", "tool_review", "opinion", "news", "other"}
)
SCORE_PLATFORMS: frozenset[str] = frozenset({"youtube", "newsletter"})
SCORE_FORMATS: frozenset[str] = frozenset(
    {"shorts", "tweet", "linkedin", "reel", "thread", "other"}
)
UTC_SECONDS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
WHITESPACE_RE = re.compile(r"\s+")


class RowValidationError(ValueError):
    pass


class ExtractValidationError(ValueError):
    pass


class ScoreValidationError(ValueError):
    pass


@dataclass(frozen=True)
class SelectedRow:
    item_id: str
    source_type: str
    url: str
    title: str
    published_at: str
    enriched_text: str
    enrichment_method: str
    evidence_snippets_raw: Any


@dataclass(frozen=True)
class ExtractPayload:
    topic: str
    core_claim: str
    workflow_steps: list[str]
    tools_mentioned: list[str]
    monetization_angle: str
    metrics_claims: list[str]
    assumptions: list[str]
    content_type: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ScorePayload:
    viral_rating: int
    rating_rationale: str
    hooks: list[str]
    platform: str
    recommended_format: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class IdeaRecord:
    item_id: str
    source_type: str
    url: str
    title: str
    published_at: str
    topic: str
    core_claim: str
    workflow_steps: list[str]
    tools_mentioned: list[str]
    monetization_angle: str
    metrics_claims: list[str]
    assumptions: list[str]
    content_type: str
    viral_rating: int
    rating_rationale: str
    hooks: list[str]
    platform: str
    recommended_format: str
    llm_provider: str
    llm_model: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def to_utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_non_bool_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def is_non_bool_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    return isinstance(value, (int, float))


def parse_selected_row(raw: Mapping[str, Any]) -> SelectedRow:
    required = (
        "item_id",
        "source_type",
        "url",
        "title",
        "published_at",
        "enriched_text",
        "enrichment_method",
    )
    values: dict[str, str] = {}
    for key in required:
        value = raw.get(key)
        if not isinstance(value, str):
            raise RowValidationError(f"{key} must be a non-empty string")
        normalized = value.strip()
        if not normalized:
            raise RowValidationError(f"{key} must be a non-empty string")
        values[key] = normalized

    source_type = values["source_type"].lower()
    if source_type not in SCORE_PLATFORMS:
        raise RowValidationError("source_type must be newsletter or youtube")

    published_at = values["published_at"]
    if UTC_SECONDS_RE.fullmatch(published_at) is None:
        raise RowValidationError("published_at must be YYYY-MM-DDTHH:MM:SSZ")

    return SelectedRow(
        item_id=values["item_id"],
        source_type=source_type,
        url=values["url"],
        title=values["title"],
        published_at=published_at,
        enriched_text=values["enriched_text"],
        enrichment_method=values["enrichment_method"],
        evidence_snippets_raw=raw.get("evidence_snippets"),
    )


def source_type_to_platform_hint(source_type: str) -> str:
    if source_type == "newsletter":
        return "newsletter"
    if source_type == "youtube":
        return "youtube"
    raise ValueError("unsupported source_type")


def parse_json_text(raw: str) -> Any:
    if not isinstance(raw, str):
        raise ValueError("response must be a string")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("invalid JSON response") from exc


def validate_extract_payload(raw: Any) -> ExtractPayload:
    expected_keys = {
        "topic",
        "core_claim",
        "workflow_steps",
        "tools_mentioned",
        "monetization_angle",
        "metrics_claims",
        "assumptions",
        "content_type",
    }
    if not isinstance(raw, dict):
        raise ExtractValidationError("extract payload must be a JSON object")
    if set(raw.keys()) != expected_keys:
        raise ExtractValidationError("extract payload keys mismatch")

    topic = _require_string(
        raw.get("topic"),
        "topic",
        allow_empty=True,
        error_cls=ExtractValidationError,
    )
    core_claim = _require_string(
        raw.get("core_claim"),
        "core_claim",
        allow_empty=True,
        error_cls=ExtractValidationError,
    )
    workflow_steps = _require_string_list(
        raw.get("workflow_steps"),
        "workflow_steps",
        error_cls=ExtractValidationError,
    )
    if len(workflow_steps) > 8:
        raise ExtractValidationError("workflow_steps length must be between 0 and 8")
    tools_mentioned = _require_string_list(
        raw.get("tools_mentioned"),
        "tools_mentioned",
        error_cls=ExtractValidationError,
    )
    monetization_angle = _require_string(
        raw.get("monetization_angle"),
        "monetization_angle",
        allow_empty=True,
        error_cls=ExtractValidationError,
    )
    metrics_claims = _require_string_list(
        raw.get("metrics_claims"),
        "metrics_claims",
        error_cls=ExtractValidationError,
    )
    assumptions = _require_string_list(
        raw.get("assumptions"),
        "assumptions",
        error_cls=ExtractValidationError,
    )
    content_type = _require_string(
        raw.get("content_type"),
        "content_type",
        allow_empty=False,
        error_cls=ExtractValidationError,
    )
    if content_type not in EXTRACT_CONTENT_TYPES:
        raise ExtractValidationError("content_type must be a supported enum value")

    return ExtractPayload(
        topic=topic,
        core_claim=core_claim,
        workflow_steps=workflow_steps,
        tools_mentioned=tools_mentioned,
        monetization_angle=monetization_angle,
        metrics_claims=metrics_claims,
        assumptions=assumptions,
        content_type=content_type,
    )


def validate_score_payload(raw: Any, *, platform_hint: str) -> ScorePayload:
    expected_keys = {
        "viral_rating",
        "rating_rationale",
        "hooks",
        "platform",
        "recommended_format",
    }
    if not isinstance(raw, dict):
        raise ScoreValidationError("score payload must be a JSON object")
    if set(raw.keys()) != expected_keys:
        raise ScoreValidationError("score payload keys mismatch")

    viral_rating = raw.get("viral_rating")
    if not is_non_bool_int(viral_rating) or int(viral_rating) < 1 or int(viral_rating) > 10:
        raise ScoreValidationError("viral_rating must be an integer between 1 and 10")

    rating_rationale = _require_string(
        raw.get("rating_rationale"),
        "rating_rationale",
        allow_empty=False,
        error_cls=ScoreValidationError,
    )

    hooks_raw = raw.get("hooks")
    hooks = _require_string_list(hooks_raw, "hooks", error_cls=ScoreValidationError)
    if len(hooks) != 3:
        raise ScoreValidationError("hooks must contain exactly 3 strings")
    normalized_hooks: list[str] = []
    for hook in hooks:
        hook_trim = hook.strip()
        if not hook_trim:
            raise ScoreValidationError("hooks must be non-empty after strip()")
        if len(hook_trim) > 140:
            raise ScoreValidationError("hooks entries must be <= 140 chars")
        normalized_hooks.append(hook_trim)

    platform = _require_string(
        raw.get("platform"),
        "platform",
        allow_empty=False,
        error_cls=ScoreValidationError,
    )
    if platform not in SCORE_PLATFORMS:
        raise ScoreValidationError("platform must be newsletter or youtube")
    if platform != platform_hint:
        raise ScoreValidationError("platform must match platform_hint")

    recommended_format = _require_string(
        raw.get("recommended_format"),
        "recommended_format",
        allow_empty=False,
        error_cls=ScoreValidationError,
    )
    if recommended_format not in SCORE_FORMATS:
        raise ScoreValidationError("recommended_format must be a supported enum value")

    return ScorePayload(
        viral_rating=int(viral_rating),
        rating_rationale=rating_rationale,
        hooks=normalized_hooks,
        platform=platform,
        recommended_format=recommended_format,
    )


def preprocess_evidence_snippets(raw: Any) -> list[str]:
    parsed: Any = []
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = []
    if not isinstance(parsed, list):
        return []

    snippets: list[str] = []
    for value in parsed:
        if not isinstance(value, dict):
            continue
        text = value.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        normalized = WHITESPACE_RE.sub(" ", text).strip()
        snippets.append(normalized[:240])
        if len(snippets) == 3:
            break
    return snippets


def build_idea_record(
    *,
    row: SelectedRow,
    extract: ExtractPayload,
    score: ScorePayload,
    llm_provider: str,
    llm_model: str,
    created_at: str,
) -> IdeaRecord:
    return IdeaRecord(
        item_id=row.item_id,
        source_type=row.source_type,
        url=row.url,
        title=row.title,
        published_at=row.published_at,
        topic=extract.topic,
        core_claim=extract.core_claim,
        workflow_steps=extract.workflow_steps,
        tools_mentioned=extract.tools_mentioned,
        monetization_angle=extract.monetization_angle,
        metrics_claims=extract.metrics_claims,
        assumptions=extract.assumptions,
        content_type=extract.content_type,
        viral_rating=score.viral_rating,
        rating_rationale=score.rating_rationale,
        hooks=score.hooks,
        platform=score.platform,
        recommended_format=score.recommended_format,
        llm_provider=llm_provider,
        llm_model=llm_model,
        created_at=created_at,
    )


def _require_string(
    value: Any,
    field_name: str,
    *,
    allow_empty: bool,
    error_cls: type[Exception],
) -> str:
    if not isinstance(value, str):
        raise error_cls(f"{field_name} must be a string")
    normalized = value.strip()
    if not allow_empty and not normalized:
        raise error_cls(f"{field_name} must be non-empty after strip()")
    return normalized


def _require_string_list(
    value: Any,
    field_name: str,
    *,
    error_cls: type[Exception],
) -> list[str]:
    if not isinstance(value, list):
        raise error_cls(f"{field_name} must be a list of strings")
    normalized: list[str] = []
    for entry in value:
        if not isinstance(entry, str):
            raise error_cls(f"{field_name} must be a list of strings")
        normalized.append(entry)
    return normalized
