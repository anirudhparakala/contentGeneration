from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


FAIL_BREAKDOWN_KEYS: tuple[str, ...] = (
    "script_llm_failed",
    "script_invalid_json",
    "script_validation_failed",
)

PLATFORMS: frozenset[str] = frozenset({"youtube", "newsletter"})
RECOMMENDED_FORMATS: frozenset[str] = frozenset(
    {"shorts", "tweet", "linkedin", "reel", "thread", "other"}
)
VIDEO_FORMATS: frozenset[str] = frozenset({"shorts", "reel"})
TEXT_FORMATS: frozenset[str] = frozenset({"tweet", "thread", "linkedin", "other"})

UTC_SECONDS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
TOKEN_RE = re.compile(r"[A-Za-z0-9]+(?:['-][A-Za-z0-9]+)*")

EXPECTED_TOP_LEVEL_KEYS: frozenset[str] = frozenset(
    {"primary_hook", "alt_hooks", "script", "cta", "disclaimer"}
)
EXPECTED_SCRIPT_KEYS: frozenset[str] = frozenset({"sections", "word_count", "estimated_seconds"})
EXPECTED_SECTION_KEYS: frozenset[str] = frozenset({"label", "text"})
EXPECTED_SECTION_LABELS: tuple[str, ...] = ("hook", "setup", "steps", "cta")


class RowValidationError(ValueError):
    pass


class ScriptValidationError(ValueError):
    pass


@dataclass(frozen=True)
class SelectedIdeaRow:
    item_id: str
    platform: str
    recommended_format: str
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
    hooks: list[str]
    viral_rating: int


@dataclass(frozen=True)
class ScriptSection:
    label: str
    text: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class ScriptPayload:
    primary_hook: str
    alt_hooks: list[str]
    script_sections: list[ScriptSection]
    word_count: int
    estimated_seconds: int
    cta: str
    disclaimer: str


@dataclass(frozen=True)
class ScriptRecord:
    item_id: str
    platform: str
    recommended_format: str
    primary_hook: str
    alt_hooks: list[str]
    script_sections: list[dict[str, str]]
    word_count: int
    estimated_seconds: int
    cta: str
    disclaimer: str
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


def parse_selected_row(raw: Mapping[str, Any]) -> SelectedIdeaRow:
    non_empty_scalars = (
        "item_id",
        "platform",
        "recommended_format",
        "url",
        "title",
        "published_at",
    )
    values: dict[str, str] = {}
    for key in non_empty_scalars:
        values[key] = _require_string(raw.get(key), key, allow_empty=False, error_cls=RowValidationError)

    if UTC_SECONDS_RE.fullmatch(values["published_at"]) is None:
        raise RowValidationError("published_at must be YYYY-MM-DDTHH:MM:SSZ")

    if values["platform"] not in PLATFORMS:
        raise RowValidationError("platform must be youtube or newsletter")
    if values["recommended_format"] not in RECOMMENDED_FORMATS:
        raise RowValidationError("recommended_format must be a supported enum value")

    topic = _require_string(raw.get("topic"), "topic", allow_empty=True, error_cls=RowValidationError)
    core_claim = _require_string(
        raw.get("core_claim"),
        "core_claim",
        allow_empty=True,
        error_cls=RowValidationError,
    )
    monetization_angle = _require_string(
        raw.get("monetization_angle"),
        "monetization_angle",
        allow_empty=True,
        error_cls=RowValidationError,
    )

    workflow_steps = _parse_json_string_array(raw.get("workflow_steps"), "workflow_steps")
    if len(workflow_steps) > 8:
        raise RowValidationError("workflow_steps length must be between 0 and 8")
    tools_mentioned = _parse_json_string_array(raw.get("tools_mentioned"), "tools_mentioned")
    metrics_claims = _parse_json_string_array(raw.get("metrics_claims"), "metrics_claims")
    assumptions = _parse_json_string_array(raw.get("assumptions"), "assumptions")
    hooks = _parse_json_string_array(raw.get("hooks"), "hooks")
    if len(hooks) != 3:
        raise RowValidationError("hooks must contain exactly 3 strings")

    viral_rating = raw.get("viral_rating")
    if not is_non_bool_int(viral_rating) or int(viral_rating) < 1 or int(viral_rating) > 10:
        raise RowValidationError("viral_rating must be an integer in [1, 10]")

    return SelectedIdeaRow(
        item_id=values["item_id"],
        platform=values["platform"],
        recommended_format=values["recommended_format"],
        url=values["url"],
        title=values["title"],
        published_at=values["published_at"],
        topic=topic,
        core_claim=core_claim,
        workflow_steps=workflow_steps,
        tools_mentioned=tools_mentioned,
        monetization_angle=monetization_angle,
        metrics_claims=metrics_claims,
        assumptions=assumptions,
        hooks=hooks,
        viral_rating=int(viral_rating),
    )


def parse_json_text(raw: str) -> Any:
    if not isinstance(raw, str):
        raise ValueError("response must be a string")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("invalid JSON response") from exc


def validate_script_payload(raw: Any, *, recommended_format: str) -> ScriptPayload:
    if not isinstance(raw, dict):
        raise ScriptValidationError("script payload must be a JSON object")
    if set(raw.keys()) != EXPECTED_TOP_LEVEL_KEYS:
        raise ScriptValidationError("script payload keys mismatch")

    primary_hook = _require_string(
        raw.get("primary_hook"),
        "primary_hook",
        allow_empty=False,
        error_cls=ScriptValidationError,
    )
    if len(primary_hook) > 140:
        raise ScriptValidationError("primary_hook must be <= 140 chars")

    alt_hooks_raw = raw.get("alt_hooks")
    if not isinstance(alt_hooks_raw, list) or len(alt_hooks_raw) != 2:
        raise ScriptValidationError("alt_hooks must contain exactly 2 strings")
    alt_hooks: list[str] = []
    for value in alt_hooks_raw:
        normalized = _require_string(
            value,
            "alt_hooks",
            allow_empty=False,
            error_cls=ScriptValidationError,
        )
        if len(normalized) > 140:
            raise ScriptValidationError("alt_hooks entries must be <= 140 chars")
        alt_hooks.append(normalized)

    script_raw = raw.get("script")
    if not isinstance(script_raw, dict):
        raise ScriptValidationError("script must be a JSON object")
    if set(script_raw.keys()) != EXPECTED_SCRIPT_KEYS:
        raise ScriptValidationError("script keys mismatch")

    sections_raw = script_raw.get("sections")
    if not isinstance(sections_raw, list) or len(sections_raw) != len(EXPECTED_SECTION_LABELS):
        raise ScriptValidationError("script.sections must contain exactly 4 labeled sections")

    sections: list[ScriptSection] = []
    for index, expected_label in enumerate(EXPECTED_SECTION_LABELS):
        entry = sections_raw[index]
        if not isinstance(entry, dict):
            raise ScriptValidationError("script.sections entries must be objects")
        if set(entry.keys()) != EXPECTED_SECTION_KEYS:
            raise ScriptValidationError("script.sections entries must contain exactly label and text")
        label = _require_string(
            entry.get("label"),
            "script.sections.label",
            allow_empty=False,
            error_cls=ScriptValidationError,
        )
        text = _require_string(
            entry.get("text"),
            "script.sections.text",
            allow_empty=False,
            error_cls=ScriptValidationError,
        )
        if label != expected_label:
            raise ScriptValidationError("script.sections labels/order mismatch")
        sections.append(ScriptSection(label=label, text=text))

    _validate_steps_section_text(sections[2].text)

    model_word_count = script_raw.get("word_count")
    if not is_non_bool_int(model_word_count):
        raise ScriptValidationError("script.word_count must be a non-boolean integer")

    estimated_seconds = script_raw.get("estimated_seconds")
    if not is_non_bool_int(estimated_seconds):
        raise ScriptValidationError("script.estimated_seconds must be a non-boolean integer")
    estimated_seconds_int = int(estimated_seconds)

    cta = _require_string(raw.get("cta"), "cta", allow_empty=False, error_cls=ScriptValidationError)
    if cta != sections[3].text:
        raise ScriptValidationError("cta must equal script.sections[label=cta].text")

    disclaimer = _require_string(
        raw.get("disclaimer"),
        "disclaimer",
        allow_empty=True,
        error_cls=ScriptValidationError,
    )

    computed_word_count = compute_word_count(sections)
    _validate_format_policy(
        recommended_format=recommended_format,
        computed_word_count=computed_word_count,
        estimated_seconds=estimated_seconds_int,
    )

    return ScriptPayload(
        primary_hook=primary_hook,
        alt_hooks=alt_hooks,
        script_sections=sections,
        word_count=computed_word_count,
        estimated_seconds=estimated_seconds_int,
        cta=cta,
        disclaimer=disclaimer,
    )


def compute_word_count(sections: list[ScriptSection]) -> int:
    total = 0
    for section in sections:
        total += len(TOKEN_RE.findall(section.text))
    return total


def build_script_record(
    *,
    row: SelectedIdeaRow,
    payload: ScriptPayload,
    llm_provider: str,
    llm_model: str,
    created_at: str,
) -> ScriptRecord:
    return ScriptRecord(
        item_id=row.item_id,
        platform=row.platform,
        recommended_format=row.recommended_format,
        primary_hook=payload.primary_hook,
        alt_hooks=list(payload.alt_hooks),
        script_sections=[section.to_dict() for section in payload.script_sections],
        word_count=payload.word_count,
        estimated_seconds=payload.estimated_seconds,
        cta=payload.cta,
        disclaimer=payload.disclaimer,
        llm_provider=llm_provider,
        llm_model=llm_model,
        created_at=created_at,
    )


def _parse_json_string_array(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, str):
        raise RowValidationError(f"{field_name} must be a JSON string array")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RowValidationError(f"{field_name} must be a JSON string array") from exc
    if not isinstance(parsed, list):
        raise RowValidationError(f"{field_name} must be a JSON string array")

    normalized: list[str] = []
    for entry in parsed:
        if not isinstance(entry, str):
            raise RowValidationError(f"{field_name} must be a JSON string array")
        trimmed = entry.strip()
        if trimmed:
            normalized.append(trimmed)
    return normalized


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


def _validate_steps_section_text(text: str) -> None:
    normalized_lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    lines = [line.strip() for line in normalized_lines if line.strip()]
    if len(lines) < 3 or len(lines) > 6:
        raise ScriptValidationError("steps section must contain exactly 3..6 non-empty lines")
    for line in lines:
        if not line.startswith("- "):
            raise ScriptValidationError("steps section lines must start with '- '")
        if not line[2:].strip():
            raise ScriptValidationError("steps section lines must contain non-empty text")


def _validate_format_policy(
    *,
    recommended_format: str,
    computed_word_count: int,
    estimated_seconds: int,
) -> None:
    if recommended_format in VIDEO_FORMATS:
        if computed_word_count < 120:
            raise ScriptValidationError("computed_word_count below minimum for shorts/reel")
        if estimated_seconds < 45 or estimated_seconds > 70:
            raise ScriptValidationError("estimated_seconds out of range for shorts/reel")
        return
    if recommended_format in TEXT_FORMATS:
        if computed_word_count < 180:
            raise ScriptValidationError("computed_word_count below minimum for text formats")
        if estimated_seconds < 70 or estimated_seconds > 110:
            raise ScriptValidationError("estimated_seconds out of range for text formats")
        return
    raise ScriptValidationError("recommended_format must be a supported enum value")
