from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


REQUIRED_HEADERS: tuple[str, ...] = (
    "item_id",
    "creator",
    "post_link",
    "topic",
    "viral_rating",
    "hook",
    "platform",
    "draft_script",
    "status",
)

OPTIONAL_HEADERS: tuple[str, ...] = (
    "monetization_angle",
    "tools_mentioned",
    "published_at",
    "updated_at",
    "notes",
)

EXPECTED_SCRIPT_LABELS: tuple[str, ...] = ("hook", "setup", "steps", "cta")
EXPECTED_SCRIPT_SECTION_KEYS: frozenset[str] = frozenset({"label", "text"})


class RowMappingError(ValueError):
    pass


class HeaderValidationError(ValueError):
    pass


@dataclass(frozen=True)
class PersistSheetRow:
    item_id: str
    creator: str
    post_link: str
    topic: str
    viral_rating: int
    hook: str
    platform: str
    draft_script: str
    monetization_angle: str
    tools_mentioned: str
    published_at: str
    updated_at: str


@dataclass(frozen=True)
class HeaderLayout:
    headers: list[str]
    index_by_name: dict[str, int]
    key_index: int
    has_notes: bool
    has_updated_at: bool


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def to_utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_non_bool_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def build_sheet_row(raw: Mapping[str, Any], *, updated_at: str) -> PersistSheetRow:
    item_id = _require_string(raw.get("item_id"), "ideas.item_id", allow_empty=False)
    post_link = _require_string(raw.get("url"), "ideas.url", allow_empty=False)
    topic = _require_string(raw.get("topic"), "ideas.topic", allow_empty=True)
    platform = _require_string(raw.get("platform"), "ideas.platform", allow_empty=True)
    monetization_angle = _require_string(
        raw.get("monetization_angle"),
        "ideas.monetization_angle",
        allow_empty=True,
    )
    published_at = _require_string(raw.get("published_at"), "ideas.published_at", allow_empty=True)

    viral_rating_raw = raw.get("viral_rating")
    if not is_non_bool_int(viral_rating_raw):
        raise RowMappingError("ideas.viral_rating must be a non-boolean integer")
    viral_rating = int(viral_rating_raw)

    hook = _resolve_hook(primary_hook=raw.get("primary_hook"), hooks_json=raw.get("hooks"))
    tools_mentioned = _resolve_tools_mentioned(raw.get("tools_mentioned"))
    draft_script = _resolve_draft_script(raw.get("script_sections"))
    creator = _resolve_creator(raw.get("creator"), raw.get("source_name"))

    return PersistSheetRow(
        item_id=item_id,
        creator=creator,
        post_link=post_link,
        topic=topic,
        viral_rating=viral_rating,
        hook=hook,
        platform=platform,
        draft_script=draft_script,
        monetization_angle=monetization_angle,
        tools_mentioned=tools_mentioned,
        published_at=published_at,
        updated_at=_require_string(updated_at, "updated_at", allow_empty=False),
    )


def build_header_layout(header_cells: Sequence[Any], *, key_column: str) -> HeaderLayout:
    normalized_headers: list[str] = []
    index_by_name: dict[str, int] = {}

    for index, cell in enumerate(header_cells):
        value = cell if isinstance(cell, str) else ("" if cell is None else str(cell))
        normalized = value.strip()
        if normalized in index_by_name:
            raise HeaderValidationError(f"duplicate header: {normalized}")
        index_by_name[normalized] = index
        normalized_headers.append(normalized)

    missing = [name for name in REQUIRED_HEADERS if name not in index_by_name]
    if missing:
        raise HeaderValidationError(
            "missing required headers: " + ", ".join(missing)
        )

    normalized_key = key_column.strip()
    if normalized_key != "item_id":
        raise HeaderValidationError("sheets.key_column must be item_id for stage 7")
    if normalized_key not in index_by_name:
        raise HeaderValidationError(f"key column not present in headers: {normalized_key}")

    return HeaderLayout(
        headers=normalized_headers,
        index_by_name=index_by_name,
        key_index=index_by_name[normalized_key],
        has_notes="notes" in index_by_name,
        has_updated_at="updated_at" in index_by_name,
    )


def _resolve_creator(creator: Any, source_name: Any) -> str:
    creator_value = _normalize_optional_string(creator)
    if creator_value:
        return creator_value

    source_name_value = _normalize_optional_string(source_name)
    if source_name_value:
        return source_name_value
    return "unknown"


def _resolve_hook(*, primary_hook: Any, hooks_json: Any) -> str:
    primary = _normalize_optional_string(primary_hook)
    if primary:
        return primary

    hooks = _parse_json_string_array(hooks_json, "ideas.hooks")
    for entry in hooks:
        if entry:
            return entry
    raise RowMappingError("hook unavailable: no non-empty primary_hook or fallback hooks")


def _resolve_tools_mentioned(raw: Any) -> str:
    values = _parse_json_string_array(raw, "ideas.tools_mentioned")
    normalized = [entry for entry in values if entry]
    return ", ".join(normalized)


def _resolve_draft_script(raw: Any) -> str:
    if not isinstance(raw, str):
        raise RowMappingError("scripts.script_sections must be a JSON string")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RowMappingError("scripts.script_sections must be a JSON array") from exc

    if not isinstance(parsed, list) or len(parsed) != len(EXPECTED_SCRIPT_LABELS):
        raise RowMappingError("scripts.script_sections must contain exactly 4 sections")

    texts: dict[str, str] = {}
    for index, expected_label in enumerate(EXPECTED_SCRIPT_LABELS):
        section = parsed[index]
        if not isinstance(section, dict):
            raise RowMappingError("scripts.script_sections entries must be objects")
        if set(section.keys()) != EXPECTED_SCRIPT_SECTION_KEYS:
            raise RowMappingError(
                "scripts.script_sections entries must contain exactly label and text"
            )
        label = _require_string(
            section.get("label"),
            "scripts.script_sections.label",
            allow_empty=False,
        )
        text = _require_string(
            section.get("text"),
            "scripts.script_sections.text",
            allow_empty=False,
        )
        if label != expected_label:
            raise RowMappingError("scripts.script_sections labels/order mismatch")
        texts[label] = text

    return (
        f"Hook: {texts['hook']}\n\n"
        f"Setup: {texts['setup']}\n\n"
        "Steps:\n"
        f"{texts['steps']}\n\n"
        f"CTA: {texts['cta']}"
    )


def _parse_json_string_array(raw: Any, field_name: str) -> list[str]:
    if not isinstance(raw, str):
        raise RowMappingError(f"{field_name} must be a JSON string array")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RowMappingError(f"{field_name} must be a JSON string array") from exc
    if not isinstance(parsed, list):
        raise RowMappingError(f"{field_name} must be a JSON string array")

    values: list[str] = []
    for entry in parsed:
        if not isinstance(entry, str):
            raise RowMappingError(f"{field_name} must be a JSON string array")
        values.append(entry.strip())
    return values


def _require_string(value: Any, field_name: str, *, allow_empty: bool) -> str:
    if not isinstance(value, str):
        raise RowMappingError(f"{field_name} must be a string")
    normalized = value.strip()
    if not allow_empty and not normalized:
        raise RowMappingError(f"{field_name} must be non-empty after strip()")
    return normalized


def _normalize_optional_string(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()

