from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


EXPECTED_SCRIPT_LABELS: tuple[str, ...] = ("hook", "setup", "steps", "cta")
EXPECTED_SCRIPT_SECTION_KEYS: frozenset[str] = frozenset({"label", "text"})


class RowMappingError(ValueError):
    pass


@dataclass(frozen=True)
class DeliveryMessage:
    item_id: str
    text: str


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def to_utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_non_bool_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def build_delivery_message(
    raw: Mapping[str, Any],
    *,
    timestamp: str,
    max_script_chars: int,
) -> DeliveryMessage:
    item_id = _require_string(raw.get("item_id"), "ideas.item_id", allow_empty=False)
    topic = _require_string(raw.get("topic"), "ideas.topic", allow_empty=True)
    url = _require_string(raw.get("url"), "ideas.url", allow_empty=False)
    hook = _require_string(raw.get("primary_hook"), "scripts.primary_hook", allow_empty=False)

    viral_rating_raw = raw.get("viral_rating")
    if not is_non_bool_int(viral_rating_raw):
        raise RowMappingError("ideas.viral_rating must be a non-boolean integer")
    viral_rating = int(viral_rating_raw)

    script_text = _build_script_text(raw.get("script_sections"), max_script_chars=max_script_chars)
    creator = resolve_creator(raw.get("creator"), raw.get("source_name"))

    message = (
        f"{viral_rating}/10 - {topic}\n"
        f"Creator: {creator}\n"
        f"Hook: {hook}\n"
        f"Link: {url}\n"
        "Script:\n"
        f"{script_text}\n"
        f"Generated at {timestamp}. Item: {item_id}"
    )
    return DeliveryMessage(item_id=item_id, text=message)


def resolve_creator(creator: Any, source_name: Any) -> str:
    creator_value = _normalize_optional_string(creator)
    if creator_value:
        return creator_value

    source_name_value = _normalize_optional_string(source_name)
    if source_name_value:
        return source_name_value
    return "unknown"


def _build_script_text(raw: Any, *, max_script_chars: int) -> str:
    if not isinstance(raw, str):
        raise RowMappingError("scripts.script_sections must be a JSON array string")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RowMappingError("scripts.script_sections must be a JSON array string") from exc

    if not isinstance(parsed, list) or len(parsed) != len(EXPECTED_SCRIPT_LABELS):
        raise RowMappingError("scripts.script_sections must contain exactly 4 sections")

    text_by_label: dict[str, str] = {}
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
        text_by_label[label] = text

    script_text = (
        f"Hook: {text_by_label['hook']}\n\n"
        f"Setup: {text_by_label['setup']}\n\n"
        "Steps:\n"
        f"{text_by_label['steps']}\n\n"
        f"CTA: {text_by_label['cta']}"
    )
    if len(script_text) > max_script_chars:
        script_text = script_text[:max_script_chars] + "...(truncated)"
    return script_text


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

