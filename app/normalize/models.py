from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from dateutil.parser import isoparse


HARD_REQUIRED_FIELDS = ("external_id", "url", "title", "published_at", "fetched_at")
SOFT_FIELDS = ("source_type", "source_id", "source_name", "creator", "summary")


@dataclass(frozen=True)
class CanonicalItem:
    item_id: str
    external_id: str
    source_type: str
    source_id: str
    source_name: str
    creator: str
    title: str
    url: str
    published_at: str
    fetched_at: str
    summary: str
    content_text: str
    raw_item_json: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NormalizeOutcome:
    item: CanonicalItem | None
    invalid_reason: str | None


def normalize_raw_item(raw: dict[str, Any]) -> NormalizeOutcome:
    invalid_field_types = _has_invalid_field_types(raw)
    if invalid_field_types:
        return NormalizeOutcome(item=None, invalid_reason="invalid_field_types")

    values = _collect_strings(raw)
    _apply_defaults(values)

    if _has_missing_required(values):
        return NormalizeOutcome(item=None, invalid_reason="missing_required_fields")

    published_at = _parse_to_utc_z(values["published_at"])
    fetched_at = _parse_to_utc_z(values["fetched_at"])
    if published_at is None or fetched_at is None:
        return NormalizeOutcome(item=None, invalid_reason="invalid_timestamps")

    source_type = values["source_type"].lower()
    if source_type not in {"newsletter", "youtube"}:
        source_type = "newsletter"

    source_id = values["source_id"] or "unknown"
    source_name = values["source_name"] or "unknown"
    creator = values["creator"] or source_name
    if not creator:
        creator = source_name

    external_id = values["external_id"]
    item_id = _item_id(source_id=source_id, external_id=external_id)
    summary = values["summary"]

    return NormalizeOutcome(
        item=CanonicalItem(
            item_id=item_id,
            external_id=external_id,
            source_type=source_type,
            source_id=source_id,
            source_name=source_name,
            creator=creator,
            title=values["title"],
            url=values["url"],
            published_at=published_at,
            fetched_at=fetched_at,
            summary=summary,
            content_text=summary,
            raw_item_json=raw,
        ),
        invalid_reason=None,
    )


def utc_now_z() -> str:
    return _to_utc_z(datetime.now(tz=timezone.utc))


def _has_invalid_field_types(raw: dict[str, Any]) -> bool:
    for key in (*HARD_REQUIRED_FIELDS, *SOFT_FIELDS):
        if key in raw and not isinstance(raw[key], str):
            return True
    return False


def _collect_strings(raw: dict[str, Any]) -> dict[str, str]:
    values: dict[str, str] = {}
    for key in HARD_REQUIRED_FIELDS:
        value = raw.get(key)
        values[key] = value.strip() if isinstance(value, str) else ""
    for key in SOFT_FIELDS:
        value = raw.get(key)
        values[key] = value.strip() if isinstance(value, str) else ""
    return values


def _apply_defaults(values: dict[str, str]) -> None:
    values["source_type"] = values["source_type"] or "newsletter"
    values["source_id"] = values["source_id"] or "unknown"
    values["source_name"] = values["source_name"] or "unknown"
    values["creator"] = values["creator"] or values["source_name"]
    values["summary"] = values["summary"] or ""


def _has_missing_required(values: dict[str, str]) -> bool:
    for key in HARD_REQUIRED_FIELDS:
        if not values[key]:
            return True
    return False


def _parse_to_utc_z(value: str) -> str | None:
    try:
        parsed = isoparse(value)
    except (TypeError, ValueError):
        return None
    return _to_utc_z(parsed)


def _to_utc_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _item_id(*, source_id: str, external_id: str) -> str:
    return hashlib.sha256(f"{source_id}|{external_id}".encode("utf-8")).hexdigest()
