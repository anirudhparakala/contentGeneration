from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

import feedparser
from dateutil import parser as date_parser


@dataclass(frozen=True)
class ParsedEntry:
    source_type: str
    source_id: str
    source_name: str
    creator: str
    title: str
    url: str
    published_at: datetime
    external_id: str
    summary: str


def parse_feed_entries(
    source_type: str,
    source_id: str,
    source_name: str,
    content: bytes,
    fetched_at: datetime,
) -> list[ParsedEntry]:
    parsed = feedparser.parse(content)
    feed_author = _read_text(parsed.feed, "author")

    results: list[ParsedEntry] = []
    for entry in parsed.entries:
        url = _extract_url(entry)
        title = _clean_text(_read_text(entry, "title"))
        summary = _clean_text(_read_text(entry, "summary") or _read_text(entry, "description"))

        published_at = (
            _extract_datetime(entry, "published_parsed")
            or _extract_datetime(entry, "updated_parsed")
            or _extract_datetime(entry, "published")
            or _extract_datetime(entry, "updated")
            or fetched_at
        )

        creator = source_name if source_type == "youtube" else (
            _clean_text(_read_text(entry, "author")) or feed_author or source_name
        )

        entry_external_id = (
            _clean_text(_read_text(entry, "id"))
            or _clean_text(_read_text(entry, "guid"))
            or (sha256(url.encode("utf-8")).hexdigest() if url else "")
        )

        results.append(
            ParsedEntry(
                source_type=source_type,
                source_id=source_id,
                source_name=source_name,
                creator=creator,
                title=title,
                url=url,
                published_at=published_at,
                external_id=entry_external_id,
                summary=summary or "",
            )
        )
    return results


def to_utc_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _extract_url(entry: Any) -> str:
    direct = _clean_text(_read_text(entry, "link"))
    if direct:
        return direct

    links = getattr(entry, "links", None) or []
    for link in links:
        href = _clean_text(_read_text(link, "href"))
        rel = _clean_text(_read_text(link, "rel"))
        if href and (not rel or rel == "alternate"):
            return href
    return ""


def _extract_datetime(entry: Any, attr: str) -> datetime | None:
    value = getattr(entry, attr, None)
    if value is None:
        return None

    if hasattr(value, "tm_year"):
        try:
            ts = calendar.timegm(value)
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, ValueError, TypeError):
            return None

    if isinstance(value, str) and value.strip():
        try:
            parsed = date_parser.parse(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except (ValueError, TypeError, OverflowError):
            return None

    return None


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split()).strip()


def _read_text(obj: Any, key: str) -> str:
    value = getattr(obj, key, "")
    if value is None:
        return ""
    return str(value)
