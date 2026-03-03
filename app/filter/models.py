from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


REQUIRED_FIELDS = (
    "item_id",
    "source_type",
    "source_id",
    "source_name",
    "creator",
    "title",
    "url",
    "published_at",
    "fetched_at",
)
TOKEN_PATTERN = r"[a-z0-9]+(?:\.[a-z0-9]+)*"
TOKEN_EXTRACT_RE = re.compile(TOKEN_PATTERN)
TOKEN_FULL_RE = re.compile(rf"^{TOKEN_PATTERN}$")
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class CandidateItem:
    item_id: str
    source_type: str
    source_id: str
    source_name: str
    creator: str
    title: str
    url: str
    published_at: str
    fetched_at: str
    content_text: str
    relevance_score: int
    matched_keywords: list[str]
    scored_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class KeywordGroup:
    weight: int
    terms: tuple[str, ...]


def utc_now_z() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_term(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value.strip().lower())


def collapse_whitespace(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value)


def canonicalize_term(term: str) -> str:
    if term == "freelanc":
        return "freelanc*"
    return term


def compile_keyword_groups(raw_groups: Any) -> dict[str, KeywordGroup]:
    if not isinstance(raw_groups, dict) or not raw_groups:
        raise ValueError("stage_3_filter.keyword_groups must be a non-empty mapping")

    groups: dict[str, KeywordGroup] = {}
    for name, raw_group in raw_groups.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("stage_3_filter.keyword_groups keys must be non-empty strings")
        if not isinstance(raw_group, dict):
            raise ValueError(f"stage_3_filter.keyword_groups.{name} must be a mapping")

        raw_weight = raw_group.get("weight")
        if not _is_non_bool_int(raw_weight) or int(raw_weight) < 1:
            raise ValueError(f"stage_3_filter.keyword_groups.{name}.weight must be an integer >= 1")
        weight = int(raw_weight)

        raw_terms = raw_group.get("terms")
        if not isinstance(raw_terms, list) or not raw_terms:
            raise ValueError(f"stage_3_filter.keyword_groups.{name}.terms must be a non-empty list")

        normalized_terms: list[str] = []
        saw_non_empty = False
        for raw_term in raw_terms:
            if not isinstance(raw_term, str):
                raise ValueError(f"stage_3_filter.keyword_groups.{name}.terms entries must be strings")
            normalized = normalize_term(raw_term)
            if not normalized:
                continue
            saw_non_empty = True
            canonical = canonicalize_term(normalized)
            _validate_term(canonical, group_name=name)
            normalized_terms.append(canonical)

        if not saw_non_empty:
            raise ValueError(
                f"stage_3_filter.keyword_groups.{name}.terms must include at least one non-empty value"
            )

        deduped_terms = tuple(dict.fromkeys(normalized_terms).keys())
        if not deduped_terms:
            raise ValueError(f"stage_3_filter.keyword_groups.{name}.terms produced no valid terms")

        groups[name.strip()] = KeywordGroup(weight=weight, terms=deduped_terms)

    return groups


def score_relevance(
    *,
    title: str,
    body_text: str,
    keyword_groups: Mapping[str, KeywordGroup],
) -> tuple[int, list[str]]:
    score_text = f"{title} {body_text}".lower()
    score_text_ws = collapse_whitespace(score_text)
    tokens = set(TOKEN_EXTRACT_RE.findall(score_text))

    matched_term_weights: dict[str, int] = {}
    for group in keyword_groups.values():
        for term in group.terms:
            if _term_matches(term=term, score_text_ws=score_text_ws, tokens=tokens):
                current = matched_term_weights.get(term)
                if current is None or group.weight > current:
                    matched_term_weights[term] = group.weight

    matched_keywords = sorted(matched_term_weights.keys())
    relevance_score = sum(matched_term_weights.values())
    return relevance_score, matched_keywords


def normalize_required_fields(row: Mapping[str, Any]) -> dict[str, str] | None:
    normalized: dict[str, str] = {}
    for field in REQUIRED_FIELDS:
        value = row.get(field)
        if not isinstance(value, str):
            return None
        trimmed = value.strip()
        if not trimmed:
            return None
        normalized[field] = trimmed
    return normalized


def select_body_text(*, summary: Any, content_text: Any) -> str:
    summary_text = summary.strip() if isinstance(summary, str) else ""
    content_text_str = content_text.strip() if isinstance(content_text, str) else ""
    if content_text_str:
        return content_text_str
    if summary_text:
        return summary_text
    return ""


def _validate_term(term: str, *, group_name: str) -> None:
    if "*" in term:
        if term.count("*") != 1 or not term.endswith("*"):
            raise ValueError(
                f"stage_3_filter.keyword_groups.{group_name}.terms invalid wildcard placement: {term}"
            )
        if " " in term:
            raise ValueError(
                f"stage_3_filter.keyword_groups.{group_name}.terms wildcard terms cannot contain spaces: {term}"
            )
        base = term[:-1]
        if not TOKEN_FULL_RE.fullmatch(base):
            raise ValueError(
                f"stage_3_filter.keyword_groups.{group_name}.terms wildcard base token invalid: {term}"
            )
        return

    if " " in term:
        tokens = term.split(" ")
        if any(not TOKEN_FULL_RE.fullmatch(token) for token in tokens):
            raise ValueError(
                f"stage_3_filter.keyword_groups.{group_name}.terms phrase token invalid: {term}"
            )
        return

    if not TOKEN_FULL_RE.fullmatch(term):
        raise ValueError(f"stage_3_filter.keyword_groups.{group_name}.terms token invalid: {term}")


def _term_matches(*, term: str, score_text_ws: str, tokens: set[str]) -> bool:
    if " " in term:
        return term in score_text_ws
    if term.endswith("*"):
        prefix = term[:-1]
        return any(token.startswith(prefix) for token in tokens)
    return term in tokens


def _is_non_bool_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)
