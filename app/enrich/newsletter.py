from __future__ import annotations

from .fetch import FetchError, HttpPolicy, fetch_url_bytes
from .models import EvidenceMeta, EvidenceSnippet, normalize_text


class NewsletterFetchError(RuntimeError):
    pass


class NewsletterExtractError(RuntimeError):
    pass


class NewsletterTextTooShortError(RuntimeError):
    pass


def enrich_newsletter(*, url: str, http_policy: HttpPolicy) -> tuple[str, list[EvidenceSnippet]]:
    try:
        payload = fetch_url_bytes(url, http_policy)
    except FetchError as exc:
        raise NewsletterFetchError(str(exc)) from exc

    try:
        import trafilatura
    except Exception as exc:
        raise NewsletterExtractError("trafilatura import failed") from exc

    try:
        extracted = trafilatura.extract(
            payload,
            output_format="txt",
            include_comments=False,
            include_tables=False,
            favor_precision=True,
            favor_recall=False,
            no_fallback=False,
        )
    except Exception as exc:
        raise NewsletterExtractError(str(exc)) from exc

    if extracted is None:
        raise NewsletterExtractError("trafilatura returned None")
    if not isinstance(extracted, str):
        raise NewsletterExtractError("trafilatura returned non-string output")

    normalized = normalize_text(extracted)
    if len(normalized) < 500:
        raise NewsletterTextTooShortError("newsletter text shorter than 500 chars")

    snippets = _build_article_snippets(normalized)
    return normalized, snippets


def _build_article_snippets(enriched_text: str) -> list[EvidenceSnippet]:
    snippets: list[EvidenceSnippet] = []
    windows = ((0, 240), (240, 480))
    for start, end in windows:
        chunk = enriched_text[start:end]
        if not chunk:
            continue
        snippets.append(
            EvidenceSnippet(
                text=chunk,
                meta=EvidenceMeta(type="article", offset=start, timestamp=None),
            )
        )
    return snippets
