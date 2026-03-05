from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


PLACEHOLDER_RE = re.compile(r"\{\{[A-Z0-9_]+\}\}")


class PromptLoadError(RuntimeError):
    pass


class PromptRenderError(RuntimeError):
    pass


@dataclass(frozen=True)
class PromptTemplates:
    extract: str
    score: str


def default_extract_prompt_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "prompts" / "stage_5_extract.md"


def default_score_prompt_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "prompts" / "stage_5_score.md"


def load_prompt_templates(
    *,
    extract_path: Path | None = None,
    score_path: Path | None = None,
) -> PromptTemplates:
    extract_file = extract_path or default_extract_prompt_path()
    score_file = score_path or default_score_prompt_path()
    return PromptTemplates(
        extract=_read_text(extract_file),
        score=_read_text(score_file),
    )


def render_prompt(template: str, replacements: dict[str, str]) -> str:
    if not isinstance(template, str) or not template:
        raise PromptRenderError("prompt template must be a non-empty string")

    rendered = template
    for key, value in replacements.items():
        token = f"{{{{{key}}}}}"
        if token not in rendered:
            raise PromptRenderError(f"prompt is missing required placeholder: {token}")
        rendered = rendered.replace(token, value)

    unresolved = PLACEHOLDER_RE.findall(rendered)
    if unresolved:
        raise PromptRenderError(f"prompt has unresolved placeholders: {', '.join(sorted(set(unresolved)))}")
    return rendered


def _read_text(path: Path) -> str:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PromptLoadError(f"failed loading prompt file: {path}") from exc
    if not text.strip():
        raise PromptLoadError(f"prompt file is empty: {path}")
    return text

