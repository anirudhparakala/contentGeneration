from __future__ import annotations

import re
from pathlib import Path


PLACEHOLDER_RE = re.compile(r"\{\{([A-Z0-9_]+)\}\}")
REQUIRED_PLACEHOLDERS: frozenset[str] = frozenset(
    {
        "PLATFORM",
        "RECOMMENDED_FORMAT",
        "TITLE",
        "URL",
        "TOPIC",
        "CORE_CLAIM",
        "WORKFLOW_STEPS",
        "TOOLS_MENTIONED",
        "MONETIZATION_ANGLE",
        "METRICS_CLAIMS",
        "ASSUMPTIONS",
        "PRIOR_HOOKS",
    }
)


class PromptLoadError(RuntimeError):
    pass


class PromptRenderError(RuntimeError):
    pass


def default_script_prompt_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "prompts" / "stage_6_script.md"


def load_prompt_template(*, path: Path | None = None) -> str:
    prompt_path = path or default_script_prompt_path()
    return _read_text(prompt_path)


def render_prompt(template: str, replacements: dict[str, str]) -> str:
    if not isinstance(template, str) or not template.strip():
        raise PromptRenderError("prompt template must be a non-empty string")

    placeholders = set(PLACEHOLDER_RE.findall(template))
    _validate_placeholder_set(placeholders)

    replacement_keys = set(replacements.keys())
    if replacement_keys != REQUIRED_PLACEHOLDERS:
        missing = sorted(REQUIRED_PLACEHOLDERS - replacement_keys)
        extra = sorted(replacement_keys - REQUIRED_PLACEHOLDERS)
        details: list[str] = []
        if missing:
            details.append(f"missing replacements: {', '.join(missing)}")
        if extra:
            details.append(f"extra replacements: {', '.join(extra)}")
        raise PromptRenderError("; ".join(details) or "prompt replacement keys mismatch")

    rendered = template
    for key in sorted(REQUIRED_PLACEHOLDERS):
        value = replacements[key]
        if not isinstance(value, str):
            raise PromptRenderError(f"replacement {key} must be a string")
        rendered = rendered.replace(f"{{{{{key}}}}}", value)

    unresolved = set(PLACEHOLDER_RE.findall(rendered))
    if unresolved:
        raise PromptRenderError(
            f"prompt has unresolved placeholders: {', '.join(sorted(unresolved))}"
        )
    return rendered


def _validate_placeholder_set(placeholders: set[str]) -> None:
    if placeholders == REQUIRED_PLACEHOLDERS:
        return
    missing = sorted(REQUIRED_PLACEHOLDERS - placeholders)
    extra = sorted(placeholders - REQUIRED_PLACEHOLDERS)
    details: list[str] = []
    if missing:
        details.append(f"missing placeholders: {', '.join(missing)}")
    if extra:
        details.append(f"extra placeholders: {', '.join(extra)}")
    raise PromptRenderError("; ".join(details) or "prompt placeholder set mismatch")


def _read_text(path: Path) -> str:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PromptLoadError(f"failed loading prompt file: {path}") from exc
    if not text.strip():
        raise PromptLoadError(f"prompt file is empty: {path}")
    return text

