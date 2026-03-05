from __future__ import annotations

from typing import Any, Callable

import requests


class SlackSendError(RuntimeError):
    pass


def send_slack_message(
    *,
    webhook_url: str,
    text: str,
    timeout_s: int = 20,
    post: Callable[..., Any] | None = None,
) -> None:
    sender = post or requests.post
    try:
        response = sender(
            webhook_url,
            json={"text": text},
            timeout=timeout_s,
        )
    except requests.RequestException as exc:
        raise SlackSendError(f"slack send failed: {exc}") from exc
    except Exception as exc:
        raise SlackSendError(f"slack send failed: {exc}") from exc

    status_code = getattr(response, "status_code", None)
    if not isinstance(status_code, int) or status_code < 200 or status_code >= 300:
        response_text = getattr(response, "text", "")
        detail = ""
        if isinstance(response_text, str) and response_text.strip():
            detail = f": {response_text.strip()}"
        raise SlackSendError(f"slack send returned HTTP {status_code}{detail}")

