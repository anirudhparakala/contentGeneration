from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Protocol


LOGGER = logging.getLogger(__name__)


class LLMSetupError(RuntimeError):
    pass


class RetryableLLMError(RuntimeError):
    pass


class NonRetryableLLMError(RuntimeError):
    pass


class FatalLLMError(RuntimeError):
    pass


class LLMClient(Protocol):
    def call_json(
        self,
        *,
        prompt: str,
        schema_name: str,
        schema: dict[str, Any],
        call_label: str,
    ) -> str:
        ...


@dataclass(frozen=True)
class LLMRuntimeConfig:
    provider: str
    model: str
    temperature: float
    max_output_tokens: int
    requests_per_minute_soft: int
    request_timeout_s: int
    retry_max_attempts: int
    retry_backoff_initial_s: float
    retry_backoff_multiplier: float
    retry_backoff_max_s: float


def build_llm_client(*, config: LLMRuntimeConfig, api_key: str) -> LLMClient:
    provider = config.provider.strip().lower()
    if provider != "openai":
        raise LLMSetupError("llm.provider must be openai")
    return _OpenAIJsonClient(config=config, api_key=api_key)


class _OpenAIJsonClient:
    def __init__(
        self,
        *,
        config: LLMRuntimeConfig,
        api_key: str,
        monotonic_fn: Any = time.monotonic,
        sleep_fn: Any = time.sleep,
    ) -> None:
        try:
            from openai import OpenAI
        except Exception as exc:
            raise LLMSetupError("missing required dependency: openai") from exc
        try:
            import httpx
        except Exception as exc:
            raise LLMSetupError("missing required dependency: httpx") from exc

        self._config = config
        self._monotonic = monotonic_fn
        self._sleep = sleep_fn
        self._last_global_attempt_start: float | None = None
        # Use explicit httpx client for compatibility across openai/httpx version combinations.
        self._http_client = httpx.Client(timeout=config.request_timeout_s)
        self._client = OpenAI(
            api_key=api_key,
            timeout=config.request_timeout_s,
            http_client=self._http_client,
        )

    def call_json(
        self,
        *,
        prompt: str,
        schema_name: str,
        schema: dict[str, Any],
        call_label: str,
    ) -> str:
        previous_attempt_end: float | None = None
        for attempt in range(1, self._config.retry_max_attempts + 1):
            self._wait_for_attempt(attempt=attempt, previous_attempt_end=previous_attempt_end)
            attempt_start = self._monotonic()
            self._last_global_attempt_start = attempt_start
            try:
                return self._single_attempt(
                    prompt=prompt,
                    schema_name=schema_name,
                    schema=schema,
                )
            except FatalLLMError:
                raise
            except NonRetryableLLMError:
                raise
            except RetryableLLMError:
                previous_attempt_end = self._monotonic()
                if attempt >= self._config.retry_max_attempts:
                    raise
                LOGGER.warning(
                    "stage_6_generate retrying call=%s attempt=%s/%s",
                    call_label,
                    attempt + 1,
                    self._config.retry_max_attempts,
                )
        raise RetryableLLMError("retry loop exhausted")

    def _single_attempt(self, *, prompt: str, schema_name: str, schema: dict[str, Any]) -> str:
        try:
            response = self._client.chat.completions.create(
                model=self._config.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self._config.temperature,
                max_tokens=self._config.max_output_tokens,
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": schema_name,
                        "strict": True,
                        "schema": schema,
                    },
                },
                timeout=self._config.request_timeout_s,
            )
        except Exception as exc:
            self._raise_mapped_error(exc)
            raise AssertionError("unreachable")

        choices = getattr(response, "choices", None)
        if not isinstance(choices, list) or not choices:
            raise NonRetryableLLMError("provider returned no choices")
        first_choice = choices[0]
        message = getattr(first_choice, "message", None)
        content = getattr(message, "content", None)
        if not isinstance(content, str) or not content.strip():
            raise NonRetryableLLMError("provider returned empty message content")
        return content

    def _wait_for_attempt(self, *, attempt: int, previous_attempt_end: float | None) -> None:
        now = self._monotonic()
        rpm_spacing_s = 60.0 / float(self._config.requests_per_minute_soft)
        earliest_rpm_start = now
        if self._last_global_attempt_start is not None:
            earliest_rpm_start = self._last_global_attempt_start + rpm_spacing_s

        earliest_backoff_start = now
        if attempt >= 2 and previous_attempt_end is not None:
            delay = min(
                self._config.retry_backoff_max_s,
                self._config.retry_backoff_initial_s
                * (self._config.retry_backoff_multiplier ** (attempt - 2)),
            )
            earliest_backoff_start = previous_attempt_end + delay

        attempt_start = max(now, earliest_rpm_start, earliest_backoff_start)
        sleep_for = attempt_start - now
        if sleep_for > 0:
            self._sleep(sleep_for)

    @staticmethod
    def _raise_mapped_error(exc: Exception) -> None:
        try:
            from openai import APIConnectionError, APIStatusError, APITimeoutError, RateLimitError
        except Exception:
            raise RetryableLLMError("provider call failed") from exc

        if isinstance(exc, (APITimeoutError, APIConnectionError)):
            raise RetryableLLMError("provider transport failure") from exc
        if isinstance(exc, RateLimitError):
            raise RetryableLLMError("provider rate limited") from exc
        if isinstance(exc, APIStatusError):
            status = int(getattr(exc, "status_code", 0) or 0)
            if status in {401, 403, 404}:
                raise FatalLLMError(f"provider fatal status: {status}") from exc
            if status == 429 or status >= 500:
                raise RetryableLLMError(f"provider retryable status: {status}") from exc
            if 400 <= status < 500:
                raise NonRetryableLLMError(f"provider non-retryable status: {status}") from exc
            raise RetryableLLMError("provider status error") from exc
        raise RetryableLLMError("provider call failed") from exc

