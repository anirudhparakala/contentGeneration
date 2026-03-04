from __future__ import annotations

from dataclasses import dataclass

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed


class FetchError(RuntimeError):
    pass


class RetryableFetchError(FetchError):
    pass


@dataclass(frozen=True)
class HttpPolicy:
    user_agent: str
    connect_timeout_s: int
    read_timeout_s: int
    max_response_bytes: int
    max_attempts: int


def fetch_url_bytes(url: str, policy: HttpPolicy) -> bytes:
    @retry(
        reraise=True,
        stop=stop_after_attempt(policy.max_attempts),
        wait=wait_fixed(1),
        retry=retry_if_exception(_should_retry_exception),
    )
    def _do_fetch() -> bytes:
        return _fetch_once(url=url, policy=policy)

    try:
        return _do_fetch()
    except Exception as exc:
        if isinstance(exc, FetchError):
            raise
        raise FetchError(str(exc)) from exc


def _fetch_once(*, url: str, policy: HttpPolicy) -> bytes:
    timeout = (policy.connect_timeout_s, policy.read_timeout_s)
    headers = {"User-Agent": policy.user_agent}

    try:
        response = requests.get(url, headers=headers, timeout=timeout, stream=True)
    except (requests.ConnectionError, requests.Timeout) as exc:
        raise RetryableFetchError(str(exc)) from exc
    except requests.RequestException as exc:
        raise FetchError(str(exc)) from exc

    try:
        status = response.status_code
        if status == 429 or 500 <= status < 600 or status >= 600:
            raise RetryableFetchError(f"retryable HTTP status {status} for {url}")
        if 400 <= status < 500:
            raise FetchError(f"non-retryable HTTP status {status} for {url}")

        content_length = response.headers.get("Content-Length")
        if content_length is not None:
            try:
                if int(content_length) > policy.max_response_bytes:
                    raise FetchError(
                        f"response too large for {url} ({content_length} bytes > cap)"
                    )
            except ValueError:
                pass

        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_content(chunk_size=32 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > policy.max_response_bytes:
                raise FetchError(f"response exceeded size cap for {url}")
            chunks.append(chunk)

        return b"".join(chunks)
    finally:
        response.close()


def _should_retry_exception(exc: BaseException) -> bool:
    return isinstance(exc, RetryableFetchError)
