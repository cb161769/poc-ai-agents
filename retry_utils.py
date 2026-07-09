"""Shared bounded retry/backoff for outbound HTTP calls to real external
services (Jira, SonarQube, Figma) -- the same pattern agent_loop.py's
_post_with_retry() already uses for the Anthropic/Ollama backends, extracted
here so every real network dependency in the pipeline retries the same way
instead of one transient blip (a dropped connection, a 503 while a service
is warming up) killing the whole run.

Only used for idempotent reads (GET). Side-effecting writes (creating a
Jira comment/ticket, transitioning a ticket) are deliberately NOT retried
with this helper -- a client-side timeout doesn't mean the request failed
server-side, and retrying a POST that actually went through would risk a
duplicate comment or ticket, which is worse than a single clear failure.
"""
import time
from typing import Callable, TypeVar

import httpx

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_DEFAULT_MAX_RETRIES = 2
_DEFAULT_BACKOFF_SECONDS = [1, 2]

T = TypeVar("T")


def retry_call(fn: Callable[[], T], max_retries: int = _DEFAULT_MAX_RETRIES, backoff_seconds: list = None) -> T:
    """Calls fn() and retries on transient failures: connection/timeout
    errors, or an httpx.HTTPStatusError whose response status is in
    _RETRYABLE_STATUS_CODES. Any other exception (4xx auth/validation
    errors, malformed requests, non-httpx exceptions) propagates on the
    first attempt -- retrying a bad token or a bad request never helps.
    """
    backoff = backoff_seconds if backoff_seconds is not None else _DEFAULT_BACKOFF_SECONDS
    last_exc: Exception = None

    for attempt in range(max_retries + 1):
        try:
            return fn()
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last_exc = exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code not in _RETRYABLE_STATUS_CODES:
                raise
            last_exc = exc

        if attempt < max_retries:
            time.sleep(backoff[min(attempt, len(backoff) - 1)])

    raise last_exc
