"""Unit tests for retry_utils.retry_call(): pure retry/backoff logic, no
real network calls -- httpx exceptions are constructed directly.
"""
from unittest.mock import patch

import httpx
import pytest

from retry_utils import retry_call


def _http_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("error", request=request, response=response)


def test_retry_call_returns_on_first_success():
    calls = []

    def fn():
        calls.append(1)
        return "ok"

    with patch("retry_utils.time.sleep") as mock_sleep:
        assert retry_call(fn) == "ok"

    assert len(calls) == 1
    mock_sleep.assert_not_called()


def test_retry_call_retries_on_connect_error_then_succeeds():
    attempts = {"n": 0}

    def fn():
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise httpx.ConnectError("connection refused", request=httpx.Request("GET", "https://example.com"))
        return "ok"

    with patch("retry_utils.time.sleep") as mock_sleep:
        assert retry_call(fn) == "ok"

    assert attempts["n"] == 2
    mock_sleep.assert_called_once_with(1)


def test_retry_call_retries_on_retryable_status_code():
    attempts = {"n": 0}

    def fn():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise _http_status_error(503)
        return "ok"

    with patch("retry_utils.time.sleep"):
        assert retry_call(fn) == "ok"

    assert attempts["n"] == 3


def test_retry_call_does_not_retry_non_retryable_status_code():
    attempts = {"n": 0}

    def fn():
        attempts["n"] += 1
        raise _http_status_error(401)

    with patch("retry_utils.time.sleep") as mock_sleep:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            retry_call(fn)

    assert attempts["n"] == 1
    assert exc_info.value.response.status_code == 401
    mock_sleep.assert_not_called()


def test_retry_call_raises_last_exception_after_exhausting_retries():
    def fn():
        raise httpx.TimeoutException("timed out", request=httpx.Request("GET", "https://example.com"))

    with patch("retry_utils.time.sleep"):
        with pytest.raises(httpx.TimeoutException):
            retry_call(fn, max_retries=2)


def test_retry_call_respects_custom_max_retries():
    attempts = {"n": 0}

    def fn():
        attempts["n"] += 1
        raise _http_status_error(500)

    with patch("retry_utils.time.sleep"):
        with pytest.raises(httpx.HTTPStatusError):
            retry_call(fn, max_retries=0)

    assert attempts["n"] == 1
