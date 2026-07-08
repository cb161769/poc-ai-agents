"""Unit tests for agent_loop.py's shared retry machinery: bounded retries
on transient HTTP failures (_post_with_retry) and the one-shot JSON
correction retry (_final_text_with_json_retry). No real network calls --
httpx.AsyncClient.post and _call_model_turn are mocked.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import agent_loop
from agent_loop import JSON_CORRECTION_MESSAGE, _final_text_with_json_retry, _post_with_retry


def _fake_response(status_code: int) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    if status_code >= 400:
        request = httpx.Request("POST", "http://test")
        resp.raise_for_status.side_effect = httpx.HTTPStatusError("error", request=request, response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


def test_post_with_retry_retries_on_503_then_succeeds(monkeypatch):
    monkeypatch.setattr(agent_loop.asyncio, "sleep", AsyncMock())
    client = MagicMock()
    client.post = AsyncMock(side_effect=[_fake_response(503), _fake_response(200)])

    resp = asyncio.run(_post_with_retry(client, "http://test", json={}))

    assert resp.status_code == 200
    assert client.post.call_count == 2


def test_post_with_retry_does_not_retry_on_401(monkeypatch):
    monkeypatch.setattr(agent_loop.asyncio, "sleep", AsyncMock())
    client = MagicMock()
    client.post = AsyncMock(return_value=_fake_response(401))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_post_with_retry(client, "http://test", json={}))

    assert client.post.call_count == 1


def test_post_with_retry_gives_up_after_max_retries(monkeypatch):
    monkeypatch.setattr(agent_loop.asyncio, "sleep", AsyncMock())
    client = MagicMock()
    client.post = AsyncMock(return_value=_fake_response(503))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_post_with_retry(client, "http://test", json={}))

    assert client.post.call_count == agent_loop._MAX_TRANSIENT_RETRIES + 1


def test_post_with_retry_retries_on_connection_error_then_succeeds(monkeypatch):
    monkeypatch.setattr(agent_loop.asyncio, "sleep", AsyncMock())
    client = MagicMock()
    client.post = AsyncMock(side_effect=[httpx.ConnectError("boom"), _fake_response(200)])

    resp = asyncio.run(_post_with_retry(client, "http://test", json={}))

    assert resp.status_code == 200
    assert client.post.call_count == 2


def test_final_text_with_json_retry_appends_messages_and_returns_new_text(monkeypatch):
    async def fake_call_model_turn(client, backend, messages, tools, system_prompt):
        return [{"type": "text", "text": '{"ok": true}'}], "end_turn", {"input_tokens": 5, "output_tokens": 3}

    monkeypatch.setattr(agent_loop, "_call_model_turn", fake_call_model_turn)

    messages = [{"role": "user", "content": "hola"}]
    final_text, usage = asyncio.run(
        _final_text_with_json_retry(client=None, backend="anthropic", messages=messages, tools=[], system_prompt="sys")
    )

    assert final_text == '{"ok": true}'
    assert usage == {"input_tokens": 5, "output_tokens": 3}
    assert messages[-2] == {"role": "user", "content": JSON_CORRECTION_MESSAGE}
    assert messages[-1]["role"] == "assistant"
