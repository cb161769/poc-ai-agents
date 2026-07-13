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
from agent_loop import (
    JSON_CORRECTION_MESSAGE,
    _final_text_with_json_retry,
    _ollama_model_available,
    _ollama_response_to_blocks,
    _post_with_retry,
    call_with_fallback,
    compact_old_tool_results,
)


def _fake_response(status_code: int) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    if status_code >= 400:
        request = httpx.Request("POST", "http://test")
        resp.raise_for_status.side_effect = httpx.HTTPStatusError("error", request=request, response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


def test_select_backend_prefers_anthropic_when_key_set(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")
    assert agent_loop._select_backend() == "anthropic"


def test_select_backend_falls_back_to_ollama_when_reachable(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(agent_loop.httpx, "get", MagicMock(return_value=_fake_response(200)))
    assert agent_loop._select_backend() == "ollama"


def test_select_backend_returns_none_when_nothing_available(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(agent_loop.httpx, "get", MagicMock(side_effect=httpx.ConnectError("no ollama")))
    assert agent_loop._select_backend() == "none"


def test_backend_available_false_when_over_budget(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")
    monkeypatch.setattr(agent_loop, "is_within_budget", lambda backend: False)
    assert agent_loop._backend_available("anthropic") is False


def test_backend_available_true_when_within_budget(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")
    monkeypatch.setattr(agent_loop, "is_within_budget", lambda backend: True)
    assert agent_loop._backend_available("anthropic") is True


def test_select_backend_respects_custom_priority_order(monkeypatch):
    """LLM_BACKEND_PRIORITY="ollama,anthropic" -- si Ollama esta alcanzable,
    gana aunque ANTHROPIC_API_KEY tambien este seteada.
    """
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")
    monkeypatch.setenv("LLM_BACKEND_PRIORITY", "ollama,anthropic")
    monkeypatch.setattr(agent_loop.httpx, "get", MagicMock(return_value=_fake_response(200)))
    assert agent_loop._select_backend() == "ollama"


def test_call_model_turn_anthropic_marks_system_and_tools_cacheable(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")
    captured = {}

    async def fake_post(url, **kwargs):
        captured["json"] = kwargs["json"]
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {
            "content": [{"type": "text", "text": "ok"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 5, "output_tokens": 2, "cache_read_input_tokens": 100},
        }
        return resp

    client = MagicMock()
    client.post = fake_post

    tools = [
        {"name": "read_file", "description": "lee un archivo", "input_schema": {"type": "object"}},
        {"name": "grep_search", "description": "busca texto", "input_schema": {"type": "object"}},
    ]

    blocks, stop_reason, usage = asyncio.run(
        agent_loop._call_model_turn(client, "anthropic", [{"role": "user", "content": "hola"}], tools, "system prompt real")
    )

    assert captured["json"]["system"] == [
        {"type": "text", "text": "system prompt real", "cache_control": {"type": "ephemeral"}}
    ]
    # Solo el ULTIMO tool lleva cache_control (Anthropic cachea el prefijo
    # completo hasta el ultimo breakpoint marcado, no hace falta marcar cada uno).
    assert "cache_control" not in captured["json"]["tools"][0]
    assert captured["json"]["tools"][-1]["cache_control"] == {"type": "ephemeral"}
    assert usage["cache_read_input_tokens"] == 100


def test_call_model_turn_anthropic_skips_tools_cache_control_when_no_tools(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")
    captured = {}

    async def fake_post(url, **kwargs):
        captured["json"] = kwargs["json"]
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"content": [], "stop_reason": "end_turn", "usage": {}}
        return resp

    client = MagicMock()
    client.post = fake_post

    asyncio.run(agent_loop._call_model_turn(client, "anthropic", [], [], "system prompt real"))

    assert "tools" not in captured["json"]


def test_post_with_retry_retries_on_503_then_succeeds(monkeypatch):
    monkeypatch.setattr(agent_loop.asyncio, "sleep", AsyncMock())
    client = MagicMock()
    client.post = AsyncMock(side_effect=[_fake_response(503), _fake_response(200)])

    resp = asyncio.run(_post_with_retry(client, "anthropic", "http://test", json={}))

    assert resp.status_code == 200
    assert client.post.call_count == 2


def test_post_with_retry_retries_on_529_for_anthropic(monkeypatch):
    """529 ("overloaded") es especifico de Anthropic -- confirma que la
    politica por backend lo incluye, a diferencia del set global anterior.
    """
    monkeypatch.setattr(agent_loop.asyncio, "sleep", AsyncMock())
    client = MagicMock()
    client.post = AsyncMock(side_effect=[_fake_response(529), _fake_response(200)])

    resp = asyncio.run(_post_with_retry(client, "anthropic", "http://test", json={}))

    assert resp.status_code == 200
    assert client.post.call_count == 2


def test_post_with_retry_does_not_retry_on_401(monkeypatch):
    monkeypatch.setattr(agent_loop.asyncio, "sleep", AsyncMock())
    client = MagicMock()
    client.post = AsyncMock(return_value=_fake_response(401))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_post_with_retry(client, "anthropic", "http://test", json={}))

    assert client.post.call_count == 1


def test_post_with_retry_gives_up_after_max_retries(monkeypatch):
    monkeypatch.setattr(agent_loop.asyncio, "sleep", AsyncMock())
    client = MagicMock()
    client.post = AsyncMock(return_value=_fake_response(503))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(_post_with_retry(client, "anthropic", "http://test", json={}))

    assert client.post.call_count == agent_loop.RETRY_POLICY_PER_BACKEND["anthropic"]["max_retries"] + 1


def test_post_with_retry_retries_on_connection_error_then_succeeds(monkeypatch):
    monkeypatch.setattr(agent_loop.asyncio, "sleep", AsyncMock())
    client = MagicMock()
    client.post = AsyncMock(side_effect=[httpx.ConnectError("boom"), _fake_response(200)])

    resp = asyncio.run(_post_with_retry(client, "anthropic", "http://test", json={}))

    assert resp.status_code == 200
    assert client.post.call_count == 2


def test_call_model_turn_ollama_uses_model_override_when_passed(monkeypatch):
    captured = {}

    async def fake_post(url, **kwargs):
        captured["json"] = kwargs["json"]
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"message": {"content": "ok"}, "prompt_eval_count": 1, "eval_count": 1}
        return resp

    client = MagicMock()
    client.post = fake_post

    asyncio.run(
        agent_loop._call_model_turn(
            client, "ollama", [{"role": "user", "content": "hola"}], [], "sys", ollama_model="qwen2.5-coder:7b"
        )
    )

    assert captured["json"]["model"] == "qwen2.5-coder:7b"


def test_call_model_turn_ollama_falls_back_to_global_model_without_override(monkeypatch):
    captured = {}

    async def fake_post(url, **kwargs):
        captured["json"] = kwargs["json"]
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"message": {"content": "ok"}, "prompt_eval_count": 1, "eval_count": 1}
        return resp

    client = MagicMock()
    client.post = fake_post

    asyncio.run(agent_loop._call_model_turn(client, "ollama", [{"role": "user", "content": "hola"}], [], "sys"))

    assert captured["json"]["model"] == agent_loop.OLLAMA_MODEL


def _fake_ollama_post_capturing(captured):
    async def fake_post(url, **kwargs):
        captured["json"] = kwargs["json"]
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"message": {"content": "ok"}, "prompt_eval_count": 1, "eval_count": 1}
        return resp

    return fake_post


def test_call_model_turn_ollama_sends_num_ctx_option(monkeypatch):
    captured = {}
    client = MagicMock()
    client.post = _fake_ollama_post_capturing(captured)

    asyncio.run(agent_loop._call_model_turn(client, "ollama", [{"role": "user", "content": "hola"}], [], "sys"))

    assert captured["json"]["options"]["num_ctx"] == agent_loop.OLLAMA_NUM_CTX


def test_call_model_turn_ollama_respects_num_ctx_env_override(monkeypatch):
    monkeypatch.setattr(agent_loop, "OLLAMA_NUM_CTX", 16384)
    captured = {}
    client = MagicMock()
    client.post = _fake_ollama_post_capturing(captured)

    asyncio.run(agent_loop._call_model_turn(client, "ollama", [{"role": "user", "content": "hola"}], [], "sys"))

    assert captured["json"]["options"]["num_ctx"] == 16384


def test_call_model_turn_ollama_force_json_sets_format(monkeypatch):
    captured = {}
    client = MagicMock()
    client.post = _fake_ollama_post_capturing(captured)

    asyncio.run(
        agent_loop._call_model_turn(client, "ollama", [{"role": "user", "content": "hola"}], [], "sys", force_json=True)
    )

    assert captured["json"]["format"] == "json"


def test_call_model_turn_ollama_without_force_json_omits_format(monkeypatch):
    captured = {}
    client = MagicMock()
    client.post = _fake_ollama_post_capturing(captured)

    asyncio.run(agent_loop._call_model_turn(client, "ollama", [{"role": "user", "content": "hola"}], [], "sys"))

    assert "format" not in captured["json"]


def test_final_text_with_json_retry_ollama_forces_json_and_drops_tools(monkeypatch):
    captured = {}

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt, **kwargs):
        captured["tools"] = tools
        captured["force_json"] = kwargs.get("force_json")
        return [{"type": "text", "text": '{"ok": true}'}], "end_turn", {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(agent_loop, "_call_model_turn", fake_call_model_turn)

    messages = [{"role": "user", "content": "hola"}]
    asyncio.run(
        _final_text_with_json_retry(
            client=None, backend="ollama", messages=messages,
            tools=[{"name": "some_tool"}], system_prompt="sys",
        )
    )

    assert captured["tools"] == []
    assert captured["force_json"] is True


def test_ollama_response_to_blocks_warns_and_defaults_to_empty_args_on_malformed_json(monkeypatch):
    """Antes esto caia a {} en silencio -- una tool real terminaba llamada
    con argumentos vacios sin ningun rastro de que el parseo fallo.
    log_utils.get_logger() usa propagate=False, asi que caplog no lo
    captura -- se mockea logger.warning directo (mismo patron que
    test_orchestration.py::test_comment_jira_logs_instead_of_raising_on_failure).
    """
    message = {
        "content": "",
        "tool_calls": [{"function": {"name": "write_file", "arguments": "{esto no es json valido"}}],
    }
    warnings = []
    monkeypatch.setattr(agent_loop.logger, "warning", lambda msg: warnings.append(msg))

    blocks, stop_reason = _ollama_response_to_blocks(message)

    assert stop_reason == "tool_use"
    assert blocks[0]["input"] == {}
    assert any("write_file" in w and "no son JSON valido" in w for w in warnings)


def test_ollama_response_to_blocks_parses_valid_string_arguments_without_warning(monkeypatch):
    message = {
        "content": "",
        "tool_calls": [{"function": {"name": "read_file", "arguments": '{"path": "a.py"}'}}],
    }
    warnings = []
    monkeypatch.setattr(agent_loop.logger, "warning", lambda msg: warnings.append(msg))

    blocks, _stop_reason = _ollama_response_to_blocks(message)

    assert blocks[0]["input"] == {"path": "a.py"}
    assert warnings == []


def test_ollama_model_available_true_when_exact_name_present(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": "llama3.1:latest"}, {"name": "qwen2.5-coder:7b"}]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)

    assert _ollama_model_available("qwen2.5-coder:7b") is True


def test_ollama_model_available_true_when_bare_name_matches_a_tag(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": "llama3.1:latest"}]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)

    assert _ollama_model_available("llama3.1") is True


def test_ollama_model_available_false_when_model_missing(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": "llama3.1:latest"}]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)

    assert _ollama_model_available("qwen2.5-coder:7b") is False


def test_ollama_model_available_false_when_server_unreachable(monkeypatch):
    def fake_get(*a, **k):
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(agent_loop.httpx, "get", fake_get)

    assert _ollama_model_available("llama3.1") is False


def test_call_with_fallback_falls_back_to_next_backend_on_failure(monkeypatch):
    monkeypatch.setenv("LLM_BACKEND_PRIORITY", "anthropic,ollama")
    monkeypatch.setattr(agent_loop, "_backend_available", lambda backend: True)

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt, **kwargs):
        if backend == "anthropic":
            raise httpx.HTTPStatusError("error", request=httpx.Request("POST", "http://test"), response=_fake_response(401))
        return [{"type": "text", "text": "ok"}], "end_turn", {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(agent_loop, "_call_model_turn", fake_call_model_turn)

    blocks, stop_reason, usage, backend_used = asyncio.run(
        call_with_fallback(client=None, messages=[], tools=[], system_prompt="sys")
    )

    assert backend_used == "ollama"
    assert stop_reason == "end_turn"


def test_call_with_fallback_reraises_when_all_backends_fail(monkeypatch):
    monkeypatch.setenv("LLM_BACKEND_PRIORITY", "anthropic,ollama")
    monkeypatch.setattr(agent_loop, "_backend_available", lambda backend: True)

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt, **kwargs):
        raise RuntimeError(f"{backend} esta caido")

    monkeypatch.setattr(agent_loop, "_call_model_turn", fake_call_model_turn)

    with pytest.raises(RuntimeError, match="esta caido"):
        asyncio.run(call_with_fallback(client=None, messages=[], tools=[], system_prompt="sys"))


def test_call_with_fallback_raises_when_no_backend_available(monkeypatch):
    monkeypatch.setenv("LLM_BACKEND_PRIORITY", "anthropic,ollama")
    monkeypatch.setattr(agent_loop, "_backend_available", lambda backend: False)

    with pytest.raises(RuntimeError, match="ningun backend"):
        asyncio.run(call_with_fallback(client=None, messages=[], tools=[], system_prompt="sys"))


def _make_turn(turn_index: int, tool_name: str) -> tuple:
    tool_id = f"call_{turn_index}"
    assistant = {"role": "assistant", "content": [{"type": "tool_use", "id": tool_id, "name": tool_name, "input": {}}]}
    user = {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tool_id, "content": f"resultado real del turno {turn_index}"}]}
    return assistant, user


def _tool_result_contents(messages: list) -> list:
    return [
        block["content"]
        for m in messages
        if m.get("role") == "user" and isinstance(m.get("content"), list)
        for block in m["content"]
        if block.get("type") == "tool_result"
    ]


def test_compact_old_tool_results_collapses_old_read_only_results():
    messages = [{"role": "user", "content": "prompt inicial"}]
    for i in range(5):
        assistant, user = _make_turn(i, "read_file")
        messages.append(assistant)
        messages.append(user)

    compact_old_tool_results(messages, {"read_file"}, keep_last_n_turns=3)

    contents = _tool_result_contents(messages)
    assert "colapsado" in contents[0]
    assert "colapsado" in contents[1]
    assert contents[2] == "resultado real del turno 2"
    assert contents[3] == "resultado real del turno 3"
    assert contents[4] == "resultado real del turno 4"


def test_compact_old_tool_results_never_touches_write_tools():
    messages = [{"role": "user", "content": "prompt inicial"}]
    for i in range(5):
        assistant, user = _make_turn(i, "write_file")
        messages.append(assistant)
        messages.append(user)

    compact_old_tool_results(messages, {"read_file"}, keep_last_n_turns=3)

    assert all("colapsado" not in c for c in _tool_result_contents(messages))


def test_compact_old_tool_results_noop_when_not_enough_turns_yet():
    messages = [{"role": "user", "content": "prompt inicial"}]
    for i in range(2):
        assistant, user = _make_turn(i, "read_file")
        messages.append(assistant)
        messages.append(user)

    compact_old_tool_results(messages, {"read_file"}, keep_last_n_turns=3)

    assert all("colapsado" not in c for c in _tool_result_contents(messages))


def test_final_text_with_json_retry_appends_messages_and_returns_new_text(monkeypatch):
    async def fake_call_model_turn(client, backend, messages, tools, system_prompt, **kwargs):
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
