"""Unit tests for agent_loop.py's shared retry machinery: bounded retries
on transient HTTP failures (_post_with_retry) and the one-shot JSON
correction retry (_final_text_with_json_retry). No real network calls --
httpx.AsyncClient.post and _call_model_turn are mocked.
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import agent_loop
from agent_loop import (
    JSON_CORRECTION_MESSAGE,
    _estimate_message_chars,
    _final_text_with_json_retry,
    _ollama_model_available,
    _ollama_response_to_blocks,
    _post_with_retry,
    _text_as_fallback_tool_call,
    call_with_fallback,
    compact_old_tool_results,
    init_ollama_model_state,
    maybe_switch_ollama_model,
    parse_ollama_model_candidates,
    resolve_ollama_model,
    warn_if_context_large,
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


def test_call_model_turn_anthropic_sends_temperature_zero_by_default(monkeypatch):
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

    asyncio.run(agent_loop._call_model_turn(client, "anthropic", [], [], "sys"))

    assert captured["json"]["temperature"] == 0.0


def test_call_model_turn_anthropic_force_json_prefills_and_restores_opening_brace(monkeypatch):
    """Anthropic no tiene un format:"json" nativo -- se usa la tecnica de
    prefill (arrancar la respuesta del asistente con "{"). La API devuelve
    la CONTINUACION, no el "{" -- hay que reponerlo para que el texto final
    sea JSON valido y parseable.
    """
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")
    captured = {}

    async def fake_post(url, **kwargs):
        captured["json"] = kwargs["json"]
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {
            "content": [{"type": "text", "text": '"status": "done", "summary": "ok"}'}],
            "stop_reason": "end_turn",
            "usage": {},
        }
        return resp

    client = MagicMock()
    client.post = fake_post

    blocks, _stop_reason, _usage = asyncio.run(
        agent_loop._call_model_turn(client, "anthropic", [{"role": "user", "content": "hola"}], [], "sys", force_json=True)
    )

    assert captured["json"]["messages"][-1] == {"role": "assistant", "content": "{"}
    assert blocks[0]["text"] == '{"status": "done", "summary": "ok"}'
    json.loads(blocks[0]["text"])  # confirma que ahora es JSON valido de verdad


def test_call_model_turn_anthropic_force_json_skipped_when_tools_offered(monkeypatch):
    """Prefillear texto le impide a Claude emitir un tool_use en ese turno
    -- con tools ofrecidas, force_json no debe aplicar el prefill."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")
    captured = {}

    async def fake_post(url, **kwargs):
        captured["json"] = kwargs["json"]
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"content": [{"type": "text", "text": "ok"}], "stop_reason": "end_turn", "usage": {}}
        return resp

    client = MagicMock()
    client.post = fake_post
    tools = [{"name": "read_file", "description": "lee un archivo", "input_schema": {"type": "object"}}]

    asyncio.run(
        agent_loop._call_model_turn(client, "anthropic", [{"role": "user", "content": "hola"}], tools, "sys", force_json=True)
    )

    assert captured["json"]["messages"][-1] == {"role": "user", "content": "hola"}


def test_call_model_turn_anthropic_warns_when_tools_offered_but_ignored_and_not_json(monkeypatch):
    """Mismo chequeo que ya existe para Ollama, por simetria: se ofrecieron
    tools reales y la respuesta ni las uso (stop_reason != tool_use) ni es
    JSON valido -- posible alucinacion sin verificar con tools."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")

    async def fake_post(url, **kwargs):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {
            "content": [{"type": "text", "text": "che, dejame pensarlo un toque"}],
            "stop_reason": "end_turn",
            "usage": {},
        }
        return resp

    client = MagicMock()
    client.post = fake_post
    warnings = []
    monkeypatch.setattr(agent_loop.logger, "warning", lambda msg: warnings.append(msg))
    tools = [{"name": "read_file", "description": "lee un archivo", "input_schema": {"type": "object"}}]

    asyncio.run(agent_loop._call_model_turn(client, "anthropic", [{"role": "user", "content": "hola"}], tools, "sys"))

    assert any("posible alucinacion" in w for w in warnings)


def test_call_model_turn_anthropic_no_warning_when_tool_use(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")

    async def fake_post(url, **kwargs):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {
            "content": [{"type": "tool_use", "id": "t1", "name": "read_file", "input": {"path": "a.py"}}],
            "stop_reason": "tool_use",
            "usage": {},
        }
        return resp

    client = MagicMock()
    client.post = fake_post
    warnings = []
    monkeypatch.setattr(agent_loop.logger, "warning", lambda msg: warnings.append(msg))
    tools = [{"name": "read_file", "description": "lee un archivo", "input_schema": {"type": "object"}}]

    asyncio.run(agent_loop._call_model_turn(client, "anthropic", [{"role": "user", "content": "hola"}], tools, "sys"))

    assert warnings == []


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


def test_call_model_turn_ollama_sends_temperature_zero_by_default(monkeypatch):
    captured = {}
    client = MagicMock()
    client.post = _fake_ollama_post_capturing(captured)

    asyncio.run(agent_loop._call_model_turn(client, "ollama", [{"role": "user", "content": "hola"}], [], "sys"))

    assert captured["json"]["options"]["temperature"] == 0.0


def test_call_model_turn_ollama_respects_temperature_env_override(monkeypatch):
    monkeypatch.setattr(agent_loop, "OLLAMA_TEMPERATURE", 0.4)
    captured = {}
    client = MagicMock()
    client.post = _fake_ollama_post_capturing(captured)

    asyncio.run(agent_loop._call_model_turn(client, "ollama", [{"role": "user", "content": "hola"}], [], "sys"))

    assert captured["json"]["options"]["temperature"] == 0.4


def test_call_model_turn_ollama_warns_when_tools_offered_but_ignored_and_not_json(monkeypatch):
    """Se ofrecieron tools reales (para investigar antes de actuar) y el
    modelo no las uso, y lo que devolvio tampoco es JSON valido -- ni
    tool-call ni respuesta final utilizable. Confirmado real esta sesion con
    qwen2.5-coder:7b contra Ollama en vivo (nunca llamo ninguna tool)."""
    captured = {}

    async def fake_post(url, **kwargs):
        captured["json"] = kwargs["json"]
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"message": {"content": "che, dejame pensarlo un toque"}, "prompt_eval_count": 1, "eval_count": 1}
        return resp

    client = MagicMock()
    client.post = fake_post
    warnings = []
    monkeypatch.setattr(agent_loop.logger, "warning", lambda msg: warnings.append(msg))

    tools = [{"name": "read_file", "description": "lee un archivo", "input_schema": {"type": "object"}}]
    asyncio.run(agent_loop._call_model_turn(client, "ollama", [{"role": "user", "content": "hola"}], tools, "sys"))

    assert any("posible alucinacion" in w for w in warnings)


def test_call_model_turn_ollama_no_warning_when_final_answer_is_valid_json(monkeypatch):
    captured = {}

    async def fake_post(url, **kwargs):
        captured["json"] = kwargs["json"]
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"message": {"content": '{"status": "done", "summary": "ok"}'}, "prompt_eval_count": 1, "eval_count": 1}
        return resp

    client = MagicMock()
    client.post = fake_post
    warnings = []
    monkeypatch.setattr(agent_loop.logger, "warning", lambda msg: warnings.append(msg))

    tools = [{"name": "read_file", "description": "lee un archivo", "input_schema": {"type": "object"}}]
    asyncio.run(agent_loop._call_model_turn(client, "ollama", [{"role": "user", "content": "hola"}], tools, "sys"))

    assert warnings == []


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


def test_call_model_turn_ollama_force_json_omits_format_when_tools_offered(monkeypatch):
    """Mismo criterio que use_prefill en la rama Anthropic (que tampoco
    aplica el prefill cuando hay tools): forzar format:"json" en un turno
    donde TAMBIEN se ofrecen tools reales empuja al modelo a responder ya
    en texto en vez de usar tool-calling -- confirmado real esta sesion con
    qwen2.5-coder:7b y ornith:9b. Con tools presentes, force_json no debe
    activar format:"json" -- solo aplica en el turno final sin tools (o el
    reintento de correccion, que ya pasa tools=[]).
    """
    captured = {}
    client = MagicMock()
    client.post = _fake_ollama_post_capturing(captured)
    tools = [{"name": "read_file", "description": "lee un archivo", "input_schema": {"type": "object"}}]

    asyncio.run(
        agent_loop._call_model_turn(client, "ollama", [{"role": "user", "content": "hola"}], tools, "sys", force_json=True)
    )

    assert "format" not in captured["json"]
    assert captured["json"]["tools"]


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


def test_ollama_response_to_blocks_falls_back_to_plaintext_json_tool_call(monkeypatch):
    """qwen2.5-coder:7b real (confirmado contra Ollama en vivo esta sesion)
    nunca completa message.tool_calls -- escribe la llamada como JSON plano
    en content: {"name": "read_file", "arguments": {...}}. Sin este
    fallback, agent_loop lo trataba como respuesta final de texto en vez de
    una tool-call real, y la corrida entera terminaba "blocked" sin haber
    intentado ni una tool.
    """
    message = {"content": '{"name": "read_file", "arguments": {"path": "README.md"}}'}
    monkeypatch.setattr(agent_loop.logger, "warning", lambda msg: None)

    blocks, stop_reason = _ollama_response_to_blocks(message, {"read_file", "write_file"})

    assert stop_reason == "tool_use"
    assert blocks == [{"type": "tool_use", "id": "ollama_call_fallback_0", "name": "read_file", "input": {"path": "README.md"}}]


def test_ollama_response_to_blocks_ignores_plaintext_json_for_unknown_tool_name(monkeypatch):
    """Si el nombre no esta entre las tools ofrecidas, no lo confunde con
    una tool-call real -- se trata como texto final normal (evita falsos
    positivos con una respuesta JSON legitima que use la clave "name" por
    otra razon)."""
    message = {"content": '{"name": "algo_que_no_ofrecimos", "arguments": {}}'}

    blocks, stop_reason = _ollama_response_to_blocks(message, {"read_file"})

    assert stop_reason == "end_turn"
    assert blocks == [{"type": "text", "text": message["content"]}]


def test_ollama_response_to_blocks_real_tool_calls_take_priority_over_fallback(monkeypatch):
    """Si message.tool_calls SI viene poblado, nunca se activa el fallback
    de texto plano, aunque content tambien tenga forma de tool-call."""
    message = {
        "content": '{"name": "algo", "arguments": {}}',
        "tool_calls": [{"function": {"name": "read_file", "arguments": {"path": "a.py"}}}],
    }

    blocks, stop_reason = _ollama_response_to_blocks(message, {"read_file"})

    assert stop_reason == "tool_use"
    tool_block = next(b for b in blocks if b["type"] == "tool_use")
    assert tool_block["name"] == "read_file"
    assert tool_block["input"] == {"path": "a.py"}
    # No aparece ningun bloque sintetizado por el fallback -- el tool_calls
    # real siempre tiene prioridad.
    assert all(b.get("id") != "ollama_call_fallback_0" for b in blocks)


def test_text_as_fallback_tool_call_recognizes_name_arguments_shape():
    """Forma confirmada real con qwen2.5-coder:7b -- regresion, no cambia."""
    result = _text_as_fallback_tool_call(
        '{"name": "read_file", "arguments": {"path": "README.md"}}', {"read_file"}
    )
    assert result == {"name": "read_file", "arguments": {"path": "README.md"}}


def test_text_as_fallback_tool_call_recognizes_tool_input_shape():
    """Caso real confirmado (juez, KAN-5, parable/fable): otro modelo narra
    su tool-call con las claves {"tool": ..., "input": ...} en vez de
    {"name": ..., "arguments": ...} -- forma nueva que agent_loop no
    reconocia, dejando esa narracion caer como "respuesta final" y fallando
    la validacion de esquema esperada aguas arriba."""
    result = _text_as_fallback_tool_call(
        '{"tool": "read_file", "input": {"path": "README.md"}}', {"read_file"}
    )
    assert result == {"name": "read_file", "arguments": {"path": "README.md"}}


def test_text_as_fallback_tool_call_rejects_unknown_tool_name_for_both_shapes():
    assert _text_as_fallback_tool_call('{"name": "algo", "arguments": {}}', {"read_file"}) is None
    assert _text_as_fallback_tool_call('{"tool": "algo", "input": {}}', {"read_file"}) is None


def test_text_as_fallback_tool_call_returns_none_when_no_tools_were_offered():
    """Caso real confirmado (juez, reintento de correccion de JSON): con
    offered_tool_names vacio (_final_text_with_json_retry pasa tools=[] a
    proposito), NINGUNA forma se reconoce como tool-call -- incluso una que
    nombra una tool real que en otro turno existiria, o una inventada como
    "Bash" (que el juez nunca ofrece). Sin este guard, agregar la forma
    tool/input haria que ese JSON se tratara como tool-call real durante el
    reintento, dejando el texto final vacio en vez de la respuesta real del
    modelo."""
    assert _text_as_fallback_tool_call('{"name": "read_file", "arguments": {}}', set()) is None
    assert _text_as_fallback_tool_call('{"tool": "Bash", "input": {"command": "ls"}}', set()) is None


def test_json_correction_message_warns_no_tools_available():
    """Caso real (juez, KAN-5): el modelo intento narrar una tool-call
    ("Bash") en el reintento de correccion de JSON, donde nunca hay tools
    disponibles -- el mensaje ahora se lo aclara explicitamente en vez de
    solo pedir JSON valido."""
    assert "NO hay ninguna herramienta disponible" in JSON_CORRECTION_MESSAGE


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


def test_parse_ollama_model_candidates_splits_on_comma_in_priority_order():
    assert parse_ollama_model_candidates("qwen2.5-coder:7b, llama3.1 ,mistral", "llama3.1") == [
        "qwen2.5-coder:7b", "llama3.1", "mistral",
    ]


def test_parse_ollama_model_candidates_single_value_matches_todays_behavior():
    assert parse_ollama_model_candidates("llama3.1", "llama3.1") == ["llama3.1"]


def test_parse_ollama_model_candidates_empty_falls_back_to_default():
    assert parse_ollama_model_candidates("", "llama3.1") == ["llama3.1"]
    assert parse_ollama_model_candidates(None, "llama3.1") == ["llama3.1"]


def test_parse_ollama_model_candidates_dedupes_preserving_first_occurrence():
    assert parse_ollama_model_candidates("llama3.1,mistral,llama3.1", "llama3.1") == ["llama3.1", "mistral"]


def test_resolve_ollama_model_returns_first_pulled_candidate(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": "mistral:latest"}, {"name": "llama3.1:latest"}]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)

    assert resolve_ollama_model(["qwen2.5-coder:7b", "mistral", "llama3.1"]) == "mistral"


def test_resolve_ollama_model_respects_exclude(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": "mistral:latest"}, {"name": "llama3.1:latest"}]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)

    assert resolve_ollama_model(["mistral", "llama3.1"], exclude={"mistral"}) == "llama3.1"


def test_resolve_ollama_model_none_when_nothing_pulled(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": "llama3.1:latest"}]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)

    assert resolve_ollama_model(["qwen2.5-coder:7b", "mistral"]) is None


def test_resolve_ollama_model_none_when_server_unreachable(monkeypatch):
    def fake_get(*a, **k):
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(agent_loop.httpx, "get", fake_get)

    assert resolve_ollama_model(["llama3.1"]) is None


def test_init_ollama_model_state_resolves_first_pulled_candidate(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": "llama3.1:latest"}]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)
    log = MagicMock()

    state = init_ollama_model_state(["qwen2.5-coder:7b", "llama3.1"], log, "coding agent")

    assert state == {"active": "llama3.1", "tried": set(), "switch_used": False}
    log.warning.assert_not_called()


def test_init_ollama_model_state_falls_back_to_first_candidate_with_warning(monkeypatch):
    def fake_get(*a, **k):
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(agent_loop.httpx, "get", fake_get)
    log = MagicMock()

    state = init_ollama_model_state(["qwen2.5-coder:7b", "llama3.1"], log, "coding agent")

    assert state["active"] == "qwen2.5-coder:7b"
    log.warning.assert_called_once()


def test_maybe_switch_ollama_model_switches_once_to_next_pulled_candidate(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": "llama3.1:latest"}, {"name": "mistral:latest"}]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)
    log = MagicMock()
    state = {"active": "llama3.1", "tried": set(), "switch_used": False}

    switched = maybe_switch_ollama_model(state, "ollama", ["llama3.1", "mistral"], log, "coding agent", "JSON invalido")

    assert switched is True
    assert state["active"] == "mistral"
    assert state["switch_used"] is True
    log.warning.assert_called_once()


def test_maybe_switch_ollama_model_only_switches_once_per_run(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": "llama3.1:latest"}, {"name": "mistral:latest"}]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)
    log = MagicMock()
    state = {"active": "mistral", "tried": {"llama3.1"}, "switch_used": True}

    switched = maybe_switch_ollama_model(state, "ollama", ["llama3.1", "mistral"], log, "coding agent", "JSON invalido de nuevo")

    assert switched is False
    assert state["active"] == "mistral"


def test_maybe_switch_ollama_model_false_when_no_more_candidates(monkeypatch):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": "llama3.1:latest"}]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)
    log = MagicMock()
    state = {"active": "llama3.1", "tried": set(), "switch_used": False}

    switched = maybe_switch_ollama_model(state, "ollama", ["llama3.1"], log, "coding agent", "JSON invalido")

    assert switched is False
    assert state["switch_used"] is False


def test_maybe_switch_ollama_model_false_when_backend_is_not_ollama():
    log = MagicMock()
    state = {"active": "llama3.1", "tried": set(), "switch_used": False}

    switched = maybe_switch_ollama_model(state, "anthropic", ["llama3.1", "mistral"], log, "coding agent", "JSON invalido")

    assert switched is False


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


def test_call_with_fallback_forwards_force_json(monkeypatch):
    monkeypatch.setattr(agent_loop, "_backend_available", lambda backend: backend == "ollama")
    captured = {}

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt, **kwargs):
        captured["force_json"] = kwargs.get("force_json")
        return [{"type": "text", "text": '{"ok": true}'}], "end_turn", {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(agent_loop, "_call_model_turn", fake_call_model_turn)

    asyncio.run(call_with_fallback(client=None, messages=[], tools=[], system_prompt="sys", force_json=True))

    assert captured["force_json"] is True


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


def test_is_hard_auth_failure_true_for_401_and_403():
    for status in (401, 403):
        exc = httpx.HTTPStatusError("error", request=httpx.Request("POST", "http://test"), response=_fake_response(status))
        assert agent_loop._is_hard_auth_failure(exc) is True


def test_is_hard_auth_failure_false_for_transient_errors():
    exc = httpx.HTTPStatusError("error", request=httpx.Request("POST", "http://test"), response=_fake_response(500))
    assert agent_loop._is_hard_auth_failure(exc) is False
    assert agent_loop._is_hard_auth_failure(RuntimeError("boom")) is False


def test_backend_available_false_when_marked_failed_hard(monkeypatch):
    monkeypatch.setattr(agent_loop, "_backends_failed_hard_this_run", {"anthropic"})
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-test")

    assert agent_loop._backend_available("anthropic") is False


def test_call_with_fallback_skips_backend_after_hard_auth_failure(monkeypatch):
    """Bug real confirmado en vivo (operacion de esta noche): con una
    ANTHROPIC_API_KEY invalida/revocada, _backend_available solo chequeaba
    que la variable estuviera SETEADA (no que fuera valida) -- call_with_fallback
    reintentaba Anthropic en CADA turno, repitiendo el mismo 401 real en
    casi todos los turnos de dos corridas completas. Ahora, tras un 401/403,
    el backend se recuerda como roto para el resto de esta corrida (el set a
    nivel de modulo) y NO se vuelve a probar en el turno siguiente.
    """
    monkeypatch.setenv("LLM_BACKEND_PRIORITY", "anthropic,ollama")
    monkeypatch.setattr(agent_loop, "_backends_failed_hard_this_run", set())

    def fake_backend_available(backend):
        return backend not in agent_loop._backends_failed_hard_this_run

    monkeypatch.setattr(agent_loop, "_backend_available", fake_backend_available)

    call_log = []

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt, **kwargs):
        call_log.append(backend)
        if backend == "anthropic":
            raise httpx.HTTPStatusError("error", request=httpx.Request("POST", "http://test"), response=_fake_response(401))
        return [{"type": "text", "text": "ok"}], "end_turn", {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(agent_loop, "_call_model_turn", fake_call_model_turn)

    # Primer turno: prueba anthropic (falla 401), cae a ollama.
    asyncio.run(call_with_fallback(client=None, messages=[], tools=[], system_prompt="sys"))
    assert call_log == ["anthropic", "ollama"]
    assert "anthropic" in agent_loop._backends_failed_hard_this_run

    # Segundo turno (misma corrida): NO vuelve a probar anthropic.
    call_log.clear()
    asyncio.run(call_with_fallback(client=None, messages=[], tools=[], system_prompt="sys"))
    assert call_log == ["ollama"]


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


def test_estimate_message_chars_sums_content_length():
    messages = [{"role": "user", "content": "abcde"}, {"role": "assistant", "content": "1234567890"}]
    assert _estimate_message_chars(messages) == 15


def test_warn_if_context_large_logs_when_over_threshold(monkeypatch):
    """Gap real (usuario, "hay gaps en el context window"): antes no habia
    NINGUNA senal de que una conversacion se estuviera acercando al limite
    real del backend -- esto agrega visibilidad (nunca trunca)."""
    monkeypatch.setattr(agent_loop, "_CONTEXT_SIZE_WARNING_CHARS_OVERRIDE", "10")
    logger = MagicMock()

    warn_if_context_large([{"role": "user", "content": "x" * 20}], logger, "juez")

    logger.warning.assert_called_once()
    assert "juez" in logger.warning.call_args[0][0]
    assert "caracteres" in logger.warning.call_args[0][0]


def test_warn_if_context_large_silent_when_under_threshold(monkeypatch):
    monkeypatch.setattr(agent_loop, "_CONTEXT_SIZE_WARNING_CHARS_OVERRIDE", "1000")
    logger = MagicMock()

    warn_if_context_large([{"role": "user", "content": "x" * 20}], logger, "juez")

    logger.warning.assert_not_called()


def test_warn_if_context_large_includes_system_prompt_in_estimate(monkeypatch):
    """Bug real confirmado esta sesion: el system_prompt se manda en CADA
    turno igual que los messages, pero antes se excluia del calculo por
    completo -- subestimaba la presion real sobre la ventana de contexto en
    varios miles de caracteres fijos por turno (confirmado real:
    CODING_AGENT_SYSTEM_PROMPT/JUDGE_SYSTEM_PROMPT son ~8-9k caracteres cada
    uno, ~27% del OLLAMA_NUM_CTX default por si solos)."""
    monkeypatch.setattr(agent_loop, "_CONTEXT_SIZE_WARNING_CHARS_OVERRIDE", "15")
    logger = MagicMock()

    # 10 caracteres de mensaje, bajo el umbral de 15 por si solo -- pero
    # sumado a un system_prompt de 10 caracteres, supera el umbral.
    warn_if_context_large([{"role": "user", "content": "x" * 10}], logger, "juez", system_prompt="y" * 10)

    logger.warning.assert_called_once()
    assert "system prompt" in logger.warning.call_args[0][0]


def test_context_warning_threshold_uses_anthropic_context_window(monkeypatch):
    """Bug real confirmado esta sesion: el umbral era fijo (siempre basado
    en OLLAMA_NUM_CTX) sin importar que backend estuviera realmente
    sirviendo el turno -- una corrida en Anthropic (ventana real ~200k
    tokens) recibia el mismo umbral chico pensado para Ollama."""
    monkeypatch.delenv("CONTEXT_SIZE_WARNING_CHARS", raising=False)
    monkeypatch.setattr(agent_loop, "_CONTEXT_SIZE_WARNING_CHARS_OVERRIDE", None)

    anthropic_threshold = agent_loop._context_warning_threshold_chars("anthropic")
    ollama_threshold = agent_loop._context_warning_threshold_chars("ollama")

    assert anthropic_threshold == 200_000 * 4
    assert ollama_threshold == agent_loop.OLLAMA_NUM_CTX * 4
    assert anthropic_threshold > ollama_threshold


def test_context_warning_threshold_respects_env_override_for_any_backend(monkeypatch):
    monkeypatch.setattr(agent_loop, "_CONTEXT_SIZE_WARNING_CHARS_OVERRIDE", "500")

    assert agent_loop._context_warning_threshold_chars("anthropic") == 500
    assert agent_loop._context_warning_threshold_chars("ollama") == 500


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


class _FakeMcpTool:
    def __init__(self, name, description="desc"):
        self.name = name
        self.description = description
        self.inputSchema = {"type": "object", "properties": {}}


def test_normalize_tool_schema_excludes_broken_get_neo4j_schema():
    """Bug real confirmado esta sesion: mcp-neo4j-cypher arma internamente
    'CALL apoc.meta.schema({sample: None})...' (el None de Python en vez del
    null de Cypher) -- SIEMPRE tira CypherSyntaxError y quema turnos hasta
    agotar MAX_TOOL_TURNS. No es algo que el prompt pueda arreglar (es
    codigo del servidor MCP externo), se excluye directo."""
    tools = agent_loop._normalize_tool_schema(
        "neo4j-cypher", [_FakeMcpTool("get_neo4j_schema"), _FakeMcpTool("read_neo4j_cypher")]
    )

    names = [t["name"] for t in tools]
    assert "neo4j-cypher__get_neo4j_schema" not in names
    assert "neo4j-cypher__read_neo4j_cypher" in names


def test_normalize_tool_schema_does_not_exclude_unrelated_tools():
    tools = agent_loop._normalize_tool_schema("qdrant-rag", [_FakeMcpTool("qdrant-find")])

    assert [t["name"] for t in tools] == ["qdrant-rag__qdrant-find"]
