"""Unit tests for coding_agent.py's local tools: path confinement (the
model's tool calls are untrusted input) and the read/list/grep/write/run
tools themselves. Real filesystem under tmp_path, no mocks for I/O; only
builtins.input is mocked to simulate the human confirmation prompts.

Also covers the loop-level guardrails (investigate-before-write,
verify-before-done) by mocking _call_model_turn/_connect_mcp_servers -- no
real model backend or MCP server involved.
"""
import asyncio
import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

import coding_agent as ca


async def _fake_connect_mcp(stack, servers, label="agente"):
    return {}


def _init_git_repo(repo_dir):
    subprocess.run(["git", "init", "-q"], cwd=repo_dir, check=True)
    (repo_dir / "f.py").write_text("original content\n")
    subprocess.run(["git", "-c", "user.email=t@t.com", "-c", "user.name=t", "add", "-A"], cwd=repo_dir, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "-q", "-m", "baseline"],
        cwd=repo_dir,
        check=True,
    )


def test_safe_path_allows_relative_path_inside_repo(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "file.py").write_text("x = 1")

    resolved = ca._safe_path(str(tmp_path), "src/file.py")

    assert resolved == (tmp_path / "src" / "file.py").resolve()


def test_safe_path_rejects_parent_traversal(tmp_path):
    with pytest.raises(ValueError):
        ca._safe_path(str(tmp_path), "../../etc/passwd")


def test_safe_path_rejects_absolute_path_outside_repo(tmp_path, tmp_path_factory):
    other_dir = tmp_path_factory.mktemp("outside")
    with pytest.raises(ValueError):
        ca._safe_path(str(tmp_path), str(other_dir / "secret.txt"))


def test_tool_read_file_returns_content(tmp_path):
    (tmp_path / "hello.txt").write_text("hola mundo")

    assert ca.tool_read_file(str(tmp_path), "hello.txt") == "hola mundo"


def test_tool_read_file_missing_file_returns_error_string(tmp_path):
    result = ca.tool_read_file(str(tmp_path), "nope.txt")
    assert "error" in result


def test_tool_list_directory_lists_entries(tmp_path):
    (tmp_path / "a.txt").write_text("")
    (tmp_path / "subdir").mkdir()

    result = ca.tool_list_directory(str(tmp_path))

    assert "a.txt" in result
    assert "subdir/" in result


def test_tool_grep_search_finds_matches(tmp_path):
    (tmp_path / "code.py").write_text("def foo():\n    pass\n")
    (tmp_path / "other.py").write_text("def bar():\n    pass\n")

    result = ca.tool_grep_search(str(tmp_path), "foo")

    assert "code.py" in result
    assert "def foo" in result
    assert "other.py" not in result


def test_tool_grep_search_no_matches(tmp_path):
    (tmp_path / "code.py").write_text("def bar(): pass\n")
    assert ca.tool_grep_search(str(tmp_path), "nonexistent_pattern") == "(sin resultados)"


def test_tool_write_file_applies_when_confirmed(tmp_path, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = ca.tool_write_file(str(tmp_path), "new.txt", "contenido nuevo")

    assert "escrito ok" in result
    assert (tmp_path / "new.txt").read_text() == "contenido nuevo"


def test_tool_write_file_skips_when_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda: "n")

    result = ca.tool_write_file(str(tmp_path), "rejected.txt", "no deberia existir")

    assert "rechazo" in result
    assert not (tmp_path / "rejected.txt").exists()


def test_tool_write_file_blocks_path_traversal(tmp_path, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = ca.tool_write_file(str(tmp_path), "../../etc/passwd", "malicious")

    assert "error" in result


def test_tool_run_shell_command_runs_when_confirmed(tmp_path, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = ca.tool_run_shell_command(str(tmp_path), "echo hello-from-test")

    assert "exit_code=0" in result
    assert "hello-from-test" in result


def test_tool_run_shell_command_skips_when_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda: "n")

    result = ca.tool_run_shell_command(str(tmp_path), "echo should-not-run")

    assert "rechazo" in result


def test_tool_run_shell_command_runs_in_monorepo_subdir_via_cwd(tmp_path, monkeypatch):
    """Confirmado real contra ai-agents-code: sin cwd, 'npm test' corria en
    la raiz del monorepo (donde no hay package.json) y siempre fallaba --
    con cwd="frontend" tiene que correr parado ahi de verdad. Se mockea
    subprocess.run (en vez de usar un comando de shell real tipo 'pwd', que
    no es portable entre cmd.exe/bash) para verificar el cwd real que
    recibe, sin depender de la shell del SO que corra los tests.
    """
    monkeypatch.setattr("builtins.input", lambda: "s")
    sub = tmp_path / "frontend"
    sub.mkdir()
    captured = {}

    def fake_run(command, **kwargs):
        captured["cwd"] = kwargs.get("cwd")
        return subprocess.CompletedProcess(args=command, returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(ca.subprocess, "run", fake_run)

    result = ca.tool_run_shell_command(str(tmp_path), "npm test", cwd="frontend")

    assert captured["cwd"] == str(sub.resolve())
    assert "exit_code=0" in result


def test_tool_run_shell_command_rejects_cwd_outside_repo(tmp_path, monkeypatch):
    result = ca.tool_run_shell_command(str(tmp_path), "echo no-deberia-correr", cwd="../../etc")

    assert "error" in result
    assert "fuera del repo objetivo" in result


def test_tool_run_shell_command_rejects_cwd_that_is_not_a_directory(tmp_path, monkeypatch):
    (tmp_path / "archivo.txt").write_text("contenido")

    result = ca.tool_run_shell_command(str(tmp_path), "echo no-deberia-correr", cwd="archivo.txt")

    assert "no es un directorio" in result


def test_build_user_prompt_does_not_precargar_root_listing(tmp_path):
    """El listado de la raiz del repo ya no viaja precargado en el prompt
    inicial -- el modelo lo pide el mismo con list_directory si le hace
    falta. Antes esto se pagaba en TODAS las corridas.
    """
    (tmp_path / "unique_marker_file.txt").write_text("x")

    prompt = ca._build_user_prompt("T-1", "hace algo", str(tmp_path))

    assert "unique_marker_file.txt" not in prompt


def test_run_coding_agent_blocks_write_before_investigation(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [
                {"type": "tool_use", "id": "call_1", "name": "write_file", "input": {"path": "x.txt", "content": "hola"}}
            ]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        content = [{"type": "text", "text": '{"status": "blocked", "summary": "no pude", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert result["status"] == "blocked"
    assert not (tmp_path / "x.txt").exists()


def test_run_coding_agent_allows_write_after_investigation(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)
    (tmp_path / "existing.txt").write_text("ya existe")

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [{"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "existing.txt"}}]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        if call_count["n"] == 2:
            content = [
                {"type": "tool_use", "id": "call_2", "name": "run_shell_command", "input": {"command": "echo verificado"}}
            ]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        content = [{"type": "text", "text": '{"status": "done", "summary": "listo y verificado", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert result["status"] == "done"
    assert result["self_verified"] is True


def test_run_coding_agent_nudges_for_verification_before_accepting_done(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        content = [{"type": "text", "text": '{"status": "done", "summary": "listo", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    # 3 llamados: original -> empujon de verificacion -> empujon de self_review
    # (ninguno de los dos se completa nunca en esta respuesta fija, asi que
    # cada uno se da UNA sola vez y despues se acepta igual).
    assert call_count["n"] == 3, "deberia haber recibido ambos empujones (verificacion y self_review) y llamado al modelo dos veces mas"
    assert result["status"] == "done"
    assert result["self_verified"] is False


def test_run_coding_agent_retries_on_malformed_final_json(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [{"type": "text", "text": "esto no es json"}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    async def fake_json_retry(client, backend, messages, tools, system_prompt, **kwargs):
        return '{"status": "blocked", "summary": "recuperado", "files_changed": []}', {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr(ca, "_final_text_with_json_retry", fake_json_retry)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert result["status"] == "blocked"
    assert result["summary"] == "recuperado"


def test_run_coding_agent_nudges_to_investigate_first_when_refusal_comes_before_investigation(monkeypatch, tmp_path):
    """Confirmado real esta sesion contra ai-agents-code: el modelo puede
    rendirse ANTES de investigar con exito -- ej. adivina mal una ruta con
    read_file (error), y en el siguiente turno se niega a crear en vez de
    reintentar la investigacion. El nudge en ese caso tiene que mandarlo a
    confirmar la ruta con list_directory, no solo repetirle "llama la tool
    ya" (eso solo chocaria con el gate real de "investiga primero").
    """
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)
    monkeypatch.setattr("builtins.input", lambda: "s")

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # read_file con una ruta que NO existe -- error, has_investigated sigue False.
            content = [{"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "no-existe.js"}}]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        if call_count["n"] == 2:
            # Se niega en texto SIN haber investigado con exito todavia.
            content = [{"type": "text", "text": "no-existe.js no existe en el repositorio. No puedo crear archivos nuevos sin confirmación humana."}]
            return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        if call_count["n"] == 3:
            # Esta vez lista el directorio de verdad (siguiendo el nudge).
            content = [{"type": "tool_use", "id": "call_2", "name": "list_directory", "input": {"path": "."}}]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        content = [{
            "type": "text",
            "text": (
                '{"status": "done", "summary": "listo", "files_changed": [], '
                '"self_review": {"scope_matches_ticket": true, "no_secrets_introduced": true, "tests_adequate": true}}'
            ),
        }]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    captured_messages = []

    async def spying_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        captured_messages.append(messages[-1])
        return await fake_call_with_fallback(client, messages, tools, system_prompt, exclude=exclude, **kwargs)

    monkeypatch.setattr(ca, "call_with_fallback", spying_call_with_fallback)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert result["status"] == "done"
    # El nudge mandado despues del rechazo tiene que ser la variante que
    # pide investigar primero, no la que asume que ya investigo.
    nudge_sent = captured_messages[2]["content"]
    assert nudge_sent == ca.TOOL_CALL_NUDGE_MESSAGE_NEEDS_INVESTIGATION


def test_run_coding_agent_nudges_when_model_refuses_to_call_tool(monkeypatch, tmp_path):
    """Confirmado real esta sesion: algunos modelos anuncian que van a crear
    un archivo pero explican en texto que "no pueden sin confirmacion
    humana" en vez de llamar write_file (que ya pide esa confirmacion
    sola). El nudge especifico le da una oportunidad real de llamar la tool
    en vez de caer directo al reintento generico de "dame JSON valido".
    """
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)
    (tmp_path / "existing.txt").write_text("ya existe")
    monkeypatch.setattr("builtins.input", lambda: "s")

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Investiga primero (para pasar el gate real).
            content = [{"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "existing.txt"}}]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        if call_count["n"] == 2:
            # Se niega en texto en vez de llamar write_file.
            content = [{"type": "text", "text": "El archivo nuevo.txt no existe. No puedo crearlo sin confirmación humana."}]
            return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        # Respuesta "done" completa (self_review + verificacion ya hecha en
        # el turno 1) para que el foco del test sea solo el nudge de
        # tool-call, sin que los otros dos empujones (verificacion/
        # self_review, ya cubiertos en otro test) agreguen turnos de mas.
        content = [{
            "type": "text",
            "text": (
                '{"status": "done", "summary": "listo tras el nudge", "files_changed": [], '
                '"self_review": {"scope_matches_ticket": true, "no_secrets_introduced": true, "tests_adequate": true}}'
            ),
        }]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert call_count["n"] == 4  # investigar -> se niega -> nudge -> responde bien -> empujon de verificacion (self_review ya venia OK)
    assert result["status"] == "done"
    assert result["summary"] == "listo tras el nudge"


def test_run_coding_agent_only_nudges_tool_call_refusal_once(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)
    (tmp_path / "existing.txt").write_text("ya existe")
    monkeypatch.setattr("builtins.input", lambda: "s")

    call_count = {"n": 0}
    refusal_text = "No puedo crear ese archivo sin confirmación humana."

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [{"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "existing.txt"}}]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        # Se niega SIEMPRE, incluso despues del nudge -- el segundo rechazo
        # no debe generar un segundo nudge (evita loop infinito).
        content = [{"type": "text", "text": refusal_text}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    async def fake_json_retry(client, backend, messages, tools, system_prompt, **kwargs):
        return '{"status": "blocked", "summary": "no se animo a llamar la tool", "files_changed": []}', {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr(ca, "_final_text_with_json_retry", fake_json_retry)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    # investigar -> se niega (nudge 1) -> se niega otra vez -> retry generico -> blocked
    assert call_count["n"] == 3
    assert result["status"] == "blocked"


def test_run_coding_agent_resume_skips_reinvestigation(monkeypatch, tmp_path):
    """Con resume_messages/resume_state (reintento tras feedback del juez),
    no deberia hacer falta investigar de nuevo -- has_investigated ya viene
    sembrado en True, asi que un write_file directo no se rechaza.
    """
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)
    monkeypatch.setattr("builtins.input", lambda: "s")

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [
                {"type": "tool_use", "id": "call_1", "name": "write_file", "input": {"path": "x.txt", "content": "hola"}}
            ]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        final = {
            "status": "done",
            "summary": "corregido",
            "files_changed": ["x.txt"],
            "self_review": {"scope_matches_ticket": True, "no_secrets_introduced": True, "tests_adequate": True},
        }
        return [{"type": "text", "text": json.dumps(final)}], "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)

    prior_messages = [
        {"role": "user", "content": "Ticket: T-1\n\narregla el boton"},
        {"role": "assistant", "content": [{"type": "text", "text": "Plan: listo"}]},
    ]

    result = asyncio.run(
        ca.run_coding_agent(
            "T-1",
            "--- FEEDBACK DEL JUEZ ---\ncorregi el alcance",
            str(tmp_path),
            resume_messages=prior_messages,
            resume_state={"has_investigated": True, "has_run_verification": True},
        )
    )

    assert result["status"] == "done"
    assert (tmp_path / "x.txt").exists(), "el write_file no deberia haberse rechazado -- ya veniamos investigados"
    assert result["self_verified"] is True


def test_run_coding_agent_writes_conversation_file_for_resume(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [{"type": "text", "text": '{"status": "blocked", "summary": "no pude", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert "_conversation_file" in result
    conversation_path = Path(result["_conversation_file"])
    assert conversation_path.exists()
    saved = json.loads(conversation_path.read_text(encoding="utf-8"))
    assert "messages" in saved
    assert "has_investigated" in saved
    conversation_path.unlink()


def test_run_coding_agent_captures_initial_plan(monkeypatch, tmp_path):
    """El primer turno que trae texto junto a una tool_use se captura como
    initial_plan -- la instruccion "Plan: ..." del prompt le pide al modelo
    ese texto antes de actuar, y hasta ahora nada lo guardaba.
    """
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)
    (tmp_path / "existing.txt").write_text("ya existe")

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [
                {"type": "text", "text": "Plan: voy a leer existing.txt y despues ajustar el mensaje."},
                {"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "existing.txt"}},
            ]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        content = [{"type": "text", "text": '{"status": "done", "summary": "listo", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert result["initial_plan"] == "Plan: voy a leer existing.txt y despues ajustar el mensaje."


def test_run_coding_agent_initial_plan_survives_resume(monkeypatch, tmp_path):
    """Un reintento no deberia perder el plan original -- resume_state lo
    siembra, y si el segundo intento no vuelve a mandar texto libre en su
    primer turno de tool_use, el plan sembrado se conserva.
    """
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)
    monkeypatch.setattr("builtins.input", lambda: "s")

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [{"type": "text", "text": '{"status": "done", "summary": "corregido", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(
        ca.run_coding_agent(
            "T-1",
            "--- FEEDBACK DEL JUEZ ---\ncorregi el alcance",
            str(tmp_path),
            resume_messages=[{"role": "user", "content": "Ticket: T-1\n\narregla el boton"}],
            resume_state={
                "has_investigated": True,
                "has_run_verification": True,
                "initial_plan": "Plan original: ajustar el boton de login.",
            },
        )
    )

    assert result["initial_plan"] == "Plan original: ajustar el boton de login."


def test_run_coding_agent_accepts_done_with_valid_self_review_no_extra_nudge(monkeypatch, tmp_path):
    """Si la respuesta final ya trae self_review completo (y ya investigo y
    verifico), no deberia pedir ningun empujon extra.
    """
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)
    (tmp_path / "existing.txt").write_text("ya existe")

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [{"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "existing.txt"}}]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        if call_count["n"] == 2:
            content = [
                {"type": "tool_use", "id": "call_2", "name": "run_shell_command", "input": {"command": "echo verificado"}}
            ]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        final = {
            "status": "done",
            "summary": "listo",
            "files_changed": [],
            "self_review": {"scope_matches_ticket": True, "no_secrets_introduced": True, "tests_adequate": True},
        }
        return [{"type": "text", "text": json.dumps(final)}], "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert call_count["n"] == 3, "no deberia haber recibido ningun empujon extra"
    assert result["status"] == "done"
    assert result["self_review"]["scope_matches_ticket"] is True


def test_run_coding_agent_nudges_once_for_missing_self_review(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)
    (tmp_path / "existing.txt").write_text("ya existe")

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [{"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "existing.txt"}}]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        if call_count["n"] == 2:
            content = [
                {"type": "tool_use", "id": "call_2", "name": "run_shell_command", "input": {"command": "echo ok"}}
            ]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        # Ya investigo y verifico (has_run_verification=True), pero nunca
        # completa self_review -- deberia recibir el empujon una sola vez y
        # aceptarse igual despues.
        content = [{"type": "text", "text": '{"status": "done", "summary": "listo", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert call_count["n"] == 4, "deberia haber recibido un solo empujon de self_review y aceptado despues"
    assert result["status"] == "done"
    assert "self_review" not in result or result.get("self_review") is None


# --- Robustez de las tools individuales ---


def test_tool_write_file_blocks_writes_inside_git_dir(tmp_path, monkeypatch):
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("[core]\n")
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = ca.tool_write_file(str(tmp_path), ".git/config", "malicious content")

    assert "error" in result
    assert (tmp_path / ".git" / "config").read_text() == "[core]\n"


def test_sanitized_subprocess_env_strips_secrets(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-secret")
    monkeypatch.setenv("SOME_OTHER_VAR", "keep-me")

    env = ca._sanitized_subprocess_env()

    assert "ANTHROPIC_API_KEY" not in env
    assert "JIRA_API_TOKEN" not in env
    assert env.get("SOME_OTHER_VAR") == "keep-me"


def test_tool_read_file_rejects_files_over_size_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(ca, "_MAX_READ_BYTES", 10)
    (tmp_path / "big.txt").write_text("x" * 100)

    result = ca.tool_read_file(str(tmp_path), "big.txt")

    assert "error" in result
    assert "demasiado grande" in result


def test_tool_list_directory_truncates_when_over_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(ca, "_MAX_LIST_ENTRIES", 2)
    for i in range(5):
        (tmp_path / f"file{i}.txt").write_text("")

    result = ca.tool_list_directory(str(tmp_path))

    assert "omitidas" in result
    assert result.count(".txt") == 2


def test_tool_grep_search_skips_files_over_size_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(ca, "_MAX_GREP_FILE_BYTES", 10)
    (tmp_path / "huge.py").write_text("findme " * 100)
    (tmp_path / "small.py").write_text("findme")

    result = ca.tool_grep_search(str(tmp_path), "findme")

    assert "small.py" in result
    assert "huge.py" not in result


def test_tool_grep_search_stops_at_files_scanned_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(ca, "_MAX_GREP_FILES_SCANNED", 2)
    for i in range(5):
        (tmp_path / f"f{i}.py").write_text("nomatch")

    result = ca.tool_grep_search(str(tmp_path), "nomatch")

    assert "limite" in result


def test_tool_run_shell_command_truncates_large_output(tmp_path, monkeypatch):
    monkeypatch.setattr(ca, "_MAX_SHELL_OUTPUT_CHARS", 20)
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = ca.tool_run_shell_command(str(tmp_path), "echo " + ("x" * 100))

    assert "truncado" in result


def test_tool_write_file_shows_diff_for_existing_file(tmp_path, monkeypatch, capsys):
    (tmp_path / "existing.txt").write_text("linea vieja\n")
    monkeypatch.setattr("builtins.input", lambda: "s")

    ca.tool_write_file(str(tmp_path), "existing.txt", "linea nueva\n")

    captured = capsys.readouterr()
    assert "-linea vieja" in captured.err
    assert "+linea nueva" in captured.err


def test_call_mcp_tool_times_out_gracefully(monkeypatch):
    import agent_loop

    monkeypatch.setattr(agent_loop, "_MCP_TOOL_TIMEOUT_SECONDS", 0.05)

    class _HangingSession:
        async def call_tool(self, tool_name, tool_input):
            await asyncio.sleep(5)

    result = asyncio.run(agent_loop._call_mcp_tool({"neo4j-cypher": _HangingSession()}, "neo4j-cypher__read", {}))

    assert "no respondio" in result


# --- tool_edit_file ---


def test_tool_edit_file_applies_unique_replacement(tmp_path, monkeypatch, capsys):
    (tmp_path / "f.py").write_text("def foo():\n    return 1\n")
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = ca.tool_edit_file(str(tmp_path), "f.py", "return 1", "return 2")

    assert result == "editado ok: f.py"
    assert (tmp_path / "f.py").read_text() == "def foo():\n    return 2\n"
    captured = capsys.readouterr()
    assert "-    return 1" in captured.err
    assert "+    return 2" in captured.err


def test_tool_edit_file_rejects_zero_matches(tmp_path, monkeypatch):
    (tmp_path / "f.py").write_text("def foo():\n    return 1\n")
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = ca.tool_edit_file(str(tmp_path), "f.py", "does not exist", "x")

    assert "error" in result
    assert "no se encontro" in result
    assert (tmp_path / "f.py").read_text() == "def foo():\n    return 1\n"


def test_tool_edit_file_rejects_multiple_matches(tmp_path, monkeypatch):
    (tmp_path / "f.py").write_text("x = 1\nx = 1\n")
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = ca.tool_edit_file(str(tmp_path), "f.py", "x = 1", "x = 2")

    assert "error" in result
    assert "2 veces" in result
    assert (tmp_path / "f.py").read_text() == "x = 1\nx = 1\n"


def test_tool_edit_file_skips_when_rejected(tmp_path, monkeypatch):
    (tmp_path / "f.py").write_text("original\n")
    monkeypatch.setattr("builtins.input", lambda: "n")

    result = ca.tool_edit_file(str(tmp_path), "f.py", "original", "cambiado")

    assert "rechazo" in result
    assert (tmp_path / "f.py").read_text() == "original\n"


def test_tool_edit_file_rejects_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = ca.tool_edit_file(str(tmp_path), "nope.py", "a", "b")

    assert "error" in result
    assert "no existe" in result


def test_tool_edit_file_blocks_path_traversal(tmp_path):
    result = ca.tool_edit_file(str(tmp_path), "../outside.py", "a", "b")

    assert "error" in result


# --- tool_git_diff / tool_git_log ---


def test_tool_git_diff_shows_uncommitted_changes(tmp_path):
    _init_git_repo(tmp_path)
    (tmp_path / "f.py").write_text("changed content\n")

    result = ca.tool_git_diff(str(tmp_path))

    assert "changed content" in result


def test_tool_git_diff_no_changes(tmp_path):
    _init_git_repo(tmp_path)

    result = ca.tool_git_diff(str(tmp_path))

    assert result == "(sin cambios)"


def test_tool_git_log_shows_commits(tmp_path):
    _init_git_repo(tmp_path)

    result = ca.tool_git_log(str(tmp_path))

    assert "baseline" in result


def test_tool_git_log_respects_n(tmp_path):
    _init_git_repo(tmp_path)

    result = ca.tool_git_log(str(tmp_path), n=1)

    assert "baseline" in result


# --- tool_detect_project_stack ---


def test_tool_detect_project_stack_finds_node(tmp_path):
    (tmp_path / "package.json").write_text("{}")

    result = ca.tool_detect_project_stack(str(tmp_path))

    assert "Node/TS" in result
    assert "npm test" in result


def test_tool_detect_project_stack_finds_maven(tmp_path):
    (tmp_path / "pom.xml").write_text("<project/>")

    result = ca.tool_detect_project_stack(str(tmp_path))

    assert "Maven/Java" in result


def test_tool_detect_project_stack_unknown(tmp_path):
    result = ca.tool_detect_project_stack(str(tmp_path))

    assert "no se detecto" in result


def test_tool_detect_project_stack_finds_monorepo_subprojects(tmp_path):
    """Confirmado real contra ai-agents-code: ningun marcador en la raiz,
    3 sub-proyectos reales un nivel abajo -- antes esto devolvia siempre
    "no se detecto ningun marcador" y el agente se rendia ahi.
    """
    (tmp_path / "auth-service").mkdir()
    (tmp_path / "auth-service" / "pom.xml").write_text("<project/>")
    (tmp_path / "frontend").mkdir()
    (tmp_path / "frontend" / "package.json").write_text("{}")
    (tmp_path / "data-worker").mkdir()
    (tmp_path / "data-worker" / "Pipfile").write_text("")

    result = ca.tool_detect_project_stack(str(tmp_path))

    assert "monorepo detectado con 3 sub-proyecto(s)" in result
    assert "auth-service/: Maven/Java" in result
    assert "frontend/: Node/TS" in result
    assert "data-worker/: Python (Pipenv)" in result


def test_tool_detect_project_stack_root_marker_wins_over_subdirs(tmp_path):
    """Si la raiz SI tiene un marcador, no escanea subcarpetas -- evita
    reportar sub-proyectos irrelevantes (ej. node_modules con su propio
    package.json) cuando el repo real es de un solo stack."""
    (tmp_path / "package.json").write_text("{}")
    (tmp_path / "vendor").mkdir()
    (tmp_path / "vendor" / "pom.xml").write_text("<project/>")

    result = ca.tool_detect_project_stack(str(tmp_path))

    assert "Node/TS" in result
    assert "monorepo" not in result


def test_tool_detect_project_stack_skips_ignored_dirs_in_monorepo_scan(tmp_path):
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules" / "package.json").write_text("{}")
    (tmp_path / ".git").mkdir()
    (tmp_path / "real-service").mkdir()
    (tmp_path / "real-service" / "pom.xml").write_text("<project/>")

    result = ca.tool_detect_project_stack(str(tmp_path))

    assert "real-service/: Maven/Java" in result
    assert "node_modules" not in result


# --- tool_query_sonar ---


def test_tool_query_sonar_formats_issues(tmp_path):
    with patch("coding_agent.sonar_client.get_issues") as mock_get_issues:
        mock_get_issues.return_value = {
            "issues": [{"severity": "BLOCKER", "rule": "java:S2068", "message": "Hardcoded credential", "line": 14}]
        }
        result = ca.tool_query_sonar(str(tmp_path), "AuthService")

    assert "BLOCKER" in result
    assert "Hardcoded credential" in result


def test_tool_query_sonar_no_issues(tmp_path):
    with patch("coding_agent.sonar_client.get_issues") as mock_get_issues:
        mock_get_issues.return_value = {"issues": []}
        result = ca.tool_query_sonar(str(tmp_path), "Frontend")

    assert "sin hallazgos" in result


# --- Guardrail: edit_file tambien exige investigar antes ---


def test_run_coding_agent_blocks_edit_before_investigation(monkeypatch, tmp_path):
    (tmp_path / "f.py").write_text("original\n")
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [
                {
                    "type": "tool_use",
                    "id": "call_1",
                    "name": "edit_file",
                    "input": {"path": "f.py", "old_string": "original", "new_string": "cambiado"},
                }
            ]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        content = [{"type": "text", "text": '{"status": "blocked", "summary": "no pude", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(ca, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert result["status"] == "blocked"
    assert (tmp_path / "f.py").read_text() == "original\n"
