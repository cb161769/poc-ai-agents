"""Unit tests for coding_agent.py's local tools: path confinement (the
model's tool calls are untrusted input) and the read/list/grep/write/run
tools themselves. Real filesystem under tmp_path, no mocks for I/O; only
builtins.input is mocked to simulate the human confirmation prompts.

Also covers the loop-level guardrails (investigate-before-write,
verify-before-done) by mocking _call_model_turn/_connect_mcp_servers -- no
real model backend or MCP server involved.
"""
import asyncio

import pytest

import coding_agent as ca


async def _fake_connect_mcp(stack, servers, label="agente"):
    return {}


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


def test_run_coding_agent_blocks_write_before_investigation(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)

    call_count = {"n": 0}

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [
                {"type": "tool_use", "id": "call_1", "name": "write_file", "input": {"path": "x.txt", "content": "hola"}}
            ]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}
        content = [{"type": "text", "text": '{"status": "blocked", "summary": "no pude", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(ca, "_call_model_turn", fake_call_model_turn)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert result["status"] == "blocked"
    assert not (tmp_path / "x.txt").exists()


def test_run_coding_agent_allows_write_after_investigation(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)
    (tmp_path / "existing.txt").write_text("ya existe")

    call_count = {"n": 0}

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [{"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "existing.txt"}}]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}
        if call_count["n"] == 2:
            content = [
                {"type": "tool_use", "id": "call_2", "name": "run_shell_command", "input": {"command": "echo verificado"}}
            ]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}
        content = [{"type": "text", "text": '{"status": "done", "summary": "listo y verificado", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(ca, "_call_model_turn", fake_call_model_turn)
    monkeypatch.setattr("builtins.input", lambda: "s")

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert result["status"] == "done"
    assert result["self_verified"] is True


def test_run_coding_agent_nudges_for_verification_before_accepting_done(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)

    call_count = {"n": 0}

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt):
        call_count["n"] += 1
        content = [{"type": "text", "text": '{"status": "done", "summary": "listo", "files_changed": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(ca, "_call_model_turn", fake_call_model_turn)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert call_count["n"] == 2, "deberia haber recibido el empujon de verificacion y llamado al modelo una vez mas"
    assert result["status"] == "done"
    assert result["self_verified"] is False


def test_run_coding_agent_retries_on_malformed_final_json(monkeypatch, tmp_path):
    monkeypatch.setattr(ca, "_select_backend", lambda: "anthropic")
    monkeypatch.setattr(ca, "_connect_mcp_servers", _fake_connect_mcp)

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt):
        content = [{"type": "text", "text": "esto no es json"}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}

    async def fake_json_retry(client, backend, messages, tools, system_prompt):
        return '{"status": "blocked", "summary": "recuperado", "files_changed": []}', {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(ca, "_call_model_turn", fake_call_model_turn)
    monkeypatch.setattr(ca, "_final_text_with_json_retry", fake_json_retry)

    result = asyncio.run(ca.run_coding_agent("T-1", "hace algo", str(tmp_path)))

    assert result["status"] == "blocked"
    assert result["summary"] == "recuperado"


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
