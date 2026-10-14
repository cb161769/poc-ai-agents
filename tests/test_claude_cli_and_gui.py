"""Tests del backend claude_cli (parseo del protocolo, sin llamar al CLI real)
y de las reglas de seguridad de chat_server.py (GUI local)."""
import claude_cli_backend as cc
import chat_server


def test_result_with_tool_calls_becomes_tool_use_blocks():
    data = {"structured_output": {"text": "plan", "tool_calls": [{"name": "read_file", "arguments": {"path": "a"}}]}}
    blocks, stop = cc._cli_result_to_blocks(data, tools_offered=True)
    assert stop == "tool_use"
    assert blocks[0] == {"type": "text", "text": "plan"}
    assert blocks[1]["name"] == "read_file" and blocks[1]["input"] == {"path": "a"}


def test_tool_call_ids_are_unique_across_calls():
    data = {"structured_output": {"text": "", "tool_calls": [{"name": "read_file", "arguments": {}}]}}
    first = cc._cli_result_to_blocks(data, True)[0][0]["id"]
    second = cc._cli_result_to_blocks(data, True)[0][0]["id"]
    assert first != second


def test_empty_tool_calls_is_final_answer():
    data = {"structured_output": {"text": '{"status": "done"}', "tool_calls": []}}
    blocks, stop = cc._cli_result_to_blocks(data, True)
    assert stop == "end_turn" and blocks == [{"type": "text", "text": '{"status": "done"}'}]


def test_nested_protocol_wrapper_inside_text_is_unwrapped():
    inner = '{"text": "x", "tool_calls": [{"name": "read_file", "arguments": {"path": "a"}}]}'
    blocks, stop = cc._cli_result_to_blocks({"structured_output": {"text": inner, "tool_calls": []}}, True)
    assert stop == "tool_use" and blocks[-1]["name"] == "read_file"


def test_no_tools_with_schema_returns_structured_output_as_json_text():
    blocks, stop = cc._cli_result_to_blocks({"structured_output": {"bucket": "x"}, "result": ""}, False)
    assert stop == "end_turn" and blocks[0]["text"] == '{"bucket": "x"}'


def test_cli_not_found_when_explicit_path_missing(monkeypatch):
    monkeypatch.setenv("CLAUDE_CLI_PATH", "/nonexistent/claude")
    assert cc.find_claude_cli() is None and not cc.claude_cli_available()


def test_gui_websocket_origin_check():
    port = chat_server.PORT
    assert chat_server._origin_allowed(None)
    assert chat_server._origin_allowed(f"http://127.0.0.1:{port}")
    assert chat_server._origin_allowed(f"http://localhost:{port}")
    assert not chat_server._origin_allowed("http://evil.example")
    assert not chat_server._origin_allowed(f"http://127.0.0.1:{port + 1}")
