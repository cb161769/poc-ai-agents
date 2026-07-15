"""Unit tests for epic_planner.py: the pure helpers (_fallback_result,
_validate_result) directly, and plan_epic() with call_with_fallback/MCP
mocked -- same pattern already used in tests/test_judge_agent.py for the
equivalent dual-backend agent loop.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import epic_planner
from epic_planner import _build_user_prompt, _extract_json, _fallback_result, _format_sprint_suffix, _validate_result, plan_epic


def _children():
    return [
        {"ticket_id": "JIRA-1", "summary": "s1", "description": "d1", "repository_origen": "AuthService"},
        {"ticket_id": "JIRA-2", "summary": "s2", "description": "d2", "repository_origen": "Frontend"},
    ]


def test_extract_json_strips_fenced_code_block():
    assert _extract_json('```json\n{"ordered_children": []}\n```') == {"ordered_children": []}


def test_format_sprint_suffix_none_is_empty():
    assert _format_sprint_suffix(None) == ""
    assert _format_sprint_suffix({"name": None}) == ""


def test_format_sprint_suffix_includes_name_and_state():
    assert _format_sprint_suffix({"name": "Sprint 12", "state": "active"}) == " (sprint: Sprint 12, active)"


def test_format_sprint_suffix_omits_state_when_missing():
    assert _format_sprint_suffix({"name": "Sprint 12", "state": None}) == " (sprint: Sprint 12)"


def test_build_user_prompt_includes_sprint_info_when_present():
    """Gap real (usuario, "gaps en el workflow"): el scrum agent no tenia
    ninguna nocion de sprint -- ahora es contexto informativo en el prompt."""
    children = [
        {"ticket_id": "JIRA-1", "summary": "s1", "description": "d1", "repository_origen": "AuthService",
         "sprint": {"name": "Sprint 12", "state": "active"}},
    ]
    prompt = _build_user_prompt({"key": "EPIC-1", "summary": "e", "description": "d"}, children)
    assert "(sprint: Sprint 12, active)" in prompt


def test_build_user_prompt_omits_sprint_suffix_when_absent():
    children = [
        {"ticket_id": "JIRA-1", "summary": "s1", "description": "d1", "repository_origen": "AuthService"},
    ]
    prompt = _build_user_prompt({"key": "EPIC-1", "summary": "e", "description": "d"}, children)
    assert "sprint:" not in prompt


def test_fallback_result_keeps_original_order():
    result = _fallback_result(_children())
    assert result == {"ordered_children": ["JIRA-1", "JIRA-2"], "coordination_notes": "", "conflicts": []}


def test_fallback_result_empty_children():
    assert _fallback_result([]) == {"ordered_children": [], "coordination_notes": "", "conflicts": []}


def test_validate_result_accepts_valid_reorder():
    result = _validate_result(
        {"ordered_children": ["JIRA-2", "JIRA-1"], "coordination_notes": "invertido", "conflicts": ["algo"]},
        _children(),
    )
    assert result == {"ordered_children": ["JIRA-2", "JIRA-1"], "coordination_notes": "invertido", "conflicts": ["algo"]}


def test_validate_result_falls_back_when_missing_a_child():
    result = _validate_result({"ordered_children": ["JIRA-1"], "coordination_notes": "incompleto"}, _children())
    assert result["ordered_children"] == ["JIRA-1", "JIRA-2"]
    assert result["coordination_notes"] == "incompleto"


def test_validate_result_falls_back_when_ordered_children_missing():
    result = _validate_result({"coordination_notes": "sin orden"}, _children())
    assert result["ordered_children"] == ["JIRA-1", "JIRA-2"]


def test_plan_epic_returns_fallback_when_no_children():
    result = asyncio.run(plan_epic({"key": "EPIC-1", "summary": "s", "description": "d"}, []))
    assert result == {"ordered_children": [], "coordination_notes": "", "conflicts": []}


def test_plan_epic_falls_back_when_no_backend(monkeypatch):
    monkeypatch.setattr(epic_planner, "_select_backend", lambda: "none")
    result = asyncio.run(plan_epic({"key": "EPIC-1", "summary": "s", "description": "d"}, _children()))
    assert result["ordered_children"] == ["JIRA-1", "JIRA-2"]


def test_plan_epic_uses_model_reorder(monkeypatch):
    monkeypatch.setattr(epic_planner, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(epic_planner, "_connect_mcp_servers", fake_connect_mcp_servers)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [
            {
                "type": "text",
                "text": '{"ordered_children": ["JIRA-2", "JIRA-1"], "coordination_notes": "JIRA-2 primero", "conflicts": []}',
            }
        ]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(epic_planner, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(plan_epic({"key": "EPIC-1", "summary": "s", "description": "d"}, _children()))

    assert result["ordered_children"] == ["JIRA-2", "JIRA-1"]
    assert result["coordination_notes"] == "JIRA-2 primero"


def test_plan_epic_compacts_old_tool_results(monkeypatch):
    """Gap real (usuario, "hay gaps en el context window"): el loop de
    tools de epic_planner.py (solo neo4j-cypher, solo lectura) nunca
    compactaba resultados viejos -- confirma que ahora se llama
    compact_old_tool_results() con el set de tools ofrecidas."""
    monkeypatch.setattr(epic_planner, "_select_backend", lambda: "anthropic")

    fake_session = AsyncMock()
    fake_session.list_tools.return_value = SimpleNamespace(tools=[])

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {"neo4j-cypher": fake_session}

    monkeypatch.setattr(epic_planner, "_connect_mcp_servers", fake_connect_mcp_servers)
    monkeypatch.setattr(
        epic_planner, "_normalize_tool_schema",
        lambda name, tools: [{"name": "neo4j-cypher__query", "description": "d", "input_schema": {}}],
    )
    monkeypatch.setattr(epic_planner, "_call_mcp_tool", AsyncMock(return_value="resultado"))

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [{"type": "tool_use", "id": "call_1", "name": "neo4j-cypher__query", "input": {}}]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        content = [{"type": "text", "text": '{"ordered_children": ["JIRA-1", "JIRA-2"], "coordination_notes": "", "conflicts": []}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(epic_planner, "call_with_fallback", fake_call_with_fallback)

    compact_calls = []
    monkeypatch.setattr(epic_planner, "compact_old_tool_results", lambda messages, names: compact_calls.append(names))

    result = asyncio.run(plan_epic({"key": "EPIC-1", "summary": "s", "description": "d"}, _children()))

    assert compact_calls == [{"neo4j-cypher__query"}]
    assert result["ordered_children"] == ["JIRA-1", "JIRA-2"]


def test_plan_epic_falls_back_when_model_returns_invalid_json(monkeypatch):
    monkeypatch.setattr(epic_planner, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(epic_planner, "_connect_mcp_servers", fake_connect_mcp_servers)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [{"type": "text", "text": "esto no es json"}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    async def fake_json_retry(client, backend, messages, tools, system_prompt):
        return "sigue sin ser json", {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(epic_planner, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr(epic_planner, "_final_text_with_json_retry", fake_json_retry)

    result = asyncio.run(plan_epic({"key": "EPIC-1", "summary": "s", "description": "d"}, _children()))

    assert result["ordered_children"] == ["JIRA-1", "JIRA-2"]
