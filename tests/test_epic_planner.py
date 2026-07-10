"""Unit tests for epic_planner.py: the pure helpers (_fallback_result,
_validate_result) directly, and plan_epic() with call_with_fallback/MCP
mocked -- same pattern already used in tests/test_judge_agent.py for the
equivalent dual-backend agent loop.
"""
import asyncio

import pytest

import epic_planner
from epic_planner import _extract_json, _fallback_result, _validate_result, plan_epic


def _children():
    return [
        {"ticket_id": "JIRA-1", "summary": "s1", "description": "d1", "repository_origen": "AuthService"},
        {"ticket_id": "JIRA-2", "summary": "s2", "description": "d2", "repository_origen": "Frontend"},
    ]


def test_extract_json_strips_fenced_code_block():
    assert _extract_json('```json\n{"ordered_children": []}\n```') == {"ordered_children": []}


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
