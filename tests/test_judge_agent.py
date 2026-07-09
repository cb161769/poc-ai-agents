"""Unit tests for judge_agent.py's pure helpers: JSON extraction from the
model's reply, the Anthropic<->Ollama message-shape adapters, and the cost
estimator. No network, no MCP servers, no model calls involved.
"""
import asyncio
from unittest.mock import patch

import pytest

import judge_agent
from judge_agent import (
    JUDGE_POLICY_IDS,
    RETRYABLE_POLICY_REFERENCES,
    _build_user_prompt,
    _estimate_cost_usd,
    _extract_json,
    _messages_to_ollama,
    _normalize_policy_reference,
    _ollama_response_to_blocks,
    _redact_payload_for_logging,
)


def test_extract_json_from_plain_json():
    assert _extract_json('{"verdict": "OK", "reasoning": "todo bien"}') == {
        "verdict": "OK",
        "reasoning": "todo bien",
    }


def test_extract_json_strips_json_fenced_code_block():
    text = '```json\n{"verdict": "FLAGGED", "reasoning": "sospechoso"}\n```'
    assert _extract_json(text) == {"verdict": "FLAGGED", "reasoning": "sospechoso"}


def test_extract_json_strips_plain_fenced_code_block():
    text = '```\n{"verdict": "OK", "reasoning": "ok"}\n```'
    assert _extract_json(text) == {"verdict": "OK", "reasoning": "ok"}


def test_extract_json_with_surrounding_whitespace():
    assert _extract_json('\n  {"verdict": "OK"}  \n') == {"verdict": "OK"}


def test_messages_to_ollama_string_content_passthrough():
    messages = [{"role": "user", "content": "hello"}]
    assert _messages_to_ollama(messages) == [{"role": "user", "content": "hello"}]


def test_messages_to_ollama_assistant_text_and_tool_use():
    messages = [
        {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "pensando"},
                {"type": "tool_use", "name": "neo4j-cypher__read", "input": {"query": "MATCH (n) RETURN n"}},
            ],
        }
    ]
    result = _messages_to_ollama(messages)
    assert result == [
        {
            "role": "assistant",
            "content": "pensando",
            "tool_calls": [{"function": {"name": "neo4j-cypher__read", "arguments": {"query": "MATCH (n) RETURN n"}}}],
        }
    ]


def test_messages_to_ollama_tool_result_becomes_tool_role():
    messages = [{"role": "user", "content": [{"type": "tool_result", "content": "resultado real de la tool"}]}]
    assert _messages_to_ollama(messages) == [{"role": "tool", "content": "resultado real de la tool"}]


def test_ollama_response_to_blocks_text_only():
    blocks, stop_reason = _ollama_response_to_blocks({"content": "todo ok"})
    assert blocks == [{"type": "text", "text": "todo ok"}]
    assert stop_reason == "end_turn"


def test_ollama_response_to_blocks_with_tool_call_dict_arguments():
    message = {
        "content": "voy a chequear el grafo",
        "tool_calls": [{"function": {"name": "neo4j-cypher__read", "arguments": {"query": "MATCH (n) RETURN n"}}}],
    }
    blocks, stop_reason = _ollama_response_to_blocks(message)
    assert stop_reason == "tool_use"
    assert blocks[0] == {"type": "text", "text": "voy a chequear el grafo"}
    assert blocks[1]["type"] == "tool_use"
    assert blocks[1]["name"] == "neo4j-cypher__read"
    assert blocks[1]["input"] == {"query": "MATCH (n) RETURN n"}


def test_ollama_response_to_blocks_with_tool_call_string_arguments():
    message = {"tool_calls": [{"function": {"name": "qdrant-rag__find", "arguments": '{"query": "auth bug"}'}}]}
    blocks, stop_reason = _ollama_response_to_blocks(message)
    assert stop_reason == "tool_use"
    tool_block = next(b for b in blocks if b["type"] == "tool_use")
    assert tool_block["input"] == {"query": "auth bug"}


@pytest.mark.parametrize(
    "input_tokens,output_tokens,expected",
    [
        (1_000_000, 0, 3.0),
        (0, 1_000_000, 15.0),
        (0, 0, 0.0),
    ],
)
def test_estimate_cost_usd_known_model(input_tokens, output_tokens, expected):
    assert _estimate_cost_usd("anthropic", "claude-sonnet-5", input_tokens, output_tokens) == expected


def test_estimate_cost_usd_unknown_model_is_zero():
    assert _estimate_cost_usd("anthropic", "some-model-not-in-the-pricing-table", 1000, 1000) == 0.0


def test_estimate_cost_usd_ollama_backend_is_always_zero():
    assert _estimate_cost_usd("ollama", "llama3.1", 1_000_000, 1_000_000) == 0.0


def test_estimate_cost_usd_unknown_backend_is_zero():
    assert _estimate_cost_usd("some-future-backend", "claude-sonnet-5", 1_000_000, 1_000_000) == 0.0


def test_redact_payload_for_logging_redacts_ticket_description_and_diff():
    payload = {
        "ticket": {"ticket_id": "T-1", "description": "la conexion usa password=Sup3rS3cr3t!"},
        "firewall": {"status": "APPROVED", "reason": None, "redactions_applied": 0},
        "change_source": "local_diff",
        "change_description": "+    private static final String DB_PASSWORD = \"password=OtroSecreto123!\";",
        "test_summary": "3 tests, 3 passed",
    }

    redacted = _redact_payload_for_logging(payload)

    assert "Sup3rS3cr3t" not in redacted["ticket"]["description"]
    assert "OtroSecreto123" not in redacted["change_description"]
    assert "[REDACTED_CORPORATE_SECRET]" in redacted["ticket"]["description"]
    assert "[REDACTED_CORPORATE_SECRET]" in redacted["change_description"]

    # El payload original no se muta -- log_verdict() no debe alterar lo que
    # ya se le mando al modelo.
    assert "Sup3rS3cr3t" in payload["ticket"]["description"]
    assert "OtroSecreto123" in payload["change_description"]


def test_redact_payload_for_logging_leaves_clean_text_untouched():
    payload = {
        "ticket": {"ticket_id": "T-2", "description": "el boton no muestra el spinner de carga"},
        "firewall": {"status": "APPROVED", "reason": None, "redactions_applied": 0},
        "change_source": "issue_only",
        "change_description": "Agregar spinner al boton de login",
        "test_summary": "sin cambios aplicados",
    }

    redacted = _redact_payload_for_logging(payload)

    assert redacted["ticket"]["description"] == payload["ticket"]["description"]
    assert redacted["change_description"] == payload["change_description"]


def test_normalize_policy_reference_sets_null_for_ok_verdict():
    verdict = _normalize_policy_reference({"verdict": "OK", "reasoning": "todo bien"})
    assert verdict["policy_reference"] is None


def test_normalize_policy_reference_keeps_valid_id_for_flagged_verdict():
    verdict = _normalize_policy_reference({"verdict": "FLAGGED", "policy_reference": "scope-mismatch"})
    assert verdict["policy_reference"] == "scope-mismatch"


def test_normalize_policy_reference_falls_back_to_other_when_missing():
    verdict = _normalize_policy_reference({"verdict": "FLAGGED"})
    assert verdict["policy_reference"] == "other"


def test_normalize_policy_reference_falls_back_to_other_when_invalid_id():
    verdict = _normalize_policy_reference({"verdict": "FLAGGED", "policy_reference": "made-up-id"})
    assert verdict["policy_reference"] == "other"


def test_judge_policy_ids_include_other_as_fallback():
    assert "other" in JUDGE_POLICY_IDS


def _base_judge_payload(**overrides):
    payload = {
        "ticket": {"ticket_id": "T-1", "summary": "Fix login", "description": "desc", "repository_origen": "AuthService"},
        "firewall": {"status": "APPROVED", "reason": None, "redactions_applied": 0},
        "change_source": "local_diff",
        "change_description": "diff",
        "test_summary": "tests passed",
    }
    payload.update(overrides)
    return payload


def test_build_user_prompt_defaults_to_pointwise_rubric():
    prompt = _build_user_prompt(_base_judge_payload())

    assert "Modo de evaluación: pointwise" in prompt


def test_build_user_prompt_includes_reference_grounded_context():
    prompt = _build_user_prompt(
        _base_judge_payload(reference_answer="La forma correcta de resolver esto es X.")
    )

    assert "Modo de evaluación: reference_grounded" in prompt
    assert "La forma correcta de resolver esto es X." in prompt


def test_build_user_prompt_includes_self_review_when_present():
    prompt = _build_user_prompt(
        _base_judge_payload(
            self_review={"scope_matches_ticket": True, "no_secrets_introduced": False, "tests_adequate": True}
        )
    )

    assert "Autoevaluacion del coding agent" in prompt
    assert "no_secrets_introduced: False" in prompt


def test_build_user_prompt_omits_self_review_section_when_absent():
    prompt = _build_user_prompt(_base_judge_payload())

    assert "Autoevaluacion del coding agent" not in prompt


def test_build_user_prompt_includes_falco_section_when_alerts_present():
    prompt = _build_user_prompt(
        _base_judge_payload(
            falco_summary={"count": 1, "alerts": [{"priority": "Warning", "rule": "Write below binary dir", "output": "..."}]}
        )
    )

    assert "Falco" in prompt
    assert "Write below binary dir" in prompt


def test_build_user_prompt_omits_falco_section_without_alerts():
    prompt = _build_user_prompt(_base_judge_payload(falco_summary={"count": 0, "alerts": []}))

    assert "DURANTE esta misma corrida" not in prompt


def test_build_user_prompt_includes_conflicts_section_when_present():
    prompt = _build_user_prompt(_base_judge_payload(conflicts=["AuthService y Frontend tocan el mismo endpoint"]))

    assert "conflictos potenciales" in prompt
    assert "AuthService y Frontend tocan el mismo endpoint" in prompt


def test_build_user_prompt_omits_conflicts_section_when_absent():
    prompt = _build_user_prompt(_base_judge_payload())

    assert "conflictos potenciales" not in prompt


def test_build_user_prompt_includes_new_sonar_issues_section_when_present():
    prompt = _build_user_prompt(
        _base_judge_payload(new_sonar_issues=["[MAJOR] rule:S123: algo nuevo (linea 42)"])
    )

    assert "re-escaneó" in prompt or "re-escaneo" in prompt.lower()
    assert "[MAJOR] rule:S123: algo nuevo (linea 42)" in prompt


def test_build_user_prompt_omits_new_sonar_issues_section_when_absent():
    prompt = _build_user_prompt(_base_judge_payload())

    assert "NO existían antes del diff" not in prompt


def test_judge_system_prompt_embeds_policy_rubric():
    assert "scope-mismatch" in judge_agent.JUDGE_SYSTEM_PROMPT
    assert "graph-impact-unverified" in judge_agent.JUDGE_SYSTEM_PROMPT


def test_retryable_policy_references_excludes_security_criteria():
    assert "data-leak-evidence" not in RETRYABLE_POLICY_REFERENCES
    assert "jailbreak-evidence" not in RETRYABLE_POLICY_REFERENCES
    assert "firewall-false-negative" not in RETRYABLE_POLICY_REFERENCES
    assert "other" not in RETRYABLE_POLICY_REFERENCES
    assert RETRYABLE_POLICY_REFERENCES <= set(JUDGE_POLICY_IDS)
    assert "scope-mismatch" in RETRYABLE_POLICY_REFERENCES


def test_judge_tool_query_sonar_formats_issues():
    with patch("judge_agent.sonar_client.get_issues") as mock_get_issues:
        mock_get_issues.return_value = {
            "issues": [
                {"severity": "CRITICAL", "rule": "python:S105", "message": "Hardcoded password", "line": 5},
            ]
        }
        result = judge_agent.tool_query_sonar("AuthService")

    mock_get_issues.assert_called_once_with("AuthService")
    assert "CRITICAL" in result
    assert "Hardcoded password" in result


def test_judge_tool_query_sonar_no_issues():
    with patch("judge_agent.sonar_client.get_issues") as mock_get_issues:
        mock_get_issues.return_value = {"issues": []}
        result = judge_agent.tool_query_sonar("Frontend")

    assert "sin hallazgos" in result


def test_judge_with_tools_dispatches_local_tool_over_mcp(monkeypatch):
    """Cuando el modelo llama a query_sonar, el loop de judge_with_tools()
    tiene que resolverlo via JUDGE_LOCAL_TOOLS -- no intentar _call_mcp_tool
    (que fallaria porque no hay ninguna sesion MCP conectada en este test).
    """
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [
                {
                    "type": "tool_use",
                    "id": "call_1",
                    "name": "query_sonar",
                    "input": {"component": "AuthService"},
                }
            ]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        content = [
            {
                "type": "text",
                "text": (
                    '{"verdict": "OK", "firewall_assessment": "ok", '
                    '"change_assessment": "ok", "reasoning": "listo"}'
                ),
            }
        ]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)

    with patch("judge_agent.sonar_client.get_issues") as mock_get_issues:
        mock_get_issues.return_value = {"issues": []}
        payload = {
            "ticket": {
                "ticket_id": "T-1",
                "summary": "algo",
                "description": "desc",
                "repository_origen": "AuthService",
            },
            "firewall": {"status": "APPROVED", "reason": None, "redactions_applied": 0},
            "change_source": "local_diff",
            "change_description": "diff",
            "test_summary": "3 tests pasaron",
        }
        result = asyncio.run(judge_agent.judge_with_tools(payload))

    assert result["verdict"] == "OK"
    mock_get_issues.assert_called_once_with("AuthService")
