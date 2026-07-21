"""Unit tests for judge_agent.py's pure helpers: JSON extraction from the
model's reply, the Anthropic<->Ollama message-shape adapters, and the cost
estimator. No network, no MCP servers, no model calls involved.
"""
import asyncio
from unittest.mock import MagicMock, patch

import pytest

import agent_loop
import judge_agent
from judge_agent import (
    JUDGE_CONSISTENCY_NUDGE_MESSAGE,
    JUDGE_CONTENT_HALLUCINATION_NUDGE_MESSAGE,
    JUDGE_POLICY_IDS,
    RETRYABLE_POLICY_REFERENCES,
    _build_user_prompt,
    _estimate_cost_usd,
    _extract_diff_file_basenames,
    _extract_json,
    _messages_to_ollama,
    _normalize_policy_reference,
    _ollama_response_to_blocks,
    _reasoning_ignores_real_diff_files,
    _redact_payload_for_logging,
    _verdict_is_self_contradictory,
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


def test_verdict_is_self_contradictory_true_for_positive_change_assessment():
    """Caso real confirmado (KAN-15, parable/fable): verdict=FLAGGED con
    change_assessment="OK" -- el propio juez contradice su veredicto."""
    verdict = {"verdict": "FLAGGED", "change_assessment": "OK", "reasoning": "algo raro"}
    assert _verdict_is_self_contradictory(verdict) is True


def test_verdict_is_self_contradictory_true_for_safe_sounding_reasoning():
    """Caso real confirmado (KAN-15): reasoning dice 'inofensivo' pero el
    veredicto es FLAGGED."""
    verdict = {
        "verdict": "FLAGGED",
        "reasoning": "El cambio es un archivo HTML estatico... el cambio es inofensivo desde una perspectiva de seguridad.",
    }
    assert _verdict_is_self_contradictory(verdict) is True


def test_verdict_is_self_contradictory_true_for_safe_sounding_reasoning_in_english():
    """Caso real confirmado (KAN-5, parable/fable via Ollama): el juez
    respondio en INGLES pese al prompt en espanol -- "low-risk... resolves
    the ticket without introducing risks" con verdict=FLAGGED. El patron
    original solo cubria frases en espanol y no detecto la contradiccion."""
    verdict = {
        "verdict": "FLAGGED",
        "reasoning": (
            "This is a low-risk addition (a single utility function with no side effects) "
            "and matches the user's request. No new secrets were introduced, and the tests "
            "pass, so the change resolves the ticket without introducing risks."
        ),
    }
    assert _verdict_is_self_contradictory(verdict) is True


def test_verdict_is_self_contradictory_false_for_ok_verdict():
    verdict = {"verdict": "OK", "change_assessment": "OK", "reasoning": "todo perfecto, inofensivo"}
    assert _verdict_is_self_contradictory(verdict) is False


def test_verdict_is_self_contradictory_false_when_reasoning_explains_real_problem():
    verdict = {
        "verdict": "FLAGGED",
        "change_assessment": "incompleto",
        "reasoning": "El diff solo crea 404.html pero el ticket tambien pide una pagina 500 -- alcance incompleto.",
    }
    assert _verdict_is_self_contradictory(verdict) is False


_REAL_DIFF_TEXT = """diff --git a/frontend/src/lazyLoader.ts b/frontend/src/lazyLoader.ts
new file mode 100644
--- /dev/null
+++ b/frontend/src/lazyLoader.ts
@@
+export class LazyLoader {}
diff --git a/frontend/src/guards/authGuard.ts b/frontend/src/guards/authGuard.ts
new file mode 100644
--- /dev/null
+++ b/frontend/src/guards/authGuard.ts
@@
+export class AuthGuard {}
"""


def test_extract_diff_file_basenames_reads_real_plus_plus_plus_lines():
    assert _extract_diff_file_basenames(_REAL_DIFF_TEXT) == ["lazyLoader.ts", "authGuard.ts"]


def test_extract_diff_file_basenames_empty_for_no_diff_markers():
    assert _extract_diff_file_basenames("solo texto plano, no es un diff") == []


def test_reasoning_ignores_real_diff_files_true_for_unrelated_content():
    """Caso real confirmado (KAN-5, parable/fable): el diff real crea
    lazyLoader.ts/authGuard.ts, pero el juez describio un `selectBrowser`/
    `PLAYWRIGHT_BROWSER` que no tiene nada que ver -- alucinacion de
    contenido, no de tono (el veredicto en si no era autocontradictorio)."""
    payload = {"change_source": "local_diff", "change_description": _REAL_DIFF_TEXT}
    verdict = {
        "verdict": "FLAGGED",
        "reasoning": "Adds a selectBrowser helper gated by PLAYWRIGHT_BROWSER, falling back to Chromium.",
    }
    assert _reasoning_ignores_real_diff_files(payload, verdict) is True


def test_reasoning_ignores_real_diff_files_false_when_reasoning_cites_real_file():
    payload = {"change_source": "local_diff", "change_description": _REAL_DIFF_TEXT}
    verdict = {"verdict": "OK", "reasoning": "El cambio agrega lazyLoader.ts para carga diferida, sin riesgos reales."}
    assert _reasoning_ignores_real_diff_files(payload, verdict) is False


def test_reasoning_ignores_real_diff_files_false_when_no_basenames_in_diff():
    payload = {"change_source": "local_diff", "change_description": "diff"}
    verdict = {"verdict": "FLAGGED", "reasoning": "cualquier cosa"}
    assert _reasoning_ignores_real_diff_files(payload, verdict) is False


def test_reasoning_ignores_real_diff_files_false_in_issue_only_mode():
    payload = {"change_source": "issue_only", "change_description": _REAL_DIFF_TEXT}
    verdict = {"verdict": "FLAGGED", "reasoning": "no hay diff real todavia"}
    assert _reasoning_ignores_real_diff_files(payload, verdict) is False


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


def test_build_user_prompt_warns_no_diff_exists_in_issue_only_mode():
    """Bug real confirmado en vivo (KAN-15 con parable/fable): la frase
    ambigua vieja ("diff real, o el texto del issue") hacia que el juez
    alucinara un diff completo con archivos y rutas inventadas a partir de
    solo el texto del ticket. El prompt en modo issue_only tiene que dejar
    explicito que NO hay ningun diff.
    """
    prompt = _build_user_prompt(
        _base_judge_payload(change_source="issue_only", change_description="Como usuario quiero X", test_summary="sin cambios aplicados")
    )

    assert "NO HAY NINGUN DIFF" in prompt
    assert "no existen" in prompt


def test_build_user_prompt_local_diff_mode_does_not_include_no_diff_warning():
    prompt = _build_user_prompt(_base_judge_payload(change_source="local_diff"))

    assert "NO HAY NINGUN DIFF" not in prompt


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


def test_build_user_prompt_includes_epic_context_when_present():
    """Gap real identificado en auditoria ("el juez necesita evaluar que
    hay unit tests y que el codigo corresponde al ticket -- HISTORIA O
    EPICA"): en modo epica secuencial, el juez evaluaba cada historia
    contra SU PROPIA descripcion puntual -- nunca sabia que la epica
    completa podia exigir un stack tecnico o requisitos de testing
    especificos. Confirmado real: una epica que pedia 'Ionic Angular +
    Capacitor' con tests unitarios obligatorios termino con un veredicto
    OK sobre un frontend Vite/vitest sin un solo test."""
    prompt = _build_user_prompt(
        _base_judge_payload(
            ticket={
                "ticket_id": "T-1", "summary": "Fix login", "description": "desc",
                "repository_origen": "AuthService",
                "epic_context": "Stack requerido: Ionic Angular + Capacitor. Requiere pruebas unitarias.",
            },
        )
    )

    assert "Contexto general de la épica" in prompt
    assert "Ionic Angular + Capacitor" in prompt


def test_build_user_prompt_omits_epic_context_section_when_absent():
    prompt = _build_user_prompt(_base_judge_payload())

    assert "Contexto general de la épica" not in prompt


def test_diff_changed_files_parses_real_unified_diff_headers():
    diff_text = (
        "diff --git a/frontend/src/components/button.ts b/frontend/src/components/button.ts\n"
        "new file mode 100644\n"
        "diff --git a/frontend/package-lock.json b/frontend/package-lock.json\n"
        "new file mode 100644\n"
    )

    files = judge_agent._diff_changed_files(diff_text)

    assert files == ["frontend/src/components/button.ts", "frontend/package-lock.json"]


def test_diff_changed_files_empty_for_no_diff():
    assert judge_agent._diff_changed_files("") == []


def test_test_coverage_evidence_flags_missing_tests_deterministically():
    """Gap real identificado en auditoria ("el juez necesita evaluar que
    hay unit tests"): 'insufficient-test-coverage' ya existia como
    policy_reference, pero era PURAMENTE cualitativo -- dependia de que el
    juez, por su cuenta, notara la ausencia de tests. Confirmado real: un
    veredicto OK sobre botones/cards reales de una epica sin un solo
    archivo de test, sin que el juez lo señalara. Esto lo detecta
    deterministicamente a partir de los paths reales del diff."""
    diff_text = (
        "diff --git a/frontend/src/components/button.ts b/frontend/src/components/button.ts\n"
        "new file mode 100644\n"
    )

    evidence = judge_agent._test_coverage_evidence("local_diff", diff_text)

    assert "NO incluye NINGUN archivo de test" in evidence
    assert "frontend/src/components/button.ts" in evidence


def test_test_coverage_evidence_recognizes_real_test_files():
    diff_text = (
        "diff --git a/frontend/src/components/button.ts b/frontend/src/components/button.ts\n"
        "new file mode 100644\n"
        "diff --git a/frontend/src/components/button.test.ts b/frontend/src/components/button.test.ts\n"
        "new file mode 100644\n"
    )

    evidence = judge_agent._test_coverage_evidence("local_diff", diff_text)

    assert "SI incluye archivo(s) de test" in evidence
    assert "button.test.ts" in evidence


def test_test_coverage_evidence_ignores_lockfiles_and_docs_only_diffs():
    diff_text = (
        "diff --git a/README.md b/README.md\nindex a..b 100644\n"
        "diff --git a/frontend/package-lock.json b/frontend/package-lock.json\nnew file mode 100644\n"
    )

    assert judge_agent._test_coverage_evidence("local_diff", diff_text) == ""


def test_test_coverage_evidence_empty_for_issue_only_mode():
    """Sin diff real (issue_only, el coding agent en la nube todavia no
    tiene PR), no hay archivos que analizar -- nunca debe inventar
    evidencia sobre un diff que no existe."""
    diff_text = "diff --git a/frontend/src/components/button.ts b/frontend/src/components/button.ts\n"

    assert judge_agent._test_coverage_evidence("issue_only", diff_text) == ""


def test_build_user_prompt_includes_test_coverage_section_when_tests_missing():
    diff_text = "diff --git a/frontend/src/components/button.ts b/frontend/src/components/button.ts\nnew file mode 100644\n"

    prompt = _build_user_prompt(_base_judge_payload(change_source="local_diff", change_description=diff_text))

    assert "Cobertura de tests del diff real" in prompt
    assert "NO incluye NINGUN archivo de test" in prompt
    assert "insufficient-test-coverage" in prompt


def test_build_user_prompt_shows_positive_test_coverage_when_tests_present():
    """La seccion tambien se muestra cuando SI hay tests reales -- es
    evidencia positiva util para el juez, no solo una alerta de ausencia."""
    diff_text = (
        "diff --git a/frontend/src/components/button.ts b/frontend/src/components/button.ts\nnew file mode 100644\n"
        "diff --git a/frontend/src/components/button.test.ts b/frontend/src/components/button.test.ts\nnew file mode 100644\n"
    )

    prompt = _build_user_prompt(_base_judge_payload(change_source="local_diff", change_description=diff_text))

    assert "Cobertura de tests del diff real" in prompt
    assert "SI incluye archivo(s) de test" in prompt


def test_build_user_prompt_omits_test_coverage_section_for_docs_only_diff():
    diff_text = "diff --git a/README.md b/README.md\nindex a..b 100644\n"

    prompt = _build_user_prompt(_base_judge_payload(change_source="local_diff", change_description=diff_text))

    assert "Cobertura de tests del diff real" not in prompt


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


def test_judge_system_prompt_requires_reasoning_consistent_with_verdict():
    assert "consistentes con tu \"verdict\"" in judge_agent.JUDGE_SYSTEM_PROMPT


def test_judge_system_prompt_requires_evidence_not_absence_of_doubt():
    """Auditoria real del prompt: 'evidencia insuficiente' no debe leerse
    como 'todo esta bien' -- el juez tiene que marcar FLAGGED ante duda, no
    asumir OK por falta de evidencia en contra."""
    assert "Evidencia insuficiente NO es lo mismo que evidencia de que todo está bien" in judge_agent.JUDGE_SYSTEM_PROMPT


def test_judge_system_prompt_treats_tool_failures_as_risk_signal():
    assert "tratalo como una señal de riesgo real" in judge_agent.JUDGE_SYSTEM_PROMPT


def test_judge_system_prompt_distinguishes_proposed_from_applied_change():
    assert "\"cambio propuesto\"" in judge_agent.JUDGE_SYSTEM_PROMPT
    assert "\"cambio realmente aplicado\"" in judge_agent.JUDGE_SYSTEM_PROMPT


def test_judge_system_prompt_requires_tests_match_same_revision():
    assert "MISMA versión del cambio" in judge_agent.JUDGE_SYSTEM_PROMPT


def test_judge_system_prompt_requires_comparison_against_acceptance_criteria():
    assert "criterios de aceptación" in judge_agent.JUDGE_SYSTEM_PROMPT
    assert "Gherkin" in judge_agent.JUDGE_SYSTEM_PROMPT


def test_judge_system_prompt_requires_verifiable_evidence_in_reasoning():
    assert "una frase cualitativa sin evidencia citada" in judge_agent.JUDGE_SYSTEM_PROMPT


def test_judge_system_prompt_warns_against_deprecated_cypher_exists_syntax():
    """Bug real confirmado esta sesion: el juez (parable/fable) escribia
    Cypher con la sintaxis vieja exists(x.prop) (deprecada en Neo4j 5.x),
    gastaba varios de sus 6 turnos en CypherSyntaxError, y terminaba
    agotando MAX_TOOL_TURNS sin dar veredicto (judge_agent.py:512)."""
    assert "IS NOT NULL" in judge_agent.JUDGE_SYSTEM_PROMPT
    assert "exists(variable.propiedad)" in judge_agent.JUDGE_SYSTEM_PROMPT


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

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
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


def test_judge_with_tools_compacts_old_tool_results(monkeypatch):
    """Gap real (usuario, "hay gaps en el context window"): coding_agent.py
    ya compacta resultados viejos de tools de solo lectura tras cada turno,
    pero judge_with_tools() nunca lo hacia -- su propio loop de tools
    (query_sonar/neo4j-cypher/qdrant-rag, todas de solo lectura) reenviaba
    el historial completo turno tras turno. Este test confirma que ahora
    compact_old_tool_results() se llama con el set de tools ofrecidas."""
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    call_count = {"n": 0}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            content = [{"type": "tool_use", "id": "call_1", "name": "query_sonar", "input": {"component": "AuthService"}}]
            return content, "tool_use", {"input_tokens": 1, "output_tokens": 1}, "anthropic"
        content = [{"type": "text", "text": '{"verdict": "OK", "firewall_assessment": "ok", "change_assessment": "ok", "reasoning": "listo"}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)

    compact_calls = []
    monkeypatch.setattr(judge_agent, "compact_old_tool_results", lambda messages, names: compact_calls.append(names))

    with patch("judge_agent.sonar_client.get_issues") as mock_get_issues:
        mock_get_issues.return_value = {"issues": []}
        payload = {
            "ticket": {"ticket_id": "T-1", "summary": "algo", "description": "desc", "repository_origen": "AuthService"},
            "firewall": {"status": "APPROVED", "reason": None, "redactions_applied": 0},
            "change_source": "local_diff", "change_description": "diff", "test_summary": "3 tests pasaron",
        }
        asyncio.run(judge_agent.judge_with_tools(payload))

    assert compact_calls == [{"query_sonar"}]


def test_judge_with_tools_nudges_once_on_self_contradictory_verdict_and_accepts_correction(monkeypatch):
    """Caso real (KAN-15): el 1er veredicto es FLAGGED con reasoning que
    dice "inofensivo" -- tiene que recibir el nudge de consistencia y, si
    el 2do intento corrige a OK, devolver ese veredicto corregido."""
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    call_count = {"n": 0}
    seen_messages = []

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        seen_messages.append(list(messages))
        if call_count["n"] == 1:
            content = [{
                "type": "text",
                "text": (
                    '{"verdict": "FLAGGED", "firewall_assessment": "ok", '
                    '"change_assessment": "OK", "reasoning": "el cambio es inofensivo desde una perspectiva de seguridad."}'
                ),
            }]
        else:
            content = [{
                "type": "text",
                "text": '{"verdict": "OK", "firewall_assessment": "ok", "change_assessment": "ok", "reasoning": "corregido, no hay problema real"}',
            }]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(judge_agent.judge_with_tools(_base_judge_payload()))

    assert call_count["n"] == 2
    assert result["verdict"] == "OK"
    # el 2do llamado tiene que haber visto el mensaje de nudge en la conversacion
    assert any(
        m.get("content") == JUDGE_CONSISTENCY_NUDGE_MESSAGE
        for m in seen_messages[1]
        if isinstance(m, dict)
    )


def test_judge_with_tools_nudges_once_on_content_hallucination_and_accepts_correction(monkeypatch):
    """Caso real (KAN-5): el 1er veredicto cita un cambio que no existe en
    el diff real -- tiene que recibir el nudge de contenido y, si el 2do
    intento corrige citando un archivo real, devolver ese veredicto."""
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    call_count = {"n": 0}
    seen_messages = []

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        seen_messages.append(list(messages))
        if call_count["n"] == 1:
            content = [{
                "type": "text",
                "text": (
                    '{"verdict": "FLAGGED", "reasoning": '
                    '"Adds a selectBrowser helper gated by PLAYWRIGHT_BROWSER."}'
                ),
            }]
        else:
            content = [{
                "type": "text",
                "text": '{"verdict": "OK", "reasoning": "Agrega lazyLoader.ts para carga diferida, sin problemas reales."}',
            }]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)

    payload = _base_judge_payload(change_description=_REAL_DIFF_TEXT)
    result = asyncio.run(judge_agent.judge_with_tools(payload))

    assert call_count["n"] == 2
    assert result["verdict"] == "OK"
    assert any(
        m.get("content") == JUDGE_CONTENT_HALLUCINATION_NUDGE_MESSAGE
        for m in seen_messages[1]
        if isinstance(m, dict)
    )


def test_judge_with_tools_accepts_verdict_if_still_contradictory_after_one_nudge(monkeypatch):
    """Un solo empujon -- si el 2do intento repite la misma contradiccion,
    se acepta igual (no bloquea infinito, mismo criterio que el resto de
    los nudges de esta sesion)."""
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    call_count = {"n": 0}
    contradictory_text = (
        '{"verdict": "FLAGGED", "firewall_assessment": "ok", '
        '"change_assessment": "OK", "reasoning": "el cambio es inofensivo desde una perspectiva de seguridad."}'
    )

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        call_count["n"] += 1
        return [{"type": "text", "text": contradictory_text}], "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(judge_agent.judge_with_tools(_base_judge_payload()))

    assert call_count["n"] == 2  # 1 original + 1 nudge, no un 3ro
    assert result["verdict"] == "FLAGGED"  # se acepta igual, sin loop infinito


def test_judge_with_tools_retries_when_json_valid_but_verdict_key_missing(monkeypatch):
    """Bug real confirmado esta sesion (KeyError: 'verdict' en
    orchestration.py::_deliver): el juez puede devolver JSON sintacticamente
    valido pero sin la clave "verdict" (o con un valor invalido) -- eso NO
    dispara json.JSONDecodeError, asi que sin este chequeo el resultado
    ambiguo se aceptaba tal cual. Tiene que disparar el mismo reintento de
    correccion que un JSON invalido."""
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [{"type": "text", "text": '{"status": "ok", "reasoning": "listo"}'}]  # sin "verdict"
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    async def fake_json_retry(client, backend, messages, tools, system_prompt, **kwargs):
        return '{"verdict": "OK", "firewall_assessment": "ok", "change_assessment": "ok", "reasoning": "corregido"}', {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr(judge_agent, "_final_text_with_json_retry", fake_json_retry)

    result = asyncio.run(judge_agent.judge_with_tools(_base_judge_payload()))

    assert result["verdict"] == "OK"
    assert result["reasoning"] == "corregido"


def test_judge_with_tools_passes_real_json_schema_to_retry(monkeypatch):
    """Gap real identificado en auditoria de herramientas: format:"json" en
    Ollama solo garantiza JSON valido, cualquiera -- no el esquema real que
    judge_agent.py espera. El reintento de correccion ahora le pasa el
    esquema real (JUDGE_RESULT_SCHEMA) via json_schema para que Ollama
    restrinja el decoding a ese esquema exacto."""
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [{"type": "text", "text": "esto no es json"}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    captured = {}

    async def fake_json_retry(client, backend, messages, tools, system_prompt, **kwargs):
        captured.update(kwargs)
        return '{"verdict": "OK", "reasoning": "listo"}', {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr(judge_agent, "_final_text_with_json_retry", fake_json_retry)

    asyncio.run(judge_agent.judge_with_tools(_base_judge_payload()))

    assert captured["json_schema"] == judge_agent.JUDGE_RESULT_SCHEMA


def test_judge_with_tools_nudges_and_corrects_hallucinated_verdict_from_json_retry(monkeypatch):
    """Bug real confirmado en vivo esta sesion (investigacion KAN-5, dos
    vueltas): el camino de reintento de JSON invalido (_final_text_with_json_retry)
    devolvia su resultado SIN pasar nunca por _verdict_is_self_contradictory
    ni _reasoning_ignores_real_diff_files -- un bypass completo. Un primer
    fix solo lo logueaba (aceptaba igual, el usuario vio ese comentario
    contradictorio real publicado en Jira y reacciono con frustracion) --
    ahora tiene que darle al modelo una correccion real (un llamado mas, no
    solo el nudge en memoria) antes de aceptar."""
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [{"type": "text", "text": "esto no es JSON en absoluto"}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    async def fake_json_retry(client, backend, messages, tools, system_prompt, **kwargs):
        return (
            '{"verdict": "FLAGGED", "reasoning": '
            '"Adds a selectBrowser helper gated by PLAYWRIGHT_BROWSER."}',
            {"input_tokens": 1, "output_tokens": 1},
        )

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt, **kwargs):
        # La correccion real -- esta vez el modelo cita un archivo real del diff.
        content = [{
            "type": "text",
            "text": '{"verdict": "OK", "reasoning": "Agrega lazyLoader.ts para carga diferida, sin problemas reales."}',
        }]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr(judge_agent, "_final_text_with_json_retry", fake_json_retry)
    monkeypatch.setattr(judge_agent, "_call_model_turn", fake_call_model_turn)

    payload = _base_judge_payload(change_description=_REAL_DIFF_TEXT)
    result = asyncio.run(judge_agent.judge_with_tools(payload))

    assert result["verdict"] == "OK"  # la correccion real reemplazo el veredicto alucinado
    assert result["reasoning"] == "Agrega lazyLoader.ts para carga diferida, sin problemas reales."


def test_judge_with_tools_accepts_original_verdict_if_correction_also_fails(monkeypatch, caplog):
    """Si la correccion post-reintento TAMPOCO trae un verdict valido, se
    acepta el veredicto original (detectado como sospechoso) igual -- nunca
    bloquea infinito -- pero con una advertencia real en el log."""
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [{"type": "text", "text": "esto no es JSON en absoluto"}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    async def fake_json_retry(client, backend, messages, tools, system_prompt, **kwargs):
        return (
            '{"verdict": "FLAGGED", "reasoning": '
            '"Adds a selectBrowser helper gated by PLAYWRIGHT_BROWSER."}',
            {"input_tokens": 1, "output_tokens": 1},
        )

    async def fake_call_model_turn(client, backend, messages, tools, system_prompt, **kwargs):
        content = [{"type": "text", "text": "sigo sin poder devolver JSON valido"}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr(judge_agent, "_final_text_with_json_retry", fake_json_retry)
    monkeypatch.setattr(judge_agent, "_call_model_turn", fake_call_model_turn)

    payload = _base_judge_payload(change_description=_REAL_DIFF_TEXT)
    with caplog.at_level("WARNING"):
        result = asyncio.run(judge_agent.judge_with_tools(payload))

    assert result["verdict"] == "FLAGGED"  # veredicto original, se acepta igual
    assert any("no fue JSON valido" in r.message for r in caplog.records)


def test_judge_with_tools_raises_when_retry_also_lacks_valid_verdict(monkeypatch):
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "anthropic")

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [{"type": "text", "text": '{"status": "ok"}'}]  # sin "verdict"
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "anthropic"

    async def fake_json_retry(client, backend, messages, tools, system_prompt, **kwargs):
        return '{"status": "todavia sin verdict"}', {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr(judge_agent, "_final_text_with_json_retry", fake_json_retry)

    with pytest.raises(RuntimeError, match="verdict"):
        asyncio.run(judge_agent.judge_with_tools(_base_judge_payload()))


def _mock_ollama_tags(monkeypatch, model_names):
    resp = MagicMock()
    resp.json.return_value = {"models": [{"name": n} for n in model_names]}
    monkeypatch.setattr(agent_loop.httpx, "get", lambda *a, **k: resp)


def test_judge_with_tools_switches_ollama_model_when_retry_also_lacks_valid_verdict(monkeypatch):
    """Con 2 candidatos configurados y backend ollama: si el reintento de
    JSON tampoco trae un 'verdict' valido, antes de levantar RuntimeError
    prueba con el segundo modelo de la lista.
    """
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "ollama")
    monkeypatch.setattr(judge_agent, "JUDGE_OLLAMA_MODELS", ["modelo-a", "modelo-b"])
    _mock_ollama_tags(monkeypatch, ["modelo-a:latest", "modelo-b:latest"])

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        if kwargs.get("ollama_model") == "modelo-b":
            content = [{"type": "text", "text": '{"verdict": "OK", "reasoning": "listo con el segundo modelo"}'}]
            return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "ollama"
        content = [{"type": "text", "text": '{"status": "ok"}'}]  # sin "verdict"
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "ollama"

    async def fake_json_retry(client, backend, messages, tools, system_prompt, **kwargs):
        return '{"status": "todavia sin verdict"}', {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr(judge_agent, "_final_text_with_json_retry", fake_json_retry)

    result = asyncio.run(judge_agent.judge_with_tools(_base_judge_payload()))

    assert result["verdict"] == "OK"
    assert result["reasoning"] == "listo con el segundo modelo"


def test_judge_with_tools_raises_when_only_one_ollama_candidate_and_retry_lacks_verdict(monkeypatch):
    """Con un solo candidato (el caso de hoy), el comportamiento tiene que
    seguir siendo identico: sin otro modelo al que cambiar, sigue
    levantando RuntimeError como siempre.
    """
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "ollama")
    monkeypatch.setattr(judge_agent, "JUDGE_OLLAMA_MODELS", ["modelo-a"])
    _mock_ollama_tags(monkeypatch, ["modelo-a:latest"])

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        content = [{"type": "text", "text": '{"status": "ok"}'}]
        return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "ollama"

    async def fake_json_retry(client, backend, messages, tools, system_prompt, **kwargs):
        return '{"status": "todavia sin verdict"}', {"input_tokens": 1, "output_tokens": 1}

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)
    monkeypatch.setattr(judge_agent, "_final_text_with_json_retry", fake_json_retry)

    with pytest.raises(RuntimeError, match="verdict"):
        asyncio.run(judge_agent.judge_with_tools(_base_judge_payload()))


def test_judge_with_tools_switches_ollama_model_when_verdict_still_contradictory_after_nudge(monkeypatch):
    """Con 2 candidatos y backend ollama: si el veredicto SIGUE
    auto-contradictorio incluso despues del nudge de consistencia (el
    empujon ya se gasto), antes de aceptarlo igual prueba con el segundo
    modelo de la lista.
    """
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "ollama")
    monkeypatch.setattr(judge_agent, "JUDGE_OLLAMA_MODELS", ["modelo-a", "modelo-b"])
    _mock_ollama_tags(monkeypatch, ["modelo-a:latest", "modelo-b:latest"])

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    contradictory_text = (
        '{"verdict": "FLAGGED", "firewall_assessment": "ok", '
        '"change_assessment": "OK", "reasoning": "el cambio es inofensivo desde una perspectiva de seguridad."}'
    )

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        if kwargs.get("ollama_model") == "modelo-b":
            content = [{"type": "text", "text": '{"verdict": "OK", "reasoning": "corregido con el segundo modelo"}'}]
            return content, "end_turn", {"input_tokens": 1, "output_tokens": 1}, "ollama"
        return [{"type": "text", "text": contradictory_text}], "end_turn", {"input_tokens": 1, "output_tokens": 1}, "ollama"

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(judge_agent.judge_with_tools(_base_judge_payload()))

    assert result["verdict"] == "OK"
    assert result["reasoning"] == "corregido con el segundo modelo"


def test_judge_with_tools_still_accepts_verdict_when_only_one_ollama_candidate(monkeypatch):
    """Con un solo candidato, el veredicto que sigue contradictorio tras el
    nudge se sigue aceptando igual que antes (sin otro modelo, no hay
    cambio posible).
    """
    monkeypatch.setattr(judge_agent, "_select_backend", lambda: "ollama")
    monkeypatch.setattr(judge_agent, "JUDGE_OLLAMA_MODELS", ["modelo-a"])
    _mock_ollama_tags(monkeypatch, ["modelo-a:latest"])

    async def fake_connect_mcp_servers(stack, servers, label="agente"):
        return {}

    monkeypatch.setattr(judge_agent, "_connect_mcp_servers", fake_connect_mcp_servers)

    contradictory_text = (
        '{"verdict": "FLAGGED", "firewall_assessment": "ok", '
        '"change_assessment": "OK", "reasoning": "el cambio es inofensivo desde una perspectiva de seguridad."}'
    )

    async def fake_call_with_fallback(client, messages, tools, system_prompt, exclude=None, **kwargs):
        return [{"type": "text", "text": contradictory_text}], "end_turn", {"input_tokens": 1, "output_tokens": 1}, "ollama"

    monkeypatch.setattr(judge_agent, "call_with_fallback", fake_call_with_fallback)

    result = asyncio.run(judge_agent.judge_with_tools(_base_judge_payload()))

    assert result["verdict"] == "FLAGGED"
