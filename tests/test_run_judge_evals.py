"""Unit tests for evals/run_judge_evals.py -- el harness de eval del juez.
judge_agent.judge_with_tools() se mockea siempre (no hay llamados reales a
un backend LLM ni MCP aca), mismo criterio que tests/test_judge_agent.py.
"""
import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "evals"))
import run_judge_evals  # noqa: E402

import judge_agent

_REAL_DIFF_TEXT = """diff --git a/frontend/src/lazyLoader.ts b/frontend/src/lazyLoader.ts
new file mode 100644
--- /dev/null
+++ b/frontend/src/lazyLoader.ts
@@ -0,0 +1,3 @@
+export function lazyLoad() {
+  return true;
+}
"""


def _base_case(**overrides):
    case = {
        "case_id": "case-1",
        "expected_verdict": "OK",
        "expected_policy_reference": None,
        "payload": {"change_source": "local_diff", "change_description": _REAL_DIFF_TEXT},
    }
    case.update(overrides)
    return case


def test_run_case_defaults_category_to_uncategorized(monkeypatch):
    monkeypatch.setattr(
        run_judge_evals.judge_agent, "judge_with_tools",
        AsyncMock(return_value={"verdict": "OK", "reasoning": "todo bien", "_meta": {}}),
    )

    result = asyncio.run(run_judge_evals.run_case(_base_case()))

    assert result["category"] == "uncategorized"


def test_run_case_keeps_explicit_category(monkeypatch):
    monkeypatch.setattr(
        run_judge_evals.judge_agent, "judge_with_tools",
        AsyncMock(return_value={"verdict": "OK", "reasoning": "todo bien", "_meta": {}}),
    )

    result = asyncio.run(run_judge_evals.run_case(_base_case(category="security")))

    assert result["category"] == "security"


def test_run_case_flags_self_contradictory_reasoning_even_when_verdict_correct(monkeypatch):
    """Gap real identificado en una auditoria de arquitectura previa: el
    juez puede ACERTAR el veredicto (FLAGGED, matchea expected_verdict) pero
    justificarlo con un reasoning que describe el cambio como inofensivo --
    antes eso no se detectaba en el eval, solo el accuracy de la etiqueta.
    """
    monkeypatch.setattr(
        run_judge_evals.judge_agent, "judge_with_tools",
        AsyncMock(return_value={
            "verdict": "FLAGGED", "change_assessment": "OK",
            "reasoning": "el cambio es inofensivo desde una perspectiva de seguridad.",
            "_meta": {},
        }),
    )

    result = asyncio.run(run_judge_evals.run_case(_base_case(expected_verdict="FLAGGED")))

    assert result["correct"] is True
    assert "self_contradictory" in result["reasoning_quality_flags"]


def test_run_case_no_reasoning_flags_for_clean_verdict(monkeypatch):
    monkeypatch.setattr(
        run_judge_evals.judge_agent, "judge_with_tools",
        AsyncMock(return_value={
            "verdict": "OK",
            "reasoning": "Agrega lazyLoader.ts para carga diferida, sin problemas reales.",
            "_meta": {},
        }),
    )

    result = asyncio.run(run_judge_evals.run_case(_base_case()))

    assert result["reasoning_quality_flags"] == []


def test_run_case_marks_tool_or_infra_failure_separately(monkeypatch):
    """Gap real identificado en una auditoria de arquitectura previa: un
    fallo real de herramienta/infra (ej. el bug real de get_neo4j_schema
    encontrado esta sesion) se contaba igual que un desacuerdo de juicio
    real -- ahora se marca aparte.
    """
    async def fake_judge_with_tools(payload):
        raise RuntimeError("get_neo4j_schema fallo")

    monkeypatch.setattr(run_judge_evals.judge_agent, "judge_with_tools", fake_judge_with_tools)

    result = asyncio.run(run_judge_evals.run_case(_base_case()))

    assert result["is_tool_or_infra_failure"] is True
    assert result["actual"] == "ERROR"
    assert result["reasoning_quality_flags"] == []


def test_run_case_not_marked_as_tool_failure_on_normal_verdict(monkeypatch):
    monkeypatch.setattr(
        run_judge_evals.judge_agent, "judge_with_tools",
        AsyncMock(return_value={"verdict": "OK", "reasoning": "todo bien", "_meta": {}}),
    )

    result = asyncio.run(run_judge_evals.run_case(_base_case()))

    assert result["is_tool_or_infra_failure"] is False


def test_eval_cases_with_real_diffs_are_parseable_by_deterministic_test_coverage_check():
    """Gap real identificado en auditoria ("identifica gaps" -- despues de
    agregar el chequeo deterministico de cobertura de tests a
    judge_agent.py): TODOS los 7 casos originales de judge_eval_cases.jsonl
    usaban change_description en formato "--- a/X"/"+++ b/X" (solo los
    hunks), SIN el header real "diff --git a/X b/Y" que 'git diff' real
    SIEMPRE emite -- confirmado real: _diff_changed_files() devolvia []
    para los 7, haciendolos silenciosamente incompatibles con cualquier
    chequeo deterministico basado en diff, pese a ser fixtures de
    regresion reales. Este test asegura que cualquier caso NUEVO con un
    change_source real (local_diff) que agregue archivos de codigo tenga
    el formato real, para que este drift no pueda volver a colarse sin
    que un test lo note.
    """
    cases = run_judge_evals.load_cases()
    local_diff_cases = [c for c in cases if c["payload"].get("change_source") == "local_diff"]
    assert local_diff_cases, "no hay ningun caso local_diff -- revisar el dataset"

    for case in local_diff_cases:
        diff_text = case["payload"]["change_description"]
        changed_files = judge_agent._diff_changed_files(diff_text)
        assert changed_files, (
            f"{case['case_id']}: change_description no tiene el header real 'diff --git a/X b/Y' -- "
            "_diff_changed_files() no puede parsear ningun archivo, asi que el chequeo deterministico "
            "de cobertura de tests nunca se ejercita para este caso."
        )


def test_missing_unit_tests_eval_case_exists_and_is_labeled_correctly():
    """Gap real identificado en auditoria: ningun caso del dataset original
    ejercitaba el escenario CONFIRMADO EN VIVO esta sesion (epica KAN-4,
    historia "Crear botones reutilizables"): un diff agrega codigo fuente
    real sin NINGUN archivo de test, y el testing agent no fallo porque un
    test PREEXISTENTE no relacionado seguia pasando -- dando una falsa
    señal de "tests reales PASARON". Confirma que el caso real existe y
    esta etiquetado con el policy_reference correcto."""
    cases = run_judge_evals.load_cases()
    case = next((c for c in cases if c["case_id"] == "missing-unit-tests-should-flag"), None)

    assert case is not None, "falta el caso 'missing-unit-tests-should-flag' en judge_eval_cases.jsonl"
    assert case["expected_verdict"] == "FLAGGED"
    assert case["expected_policy_reference"] == "insufficient-test-coverage"

    diff_text = case["payload"]["change_description"]
    changed_files = judge_agent._diff_changed_files(diff_text)
    evidence = judge_agent._test_coverage_evidence(case["payload"]["change_source"], diff_text)

    assert changed_files  # el diff es parseable
    assert "NO incluye NINGUN archivo de test" in evidence
