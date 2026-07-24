"""Unit tests for gap_classifier.py -- la clasificacion la redacta el
propio backend LLM via call_with_fallback (mockeado aca, sin red real);
estos tests verifican el envoltorio: que se arme el prompt con la evidencia
real, que el resultado se parsee/valide, y que cualquier fallo se trague en
silencio (best-effort, nunca bloquea la corrida real).
"""
import pytest

import gap_classifier

# tests/conftest.py tiene un fixture autouse (_default_no_gap_classification)
# que monkeypatchea gap_classifier.classify_gap a "siempre None" para el
# RESTO de la suite (evita que tests de orchestration.py ajenos a esto
# disparen un LLM call real). Ese mismo autouse rompe estos tests, que
# necesitan la implementacion REAL de classify_gap (con call_with_fallback
# mockeado, no la funcion entera). Capturar la referencia real ACA, al
# importar el modulo (antes de que cualquier fixture corra), la deja
# inmune a ese monkeypatch posterior.
_real_classify_gap = gap_classifier.classify_gap


def test_classify_gap_returns_none_when_disabled(monkeypatch):
    monkeypatch.setattr(gap_classifier, "GAP_CLASSIFIER_ENABLED", False)

    async def boom(*a, **k):
        pytest.fail("no deberia llamar al backend si esta deshabilitado")
    monkeypatch.setattr(gap_classifier, "call_with_fallback", boom)

    assert _real_classify_gap({"motivo_del_bloqueo": "tests fallaron"}) is None


def test_classify_gap_parses_infra_bug_with_proposal(monkeypatch):
    monkeypatch.setattr(gap_classifier, "GAP_CLASSIFIER_ENABLED", True)
    monkeypatch.setattr(gap_classifier, "verify_claim_grounded", lambda document, claim: True)
    captured = {}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, ollama_model=None, force_json=False, json_schema=None):
        captured["messages"] = messages
        captured["json_schema"] = json_schema
        text = (
            '{"bucket": "infra_bug", "reasoning": "el comando capacitor no esta reconocido como stack runtime", '
            '"proposal": {"suspected_file": "coding_agent.py", "suspected_root_cause": '
            '"_STACK_RUNTIME_IMAGES no incluye capacitor", "suggested_fix": "agregar capacitor/cap al mapeo", '
            '"regression_test_sketch": "test que confirma _resolve_stack_image_if_needed reconoce cap"}}'
        )
        return [{"type": "text", "text": text}], "end_turn", {}, "ollama"

    monkeypatch.setattr(gap_classifier, "call_with_fallback", fake_call_with_fallback)

    result = _real_classify_gap({"motivo_del_bloqueo": "comando 'capacitor' no encontrado"})

    assert result["bucket"] == "infra_bug"
    assert result["proposal"]["suspected_file"] == "coding_agent.py"
    assert captured["json_schema"] == gap_classifier.GAP_CLASSIFIER_RESULT_SCHEMA
    assert "comando 'capacitor' no encontrado" in captured["messages"][0]["content"]


def test_classify_gap_forces_proposal_null_for_non_infra_buckets(monkeypatch):
    """Confirmado real: aunque el modelo devuelva una 'proposal' para un
    bucket que no es infra_bug (alucinacion/inconsistencia), nunca se debe
    reportar una propuesta de codigo para un content_gap/model_ceiling --
    esos bloqueos no se resuelven editando el pipeline.
    """
    monkeypatch.setattr(gap_classifier, "GAP_CLASSIFIER_ENABLED", True)
    monkeypatch.setattr(gap_classifier, "verify_claim_grounded", lambda document, claim: True)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, ollama_model=None, force_json=False, json_schema=None):
        text = (
            '{"bucket": "model_ceiling", "reasoning": "el modelo no sabe mockear async de forma confiable", '
            '"proposal": {"suspected_file": "coding_agent.py", "suspected_root_cause": "x", "suggested_fix": "y", "regression_test_sketch": "z"}}'
        )
        return [{"type": "text", "text": text}], "end_turn", {}, "ollama"

    monkeypatch.setattr(gap_classifier, "call_with_fallback", fake_call_with_fallback)

    result = _real_classify_gap({"motivo_del_bloqueo": "insufficient-test-coverage tras agotar reintentos"})

    assert result["bucket"] == "model_ceiling"
    assert result["proposal"] is None


def test_classify_gap_flags_ungrounded_reasoning(monkeypatch):
    """Gap real confirmado en vivo esta sesion: gap_classifier clasifico
    "infra_bug" correctamente pero su "reasoning" citaba un dato especifico
    FALSO (una imagen real que SI estaba disponible, ya confirmado en vivo,
    pero el modelo dijo que no). bespoke-minicheck (grounding_check.py)
    ahora confirma si el reasoning esta de verdad respaldado por la
    evidencia real -- si no, se marca explicitamente en vez de dejarlo
    pasar como un hecho verificado.
    """
    monkeypatch.setattr(gap_classifier, "GAP_CLASSIFIER_ENABLED", True)
    monkeypatch.setattr(gap_classifier, "verify_claim_grounded", lambda document, claim: False)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, ollama_model=None, force_json=False, json_schema=None):
        text = '{"bucket": "infra_bug", "reasoning": "la imagen X no esta disponible (esto es falso)", "proposal": null}'
        return [{"type": "text", "text": text}], "end_turn", {}, "ollama"

    monkeypatch.setattr(gap_classifier, "call_with_fallback", fake_call_with_fallback)

    result = _real_classify_gap({"motivo_del_bloqueo": "x"})

    assert result["reasoning_grounded"] is False
    assert "NO se pudo confirmar" in result["reasoning"]
    assert "la imagen X no esta disponible" in result["reasoning"]  # el reasoning original se preserva, no se descarta


def test_classify_gap_does_not_flag_reasoning_when_grounding_uncertain(monkeypatch):
    """grounded=None (no se pudo verificar, ej. bespoke-minicheck no
    disponible) NUNCA se trata igual que grounded=False -- best-effort real,
    no inventa una advertencia que no pudo confirmar."""
    monkeypatch.setattr(gap_classifier, "GAP_CLASSIFIER_ENABLED", True)
    monkeypatch.setattr(gap_classifier, "verify_claim_grounded", lambda document, claim: None)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, ollama_model=None, force_json=False, json_schema=None):
        text = '{"bucket": "content_gap", "reasoning": "razonamiento normal", "proposal": null}'
        return [{"type": "text", "text": text}], "end_turn", {}, "ollama"

    monkeypatch.setattr(gap_classifier, "call_with_fallback", fake_call_with_fallback)

    result = _real_classify_gap({"motivo_del_bloqueo": "x"})

    assert result["reasoning_grounded"] is None
    assert result["reasoning"] == "razonamiento normal"


def test_classify_gap_returns_none_on_invalid_json(monkeypatch):
    monkeypatch.setattr(gap_classifier, "GAP_CLASSIFIER_ENABLED", True)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, ollama_model=None, force_json=False, json_schema=None):
        return [{"type": "text", "text": "no puedo ayudar con eso"}], "end_turn", {}, "ollama"

    monkeypatch.setattr(gap_classifier, "call_with_fallback", fake_call_with_fallback)

    assert _real_classify_gap({"motivo_del_bloqueo": "x"}) is None


def test_classify_gap_returns_none_on_invalid_bucket_value(monkeypatch):
    monkeypatch.setattr(gap_classifier, "GAP_CLASSIFIER_ENABLED", True)

    async def fake_call_with_fallback(client, messages, tools, system_prompt, ollama_model=None, force_json=False, json_schema=None):
        return [{"type": "text", "text": '{"bucket": "not_a_real_bucket", "reasoning": "x"}'}], "end_turn", {}, "ollama"

    monkeypatch.setattr(gap_classifier, "call_with_fallback", fake_call_with_fallback)

    assert _real_classify_gap({"motivo_del_bloqueo": "x"}) is None


def test_classify_gap_returns_none_when_backend_call_fails(monkeypatch):
    monkeypatch.setattr(gap_classifier, "GAP_CLASSIFIER_ENABLED", True)

    async def boom(*a, **k):
        raise RuntimeError("ollama no disponible")

    monkeypatch.setattr(gap_classifier, "call_with_fallback", boom)

    assert _real_classify_gap({"motivo_del_bloqueo": "x"}) is None
