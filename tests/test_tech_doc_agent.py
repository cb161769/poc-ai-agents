"""Unit tests for tech_doc_agent.py -- el comprobante tecnico lo redacta
el propio backend LLM via call_with_fallback (mockeado aca, sin red real);
estos tests solo verifican el envoltorio: que se arme el prompt con la
evidencia real, que el texto devuelto se prefije con el backend real que
respondio, y que cualquier fallo se trague en silencio (best-effort).
"""
import pytest

import tech_doc_agent
from tech_doc_agent import _looks_like_refusal, _strip_filler_sections


def test_generate_technical_report_returns_none_when_disabled(monkeypatch):
    monkeypatch.setattr(tech_doc_agent, "TECH_DOC_ENABLED", False)

    async def boom(*a, **k):
        pytest.fail("no deberia llamar al backend si esta deshabilitado")
    monkeypatch.setattr(tech_doc_agent, "call_with_fallback", boom)

    assert tech_doc_agent.generate_technical_report({"epica": "EPIC-1"}) is None


def test_generate_technical_report_returns_text_prefixed_with_real_backend(monkeypatch):
    monkeypatch.setattr(tech_doc_agent, "TECH_DOC_ENABLED", True)
    captured = {}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, ollama_model=None):
        captured["messages"] = messages
        captured["tools"] = tools
        return (
            [{"type": "text", "text": "# Comprobante\n\nContenido real generado por el modelo."}],
            "end_turn",
            {},
            "ollama",
        )

    monkeypatch.setattr(tech_doc_agent, "call_with_fallback", fake_call_with_fallback)

    result = tech_doc_agent.generate_technical_report({"epica": "EPIC-1", "backend_usado": "ollama"})

    assert result is not None
    assert "backend 'ollama'" in result
    assert "Contenido real generado por el modelo." in result
    assert captured["tools"] == []
    assert "EPIC-1" in captured["messages"][0]["content"]


def test_generate_technical_report_returns_none_when_no_backend_available(monkeypatch):
    monkeypatch.setattr(tech_doc_agent, "TECH_DOC_ENABLED", True)

    async def fake_call_with_fallback(*a, **k):
        raise RuntimeError("ningun backend disponible (ni alcanzable ni dentro de presupuesto)")

    monkeypatch.setattr(tech_doc_agent, "call_with_fallback", fake_call_with_fallback)

    assert tech_doc_agent.generate_technical_report({"epica": "EPIC-1"}) is None


def test_generate_test_plan_returns_none_when_disabled(monkeypatch):
    monkeypatch.setattr(tech_doc_agent, "TEST_PLAN_ENABLED", False)

    async def boom(*a, **k):
        pytest.fail("no deberia llamar al backend si esta deshabilitado")
    monkeypatch.setattr(tech_doc_agent, "call_with_fallback", boom)

    assert tech_doc_agent.generate_test_plan({"ticket": "T-1"}) is None


def test_generate_test_plan_returns_text_prefixed_with_real_backend(monkeypatch):
    """Testing Agent liviano (evaluacion del workflow multi-agente pedida por
    el usuario): reusa la MISMA infraestructura de un solo llamado real a
    LLM que ya usa generate_technical_report -- este test confirma que el
    Test Plan real llega con el mismo envoltorio (backend real, sin relleno)."""
    monkeypatch.setattr(tech_doc_agent, "TEST_PLAN_ENABLED", True)
    captured = {}

    async def fake_call_with_fallback(client, messages, tools, system_prompt, ollama_model=None):
        captured["messages"] = messages
        captured["system_prompt"] = system_prompt
        return (
            [{"type": "text", "text": "# Test Plan\n\n## Casos Negativos\n- entrada invalida"}],
            "end_turn",
            {},
            "ollama",
        )

    monkeypatch.setattr(tech_doc_agent, "call_with_fallback", fake_call_with_fallback)

    result = tech_doc_agent.generate_test_plan({"ticket": "T-1", "resumen": "Agregar boton de login"})

    assert result is not None
    assert "backend 'ollama'" in result
    assert "entrada invalida" in result
    assert captured["system_prompt"] is tech_doc_agent._TEST_PLAN_SYSTEM_PROMPT
    assert "T-1" in captured["messages"][0]["content"]


def test_generate_test_plan_returns_none_when_backend_fails(monkeypatch):
    monkeypatch.setattr(tech_doc_agent, "TEST_PLAN_ENABLED", True)

    async def fake_call_with_fallback(*a, **k):
        raise RuntimeError("ningun backend disponible")

    monkeypatch.setattr(tech_doc_agent, "call_with_fallback", fake_call_with_fallback)

    assert tech_doc_agent.generate_test_plan({"ticket": "T-1"}) is None


def test_generate_test_plan_strips_filler_and_detects_refusal(monkeypatch):
    """generate_test_plan reusa _strip_filler_sections/_looks_like_refusal
    (no las duplica) -- confirma que un rechazo del modelo se trata como
    None, no como contenido real."""
    monkeypatch.setattr(tech_doc_agent, "TEST_PLAN_ENABLED", True)

    async def fake_refusal(client, messages, tools, system_prompt, ollama_model=None):
        return ([{"type": "text", "text": "Lo siento, pero no puedo generar ese contenido."}], "end_turn", {}, "ollama")

    monkeypatch.setattr(tech_doc_agent, "call_with_fallback", fake_refusal)

    assert tech_doc_agent.generate_test_plan({"ticket": "T-1"}) is None


def test_test_plan_system_prompt_requires_at_least_one_negative_case():
    assert "OBLIGATORIO al menos uno" in tech_doc_agent._TEST_PLAN_SYSTEM_PROMPT
    assert "Casos Negativos NUNCA se omite" in tech_doc_agent._TEST_PLAN_SYSTEM_PROMPT


def test_system_prompt_requires_omitting_sections_without_real_data():
    """Auditoria real (confirmado esta sesion en KAN-2/KAN-4): el prompt
    viejo pedia rellenar las 7 secciones SIEMPRE, lo que producia relleno
    generico ("no se proporciona informacion... no fue provista en los
    datos reales") en vez de omitir la seccion sin contenido real."""
    assert "OMITILA POR COMPLETO" in tech_doc_agent._TECH_REPORT_SYSTEM_PROMPT
    assert "es mejor que uno completo con relleno" in tech_doc_agent._TECH_REPORT_SYSTEM_PROMPT


def test_strip_filler_sections_removes_real_boilerplate_observed_live():
    """Caso real confirmado en vivo (KAN-5, epica KAN-4): pedirle al modelo
    por prompt que omita secciones sin datos no alcanzo -- ollama igual
    "completo el patron" de las 7 secciones con relleno. Este texto es
    (recortado) el comprobante real que devolvio el modelo esa corrida."""
    text = (
        "**Resumen Ejecutivo y Objetivo**\n"
        "-----------------------------\n\n"
        "La corrida realizada correspondio a la epica KAN-4 y la historia KAN-5.\n\n"
        "**Configuración del Entorno y Variables de Entorno**\n"
        "-------------------------------------------------\n\n"
        "No se proporciona información sobre la configuración del entorno o las "
        "variables de entorno utilizadas en esta corrida.\n\n"
        "**Resultado Real de la Corrida**\n"
        "------------------------------\n\n"
        "* **Resultado:** Bloqueada\n\n"
        "**Prueba de Integración y Validación de API**\n"
        "------------------------------------------\n\n"
        "No se proporciona información sobre la prueba de integración y validación "
        "de API realizada en esta corrida.\n"
    )

    cleaned = _strip_filler_sections(text)

    assert "Configuración del Entorno" not in cleaned
    assert "Prueba de Integración y Validación de API" not in cleaned
    assert "No se proporciona información" not in cleaned
    assert "Resumen Ejecutivo y Objetivo" in cleaned
    assert "Resultado Real de la Corrida" in cleaned
    assert "**Resultado:** Bloqueada" in cleaned


def test_strip_filler_sections_removes_plural_conjugation_observed_live():
    """Caso real confirmado en vivo (KAN-5, segunda corrida despues del
    primer fix): "No se proporcionAN metricas..." (plural) no matcheaba con
    el patron anterior (solo cubria singular/pasado), asi que esta seccion
    de relleno seguia colandose pese al fix."""
    text = (
        "**Métricas de Rendimiento y Eficiencia (KPIs)**\n"
        "--------------------------------------------\n\n"
        "No se proporcionan métricas de rendimiento ni eficiencia.\n"
    )
    assert _strip_filler_sections(text) == text  # sin otras secciones, se devuelve el original (nunca vacio)

    text_with_real_section = (
        "**Resumen Ejecutivo**\n---------------------\n\nContenido real con datos concretos de la corrida.\n\n"
        + text
    )
    cleaned = _strip_filler_sections(text_with_real_section)
    assert "Resumen Ejecutivo" in cleaned
    assert "Métricas de Rendimiento" not in cleaned


def test_strip_filler_sections_keeps_section_with_real_content():
    text = (
        "**Ficha Tecnica del Modelo y Entorno**\n"
        "-----------------------------------\n\n"
        "* **Backend utilizado:** Ollama\n"
        "* **Modelo Ollama Coding Agent:** Ornith:9b\n"
    )

    assert _strip_filler_sections(text) == text.strip()


def test_strip_filler_sections_keeps_long_section_mentioning_filler_phrase_in_passing():
    """Una seccion con contenido real sustancial no se descarta solo porque
    UNA frase adentro se parezca al patron de relleno -- solo se descarta
    si el cuerpo ENTERO es puro relleno (cuerpo corto)."""
    long_body = (
        "El coding agent aplico un cambio real en src/services/logger.ts, con 57 "
        "inserciones. Los tests reales (vitest) pasaron 3/3. Nota: el tiempo de "
        "carga en frio del modelo no se midieron con precision, pero el resto de "
        "las metricas reales de esta corrida si estan disponibles y documentadas "
        "arriba con el detalle correspondiente para auditoria completa."
    )
    text = f"**Resultado Real de la Corrida**\n------------------------------\n\n{long_body}\n"

    assert _strip_filler_sections(text) == text


def test_strip_filler_sections_no_headings_returns_text_unchanged():
    assert _strip_filler_sections("solo texto plano, sin encabezados") == "solo texto plano, sin encabezados"


def test_strip_filler_sections_never_returns_empty_string():
    """Si TODAS las secciones fueran relleno (caso extremo), preferir
    devolver el texto original a un comprobante completamente vacio."""
    text = "**Seccion**\n-----------\n\nNo se proporciona información sobre esto.\n"

    assert _strip_filler_sections(text) == text


def test_looks_like_refusal_detects_real_spanish_refusal_observed_live():
    """Caso real confirmado en vivo (KAN-5, epica KAN-4): el modelo se nego
    por completo a generar el comprobante -- un falso positivo de seguridad
    (la evidencia real, ej. un nombre de variable de entorno, no es un
    secreto en si mismo)."""
    text = (
        "Lo siento, pero no puedo generar un comprobante de desarrollo técnico "
        "que incluya información confidencial o sensible. ¿Hay algo más en lo "
        "que pueda ayudarte?"
    )
    assert _looks_like_refusal(text) is True


def test_looks_like_refusal_false_for_real_report_text():
    assert _looks_like_refusal("**Resumen Ejecutivo y Objetivo**\n\nLa corrida...") is False


def test_generate_technical_report_returns_none_when_model_refuses(monkeypatch):
    monkeypatch.setattr(tech_doc_agent, "TECH_DOC_ENABLED", True)

    async def fake_call_with_fallback(*a, **k):
        return (
            [{"type": "text", "text": "Lo siento, pero no puedo generar contenido que pueda ser confidencial."}],
            "end_turn",
            {},
            "ollama",
        )

    monkeypatch.setattr(tech_doc_agent, "call_with_fallback", fake_call_with_fallback)

    assert tech_doc_agent.generate_technical_report({"epica": "EPIC-1"}) is None


def test_generate_technical_report_returns_none_on_empty_text(monkeypatch):
    monkeypatch.setattr(tech_doc_agent, "TECH_DOC_ENABLED", True)

    async def fake_call_with_fallback(*a, **k):
        return ([{"type": "text", "text": "   "}], "end_turn", {}, "ollama")

    monkeypatch.setattr(tech_doc_agent, "call_with_fallback", fake_call_with_fallback)

    assert tech_doc_agent.generate_technical_report({"epica": "EPIC-1"}) is None
