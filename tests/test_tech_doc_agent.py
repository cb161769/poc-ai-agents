"""Unit tests for tech_doc_agent.py -- el comprobante tecnico lo redacta
el propio backend LLM via call_with_fallback (mockeado aca, sin red real);
estos tests solo verifican el envoltorio: que se arme el prompt con la
evidencia real, que el texto devuelto se prefije con el backend real que
respondio, y que cualquier fallo se trague en silencio (best-effort).
"""
import pytest

import tech_doc_agent


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


def test_generate_technical_report_returns_none_on_empty_text(monkeypatch):
    monkeypatch.setattr(tech_doc_agent, "TECH_DOC_ENABLED", True)

    async def fake_call_with_fallback(*a, **k):
        return ([{"type": "text", "text": "   "}], "end_turn", {}, "ollama")

    monkeypatch.setattr(tech_doc_agent, "call_with_fallback", fake_call_with_fallback)

    assert tech_doc_agent.generate_technical_report({"epica": "EPIC-1"}) is None
