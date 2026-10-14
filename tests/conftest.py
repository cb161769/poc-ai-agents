"""Shared pytest fixtures for the whole tests/ suite.

generate_test_plan() (tech_doc_agent.py) is a real LLM call wired into
_deliver()/_deliver_epic_sequential() (orchestration.py) -- most existing
tests mock the coding agent/judge/etc but don't know about this new call,
so without a default mock they'd hit the real network (Anthropic/Ollama
fallback chain) on every run, which is slow and flaky in CI. Auto-mocked to
return None (test plan disabled) by default; a test that wants to exercise
the real wiring overrides it explicitly with monkeypatch.setattr.
"""
import pytest


@pytest.fixture(autouse=True)
def _default_no_test_plan(monkeypatch):
    import orchestration
    monkeypatch.setattr(orchestration, "generate_test_plan", lambda *a, **k: None)


@pytest.fixture(autouse=True)
def _default_no_gap_classification(monkeypatch):
    """Mismo criterio que _default_no_test_plan -- gap_classifier.classify_gap()
    (llamado desde _classify_and_report_gap en cada punto donde
    _deliver_epic_sequential corta una historia) es otro real LLM call que
    los tests existentes de bloqueo no conocen. None por default (best-effort,
    "no se pudo clasificar" -- el mismo comportamiento que si el backend no
    estuviera disponible); un test que quiera probar la clasificacion real
    lo sobreescribe con monkeypatch.setattr."""
    import gap_classifier
    monkeypatch.setattr(gap_classifier, "classify_gap", lambda *a, **k: None)


@pytest.fixture(autouse=True)
def _default_no_grounding_check(monkeypatch):
    """Mismo criterio que _default_no_gap_classification -- grounding_check.verify_claim_grounded()
    (llamado desde judge_agent.judge_with_tools()._finalize() para todo
    veredicto FLAGGED, y desde gap_classifier._classify_async()) es otro
    real LLM call (bespoke-minicheck) que los tests existentes de
    judge_agent no conocen. None por default (best-effort, "no se pudo
    verificar" -- mismo comportamiento que si bespoke-minicheck no
    estuviera disponible, nunca reescribe el reasoning); un test que quiera
    probar el grounding real lo sobreescribe con monkeypatch.setattr."""
    import judge_agent
    monkeypatch.setattr(judge_agent, "verify_claim_grounded", lambda *a, **k: None)


@pytest.fixture(autouse=True)
def _default_no_claude_cli(monkeypatch):
    """El backend claude_cli se auto-detecta (PATH / extension de VS Code),
    asi que en una maquina con Claude Code instalado cambiaria el backend
    elegido en tests que asumen solo anthropic/ollama (y haria llamadas
    reales al CLI). CLAUDE_CLI_PATH apuntando a un archivo inexistente lo
    marca como no disponible; un test del backend lo sobreescribe."""
    monkeypatch.setenv("CLAUDE_CLI_PATH", "/nonexistent/claude")
