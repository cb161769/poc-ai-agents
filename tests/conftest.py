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
