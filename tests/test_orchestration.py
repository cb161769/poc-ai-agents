"""Unit tests for orchestration.py's pure epic-mode helper (deciding
whether all of an epic's children live in the same repo) and the
FLAGGED-retry helper (_retry_local_diff / the retry gate in _deliver()).
No real Neo4j/Jira/coding-agent network calls -- every Prefect task the
retry path touches is monkeypatched at the module level.
"""
import pytest

import orchestration
from orchestration import PipelineBlocked, _resolve_single_repo, _retry_local_diff


def test_resolve_single_repo_ok_when_all_agree():
    ok, repo_url, reason = _resolve_single_repo({"AuthService": "https://github.com/org/repo", "Frontend": "https://github.com/org/repo"})

    assert ok is True
    assert repo_url == "https://github.com/org/repo"
    assert reason == ""


def test_resolve_single_repo_rejects_when_repo_url_missing():
    ok, repo_url, reason = _resolve_single_repo({"AuthService": "https://github.com/org/repo", "Frontend": None})

    assert ok is False
    assert repo_url is None
    assert "Frontend" in reason


def test_resolve_single_repo_rejects_when_repos_differ():
    ok, repo_url, reason = _resolve_single_repo(
        {"AuthService": "https://github.com/org/repo-a", "DataWorker": "https://github.com/org/repo-b"}
    )

    assert ok is False
    assert repo_url is None
    assert "repo-a" in reason
    assert "repo-b" in reason


def test_resolve_single_repo_ok_with_single_component():
    ok, repo_url, reason = _resolve_single_repo({"AuthService": "https://github.com/org/repo"})

    assert ok is True
    assert repo_url == "https://github.com/org/repo"


_AGENT_RESULT = {"branch": "copilot/T-1-123", "base_branch": "main"}
_FLAGGED_RETRYABLE = {"verdict": "FLAGGED", "reasoning": "alcance raro", "policy_reference": "scope-mismatch"}


def test_retry_local_diff_returns_new_verdict_when_retry_applies_changes(monkeypatch):
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: {"applied": True, "backend": "anthropic"})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "2 tests passed"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff text del segundo intento")
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "corregido"})

    result = _retry_local_diff(
        "T-1", "prompt original", "/repo", _AGENT_RESULT, _FLAGGED_RETRYABLE,
        {"ticket_id": "T-1"}, {"status": "APPROVED"}, ["AuthService"], "summary",
        False, None, "2026-01-01T00:00:00Z",
    )

    assert result == {"verdict": "OK", "reasoning": "corregido"}


def test_retry_local_diff_returns_none_when_no_new_changes(monkeypatch):
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: {"applied": False, "backend": None})

    result = _retry_local_diff(
        "T-1", "prompt original", "/repo", _AGENT_RESULT, _FLAGGED_RETRYABLE,
        {"ticket_id": "T-1"}, {"status": "APPROVED"}, ["AuthService"], "summary",
        False, None, "2026-01-01T00:00:00Z",
    )

    assert result is None


def test_retry_local_diff_blocks_when_retry_tests_fail(monkeypatch):
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: {"applied": True, "backend": "anthropic"})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": False, "output": "fallo"})
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)

    with pytest.raises(PipelineBlocked):
        _retry_local_diff(
            "T-1", "prompt original", "/repo", _AGENT_RESULT, _FLAGGED_RETRYABLE,
            {"ticket_id": "T-1"}, {"status": "APPROVED"}, ["AuthService"], "summary",
            False, None, "2026-01-01T00:00:00Z",
        )


def test_retryable_policy_references_matches_judge_agent():
    """orchestration.py duplica esta constante (judge_agent.py se invoca
    como subprocess, no se importa) -- este test es la red de seguridad
    para que no se desincronicen.
    """
    import judge_agent

    assert orchestration.RETRYABLE_POLICY_REFERENCES == judge_agent.RETRYABLE_POLICY_REFERENCES
