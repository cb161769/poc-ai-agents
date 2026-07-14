"""Unit tests for orchestration.py's pure epic-mode helper (deciding
whether all of an epic's children live in the same repo) and the
FLAGGED-retry helper (_retry_local_diff / the retry gate in _deliver()).
No real Neo4j/Jira/coding-agent network calls -- every Prefect task the
retry path touches is monkeypatched at the module level.
"""
import json
import os
import sys
from pathlib import Path

import pytest

import orchestration
from orchestration import (
    PipelineBlocked,
    _check_not_epic,
    _comment_all,
    _deliver,
    _format_conflicts_section,
    _handle_rejected,
    _resolve_single_repo,
    _retry_after_no_changes,
    _retry_local_diff,
    _transition_all,
)


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


_CLEAN_GUARD_RESULT = {"redactions_applied": 0, "jailbreak_reason": None, "clean": True}


def test_retry_local_diff_returns_new_verdict_when_retry_applies_changes(monkeypatch):
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: {"applied": True, "backend": "anthropic"})
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "2 tests passed"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff text del segundo intento")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
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
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff text del segundo intento")
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


def test_retry_local_diff_blocks_when_output_guard_finds_a_leak(monkeypatch):
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: {"applied": True, "backend": "anthropic"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff text con un secreto")
    monkeypatch.setattr(
        orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 1, "jailbreak_reason": None, "clean": False}
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)

    with pytest.raises(PipelineBlocked, match="guardia de salida"):
        _retry_local_diff(
            "T-1", "prompt original", "/repo", _AGENT_RESULT, _FLAGGED_RETRYABLE,
            {"ticket_id": "T-1"}, {"status": "APPROVED"}, ["AuthService"], "summary",
            False, None, "2026-01-01T00:00:00Z",
        )


def test_retry_local_diff_passes_conversation_file_when_present(monkeypatch):
    """Si el primer intento genero un conversation_file, _retry_local_diff
    tiene que pasarselo a retry_coding_agent_local_real() para que la
    conversacion continue en vez de repagar la investigacion.
    """
    captured = {}

    def fake_retry(ticket_id, feedback_text, target_repo_dir, conversation_file=None):
        captured["feedback_text"] = feedback_text
        captured["conversation_file"] = conversation_file
        return {"applied": True, "backend": "anthropic"}

    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", fake_retry)
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "2 tests passed"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff text del segundo intento")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "corregido"})

    agent_result_with_conversation = {**_AGENT_RESULT, "conversation_file": "/tmp/some_conversation.json"}

    _retry_local_diff(
        "T-1", "prompt original", "/repo", agent_result_with_conversation, _FLAGGED_RETRYABLE,
        {"ticket_id": "T-1"}, {"status": "APPROVED"}, ["AuthService"], "summary",
        False, None, "2026-01-01T00:00:00Z",
    )

    assert captured["conversation_file"] == "/tmp/some_conversation.json"
    # Sin conversation_file, el feedback iria prefijado con el prompt
    # original completo -- CON conversation_file, es solo el feedback.
    assert "prompt original" not in captured["feedback_text"]
    assert "alcance raro" in captured["feedback_text"]


def test_retry_local_diff_falls_back_to_full_prompt_without_conversation_file(monkeypatch):
    captured = {}

    def fake_retry(ticket_id, feedback_text, target_repo_dir, conversation_file=None):
        captured["feedback_text"] = feedback_text
        captured["conversation_file"] = conversation_file
        return {"applied": True, "backend": "anthropic"}

    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", fake_retry)
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "2 tests passed"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff text del segundo intento")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "corregido"})

    _retry_local_diff(
        "T-1", "prompt original", "/repo", _AGENT_RESULT, _FLAGGED_RETRYABLE,
        {"ticket_id": "T-1"}, {"status": "APPROVED"}, ["AuthService"], "summary",
        False, None, "2026-01-01T00:00:00Z",
    )

    assert captured["conversation_file"] is None
    assert "prompt original" in captured["feedback_text"]


def test_retry_local_diff_mirrors_blocked_transition_to_epic_children(monkeypatch):
    """En modo epica, un bloqueo dentro del segundo intento (tests reales
    fallando de nuevo) tiene que dejar BLOCKED tanto a la epica como a cada
    hijo -- antes del fan-out, solo se transicionaba el ticket_id (la
    epica), y los hijos se quedaban visualmente sin cambios.
    """
    transitioned = []
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: {"applied": True, "backend": "anthropic"})
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff text del segundo intento")
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": False, "output": "fallo de nuevo"})
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: transitioned.append((status, ticket_key)))
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)

    with pytest.raises(PipelineBlocked):
        _retry_local_diff(
            "EPIC-1", "prompt original", "/repo", _AGENT_RESULT, _FLAGGED_RETRYABLE,
            {"ticket_id": "EPIC-1"}, {"status": "APPROVED"}, ["AuthService"], "summary",
            True, ["CHILD-1", "CHILD-2"], "2026-01-01T00:00:00Z",
        )

    assert transitioned == [
        (orchestration.JIRA_BLOCKED_STATUS, "EPIC-1"),
        (orchestration.JIRA_BLOCKED_STATUS, "CHILD-1"),
        (orchestration.JIRA_BLOCKED_STATUS, "CHILD-2"),
    ]


_NOT_APPLIED_AGENT_RESULT = {
    "applied": False, "branch": None, "base_branch": "main", "backend": "ollama",
    "conversation_file": "/tmp/conv_no_changes.json", "self_review": None,
}
_FLAGGED_OTHER = {"verdict": "FLAGGED", "reasoning": "implementa las paginas de error", "policy_reference": "other"}


def test_retry_after_no_changes_returns_new_verdict_and_mutates_agent_result(monkeypatch):
    """Bug real confirmado esta sesion (KAN-15): antes, cuando el primer
    intento no aplicaba nada y el juez marcaba FLAGGED con una sugerencia
    concreta, esa sugerencia nunca volvia al coding agent. Este reintento
    tiene que: crear una rama nueva (la primera se borro), pasarle el
    feedback del juez, y si esta vez SI aplica algo, mutar agent_result
    (branch/applied/backend) para que _deliver() vea la rama nueva.
    """
    git_calls = []
    monkeypatch.setattr(orchestration.subprocess, "run", lambda cmd, **k: git_calls.append(cmd))
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: {"applied": True, "backend": "ollama", "self_review": {}})
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff nuevo")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "ahora si"})

    agent_result = dict(_NOT_APPLIED_AGENT_RESULT)
    result = _retry_after_no_changes(
        "T-1", "prompt original", "/repo", agent_result, _FLAGGED_OTHER,
        {"ticket_id": "T-1"}, {"status": "APPROVED"}, ["Frontend"], "summary",
        False, None, "2026-01-01T00:00:00Z",
    )

    assert result == {"verdict": "OK", "reasoning": "ahora si"}
    assert agent_result["applied"] is True
    assert agent_result["branch"] is not None
    assert any(cmd[:4] == ["git", "-C", "/repo", "checkout"] and "-b" in cmd for cmd in git_calls)


def test_retry_after_no_changes_returns_none_and_cleans_up_when_still_nothing_applied(monkeypatch):
    git_calls = []
    monkeypatch.setattr(orchestration.subprocess, "run", lambda cmd, **k: git_calls.append(cmd))
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: {"applied": False, "backend": None})

    agent_result = dict(_NOT_APPLIED_AGENT_RESULT)
    result = _retry_after_no_changes(
        "T-1", "prompt original", "/repo", agent_result, _FLAGGED_OTHER,
        {"ticket_id": "T-1"}, {"status": "APPROVED"}, ["Frontend"], "summary",
        False, None, "2026-01-01T00:00:00Z",
    )

    assert result is None
    assert agent_result["applied"] is False  # no mutado -- el reintento tampoco aplico nada
    # se limpia la rama nueva que se creo para el reintento (checkout de vuelta + delete)
    assert ["git", "-C", "/repo", "checkout", "main"] in git_calls
    assert any(cmd[:4] == ["git", "-C", "/repo", "branch"] and cmd[4] == "-D" for cmd in git_calls)


def test_deliver_retries_and_recovers_when_no_changes_applied_but_judge_flags(monkeypatch):
    """Integracion real: _deliver() en el camino local, primer intento no
    aplica nada, el juez marca FLAGGED con policy_reference "other" (no
    esta en RETRYABLE_POLICY_REFERENCES -- este camino no se gatea por
    eso) -- tiene que reintentar solo, y si el reintento aplica algo y
    queda OK, terminar en push_and_open_pr con la rama NUEVA.
    """
    monkeypatch.setattr(orchestration, "GITHUB_REPO", "")
    monkeypatch.setattr(orchestration, "_local_coding_agent_backend_available", lambda: True)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": False, "branch": None, "base_branch": "main", "backend": "ollama", "conversation_file": None, "self_review": None},
    )

    judge_calls = {"n": 0}

    def fake_judge_safe(*a, **k):
        judge_calls["n"] += 1
        if judge_calls["n"] == 1:
            return dict(_FLAGGED_OTHER)
        return {"verdict": "OK", "reasoning": "el reintento si aplico algo"}

    monkeypatch.setattr(orchestration, "_run_judge_safe", fake_judge_safe)
    monkeypatch.setattr(orchestration.subprocess, "run", lambda cmd, **k: None)
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: {"applied": True, "backend": "ollama", "self_review": {}})
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff nuevo")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])

    pr_calls = []
    monkeypatch.setattr(
        orchestration, "push_and_open_pr",
        lambda target_repo_dir, branch, base_branch, *a, **k: pr_calls.append(branch) or {"pr_url": "https://x/pr/1", "pushed": True, "reason": None},
    )

    result = _deliver(
        "T-1", "summary", {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
        {"ticket_id": "T-1", "repository_origen": "Frontend"}, "/repo",
    )

    assert judge_calls["n"] == 2
    assert result["judge"]["verdict"] == "OK"
    assert len(pr_calls) == 1
    assert pr_calls[0] is not None  # la rama NUEVA creada por el reintento, no None


def test_comment_all_posts_once_in_ticket_mode(monkeypatch):
    calls = []
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: calls.append((text, ticket_key)))

    _comment_all("hola", "T-1", False, None)

    assert calls == [("hola", "T-1")]


def test_comment_all_mirrors_to_children_in_epic_mode(monkeypatch):
    calls = []
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: calls.append((text, ticket_key)))

    _comment_all("hola", "EPIC-1", True, ["CHILD-1", "CHILD-2"])

    assert calls == [("hola", "EPIC-1"), ("hola", "CHILD-1"), ("hola", "CHILD-2")]


def test_transition_all_posts_once_in_ticket_mode(monkeypatch):
    calls = []
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: calls.append((status, ticket_key)))

    _transition_all("Blocked", "T-1", False, None)

    assert calls == [("Blocked", "T-1")]


def test_transition_all_mirrors_to_children_in_epic_mode(monkeypatch):
    calls = []
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: calls.append((status, ticket_key)))

    _transition_all("Blocked", "EPIC-1", True, ["CHILD-1", "CHILD-2"])

    assert calls == [("Blocked", "EPIC-1"), ("Blocked", "CHILD-1"), ("Blocked", "CHILD-2")]


def test_handle_rejected_transitions_to_blocked_status(monkeypatch):
    """Antes de este cambio, un rechazo del firewall no transicionaba el
    ticket a ningun estado -- se quedaba donde estaba, sin senal visible en
    Jira de que el pipeline ya lo proceso.
    """
    transitioned = []
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: transitioned.append((status, ticket_key)))
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)

    with pytest.raises(PipelineBlocked):
        _handle_rejected("T-1", {"ticket_id": "T-1", "summary": "s"}, {"status": "REJECTED", "reason": "match jailbreak"})

    assert transitioned == [(orchestration.JIRA_BLOCKED_STATUS, "T-1")]


def test_handle_rejected_mirrors_blocked_transition_to_epic_children(monkeypatch):
    transitioned = []
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: transitioned.append((status, ticket_key)))
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)

    with pytest.raises(PipelineBlocked):
        _handle_rejected(
            "EPIC-1", {"ticket_id": "EPIC-1", "summary": "s"}, {"status": "REJECTED", "reason": "match jailbreak"},
            is_epic=True, child_ticket_keys=["CHILD-1", "CHILD-2"],
        )

    assert transitioned == [
        (orchestration.JIRA_BLOCKED_STATUS, "EPIC-1"),
        (orchestration.JIRA_BLOCKED_STATUS, "CHILD-1"),
        (orchestration.JIRA_BLOCKED_STATUS, "CHILD-2"),
    ]


def _fake_child(ticket_id: str, repo: str = "AuthService") -> dict:
    return {"ticket_id": ticket_id, "summary": f"summary {ticket_id}", "description": f"desc {ticket_id}", "repository_origen": repo}


def test_deliver_epic_sequential_chains_conversation_across_children(monkeypatch):
    """2 hijos aprobados y aplicados: el primero crea la rama
    (run_coding_agent_local_real), el segundo CONTINUA la misma conversacion
    (retry_coding_agent_local_real con el conversation_file del primero) --
    y cada uno recibe su propio comentario, no uno generico compartido.
    """
    children = [_fake_child("C-1"), _fake_child("C-2")]
    comments = []
    coding_calls = []
    guard_diffs = []

    monkeypatch.setattr(
        orchestration, "evaluate_firewall",
        lambda *a, **k: {"status": "APPROVED", "sanitized_prompt": "prompt saneado", "redactions_applied": 0, "reason": None},
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append((ticket_key, text)))
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)

    def fake_run(cmd, input_text=None, check=True, env=None):
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return "checkpointhash\n"
        return f"diff {cmd[-1]}"
    monkeypatch.setattr(orchestration, "_run", fake_run)

    def fake_run_first(ticket_id, sanitized, target_repo_dir):
        coding_calls.append(("first", sanitized))
        return {
            "applied": True, "branch": "copilot/EPIC-1-123", "base_branch": "main",
            "backend": "anthropic", "conversation_file": "/tmp/conv1.json", "self_review": {},
        }

    def fake_retry(ticket_id, feedback_text, target_repo_dir, conversation_file=None):
        coding_calls.append(("retry", feedback_text, conversation_file))
        return {"applied": True, "backend": "anthropic", "self_review": {}, "conversation_file": "/tmp/conv2.json"}

    monkeypatch.setattr(orchestration, "run_coding_agent_local_real", fake_run_first)
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", fake_retry)

    def fake_guard(diff_text, jira_context):
        guard_diffs.append(diff_text)
        return {"redactions_applied": 0, "jailbreak_reason": None, "clean": True}

    monkeypatch.setattr(orchestration, "run_output_guard", fake_guard)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "todo bien"})
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": "https://github.com/org/repo/pull/1", "pushed": True, "reason": None})
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration.Path, "unlink", lambda self, missing_ok=True: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)

    result = orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc epica"}, children, "/repo",
        ["--- AuthService ---\nsin dependencias"], ["--- AuthService ---\n"], [], "", [],
    )

    assert result["completed"] == [
        {"ticket_id": "C-1", "outcome": "ok"},
        {"ticket_id": "C-2", "outcome": "ok"},
    ]
    assert result["blocked_at"] is None
    assert coding_calls[0][0] == "first"
    assert coding_calls[1] == ("retry", "prompt saneado", "/tmp/conv1.json")
    # El diff que ve el guardia para el segundo hijo es el incremental
    # (checkpoint..HEAD), no el acumulado desde el inicio de la rama.
    assert guard_diffs[1] == "diff checkpointhash..HEAD"
    child_comment_keys = [tk for tk, _ in comments if tk in ("C-1", "C-2")]
    assert child_comment_keys.count("C-1") >= 2  # coding agent + juez
    assert child_comment_keys.count("C-2") >= 2
    assert any(tk == "EPIC-1" for tk, _ in comments)  # resumen final en la epica


def test_deliver_epic_sequential_skips_rejected_child_and_continues(monkeypatch):
    children = [_fake_child("C-1"), _fake_child("C-2"), _fake_child("C-3")]
    comments = []
    coding_backend_calls = []

    def fake_firewall(prompt, jira_context, sonar_errors):
        if jira_context["ticket_id"] == "C-2":
            return {"status": "REJECTED", "reason": "jailbreak detectado", "sanitized_prompt": None, "redactions_applied": 0}
        return {"status": "APPROVED", "sanitized_prompt": "prompt saneado", "redactions_applied": 0, "reason": None}

    monkeypatch.setattr(orchestration, "evaluate_firewall", fake_firewall)
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append(ticket_key))
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")

    def fake_run_first(ticket_id, sanitized, target_repo_dir):
        coding_backend_calls.append("first")
        return {"applied": False, "branch": None, "base_branch": "main", "backend": "anthropic", "conversation_file": None, "self_review": None}

    monkeypatch.setattr(orchestration, "run_coding_agent_local_real", fake_run_first)
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: pytest.fail("no deberia reintentar sin rama"))
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)

    result = orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    assert result["blocked_at"] is None
    outcomes = {c["ticket_id"]: c["outcome"] for c in result["completed"]}
    assert outcomes["C-1"] == "no-op"
    assert outcomes["C-3"] == "no-op"
    assert "C-2" not in outcomes
    # ninguno de los dos "no-op" dejo rama real -> ambos entran por
    # run_coding_agent_local_real, ninguno por el reintento encadenado
    assert coding_backend_calls == ["first", "first"]


def test_deliver_epic_sequential_blocks_on_test_failure_and_skips_remaining_children(monkeypatch):
    children = [_fake_child("C-1"), _fake_child("C-2")]
    comments = []
    transitions = []

    monkeypatch.setattr(
        orchestration, "evaluate_firewall",
        lambda *a, **k: {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append(ticket_key))
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: transitions.append((ticket_key, status)))
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "branch": "copilot/EPIC-1-1", "base_branch": "main", "backend": "anthropic", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": False, "output": "fallo"})
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: pytest.fail("no deberia abrir PR: nada quedo OK"))
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: pytest.fail("no deberia llegar al segundo hijo"))
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)

    result = orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    assert result["blocked_at"] == "C-1"
    assert result["completed"] == []
    assert ("C-1", orchestration.JIRA_BLOCKED_STATUS) in transitions
    assert "C-2" not in comments  # el segundo hijo nunca se toco


def test_deliver_epic_sequential_posts_technical_report_when_generated(monkeypatch):
    """El comprobante tecnico (poblado por el LLM, no por texto fijo) se
    pide POR HISTORIA, con la evidencia real de esa historia puntual, y si
    se genera se postea como un comentario adicional en ESA historia (no
    solo en la epica) -- best-effort: si generate_technical_report
    devuelve None (backend no disponible, ver otros tests), no se postea
    nada extra.
    """
    children = [_fake_child("C-1")]
    comments = []
    report_calls = []

    monkeypatch.setattr(
        orchestration, "evaluate_firewall",
        lambda *a, **k: {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append((ticket_key, text)))
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "branch": "copilot/EPIC-1-1", "base_branch": "main", "backend": "ollama", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "todo bien"})
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": "https://github.com/org/repo/pull/9", "pushed": True, "reason": None})
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)

    def fake_report(evidence):
        report_calls.append(evidence)
        return "# Comprobante real generado por el modelo"

    monkeypatch.setattr(orchestration, "generate_technical_report", fake_report)

    result = orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    assert result["completed"] == [{"ticket_id": "C-1", "outcome": "ok"}]
    assert len(report_calls) == 1
    evidence = report_calls[0]
    assert evidence["epica"] == "EPIC-1"
    assert evidence["historia"] == "C-1"
    assert evidence["backend_usado"] == "ollama"
    assert evidence["resultado"] == "OK -- listo para revision humana"
    assert evidence["veredicto_juez"] == "todo bien"
    assert ("C-1", "# Comprobante real generado por el modelo") in comments


def test_deliver_epic_sequential_skips_report_comment_when_generation_fails(monkeypatch):
    """Si generate_technical_report devuelve None (backend no disponible o
    fallo real), no se postea ningun comentario extra -- best-effort, no
    debe inventarse un comentario vacio ni romper la corrida."""
    children = [_fake_child("C-1")]
    comments = []

    monkeypatch.setattr(
        orchestration, "evaluate_firewall",
        lambda *a, **k: {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append((ticket_key, text)))
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "branch": "copilot/EPIC-1-1", "base_branch": "main", "backend": "ollama", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "todo bien"})
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": "https://github.com/org/repo/pull/9", "pushed": True, "reason": None})
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda evidence: None)

    orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    summary_comments = [text for _tk, text in comments if "Modo epica secuencial" in text]
    assert len(summary_comments) == 1  # solo el resumen -- nada extra del comprobante


def test_deliver_posts_technical_report_for_single_ticket_run(monkeypatch):
    """Ticket unico (no epica): _deliver tambien pide un comprobante
    tecnico real al terminar y lo postea en el mismo ticket -- este es el
    camino que corre run_pipeline() para una historia comun (ej. KAN-15),
    no solo el modo epica secuencial.
    """
    comments = []
    report_calls = []

    monkeypatch.setattr(orchestration, "GITHUB_REPO", "")
    monkeypatch.setattr(orchestration, "_local_coding_agent_backend_available", lambda: True)
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append((ticket_key, text)))
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff real")
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "branch": "copilot/T-1", "base_branch": "main", "backend": "ollama", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "todo bien"})
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": None, "pushed": False, "reason": "sin remote"})
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)

    def fake_report(evidence):
        report_calls.append(evidence)
        return "# Comprobante real de T-1"

    monkeypatch.setattr(orchestration, "generate_technical_report", fake_report)

    orchestration._deliver(
        "T-1", "resumen", {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
        {"ticket_id": "T-1", "repository_origen": "AuthService"}, "/repo",
    )

    assert len(report_calls) == 1
    assert report_calls[0]["ticket"] == "T-1"
    assert report_calls[0]["backend_usado"] == "ollama"
    assert report_calls[0]["veredicto_juez"] == "OK"
    assert ("T-1", "# Comprobante real de T-1") in comments


class _FakeFuture:
    def __init__(self, value=None):
        self._value = value

    def result(self):
        return self._value


def test_check_log_evidence_skips_non_bug_ticket_without_stack_trace(monkeypatch):
    """Bug real de esta sesion: check_log_evidence le pedia un stack trace
    a CUALQUIER ticket sin bloque de codigo, sin mirar el tipo de issue --
    incluidas Historias/Tareas reales que nunca tuvieron un error que
    diagnosticar (confirmado en vivo: el proyecto KAN real ni siquiera
    tiene un tipo 'Bug', solo Epic/Historia/Tarea/Subtask).
    """
    calls = []
    monkeypatch.setattr(orchestration.comment_jira, "submit", lambda *a, **k: calls.append((a, k)) or _FakeFuture())

    orchestration.check_log_evidence({"issue_type": "Historia", "has_log_evidence": False, "repository_origen": "Frontend"})

    assert calls == []


def test_check_log_evidence_fires_for_real_bug_without_evidence(monkeypatch):
    calls = []
    monkeypatch.setattr(orchestration.comment_jira, "submit", lambda *a, **k: calls.append((a, k)) or _FakeFuture())

    orchestration.check_log_evidence({"issue_type": "Bug", "has_log_evidence": False, "repository_origen": "Frontend"})

    assert len(calls) == 1


def test_check_log_evidence_skips_bug_that_already_has_evidence(monkeypatch):
    calls = []
    monkeypatch.setattr(orchestration.comment_jira, "submit", lambda *a, **k: calls.append((a, k)) or _FakeFuture())

    orchestration.check_log_evidence({"issue_type": "Bug", "has_log_evidence": True, "repository_origen": "Frontend"})

    assert calls == []


def test_run_tests_passes_target_repo_dir_as_host_path_by_default(monkeypatch):
    """Sin HOST_TARGET_REPO_DIR seteada (host real, no Docker-outside-of-
    Docker), el segundo argumento para run_module_tests.sh cae al mismo
    target_repo_dir -- comportamiento identico al de antes de este fix."""
    captured = {}
    monkeypatch.setattr(orchestration.shutil, "which", lambda name: "/usr/bin/docker")
    monkeypatch.delenv("HOST_TARGET_REPO_DIR", raising=False)

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        class R:
            returncode = 0
            stdout = "ok"
            stderr = ""
        return R()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    result = orchestration.run_tests("/target-repo")

    assert result["passed"] is True
    assert captured["cmd"][-2:] == ["/target-repo", "/target-repo"]


def test_run_tests_uses_host_target_repo_dir_for_docker_outside_of_docker(monkeypatch):
    """Confirmado real esta sesion: corriendo DENTRO de un contenedor con
    /var/run/docker.sock montado, el docker run ANIDADO de
    run_module_tests.sh lo ejecuta el daemon del HOST -- que no puede
    montar /target-repo (un path que solo existe dentro de ESTE
    contenedor). HOST_TARGET_REPO_DIR le pasa el path real del host."""
    captured = {}
    monkeypatch.setattr(orchestration.shutil, "which", lambda name: "/usr/bin/docker")
    monkeypatch.setenv("HOST_TARGET_REPO_DIR", "/c/Users/real/scratchpad/ai-agents-code")

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        class R:
            returncode = 0
            stdout = "ok"
            stderr = ""
        return R()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    orchestration.run_tests("/target-repo")

    assert captured["cmd"][-2:] == ["/target-repo", "/c/Users/real/scratchpad/ai-agents-code"]


def test_check_copilot_assignable_returns_yes_when_login_present(monkeypatch):
    class R:
        returncode = 0
        stdout = json.dumps({"data": {"repository": {"suggestedActors": {"nodes": [{"login": "copilot-swe-agent"}]}}}})
        stderr = ""

    monkeypatch.setattr(orchestration.subprocess, "run", lambda *a, **k: R())

    assert orchestration.check_copilot_assignable("org/repo", "copilot-swe-agent") == "yes"


def test_check_copilot_assignable_returns_no_when_login_absent(monkeypatch):
    class R:
        returncode = 0
        stdout = json.dumps({"data": {"repository": {"suggestedActors": {"nodes": [{"login": "some-other-bot"}]}}}})
        stderr = ""

    monkeypatch.setattr(orchestration.subprocess, "run", lambda *a, **k: R())

    assert orchestration.check_copilot_assignable("org/repo", "copilot-swe-agent") == "no"


def test_check_copilot_assignable_returns_unknown_on_query_failure(monkeypatch):
    class R:
        returncode = 1
        stdout = ""
        stderr = "HTTP 401"

    monkeypatch.setattr(orchestration.subprocess, "run", lambda *a, **k: R())

    assert orchestration.check_copilot_assignable("org/repo", "copilot-swe-agent") == "unknown"


def test_run_coding_agent_cloud_still_creates_issue_when_not_assignable(monkeypatch):
    """El chequeo proactivo es solo diagnostico -- si dice "no", igual se
    intenta crear el issue y asignar (puede ser un falso negativo de
    permisos del token), nada del contrato/comportamiento cambia.
    """
    captured_cmds = []

    def fake_run(cmd, input_text=None, check=True, env=None):
        captured_cmds.append(cmd)
        if cmd[:2] == ["gh", "issue"] and "create" in cmd:
            return "https://github.com/org/repo/issues/1"
        return ""

    monkeypatch.setattr(orchestration, "_run", fake_run)
    monkeypatch.setattr(orchestration, "check_copilot_assignable", lambda *a, **k: "no")

    result = orchestration.run_coding_agent_cloud.fn("T-1", "Fix login", "hace algo")

    assert result["issue_url"] == "https://github.com/org/repo/issues/1"
    assert any(cmd[:2] == ["gh", "issue"] and "create" in cmd for cmd in captured_cmds)


def test_run_coding_agent_cloud_redacts_secret_from_issue_body(monkeypatch):
    """Camino A no tenia ninguna guardia de salida sobre el issue_body que
    sube a GitHub (a diferencia del diff de Camino B1, que si pasa por
    output_guard.py) -- confirma que un secreto en el prompt sanitizado no
    llega intacto al 'gh issue create'.
    """
    captured_cmds = []

    def fake_run(cmd, input_text=None, check=True, env=None):
        captured_cmds.append(cmd)
        if cmd[:2] == ["gh", "issue"] and "create" in cmd:
            return "https://github.com/org/repo/issues/1"
        return ""

    monkeypatch.setattr(orchestration, "_run", fake_run)
    monkeypatch.setattr(orchestration, "check_copilot_assignable", lambda *a, **k: "unknown")

    orchestration.run_coding_agent_cloud.fn(
        "T-1", "Rotar password", 'private static final String DB_PASSWORD = "password=Sup3rS3cr3tDbP4ss!";'
    )

    create_cmd = next(c for c in captured_cmds if "create" in c)
    body = create_cmd[create_cmd.index("--body") + 1]
    assert "Sup3rS3cr3tDbP4ss!" not in body


def test_check_not_epic_raises_pipeline_blocked_for_epic_ticket():
    """KAN-4 (real, corrida en vivo esta sesion) es una Epica pero
    fetch_ticket_live() no traia issue_type hasta este cambio -- el pipeline
    la procesaba como ticket normal, sin hijos, en silencio."""
    with pytest.raises(PipelineBlocked, match="es una Epica"):
        _check_not_epic({"ticket_id": "KAN-4", "issue_type": "Epic"})


def test_check_not_epic_allows_non_epic_ticket():
    _check_not_epic({"ticket_id": "T-1", "issue_type": "Task"})  # no debe lanzar


def test_check_not_epic_allows_missing_issue_type():
    _check_not_epic({"ticket_id": "T-1"})  # no debe lanzar


def test_format_conflicts_section_empty_when_no_conflicts():
    assert _format_conflicts_section([]) == ""


def test_format_conflicts_section_lists_each_conflict():
    section = _format_conflicts_section(["AuthService y Frontend tocan el mismo endpoint", "DataWorker depende de AuthService"])

    assert "Conflictos detectados por el planificador" in section
    assert "- AuthService y Frontend tocan el mismo endpoint" in section
    assert "- DataWorker depende de AuthService" in section


def test_run_judge_payload_includes_self_review(monkeypatch):
    """run_judge() manda self_review al juez (antes se computaba en
    coding_agent.py y nadie lo leia aguas abajo) -- este test confirma que
    el payload que le llega al subprocess de judge_agent.py lo incluye.
    """
    captured = {}

    class FakeCompletedProcess:
        returncode = 0
        stdout = json.dumps({"verdict": "OK", "reasoning": "ok"})
        stderr = ""

    def fake_run(cmd, input=None, capture_output=None, text=None, cwd=None):
        captured["input"] = json.loads(input)
        return FakeCompletedProcess()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    self_review = {"scope_matches_ticket": True, "no_secrets_introduced": True, "tests_adequate": False}
    orchestration.run_judge.fn(
        {"ticket_id": "T-1"}, {"status": "APPROVED"}, "local_diff", "diff", "tests", self_review=self_review
    )

    assert captured["input"]["self_review"] == self_review


def test_run_judge_payload_includes_new_sonar_issues(monkeypatch):
    """run_judge() manda new_sonar_issues al juez -- confirma que el re-scan
    de Sonar sobre el diff real (rescan_sonar()) llega al payload."""
    captured = {}

    class FakeCompletedProcess:
        returncode = 0
        stdout = json.dumps({"verdict": "OK", "reasoning": "ok"})
        stderr = ""

    def fake_run(cmd, input=None, capture_output=None, text=None, cwd=None):
        captured["input"] = json.loads(input)
        return FakeCompletedProcess()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    new_sonar_issues = ["[MAJOR] rule:S123: algo nuevo (linea 42)"]
    orchestration.run_judge.fn(
        {"ticket_id": "T-1"}, {"status": "APPROVED"}, "local_diff", "diff", "tests",
        new_sonar_issues=new_sonar_issues,
    )

    assert captured["input"]["new_sonar_issues"] == new_sonar_issues


def test_rescan_sonar_returns_empty_list_when_script_fails(monkeypatch):
    """rescan_sonar.sh es best-effort -- si falla o devuelve algo invalido,
    rescan_sonar() nunca debe propagar la excepcion, solo devolver []."""
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "esto no es json")

    result = orchestration.rescan_sonar.fn("/repo", "AuthService")

    assert result == []


def test_rescan_sonar_returns_new_issues_from_script(monkeypatch):
    monkeypatch.setattr(
        orchestration, "_run",
        lambda *a, **k: json.dumps({"scanned": True, "new_issues": ["[MAJOR] rule:S1: x (linea 1)"], "reason": None}),
    )

    result = orchestration.rescan_sonar.fn("/repo", "AuthService")

    assert result == ["[MAJOR] rule:S1: x (linea 1)"]


def test_push_and_open_pr_skips_without_remote(monkeypatch):
    """Si el repo objetivo no tiene un remote 'origin', push_and_open_pr()
    se omite sin lanzar excepcion -- nunca bloquea la corrida."""
    def fake_subprocess_run(cmd, capture_output=None, text=None, cwd=None):
        class R:
            returncode = 1
            stdout = ""
            stderr = "fatal: no such remote 'origin'"
        return R()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_subprocess_run)

    result = orchestration.push_and_open_pr.fn("/repo", "copilot/T-1-123", "main", "T-1", "Fix login", "body")

    assert result["pushed"] is False
    assert result["pr_url"] is None
    assert "remote" in result["reason"]


def test_push_and_open_pr_pushes_and_creates_pr(monkeypatch):
    calls = []

    def fake_subprocess_run(cmd, capture_output=None, text=None, cwd=None):
        calls.append(cmd)
        class R:
            returncode = 0
            stdout = ""
            stderr = ""
        r = R()
        if "get-url" in cmd:
            r.stdout = "https://github.com/org/repo.git"
        elif cmd[:2] == ["gh", "pr"]:
            r.stdout = "https://github.com/org/repo/pull/7"
        return r

    monkeypatch.setattr(orchestration.subprocess, "run", fake_subprocess_run)

    result = orchestration.push_and_open_pr.fn("/repo", "copilot/T-1-123", "main", "T-1", "Fix login", "body")

    assert result["pushed"] is True
    assert result["pr_url"] == "https://github.com/org/repo/pull/7"
    push_cmd = next(c for c in calls if "push" in c)
    assert "copilot/T-1-123" in push_cmd
    pr_cmd = next(c for c in calls if c[:2] == ["gh", "pr"])
    assert "--base" in pr_cmd and "main" in pr_cmd


def test_comment_jira_calls_jira_client_directly(monkeypatch):
    """comment_jira() ya no shellea a 'python3 jira_client.py comment' --
    importa jira_client y llama post_audit_comment() directo."""
    captured = {}
    monkeypatch.setattr(
        orchestration.jira_client, "post_audit_comment",
        lambda ticket_key, text: captured.update(ticket_key=ticket_key, text=text),
    )

    orchestration.comment_jira.fn("hola", ticket_key="T-1")

    assert captured == {"ticket_key": "T-1", "text": "hola"}


def test_comment_jira_logs_instead_of_raising_on_failure(monkeypatch):
    """Antes un fallo real de Jira en comment_jira/transition_jira
    desaparecia en silencio (subprocess con check=False, resultado
    descartado), despues se loggeaba con log_utils.get_logger() (invisible
    en la UI de Prefect, gap real identificado esta sesion) -- ahora usa
    get_run_logger() para que quede visible en los logs de ESA tarea de
    Prefect, sin dejar de ser best-effort (no levanta excepcion, no
    bloquea la corrida). Se llama al Task (no .fn()) para que Prefect le
    arme su propio contexto de tarea, que es lo que get_run_logger()
    necesita.
    """
    def fake_post(ticket_key, text):
        raise RuntimeError("Jira esta caido")

    class _FakeLogger:
        def __init__(self):
            self.warnings = []

        def warning(self, msg):
            self.warnings.append(msg)

    fake_logger = _FakeLogger()
    monkeypatch.setattr(orchestration.jira_client, "post_audit_comment", fake_post)
    monkeypatch.setattr(orchestration, "get_run_logger", lambda: fake_logger)

    orchestration.comment_jira("hola", ticket_key="T-1")  # no debe lanzar

    assert any("Jira esta caido" in w for w in fake_logger.warnings)


def test_transition_jira_calls_jira_client_directly(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        orchestration.jira_client, "transition_ticket",
        lambda ticket_key, status: captured.update(ticket_key=ticket_key, status=status),
    )

    orchestration.transition_jira.fn("Blocked", ticket_key="T-1")

    assert captured == {"ticket_key": "T-1", "status": "Blocked"}


def test_fetch_jira_ticket_passes_known_components_to_jira_client(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        orchestration.jira_client, "fetch_ticket_live",
        lambda known_repos=None: captured.update(known_repos=known_repos) or {"ticket_id": "T-1"},
    )

    orchestration.fetch_jira_ticket.fn(["AuthService", "Frontend"])

    assert captured["known_repos"] == {"AuthService", "Frontend"}


def test_discover_known_components_returns_list_without_mutating_environ(monkeypatch):
    """discover_known_components() ya no muta os.environ (esa mutacion solo
    hacia falta cuando fetch_jira_ticket() invocaba jira_client.py como
    subprocess aparte, que heredaba el entorno) -- ahora devuelve la lista
    y el llamador se la pasa directo como argumento."""
    monkeypatch.delenv("JIRA_KNOWN_COMPONENTS", raising=False)
    monkeypatch.setattr(
        orchestration, "_run",
        lambda *a, **k: 'header\n"AuthService"\n"Frontend"\n',
    )

    result = orchestration.discover_known_components.fn()

    assert result == ["AuthService", "Frontend"]
    assert "JIRA_KNOWN_COMPONENTS" not in os.environ


def test_retryable_policy_references_matches_judge_agent():
    """orchestration.py duplica esta constante (judge_agent.py se invoca
    como subprocess, no se importa) -- este test es la red de seguridad
    para que no se desincronicen.
    """
    import judge_agent

    assert orchestration.RETRYABLE_POLICY_REFERENCES == judge_agent.RETRYABLE_POLICY_REFERENCES


def test_run_poc_loop_reads_retryable_policy_refs_from_pipeline_shared():
    """run_poc_loop.sh solia duplicar RETRYABLE_POLICY_REFERENCES a mano como
    un array bash, sin ningun test protegiendolo contra drift -- ahora lo
    deriva de pipeline_shared.py (la misma fuente que judge_agent.py/
    orchestration.py importan), eliminando la duplicacion de raiz. Se lee el
    archivo como texto (sin ejecutar bash) para no arrastrar los efectos
    secundarios de nivel de script de run_poc_loop.sh -- solo confirma que
    ya no hay un array hardcodeado y que se usa pipeline_shared.py.
    """
    script_path = Path(__file__).resolve().parent.parent / "run_poc_loop.sh"
    script_text = script_path.read_text(encoding="utf-8")

    assert "RETRYABLE_POLICY_REFS=(scope-mismatch" not in script_text, "no deberia quedar un array hardcodeado"
    assert "pipeline_shared.py" in script_text and "retryable-policy-references" in script_text


def test_pipeline_shared_cli_prints_retryable_policy_references(capsys):
    import pipeline_shared

    sys_argv_backup = sys.argv
    sys.argv = ["pipeline_shared.py", "retryable-policy-references"]
    try:
        pipeline_shared.main()
    finally:
        sys.argv = sys_argv_backup

    printed = set(capsys.readouterr().out.split())
    assert printed == pipeline_shared.RETRYABLE_POLICY_REFERENCES
