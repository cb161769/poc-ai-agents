"""Unit tests for orchestration.py's pure epic-mode helper (deciding
whether all of an epic's children live in the same repo) and the
FLAGGED-retry helper (_retry_local_diff / the retry gate in _deliver()).
No real Neo4j/Jira/coding-agent network calls -- every Prefect task the
retry path touches is monkeypatched at the module level.
"""
import json
from pathlib import Path

import pytest

import orchestration
from orchestration import PipelineBlocked, _format_conflicts_section, _resolve_single_repo, _retry_local_diff


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
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "corregido"})

    _retry_local_diff(
        "T-1", "prompt original", "/repo", _AGENT_RESULT, _FLAGGED_RETRYABLE,
        {"ticket_id": "T-1"}, {"status": "APPROVED"}, ["AuthService"], "summary",
        False, None, "2026-01-01T00:00:00Z",
    )

    assert captured["conversation_file"] is None
    assert "prompt original" in captured["feedback_text"]


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

    orchestration.run_coding_agent_cloud.fn(
        "T-1", "Rotar password", 'private static final String DB_PASSWORD = "password=Sup3rS3cr3tDbP4ss!";'
    )

    create_cmd = next(c for c in captured_cmds if "create" in c)
    body = create_cmd[create_cmd.index("--body") + 1]
    assert "Sup3rS3cr3tDbP4ss!" not in body


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


def test_retryable_policy_references_matches_judge_agent():
    """orchestration.py duplica esta constante (judge_agent.py se invoca
    como subprocess, no se importa) -- este test es la red de seguridad
    para que no se desincronicen.
    """
    import judge_agent

    assert orchestration.RETRYABLE_POLICY_REFERENCES == judge_agent.RETRYABLE_POLICY_REFERENCES


def test_retryable_policy_refs_bash_array_matches_judge_agent():
    """run_poc_loop.sh duplica la MISMA constante una tercera vez, como un
    array bash (RETRYABLE_POLICY_REFS) -- a diferencia del duplicado de
    orchestration.py, este no tenia ningun test protegiendolo contra drift.
    Se lee el archivo como texto (sin ejecutar bash) para no arrastrar los
    efectos secundarios de nivel de script de run_poc_loop.sh.
    """
    import re

    import judge_agent

    script_path = Path(__file__).resolve().parent.parent / "run_poc_loop.sh"
    script_text = script_path.read_text(encoding="utf-8")

    match = re.search(r"RETRYABLE_POLICY_REFS=\(([^)]*)\)", script_text)
    assert match is not None, "no se encontro el array RETRYABLE_POLICY_REFS en run_poc_loop.sh"

    bash_refs = set(match.group(1).split())
    assert bash_refs == judge_agent.RETRYABLE_POLICY_REFERENCES
