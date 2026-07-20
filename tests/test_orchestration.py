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
from unittest.mock import MagicMock, patch

import pytest

import orchestration
from orchestration import (
    PipelineBlocked,
    _check_not_epic,
    _comment_all,
    _deliver,
    _check_pr_rejected_for_branch,
    _fetch_unresolved_pr_comments,
    _filter_active_children,
    _git_add_all_excluding_vendor,
    _has_duplicate_project_scaffolding,
    _query_component_risk_history,
    _resolve_pr_threads,
    _find_open_branch_for_ticket,
    _format_conflicts_section,
    _handle_rejected,
    _resolve_single_repo,
    _retry_after_no_changes,
    _retry_local_diff,
    _transition_all,
    ensure_on_trunk_branch,
)


def _init_real_git_repo(repo_dir):
    import subprocess as sp
    sp.run(["git", "init", "-q"], cwd=repo_dir, check=True)
    sp.run(["git", "-c", "user.email=t@t.com", "-c", "user.name=t", "commit", "--allow-empty", "-q", "-m", "baseline"], cwd=repo_dir, check=True)


def test_git_add_all_excluding_vendor_stages_real_files(tmp_path):
    _init_real_git_repo(tmp_path)
    (tmp_path / "app.py").write_text("print('hi')")

    _git_add_all_excluding_vendor(str(tmp_path))

    import subprocess as sp
    staged = sp.run(["git", "-C", str(tmp_path), "diff", "--cached", "--name-only"], capture_output=True, text=True).stdout
    assert "app.py" in staged


def test_git_add_all_excluding_vendor_skips_node_modules(tmp_path):
    """Bug real confirmado en vivo (operacion de esta sesion, epica KAN-4):
    un 'npm install' real corrido por el testing agent generaba
    node_modules/ completo -- sin esto, 'git add -A' lo meteria ENTERO en
    el commit, y output_guard bloqueaba la epica entera por un falso
    positivo (matcheo 'rm -rf' dentro de un script de build de un paquete
    vendoreado de terceros, no nada escrito por el agente).
    """
    _init_real_git_repo(tmp_path)
    (tmp_path / "app.py").write_text("print('hi')")
    vendor_dir = tmp_path / "node_modules" / "some-package"
    vendor_dir.mkdir(parents=True)
    (vendor_dir / "package.json").write_text('{"scripts": {"build": "rm -rf dist"}}')

    _git_add_all_excluding_vendor(str(tmp_path))

    import subprocess as sp
    staged = sp.run(["git", "-C", str(tmp_path), "diff", "--cached", "--name-only"], capture_output=True, text=True).stdout
    assert "app.py" in staged
    assert "node_modules" not in staged


@pytest.mark.parametrize("vendor_dir", ["node_modules", "vendor", "target", "dist", "build", ".venv", "__pycache__"])
def test_git_add_all_excluding_vendor_skips_every_known_vendor_dir(tmp_path, vendor_dir):
    _init_real_git_repo(tmp_path)
    nested = tmp_path / vendor_dir / "some-file.txt"
    nested.parent.mkdir(parents=True)
    nested.write_text("vendored content")

    _git_add_all_excluding_vendor(str(tmp_path))

    import subprocess as sp
    staged = sp.run(["git", "-C", str(tmp_path), "diff", "--cached", "--name-only"], capture_output=True, text=True).stdout
    assert staged.strip() == ""


def test_ensure_on_trunk_branch_checks_out_main_and_pulls(monkeypatch):
    """Bug real confirmado en vivo (PR #240/#241, epica KAN-4): base_branch
    se calculaba como "lo que sea que este en HEAD ahora mismo" -- si el
    working tree quedaba parado en una rama copilot/... vieja de una
    corrida anterior, todo lo nuevo se ramificaba desde ahi. Esto confirma
    que arranca dejando el repo parado en main de verdad."""
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[-2:] == ["checkout", "main"]:
            return _fake_subprocess_result(returncode=0)
        return _fake_subprocess_result(returncode=0)

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    result = ensure_on_trunk_branch.fn("/repo")

    assert result == "main"
    assert any(cmd[-2:] == ["checkout", "main"] for cmd in calls)
    assert any("pull" in cmd for cmd in calls)


def test_ensure_on_trunk_branch_falls_back_to_master(monkeypatch):
    def fake_run(cmd, **kwargs):
        if cmd[-2:] == ["checkout", "main"]:
            return _fake_subprocess_result(returncode=1)
        if cmd[-2:] == ["checkout", "master"]:
            return _fake_subprocess_result(returncode=0)
        return _fake_subprocess_result(returncode=0)

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    assert ensure_on_trunk_branch.fn("/repo") == "master"


def test_ensure_on_trunk_branch_raises_when_no_known_trunk(monkeypatch):
    monkeypatch.setattr(orchestration.subprocess, "run", lambda cmd, **k: _fake_subprocess_result(returncode=1))

    with pytest.raises(PipelineBlocked):
        ensure_on_trunk_branch.fn("/repo")


def test_ensure_on_trunk_branch_respects_env_override(monkeypatch):
    monkeypatch.setattr(orchestration, "TRUNK_BRANCH", "trunk")
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return _fake_subprocess_result(returncode=0)

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    assert ensure_on_trunk_branch.fn("/repo") == "trunk"
    assert any(cmd[-2:] == ["checkout", "trunk"] for cmd in calls)
    assert not any(cmd[-2:] == ["checkout", "main"] for cmd in calls)


def test_ensure_on_trunk_branch_pull_failure_is_best_effort(monkeypatch):
    """El pull es best-effort -- si falla (sin remoto, offline), no debe
    tirar la corrida, solo deja el trunk local como esta."""
    def fake_run(cmd, **kwargs):
        if "pull" in cmd:
            return _fake_subprocess_result(returncode=1, stderr="no remote")
        return _fake_subprocess_result(returncode=0)

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    assert ensure_on_trunk_branch.fn("/repo") == "main"  # no lanza


def test_has_duplicate_project_scaffolding_detects_nested_project_roots(tmp_path):
    """Bug real confirmado en vivo (PR #240/#241, epica KAN-4): "ionic start
    my-app" corrio dentro de frontend/ (que YA tenia su propio package.json
    + src/), creando una segunda raiz de proyecto anidada adentro."""
    frontend = tmp_path / "frontend"
    (frontend / "src").mkdir(parents=True)
    (frontend / "package.json").write_text("{}")

    nested = frontend / "my-app"
    (nested / "src").mkdir(parents=True)
    (nested / "package.json").write_text("{}")

    result = _has_duplicate_project_scaffolding(str(tmp_path))

    assert result is not None
    assert "frontend" in result and "my-app" in result


def test_has_duplicate_project_scaffolding_no_false_positive_for_sibling_subprojects(tmp_path):
    """Un monorepo real (sub-proyectos HERMANOS, ninguno anidado dentro del
    otro) no debe dispararlo -- ese es un patron legitimo, no el bug."""
    for name in ("frontend", "backend"):
        sub = tmp_path / name
        (sub / "src").mkdir(parents=True)
        (sub / "package.json").write_text("{}")

    assert _has_duplicate_project_scaffolding(str(tmp_path)) is None


def test_has_duplicate_project_scaffolding_none_when_clean(tmp_path):
    frontend = tmp_path / "frontend"
    (frontend / "src").mkdir(parents=True)
    (frontend / "package.json").write_text("{}")

    assert _has_duplicate_project_scaffolding(str(tmp_path)) is None


@pytest.mark.parametrize(
    "marker",
    ["pom.xml", "go.mod", "Gemfile", "Cargo.toml", "Pipfile", "requirements.txt", "package.json"],
)
def test_has_duplicate_project_scaffolding_detects_any_stack_marker(tmp_path, marker):
    """Gap real (usuario, "debe de aplicar a cualquier lenguaje de
    programacion"): el chequeo original solo miraba package.json+src/ --
    ahora usa los mismos marcadores reales que coding_agent.py._STACK_MARKERS
    (Maven, Go, Ruby, Rust, Python, Node), no solo Node/TS."""
    service = tmp_path / "service"
    service.mkdir()
    (service / marker).write_text("")

    nested = service / "nested-app"
    nested.mkdir()
    (nested / marker).write_text("")

    result = _has_duplicate_project_scaffolding(str(tmp_path))

    assert result is not None
    assert "service" in result and "nested-app" in result


def test_has_duplicate_project_scaffolding_detects_dotnet_marker(tmp_path):
    service = tmp_path / "service"
    service.mkdir()
    (service / "Service.csproj").write_text("")

    nested = service / "nested-app"
    nested.mkdir()
    (nested / "NestedApp.csproj").write_text("")

    result = _has_duplicate_project_scaffolding(str(tmp_path))

    assert result is not None


def test_has_duplicate_project_scaffolding_ignores_vendored_markers(tmp_path):
    """Un marcador dentro de un directorio de dependencias reales (vendor/,
    target/, etc.) no es un proyecto duplicado -- es una dependencia
    normal, no debe dispararlo."""
    service = tmp_path / "service"
    service.mkdir()
    (service / "go.mod").write_text("")

    vendored = service / "vendor" / "some-dep"
    vendored.mkdir(parents=True)
    (vendored / "go.mod").write_text("")

    assert _has_duplicate_project_scaffolding(str(tmp_path)) is None


def test_filter_active_children_excludes_terminal_status():
    """Gap real (usuario, "gaps en el scrum agent"): el planificador
    (epic_planner.py) gastaba un turno real razonando sobre historias ya
    Done -- este filtro las saca ANTES de que le lleguen."""
    children = [
        {"ticket_id": "C-1", "status": "Done"},
        {"ticket_id": "C-2", "status": "In Progress"},
    ]

    active, already_terminal = _filter_active_children(children, {"Done"})

    assert active == [{"ticket_id": "C-2", "status": "In Progress"}]
    assert already_terminal == [{"ticket_id": "C-1", "status": "Done"}]


def test_filter_active_children_treats_missing_status_as_active():
    """Fail-safe: un child sin status (JQL viejo, o el campo no vino) nunca
    se excluye por falta de dato."""
    children = [{"ticket_id": "C-1"}, {"ticket_id": "C-2", "status": None}]

    active, already_terminal = _filter_active_children(children, {"Done"})

    assert active == children
    assert already_terminal == []


def test_filter_active_children_respects_custom_terminal_statuses():
    children = [
        {"ticket_id": "C-1", "status": "Won't Do"},
        {"ticket_id": "C-2", "status": "Done"},
        {"ticket_id": "C-3", "status": "In Progress"},
    ]

    active, already_terminal = _filter_active_children(children, {"Done", "Won't Do"})

    assert active == [{"ticket_id": "C-3", "status": "In Progress"}]
    assert {c["ticket_id"] for c in already_terminal} == {"C-1", "C-2"}


def test_query_component_risk_history_combines_both_queries(monkeypatch):
    """Gap real (usuario, "como Neo4j relaciona cada tema"): graph_writer.py
    ya escribe Risk/Run/Epic/Story reales pero nadie los leia de vuelta --
    confirma que el historial real de riesgos Y de tickets que tocaron el
    componente llega combinado."""
    def fake_run(cmd, **kwargs):
        query = cmd[-1]
        if "Risk" in query:
            return "riesgo_documentado | veces\nscope-mismatch | 2\n"
        return "ticket | resumen\nKAN-5 | Historia real\n"

    monkeypatch.setattr(orchestration, "_run", fake_run)

    result = _query_component_risk_history("Frontend")

    assert "Riesgos documentados" in result
    assert "scope-mismatch | 2" in result
    assert "Tickets que ya tocaron este componente" in result
    assert "KAN-5" in result


def test_query_component_risk_history_returns_empty_when_no_data(monkeypatch):
    monkeypatch.setattr(orchestration, "_run", lambda cmd, **k: "")

    assert _query_component_risk_history("Frontend") == ""


def test_query_component_risk_history_degrades_gracefully_on_failure(monkeypatch):
    """Best-effort real: a diferencia de la query de dependencias existente
    (retries=2, puede fallar la tarea), esta nunca debe tirar la corrida --
    si una de las dos queries falla, la otra igual se devuelve."""
    def fake_run(cmd, **kwargs):
        query = cmd[-1]
        if "Risk" in query:
            raise RuntimeError("comando fallo (cypher-shell): conexion rechazada")
        return "ticket | resumen\nKAN-5 | Historia real\n"

    monkeypatch.setattr(orchestration, "_run", fake_run)

    result = _query_component_risk_history("Frontend")

    assert "Riesgos documentados" not in result
    assert "KAN-5" in result


def test_query_graph_appends_risk_history_when_present(monkeypatch):
    monkeypatch.setattr(orchestration, "_run", lambda cmd, **k: "servicio | lenguaje\nAuthService | Java\n")
    monkeypatch.setattr(orchestration, "_query_component_risk_history", lambda component: "Riesgos documentados:\nscope-mismatch | 1")

    result = orchestration.query_graph.fn("Frontend")

    assert "AuthService | Java" in result
    assert "Historial real de riesgos/corridas para este componente" in result
    assert "scope-mismatch | 1" in result


def test_query_graph_omits_risk_history_section_when_empty(monkeypatch):
    monkeypatch.setattr(orchestration, "_run", lambda cmd, **k: "servicio | lenguaje\nAuthService | Java\n")
    monkeypatch.setattr(orchestration, "_query_component_risk_history", lambda component: "")

    result = orchestration.query_graph.fn("Frontend")

    assert "AuthService | Java" in result
    assert "Historial real de riesgos/corridas" not in result


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
    monkeypatch.setattr(orchestration, "_check_already_completed", lambda *a, **k: False)
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


def test_deliver_transitions_to_blocked_and_comments_when_judge_gives_no_verdict(monkeypatch):
    """Bug real confirmado en vivo (KAN-2, epica KAN-4): cuando el juez no
    puede evaluar (judge_verdict is None), _deliver() antes solo hacia
    print() -- ni comentario en Jira ni transicion de status -- y el ticket
    quedaba clavado en JIRA_IN_PROGRESS_STATUS para siempre, indistinguible
    de una corrida que sigue en curso de verdad."""
    monkeypatch.setattr(orchestration, "GITHUB_REPO", "")
    monkeypatch.setattr(orchestration, "_local_coding_agent_backend_available", lambda: True)
    monkeypatch.setattr(orchestration, "_check_already_completed", lambda *a, **k: False)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "branch": "copilot/T-1-1", "base_branch": "main", "backend": "ollama", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: None)
    monkeypatch.setattr(orchestration.subprocess, "run", lambda cmd, **k: None)
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])

    transitions = []
    comments = []
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: transitions.append((status, ticket_key)))
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append((ticket_key, text)))

    result = _deliver(
        "T-1", "summary", {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
        {"ticket_id": "T-1", "repository_origen": "Frontend"}, "/repo",
    )

    assert result["judge"] is None
    assert (orchestration.JIRA_BLOCKED_STATUS, "T-1") in transitions
    assert any("no pudo evaluar" in text for _key, text in comments)


def test_retry_for_inadequate_tests_preserves_branch_when_retry_omits_it(monkeypatch):
    """retry_coding_agent_local_real() nunca devuelve branch/base_branch (
    reusa la rama ya checked out por el primer intento) -- si
    _retry_for_inadequate_tests() devolviera el resultado del reintento tal
    cual, el resto de _deliver perderia la referencia a la rama real donde
    esta el commit nuevo. Este test confirma que branch/base_branch del
    agent_result original sobreviven al merge.
    """
    captured = {}

    def fake_retry(ticket_id, feedback_text, target_repo_dir, conversation_file=None):
        captured["ticket_id"] = ticket_id
        captured["feedback_text"] = feedback_text
        captured["conversation_file"] = conversation_file
        return {"applied": True, "backend": "anthropic", "self_review": {"tests_adequate": True}, "conversation_file": "/tmp/conv2.json"}

    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", fake_retry)

    original = {
        "applied": True, "branch": "copilot/T-1-123", "base_branch": "main",
        "backend": "ollama", "conversation_file": "/tmp/conv1.json", "self_review": {"tests_adequate": False},
    }

    result = orchestration._retry_for_inadequate_tests("T-1", "/repo", original)

    assert result["branch"] == "copilot/T-1-123"
    assert result["base_branch"] == "main"
    assert result["conversation_file"] == "/tmp/conv2.json"
    assert result["self_review"] == {"tests_adequate": True}
    assert captured["ticket_id"] == "T-1"
    assert captured["conversation_file"] == "/tmp/conv1.json"
    assert "test real" in captured["feedback_text"]


def test_retry_for_inadequate_tests_returns_original_when_retry_applies_nothing(monkeypatch):
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: {"applied": False})

    original = {
        "applied": True, "branch": "copilot/T-1-123", "base_branch": "main",
        "backend": "ollama", "conversation_file": "/tmp/conv1.json", "self_review": {"tests_adequate": False},
    }

    result = orchestration._retry_for_inadequate_tests("T-1", "/repo", original)

    assert result is original


def test_deliver_retries_coding_agent_when_self_review_flags_inadequate_tests(monkeypatch):
    """El coding agent se autoevalua honestamente con tests_adequate=False
    (confirmado real en KAN-15/KAN-5) pero antes nadie actuaba sobre eso --
    _deliver() ahora le da un turno mas ANTES de correr tests/juez."""
    retry_calls = []
    monkeypatch.setattr(orchestration, "GITHUB_REPO", "")
    monkeypatch.setattr(orchestration, "_local_coding_agent_backend_available", lambda: True)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {
            "applied": True, "branch": "copilot/T-1-1", "base_branch": "main", "backend": "ollama",
            "conversation_file": "/tmp/conv1.json", "self_review": {"tests_adequate": False},
        },
    )

    def fake_retry(ticket_id, feedback_text, target_repo_dir, conversation_file=None):
        retry_calls.append((ticket_id, conversation_file))
        return {
            "applied": True, "backend": "ollama",
            "self_review": {"tests_adequate": True}, "conversation_file": "/tmp/conv2.json",
        }

    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", fake_retry)
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "ok"})
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "3 passed"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": None, "pushed": False, "reason": "sin gh"})
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)

    result = _deliver(
        "T-1", "summary", {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
        {"ticket_id": "T-1", "repository_origen": "Frontend"}, "/repo",
    )

    assert retry_calls == [("T-1", "/tmp/conv1.json")]
    # branch/base_branch del intento original sobreviven al merge, aunque
    # retry_coding_agent_local_real() no los devuelva.
    assert result["agent"]["branch"] == "copilot/T-1-1"


def test_deliver_does_not_retry_when_tests_adequate_true_or_missing(monkeypatch):
    monkeypatch.setattr(orchestration, "GITHUB_REPO", "")
    monkeypatch.setattr(orchestration, "_local_coding_agent_backend_available", lambda: True)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {
            "applied": True, "branch": "copilot/T-1-1", "base_branch": "main", "backend": "ollama",
            "conversation_file": None, "self_review": {"tests_adequate": True},
        },
    )
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: pytest.fail("no deberia reintentar"))
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "ok"})
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "3 passed"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": None, "pushed": False, "reason": "sin gh"})
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)

    _deliver(
        "T-1", "summary", {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
        {"ticket_id": "T-1", "repository_origen": "Frontend"}, "/repo",
    )


def test_deliver_comments_real_test_output_when_tests_pass(monkeypatch):
    """Confirmado real (usuario): "no hay visibilidad de las pruebas" --
    cuando run_tests() PASA, el output real nunca llegaba a ningun lado
    visible para un humano. Este test confirma que ahora se postea un
    comentario real con el output."""
    monkeypatch.setattr(orchestration, "GITHUB_REPO", "")
    monkeypatch.setattr(orchestration, "_local_coding_agent_backend_available", lambda: True)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {
            "applied": True, "branch": "copilot/T-1-1", "base_branch": "main", "backend": "ollama",
            "conversation_file": None, "self_review": {"tests_adequate": True},
        },
    )
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "ok"})
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "======== 7 passed in 1.2s ========"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": None, "pushed": False, "reason": "sin gh"})
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)

    comments = []
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append(text))

    _deliver(
        "T-1", "summary", {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
        {"ticket_id": "T-1", "repository_origen": "Frontend"}, "/repo",
    )

    assert any("PASARON" in text and "7 passed in 1.2s" in text for text in comments)


def test_deliver_injects_test_plan_into_coding_agent_prompt_and_comments_it(monkeypatch):
    """Testing Agent liviano (evaluacion del workflow multi-agente pedida por
    el usuario, version reducida aprobada): un Test Plan real generado ANTES
    de implementar se postea en Jira Y se inyecta en el prompt que recibe el
    coding agent."""
    monkeypatch.setattr(orchestration, "GITHUB_REPO", "")
    monkeypatch.setattr(orchestration, "_local_coding_agent_backend_available", lambda: True)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(
        orchestration, "generate_test_plan",
        lambda evidence: "## Casos Negativos\n- entrada invalida" if evidence["ticket"] == "T-1" else None,
    )

    captured_prompts = []

    def fake_run_first(ticket_id, sanitized, target_repo_dir):
        captured_prompts.append(sanitized)
        return {
            "applied": True, "branch": "copilot/T-1-1", "base_branch": "main", "backend": "ollama",
            "conversation_file": None, "self_review": {"tests_adequate": True},
        }

    monkeypatch.setattr(orchestration, "run_coding_agent_local_real", fake_run_first)
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "ok"})
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": None, "pushed": False, "reason": "sin gh"})
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)

    comments = []
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append(text))

    _deliver(
        "T-1", "summary", {"status": "APPROVED", "sanitized_prompt": "prompt original", "redactions_applied": 0, "reason": None},
        {"ticket_id": "T-1", "repository_origen": "Frontend"}, "/repo",
    )

    assert any("Test Plan" in text and "entrada invalida" in text for text in comments)
    assert len(captured_prompts) == 1
    assert "prompt original" in captured_prompts[0]
    assert "entrada invalida" in captured_prompts[0]
    assert "especialmente los negativos" in captured_prompts[0]


def test_deliver_does_not_touch_prompt_when_test_plan_generation_returns_none(monkeypatch):
    monkeypatch.setattr(orchestration, "GITHUB_REPO", "")
    monkeypatch.setattr(orchestration, "_local_coding_agent_backend_available", lambda: True)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_test_plan", lambda *a, **k: None)

    captured_prompts = []

    def fake_run_first(ticket_id, sanitized, target_repo_dir):
        captured_prompts.append(sanitized)
        return {
            "applied": True, "branch": "copilot/T-1-1", "base_branch": "main", "backend": "ollama",
            "conversation_file": None, "self_review": {"tests_adequate": True},
        }

    monkeypatch.setattr(orchestration, "run_coding_agent_local_real", fake_run_first)
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "ok"})
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": None, "pushed": False, "reason": "sin gh"})
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)

    _deliver(
        "T-1", "summary", {"status": "APPROVED", "sanitized_prompt": "prompt original", "redactions_applied": 0, "reason": None},
        {"ticket_id": "T-1", "repository_origen": "Frontend"}, "/repo",
    )

    assert captured_prompts == ["prompt original"]


def test_deliver_includes_salida_tests_in_technical_report(monkeypatch):
    monkeypatch.setattr(orchestration, "GITHUB_REPO", "")
    monkeypatch.setattr(orchestration, "_local_coding_agent_backend_available", lambda: True)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {
            "applied": True, "branch": "copilot/T-1-1", "base_branch": "main", "backend": "ollama",
            "conversation_file": None, "self_review": {"tests_adequate": True},
        },
    )
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "ok"})
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: _CLEAN_GUARD_RESULT)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "7 passed"})
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "diff")
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": None, "pushed": False, "reason": "sin gh"})
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)

    captured_evidence = {}
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda evidence: captured_evidence.update(evidence) or None)

    _deliver(
        "T-1", "summary", {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
        {"ticket_id": "T-1", "repository_origen": "Frontend"}, "/repo",
    )

    assert captured_evidence["salida_tests"] == "7 passed"


def test_deliver_skips_pipeline_when_already_completed(monkeypatch):
    """Gap real (usuario): _deliver() no deberia correr firewall/coding
    agent/tests si _check_already_completed() ya determino que no hace
    falta -- ni un solo llamado a run_coding_agent_local_real."""
    monkeypatch.setattr(orchestration, "_check_already_completed", lambda *a, **k: True)
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: pytest.fail("no deberia correr el coding agent"),
    )
    monkeypatch.setattr(orchestration, "_transition_all", lambda *a, **k: pytest.fail("no deberia transicionar de nuevo"))

    result = _deliver(
        "T-1", "summary", {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
        {"ticket_id": "T-1", "repository_origen": "Frontend", "status": orchestration.JIRA_DONE_STATUS}, "/repo",
    )

    assert result == {
        "firewall": {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
        "agent": None, "judge": None, "skipped": "already_completed",
    }


def test_deliver_epic_sequential_skips_already_completed_child_but_processes_siblings(monkeypatch):
    """Mismo gap, en modo epica: un hijo ya mergeado no deberia volver a
    pasar por firewall/coding agent, pero sus hermanos SI tienen que
    seguir procesandose con normalidad."""
    children = [_fake_child("C-1"), _fake_child("C-2")]
    firewall_calls = []

    def fake_check_already_completed(ticket_id, jira_context, target_repo_dir, is_epic=False, child_ticket_keys=None):
        return ticket_id == "C-1"

    monkeypatch.setattr(orchestration, "_check_already_completed", fake_check_already_completed)

    def fake_firewall(prompt, jira_context, sonar_errors):
        firewall_calls.append(jira_context["ticket_id"])
        return {"status": "APPROVED", "sanitized_prompt": "prompt saneado", "redactions_applied": 0, "reason": None}

    monkeypatch.setattr(orchestration, "evaluate_firewall", fake_firewall)
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "branch": "copilot/EPIC-1-1", "base_branch": "main", "backend": "anthropic", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "todo bien"})
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": "https://x/pr/1", "pushed": True, "reason": None})
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration.Path, "unlink", lambda self, missing_ok=True: None)

    result = orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    assert firewall_calls == ["C-2"]  # C-1 nunca llego al firewall
    assert result["completed"] == [
        {"ticket_id": "C-1", "outcome": "already-completed"},
        {"ticket_id": "C-2", "outcome": "ok"},
    ]


def test_deliver_epic_sequential_transitions_epic_to_done_when_all_children_already_completed(monkeypatch):
    children = [_fake_child("C-1"), _fake_child("C-2")]
    monkeypatch.setattr(orchestration, "_check_already_completed", lambda *a, **k: True)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)

    transitions = []
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: transitions.append((ticket_key, status)))

    result = orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    assert result["completed"] == [
        {"ticket_id": "C-1", "outcome": "already-completed"},
        {"ticket_id": "C-2", "outcome": "already-completed"},
    ]
    assert ("EPIC-1", orchestration.JIRA_DONE_STATUS) in transitions


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
    # Bug real confirmado (KAN-4): la epica en si nunca recibia un status
    # final -- quedaba clavada en JIRA_IN_PROGRESS_STATUS para siempre.
    assert ("EPIC-1", orchestration.JIRA_BLOCKED_STATUS) in transitions


def test_deliver_epic_sequential_transitions_epic_to_review_when_all_children_ok(monkeypatch):
    """Bug real confirmado (KAN-4): con TODAS las historias en outcome "ok",
    la epica en si nunca se transicionaba -- quedaba en JIRA_IN_PROGRESS_STATUS
    para siempre aunque las 2 historias terminaran listas para revision."""
    children = [_fake_child("C-1"), _fake_child("C-2")]
    transitions = []

    monkeypatch.setattr(
        orchestration, "evaluate_firewall",
        lambda *a, **k: {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: transitions.append((ticket_key, status)))
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "branch": "copilot/EPIC-1-1", "base_branch": "main", "backend": "anthropic", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(
        orchestration, "retry_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "backend": "anthropic", "self_review": None, "conversation_file": None},
    )
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "todo bien"})
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": "https://x/pr/1", "pushed": True, "reason": None})
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration.Path, "unlink", lambda self, missing_ok=True: None)

    result = orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    assert result["blocked_at"] is None
    assert ("EPIC-1", orchestration.JIRA_REVIEW_STATUS) in transitions


def test_deliver_epic_sequential_pr_body_describes_real_changes_not_raw_epic_description(monkeypatch):
    """Bug real confirmado (usuario, PR #238 real en Azure DevOps): el body
    de la PR pasaba epic["description"] tal cual -- el texto ORIGINAL de la
    epica (miles de caracteres de un prompt de planificacion), sin ninguna
    relacion con lo que esa rama puntual realmente cambio. Ahora tiene que
    describir que historias se procesaron."""
    children = [_fake_child("C-1"), _fake_child("C-2")]
    pr_calls = []

    monkeypatch.setattr(
        orchestration, "evaluate_firewall",
        lambda *a, **k: {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "branch": "copilot/EPIC-1-1", "base_branch": "main", "backend": "anthropic", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(
        orchestration, "retry_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "backend": "anthropic", "self_review": None, "conversation_file": None},
    )
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "todo bien"})
    monkeypatch.setattr(
        orchestration, "push_and_open_pr",
        lambda target_repo_dir, branch, base_branch, ticket_id, summary, body_text: pr_calls.append(body_text) or {"pr_url": "https://x/pr/1", "pushed": True, "reason": None},
    )
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration.Path, "unlink", lambda self, missing_ok=True: None)

    huge_raw_description = "/ai Actua como un Product Owner Senior..." * 50
    orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": huge_raw_description}, children, "/repo", [], [], [], "", [],
    )

    assert len(pr_calls) == 1
    assert huge_raw_description not in pr_calls[0]
    assert "C-1" in pr_calls[0] and "C-2" in pr_calls[0]
    assert "ok" in pr_calls[0]


def test_deliver_epic_sequential_child_no_verdict_transitions_to_blocked(monkeypatch):
    """Bug real confirmado en vivo (KAN-2, epica KAN-4): cuando el juez no
    puede evaluar un hijo, antes solo se comentaba -- el hijo quedaba
    clavado en JIRA_IN_PROGRESS_STATUS para siempre."""
    children = [_fake_child("C-1")]
    transitions = []
    comments = []

    monkeypatch.setattr(
        orchestration, "evaluate_firewall",
        lambda *a, **k: {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append((ticket_key, text)))
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: transitions.append((ticket_key, status)))
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": True, "branch": "copilot/EPIC-1-1", "base_branch": "main", "backend": "anthropic", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": "https://x/pr/1", "pushed": True, "reason": None})
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration.Path, "unlink", lambda self, missing_ok=True: None)

    result = orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    assert result["completed"] == [{"ticket_id": "C-1", "outcome": "no-verdict"}]
    assert ("C-1", orchestration.JIRA_BLOCKED_STATUS) in transitions
    assert any("no pudo evaluar" in text for _key, text in comments if _key == "C-1")


def test_deliver_epic_sequential_retries_child_when_self_review_flags_inadequate_tests(monkeypatch):
    """Mismo mecanismo que en modo ticket unico, pero para el fan-out de
    hijos de una epica: el coding agent se autoevalua con
    tests_adequate=False para un hijo especifico -- se le da un turno mas
    ANTES de correr tests/juez para ESE hijo."""
    children = [_fake_child("C-1")]
    retry_calls = []

    monkeypatch.setattr(
        orchestration, "evaluate_firewall",
        lambda *a, **k: {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {
            "applied": True, "branch": "copilot/EPIC-1-1", "base_branch": "main", "backend": "anthropic",
            "conversation_file": "/tmp/conv1.json", "self_review": {"tests_adequate": False},
        },
    )

    def fake_retry(ticket_id, feedback_text, target_repo_dir, conversation_file=None):
        retry_calls.append((ticket_id, conversation_file))
        return {
            "applied": True, "backend": "anthropic",
            "self_review": {"tests_adequate": True}, "conversation_file": "/tmp/conv2.json",
        }

    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", fake_retry)
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "todo bien"})
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": "https://x/pr/1", "pushed": True, "reason": None})
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration.Path, "unlink", lambda self, missing_ok=True: None)

    result = orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    # _retry_for_inadequate_tests() en modo epica usa epic_key como ticket_id
    # (mismo criterio ya establecido por _retry_local_diff/_retry_after_no_changes
    # para este modo), no el child_id.
    assert retry_calls == [("EPIC-1", "/tmp/conv1.json")]
    assert result["completed"] == [{"ticket_id": "C-1", "outcome": "ok"}]


def test_deliver_epic_sequential_comments_real_test_output_and_report_for_ok_child(monkeypatch):
    """Confirmado real (usuario): "no hay visibilidad de las pruebas" en modo
    epica tambien -- ahora el comentario de tests pasando y el comprobante
    tecnico final del hijo OK incluyen la salida real."""
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
        lambda *a, **k: {
            "applied": True, "branch": "copilot/EPIC-1-1", "base_branch": "main", "backend": "anthropic",
            "conversation_file": None, "self_review": {"tests_adequate": True},
        },
    )
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "==== 4 passed ===="})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "todo bien"})
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": "https://x/pr/1", "pushed": True, "reason": None})
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration.Path, "unlink", lambda self, missing_ok=True: None)

    def fake_generate_report(evidence):
        report_calls.append(evidence)
        return None

    monkeypatch.setattr(orchestration, "generate_technical_report", fake_generate_report)

    orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    assert any("PASARON" in text and "4 passed" in text for _key, text in comments if _key == "C-1")
    ok_reports = [ev for ev in report_calls if ev.get("resultado", "").startswith("OK")]
    assert ok_reports and ok_reports[0]["salida_tests"] == "==== 4 passed ===="


def test_deliver_epic_sequential_injects_test_plan_per_child(monkeypatch):
    """Testing Agent liviano en modo epica: cada hijo recibe su PROPIO Test
    Plan real, inyectado en el prompt/feedback que se le pasa al coding
    agent para esa historia puntual."""
    children = [_fake_child("C-1"), _fake_child("C-2")]
    comments = []
    captured_prompts = []

    monkeypatch.setattr(
        orchestration, "evaluate_firewall",
        lambda *a, **k: {"status": "APPROVED", "sanitized_prompt": "prompt saneado", "redactions_applied": 0, "reason": None},
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append((ticket_key, text)))
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")
    monkeypatch.setattr(
        orchestration, "generate_test_plan",
        lambda evidence: f"## Casos Negativos\n- caso negativo de {evidence['ticket']}",
    )

    def fake_run_first(ticket_id, sanitized, target_repo_dir):
        captured_prompts.append(("first", sanitized))
        return {
            "applied": True, "branch": "copilot/EPIC-1-123", "base_branch": "main",
            "backend": "anthropic", "conversation_file": "/tmp/conv1.json", "self_review": {},
        }

    def fake_retry(ticket_id, feedback_text, target_repo_dir, conversation_file=None):
        captured_prompts.append(("retry", feedback_text))
        return {"applied": True, "backend": "anthropic", "self_review": {}, "conversation_file": "/tmp/conv2.json"}

    monkeypatch.setattr(orchestration, "run_coding_agent_local_real", fake_run_first)
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", fake_retry)
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})
    monkeypatch.setattr(orchestration, "rescan_sonar", lambda *a, **k: [])
    monkeypatch.setattr(orchestration, "_run_judge_safe", lambda *a, **k: {"verdict": "OK", "reasoning": "todo bien"})
    monkeypatch.setattr(orchestration, "push_and_open_pr", lambda *a, **k: {"pr_url": "https://x/pr/1", "pushed": True, "reason": None})
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)
    monkeypatch.setattr(orchestration.Path, "unlink", lambda self, missing_ok=True: None)

    orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc epica"}, children, "/repo", [], [], [], "", [],
    )

    assert captured_prompts[0] == ("first", "prompt saneado\n\n--- Test Plan real generado antes de implementar ---\n## Casos Negativos\n- caso negativo de C-1\nImplementa cubriendo estos casos, especialmente los negativos.")
    assert "caso negativo de C-2" in captured_prompts[1][1]
    assert any("Test Plan" in text and "caso negativo de C-1" in text for key, text in comments if key == "C-1")
    assert any("Test Plan" in text and "caso negativo de C-2" in text for key, text in comments if key == "C-2")


def test_deliver_epic_sequential_comment_names_untouched_siblings_on_block(monkeypatch):
    """Confirmado real (epica KAN-4): un humano mirando SOLO el ticket hijo
    bloqueado no tenia forma de saber si el resto de la epica sigue o se
    corta -- esa info solo vivia en el comentario-resumen final de la
    epica. El comentario de bloqueo de ESE hijo ahora la incluye."""
    children = [_fake_child("C-1"), _fake_child("C-2"), _fake_child("C-3")]
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
        lambda *a, **k: {"applied": True, "branch": "copilot/EPIC-1-1", "base_branch": "main", "backend": "anthropic", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(orchestration, "run_output_guard", lambda *a, **k: {"redactions_applied": 0, "jailbreak_reason": None, "clean": True})
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": False, "output": "fallo"})
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "retry_coding_agent_local_real", lambda *a, **k: pytest.fail("no deberia llegar al tercer hijo"))
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda *a, **k: None)

    orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    # se corta en C-1 (el primero); C-2 y C-3 quedan sin tocar
    blocked_comment = next(text for key, text in comments if key == "C-1" and "FALLARON" in text)
    assert "C-2" in blocked_comment and "C-3" in blocked_comment


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


def test_deliver_epic_sequential_summary_warns_when_no_real_changes_applied(monkeypatch):
    """Bug real confirmado en vivo (operacion de esta noche): con TODAS las
    historias en outcome "no-op" (el agente no aplico ningun cambio real --
    ej. confirmaciones interactivas auto-rechazadas por falta de stdin), el
    comentario resumen antes decia "2/2 historias procesadas" sin ninguna
    forma de distinguir eso de una epica genuinamente completa. Ahora
    desglosa por outcome real y agrega una advertencia explicita.
    """
    children = [_fake_child("C-1"), _fake_child("C-2")]
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
        lambda *a, **k: {"applied": False, "branch": None, "base_branch": "main", "backend": "ollama", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda evidence: None)

    orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    epic_summary = next(text for tk, text in comments if tk == "EPIC-1" and "Modo epica secuencial" in text)
    assert "2 no-op" in epic_summary
    assert "Ninguna historia aplico cambios reales" in epic_summary


def test_deliver_epic_sequential_records_blocked_agent_state_when_all_no_op(monkeypatch):
    """El estado agregado de la epica (TicketState, pipeline_shared.py) que
    se persiste en el grafo real via record_run_in_graph tiene que reflejar
    que NINGUNA historia aplico un cambio real -- BLOCKED_AGENT, no DONE."""
    children = [_fake_child("C-1"), _fake_child("C-2")]

    monkeypatch.setattr(
        orchestration, "evaluate_firewall",
        lambda *a, **k: {"status": "APPROVED", "sanitized_prompt": "prompt", "redactions_applied": 0, "reason": None},
    )
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "_run", lambda *a, **k: "hash\n")
    monkeypatch.setattr(
        orchestration, "run_coding_agent_local_real",
        lambda *a, **k: {"applied": False, "branch": None, "base_branch": "main", "backend": "ollama", "conversation_file": None, "self_review": None},
    )
    monkeypatch.setattr(orchestration, "check_falco_correlation", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "post_alert_webhook", lambda *a, **k: None)
    monkeypatch.setattr(orchestration, "generate_technical_report", lambda evidence: None)

    recorded_payloads = []
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda payload: recorded_payloads.append(payload))

    orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    assert recorded_payloads[0]["state"] == "blocked_agent"


def test_deliver_epic_sequential_records_done_state_when_all_already_completed(monkeypatch):
    children = [_fake_child("C-1"), _fake_child("C-2")]

    monkeypatch.setattr(orchestration, "_check_already_completed", lambda *a, **k: True)
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: None)

    recorded_payloads = []
    monkeypatch.setattr(orchestration, "record_run_in_graph", lambda payload: recorded_payloads.append(payload))

    orchestration._deliver_epic_sequential(
        "EPIC-1", {"summary": "epica", "description": "desc"}, children, "/repo", [], [], [], "", [],
    )

    assert recorded_payloads[0]["state"] == "done"


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

    # El resumen final se postea a la epica Y se mirrorea a C-1 (outcome
    # "ok", realmente aporto a la rama/PR) -- confirmado real (usuario): la
    # URL de la PR tiene que verse tambien en la documentacion del ticket,
    # no solo en la epica. Nada extra del comprobante (que devolvio None).
    summary_comments = [(tk, text) for tk, text in comments if "Modo epica secuencial" in text]
    assert len(summary_comments) == 2
    assert {tk for tk, _text in summary_comments} == {"EPIC-1", "C-1"}


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


def test_run_tests_logs_real_output_on_failure(monkeypatch):
    """Gap real de observabilidad: si los tests fallan, el juez ni se llama
    (PipelineBlocked antes de eso), asi que la salida real de
    run_module_tests.sh quedaba completamente invisible en Prefect/Jira."""
    monkeypatch.setattr(orchestration.shutil, "which", lambda name: "/usr/bin/docker")
    monkeypatch.delenv("HOST_TARGET_REPO_DIR", raising=False)

    class _FakeLogger:
        def __init__(self):
            self.warnings = []

        def warning(self, msg):
            self.warnings.append(msg)

    fake_logger = _FakeLogger()
    monkeypatch.setattr(orchestration, "get_run_logger", lambda: fake_logger)

    def fake_run(cmd, **kwargs):
        class R:
            returncode = 1
            stdout = "FAIL frontend/public/404.html: elemento no encontrado\n"
            stderr = ""
        return R()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    result = orchestration.run_tests("/target-repo")

    assert result["passed"] is False
    assert any("elemento no encontrado" in w for w in fake_logger.warnings)


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


def _fake_subprocess_result(returncode=0, stdout="", stderr=""):
    class R:
        pass
    r = R()
    r.returncode = returncode
    r.stdout = stdout
    r.stderr = stderr
    return r


def test_run_coding_agent_local_real_detects_commits_made_by_the_model_itself(monkeypatch, tmp_path):
    """Bug real confirmado esta sesion (KAN-15): el modelo puede llamar
    "git commit" el mismo via run_shell_command, en vez de solo escribir
    archivos y dejar que run_coding_agent_local_real() comitee -- en ese
    caso 'git status --porcelain' queda LIMPIO (ya esta commiteado). Mirar
    solo el working tree hacia que ese commit real se interpretara como
    "no aplico nada", BORRANDO la rama con el commit real adentro.
    """
    git_calls = []

    class FakeResult:
        returncode = 0
        stdout = json.dumps({"status": "done", "summary": "listo", "_meta": {"backend": "ollama"}})

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            return FakeResult()
        git_calls.append(cmd)
        if cmd[-2:] == ["--abbrev-ref", "HEAD"]:
            return _fake_subprocess_result(stdout="main\n")
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout="")  # limpio -- el modelo ya commiteo el mismo
        if "rev-list" in cmd:
            return _fake_subprocess_result(stdout="1\n")  # 1 commit real por encima de base_branch
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    result = orchestration.run_coding_agent_local_real("T-1", "prompt", str(tmp_path))

    assert result["applied"] is True
    assert result["branch"] is not None
    # NO se llamo "branch -D" (la rama con el commit real no se borro)
    assert not any(cmd[-2:-1] == ["branch"] and "-D" in cmd for cmd in git_calls)


def test_run_coding_agent_local_does_not_crash_when_git_commit_fails(monkeypatch, tmp_path):
    """Mismo bug que run_coding_agent_local_real, camino gh_copilot_suggest
    (Camino B2): un 'git commit' fallido no debe crashear la epica.
    """
    class FakeSuggestResult:
        returncode = 0

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "gh":
            return FakeSuggestResult()
        if cmd[-2:] == ["--abbrev-ref", "HEAD"]:
            return _fake_subprocess_result(stdout="main\n")
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout="M file.txt\n")
        if "commit" in cmd:
            raise orchestration.subprocess.CalledProcessError(128, cmd, stderr="Author identity unknown")
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    result = orchestration.run_coding_agent_local("T-1", "prompt", str(tmp_path))

    assert result["applied"] is False


def test_retry_coding_agent_local_real_does_not_crash_when_git_commit_fails(monkeypatch, tmp_path):
    """Mismo bug en el camino de reintento (feedback del juez): un
    'git commit' fallido no debe crashear la epica, y ademas no debe
    reportar applied=True cuando en realidad no quedo nada commiteado.
    """
    class FakeResult:
        returncode = 0
        stdout = json.dumps({"status": "done", "summary": "listo", "_meta": {"backend": "ollama"}})

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            return FakeResult()
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return _fake_subprocess_result(stdout="abc123\n")  # mismo checkpoint siempre -- sin commits propios del modelo
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout="M file.txt\n")  # el agente escribio algo real
        if "commit" in cmd:
            raise orchestration.subprocess.CalledProcessError(128, cmd, stderr="Author identity unknown")
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    result = orchestration.retry_coding_agent_local_real("T-1", "feedback", str(tmp_path))

    assert result["applied"] is False


def test_run_coding_agent_local_real_does_not_crash_when_git_commit_fails(monkeypatch, tmp_path):
    """Bug real confirmado en vivo (operacion de esta noche): un 'git commit'
    fallido (ej. identidad de git no configurada en el clon) lanzaba
    CalledProcessError sin atrapar, tirando abajo la epica ENTERA via Prefect
    en vez de bloquear solo esta historia puntual y seguir con las demas.
    """
    class FakeResult:
        returncode = 0
        stdout = json.dumps({"status": "done", "summary": "listo", "_meta": {"backend": "ollama"}})

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            return FakeResult()
        if cmd[-2:] == ["--abbrev-ref", "HEAD"]:
            return _fake_subprocess_result(stdout="main\n")
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout="M file.txt\n")  # el agente escribio algo real
        if "rev-list" in cmd:
            return _fake_subprocess_result(stdout="0\n")  # sin commits propios del modelo todavia
        if "commit" in cmd:
            raise orchestration.subprocess.CalledProcessError(128, cmd, stderr="Author identity unknown")
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    result = orchestration.run_coding_agent_local_real("T-1", "prompt", str(tmp_path))

    # No crashea (no propaga CalledProcessError) -- se degrada a "no aplicado".
    assert result["applied"] is False


def test_run_coding_agent_local_real_surfaces_self_verified_and_risk_graph(monkeypatch, tmp_path):
    """Gap real (usuario, "hay gaps en el coding agent"): coding_agent.py
    calcula self_verified/consulted_risk_graph (evidencia real, no
    autoreportada) pero antes se perdian ahi mismo -- run_coding_agent_local_real()
    nunca los leia del JSON del subprocess."""
    class FakeResult:
        returncode = 0
        stdout = json.dumps({
            "status": "done", "summary": "listo", "_meta": {"backend": "ollama"},
            "self_verified": True, "consulted_risk_graph": True,
        })

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            return FakeResult()
        if cmd[-2:] == ["--abbrev-ref", "HEAD"]:
            return _fake_subprocess_result(stdout="main\n")
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout=" M archivo.txt\n")
        if "rev-list" in cmd:
            return _fake_subprocess_result(stdout="0\n")
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    result = orchestration.run_coding_agent_local_real("T-1", "prompt", str(tmp_path))

    assert result["self_verified"] is True
    assert result["consulted_risk_graph"] is True


def test_retry_coding_agent_local_real_preserves_consulted_risk_graph_across_turns(monkeypatch, tmp_path):
    """Gap real: antes, cada reintento perdia consulted_risk_graph aunque
    coding_agent.py SI lo acepta como seed (resume_state) -- si el primer
    intento ya consulto el grafo de riesgo, el segundo turno lo reseteaba a
    False."""
    conversation_file = tmp_path / "conv.json"
    conversation_file.write_text(json.dumps({
        "messages": [], "has_investigated": True, "has_run_verification": True,
        "initial_plan": "plan", "consulted_risk_graph": True,
    }), encoding="utf-8")

    captured_payload = {}
    heads = iter(["checkpoint\n", "checkpoint\n"])  # sin commits nuevos

    class FakeResult:
        returncode = 0
        stdout = json.dumps({
            "status": "blocked", "summary": "no pude", "_meta": {"backend": "ollama"},
            "self_verified": True, "consulted_risk_graph": True,
        })

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            payload_path = cmd[2]
            captured_payload.update(json.loads(Path(payload_path).read_text(encoding="utf-8")))
            return FakeResult()
        if cmd[-1:] == ["HEAD"] and "rev-parse" in cmd:
            return _fake_subprocess_result(stdout=next(heads))
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout="")
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    result = orchestration.retry_coding_agent_local_real(
        "T-1", "feedback", str(tmp_path), conversation_file=str(conversation_file),
    )

    assert captured_payload["resume_state"]["consulted_risk_graph"] is True
    assert result["self_verified"] is True
    assert result["consulted_risk_graph"] is True


def test_retry_coding_agent_local_real_detects_commits_made_by_the_model_itself(monkeypatch, tmp_path):
    """Mismo bug que run_coding_agent_local_real, para el camino de
    reintento -- aca no hay base_branch fijo (se reusa la rama del primer
    intento), asi que se compara HEAD antes/despues del turno (checkpoint)."""
    heads = iter(["hash-antes\n", "hash-despues\n"])  # checkpoint, luego el head_now final

    def fake_git(cmd, **kwargs):
        if cmd[-1:] == ["HEAD"] and "rev-parse" in cmd:
            return _fake_subprocess_result(stdout=next(heads, "hash-despues\n"))
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout="")  # limpio -- el modelo ya commiteo el mismo
        return _fake_subprocess_result()

    class FakeResult:
        returncode = 0
        stdout = json.dumps({"status": "done", "summary": "listo", "_meta": {"backend": "ollama"}})

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            return FakeResult()
        return fake_git(cmd, **kwargs)

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    result = orchestration.retry_coding_agent_local_real("T-1", "feedback", str(tmp_path))

    assert result["applied"] is True


def _fake_branch_list_and_merge_base(monkeypatch, branch_list_stdout: str, merged: set):
    """Comparte el mismo mock entre los tests de _find_open_branch_for_ticket:
    responde a "git branch -a --list ..." con branch_list_stdout, y a
    "git merge-base --is-ancestor {branch} {base}" con returncode=0 (ya
    mergeada) si branch esta en `merged`, o 1 (todavia abierta) si no."""
    def fake_run(cmd, **kwargs):
        if "branch" in cmd and "--list" in cmd:
            return _fake_subprocess_result(stdout=branch_list_stdout)
        if "merge-base" in cmd:
            branch = cmd[-2]
            return _fake_subprocess_result(returncode=0 if branch in merged else 1)
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)


def _fake_remote_url(monkeypatch, url: str):
    def fake_run(cmd, **kwargs):
        if "remote" in cmd and "get-url" in cmd:
            return _fake_subprocess_result(stdout=url)
        return _fake_subprocess_result()
    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)


def test_check_pr_rejected_for_branch_false_when_no_remote(monkeypatch):
    _fake_remote_url(monkeypatch, url="")
    assert _check_pr_rejected_for_branch("/repo", "copilot/T-1-100") is False


def test_check_pr_rejected_for_branch_azure_devops_false_without_pat(monkeypatch):
    """Confirmado real: el repo objetivo real de esta sesion es Azure
    DevOps -- sin AZURE_DEVOPS_PAT seteada, no hay forma de consultar el
    estado real de la PR (graceful-degradation, nunca lanza)."""
    monkeypatch.delenv("AZURE_DEVOPS_PAT", raising=False)
    _fake_remote_url(monkeypatch, url="https://dev.azure.com/org/proj/_git/repo\n")

    assert _check_pr_rejected_for_branch("/repo", "copilot/T-1-100") is False


def test_check_pr_rejected_for_branch_azure_devops_true_when_only_abandoned(monkeypatch):
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "fake-pat")
    _fake_remote_url(monkeypatch, url="https://dev.azure.com/org/proj/_git/repo\n")

    class FakeResp:
        def raise_for_status(self):
            pass
        def json(self):
            return {"value": [{"status": "abandoned"}]}

    monkeypatch.setattr(orchestration.httpx, "get", lambda *a, **k: FakeResp())

    assert _check_pr_rejected_for_branch("/repo", "copilot/T-1-100") is True


def test_check_pr_rejected_for_branch_azure_devops_false_when_still_active(monkeypatch):
    """Aunque exista una PR vieja "abandoned", si TAMBIEN hay una activa
    para la misma rama, no se considera rechazada."""
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "fake-pat")
    _fake_remote_url(monkeypatch, url="https://dev.azure.com/org/proj/_git/repo\n")

    class FakeResp:
        def raise_for_status(self):
            pass
        def json(self):
            return {"value": [{"status": "abandoned"}, {"status": "active"}]}

    monkeypatch.setattr(orchestration.httpx, "get", lambda *a, **k: FakeResp())

    assert _check_pr_rejected_for_branch("/repo", "copilot/T-1-100") is False


def test_check_pr_rejected_for_branch_azure_devops_false_on_http_error(monkeypatch):
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "fake-pat")
    _fake_remote_url(monkeypatch, url="https://dev.azure.com/org/proj/_git/repo\n")

    def fake_get(*a, **k):
        raise orchestration.httpx.HTTPError("boom")

    monkeypatch.setattr(orchestration.httpx, "get", fake_get)

    assert _check_pr_rejected_for_branch("/repo", "copilot/T-1-100") is False


def test_check_pr_rejected_for_branch_github_true_when_closed(monkeypatch):
    monkeypatch.setattr(orchestration.shutil, "which", lambda name: "/usr/bin/gh")

    def fake_run(cmd, **kwargs):
        if "remote" in cmd and "get-url" in cmd:
            return _fake_subprocess_result(stdout="https://github.com/org/repo.git\n")
        if cmd[:3] == ["gh", "pr", "view"]:
            return _fake_subprocess_result(stdout=json.dumps({"state": "CLOSED"}))
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    assert _check_pr_rejected_for_branch("/repo", "copilot/T-1-100") is True


def test_check_pr_rejected_for_branch_github_false_when_open(monkeypatch):
    monkeypatch.setattr(orchestration.shutil, "which", lambda name: "/usr/bin/gh")

    def fake_run(cmd, **kwargs):
        if "remote" in cmd and "get-url" in cmd:
            return _fake_subprocess_result(stdout="https://github.com/org/repo.git\n")
        if cmd[:3] == ["gh", "pr", "view"]:
            return _fake_subprocess_result(stdout=json.dumps({"state": "OPEN"}))
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)

    assert _check_pr_rejected_for_branch("/repo", "copilot/T-1-100") is False


def _fake_remote_url_run(url: str):
    def fake_run(cmd, **kwargs):
        if "remote" in cmd and "get-url" in cmd:
            return _fake_subprocess_result(stdout=url)
        return _fake_subprocess_result()
    return fake_run


def test_fetch_unresolved_pr_comments_empty_without_active_pr(monkeypatch):
    monkeypatch.setattr(orchestration.subprocess, "run", _fake_remote_url_run("https://dev.azure.com/org/proj/_git/repo\n"))
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "fake-pat")

    class FakeResp:
        def raise_for_status(self):
            pass
        def json(self):
            return {"value": []}  # sin ninguna PR para esta rama

    monkeypatch.setattr(orchestration.httpx, "get", lambda *a, **k: FakeResp())

    assert _fetch_unresolved_pr_comments("/repo", "copilot/T-1-100") == []


def test_fetch_unresolved_pr_comments_empty_without_pat(monkeypatch):
    monkeypatch.delenv("AZURE_DEVOPS_PAT", raising=False)
    monkeypatch.setattr(orchestration.subprocess, "run", _fake_remote_url_run("https://dev.azure.com/org/proj/_git/repo\n"))

    assert _fetch_unresolved_pr_comments("/repo", "copilot/T-1-100") == []


def test_fetch_unresolved_pr_comments_returns_real_unresolved_threads(monkeypatch):
    """Caso real (pedido explicito del usuario): un comentario de revision
    humano real dejado en la PR abierta tiene que llegar como feedback."""
    monkeypatch.setattr(orchestration.subprocess, "run", _fake_remote_url_run("https://dev.azure.com/org/proj/_git/repo\n"))
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "fake-pat")

    calls = {"n": 0}

    class FakeResp:
        def __init__(self, payload):
            self._payload = payload
        def raise_for_status(self):
            pass
        def json(self):
            return self._payload

    def fake_get(url, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResp({"value": [{"status": "active", "pullRequestId": 238}]})
        return FakeResp({
            "value": [
                {
                    "id": 5, "status": "active", "isDeleted": False,
                    "comments": [{"commentType": "text", "content": "Falta manejar el caso de token expirado."}],
                },
                {
                    "id": 6, "status": "fixed", "isDeleted": False,
                    "comments": [{"commentType": "text", "content": "ya resuelto, no deberia aparecer"}],
                },
                {
                    "id": 7, "status": "active", "isDeleted": False,
                    "comments": [{"commentType": "system", "content": "se subio una nueva iteracion"}],
                },
            ]
        })

    monkeypatch.setattr(orchestration.httpx, "get", fake_get)

    result = _fetch_unresolved_pr_comments("/repo", "copilot/T-1-100")

    assert result == [{"thread_id": 5, "text": "Falta manejar el caso de token expirado."}]


def test_resolve_pr_threads_patches_each_thread(monkeypatch):
    monkeypatch.setattr(orchestration.subprocess, "run", _fake_remote_url_run("https://dev.azure.com/org/proj/_git/repo\n"))
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "fake-pat")

    class FakeGetResp:
        def raise_for_status(self):
            pass
        def json(self):
            return {"value": [{"status": "active", "pullRequestId": 238}]}

    monkeypatch.setattr(orchestration.httpx, "get", lambda *a, **k: FakeGetResp())

    patched = []

    class FakePatchResp:
        def raise_for_status(self):
            pass

    def fake_patch(url, **kwargs):
        patched.append((url, kwargs.get("json")))
        return FakePatchResp()

    monkeypatch.setattr(orchestration.httpx, "patch", fake_patch)

    _resolve_pr_threads("/repo", "copilot/T-1-100", [5, 9])

    assert len(patched) == 2
    assert all(body == {"status": "fixed"} for _url, body in patched)
    assert patched[0][0].endswith("/threads/5")
    assert patched[1][0].endswith("/threads/9")


def test_resolve_pr_threads_noop_for_empty_list(monkeypatch):
    calls = []
    monkeypatch.setattr(orchestration.subprocess, "run", lambda *a, **k: calls.append(1) or _fake_subprocess_result())

    _resolve_pr_threads("/repo", "copilot/T-1-100", [])

    assert calls == []  # no llego ni a consultar el remote


def test_resolve_pr_threads_does_not_raise_on_http_error(monkeypatch):
    monkeypatch.setattr(orchestration.subprocess, "run", _fake_remote_url_run("https://dev.azure.com/org/proj/_git/repo\n"))
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "fake-pat")

    class FakeGetResp:
        def raise_for_status(self):
            pass
        def json(self):
            return {"value": [{"status": "active", "pullRequestId": 238}]}

    monkeypatch.setattr(orchestration.httpx, "get", lambda *a, **k: FakeGetResp())

    def fake_patch(url, **kwargs):
        raise orchestration.httpx.HTTPError("boom")

    monkeypatch.setattr(orchestration.httpx, "patch", fake_patch)

    _resolve_pr_threads("/repo", "copilot/T-1-100", [5])  # no debe lanzar


def test_find_open_branch_for_ticket_returns_none_when_no_branches_match(monkeypatch):
    _fake_branch_list_and_merge_base(monkeypatch, branch_list_stdout="", merged=set())

    assert _find_open_branch_for_ticket("/repo", "KAN-2", "main") is None


def test_find_open_branch_for_ticket_returns_branch_when_open_and_unmerged(monkeypatch):
    _fake_branch_list_and_merge_base(
        monkeypatch, branch_list_stdout="  copilot/KAN-2-100\n", merged=set()
    )

    assert _find_open_branch_for_ticket("/repo", "KAN-2", "main") == "copilot/KAN-2-100"


def test_find_open_branch_for_ticket_ignores_already_merged_branch(monkeypatch):
    _fake_branch_list_and_merge_base(
        monkeypatch, branch_list_stdout="  copilot/KAN-2-100\n", merged={"copilot/KAN-2-100"}
    )

    assert _find_open_branch_for_ticket("/repo", "KAN-2", "main") is None


def test_find_open_branch_for_ticket_picks_most_recent_of_multiple_open(monkeypatch):
    _fake_branch_list_and_merge_base(
        monkeypatch,
        branch_list_stdout="  copilot/KAN-2-100\n  remotes/origin/copilot/KAN-2-200\n",
        merged=set(),
    )

    assert _find_open_branch_for_ticket("/repo", "KAN-2", "main") == "copilot/KAN-2-200"


def test_find_merged_branch_for_ticket_returns_none_when_no_branches_match(monkeypatch):
    _fake_branch_list_and_merge_base(monkeypatch, branch_list_stdout="", merged=set())

    assert orchestration._find_merged_branch_for_ticket("/repo", "KAN-2", "main") is None


def test_find_merged_branch_for_ticket_ignores_unmerged_branch(monkeypatch):
    _fake_branch_list_and_merge_base(
        monkeypatch, branch_list_stdout="  copilot/KAN-2-100\n", merged=set()
    )

    assert orchestration._find_merged_branch_for_ticket("/repo", "KAN-2", "main") is None


def test_find_merged_branch_for_ticket_returns_branch_when_merged(monkeypatch):
    """Caso inverso a _find_open_branch_for_ticket: una rama copilot/{ticket}-*
    que YA es ancestro de base_branch significa que un humano ya la mergeo --
    confirma que el pipeline puede detectar esto para cerrar el ticket."""
    _fake_branch_list_and_merge_base(
        monkeypatch, branch_list_stdout="  copilot/KAN-2-100\n", merged={"copilot/KAN-2-100"}
    )

    assert orchestration._find_merged_branch_for_ticket("/repo", "KAN-2", "main") == "copilot/KAN-2-100"


def _fake_branch_list_merge_base_and_rev_parse(monkeypatch, branch_list_stdout: str, merged: set, base_branch: str = "main"):
    def fake_run(cmd, **kwargs):
        if "branch" in cmd and "--list" in cmd:
            return _fake_subprocess_result(stdout=branch_list_stdout)
        if "merge-base" in cmd:
            branch = cmd[-2]
            return _fake_subprocess_result(returncode=0 if branch in merged else 1)
        if "rev-parse" in cmd:
            return _fake_subprocess_result(stdout=f"{base_branch}\n")
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)


def test_check_already_completed_true_when_jira_status_already_done(monkeypatch):
    """Gap real (usuario): un webhook viejo/duplicado que re-dispara el
    pipeline sobre un ticket que un humano ya cerro no deberia volver a
    correr firewall/coding agent/tests -- esto se detecta ANTES de tocar
    git, con el status que ya trae fetch_ticket_live()."""
    monkeypatch.setattr(orchestration.subprocess, "run", lambda *a, **k: pytest.fail("no deberia tocar git"))
    comments, transitions = [], []
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append(ticket_key))
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: transitions.append(ticket_key))

    result = orchestration._check_already_completed(
        "T-1", {"ticket_id": "T-1", "status": orchestration.JIRA_DONE_STATUS}, "/repo",
    )

    assert result is True
    assert comments == [] and transitions == []  # ya esta Done, no hace falta re-comentar/re-transicionar


def test_check_already_completed_true_and_transitions_to_done_when_branch_merged(monkeypatch):
    _fake_branch_list_merge_base_and_rev_parse(
        monkeypatch, branch_list_stdout="  copilot/T-1-100\n", merged={"copilot/T-1-100"}, base_branch="main",
    )
    transitions = []
    comments = []
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: comments.append((ticket_key, text)))
    monkeypatch.setattr(orchestration, "transition_jira", lambda status, ticket_key=None: transitions.append((ticket_key, status)))

    result = orchestration._check_already_completed("T-1", {"ticket_id": "T-1", "status": "Code Review"}, "/repo")

    assert result is True
    assert transitions == [("T-1", orchestration.JIRA_DONE_STATUS)]
    assert any("copilot/T-1-100" in text for _key, text in comments)


def test_check_already_completed_false_when_nothing_indicates_completion(monkeypatch):
    _fake_branch_list_merge_base_and_rev_parse(
        monkeypatch, branch_list_stdout="  copilot/T-1-100\n", merged=set(), base_branch="main",
    )
    monkeypatch.setattr(orchestration, "transition_jira", lambda *a, **k: pytest.fail("no deberia transicionar"))

    result = orchestration._check_already_completed("T-1", {"ticket_id": "T-1", "status": "In Progress"}, "/repo")

    assert result is False


def test_run_coding_agent_local_real_reuses_existing_open_branch(monkeypatch, tmp_path):
    """Confirmado real (epica KAN-4): sin esto, cada re-corrida de un ticket
    (ej. un humano revierte el status en Jira pidiendo un redo) creaba una
    rama nueva en paralelo, huerfana, en vez de retomar la existente."""
    git_calls = []

    class FakeResult:
        returncode = 0
        stdout = json.dumps({"status": "done", "summary": "listo", "_meta": {"backend": "ollama"}})

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            return FakeResult()
        git_calls.append(cmd)
        if cmd[-2:] == ["--abbrev-ref", "HEAD"]:
            return _fake_subprocess_result(stdout="main\n")
        if "branch" in cmd and "--list" in cmd:
            return _fake_subprocess_result(stdout="  copilot/T-1-100\n")
        if "merge-base" in cmd:
            return _fake_subprocess_result(returncode=1)  # todavia abierta
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout="")
        if "rev-list" in cmd:
            return _fake_subprocess_result(stdout="1\n")
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})

    result = orchestration.run_coding_agent_local_real("T-1", "prompt", str(tmp_path))

    assert result["branch"] == "copilot/T-1-100"
    assert result["resumed_branch"] is True
    # Se hizo "checkout copilot/T-1-100", NUNCA "checkout -b" con una rama nueva
    assert not any("-b" in cmd for cmd in git_calls if cmd[:3] == ["git", "-C", str(tmp_path)] and "checkout" in cmd)
    assert any(cmd[-1] == "copilot/T-1-100" and "checkout" in cmd and "-b" not in cmd for cmd in git_calls)


def test_run_coding_agent_local_real_abandons_branch_when_tests_already_fail(monkeypatch, tmp_path):
    """Bug real confirmado en vivo (epica KAN-4, la misma PR con dos arboles
    src/ desconectados): antes se reusaba una rama rota sin chequear nada,
    y el coding agent aplicaba un cambio nuevo sobre codigo ya roto. Ahora,
    si los tests reales YA fallan en la rama tal cual esta (antes de
    aplicar nada nuevo), se abandona y arranca de cero desde base_branch."""
    git_calls = []

    class FakeResult:
        returncode = 0
        stdout = json.dumps({"status": "done", "summary": "listo", "_meta": {"backend": "ollama"}})

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            return FakeResult()
        git_calls.append(cmd)
        if cmd[-2:] == ["--abbrev-ref", "HEAD"]:
            return _fake_subprocess_result(stdout="main\n")
        if "branch" in cmd and "--list" in cmd:
            return _fake_subprocess_result(stdout="  copilot/T-1-100\n")
        if "merge-base" in cmd:
            return _fake_subprocess_result(returncode=1)  # todavia abierta
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout=" M archivo.txt\n")
        if "rev-list" in cmd:
            return _fake_subprocess_result(stdout="1\n")
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": False, "output": "fallo real"})

    result = orchestration.run_coding_agent_local_real("T-1", "prompt", str(tmp_path))

    assert result["resumed_branch"] is False
    assert result["branch"] != "copilot/T-1-100"
    assert any("-b" in cmd for cmd in git_calls if "checkout" in cmd)


def test_run_coding_agent_local_real_abandons_branch_when_structure_duplicated(monkeypatch, tmp_path):
    """Bug real confirmado en vivo (PR #240/#241, epica KAN-4): los tests
    reales PASABAN en la rama vieja aunque tuviera frontend/my-app/
    duplicado -- el chequeo de tests solo no alcanzaba. Confirma que la
    señal estructural abandona la rama igual, aunque los tests pasen."""
    git_calls = []

    class FakeResult:
        returncode = 0
        stdout = json.dumps({"status": "done", "summary": "listo", "_meta": {"backend": "ollama"}})

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            return FakeResult()
        git_calls.append(cmd)
        if cmd[-2:] == ["--abbrev-ref", "HEAD"]:
            return _fake_subprocess_result(stdout="main\n")
        if "branch" in cmd and "--list" in cmd:
            return _fake_subprocess_result(stdout="  copilot/T-1-100\n")
        if "merge-base" in cmd:
            return _fake_subprocess_result(returncode=1)
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout=" M archivo.txt\n")
        if "rev-list" in cmd:
            return _fake_subprocess_result(stdout="1\n")
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "todo bien (pero la estructura esta duplicada)"})

    frontend = tmp_path / "frontend"
    (frontend / "src").mkdir(parents=True)
    (frontend / "package.json").write_text("{}")
    nested = frontend / "my-app"
    (nested / "src").mkdir(parents=True)
    (nested / "package.json").write_text("{}")

    result = orchestration.run_coding_agent_local_real("T-1", "prompt", str(tmp_path))

    assert result["resumed_branch"] is False
    assert result["branch"] != "copilot/T-1-100"
    assert any("-b" in cmd for cmd in git_calls if "checkout" in cmd)


def test_run_coding_agent_local_real_abandons_branch_when_pr_rejected(monkeypatch, tmp_path):
    """PR rechazada es motivo suficiente por si sola -- no hace falta ni
    correr los tests de salud para decidir abandonar."""
    class FakeResult:
        returncode = 0
        stdout = json.dumps({"status": "done", "summary": "listo", "_meta": {"backend": "ollama"}})

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            return FakeResult()
        if cmd[-2:] == ["--abbrev-ref", "HEAD"]:
            return _fake_subprocess_result(stdout="main\n")
        if "branch" in cmd and "--list" in cmd:
            return _fake_subprocess_result(stdout="  copilot/T-1-100\n")
        if "merge-base" in cmd:
            return _fake_subprocess_result(returncode=1)
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout=" M archivo.txt\n")
        if "rev-list" in cmd:
            return _fake_subprocess_result(stdout="1\n")
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)
    monkeypatch.setattr(orchestration, "_check_pr_rejected_for_branch", lambda *a, **k: True)

    tests_called = {"n": 0}

    def fail_if_tests_called(*a, **k):
        tests_called["n"] += 1
        return {"passed": True, "output": "no deberia llegar aca"}

    monkeypatch.setattr(orchestration, "run_tests", fail_if_tests_called)

    result = orchestration.run_coding_agent_local_real("T-1", "prompt", str(tmp_path))

    assert result["resumed_branch"] is False
    assert result["resumed_pr_rejected"] is False
    assert tests_called["n"] == 0


def test_run_coding_agent_local_real_does_not_delete_reused_branch_when_nothing_applied(monkeypatch, tmp_path):
    """Una rama RETOMADA no se borra solo porque ESTE intento puntual no
    sumo commits nuevos -- podria tener trabajo real de una corrida
    anterior que se perderia con "branch -D"."""
    git_calls = []

    class FakeResult:
        returncode = 0
        stdout = json.dumps({"status": "blocked", "summary": "", "_meta": {"backend": "ollama"}})

    def fake_run(cmd, **kwargs):
        if cmd and cmd[0] == "python3":
            return FakeResult()
        git_calls.append(cmd)
        if cmd[-2:] == ["--abbrev-ref", "HEAD"]:
            return _fake_subprocess_result(stdout="main\n")
        if "branch" in cmd and "--list" in cmd:
            return _fake_subprocess_result(stdout="  copilot/T-1-100\n")
        if "merge-base" in cmd:
            return _fake_subprocess_result(returncode=1)  # todavia abierta
        if cmd[-2:] == ["status", "--porcelain"]:
            return _fake_subprocess_result(stdout="")  # nada nuevo
        if "rev-list" in cmd:
            return _fake_subprocess_result(stdout="0\n")  # sin commits nuevos
        return _fake_subprocess_result()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_run)
    monkeypatch.setattr(orchestration, "run_tests", lambda *a, **k: {"passed": True, "output": "ok"})

    result = orchestration.run_coding_agent_local_real("T-1", "prompt", str(tmp_path))

    assert result["applied"] is False
    assert not any(cmd[-2:-1] == ["branch"] and "-D" in cmd for cmd in git_calls)


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


def test_push_and_open_pr_degrades_gracefully_when_gh_is_not_installed(monkeypatch):
    """Bug real confirmado esta sesion (remote no-GitHub, no Azure DevOps,
    ej. Bitbucket): "gh" ni siquiera esta instalado, y subprocess.run lanza
    FileNotFoundError (no un returncode!=0) -- el docstring de esta funcion
    ya prometia "best-effort, nunca bloquea la corrida" para este caso, pero
    el codigo solo atajaba "gh corrio y fallo", no "gh no existe". El push
    real (que no depende de gh) ya funciono para cuando esto pasa."""
    def fake_subprocess_run(cmd, capture_output=None, text=None, cwd=None):
        if cmd[:2] == ["gh", "pr"]:
            raise FileNotFoundError("[Errno 2] No such file or directory: 'gh'")
        class R:
            returncode = 0
            stdout = "https://bitbucket.org/org/repo.git" if "get-url" in cmd else ""
            stderr = ""
        return R()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_subprocess_run)

    result = orchestration.push_and_open_pr.fn("/repo", "copilot/T-1-123", "main", "T-1", "Fix login", "body")

    assert result["pushed"] is True
    assert result["pr_url"] is None
    assert "gh" in result["reason"]


def test_push_and_open_pr_opens_real_pr_via_azure_devops_rest_api(monkeypatch):
    """Bug real confirmado esta sesion (repo objetivo real de esta sesion
    es Azure DevOps): antes SIEMPRE degradaba a "pushea y abri el PR a
    mano" para este remote, aunque AZURE_DEVOPS_PAT ya esta disponible en
    .env -- ahora abre la PR de verdad via la REST API real."""
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "fake-pat")

    def fake_subprocess_run(cmd, capture_output=None, text=None, cwd=None):
        class R:
            returncode = 0
            stdout = "https://dev.azure.com/org/proj/_git/repo" if "get-url" in cmd else ""
            stderr = ""
        return R()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_subprocess_run)

    captured = {}

    class FakeResp:
        def raise_for_status(self):
            pass
        def json(self):
            return {"pullRequestId": 42}

    def fake_post(url, **kwargs):
        captured["url"] = url
        captured["json"] = kwargs.get("json")
        return FakeResp()

    monkeypatch.setattr(orchestration.httpx, "post", fake_post)

    result = orchestration.push_and_open_pr.fn("/repo", "copilot/T-1-123", "main", "T-1", "Fix login", "body")

    assert result["pushed"] is True
    assert result["pr_url"] == "https://dev.azure.com/org/proj/_git/repo/pullrequest/42"
    assert captured["json"]["sourceRefName"] == "refs/heads/copilot/T-1-123"
    assert captured["json"]["targetRefName"] == "refs/heads/main"


def test_push_and_open_pr_azure_devops_without_pat_degrades_gracefully(monkeypatch):
    monkeypatch.delenv("AZURE_DEVOPS_PAT", raising=False)

    def fake_subprocess_run(cmd, capture_output=None, text=None, cwd=None):
        class R:
            returncode = 0
            stdout = "https://dev.azure.com/org/proj/_git/repo" if "get-url" in cmd else ""
            stderr = ""
        return R()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_subprocess_run)

    result = orchestration.push_and_open_pr.fn("/repo", "copilot/T-1-123", "main", "T-1", "Fix login", "body")

    assert result["pushed"] is True
    assert result["pr_url"] is None
    assert "AZURE_DEVOPS_PAT" in result["reason"]


def test_push_and_open_pr_azure_devops_rest_api_failure_degrades_gracefully(monkeypatch):
    monkeypatch.setenv("AZURE_DEVOPS_PAT", "fake-pat")

    def fake_subprocess_run(cmd, capture_output=None, text=None, cwd=None):
        class R:
            returncode = 0
            stdout = "https://dev.azure.com/org/proj/_git/repo" if "get-url" in cmd else ""
            stderr = ""
        return R()

    monkeypatch.setattr(orchestration.subprocess, "run", fake_subprocess_run)

    def fake_post(url, **kwargs):
        raise orchestration.httpx.HTTPError("boom")

    monkeypatch.setattr(orchestration.httpx, "post", fake_post)

    result = orchestration.push_and_open_pr.fn("/repo", "copilot/T-1-123", "main", "T-1", "Fix login", "body")

    assert result["pushed"] is True
    assert result["pr_url"] is None
    assert "Azure DevOps" in result["reason"]


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


def _fake_flow_run_context(flow_run_id: str):
    ctx = MagicMock()
    ctx.flow_run.id = flow_run_id
    return ctx


def test_comment_idempotency_marker_empty_without_active_flow_context():
    """Sin contexto real de Prefect (ej. tests que llaman .fn() directo, o
    codigo que corre fuera de un flow), el marcador es "" -- comment_jira
    cae al comportamiento de siempre, nunca bloquea por esto."""
    assert orchestration._comment_idempotency_marker("hola") == ""


def test_comment_idempotency_marker_stable_for_same_text_and_flow_run():
    with patch("prefect.context.get_run_context", return_value=_fake_flow_run_context("run-1")):
        marker_a = orchestration._comment_idempotency_marker("mismo texto")
        marker_b = orchestration._comment_idempotency_marker("mismo texto")
    assert marker_a == marker_b
    assert marker_a.startswith("[ref: run-1:")


def test_comment_idempotency_marker_differs_for_different_text():
    with patch("prefect.context.get_run_context", return_value=_fake_flow_run_context("run-1")):
        marker_a = orchestration._comment_idempotency_marker("texto A")
        marker_b = orchestration._comment_idempotency_marker("texto B")
    assert marker_a != marker_b


def test_comment_jira_skips_posting_when_marker_already_exists(monkeypatch):
    """Bug real identificado en una auditoria de arquitectura previa:
    comment_jira (un @task(retries=2)) no tenia ninguna proteccion contra
    postear el mismo comentario dos veces si un reintento real de Prefect
    corria despues de que el POST original ya hubiera llegado a Jira.
    """
    posted = []
    monkeypatch.setattr(orchestration.jira_client, "post_audit_comment", lambda key, text: posted.append((key, text)))
    monkeypatch.setattr(orchestration.jira_client, "comment_already_posted", lambda key, marker: True)
    monkeypatch.setattr(orchestration, "get_run_logger", lambda: MagicMock())

    with patch("prefect.context.get_run_context", return_value=_fake_flow_run_context("run-1")):
        orchestration.comment_jira.fn("hola de nuevo", ticket_key="T-1")

    assert posted == []


def test_comment_jira_posts_with_marker_when_not_already_posted(monkeypatch):
    posted = []
    checked = []
    monkeypatch.setattr(orchestration.jira_client, "post_audit_comment", lambda key, text: posted.append((key, text)))
    monkeypatch.setattr(
        orchestration.jira_client, "comment_already_posted",
        lambda key, marker: checked.append((key, marker)) or False,
    )

    with patch("prefect.context.get_run_context", return_value=_fake_flow_run_context("run-1")):
        orchestration.comment_jira.fn("hola", ticket_key="T-1")

    assert len(posted) == 1
    key, text = posted[0]
    assert key == "T-1"
    assert text.startswith("hola\n\n[ref: run-1:")
    assert checked[0][1] == text.split("\n\n", 1)[1]


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


def test_abandon_ticket_branch_no_op_when_nothing_found(monkeypatch, capsys):
    _fake_branch_list_and_merge_base(monkeypatch, branch_list_stdout="", merged=set())
    commented = []
    monkeypatch.setattr(orchestration, "comment_jira", lambda *a, **k: commented.append(a))

    orchestration.abandon_ticket_branch("KAN-99", "/repo")

    assert "no se encontro ninguna rama" in capsys.readouterr().out
    assert commented == []


def test_abandon_ticket_branch_deletes_local_and_remote_and_comments(monkeypatch, capsys):
    _fake_branch_list_and_merge_base(
        monkeypatch, branch_list_stdout="  copilot/KAN-2-100\n", merged=set()
    )
    commented = []
    monkeypatch.setattr(orchestration, "comment_jira", lambda text, ticket_key=None: commented.append((ticket_key, text)))

    orchestration.abandon_ticket_branch("KAN-2", "/repo")

    out = capsys.readouterr().out
    assert "copilot/KAN-2-100" in out
    assert len(commented) == 1
    ticket_key, text = commented[0]
    assert ticket_key == "KAN-2"
    assert "abandonada" in text
    assert "copilot/KAN-2-100" in text


def test_abandon_ticket_branch_survives_jira_comment_failure(monkeypatch, capsys):
    """Best-effort: si comentar en Jira falla, las ramas ya se borraron --
    no debe propagar la excepcion (mismo criterio de graceful-degradation
    que el resto de las acciones de Jira en este modulo)."""
    _fake_branch_list_and_merge_base(
        monkeypatch, branch_list_stdout="  copilot/KAN-2-100\n", merged=set()
    )

    def fake_comment_jira(*a, **k):
        raise RuntimeError("Jira no disponible")

    monkeypatch.setattr(orchestration, "comment_jira", fake_comment_jira)

    orchestration.abandon_ticket_branch("KAN-2", "/repo")  # no debe lanzar

    assert "no se pudo comentar en Jira" in capsys.readouterr().out
