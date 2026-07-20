"""Unit tests for scripts/branch_health.py -- el CLI delgado que expone la
logica de salud de rama de orchestration.py a run_poc_loop.sh (bash). Los
casos de la logica subyacente (_find_open_branch_for_ticket,
_check_pr_rejected_for_branch, etc.) ya estan cubiertos a fondo en
tests/test_orchestration.py -- estos tests ejercitan el WRAPPER: que
resolve()/main() llamen a las funciones correctas segun el escenario y que
la salida sea texto parseable por 'eval' en bash.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import branch_health  # noqa: E402


def _fake_subprocess_result(returncode=0, stdout="", stderr=""):
    class R:
        pass
    r = R()
    r.returncode = returncode
    r.stdout = stdout
    r.stderr = stderr
    return r


def test_shell_quote_escapes_single_quotes():
    assert branch_health._shell_quote("no hay 'ancestro'") == "'no hay '\\''ancestro'\\'''"


def test_resolve_creates_new_branch_when_none_open(monkeypatch):
    monkeypatch.setattr(branch_health.ensure_on_trunk_branch, "fn", lambda repo: "main")
    monkeypatch.setattr(branch_health, "_find_open_branch_for_ticket", lambda *a: None)

    def fake_run(cmd, **kwargs):
        return _fake_subprocess_result()

    monkeypatch.setattr(branch_health.subprocess, "run", fake_run)

    result = branch_health.resolve("KAN-2", "/repo")

    assert result["resumed"] is False
    assert result["pr_rejected"] is False
    assert result["abandon_reason"] == ""
    assert result["branch"].startswith("copilot/KAN-2-")
    assert result["base_branch"] == "main"


def test_resolve_resumes_healthy_existing_branch(monkeypatch):
    monkeypatch.setattr(branch_health.ensure_on_trunk_branch, "fn", lambda repo: "main")
    monkeypatch.setattr(branch_health, "_find_open_branch_for_ticket", lambda *a: "copilot/KAN-2-100")
    monkeypatch.setattr(branch_health, "_check_pr_rejected_for_branch", lambda *a: False)
    monkeypatch.setattr(branch_health.run_tests, "fn", lambda repo: {"passed": True, "output": "ok"})
    monkeypatch.setattr(branch_health, "_has_duplicate_project_scaffolding", lambda repo: None)
    monkeypatch.setattr(branch_health.subprocess, "run", lambda cmd, **k: _fake_subprocess_result())

    result = branch_health.resolve("KAN-2", "/repo")

    assert result == {
        "branch": "copilot/KAN-2-100",
        "base_branch": "main",
        "resumed": True,
        "pr_rejected": False,
        "abandon_reason": "",
    }


def test_resolve_abandons_when_pr_rejected(monkeypatch):
    monkeypatch.setattr(branch_health.ensure_on_trunk_branch, "fn", lambda repo: "main")
    monkeypatch.setattr(branch_health, "_find_open_branch_for_ticket", lambda *a: "copilot/KAN-2-100")
    monkeypatch.setattr(branch_health, "_check_pr_rejected_for_branch", lambda *a: True)
    monkeypatch.setattr(branch_health.subprocess, "run", lambda cmd, **k: _fake_subprocess_result())

    result = branch_health.resolve("KAN-2", "/repo")

    assert result["resumed"] is False
    assert result["abandon_reason"] == "la PR previa de esta rama fue rechazada/cerrada sin mergear"
    assert result["branch"] != "copilot/KAN-2-100"
    assert result["branch"].startswith("copilot/KAN-2-")


def test_resolve_abandons_when_tests_already_fail(monkeypatch):
    monkeypatch.setattr(branch_health.ensure_on_trunk_branch, "fn", lambda repo: "main")
    monkeypatch.setattr(branch_health, "_find_open_branch_for_ticket", lambda *a: "copilot/KAN-2-100")
    monkeypatch.setattr(branch_health, "_check_pr_rejected_for_branch", lambda *a: False)
    monkeypatch.setattr(branch_health.run_tests, "fn", lambda repo: {"passed": False, "output": "fail"})
    monkeypatch.setattr(branch_health.subprocess, "run", lambda cmd, **k: _fake_subprocess_result())

    result = branch_health.resolve("KAN-2", "/repo")

    assert "los tests reales YA fallan" in result["abandon_reason"]
    assert result["resumed"] is False


def test_resolve_abandons_when_duplicate_scaffolding(monkeypatch):
    monkeypatch.setattr(branch_health.ensure_on_trunk_branch, "fn", lambda repo: "main")
    monkeypatch.setattr(branch_health, "_find_open_branch_for_ticket", lambda *a: "copilot/KAN-2-100")
    monkeypatch.setattr(branch_health, "_check_pr_rejected_for_branch", lambda *a: False)
    monkeypatch.setattr(branch_health.run_tests, "fn", lambda repo: {"passed": True, "output": "ok"})
    monkeypatch.setattr(branch_health, "_has_duplicate_project_scaffolding", lambda repo: "frontend y frontend/my-app son ambas raices de proyecto")
    monkeypatch.setattr(branch_health.subprocess, "run", lambda cmd, **k: _fake_subprocess_result())

    result = branch_health.resolve("KAN-2", "/repo")

    assert "estructura duplicada" in result["abandon_reason"]
    assert result["resumed"] is False


def test_main_prints_shell_parseable_output(monkeypatch, capsys):
    monkeypatch.setattr(
        branch_health, "resolve",
        lambda ticket_id, repo: {
            "branch": "copilot/KAN-2-100", "base_branch": "main",
            "resumed": True, "pr_rejected": False, "abandon_reason": "",
        },
    )
    monkeypatch.setattr(sys, "argv", ["branch_health.py", "resolve", "KAN-2", "/repo"])

    branch_health.main()

    out = capsys.readouterr().out
    assert "BRANCH='copilot/KAN-2-100'" in out
    assert "BASE_BRANCH='main'" in out
    assert "RESUMED=true" in out
    assert "PR_REJECTED=false" in out
    assert "ABANDON_REASON=''" in out


def test_main_rejects_bad_usage(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["branch_health.py", "wrong-subcommand"])

    with pytest.raises(SystemExit):
        branch_health.main()
