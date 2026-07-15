"""Unit tests for pr_webhook.py -- el webhook entrante real que re-dispara
orchestration.py cuando Azure DevOps notifica un comentario nuevo en la PR
abierta. Usa FastAPI TestClient directo contra el app real; subprocess.Popen
siempre mockeado (nunca lanza un docker run real en los tests).
"""
import importlib

import pytest
from fastapi.testclient import TestClient

import pr_webhook


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(pr_webhook, "LOG_DIR", tmp_path)
    monkeypatch.setattr(pr_webhook, "AUDIT_LOG", tmp_path / "pr_webhook_audit.jsonl")
    monkeypatch.setattr(pr_webhook, "_last_triggered_at", {})
    return TestClient(pr_webhook.app)


def _comment_event(branch: str, author_unique_name: str = "revisor@empresa.com") -> dict:
    return {
        "eventType": "ms.vss-code.git-pullrequest-comment-event",
        "resource": {
            "comment": {"content": "Falta manejar el token expirado.", "author": {"uniqueName": author_unique_name}},
            "pullRequest": {"sourceRefName": branch, "pullRequestId": 238},
        },
    }


def test_extract_ticket_id_from_branch_matches_pipeline_convention():
    assert pr_webhook.extract_ticket_id_from_branch("refs/heads/copilot/KAN-5-1784059321") == "KAN-5"


def test_extract_ticket_id_from_branch_none_for_unrelated_branch():
    assert pr_webhook.extract_ticket_id_from_branch("refs/heads/main") is None
    assert pr_webhook.extract_ticket_id_from_branch("") is None


def test_ignores_event_types_other_than_pr_comment(client, monkeypatch):
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post("/webhooks/azure-devops", json={"eventType": "git.push", "resource": {}})

    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"
    assert triggered == []


def test_ignores_comment_authored_by_pipeline_identity(client, monkeypatch):
    monkeypatch.setattr(pr_webhook, "AZURE_DEVOPS_BOT_IDENTITY", "pipeline-bot@empresa.com")
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post(
        "/webhooks/azure-devops",
        json=_comment_event("refs/heads/copilot/KAN-5-123", author_unique_name="pipeline-bot@empresa.com"),
    )

    assert resp.status_code == 200
    assert resp.json()["reason"] == "comment_authored_by_pipeline_identity"
    assert triggered == []


def test_ignores_branch_not_recognized_as_pipeline_branch(client, monkeypatch):
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post("/webhooks/azure-devops", json=_comment_event("refs/heads/main"))

    assert resp.status_code == 200
    assert resp.json()["reason"] == "branch_not_a_pipeline_branch"
    assert triggered == []


def test_triggers_pipeline_for_real_comment_on_pipeline_branch(client, monkeypatch):
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post("/webhooks/azure-devops", json=_comment_event("refs/heads/copilot/KAN-5-123"))

    assert resp.status_code == 200
    assert resp.json() == {"status": "triggered", "ticket_id": "KAN-5"}
    assert triggered == ["KAN-5"]


def test_debounces_second_event_for_same_ticket_within_window(client, monkeypatch):
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))
    monkeypatch.setattr(pr_webhook, "WEBHOOK_DEBOUNCE_SECONDS", 120.0)

    first = client.post("/webhooks/azure-devops", json=_comment_event("refs/heads/copilot/KAN-5-123"))
    second = client.post("/webhooks/azure-devops", json=_comment_event("refs/heads/copilot/KAN-5-123"))

    assert first.json()["status"] == "triggered"
    assert second.json()["status"] == "debounced"
    assert triggered == ["KAN-5"]  # solo se disparo una vez


def test_build_docker_run_command_uses_real_dood_pattern(monkeypatch):
    """Confirmado real (mismo patron usado manualmente toda la sesion):
    Docker-outside-of-Docker -- el daemon que recibe este comando es el del
    HOST, no este contenedor, asi que los -v tienen que usar paths reales
    del host (WEBHOOK_*), no paths internos de pr_webhook."""
    monkeypatch.setattr(pr_webhook, "WEBHOOK_REPO_ROOT", "/c/real/poc-ai-agents")
    monkeypatch.setattr(pr_webhook, "WEBHOOK_ENV_FILE", "/c/real/scratch/env")
    monkeypatch.setattr(pr_webhook, "WEBHOOK_TARGET_REPO_DIR", "/c/real/scratch/ai-agents-code")

    cmd = pr_webhook.build_docker_run_command("KAN-5")

    assert "/var/run/docker.sock:/var/run/docker.sock" in cmd
    assert "/c/real/poc-ai-agents:/repo" in cmd
    assert "/c/real/scratch/ai-agents-code:/target-repo" in cmd
    assert cmd[-1] == "KAN-5"
    assert cmd[-3:-1] == ["python3", "/repo/orchestration.py"]


def test_webhook_requires_header_when_api_key_configured(tmp_path, monkeypatch):
    monkeypatch.setenv("PR_WEBHOOK_API_KEY", "test-secret-key")
    reloaded = importlib.reload(pr_webhook)
    monkeypatch.setattr(reloaded, "LOG_DIR", tmp_path)
    monkeypatch.setattr(reloaded, "AUDIT_LOG", tmp_path / "pr_webhook_audit.jsonl")
    monkeypatch.setattr(reloaded, "trigger_pipeline_for_ticket", lambda ticket_id: None)
    reloaded_client = TestClient(reloaded.app)

    try:
        no_header = reloaded_client.post("/webhooks/azure-devops", json=_comment_event("refs/heads/copilot/KAN-5-123"))
        assert no_header.status_code == 401

        wrong_header = reloaded_client.post(
            "/webhooks/azure-devops", json=_comment_event("refs/heads/copilot/KAN-5-123"),
            headers={"X-Webhook-Key": "not-the-key"},
        )
        assert wrong_header.status_code == 401

        right_header = reloaded_client.post(
            "/webhooks/azure-devops", json=_comment_event("refs/heads/copilot/KAN-5-123"),
            headers={"X-Webhook-Key": "test-secret-key"},
        )
        assert right_header.status_code == 200
    finally:
        monkeypatch.delenv("PR_WEBHOOK_API_KEY", raising=False)
        importlib.reload(pr_webhook)


def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# ---------- GitHub ----------

def _github_review_comment_event(branch: str, author_login: str = "revisor-real") -> dict:
    return {
        "action": "created",
        "comment": {"body": "Falta manejar el token expirado.", "user": {"login": author_login}},
        "pull_request": {"head": {"ref": branch}},
    }


def test_github_webhook_ignores_wrong_event_type(client, monkeypatch):
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post(
        "/webhooks/github", json=_github_review_comment_event("refs/heads/copilot/KAN-5-123"),
        headers={"X-GitHub-Event": "push"},
    )

    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"
    assert triggered == []


def test_github_webhook_ignores_own_comment(client, monkeypatch):
    monkeypatch.setattr(pr_webhook, "GITHUB_BOT_IDENTITY", "pipeline-bot")
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post(
        "/webhooks/github", json=_github_review_comment_event("refs/heads/copilot/KAN-5-123", author_login="pipeline-bot"),
        headers={"X-GitHub-Event": "pull_request_review_comment"},
    )

    assert resp.status_code == 200
    assert resp.json()["reason"] == "comment_authored_by_pipeline_identity"
    assert triggered == []


def test_github_webhook_triggers_for_real_review_comment(client, monkeypatch):
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post(
        "/webhooks/github", json=_github_review_comment_event("refs/heads/copilot/KAN-5-123"),
        headers={"X-GitHub-Event": "pull_request_review_comment"},
    )

    assert resp.status_code == 200
    assert resp.json() == {"status": "triggered", "ticket_id": "KAN-5"}
    assert triggered == ["KAN-5"]


def test_github_webhook_requires_valid_signature_when_secret_configured(client, monkeypatch):
    monkeypatch.setattr(pr_webhook, "GITHUB_WEBHOOK_SECRET", "test-github-secret")
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: None)

    body = _github_review_comment_event("refs/heads/copilot/KAN-5-123")

    no_sig = client.post("/webhooks/github", json=body, headers={"X-GitHub-Event": "pull_request_review_comment"})
    assert no_sig.status_code == 401

    import hashlib
    import hmac as hmac_module
    import json as json_module
    raw = json_module.dumps(body).encode("utf-8")
    valid_sig = "sha256=" + hmac_module.new(b"test-github-secret", raw, hashlib.sha256).hexdigest()
    valid = client.post(
        "/webhooks/github", content=raw,
        headers={"X-GitHub-Event": "pull_request_review_comment", "X-Hub-Signature-256": valid_sig, "Content-Type": "application/json"},
    )
    assert valid.status_code == 200
    assert valid.json()["status"] == "triggered"


# ---------- Jira ----------

def _jira_comment_event(ticket_id: str, author_email: str = "revisor@empresa.com") -> dict:
    return {
        "webhookEvent": "comment_created",
        "issue": {"key": ticket_id},
        "comment": {"body": "Falta manejar el token expirado.", "author": {"emailAddress": author_email}},
    }


def test_jira_webhook_ignores_unrelated_event(client, monkeypatch):
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post("/webhooks/jira", json={"webhookEvent": "worklog_created", "issue": {"key": "KAN-5"}})

    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"
    assert triggered == []


def test_jira_webhook_ignores_own_comment(client, monkeypatch):
    monkeypatch.setattr(pr_webhook, "JIRA_BOT_EMAIL", "pipeline@empresa.com")
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post("/webhooks/jira", json=_jira_comment_event("KAN-5", author_email="pipeline@empresa.com"))

    assert resp.status_code == 200
    assert resp.json()["reason"] == "comment_authored_by_pipeline_identity"
    assert triggered == []


def test_jira_webhook_triggers_for_real_comment(client, monkeypatch):
    triggered = []
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post("/webhooks/jira", json=_jira_comment_event("KAN-5"))

    assert resp.status_code == 200
    assert resp.json() == {"status": "triggered", "ticket_id": "KAN-5"}
    assert triggered == ["KAN-5"]


# ---------- SonarQube ----------

def test_sonarqube_webhook_ignores_passing_quality_gate(client, monkeypatch):
    alerts = []
    monkeypatch.setattr(pr_webhook, "post_alert", lambda text: alerts.append(text))

    resp = client.post("/webhooks/sonarqube", json={"qualityGate": {"status": "OK"}, "project": {"key": "Frontend"}})

    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"
    assert alerts == []


def test_sonarqube_webhook_alerts_but_never_triggers_pipeline(client, monkeypatch):
    """Decision explicita del usuario: un quality gate en ERROR SOLO
    alerta -- no dispara ninguna corrida de coding agent sola, porque no
    hay un ticket especifico al que apuntar como en los otros webhooks."""
    alerts = []
    triggered = []
    monkeypatch.setattr(pr_webhook, "post_alert", lambda text: alerts.append(text))
    monkeypatch.setattr(pr_webhook, "trigger_pipeline_for_ticket", lambda ticket_id: triggered.append(ticket_id))

    resp = client.post("/webhooks/sonarqube", json={"qualityGate": {"status": "ERROR"}, "project": {"key": "Frontend"}})

    assert resp.status_code == 200
    assert resp.json() == {"status": "alerted", "project_key": "Frontend"}
    assert len(alerts) == 1
    assert "Frontend" in alerts[0]
    assert triggered == []  # nunca dispara el pipeline solo
