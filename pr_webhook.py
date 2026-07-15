"""Webhooks entrantes reales -- distintas fuentes disparan la re-corrida
automatica del pipeline (o una alerta) sin que alguien tenga que volver a
lanzar orchestration.py a mano:

  - POST /webhooks/azure-devops: comentario de revision real en la PR
    abierta (Azure DevOps).
  - POST /webhooks/github: idem, para un repo objetivo real en GitHub
    (evento pull_request_review_comment).
  - POST /webhooks/jira: comentario nuevo o cambio de status en el ticket
    de Jira en si (no en la PR) -- ej. un humano revierte el status a "En
    curso" pidiendo un redo.
  - POST /webhooks/sonarqube: SOLO alerta (no dispara ninguna corrida) --
    un quality gate en ERROR para un componente ya entregado no apunta a
    un ticket especifico como los otros tres, asi que un humano decide si
    corresponde abrir un ticket nuevo.

Confirmado esta sesion: orchestration.py ya sabe leer los comentarios
reales sin resolver de la PR abierta y pasarlos como feedback al coding
agent (ver _fetch_unresolved_pr_comments/_resolve_pr_threads), pero antes
de esto solo se ejecutaba si alguien volvia a lanzar orchestration.py a
mano -- este servicio cierra ese loop en tiempo real.

orchestration.py es un script batch, no un servicio -- los tres webhooks
que disparan una corrida lo hacen via el mismo mecanismo
Docker-outside-of-Docker ya usado toda esta sesion (docker run contra el
daemon del HOST, /var/run/docker.sock montado), no lo importan en el
mismo proceso.

Auth: mismo patron que firewall_proxy.py para azure-devops/jira/sonarqube
-- si PR_WEBHOOK_API_KEY esta seteada, exigen el header X-Webhook-Key (401
si no matchea). /webhooks/github usa el mecanismo real y distinto de
GitHub (HMAC-SHA256 sobre el body crudo, header X-Hub-Signature-256, ver
GITHUB_WEBHOOK_SECRET) -- no tiene sentido pedirle a GitHub que mande un
header custom que no es su convencion.

NOTA IMPORTANTE (marcado en el plan): el payload exacto de Azure DevOps
para ms.vss-code.git-pullrequest-comment-event no se pudo verificar contra
la documentacion oficial en esta sesion (busqueda web sin resultado por
limite de sesion) -- antes de conectar el service hook real, usar el boton
"Test" de Azure DevOps (Project Settings > Service Hooks) para capturar un
payload real y confirmar los nombres de campo exactos. Lo mismo aplica al
payload real de Jira (Settings > System > WebHooks) y de SonarQube
(Administration > Webhooks) -- confirmar contra un evento real de cada uno
antes de conectarlos en produccion.
"""
import hashlib
import hmac
import json
import os
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request

from log_utils import get_logger
from secrets_provider import get_secret

app = FastAPI(title="Webhooks entrantes (Azure DevOps / GitHub / Jira / SonarQube -> pipeline)")

logger = get_logger(__name__)

LOG_DIR = Path("/app/logs") if Path("/app").exists() else Path("./logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_LOG = LOG_DIR / "pr_webhook_audit.jsonl"

PR_WEBHOOK_API_KEY = get_secret("PR_WEBHOOK_API_KEY")
if not PR_WEBHOOK_API_KEY:
    logger.warning(
        "PR_WEBHOOK_API_KEY no esta seteada -- /webhooks/azure-devops acepta pedidos sin autenticar. "
        "Setea PR_WEBHOOK_API_KEY en .env para requerir el header X-Webhook-Key."
    )

# Identidad real dueña del AZURE_DEVOPS_PAT -- evita que el propio pipeline
# (al comentar en la PR o marcar threads resueltos) se dispare a si mismo.
AZURE_DEVOPS_BOT_IDENTITY = os.environ.get("AZURE_DEVOPS_BOT_IDENTITY", "").strip().lower()
# Mismas variables ya reales del resto del proyecto -- reusadas aca solo
# para el chequeo anti-loop (nunca se llama a la API de Jira/GitHub desde
# este servicio).
JIRA_BOT_EMAIL = os.environ.get("JIRA_EMAIL", "").strip().lower()
GITHUB_BOT_IDENTITY = os.environ.get("GITHUB_BOT_IDENTITY", "").strip().lower()
GITHUB_WEBHOOK_SECRET = get_secret("GITHUB_WEBHOOK_SECRET")
if not GITHUB_WEBHOOK_SECRET:
    logger.warning(
        "GITHUB_WEBHOOK_SECRET no esta seteada -- /webhooks/github acepta pedidos sin validar firma. "
        "Setea GITHUB_WEBHOOK_SECRET en .env (mismo secreto que se configura en GitHub) para requerir "
        "una firma HMAC-SHA256 valida en X-Hub-Signature-256."
    )
# Mismo webhook Slack-compatible que ya usa post_alert_webhook en
# orchestration.py (Falco/juez FLAGGED/tests fallidos) -- reusado aca para
# no duplicar credenciales.
ALERT_WEBHOOK_URL = os.environ.get("FALCO_ALERT_WEBHOOK_URL", "")

COMMENT_EVENT_TYPE = "ms.vss-code.git-pullrequest-comment-event"
GITHUB_COMMENT_EVENT = "pull_request_review_comment"
JIRA_RELEVANT_EVENTS = {"comment_created", "jira:issue_updated"}
_BRANCH_TICKET_PATTERN = re.compile(r"copilot/([A-Za-z]+-\d+)-\d+")

WEBHOOK_TARGET_REPO_DIR = os.environ.get("WEBHOOK_TARGET_REPO_DIR", "")
WEBHOOK_ENV_FILE = os.environ.get("WEBHOOK_ENV_FILE", "")
WEBHOOK_REPO_ROOT = os.environ.get("WEBHOOK_REPO_ROOT", "")
WEBHOOK_DEBOUNCE_SECONDS = float(os.environ.get("WEBHOOK_DEBOUNCE_SECONDS", "120"))

_debounce_lock = threading.Lock()
_last_triggered_at: dict = {}


def require_api_key(x_webhook_key: Optional[str] = Header(default=None)):
    if PR_WEBHOOK_API_KEY and not hmac.compare_digest(x_webhook_key or "", PR_WEBHOOK_API_KEY):
        raise HTTPException(status_code=401, detail="missing_or_invalid_x_webhook_key")


def _audit(event: str, ticket_id: Optional[str], detail: str) -> None:
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "event": event,
        "ticket_id": ticket_id,
        "detail": detail,
    }
    with AUDIT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def extract_ticket_id_from_branch(source_ref_name: str) -> Optional[str]:
    """refs/heads/copilot/KAN-5-1784059321 -> "KAN-5". None si la rama no
    sigue la convencion de nombres que usa orchestration.py para las ramas
    que crea (copilot/{ticket_id}-{timestamp})."""
    match = _BRANCH_TICKET_PATTERN.search(source_ref_name or "")
    return match.group(1) if match else None


def _is_debounced(ticket_id: str) -> bool:
    now = time.monotonic()
    with _debounce_lock:
        last = _last_triggered_at.get(ticket_id)
        if last is not None and (now - last) < WEBHOOK_DEBOUNCE_SECONDS:
            return True
        _last_triggered_at[ticket_id] = now
        return False


def build_docker_run_command(ticket_id: str) -> list:
    """Mismo comando Docker-outside-of-Docker usado manualmente toda esta
    sesion para invocar orchestration.py -- ver el resto de comentarios de
    esta sesion sobre HOST_TARGET_REPO_DIR y el mount de
    /var/run/docker.sock."""
    return [
        "docker", "run", "-i", "--rm", "--network", "poc-ai-agents_poc-net",
        "-v", f"{WEBHOOK_REPO_ROOT}:/repo",
        "-v", f"{WEBHOOK_ENV_FILE}:/repo/.env:ro",
        "-v", f"{WEBHOOK_TARGET_REPO_DIR}:/target-repo",
        "-v", "/var/run/docker.sock:/var/run/docker.sock",
        "-e", f"HOST_TARGET_REPO_DIR={WEBHOOK_TARGET_REPO_DIR}",
        "-w", "/target-repo",
        "poc-ai-agents-testrunner",
        "python3", "/repo/orchestration.py", ticket_id,
    ]


def trigger_pipeline_for_ticket(ticket_id: str) -> None:
    cmd = build_docker_run_command(ticket_id)
    subprocess.Popen(cmd)  # no bloqueante -- el webhook ya respondio 200


@app.post("/webhooks/azure-devops", dependencies=[Depends(require_api_key)])
def azure_devops_webhook(payload: dict, request: Request):
    event_type = payload.get("eventType")
    if event_type != COMMENT_EVENT_TYPE:
        _audit("ignored_event_type", None, str(event_type))
        return {"status": "ignored", "reason": "not_a_pr_comment_event"}

    resource = payload.get("resource", {}) or {}
    comment = resource.get("comment", {}) or {}
    pull_request = resource.get("pullRequest", {}) or {}

    author = (comment.get("author") or {})
    author_identity = (author.get("uniqueName") or author.get("displayName") or "").strip().lower()
    if AZURE_DEVOPS_BOT_IDENTITY and author_identity == AZURE_DEVOPS_BOT_IDENTITY:
        _audit("ignored_own_comment", None, author_identity)
        return {"status": "ignored", "reason": "comment_authored_by_pipeline_identity"}

    source_ref_name = pull_request.get("sourceRefName", "")
    ticket_id = extract_ticket_id_from_branch(source_ref_name)
    if not ticket_id:
        _audit("ignored_branch_not_recognized", None, source_ref_name)
        return {"status": "ignored", "reason": "branch_not_a_pipeline_branch"}

    if _is_debounced(ticket_id):
        _audit("debounced", ticket_id, f"otra corrida disparada hace menos de {WEBHOOK_DEBOUNCE_SECONDS}s")
        return {"status": "debounced", "ticket_id": ticket_id}

    trigger_pipeline_for_ticket(ticket_id)
    _audit("triggered", ticket_id, source_ref_name)
    return {"status": "triggered", "ticket_id": ticket_id}


def verify_github_signature(raw_body: bytes, signature_header: str) -> bool:
    """HMAC-SHA256 real sobre el body crudo, mismo mecanismo documentado
    de GitHub (no el header X-Webhook-Key generico que usan los otros
    tres webhooks -- GitHub no lo entiende, tiene su propia convencion)."""
    if not GITHUB_WEBHOOK_SECRET:
        return True  # sin secreto configurado, igual criterio "abierto con warning" que el resto
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(GITHUB_WEBHOOK_SECRET.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature_header)


@app.post("/webhooks/github")
async def github_webhook(request: Request):
    raw_body = await request.body()
    if not verify_github_signature(raw_body, request.headers.get("X-Hub-Signature-256", "")):
        raise HTTPException(status_code=401, detail="invalid_or_missing_github_signature")

    try:
        payload = json.loads(raw_body or b"{}")
    except json.JSONDecodeError:
        _audit("ignored_github_invalid_json", None, "body no era JSON valido")
        return {"status": "ignored", "reason": "invalid_json_body"}

    if request.headers.get("X-GitHub-Event", "") != GITHUB_COMMENT_EVENT:
        _audit("ignored_github_event_type", None, request.headers.get("X-GitHub-Event", ""))
        return {"status": "ignored", "reason": "not_a_pr_review_comment_event"}
    if payload.get("action") not in ("created", "edited"):
        _audit("ignored_github_action", None, str(payload.get("action")))
        return {"status": "ignored", "reason": "not_a_new_comment_action"}

    comment_author = ((payload.get("comment") or {}).get("user") or {}).get("login", "").strip().lower()
    if GITHUB_BOT_IDENTITY and comment_author == GITHUB_BOT_IDENTITY:
        _audit("ignored_own_github_comment", None, comment_author)
        return {"status": "ignored", "reason": "comment_authored_by_pipeline_identity"}

    branch = ((payload.get("pull_request") or {}).get("head") or {}).get("ref", "")
    ticket_id = extract_ticket_id_from_branch(branch)
    if not ticket_id:
        _audit("ignored_branch_not_recognized", None, branch)
        return {"status": "ignored", "reason": "branch_not_a_pipeline_branch"}

    if _is_debounced(ticket_id):
        _audit("debounced", ticket_id, "github")
        return {"status": "debounced", "ticket_id": ticket_id}

    trigger_pipeline_for_ticket(ticket_id)
    _audit("triggered_from_github", ticket_id, branch)
    return {"status": "triggered", "ticket_id": ticket_id}


@app.post("/webhooks/jira", dependencies=[Depends(require_api_key)])
def jira_webhook(payload: dict):
    """Comentario nuevo o cambio de status en el TICKET (no en la PR) --
    ej. un humano revierte manualmente el status a JIRA_IN_PROGRESS_STATUS
    ("En curso") pidiendo un redo, o deja feedback directo en el ticket en
    vez de en la PR."""
    webhook_event = payload.get("webhookEvent")
    if webhook_event not in JIRA_RELEVANT_EVENTS:
        _audit("ignored_jira_event_type", None, str(webhook_event))
        return {"status": "ignored", "reason": "not_a_relevant_jira_event"}

    author_email = ((payload.get("comment") or {}).get("author") or {}).get("emailAddress", "").strip().lower()
    if JIRA_BOT_EMAIL and author_email == JIRA_BOT_EMAIL:
        _audit("ignored_own_jira_comment", None, author_email)
        return {"status": "ignored", "reason": "comment_authored_by_pipeline_identity"}

    ticket_id = (payload.get("issue") or {}).get("key")
    if not ticket_id:
        _audit("ignored_jira_no_ticket", None, "sin issue.key en el payload")
        return {"status": "ignored", "reason": "no_ticket_key_in_payload"}

    if _is_debounced(ticket_id):
        _audit("debounced", ticket_id, "jira")
        return {"status": "debounced", "ticket_id": ticket_id}

    trigger_pipeline_for_ticket(ticket_id)
    _audit("triggered_from_jira", ticket_id, str(webhook_event))
    return {"status": "triggered", "ticket_id": ticket_id}


def post_alert(text: str) -> None:
    """Mismo formato Slack-compatible que post_alert_webhook en
    orchestration.py -- reimplementado aca (no se importa orchestration.py
    completo, que trae Prefect y el resto de dependencias pesadas, solo
    para esto)."""
    if not ALERT_WEBHOOK_URL:
        return
    try:
        httpx.post(ALERT_WEBHOOK_URL, json={"text": text}, timeout=10.0)
    except httpx.HTTPError as exc:
        logger.warning(f"no se pudo postear la alerta de SonarQube al webhook: {exc}")


@app.post("/webhooks/sonarqube", dependencies=[Depends(require_api_key)])
def sonarqube_webhook(payload: dict):
    """SOLO alerta -- decision explicita del usuario: un quality gate en
    ERROR para un componente ya entregado no apunta a un ticket especifico
    como los otros tres webhooks, asi que no dispara ninguna corrida de
    coding agent sola. Un humano decide si corresponde abrir un ticket."""
    quality_gate_status = (payload.get("qualityGate") or {}).get("status")
    project_key = (payload.get("project") or {}).get("key", "desconocido")

    if quality_gate_status != "ERROR":
        _audit("ignored_sonarqube_quality_gate_ok", None, project_key)
        return {"status": "ignored", "reason": "quality_gate_not_in_error"}

    post_alert(
        f"🛑 SonarQube: quality gate en ERROR para '{project_key}' -- revisar hallazgos nuevos "
        "antes de la proxima corrida real sobre ese componente."
    )
    _audit("alerted_sonarqube_quality_gate_error", None, project_key)
    return {"status": "alerted", "project_key": project_key}


@app.get("/health")
def health():
    return {"status": "ok"}
