"""AI Firewall — local reverse proxy for the autonomous coding agent.

POST /evaluate receives the composed prompt (Jira + graph impact + real
Sonar findings) and runs it through two strict gates, defined in the
versioned rule file firewall/policies.yaml (not hardcoded in this module,
so the ruleset is auditable/reviewable on its own):

  1. Ingress (jailbreak): blocks known prompt-injection patterns found in the
     Jira ticket description or the composed prompt itself. Runs first and
     short-circuits — egress never processes rejected input.
  2. Egress (data leak): regex-redacts explicit passwords, secret_key=..., and
     Azure-key-shaped base64 blobs from the prompt before it can reach the
     agent.

Every call (approved or rejected) is appended as one JSON line to
logs/firewall_audit.jsonl, without ever writing the raw secret to disk.

Auth: if FIREWALL_API_KEY is set in the environment, /evaluate requires a
matching X-Firewall-Key header (401 otherwise) -- so this can't be hit by
anything that didn't go through the orchestrator. If the variable is unset,
the firewall stays open (with a startup warning) so the local demo keeps
working for anyone who hasn't configured it yet.

Rate limiting: a simple in-memory sliding window per caller IP (this is a
single-instance local service, no shared state/Redis needed) caps how many
times /evaluate can be called in RATE_LIMIT_WINDOW_SECONDS, so the endpoint
isn't wide open to abuse now that it's reachable on the host network.
"""
import collections
import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Optional

import yaml
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import REGISTRY, CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic import BaseModel

from log_utils import get_logger
from secrets_provider import get_secret

app = FastAPI(title="AI Firewall PoC")

logger = get_logger(__name__)

# Mismos eventos que ya se auditan en firewall_audit.jsonl, expuestos en
# formato Prometheus para que /metrics se pueda scrapear o consultar con un
# curl directo, sin depender de leer el JSONL a mano para saber si algo esta
# fallando en agregado.
def _get_or_create_counter(name: str, documentation: str) -> Counter:
    # Reusa el collector ya registrado si el modulo se recarga en el mismo
    # proceso (como hacen algunos tests) -- prometheus_client no permite
    # registrar dos veces la misma serie en el registry global por default.
    existing = REGISTRY._names_to_collectors.get(name)
    if existing is not None:
        return existing
    return Counter(name, documentation)


FIREWALL_APPROVED_TOTAL = _get_or_create_counter("firewall_approved_total", "Solicitudes aprobadas por el AI Firewall")
FIREWALL_REJECTED_TOTAL = _get_or_create_counter("firewall_rejected_total", "Solicitudes rechazadas por el AI Firewall")
FIREWALL_REDACTIONS_TOTAL = _get_or_create_counter("firewall_redactions_total", "Redacciones de secretos aplicadas en total")

LOG_DIR = Path("/app/logs") if Path("/app").exists() else Path("./logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_LOG = LOG_DIR / "firewall_audit.jsonl"

POLICIES_PATH = Path(__file__).resolve().parent / "firewall" / "policies.yaml"

FIREWALL_API_KEY = get_secret("FIREWALL_API_KEY")
if not FIREWALL_API_KEY:
    logger.warning(
        "FIREWALL_API_KEY no esta seteada -- /evaluate acepta pedidos sin autenticar. "
        "Setea FIREWALL_API_KEY en .env para requerir el header X-Firewall-Key."
    )


def require_api_key(x_firewall_key: Optional[str] = Header(default=None)):
    if FIREWALL_API_KEY and x_firewall_key != FIREWALL_API_KEY:
        raise HTTPException(status_code=401, detail="missing_or_invalid_x_firewall_key")


def load_policies(path: Path = POLICIES_PATH) -> dict:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


_POLICIES = load_policies()
JAILBREAK_RULES = _POLICIES["jailbreak_rules"]
REDACTION_RULES = _POLICIES["redaction_rules"]

_JAILBREAK_COMPILED = [(rule, re.compile(rule["pattern"], re.IGNORECASE)) for rule in JAILBREAK_RULES]
_REDACTION_COMPILED = [(rule, re.compile(rule["pattern"], re.IGNORECASE)) for rule in REDACTION_RULES]

REDACTED_TOKEN = "[REDACTED_CORPORATE_SECRET]"

RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS", "30"))
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))

_rate_limit_lock = threading.Lock()
_rate_limit_hits: dict = collections.defaultdict(collections.deque)


def check_rate_limit(client_key: str):
    now = time.time()
    with _rate_limit_lock:
        hits = _rate_limit_hits[client_key]
        while hits and now - hits[0] > RATE_LIMIT_WINDOW_SECONDS:
            hits.popleft()
        if len(hits) >= RATE_LIMIT_MAX_REQUESTS:
            raise HTTPException(status_code=429, detail="rate_limit_exceeded")
        hits.append(now)


class EvaluateRequest(BaseModel):
    prompt: str
    jira_context: dict
    sonar_errors: list


class EvaluateResponse(BaseModel):
    status: str
    reason: Optional[str] = None
    sanitized_prompt: Optional[str] = None
    redactions_applied: int = 0


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _check_jailbreak(prompt: str, jira_context: dict) -> Optional[str]:
    description = jira_context.get("description", "") or ""
    combined = _normalize(prompt) + " " + _normalize(description)
    for rule, compiled in _JAILBREAK_COMPILED:
        match = compiled.search(combined)
        if match:
            return f"jailbreak_pattern_matched:{rule['id']}:{match.group(0)!r}"
    return None


def _redact(text: str) -> tuple[str, int]:
    count = 0
    sanitized = text
    for _rule, compiled in _REDACTION_COMPILED:
        sanitized, n = compiled.subn(REDACTED_TOKEN, sanitized)
        count += n
    return sanitized, count


def _audit(ticket_id: str, status: str, reason: Optional[str], redactions_applied: int):
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ticket_id": ticket_id,
        "status": status,
        "reason": reason,
        "redactions_applied": redactions_applied,
    }
    with AUDIT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    if status == "APPROVED":
        FIREWALL_APPROVED_TOTAL.inc()
    else:
        FIREWALL_REJECTED_TOTAL.inc()
    if redactions_applied:
        FIREWALL_REDACTIONS_TOTAL.inc(redactions_applied)


@app.post("/evaluate", response_model=EvaluateResponse, dependencies=[Depends(require_api_key)])
def evaluate(req: EvaluateRequest, request: Request):
    check_rate_limit(request.client.host if request.client else "unknown")

    ticket_id = req.jira_context.get("ticket_id", "UNKNOWN")

    jailbreak_reason = _check_jailbreak(req.prompt, req.jira_context)
    if jailbreak_reason:
        _audit(ticket_id, "REJECTED", jailbreak_reason, 0)
        return JSONResponse(
            status_code=403,
            content={
                "status": "REJECTED",
                "reason": jailbreak_reason,
                "sanitized_prompt": None,
                "redactions_applied": 0,
            },
        )

    sanitized_prompt, redactions_applied = _redact(req.prompt)

    _audit(ticket_id, "APPROVED", None, redactions_applied)
    return {
        "status": "APPROVED",
        "reason": None,
        "sanitized_prompt": sanitized_prompt,
        "redactions_applied": redactions_applied,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
