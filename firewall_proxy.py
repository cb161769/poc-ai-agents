"""AI Firewall — local reverse proxy for the autonomous coding agent.

POST /evaluate receives the composed prompt (Jira + graph impact + real
Sonar findings) and runs it through two strict gates:

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
"""
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI(title="AI Firewall PoC")

LOG_DIR = Path("/app/logs") if Path("/app").exists() else Path("./logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_LOG = LOG_DIR / "firewall_audit.jsonl"

FIREWALL_API_KEY = os.environ.get("FIREWALL_API_KEY", "")
if not FIREWALL_API_KEY:
    print(
        "AVISO: FIREWALL_API_KEY no esta seteada -- /evaluate acepta pedidos sin autenticar. "
        "Setea FIREWALL_API_KEY en .env para requerir el header X-Firewall-Key.",
        file=sys.stderr,
    )


def require_api_key(x_firewall_key: Optional[str] = Header(default=None)):
    if FIREWALL_API_KEY and x_firewall_key != FIREWALL_API_KEY:
        raise HTTPException(status_code=401, detail="missing_or_invalid_x_firewall_key")


JAILBREAK_PATTERNS = [
    r"ignore previous instructions",
    r"ignore all previous instructions",
    r"olvida las instrucciones anteriores",
    r"disregard prior instructions",
    r"rm\s+-rf\b",
    r"drop\s+table\b",
]
_JAILBREAK_RE = re.compile("|".join(JAILBREAK_PATTERNS), re.IGNORECASE)

REDACTION_PATTERNS = [
    re.compile(r"password\s*=\s*\S+", re.IGNORECASE),
    re.compile(r"secret_key\s*=\s*\S+", re.IGNORECASE),
    # Azure-Storage-style account keys: long base64 blob after key[:=]
    re.compile(r"(?:account)?key\s*[:=]\s*[A-Za-z0-9+/]{40,88}={0,2}", re.IGNORECASE),
]
REDACTED_TOKEN = "[REDACTED_CORPORATE_SECRET]"


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
    match = _JAILBREAK_RE.search(combined)
    if match:
        return f"jailbreak_pattern_matched:{match.group(0)!r}"
    return None


def _redact(text: str) -> tuple[str, int]:
    count = 0
    sanitized = text
    for pattern in REDACTION_PATTERNS:
        sanitized, n = pattern.subn(REDACTED_TOKEN, sanitized)
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


@app.post("/evaluate", response_model=EvaluateResponse, dependencies=[Depends(require_api_key)])
def evaluate(req: EvaluateRequest):
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
