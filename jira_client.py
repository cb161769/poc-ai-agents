"""Real Jira Cloud client used by run_poc_loop.sh (stage 1).

Reads JIRA_URL / JIRA_EMAIL / JIRA_API_TOKEN / JIRA_TICKET_KEY from the
environment (see .env.example), calls the real Jira REST API v3, and prints
a single JSON object on stdout so the bash orchestrator can consume it with
`jq`.

repository_origen is resolved from the ticket's labels: the first label that
matches a known node name in the dependency graph is used. This lets a real
Jira ticket drive the graph impact query without inventing a custom field.

Known components are NOT hardcoded to the three sample-repo/ modules — set
JIRA_KNOWN_COMPONENTS in .env as a comma-separated list matching your real
service names (however many languages/frameworks they're in) so this
generalizes to real backends without editing this file.
"""
import base64
import json
import os
import sys

import httpx
from dotenv import load_dotenv

from cache_utils import cached_call

load_dotenv()

_DEFAULT_KNOWN_REPOS = "AuthService,Frontend,DataWorker"
KNOWN_REPOS = {
    name.strip()
    for name in os.environ.get("JIRA_KNOWN_COMPONENTS", _DEFAULT_KNOWN_REPOS).split(",")
    if name.strip()
}

# How Rovo identifies itself as a comment author in your instance — adjust
# if your org's Rovo integration shows up under a different display name.
ROVO_AUTHOR_MATCH = os.environ.get("ROVO_AUTHOR_NAME_MATCH", "rovo").lower()


def _adf_to_text(node) -> str:
    """Flatten Atlassian Document Format into plain text."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    text_parts = []
    if isinstance(node, dict):
        if node.get("type") == "text":
            text_parts.append(node.get("text", ""))
        for child in node.get("content", []) or []:
            text_parts.append(_adf_to_text(child))
        if node.get("type") in ("paragraph", "heading"):
            text_parts.append("\n")
    elif isinstance(node, list):
        for child in node:
            text_parts.append(_adf_to_text(child))
    return "".join(text_parts)


def _adf_has_code_block(node) -> bool:
    """Structured signal that the reporter actually pasted a log/stack trace,
    instead of guessing from free text with regex: Jira's editor wraps any
    "insert code" block as an explicit `codeBlock` node in the ADF tree, so
    this is a deterministic check, not a heuristic on keywords.
    """
    if node is None:
        return False
    if isinstance(node, dict):
        if node.get("type") == "codeBlock":
            return True
        return any(_adf_has_code_block(child) for child in node.get("content", []) or [])
    if isinstance(node, list):
        return any(_adf_has_code_block(child) for child in node)
    return False


def _fetch_rovo_attachment_context(jira_url: str, headers: dict, ticket_key: str, attachments: list) -> dict:
    """If the ticket has attachments (e.g. a bug-report video) and Rovo has
    already described them in a comment, surface that description as text —
    instead of us downloading/processing the video ourselves. Still treated
    as untrusted external content: it flows through the same egress firewall
    as everything else before it reaches any agent.
    """
    if not attachments:
        return {"has_attachments": False, "attachment_names": [], "attachment_context": ""}

    attachment_names = [a.get("filename", "unknown") for a in attachments]

    resp = httpx.get(
        f"{jira_url}/rest/api/3/issue/{ticket_key}/comment",
        headers=headers,
        params={"orderBy": "created"},
        timeout=15.0,
    )
    resp.raise_for_status()
    comments = resp.json().get("comments", [])

    rovo_texts = [
        _adf_to_text(c.get("body")).strip()
        for c in comments
        if ROVO_AUTHOR_MATCH in (c.get("author", {}).get("displayName", "") or "").lower()
    ]

    if not rovo_texts:
        return {
            "has_attachments": True,
            "attachment_names": attachment_names,
            "attachment_context": (
                f"[{len(attachments)} adjunto(s): {', '.join(attachment_names)} — "
                "Rovo todavia no genero una descripcion en los comentarios. "
                "Requiere revision humana antes de continuar.]"
            ),
        }

    return {
        "has_attachments": True,
        "attachment_names": attachment_names,
        "attachment_context": "\n".join(rovo_texts),
    }


def fetch_ticket_live() -> dict:
    jira_url = os.environ["JIRA_URL"].rstrip("/")
    email = os.environ["JIRA_EMAIL"]
    token = os.environ["JIRA_API_TOKEN"]
    ticket_key = os.environ["JIRA_TICKET_KEY"]

    auth = base64.b64encode(f"{email}:{token}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}", "Accept": "application/json"}

    resp = httpx.get(
        f"{jira_url}/rest/api/3/issue/{ticket_key}",
        headers=headers,
        params={"fields": "summary,description,labels,status,attachment"},
        timeout=15.0,
    )
    resp.raise_for_status()
    issue = resp.json()

    fields = issue.get("fields", {})
    labels = fields.get("labels", []) or []
    repository_origen = next((lbl for lbl in labels if lbl in KNOWN_REPOS), None)
    attachment_info = _fetch_rovo_attachment_context(jira_url, headers, ticket_key, fields.get("attachment", []) or [])

    return {
        "ticket_id": issue.get("key"),
        "summary": fields.get("summary", ""),
        "description": _adf_to_text(fields.get("description")).strip(),
        "labels": labels,
        "repository_origen": repository_origen,
        "status": (fields.get("status") or {}).get("name"),
        "has_log_evidence": _adf_has_code_block(fields.get("description")),
        **attachment_info,
    }


def get_ticket() -> dict:
    ticket_key = os.environ.get("JIRA_TICKET_KEY", "UNSET")
    return cached_call(
        namespace="jira_issue",
        params={"ticket_key": ticket_key, "jira_url": os.environ.get("JIRA_URL", "")},
        fetch_fn=fetch_ticket_live,
    )


def post_audit_comment(ticket_key: str, text: str) -> dict:
    """Posts a plain-text audit comment on the real Jira ticket, so the
    firewall's decision and whatever Copilot did are visible directly in
    Jira's own comment history — not just in the local logs/*.jsonl files.
    """
    jira_url = os.environ["JIRA_URL"].rstrip("/")
    email = os.environ["JIRA_EMAIL"]
    token = os.environ["JIRA_API_TOKEN"]

    auth = base64.b64encode(f"{email}:{token}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}", "Accept": "application/json", "Content-Type": "application/json"}

    body_adf = {
        "type": "doc",
        "version": 1,
        "content": [{"type": "paragraph", "content": [{"type": "text", "text": text}]}],
    }

    resp = httpx.post(
        f"{jira_url}/rest/api/3/issue/{ticket_key}/comment",
        headers=headers,
        json={"body": body_adf},
        timeout=15.0,
    )
    resp.raise_for_status()
    return resp.json()


def transition_ticket(ticket_key: str, target_status_name: str) -> dict:
    """Moves the real Jira ticket to the workflow status matching
    target_status_name (case-insensitive), e.g. "In Progress", so the
    ticket's status reflects that the firewall approved it and an agent is
    working on it — without a human having to drag it across the board.

    Jira transitions are workflow-specific: the available target statuses
    (and their transition ids) depend on the ticket's current state and the
    project's configured workflow, so we look them up live instead of
    hardcoding an id.
    """
    jira_url = os.environ["JIRA_URL"].rstrip("/")
    email = os.environ["JIRA_EMAIL"]
    token = os.environ["JIRA_API_TOKEN"]

    auth = base64.b64encode(f"{email}:{token}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}", "Accept": "application/json", "Content-Type": "application/json"}

    resp = httpx.get(
        f"{jira_url}/rest/api/3/issue/{ticket_key}/transitions",
        headers=headers,
        timeout=15.0,
    )
    resp.raise_for_status()
    transitions = resp.json().get("transitions", [])

    match = next(
        (t for t in transitions if t.get("name", "").strip().lower() == target_status_name.strip().lower()),
        None,
    )
    if match is None:
        available = [t.get("name") for t in transitions]
        raise ValueError(
            f"no hay una transicion llamada '{target_status_name}' disponible desde el estado actual. "
            f"Disponibles: {available}"
        )

    resp = httpx.post(
        f"{jira_url}/rest/api/3/issue/{ticket_key}/transitions",
        headers=headers,
        json={"transition": {"id": match["id"]}},
        timeout=15.0,
    )
    resp.raise_for_status()
    return {"ticket_id": ticket_key, "transitioned_to": target_status_name}


def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "comment":
        if len(sys.argv) < 3:
            print(json.dumps({"error": "usage: jira_client.py comment \"<texto>\""}), file=sys.stderr)
            sys.exit(1)
        ticket_key = os.environ.get("JIRA_TICKET_KEY", "")
        try:
            result = post_audit_comment(ticket_key, sys.argv[2])
        except KeyError as exc:
            print(json.dumps({"error": f"missing_env_var:{exc.args[0]}"}), file=sys.stderr)
            sys.exit(1)
        except httpx.HTTPStatusError as exc:
            print(
                json.dumps({"error": "jira_http_error", "status_code": exc.response.status_code, "body": exc.response.text}),
                file=sys.stderr,
            )
            sys.exit(1)
        print(json.dumps({"comment_id": result.get("id"), "ticket_id": ticket_key}, ensure_ascii=False))
        return

    if len(sys.argv) >= 2 and sys.argv[1] == "transition":
        if len(sys.argv) < 3:
            print(json.dumps({"error": "usage: jira_client.py transition \"<nombre del estado>\""}), file=sys.stderr)
            sys.exit(1)
        ticket_key = os.environ.get("JIRA_TICKET_KEY", "")
        try:
            result = transition_ticket(ticket_key, sys.argv[2])
        except KeyError as exc:
            print(json.dumps({"error": f"missing_env_var:{exc.args[0]}"}), file=sys.stderr)
            sys.exit(1)
        except (httpx.HTTPStatusError, ValueError) as exc:
            print(json.dumps({"error": "jira_transition_error", "detail": str(exc)}), file=sys.stderr)
            sys.exit(1)
        print(json.dumps(result, ensure_ascii=False))
        return

    try:
        ticket = get_ticket()
    except KeyError as exc:
        print(json.dumps({"error": f"missing_env_var:{exc.args[0]}"}), file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPStatusError as exc:
        print(
            json.dumps({"error": "jira_http_error", "status_code": exc.response.status_code, "body": exc.response.text}),
            file=sys.stderr,
        )
        sys.exit(1)

    if not ticket.get("repository_origen"):
        print(
            json.dumps(
                {
                    "error": "no_matching_label",
                    "detail": f"El ticket {ticket.get('ticket_id')} no tiene ninguna etiqueta en {sorted(KNOWN_REPOS)}. "
                    "Agrega una etiqueta (label) con el nombre exacto del nodo del grafo afectado.",
                }
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    print(json.dumps(ticket, ensure_ascii=False))


if __name__ == "__main__":
    main()
