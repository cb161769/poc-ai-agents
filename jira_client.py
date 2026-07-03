"""Real Jira Cloud client used by run_poc_loop.sh (stage 1).

Reads JIRA_URL / JIRA_EMAIL / JIRA_API_TOKEN / JIRA_TICKET_KEY from the
environment (see .env.example), calls the real Jira REST API v3, and prints
a single JSON object on stdout so the bash orchestrator can consume it with
`jq`.

repository_origen is resolved from the ticket's labels: the first label that
matches a known node name in the dependency graph (AuthService, Frontend,
DataWorker) is used. This lets a real Jira ticket drive the graph impact
query without inventing a custom field.
"""
import base64
import json
import os
import sys

import httpx
from dotenv import load_dotenv

from cache_utils import cached_call

load_dotenv()

KNOWN_REPOS = {"AuthService", "Frontend", "DataWorker"}


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
        params={"fields": "summary,description,labels,status"},
        timeout=15.0,
    )
    resp.raise_for_status()
    issue = resp.json()

    fields = issue.get("fields", {})
    labels = fields.get("labels", []) or []
    repository_origen = next((lbl for lbl in labels if lbl in KNOWN_REPOS), None)

    return {
        "ticket_id": issue.get("key"),
        "summary": fields.get("summary", ""),
        "description": _adf_to_text(fields.get("description")).strip(),
        "labels": labels,
        "repository_origen": repository_origen,
        "status": (fields.get("status") or {}).get("name"),
    }


def get_ticket() -> dict:
    ticket_key = os.environ.get("JIRA_TICKET_KEY", "UNSET")
    return cached_call(
        namespace="jira_issue",
        params={"ticket_key": ticket_key, "jira_url": os.environ.get("JIRA_URL", "")},
        fetch_fn=fetch_ticket_live,
    )


def main():
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
