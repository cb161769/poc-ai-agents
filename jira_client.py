"""Real Jira Cloud client used by run_poc_loop.sh (stage 1).

Reads JIRA_URL / JIRA_EMAIL / JIRA_API_TOKEN / JIRA_TICKET_KEY from the
environment (see .env.example), calls the real Jira REST API v3, and prints
a single JSON object on stdout so the bash orchestrator can consume it with
`jq`.

repository_origen is resolved primarily from the ticket's native Jira
**Components** field (Settings > Components in your Jira project) -- the
field Jira actually has for "which part of the system does this affect".
If no Component matches, falls back to labels (the original heuristic),
for tickets/projects that only use labels. Either way, the match has to
land in a known node name in the dependency graph.

Known components are NOT hardcoded to the three sample-repo/ modules.
run_poc_loop.sh/orchestration.py derive this set from the real Neo4j graph
at the start of each run (whatever node names already exist there) and
export it as JIRA_KNOWN_COMPONENTS before invoking this script -- so it
stays in sync with the graph automatically. JIRA_KNOWN_COMPONENTS in .env
is only the fallback for when Neo4j isn't reachable yet.
"""
import base64
import json
import os
import re
import sys

import httpx
from dotenv import load_dotenv

from cache_utils import cached_call
from retry_utils import retry_call
from secrets_provider import require_secret

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


_FIGMA_URL_RE = re.compile(r"https?://(?:www\.)?figma\.com/(?:file|design)/([A-Za-z0-9]+)/\S*")
_FIGMA_NODE_ID_RE = re.compile(r"node-id=([0-9]+-[0-9]+)")


def _extract_figma_link(description: str) -> dict | None:
    """Pulls a Figma file+node reference out of the ticket description, so
    the automated pipeline (figma_client.py) can pull real specs without a
    human manually pointing Copilot Chat at a frame first. Requires both a
    file key AND a node-id in the URL -- a link to the whole file with no
    specific frame selected isn't actionable for figma_client.py.
    """
    if not description:
        return None
    url_match = _FIGMA_URL_RE.search(description)
    if not url_match:
        return None
    node_match = _FIGMA_NODE_ID_RE.search(url_match.group(0))
    if not node_match:
        return None
    return {
        "file_key": url_match.group(1),
        "node_id": node_match.group(1).replace("-", ":", 1),
        "url": url_match.group(0),
    }


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

    def _fetch():
        r = httpx.get(
            f"{jira_url}/rest/api/3/issue/{ticket_key}/comment",
            headers=headers,
            params={"orderBy": "created"},
            timeout=15.0,
        )
        r.raise_for_status()
        return r

    resp = retry_call(_fetch)
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


def _resolve_repository_origen(fields: dict, known_repos: set | None = None) -> str | None:
    """Prefer the native Components field (what it's actually for); fall
    back to labels for tickets/projects that only use those. Shared by
    fetch_ticket_live() and fetch_epic_with_children() so both resolve
    repository_origen the exact same way.

    known_repos overrides the module-level KNOWN_REPOS (frozen at import
    time from JIRA_KNOWN_COMPONENTS) -- callers that discover the real set
    at runtime (orchestration.py's discover_known_components(), querying
    Neo4j) can pass it here directly instead of mutating os.environ before
    a subprocess re-import, which was the only way to make it take effect
    when this ran exclusively as a CLI subprocess.
    """
    repos = known_repos if known_repos is not None else KNOWN_REPOS
    labels = fields.get("labels", []) or []
    components = [c.get("name") for c in (fields.get("components") or []) if c.get("name")]
    return (
        next((c for c in components if c in repos), None)
        or next((lbl for lbl in labels if lbl in repos), None)
    )


def _auth_headers() -> dict:
    email = require_secret("JIRA_EMAIL")
    token = require_secret("JIRA_API_TOKEN")
    auth = base64.b64encode(f"{email}:{token}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {auth}", "Accept": "application/json"}


def fetch_ticket_live(ticket_key: str | None = None, known_repos: set | None = None) -> dict:
    """ticket_key/known_repos, si se pasan, tienen prioridad sobre
    JIRA_TICKET_KEY/KNOWN_REPOS -- permite que orchestration.py llame esto
    directo (import) pasando el ticket y los componentes ya descubiertos de
    Neo4j como argumentos reales, en vez de mutar os.environ antes de
    invocar esto como subprocess (que sigue funcionando igual via main()).
    """
    jira_url = os.environ["JIRA_URL"].rstrip("/")
    ticket_key = ticket_key or os.environ["JIRA_TICKET_KEY"]
    headers = _auth_headers()

    def _fetch():
        r = httpx.get(
            f"{jira_url}/rest/api/3/issue/{ticket_key}",
            headers=headers,
            params={"fields": "summary,description,labels,status,attachment,components"},
            timeout=15.0,
        )
        r.raise_for_status()
        return r

    resp = retry_call(_fetch)
    issue = resp.json()

    fields = issue.get("fields", {})
    attachment_info = _fetch_rovo_attachment_context(jira_url, headers, ticket_key, fields.get("attachment", []) or [])
    description_text = _adf_to_text(fields.get("description")).strip()

    return {
        "ticket_id": issue.get("key"),
        "summary": fields.get("summary", ""),
        "description": description_text,
        "labels": fields.get("labels", []) or [],
        "components": [c.get("name") for c in (fields.get("components") or []) if c.get("name")],
        "repository_origen": _resolve_repository_origen(fields, known_repos),
        "status": (fields.get("status") or {}).get("name"),
        "has_log_evidence": _adf_has_code_block(fields.get("description")),
        "figma_link": _extract_figma_link(description_text),
        **attachment_info,
    }


def fetch_epic_with_children(epic_key: str, known_repos: set | None = None) -> dict:
    """Fetches an Epic and its child issues, each with repository_origen
    already resolved -- used by --epic mode (run_poc_loop.sh/orchestration.py)
    to build one combined prompt instead of processing children one by one.

    Children are looked up via JQL, not the attachment/Rovo machinery of
    fetch_ticket_live() (that's N extra HTTP round-trips for what's meant to
    be a lightweight list) -- just summary/description/repository_origen.

    JQL default targets team-managed projects (the "parent" field). Override
    JIRA_EPIC_LINK_JQL in .env (a template with {epic_key}) for
    company-managed projects still using the custom "Epic Link" field.
    """
    jira_url = os.environ["JIRA_URL"].rstrip("/")
    headers = _auth_headers()

    def _fetch_epic():
        r = httpx.get(
            f"{jira_url}/rest/api/3/issue/{epic_key}",
            headers=headers,
            params={"fields": "summary,description,labels,status,components"},
            timeout=15.0,
        )
        r.raise_for_status()
        return r

    epic_resp = retry_call(_fetch_epic)
    epic_issue = epic_resp.json()
    epic_fields = epic_issue.get("fields", {})
    epic = {
        "ticket_id": epic_issue.get("key"),
        "summary": epic_fields.get("summary", ""),
        "description": _adf_to_text(epic_fields.get("description")).strip(),
        "repository_origen": _resolve_repository_origen(epic_fields, known_repos),
    }

    jql_template = os.environ.get("JIRA_EPIC_LINK_JQL", 'parent = "{epic_key}"')
    jql = jql_template.format(epic_key=epic_key)
    def _search():
        # GET /rest/api/3/search fue dado de baja por Atlassian (410 Gone) --
        # el reemplazo es POST /rest/api/3/search/jql, con el JQL/campos en
        # el body en vez de query params.
        r = httpx.post(
            f"{jira_url}/rest/api/3/search/jql",
            headers=headers,
            json={"jql": jql, "fields": ["summary", "description", "labels", "components"]},
            timeout=15.0,
        )
        r.raise_for_status()
        return r

    search_resp = retry_call(_search)
    children = [
        {
            "ticket_id": issue.get("key"),
            "summary": issue.get("fields", {}).get("summary", ""),
            "description": _adf_to_text(issue.get("fields", {}).get("description")).strip(),
            "repository_origen": _resolve_repository_origen(issue.get("fields", {}), known_repos),
        }
        for issue in search_resp.json().get("issues", [])
    ]

    return {"epic": epic, "children": children}


def get_ticket() -> dict:
    ticket_key = os.environ.get("JIRA_TICKET_KEY", "UNSET")
    return cached_call(
        namespace="jira_issue",
        params={"ticket_key": ticket_key, "jira_url": os.environ.get("JIRA_URL", "")},
        fetch_fn=fetch_ticket_live,
    )


def _build_smoke_ticket_payload(component: str) -> dict:
    """Body for POST /rest/api/3/issue used by scripts/smoke_test.sh to
    create a real, disposable Jira ticket per run. Uses the labels-fallback
    resolution path for repository_origen (not the native Components field)
    because that requires the named Component to already be configured in
    the Jira project's Settings, which a smoke test can't safely assume.
    The description carries a real codeBlock node (exercises
    has_log_evidence) with synthetic, boring text -- no jailbreak patterns,
    no secrets, this is meant to sail cleanly through the firewall so the
    smoke test validates the happy path.
    """
    issue_type = os.environ.get("JIRA_SMOKE_TEST_ISSUE_TYPE", "Task")
    return {
        "fields": {
            "project": {"key": os.environ["JIRA_PROJECT_KEY"]},
            "issuetype": {"name": issue_type},
            "summary": f"[smoke-test] Validacion automatizada del pipeline ({component})",
            "labels": ["smoke-test", component],
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": "Ticket generado automaticamente por scripts/smoke_test.sh."}],
                    },
                    {
                        "type": "codeBlock",
                        "content": [{"type": "text", "text": "SmokeTestException: synthetic stack trace for has_log_evidence"}],
                    },
                ],
            },
        }
    }


def create_smoke_ticket(component: str) -> str:
    jira_url = os.environ["JIRA_URL"].rstrip("/")
    email = require_secret("JIRA_EMAIL")
    token = require_secret("JIRA_API_TOKEN")

    auth = base64.b64encode(f"{email}:{token}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}", "Accept": "application/json", "Content-Type": "application/json"}

    resp = httpx.post(
        f"{jira_url}/rest/api/3/issue",
        headers=headers,
        json=_build_smoke_ticket_payload(component),
        timeout=15.0,
    )
    resp.raise_for_status()
    return resp.json()["key"]


def post_audit_comment(ticket_key: str, text: str) -> dict:
    """Posts a plain-text audit comment on the real Jira ticket, so the
    firewall's decision and whatever Copilot did are visible directly in
    Jira's own comment history — not just in the local logs/*.jsonl files.
    """
    jira_url = os.environ["JIRA_URL"].rstrip("/")
    email = require_secret("JIRA_EMAIL")
    token = require_secret("JIRA_API_TOKEN")

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
    email = require_secret("JIRA_EMAIL")
    token = require_secret("JIRA_API_TOKEN")

    auth = base64.b64encode(f"{email}:{token}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}", "Accept": "application/json", "Content-Type": "application/json"}

    def _list_transitions():
        r = httpx.get(
            f"{jira_url}/rest/api/3/issue/{ticket_key}/transitions",
            headers=headers,
            timeout=15.0,
        )
        r.raise_for_status()
        return r

    resp = retry_call(_list_transitions)
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
    if len(sys.argv) >= 2 and sys.argv[1] == "fetch-epic":
        if len(sys.argv) < 3:
            print(json.dumps({"error": "usage: jira_client.py fetch-epic <EPIC_KEY>"}), file=sys.stderr)
            sys.exit(1)
        try:
            result = fetch_epic_with_children(sys.argv[2])
        except KeyError as exc:
            print(json.dumps({"error": f"missing_env_var:{exc.args[0]}"}), file=sys.stderr)
            sys.exit(1)
        except httpx.HTTPStatusError as exc:
            print(
                json.dumps({"error": "jira_http_error", "status_code": exc.response.status_code, "body": exc.response.text}),
                file=sys.stderr,
            )
            sys.exit(1)
        print(json.dumps(result, ensure_ascii=False))
        return

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

    if len(sys.argv) >= 2 and sys.argv[1] == "create-smoke-ticket":
        if len(sys.argv) < 3:
            print(json.dumps({"error": "usage: jira_client.py create-smoke-ticket <component>"}), file=sys.stderr)
            sys.exit(1)
        try:
            ticket_id = create_smoke_ticket(sys.argv[2])
        except KeyError as exc:
            print(json.dumps({"error": f"missing_env_var:{exc.args[0]}"}), file=sys.stderr)
            sys.exit(1)
        except httpx.HTTPStatusError as exc:
            print(
                json.dumps({"error": "jira_http_error", "status_code": exc.response.status_code, "body": exc.response.text}),
                file=sys.stderr,
            )
            sys.exit(1)
        print(json.dumps({"ticket_id": ticket_id}, ensure_ascii=False))
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
                    "error": "no_matching_component_or_label",
                    "detail": f"El ticket {ticket.get('ticket_id')} no tiene ningun Component ni etiqueta en {sorted(KNOWN_REPOS)}. "
                    "Asigna el campo Components de Jira (recomendado, Settings > Components de tu proyecto) o una "
                    "etiqueta (label) con el nombre exacto del nodo del grafo afectado.",
                }
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    print(json.dumps(ticket, ensure_ascii=False))


if __name__ == "__main__":
    main()
