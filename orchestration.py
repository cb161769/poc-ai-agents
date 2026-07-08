"""Prefect-based orchestration layer — replaces the linear bash script
(run_poc_loop.sh) with a real workflow engine: per-step retries, persisted
state, and a web UI (http://localhost:4200, the `prefect-server` compose
service) to watch every run step by step instead of reading terminal output
top to bottom.

Important: this orchestrates the SAME building blocks as run_poc_loop.sh
(jira_client.py, sonar_client.py, cypher-shell, firewall_proxy.py, gh/git,
scripts/run_module_tests.sh, judge_agent.py) — it does not reimplement their
logic. What it adds on top: automatic retries per step, a run history you
can inspect in the UI, and a single place where the whole pipeline's shape
is visible as a graph instead of a script.

IMPORTANT: this operates on whatever git repo you are standing in when you
run `python3 orchestration.py` (cd to your real project first) — it does
NOT use sample-repo/ by default. sample-repo/ stays in this project only as
a reference of what project file each stack needs for
scripts/run_module_tests.sh to auto-detect it.

Which ticket: pass it as the first argument (python3 orchestration.py
JIRA-123) to work any ticket someone hands to Copilot without touching
.env. Without an argument, falls back to JIRA_TICKET_KEY from .env.

Epic mode: python3 orchestration.py --epic EPIC-123 fetches the epic and
ALL its children, and runs ONE combined prompt through the pipeline instead
of processing children one by one. Only works if every child's component
resolves to the SAME repo_url in the Neo4j graph -- refuses (never guesses)
if the epic's children genuinely live in different repos, since this
pipeline is built around one repo per run.

Usage:
  docker compose up -d prefect-server
  export PREFECT_API_URL=http://localhost:4200/api
  cd /path/to/your/real/project   # <-- the repo the ticket is about
  python3 /path/to/poc-ai-agents/orchestration.py [JIRA_TICKET_KEY]
  python3 /path/to/poc-ai-agents/orchestration.py --epic EPIC_KEY

run_poc_loop.sh is kept as-is for anyone who prefers a plain bash run
without standing up Prefect — both drive the exact same scripts underneath.
"""
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
FIREWALL_URL = os.environ.get("FIREWALL_URL", "http://localhost:8080")
JIRA_IN_PROGRESS_STATUS = os.environ.get("JIRA_IN_PROGRESS_STATUS", "In Progress")
JIRA_BLOCKED_STATUS = os.environ.get("JIRA_BLOCKED_STATUS", "Blocked")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")
GITHUB_COPILOT_ASSIGNEE = os.environ.get("GITHUB_COPILOT_ASSIGNEE", "copilot-swe-agent")
FIREWALL_API_KEY = os.environ.get("FIREWALL_API_KEY", "")


class PipelineBlocked(Exception):
    """Raised to stop the flow early (rejected by firewall, tests failed,
    attachments unresolved) — surfaces as a failed run in the Prefect UI,
    which is the observability win over a bash `exit 1` nobody's watching.
    """


def _run(cmd: list, input_text: str | None = None, check: bool = True, env: dict | None = None) -> str:
    run_env = {**os.environ, **env} if env else None
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=SCRIPT_DIR, input=input_text, env=run_env)
    if check and result.returncode != 0:
        raise RuntimeError(f"comando fallo ({' '.join(cmd)}): {result.stderr.strip()}")
    return result.stdout


@task(retries=0, name="detect-target-repo")
def detect_target_repo() -> str:
    # Deliberately NOT cwd=SCRIPT_DIR: we want the repo the user invoked
    # `python3 orchestration.py` from, not where this tool lives.
    result = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True)
    if result.returncode != 0:
        raise PipelineBlocked(
            "No estas parado dentro de un repositorio git. cd a tu proyecto real (el que corresponde "
            "al ticket) antes de correr orchestration.py — ya no se usa sample-repo/ por defecto."
        )
    return result.stdout.strip()


@task(retries=0, name="dirty-tree-gate")
def check_dirty_tree(target_repo_dir: str):
    status = subprocess.run(
        ["git", "-C", target_repo_dir, "status", "--porcelain"], capture_output=True, text=True
    ).stdout
    if status.strip():
        raise PipelineBlocked(
            f"El repo en {target_repo_dir} tiene cambios sin commitear. Hace commit o 'git stash' antes "
            "de correr esto — el pipeline va a crear una rama nueva y no queremos mezclar tu trabajo en "
            "progreso con lo que haga Copilot."
        )


@task(retries=1, retry_delay_seconds=5, name="discover-known-components")
def discover_known_components():
    """Best-effort: derive the known-components set from whatever node
    names already exist in the real Neo4j graph, instead of relying only on
    the static JIRA_KNOWN_COMPONENTS list in .env staying in sync with it.
    Mutates os.environ so the fetch_jira_ticket() subprocess (python3
    jira_client.py) inherits it. If Neo4j isn't reachable, this leaves
    JIRA_KNOWN_COMPONENTS as whatever was already in the environment.
    """
    neo4j_uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.environ.get("NEO4J_USERNAME", "neo4j")
    neo4j_pass = os.environ.get("NEO4J_PASSWORD", "test_password_local")
    try:
        output = _run(
            [
                "cypher-shell", "-a", neo4j_uri, "-u", neo4j_user, "-p", neo4j_pass,
                "--format", "plain", "MATCH (n) RETURN DISTINCT n.name AS name",
            ]
        )
    except RuntimeError:
        return

    names = [line.strip().strip('"') for line in output.splitlines()[1:] if line.strip()]
    if names:
        os.environ["JIRA_KNOWN_COMPONENTS"] = ",".join(names)
        print(f"Componentes conocidos derivados del grafo Neo4j: {os.environ['JIRA_KNOWN_COMPONENTS']}")


@task(retries=2, retry_delay_seconds=5, name="fetch-jira-ticket")
def fetch_jira_ticket() -> dict:
    return json.loads(_run(["python3", "jira_client.py"]))


@task(name="attachments-gate")
def check_attachments_gate(ticket: dict):
    if ticket.get("has_attachments") and "Requiere revision humana" in (ticket.get("attachment_context") or ""):
        comment_jira.submit(
            "🛑 Pipeline (Prefect): el ticket tiene adjuntos sin descripcion de Rovo todavia. "
            "Bloqueado antes del firewall — requiere revision humana."
        ).result()
        raise PipelineBlocked("adjuntos sin describir por Rovo")


@task(name="log-evidence-nudge")
def check_log_evidence(ticket: dict):
    if not ticket.get("has_log_evidence"):
        comment_jira.submit(
            f"📋 Pipeline (Prefect): para diagnosticar este bug en '{ticket.get('repository_origen')}' con "
            "precision, pega el log o stack trace real como bloque de codigo en la descripcion."
        ).result()


@task(retries=2, retry_delay_seconds=10, name="query-graph")
def query_graph(component: str) -> str:
    neo4j_uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.environ.get("NEO4J_USERNAME", "neo4j")
    neo4j_pass = os.environ.get("NEO4J_PASSWORD", "test_password_local")
    query = (
        f"MATCH (origin {{name: '{component}'}})<-[:DEPENDS_ON]-(dependent) "
        "RETURN dependent.name AS servicio, dependent.language AS lenguaje"
    )
    return _run(["cypher-shell", "-a", neo4j_uri, "-u", neo4j_user, "-p", neo4j_pass, "--format", "plain", query])


@task(retries=2, retry_delay_seconds=5, name="query-sonar")
def query_sonar(component: str) -> dict:
    return json.loads(_run(["python3", "sonar_client.py", component]))


@task(retries=1, retry_delay_seconds=5, name="query-figma")
def query_figma(figma_link: dict | None) -> dict | None:
    """Optional: only runs if the ticket's description carried a Figma link
    (jira_client.py._extract_figma_link) and FIGMA_API_TOKEN is configured.
    Best-effort like the judge/Falco correlation -- a Figma hiccup never
    blocks the pipeline, it just means the prompt goes out without that
    section, same as a ticket with no Figma link at all.
    """
    if not figma_link:
        return None
    if not os.environ.get("FIGMA_API_TOKEN"):
        print("El ticket trae un link de Figma pero falta FIGMA_API_TOKEN — se omite esa seccion del prompt.")
        return None

    result = subprocess.run(
        ["python3", str(SCRIPT_DIR / "figma_client.py"), figma_link["file_key"], figma_link["node_id"]],
        capture_output=True,
        text=True,
        cwd=SCRIPT_DIR,
    )
    if result.returncode != 0:
        print(f"No se pudo consultar Figma para este ticket, se omite esa seccion del prompt: {result.stderr.strip()}")
        return None
    return json.loads(result.stdout)


@task(retries=1, name="evaluate-firewall")
def evaluate_firewall(prompt: str, jira_context: dict, sonar_errors: list) -> dict:
    headers = {"X-Firewall-Key": FIREWALL_API_KEY} if FIREWALL_API_KEY else {}
    resp = httpx.post(
        f"{FIREWALL_URL}/evaluate",
        json={"prompt": prompt, "jira_context": jira_context, "sonar_errors": sonar_errors},
        headers=headers,
        timeout=15.0,
    )
    return resp.json()


@task(retries=2, retry_delay_seconds=5, name="transition-jira")
def transition_jira(status: str, ticket_key: str | None = None):
    env = {"JIRA_TICKET_KEY": ticket_key} if ticket_key else None
    _run(["python3", "jira_client.py", "transition", status], check=False, env=env)


@task(retries=2, retry_delay_seconds=5, name="comment-jira")
def comment_jira(text: str, ticket_key: str | None = None):
    env = {"JIRA_TICKET_KEY": ticket_key} if ticket_key else None
    _run(["python3", "jira_client.py", "comment", text], check=False, env=env)


@task(retries=1, name="coding-agent-cloud")
def run_coding_agent_cloud(ticket_id: str, summary: str, sanitized_prompt: str) -> dict:
    issue_body = (
        f"{sanitized_prompt}\n\n---\nGenerado automaticamente por poc-ai-agents "
        f"(orchestration.py / Prefect) desde el ticket Jira {ticket_id}."
    )
    issue_url = _run(["gh", "issue", "create", "--repo", GITHUB_REPO, "--title", summary, "--body", issue_body]).strip()
    assigned = True
    try:
        _run(["gh", "issue", "edit", issue_url, "--add-assignee", GITHUB_COPILOT_ASSIGNEE])
    except RuntimeError:
        assigned = False
    return {"issue_url": issue_url, "assigned": assigned}


@task(retries=0, name="coding-agent-local-fallback")
def run_coding_agent_local(ticket_id: str, sanitized_prompt: str, target_repo_dir: str) -> dict:
    base_branch = subprocess.run(
        ["git", "-C", target_repo_dir, "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True
    ).stdout.strip()

    branch = f"copilot/{ticket_id}-{int(time.time())}"
    subprocess.run(["git", "-C", target_repo_dir, "checkout", "-b", branch], check=True)
    suggest = subprocess.run(["gh", "copilot", "suggest", "-t", "shell", sanitized_prompt], cwd=target_repo_dir)
    status = subprocess.run(
        ["git", "-C", target_repo_dir, "status", "--porcelain"], capture_output=True, text=True
    ).stdout

    if suggest.returncode == 0 and status.strip():
        subprocess.run(["git", "-C", target_repo_dir, "add", "-A"], check=True)
        subprocess.run(["git", "-C", target_repo_dir, "commit", "-m", f"Copilot suggestion for {ticket_id}"], check=True)
        return {"applied": True, "branch": branch, "base_branch": base_branch}

    subprocess.run(["git", "-C", target_repo_dir, "checkout", base_branch])
    subprocess.run(["git", "-C", target_repo_dir, "branch", "-D", branch])
    return {"applied": False, "branch": None, "base_branch": base_branch}


def _local_coding_agent_backend_available() -> bool:
    """Same check the judge/coding_agent.py use to pick a backend -- if
    neither is available, Camino B falls back to gh copilot suggest
    (run_coding_agent_local) instead of the real agent (coding_agent.py).
    """
    if os.environ.get("ANTHROPIC_API_KEY"):
        return True
    try:
        httpx.get(f"{os.environ.get('OLLAMA_URL', 'http://localhost:11434')}/api/tags", timeout=3.0)
        return True
    except httpx.HTTPError:
        return False


@task(retries=0, name="coding-agent-local-real")
def run_coding_agent_local_real(ticket_id: str, sanitized_prompt: str, target_repo_dir: str) -> dict:
    """Camino B1: the real local coding agent (coding_agent.py) -- reasons
    over several turns, can read/write/list/grep the repo and query the same
    MCP tools the judge has, with human confirmation before every write or
    shell command. Same {"applied","branch","base_branch"} shape as
    run_coding_agent_local() so _deliver() doesn't need to know which ran.
    """
    base_branch = subprocess.run(
        ["git", "-C", target_repo_dir, "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True
    ).stdout.strip()

    branch = f"copilot/{ticket_id}-{int(time.time())}"
    subprocess.run(["git", "-C", target_repo_dir, "checkout", "-b", branch], check=True)

    payload_file = SCRIPT_DIR / "logs" / f".coding_agent_payload_{ticket_id}_{int(time.time())}.json"
    payload_file.parent.mkdir(parents=True, exist_ok=True)
    payload_file.write_text(
        json.dumps({"ticket_id": ticket_id, "sanitized_prompt": sanitized_prompt, "target_repo_dir": target_repo_dir}),
        encoding="utf-8",
    )
    try:
        # stdout=PIPE only (not capture_output=True): stdin/stderr stay
        # inherited from the terminal so the interactive confirmations in
        # coding_agent.py are visible and answerable live.
        result = subprocess.run(
            ["python3", str(SCRIPT_DIR / "coding_agent.py"), str(payload_file)],
            stdout=subprocess.PIPE,
            text=True,
            cwd=target_repo_dir,
        )
    finally:
        payload_file.unlink(missing_ok=True)

    try:
        agent_result = json.loads(result.stdout)
        print(f"Resultado del agente: {agent_result.get('status')} — {agent_result.get('summary')}")
    except json.JSONDecodeError:
        print("El agente no devolvio un JSON valido en stdout.")

    status = subprocess.run(
        ["git", "-C", target_repo_dir, "status", "--porcelain"], capture_output=True, text=True
    ).stdout

    if status.strip():
        subprocess.run(["git", "-C", target_repo_dir, "add", "-A"], check=True)
        subprocess.run(["git", "-C", target_repo_dir, "commit", "-m", f"Coding agent change for {ticket_id}"], check=True)
        return {"applied": True, "branch": branch, "base_branch": base_branch}

    subprocess.run(["git", "-C", target_repo_dir, "checkout", base_branch])
    subprocess.run(["git", "-C", target_repo_dir, "branch", "-D", branch])
    return {"applied": False, "branch": None, "base_branch": base_branch}


@task(retries=0, name="testing-agent")
def run_tests(target_repo_dir: str) -> dict:
    if shutil.which("docker") is None:
        return {"passed": True, "output": "(testing agent omitido: docker no disponible en el host)"}

    result = subprocess.run(
        ["bash", str(SCRIPT_DIR / "scripts" / "run_module_tests.sh"), target_repo_dir],
        capture_output=True,
        text=True,
    )
    return {"passed": result.returncode == 0, "output": result.stdout + result.stderr}


@task(retries=1, retry_delay_seconds=5, name="fetch-epic")
def fetch_epic(epic_key: str) -> dict:
    return json.loads(_run(["python3", "jira_client.py", "fetch-epic", epic_key]))


def _resolve_single_repo(name_to_repo_url: dict) -> tuple:
    """Pure: given {component_name: repo_url_or_None}, decides whether all
    components live in the same repo. Fail-safe -- missing repo_url or a
    disagreement both mean "no", never guessed as "yes".
    Returns (ok, repo_url_or_None, reason_if_not_ok).
    """
    missing = [name for name, url in name_to_repo_url.items() if not url]
    if missing:
        return False, None, f"Estos componentes no tienen repo_url en el grafo: {', '.join(missing)}."

    distinct = {url for url in name_to_repo_url.values() if url}
    if len(distinct) != 1:
        joined = ", ".join(f"{name}={url}" for name, url in name_to_repo_url.items())
        return False, None, f"Los componentes de esta epica viven en repos distintos segun el grafo: {joined}."

    return True, next(iter(distinct)), ""


@task(retries=1, retry_delay_seconds=5, name="check-epic-single-repo")
def check_epic_single_repo_task(component_names: list) -> str:
    """Queries Neo4j for repo_url of every component an epic's children
    touch, and raises PipelineBlocked (never guesses) unless they all agree
    on the exact same repo -- this pipeline is built around one repo per
    run (target_repo_dir), so an epic spanning multiple real repos can't be
    processed in a single combined run.
    """
    neo4j_uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.environ.get("NEO4J_USERNAME", "neo4j")
    neo4j_pass = os.environ.get("NEO4J_PASSWORD", "test_password_local")
    quoted = ", ".join(f"'{name}'" for name in component_names)
    query = f"MATCH (n) WHERE n.name IN [{quoted}] RETURN n.name + '|' + coalesce(n.repo_url, '') AS row"
    output = _run(["cypher-shell", "-a", neo4j_uri, "-u", neo4j_user, "-p", neo4j_pass, "--format", "plain", query])

    name_to_repo_url = {}
    for line in output.splitlines()[1:]:
        row = line.strip().strip('"')
        if not row:
            continue
        name, _, repo_url = row.partition("|")
        name_to_repo_url[name] = repo_url or None

    missing_from_graph = [n for n in component_names if n not in name_to_repo_url]
    if missing_from_graph:
        raise PipelineBlocked(
            f"Estos componentes de la epica no existen en el grafo Neo4j: {', '.join(missing_from_graph)}."
        )

    ok, repo_url, reason = _resolve_single_repo(name_to_repo_url)
    if not ok:
        raise PipelineBlocked(f"{reason} No se puede procesar una epica que toca mas de un repo en una sola corrida.")
    return repo_url


@task(retries=1, name="judge-agent")
def run_judge(ticket: dict, firewall_result: dict, change_source: str, change_description: str, test_summary: str) -> dict:
    payload = {
        "ticket": ticket,
        "firewall": firewall_result,
        "change_source": change_source,
        "change_description": change_description,
        "test_summary": test_summary,
    }
    result = subprocess.run(
        ["python3", str(SCRIPT_DIR / "judge_agent.py")],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        cwd=SCRIPT_DIR,
    )
    if result.returncode != 0:
        raise RuntimeError(f"juez fallo: {result.stderr.strip()}")
    return json.loads(result.stdout)


@task(retries=0, name="falco-correlation")
def check_falco_correlation(since_iso: str, ticket_id: str):
    """Correlates logs/falco_alerts.jsonl (Falco already writes these in real
    time, see falco/custom_rules.yaml) with this run's time window. Advisory
    only -- never blocks the flow, just surfaces what Falco saw via a Jira
    comment and, if configured, a webhook POST.
    """
    result = subprocess.run(
        ["python3", str(SCRIPT_DIR / "scripts" / "check_falco_alerts.py"), since_iso, str(SCRIPT_DIR / "logs" / "falco_alerts.jsonl")],
        capture_output=True,
        text=True,
    )
    if not result.stdout.strip():
        return
    try:
        summary = json.loads(result.stdout)
    except json.JSONDecodeError:
        return

    count = summary.get("count", 0)
    if not count:
        return

    alert_lines = " ".join(f"- [{a['priority']}] {a['rule']}: {a['output']}" for a in summary["alerts"])
    comment_jira(
        f"🚨 Falco (monitoreo a nivel de sistema, automatizado, Prefect): se detectaron {count} alerta(s) durante esta corrida — {alert_lines}",
        ticket_key=ticket_id,
    )

    webhook_url = os.environ.get("FALCO_ALERT_WEBHOOK_URL", "")
    if webhook_url:
        try:
            httpx.post(webhook_url, json={"text": f"🚨 Falco detecto {count} alerta(s) en la corrida de {ticket_id}: {alert_lines}"}, timeout=10.0)
        except httpx.HTTPError:
            print("(no se pudo postear al webhook de Falco)")


def _run_judge_safe(*args, **kwargs) -> dict | None:
    """Same tolerance as run_poc_loop.sh: if the judge can't run at all (no
    ANTHROPIC_API_KEY, no reachable Ollama, network failure), the pipeline
    continues without a verdict instead of failing the whole flow.
    """
    try:
        return run_judge(*args, **kwargs)
    except Exception as exc:
        print(f"El juez no pudo evaluar esta corrida (revisa ANTHROPIC_API_KEY, Ollama local, o conectividad): {exc}")
        return None


def _handle_rejected(ticket_id: str, jira_context: dict, firewall_result: dict):
    """Shared by ticket mode and epic mode: the firewall said REJECTED, the
    judge gets a chance to flag a possible false positive (advisory only,
    never overrides the firewall), then the flow stops.
    """
    comment_jira(f"🛡️ AI Firewall (Prefect): RECHAZADA. Motivo: {firewall_result['reason']}.", ticket_key=ticket_id)

    judge_verdict = _run_judge_safe(
        jira_context, firewall_result, "firewall_rejected", firewall_result["reason"],
        "sin tests corridos para esta corrida",
    )
    if judge_verdict is not None:
        if judge_verdict["verdict"] == "FLAGGED":
            comment_jira(
                f"🧑‍⚖️ Agente juez (Prefect): el rechazo del firewall podria ser incorrecto — "
                f"{judge_verdict['reasoning']} La solicitud SIGUE RECHAZADA (el juez no puede "
                "revertir al firewall); revision humana recomendada.",
                ticket_key=ticket_id,
            )
        else:
            comment_jira(
                f"🧑‍⚖️ Agente juez (Prefect): OK, el rechazo del firewall fue correcto. {judge_verdict['reasoning']}",
                ticket_key=ticket_id,
            )

    raise PipelineBlocked(firewall_result["reason"])


def _deliver(ticket_id: str, summary: str, firewall_result: dict, jira_context: dict, target_repo_dir: str) -> dict:
    """Etapa 5 en adelante: coding agent (nube o fallback local), testing
    agent, juez, correlacion de Falco. Compartido por ticket mode y epic
    mode -- ambos llegan aca con el firewall ya en APPROVED y jira_context
    armado a su manera (un ticket, o una epica + sus hijos combinados).
    """
    sanitized = firewall_result["sanitized_prompt"]
    transition_jira(JIRA_IN_PROGRESS_STATUS, ticket_key=ticket_id)
    falco_since = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if GITHUB_REPO:
        agent_result = run_coding_agent_cloud(ticket_id, summary, sanitized)
        comment_jira(
            f"🤖 Coding agent (Prefect): issue {agent_result['issue_url']} creado, "
            f"asignado={agent_result['assigned']}. El agente trabaja en la nube y abrira un PR.",
            ticket_key=ticket_id,
        )
        judge_verdict = _run_judge_safe(
            jira_context, firewall_result, "issue_only", sanitized,
            "sin tests (coding agent en la nube, el PR aun no existe en el momento de esta corrida)",
        )
    else:
        if _local_coding_agent_backend_available():
            agent_result = run_coding_agent_local_real(ticket_id, sanitized, target_repo_dir)
        else:
            agent_result = run_coding_agent_local(ticket_id, sanitized, target_repo_dir)

        if agent_result["applied"]:
            comment_jira(
                f"🤖 Copilot (Prefect): AI Firewall aprobo la solicitud (redacciones: {firewall_result['redactions_applied']}). "
                f"Copilot aplico un cambio en la rama '{agent_result['branch']}' de {target_repo_dir}, "
                f"pendiente de revision humana (no en '{agent_result['base_branch']}').",
                ticket_key=ticket_id,
            )
            test_result = run_tests(target_repo_dir)
            if not test_result["passed"]:
                comment_jira(f"🧪 Testing agent (Prefect): los tests reales FALLARON en '{agent_result['branch']}'.", ticket_key=ticket_id)
                transition_jira(JIRA_BLOCKED_STATUS, ticket_key=ticket_id)
                check_falco_correlation(falco_since, ticket_id)
                raise PipelineBlocked("tests reales fallaron")

            diff_text = _run([
                "git", "-C", target_repo_dir, "diff", f"{agent_result['base_branch']}..{agent_result['branch']}"
            ])
            judge_verdict = _run_judge_safe(jira_context, firewall_result, "local_diff", diff_text, test_result["output"])
        else:
            comment_jira(
                "🤖 Copilot (Prefect): AI Firewall aprobo la solicitud "
                f"(redacciones: {firewall_result['redactions_applied']}). Copilot no aplico ningun cambio en esta corrida.",
                ticket_key=ticket_id,
            )
            judge_verdict = _run_judge_safe(jira_context, firewall_result, "issue_only", sanitized, "sin cambios aplicados")

    if judge_verdict is None:
        print("El juez no pudo evaluar esta corrida — continua sin veredicto.")
    elif judge_verdict["verdict"] == "FLAGGED":
        comment_jira(f"🧑‍⚖️ Agente juez (Prefect): FLAGGED. {judge_verdict['reasoning']}", ticket_key=ticket_id)
        transition_jira(JIRA_BLOCKED_STATUS, ticket_key=ticket_id)
    else:
        comment_jira(f"🧑‍⚖️ Agente juez (Prefect): OK. {judge_verdict['reasoning']}", ticket_key=ticket_id)

    check_falco_correlation(falco_since, ticket_id)

    return {"firewall": firewall_result, "agent": agent_result, "judge": judge_verdict}


@flow(name="poc-ai-agents-pipeline", log_prints=True)
def run_pipeline():
    logger = get_run_logger()

    target_repo_dir = detect_target_repo()
    check_dirty_tree(target_repo_dir)
    logger.info(f"Repo objetivo detectado: {target_repo_dir}")

    discover_known_components()

    ticket = fetch_jira_ticket()
    component = ticket["repository_origen"]
    logger.info(f"Ticket {ticket['ticket_id']} — componente {component}")

    check_attachments_gate(ticket)
    check_log_evidence(ticket)

    graph_result = query_graph(component)
    sonar_result = query_sonar(component)
    sonar_issues_text = "\n".join(
        f"- [{i['severity']}] {i['rule']}: {i['message']} (linea {i['line']})" for i in sonar_result["issues"]
    )
    figma_result = query_figma(ticket.get("figma_link"))

    prompt = (
        f"{ticket['summary']}\n{ticket['description']}\n"
        f"--- Adjuntos (descritos por Rovo) ---\n{ticket.get('attachment_context', '')}\n"
        f"--- Grafo de impacto ---\n{graph_result}\n"
        f"--- Hallazgos Sonar (reales) ---\n{sonar_issues_text}"
    )
    if figma_result and figma_result.get("found"):
        prompt += f"\n--- Specs reales de Figma ---\n{json.dumps(figma_result['summary'], ensure_ascii=False, indent=2)}"
    jira_context = {
        "ticket_id": ticket["ticket_id"],
        "summary": ticket["summary"],
        "description": ticket["description"],
        "repository_origen": component,
    }

    firewall_result = evaluate_firewall(prompt, jira_context, [i["message"] for i in sonar_result["issues"]])

    if firewall_result["status"] == "REJECTED":
        logger.warning(f"Rechazado por el firewall: {firewall_result['reason']}")
        _handle_rejected(ticket["ticket_id"], jira_context, firewall_result)

    return _deliver(ticket["ticket_id"], ticket["summary"], firewall_result, jira_context, target_repo_dir)


@flow(name="poc-ai-agents-epic-pipeline", log_prints=True)
def run_epic_pipeline(epic_key: str):
    """--epic EPIC-123: fetches the epic and ALL its children, and runs ONE
    combined prompt through the whole pipeline instead of processing
    children one by one. Only proceeds if every child's component resolves
    to the SAME repo_url in the Neo4j graph -- refuses (never guesses) if
    they don't, since this pipeline is built around one repo per run.
    """
    logger = get_run_logger()

    target_repo_dir = detect_target_repo()
    check_dirty_tree(target_repo_dir)
    logger.info(f"Repo objetivo detectado: {target_repo_dir}")

    discover_known_components()

    epic_data = fetch_epic(epic_key)
    epic = epic_data["epic"]
    children = epic_data["children"]
    logger.info(f"Epica {epic_key} — {len(children)} hijos")

    if not children:
        raise PipelineBlocked(
            f"La epica {epic_key} no tiene hijos segun el JQL configurado (JIRA_EPIC_LINK_JQL). "
            "Si tu proyecto Jira es 'company-managed', probablemente necesites el campo custom 'Epic Link' en vez de 'parent'."
        )

    unresolved = [c["ticket_id"] for c in children if not c.get("repository_origen")]
    if unresolved:
        reason = (
            f"Estos hijos de {epic_key} no tienen un componente resuelto (Components/labels no matchean "
            f"ningun nodo conocido del grafo): {', '.join(unresolved)}."
        )
        comment_jira(f"🚫 Modo epica (Prefect): no se pudo procesar {epic_key} — {reason}", ticket_key=epic_key)
        raise PipelineBlocked(reason)

    distinct_components = sorted({c["repository_origen"] for c in children})

    try:
        repo_url = check_epic_single_repo_task(distinct_components)
    except PipelineBlocked as exc:
        comment_jira(f"🚫 Modo epica (Prefect): no se pudo procesar {epic_key} — {exc}", ticket_key=epic_key)
        raise
    logger.info(f"Todos los componentes de {epic_key} confirmados en el mismo repo: {repo_url}")

    origin_url = subprocess.run(
        ["git", "-C", target_repo_dir, "remote", "get-url", "origin"], capture_output=True, text=True
    ).stdout.strip()
    if origin_url and origin_url != repo_url:
        logger.warning(
            f"El remote 'origin' de {target_repo_dir} ({origin_url}) no coincide exactamente con repo_url "
            f"del grafo ({repo_url}) — puede ser solo ssh vs https, pero confirma que estas parado en el repo correcto."
        )

    graph_parts, sonar_parts, sonar_errors = [], [], []
    for component in distinct_components:
        graph_parts.append(f"--- {component} ---\n{query_graph(component)}")
        sonar_result = query_sonar(component)
        sonar_parts.append(
            f"--- {component} ---\n"
            + "\n".join(f"- [{i['severity']}] {i['rule']}: {i['message']} (linea {i['line']})" for i in sonar_result["issues"])
        )
        sonar_errors.extend(i["message"] for i in sonar_result["issues"])

    children_text = "\n".join(
        f"- {c['ticket_id']} ({c['repository_origen']}): {c['summary']}\n  {c['description']}" for c in children
    )
    prompt = (
        f"ESTO ES UNA EPICA con {len(children)} historias hijas. Resolvelas todas juntas, "
        "coordinando los cambios entre los componentes que toca cada una.\n\n"
        f"Epica {epic_key}: {epic['summary']}\n{epic['description']}\n\n"
        f"--- Historias hijas ---\n{children_text}\n"
        f"--- Grafo de impacto (por componente) ---\n{chr(10).join(graph_parts)}\n"
        f"--- Hallazgos Sonar (reales, por componente) ---\n{chr(10).join(sonar_parts)}"
    )
    jira_context = {
        "ticket_id": epic_key,
        "summary": epic["summary"],
        "description": epic["description"],
        "repository_origen": ",".join(distinct_components),
    }

    firewall_result = evaluate_firewall(prompt, jira_context, sonar_errors)

    if firewall_result["status"] == "REJECTED":
        logger.warning(f"Rechazado por el firewall: {firewall_result['reason']}")
        _handle_rejected(epic_key, jira_context, firewall_result)

    result = _deliver(epic_key, epic["summary"], firewall_result, jira_context, target_repo_dir)

    for child in children:
        comment_jira(
            f"🧩 Modo epica (Prefect): esta historia se proceso como parte de una corrida combinada de la epica {epic_key}.",
            ticket_key=child["ticket_id"],
        )

    return result


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--epic":
        if len(sys.argv) < 3:
            print("usage: orchestration.py --epic <EPIC_KEY>", file=sys.stderr)
            sys.exit(1)
        run_epic_pipeline(sys.argv[2])
    else:
        if len(sys.argv) > 1:
            os.environ["JIRA_TICKET_KEY"] = sys.argv[1]
        run_pipeline()
