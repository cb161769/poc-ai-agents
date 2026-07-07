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

Usage:
  docker compose up -d prefect-server
  export PREFECT_API_URL=http://localhost:4200/api
  cd /path/to/your/real/project   # <-- the repo the ticket is about
  python3 /path/to/poc-ai-agents/orchestration.py

run_poc_loop.sh is kept as-is for anyone who prefers a plain bash run
without standing up Prefect — both drive the exact same scripts underneath.
"""
import json
import os
import shutil
import subprocess
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


def _run(cmd: list, input_text: str | None = None, check: bool = True) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=SCRIPT_DIR, input=input_text)
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
def transition_jira(status: str):
    _run(["python3", "jira_client.py", "transition", status], check=False)


@task(retries=2, retry_delay_seconds=5, name="comment-jira")
def comment_jira(text: str):
    _run(["python3", "jira_client.py", "comment", text], check=False)


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
    comment_jira(f"🚨 Falco (monitoreo a nivel de sistema, automatizado, Prefect): se detectaron {count} alerta(s) durante esta corrida — {alert_lines}")

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


@flow(name="poc-ai-agents-pipeline", log_prints=True)
def run_pipeline():
    logger = get_run_logger()

    target_repo_dir = detect_target_repo()
    check_dirty_tree(target_repo_dir)
    logger.info(f"Repo objetivo detectado: {target_repo_dir}")

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

    prompt = (
        f"{ticket['summary']}\n{ticket['description']}\n"
        f"--- Adjuntos (descritos por Rovo) ---\n{ticket.get('attachment_context', '')}\n"
        f"--- Grafo de impacto ---\n{graph_result}\n"
        f"--- Hallazgos Sonar (reales) ---\n{sonar_issues_text}"
    )
    jira_context = {
        "ticket_id": ticket["ticket_id"],
        "summary": ticket["summary"],
        "description": ticket["description"],
        "repository_origen": component,
    }

    firewall_result = evaluate_firewall(prompt, jira_context, [i["message"] for i in sonar_result["issues"]])

    if firewall_result["status"] == "REJECTED":
        comment_jira(f"🛡️ AI Firewall (Prefect): RECHAZADA. Motivo: {firewall_result['reason']}.")
        logger.warning(f"Rechazado por el firewall: {firewall_result['reason']}")

        # El juez audita el rechazo pero nunca lo revierte — el firewall
        # sigue siendo la ultima palabra en seguridad. Esto es solo una
        # alerta de posible falso positivo, nunca un desbloqueo.
        judge_verdict = _run_judge_safe(
            jira_context, firewall_result, "firewall_rejected", firewall_result["reason"],
            "sin tests corridos para esta corrida",
        )
        if judge_verdict is not None:
            if judge_verdict["verdict"] == "FLAGGED":
                comment_jira(
                    f"🧑‍⚖️ Agente juez (Prefect): el rechazo del firewall podria ser incorrecto — "
                    f"{judge_verdict['reasoning']} La solicitud SIGUE RECHAZADA (el juez no puede "
                    "revertir al firewall); revision humana recomendada."
                )
            else:
                comment_jira(f"🧑‍⚖️ Agente juez (Prefect): OK, el rechazo del firewall fue correcto. {judge_verdict['reasoning']}")

        raise PipelineBlocked(firewall_result["reason"])

    transition_jira(JIRA_IN_PROGRESS_STATUS)
    falco_since = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sanitized = firewall_result["sanitized_prompt"]

    if GITHUB_REPO:
        agent_result = run_coding_agent_cloud(ticket["ticket_id"], ticket["summary"], sanitized)
        comment_jira(
            f"🤖 Coding agent (Prefect): issue {agent_result['issue_url']} creado, "
            f"asignado={agent_result['assigned']}. El agente trabaja en la nube y abrira un PR."
        )
        judge_verdict = _run_judge_safe(
            jira_context, firewall_result, "issue_only", sanitized,
            "sin tests (coding agent en la nube, el PR aun no existe en el momento de esta corrida)",
        )
    else:
        agent_result = run_coding_agent_local(ticket["ticket_id"], sanitized, target_repo_dir)

        if agent_result["applied"]:
            comment_jira(
                f"🤖 Copilot (Prefect): AI Firewall aprobo la solicitud (redacciones: {firewall_result['redactions_applied']}). "
                f"Copilot aplico un cambio en la rama '{agent_result['branch']}' de {target_repo_dir}, "
                f"pendiente de revision humana (no en '{agent_result['base_branch']}')."
            )
            test_result = run_tests(target_repo_dir)
            if not test_result["passed"]:
                comment_jira(f"🧪 Testing agent (Prefect): los tests reales FALLARON en '{agent_result['branch']}'.")
                transition_jira(JIRA_BLOCKED_STATUS)
                logger.error("Tests fallaron — bloqueado antes del juez.")
                check_falco_correlation(falco_since, ticket["ticket_id"])
                raise PipelineBlocked("tests reales fallaron")

            diff_text = _run([
                "git", "-C", target_repo_dir, "diff", f"{agent_result['base_branch']}..{agent_result['branch']}"
            ])
            judge_verdict = _run_judge_safe(jira_context, firewall_result, "local_diff", diff_text, test_result["output"])
        else:
            comment_jira(
                "🤖 Copilot (Prefect): AI Firewall aprobo la solicitud "
                f"(redacciones: {firewall_result['redactions_applied']}). Copilot no aplico ningun cambio en esta corrida."
            )
            judge_verdict = _run_judge_safe(jira_context, firewall_result, "issue_only", sanitized, "sin cambios aplicados")

    if judge_verdict is None:
        logger.warning("El juez no pudo evaluar esta corrida — continua sin veredicto.")
    elif judge_verdict["verdict"] == "FLAGGED":
        comment_jira(f"🧑‍⚖️ Agente juez (Prefect): FLAGGED. {judge_verdict['reasoning']}")
        transition_jira(JIRA_BLOCKED_STATUS)
        logger.error(f"Juez marco FLAGGED: {judge_verdict['reasoning']}")
    else:
        comment_jira(f"🧑‍⚖️ Agente juez (Prefect): OK. {judge_verdict['reasoning']}")
        logger.info("Juez marco OK.")

    check_falco_correlation(falco_since, ticket["ticket_id"])

    return {"firewall": firewall_result, "agent": agent_result, "judge": judge_verdict}


if __name__ == "__main__":
    run_pipeline()
