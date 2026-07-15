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
import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# load_dotenv() TIENE que correr antes de "from prefect import ..." -- Prefect
# resuelve su configuracion (PREFECT_API_URL incluido) al importarse, no
# perezosamente en cada llamada como el resto de los clientes de este
# proyecto (Jira/Neo4j/Sonar leen os.environ.get(...) recien cuando se
# invocan). Si .env todavia no esta cargado en ese momento, Prefect cae a su
# modo "ephemeral" (una base SQLite local dentro del propio proceso) y nunca
# le manda nada al servidor real -- los flows/tasks se ven "correr bien" en
# el log de este proceso, pero jamas aparecen en la UI de prefect-server.
load_dotenv()

import httpx
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

import epic_planner
import graph_writer
import jira_client
import output_guard
from tech_doc_agent import generate_technical_report
from firewall_proxy import _redact
from log_utils import get_logger
from pipeline_shared import RETRYABLE_POLICY_REFERENCES

logger = get_logger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
FIREWALL_URL = os.environ.get("FIREWALL_URL", "http://localhost:8080")
JIRA_IN_PROGRESS_STATUS = os.environ.get("JIRA_IN_PROGRESS_STATUS", "In Progress")
JIRA_BLOCKED_STATUS = os.environ.get("JIRA_BLOCKED_STATUS", "Blocked")
# Corrida exitosa (juez OK) -- antes no habia NINGUNA transicion de exito, un
# ticket que pasaba todo el pipeline se quedaba atascado en JIRA_IN_PROGRESS_STATUS.
JIRA_REVIEW_STATUS = os.environ.get("JIRA_REVIEW_STATUS", "Code Review")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")
GITHUB_COPILOT_ASSIGNEE = os.environ.get("GITHUB_COPILOT_ASSIGNEE", "copilot-swe-agent")
FIREWALL_API_KEY = os.environ.get("FIREWALL_API_KEY", "")
# ALERT_WEBHOOK_URL es el nombre generalizado (antes solo existia para
# Falco); FALCO_ALERT_WEBHOOK_URL sigue funcionando como alias retrocompatible.
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL") or os.environ.get("FALCO_ALERT_WEBHOOK_URL", "")

# Importado de pipeline_shared.py (fuente unica) -- antes estaba duplicada
# a mano acá Y en judge_agent.py Y en un array bash en run_poc_loop.sh.


def post_alert_webhook(text: str):
    """Best-effort POST (formato compatible con un incoming webhook de
    Slack: {"text": "..."}) usado cuando Falco detecta algo, el juez marca
    FLAGGED, o el testing agent bloquea una corrida -- para que alguien
    reciba una alerta activa en vez de tener que leer Jira o los logs JSONL
    a mano. Nunca bloquea el flow por su cuenta.
    """
    if not ALERT_WEBHOOK_URL:
        return
    try:
        httpx.post(ALERT_WEBHOOK_URL, json={"text": text}, timeout=10.0)
    except httpx.HTTPError:
        print("(no se pudo postear al webhook de alertas)")


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
def discover_known_components() -> list:
    """Best-effort: derive the known-components set from whatever node
    names already exist in the real Neo4j graph, instead of relying only on
    the static JIRA_KNOWN_COMPONENTS list in .env staying in sync with it.
    Devuelve la lista para que el llamador se la pase directo a
    jira_client.fetch_ticket_live()/fetch_epic_with_children() como
    argumento -- ya no muta os.environ (eso solo hacia falta cuando
    fetch_jira_ticket() invocaba jira_client.py como subprocess aparte, que
    heredaba el entorno; ahora es un import directo). Si Neo4j no esta
    alcanzable, devuelve [] y el llamador cae al KNOWN_REPOS por default de
    jira_client.py.
    """
    neo4j_uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.environ.get("NEO4J_USERNAME", "neo4j")
    neo4j_pass = os.environ.get("NEO4J_PASSWORD", "test_password_local")
    try:
        output = _run(
            [
                "cypher-shell", "-a", neo4j_uri, "-u", neo4j_user, "-p", neo4j_pass,
                "--format", "plain", "MATCH (n:Service) RETURN DISTINCT n.name AS name",
            ]
        )
    except RuntimeError:
        return []

    names = [line.strip().strip('"') for line in output.splitlines()[1:] if line.strip()]
    if names:
        print(f"Componentes conocidos derivados del grafo Neo4j: {','.join(names)}")
    return names


@task(retries=2, retry_delay_seconds=5, name="fetch-jira-ticket")
def fetch_jira_ticket(known_components: list | None = None) -> dict:
    return jira_client.fetch_ticket_live(known_repos=set(known_components) if known_components else None)


@task(name="attachments-gate")
def check_attachments_gate(ticket: dict):
    if ticket.get("has_attachments") and "Requiere revision humana" in (ticket.get("attachment_context") or ""):
        comment_jira.submit(
            "🛑 Pipeline (Prefect): el ticket tiene adjuntos sin descripcion de Rovo todavia. "
            "Bloqueado antes del firewall — requiere revision humana."
        ).result()
        raise PipelineBlocked("adjuntos sin describir por Rovo")


# Nombres reales de issue type que representan un BUG -- varian por
# instancia/idioma de Jira (confirmado real: el proyecto KAN de esta
# sesion ni siquiera tiene un tipo "Bug", solo Epic/Historia/Tarea/
# Subtask). Sin este chequeo, check_log_evidence le pedia un stack trace a
# CUALQUIER ticket sin bloque de codigo en la descripcion -- incluidas
# Historias/Tareas reales que nunca tuvieron un error que diagnosticar.
_BUG_ISSUE_TYPE_NAMES = {"bug", "error", "defecto", "fallo", "incidencia"}


@task(name="log-evidence-nudge")
def check_log_evidence(ticket: dict):
    is_bug = (ticket.get("issue_type") or "").strip().lower() in _BUG_ISSUE_TYPE_NAMES
    if is_bug and not ticket.get("has_log_evidence"):
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


@task(retries=0, name="rescan-sonar")
def rescan_sonar(target_repo_dir: str, component: str) -> list:
    """query_sonar() de arriba SOLO lee analisis YA existentes -- nada
    volvia a escanear despues de que el coding agent aplicara su cambio, asi
    que un hallazgo nuevo introducido por el propio cambio era invisible
    para este pipeline. scripts/rescan_sonar.sh es best-effort real: si el
    repo objetivo no tiene Sonar configurado o el scan/polling fallan,
    devuelve scanned=false sin lanzar excepcion -- nunca bloquea la corrida.
    """
    try:
        result = json.loads(
            _run(["bash", str(SCRIPT_DIR / "scripts" / "rescan_sonar.sh"), target_repo_dir, component], check=False)
        )
    except (json.JSONDecodeError, RuntimeError):
        return []
    return result.get("new_issues", [])


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
    key = ticket_key or os.environ["JIRA_TICKET_KEY"]
    try:
        jira_client.transition_ticket(key, status)
    except Exception as exc:
        # Best-effort (igual que antes, con check=False) pero YA NO en
        # silencio -- antes un fallo real de Jira (nombre de transicion
        # invalido, red caida) desaparecia sin dejar rastro. Gap real de
        # observabilidad (Prefect): esto usaba el logger de log_utils
        # (stderr crudo del contenedor), invisible en la UI de Prefect --
        # aunque la tarea agota sus 2 reintentos y "completa" igual (best-
        # effort a proposito), get_run_logger() deja el fallo real visible
        # en los logs de ESA tarea puntual en vez de perderse en stderr.
        get_run_logger().warning(f"No se pudo transicionar el ticket {key} a '{status}': {exc}")


@task(retries=2, retry_delay_seconds=5, name="comment-jira")
def comment_jira(text: str, ticket_key: str | None = None):
    key = ticket_key or os.environ["JIRA_TICKET_KEY"]
    try:
        jira_client.post_audit_comment(key, text)
    except Exception as exc:
        get_run_logger().warning(f"No se pudo comentar en el ticket {key}: {exc}")


def _post_child_technical_report(epic_key: str, child_id: str, backend: str | None, resultado: str, extra: dict | None = None) -> None:
    """Comprobante tecnico POR HISTORIA -- a diferencia del resumen fijo
    (que arma orchestration.py con texto propio), esto lo redacta el
    backend LLM que realmente trabajo esa historia (ver
    tech_doc_agent.py). Se llama en cada punto de salida real del loop de
    _deliver_epic_sequential (no-op, bloqueada, o con veredicto), asi cada
    historia que el agente efectivamente toco (no las rechazadas por el
    firewall, que nunca llegaron a un agente) deja su propio comprobante.
    Best-effort: si no se genera nada, no se postea nada extra.
    """
    evidence = {
        "epica": epic_key,
        "historia": child_id,
        "backend_usado": backend or "ninguno",
        "modelo_ollama_coding_agent": os.environ.get("CODING_AGENT_OLLAMA_MODEL") or os.environ.get("OLLAMA_MODEL", ""),
        "modelo_ollama_juez": os.environ.get("JUDGE_OLLAMA_MODEL") or os.environ.get("OLLAMA_MODEL", ""),
        "resultado": resultado,
    }
    if extra:
        evidence.update(extra)
    technical_report = generate_technical_report(evidence)
    if technical_report:
        comment_jira(technical_report, ticket_key=child_id)


def _comment_all(text: str, ticket_id: str, is_epic: bool, child_ticket_keys: list | None) -> None:
    """En modo epica, espeja cada comentario que ya se le hace al ticket
    principal hacia cada hijo tambien -- es una corrida combinada real (un
    solo diff/test/veredicto), asi que la evidencia que reciben los hijos es
    la misma que la epica, no evidencia distinta inventada por hijo.
    """
    comment_jira(text, ticket_key=ticket_id)
    if is_epic:
        for child_key in (child_ticket_keys or []):
            comment_jira(text, ticket_key=child_key)


def _transition_all(status: str, ticket_id: str, is_epic: bool, child_ticket_keys: list | None) -> None:
    """Mismo criterio que _comment_all, para transiciones de estado."""
    transition_jira(status, ticket_key=ticket_id)
    if is_epic:
        for child_key in (child_ticket_keys or []):
            transition_jira(status, ticket_key=child_key)


@task(retries=0, name="push-and-open-pr")
def push_and_open_pr(target_repo_dir: str, branch: str, base_branch: str, ticket_id: str, summary: str, body_text: str) -> dict:
    """Camino B1 (coding agent local) nunca pasaba de un commit local -- code
    review y merge quedaban 100% manuales, sin ningun artefacto reviewable
    en GitHub. Best-effort: si el repo objetivo no tiene un remote real (o
    git/gh fallan), se omite sin bloquear la corrida -- el veredicto del
    juez ya fue OK, la rama sigue ahi intacta para pushear a mano.
    """
    remote_check = subprocess.run(
        ["git", "-C", target_repo_dir, "remote", "get-url", "origin"], capture_output=True, text=True
    )
    remote_url = remote_check.stdout.strip()
    if remote_check.returncode != 0 or not remote_url:
        return {"pushed": False, "pr_url": None, "reason": "el repo objetivo no tiene un remote 'origin' configurado"}

    push_result = subprocess.run(
        ["git", "-C", target_repo_dir, "push", "-u", "origin", branch], capture_output=True, text=True
    )
    if push_result.returncode != 0:
        return {"pushed": False, "pr_url": None, "reason": f"git push fallo: {push_result.stderr.strip()[:300]}"}

    pr_body = (
        f"{body_text}\n\n---\nGenerado automaticamente por poc-ai-agents (orchestration.py) para el ticket "
        f"Jira {ticket_id} -- paso firewall, tests reales, y el agente juez independiente antes de llegar aca."
    )

    # Confirmado real (repo objetivo real de esta sesion): "gh" JAMAS
    # funciona contra Azure DevOps -- antes esto SIEMPRE degradaba a
    # "pushea y abri el PR a mano" para ese caso, dejando la rama pusheada
    # sin ninguna PR real abierta ni adjuntada al ticket, pese a tener
    # AZURE_DEVOPS_PAT real disponible en .env. Abre la PR de verdad via la
    # REST API de Azure DevOps cuando el remote es de ese proveedor.
    azure_match = _AZURE_DEVOPS_REPO_PATTERN.search(remote_url)
    if azure_match:
        pat = os.environ.get("AZURE_DEVOPS_PAT")
        if not pat:
            return {
                "pushed": True, "pr_url": None,
                "reason": "el repo objetivo es Azure DevOps pero AZURE_DEVOPS_PAT no esta seteada -- "
                "la rama ya se pusheo, abri el PR a mano.",
            }
        org, project, repo = azure_match.group("org"), azure_match.group("project"), azure_match.group("repo")
        try:
            pr_resp = httpx.post(
                f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo}/pullrequests",
                params={"api-version": "7.1"},
                json={
                    "sourceRefName": f"refs/heads/{branch}",
                    "targetRefName": f"refs/heads/{base_branch}",
                    "title": summary,
                    "description": pr_body[:4000],  # Azure DevOps trunca descripciones muy largas
                },
                auth=("", pat),
                timeout=15.0,
            )
            pr_resp.raise_for_status()
            pr_id = pr_resp.json()["pullRequestId"]
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            return {"pushed": True, "pr_url": None, "reason": f"Azure DevOps pull request create fallo: {exc}"}
        return {
            "pushed": True,
            "pr_url": f"https://dev.azure.com/{org}/{project}/_git/{repo}/pullrequest/{pr_id}",
            "reason": None,
        }

    # cwd=target_repo_dir (no --repo explicito) para que gh auto-detecte el
    # owner/repo del remote local -- a diferencia de Camino A, que corre
    # desde SCRIPT_DIR y por eso SI necesita GITHUB_REPO.
    try:
        # Bug real confirmado esta sesion: "gh" es especifico de GitHub -- un
        # repo objetivo real en Azure DevOps (u otro remote no-GitHub) ni
        # siquiera tiene el binario "gh" instalado, lo que antes reventaba
        # toda la corrida con FileNotFoundError (subprocess.run no encuentra
        # el ejecutable) DESPUES de que el push YA habia funcionado -- el
        # docstring de esta funcion ya prometia "best-effort, nunca bloquea
        # la corrida" para este caso, pero el codigo solo manejaba "gh corrio
        # y fallo" (returncode!=0), no "gh no existe".
        pr_result = subprocess.run(
            ["gh", "pr", "create", "--title", summary, "--body", pr_body, "--base", base_branch, "--head", branch],
            capture_output=True, text=True, cwd=target_repo_dir,
        )
    except FileNotFoundError:
        return {
            "pushed": True, "pr_url": None,
            "reason": "el CLI 'gh' no esta disponible (repo objetivo no-GitHub, ej. Azure DevOps) -- "
            "la rama ya se pusheo, abri el PR/pull request a mano en tu plataforma real",
        }
    if pr_result.returncode != 0:
        return {"pushed": True, "pr_url": None, "reason": f"gh pr create fallo: {pr_result.stderr.strip()[:300]}"}

    return {"pushed": True, "pr_url": pr_result.stdout.strip(), "reason": None}


def check_copilot_assignable(repo: str, assignee: str) -> str:
    """Repository.suggestedActors (capability CAN_BE_ASSIGNED) -- antes esto
    solo se sabia DESPUES de crear un issue y que la asignacion fallara.
    Devuelve "yes"/"no"/"unknown" -- best-effort total, cualquier fallo de
    la query (auth, red, schema) cae a "unknown", nunca lanza.
    """
    owner, _, name = repo.partition("/")
    query = """
    query($owner: String!, $name: String!) {
      repository(owner: $owner, name: $name) {
        suggestedActors(capabilities: [CAN_BE_ASSIGNED], first: 100) {
          nodes { login }
        }
      }
    }"""
    try:
        result = subprocess.run(
            ["gh", "api", "graphql", "-f", f"query={query}", "-f", f"owner={owner}", "-f", f"name={name}"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            return "unknown"
        data = json.loads(result.stdout)
        logins = {n["login"] for n in data["data"]["repository"]["suggestedActors"]["nodes"]}
    except (json.JSONDecodeError, KeyError, TypeError):
        return "unknown"
    return "yes" if assignee in logins else "no"


@task(retries=1, name="coding-agent-cloud")
def run_coding_agent_cloud(ticket_id: str, summary: str, sanitized_prompt: str) -> dict:
    issue_body = (
        f"{sanitized_prompt}\n\n---\nGenerado automaticamente por poc-ai-agents "
        f"(orchestration.py / Prefect) desde el ticket Jira {ticket_id}."
    )
    # Camino A no tiene diff local -- output_guard.py (que audita el diff de
    # Camino B1) no aplica aca. Se reusa igual la funcion de redaccion de
    # secretos del firewall (no _check_jailbreak, el issue_body es contenido
    # para humanos, no una instruccion a un modelo) como ultima linea antes
    # de publicar en un issue de GitHub, que puede ser publico.
    issue_body, redactions_applied = _redact(issue_body)
    if redactions_applied:
        print(f"output_guard (Camino A): {redactions_applied} redaccion(es) aplicada(s) al issue_body antes de publicarlo.")

    # Diagnostico ANTES de crear el issue -- si "no", probablemente la
    # asignacion de abajo va a fallar; se avisa temprano pero se intenta
    # igual (el chequeo puede tener falsos negativos por permisos del
    # token, y crear-igual-y-reportar-si-falla sigue siendo la red de
    # seguridad real).
    if check_copilot_assignable(GITHUB_REPO, GITHUB_COPILOT_ASSIGNEE) == "no":
        print(
            f"⚠️ '{GITHUB_COPILOT_ASSIGNEE}' no aparece como asignable en {GITHUB_REPO} -- la asignacion de abajo "
            "probablemente falle. Revisa Settings > Copilot > Coding agent en GitHub (requiere plan "
            "Business/Enterprise). Se intenta igual."
        )

    issue_url = _run(["gh", "issue", "create", "--repo", GITHUB_REPO, "--title", summary, "--body", issue_body]).strip()
    assigned = True
    try:
        _run(["gh", "issue", "edit", issue_url, "--add-assignee", GITHUB_COPILOT_ASSIGNEE])
    except RuntimeError:
        assigned = False
    return {"issue_url": issue_url, "assigned": assigned}


_AZURE_DEVOPS_REPO_PATTERN = re.compile(r"dev\.azure\.com/(?P<org>[^/]+)/(?P<project>[^/]+)/_git/(?P<repo>[^/]+)")


def _find_azure_devops_prs_for_branch(target_repo_dir: str, branch: str) -> tuple:
    """Compartido por _check_pr_rejected_for_branch y
    _fetch_unresolved_pr_comments -- ambas necesitan encontrar la(s) PR(s)
    reales de Azure DevOps asociadas a una rama, antes de mirar cosas
    distintas de esa PR (status vs. threads de comentarios). Devuelve
    (remote_info, prs): remote_info es {"org","project","repo","pat"} o
    None si el remote no es Azure DevOps o falta AZURE_DEVOPS_PAT; prs es
    la lista real devuelta por la API (searchCriteria.status=all), o []
    si no hay ninguna o la consulta fallo. Nunca lanza (graceful-degradation,
    mismo criterio que el resto de las funciones de Azure DevOps de esta
    sesion).
    """
    remote_url = subprocess.run(
        ["git", "-C", target_repo_dir, "remote", "get-url", "origin"], capture_output=True, text=True
    ).stdout.strip()
    if not remote_url or "dev.azure.com" not in remote_url:
        return None, []

    match = _AZURE_DEVOPS_REPO_PATTERN.search(remote_url)
    pat = os.environ.get("AZURE_DEVOPS_PAT")
    if not match or not pat:
        return None, []
    org, project, repo = match.group("org"), match.group("project"), match.group("repo")
    remote_info = {"org": org, "project": project, "repo": repo, "pat": pat}

    try:
        resp = httpx.get(
            f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo}/pullrequests",
            params={"searchCriteria.sourceRefName": f"refs/heads/{branch}", "searchCriteria.status": "all", "api-version": "7.1"},
            auth=("", pat),
            timeout=10.0,
        )
        resp.raise_for_status()
        prs = resp.json().get("value", [])
    except (httpx.HTTPError, ValueError):
        prs = []
    return remote_info, prs


def _check_pr_rejected_for_branch(target_repo_dir: str, branch: str) -> bool:
    """Confirmado real (pregunta del usuario): _find_open_branch_for_ticket
    solo mira si la rama es ancestro de base_branch via git -- una PR
    RECHAZADA/cerrada sin mergear deja la rama en el mismo estado git que
    una PR todavia abierta (no es ancestro en ninguno de los dos casos), asi
    que sin esto una rama cuyo trabajo un humano ya rechazo explicitamente
    se reusaria igual que una legitimamente abierta, sin ninguna señal.

    Best-effort, nunca bloquea ni lanza: intenta 'gh pr view' (GitHub) o la
    REST API real de Azure DevOps (AZURE_DEVOPS_PAT, mismo repo objetivo
    real de esta sesion) segun el remote 'origin' detectado -- si ninguno
    aplica o falla (sin CLI/credenciales, red, JSON invalido), devuelve
    False (no se puede confirmar el rechazo, se asume que no fue rechazada
    -- mismo criterio de graceful-degradation que push_and_open_pr).
    """
    remote_url = subprocess.run(
        ["git", "-C", target_repo_dir, "remote", "get-url", "origin"], capture_output=True, text=True
    ).stdout.strip()
    if not remote_url:
        return False

    if "dev.azure.com" in remote_url:
        _remote_info, prs = _find_azure_devops_prs_for_branch(target_repo_dir, branch)
        if not prs:
            return False
        # Si hay al menos una PR todavia activa para esta rama, no se
        # considera rechazada aunque tambien exista una vieja "abandoned".
        if any(pr.get("status") == "active" for pr in prs):
            return False
        return any(pr.get("status") == "abandoned" for pr in prs)

    if shutil.which("gh"):
        result = subprocess.run(
            ["gh", "pr", "view", branch, "--json", "state"], capture_output=True, text=True, cwd=target_repo_dir,
        )
        if result.returncode != 0:
            return False
        try:
            state = json.loads(result.stdout).get("state")
        except json.JSONDecodeError:
            return False
        return state == "CLOSED"  # "MERGED" es un caso distinto, ya cubierto por merge-base

    return False


def _fetch_unresolved_pr_comments(target_repo_dir: str, branch: str) -> list:
    """Confirmado real (usuario): el pipeline abre una PR real pero nunca
    vuelve a consultarla -- un comentario de revision humano real dejado en
    la PR abierta se ignoraba por completo. Busca la PR ACTIVA de esta rama
    (Azure DevOps) y devuelve sus threads de comentarios sin resolver
    (status "active", no generados por el sistema) como
    [{"thread_id": int, "text": "..."}]. Best-effort total: sin PR activa,
    sin AZURE_DEVOPS_PAT, o cualquier error de red/JSON, devuelve [] --
    nunca lanza, nunca bloquea la corrida.
    """
    remote_info, prs = _find_azure_devops_prs_for_branch(target_repo_dir, branch)
    if not remote_info:
        return []
    active_pr = next((pr for pr in prs if pr.get("status") == "active"), None)
    if not active_pr or not active_pr.get("pullRequestId"):
        return []

    org, project, repo, pat = remote_info["org"], remote_info["project"], remote_info["repo"], remote_info["pat"]
    try:
        resp = httpx.get(
            f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo}/pullrequests/"
            f"{active_pr['pullRequestId']}/threads",
            params={"api-version": "7.1"},
            auth=("", pat),
            timeout=10.0,
        )
        resp.raise_for_status()
        threads = resp.json().get("value", [])
    except (httpx.HTTPError, ValueError):
        return []

    unresolved = []
    for thread in threads:
        if thread.get("status") != "active" or thread.get("isDeleted"):
            continue
        real_comments = [
            c.get("content", "") for c in thread.get("comments", []) or []
            if c.get("commentType") != "system" and c.get("content")
        ]
        if real_comments:
            unresolved.append({"thread_id": thread["id"], "text": "\n".join(real_comments)})
    return unresolved


def _build_pr_feedback_section(pr_comments: list) -> str:
    """Arma el bloque de texto que se le agrega al prompt del coding agent
    cuando la rama retomada tiene comentarios de revision humana reales sin
    resolver en la PR abierta -- mismo criterio que ya usa
    _retry_after_no_changes para inyectar el feedback del juez al prompt."""
    lines = "\n".join(f"- {c['text']}" for c in pr_comments)
    return (
        "\n\nAdemas, la PR real abierta para este ticket tiene comentarios de revision "
        f"humana SIN resolver -- atendelos en este cambio:\n{lines}"
    )


def _resolve_pr_threads(target_repo_dir: str, branch: str, thread_ids: list) -> None:
    """Marca como resueltos ("fixed") los threads de la PR real cuya
    referencia se guardo en un turno anterior (ver _fetch_unresolved_pr_comments
    / "pr_thread_ids_to_resolve") -- se llama cuando el coding agent aplico
    un commit nuevo intentando atenderlos. Simplificacion explicita: no hay
    forma barata de confirmar que CADA thread puntual quedo resuelto, asi
    que se marcan todos los que estaban activos al arrancar el turno: si el
    juez despues bloquea el cambio, eso ya queda visible via el
    comentario/status BLOCKED normal, y un humano puede reabrir el thread a
    mano si la correccion fue insuficiente. Best-effort total: nunca lanza.
    """
    if not thread_ids:
        return
    remote_info, prs = _find_azure_devops_prs_for_branch(target_repo_dir, branch)
    if not remote_info:
        return
    active_pr = next((pr for pr in prs if pr.get("status") == "active"), None)
    if not active_pr or not active_pr.get("pullRequestId"):
        return

    org, project, repo, pat = remote_info["org"], remote_info["project"], remote_info["repo"], remote_info["pat"]
    for thread_id in thread_ids:
        try:
            resp = httpx.patch(
                f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo}/pullrequests/"
                f"{active_pr['pullRequestId']}/threads/{thread_id}",
                params={"api-version": "7.1"},
                json={"status": "fixed"},
                auth=("", pat),
                timeout=10.0,
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            # Funcion pure-ish (no es un @task) llamada desde run_coding_agent_local_real
            # (que si es task, ya tiene contexto de Prefect) -- se usa el logger de
            # modulo, no get_run_logger(), mismo criterio que _find_open_branch_for_ticket.
            logger.warning(f"No se pudo marcar como resuelto el thread {thread_id} de la PR real: {exc}")


def _find_open_branch_for_ticket(target_repo_dir: str, ticket_id: str, base_branch: str) -> str | None:
    """Confirmado real (epica KAN-4): antes de este chequeo, cada re-corrida
    de un ticket (ej. un humano revierte manualmente el status en Jira
    pidiendo un redo) creaba una rama copilot/{ticket_id}-{timestamp} NUEVA
    e incondicional -- sin mirar si ya habia una abierta de una corrida
    anterior, dejando ramas huerfanas en paralelo sin ningun vinculo con el
    trabajo/comentarios/veredicto previos. Esto busca una rama real (local o
    remota) para ESE ticket que todavia no este mergeada en base_branch, para
    que la corrida pueda retomarla en vez de empezar de cero. Pure-ish (solo
    lee vía git, no escribe nada) -- devuelve None si no hay ninguna abierta,
    mismo comportamiento que hoy.
    """
    listed = subprocess.run(
        ["git", "-C", target_repo_dir, "branch", "-a", "--list", f"*copilot/{ticket_id}-*"],
        capture_output=True, text=True,
    ).stdout
    candidates = []
    for line in listed.splitlines():
        name = line.strip().lstrip("*").strip()
        if not name or "->" in name:
            continue
        name = name.removeprefix("remotes/origin/")
        if name not in candidates:
            candidates.append(name)

    open_branches = []
    for branch in candidates:
        merged = subprocess.run(
            ["git", "-C", target_repo_dir, "merge-base", "--is-ancestor", branch, base_branch],
            capture_output=True,
        )
        if merged.returncode != 0:  # no es ancestro de base_branch -> todavia no mergeada
            open_branches.append(branch)

    if not open_branches:
        return None

    open_branches.sort(key=lambda b: b.rsplit("-", 1)[-1], reverse=True)
    if len(open_branches) > 1:
        logger.warning(
            f"{ticket_id}: hay {len(open_branches)} ramas abiertas sin mergear -- se retoma la mas reciente "
            f"'{open_branches[0]}', las demas quedan sin tocar (revisar a mano): {open_branches[1:]}"
        )
    return open_branches[0]


@task(retries=0, name="coding-agent-local-fallback")
def run_coding_agent_local(ticket_id: str, sanitized_prompt: str, target_repo_dir: str) -> dict:
    base_branch = subprocess.run(
        ["git", "-C", target_repo_dir, "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True
    ).stdout.strip()

    existing_branch = _find_open_branch_for_ticket(target_repo_dir, ticket_id, base_branch)
    pr_rejected = False
    if existing_branch:
        pr_rejected = _check_pr_rejected_for_branch(target_repo_dir, existing_branch)
        run_logger = get_run_logger()
        run_logger.info(f"{ticket_id}: retomando rama existente '{existing_branch}' en vez de crear una nueva.")
        if pr_rejected:
            run_logger.warning(
                f"{ticket_id}: la rama retomada '{existing_branch}' tiene una PR previa RECHAZADA/cerrada sin "
                "mergear -- se reusa igual, pero revisar si el codigo previo sigue siendo valido."
            )
        branch = existing_branch
        subprocess.run(["git", "-C", target_repo_dir, "checkout", branch], check=True)
    else:
        branch = f"copilot/{ticket_id}-{int(time.time())}"
        subprocess.run(["git", "-C", target_repo_dir, "checkout", "-b", branch], check=True)

    pr_comments = _fetch_unresolved_pr_comments(target_repo_dir, existing_branch) if existing_branch else []
    if pr_comments:
        sanitized_prompt = sanitized_prompt + _build_pr_feedback_section(pr_comments)
    pr_thread_ids_to_resolve = [c["thread_id"] for c in pr_comments]

    suggest = subprocess.run(["gh", "copilot", "suggest", "-t", "shell", sanitized_prompt], cwd=target_repo_dir)
    status = subprocess.run(
        ["git", "-C", target_repo_dir, "status", "--porcelain"], capture_output=True, text=True
    ).stdout

    if suggest.returncode == 0 and status.strip():
        subprocess.run(["git", "-C", target_repo_dir, "add", "-A"], check=True)
        subprocess.run(["git", "-C", target_repo_dir, "commit", "-m", f"Copilot suggestion for {ticket_id}"], check=True)
        return {
            "applied": True, "branch": branch, "base_branch": base_branch, "backend": "gh_copilot_suggest",
            "resumed_branch": bool(existing_branch), "resumed_pr_rejected": pr_rejected,
            "pr_thread_ids_to_resolve": pr_thread_ids_to_resolve,
        }

    subprocess.run(["git", "-C", target_repo_dir, "checkout", base_branch])
    if not existing_branch:
        # Solo se borra una rama recien creada y vacia -- una rama RETOMADA
        # (existing_branch) puede ya traer commits reales de una corrida
        # anterior; borrarla aca destruiria ese trabajo solo porque ESTE
        # intento puntual no sumo nada nuevo.
        subprocess.run(["git", "-C", target_repo_dir, "branch", "-D", branch])
    return {"applied": False, "branch": None, "base_branch": base_branch, "backend": "gh_copilot_suggest"}


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

    existing_branch = _find_open_branch_for_ticket(target_repo_dir, ticket_id, base_branch)
    pr_rejected = False
    if existing_branch:
        pr_rejected = _check_pr_rejected_for_branch(target_repo_dir, existing_branch)
        run_logger = get_run_logger()
        run_logger.info(f"{ticket_id}: retomando rama existente '{existing_branch}' en vez de crear una nueva.")
        if pr_rejected:
            run_logger.warning(
                f"{ticket_id}: la rama retomada '{existing_branch}' tiene una PR previa RECHAZADA/cerrada sin "
                "mergear -- se reusa igual, pero revisar si el codigo previo sigue siendo valido."
            )
        branch = existing_branch
        subprocess.run(["git", "-C", target_repo_dir, "checkout", branch], check=True)
    else:
        branch = f"copilot/{ticket_id}-{int(time.time())}"
        subprocess.run(["git", "-C", target_repo_dir, "checkout", "-b", branch], check=True)

    pr_comments = _fetch_unresolved_pr_comments(target_repo_dir, existing_branch) if existing_branch else []
    if pr_comments:
        get_run_logger().info(f"{ticket_id}: {len(pr_comments)} comentario(s) de revision real sin resolver en la PR abierta -- se le pasan al coding agent.")
        sanitized_prompt = sanitized_prompt + _build_pr_feedback_section(pr_comments)
    pr_thread_ids_to_resolve = [c["thread_id"] for c in pr_comments]

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

    # Gap real de observabilidad (Prefect): esta corrida usa print() en vez
    # de get_run_logger(), asi que nunca aparecia en los logs de tarea de
    # Prefect -- solo en el stdout crudo del contenedor. stderr sigue sin
    # capturarse a proposito (ver comentario arriba del subprocess.run): si
    # lo capturamos ahi perdemos la confirmacion interactiva [s/n] en vivo,
    # que es mas importante que la observabilidad aca. Un returncode
    # distinto de cero SI se loguea -- antes ni eso quedaba registrado.
    run_logger = get_run_logger()
    if result.returncode != 0:
        run_logger.warning(f"coding_agent.py salio con returncode={result.returncode} para {ticket_id} (stderr no capturado, ver confirmacion interactiva en la terminal real).")

    agent_backend = None
    conversation_file = None
    self_review = None
    try:
        agent_result = json.loads(result.stdout)
        run_logger.info(f"Resultado del agente: {agent_result.get('status')} — {agent_result.get('summary')}")
        agent_backend = agent_result.get("_meta", {}).get("backend")
        conversation_file = agent_result.get("_conversation_file")
        self_review = agent_result.get("self_review")
    except json.JSONDecodeError:
        run_logger.warning(f"El agente no devolvio un JSON valido en stdout para {ticket_id}.")

    status = subprocess.run(
        ["git", "-C", target_repo_dir, "status", "--porcelain"], capture_output=True, text=True
    ).stdout

    # Bug real confirmado esta sesion (KAN-15): el modelo puede llamar "git
    # commit" el mismo via run_shell_command, en vez de solo escribir
    # archivos y dejar que ESTE codigo comitee -- en ese caso git status
    # queda LIMPIO (ya esta commiteado), y mirar solo el working tree hacia
    # que un commit real se interpretara como "no aplico nada", BORRANDO la
    # rama con el commit real adentro. Por eso ademas se chequea si HEAD
    # tiene commits reales por encima de base_branch, no solo si el working
    # tree esta sucio.
    commits_ahead = subprocess.run(
        ["git", "-C", target_repo_dir, "rev-list", "--count", f"{base_branch}..HEAD"],
        capture_output=True, text=True,
    ).stdout.strip()
    has_own_commits = commits_ahead.isdigit() and int(commits_ahead) > 0

    if status.strip():
        subprocess.run(["git", "-C", target_repo_dir, "add", "-A"], check=True)
        subprocess.run(["git", "-C", target_repo_dir, "commit", "-m", f"Coding agent change for {ticket_id}"], check=True)
        has_own_commits = True

    if has_own_commits:
        return {
            "applied": True,
            "branch": branch,
            "base_branch": base_branch,
            "backend": agent_backend,
            "conversation_file": conversation_file,
            "self_review": self_review,
            "resumed_branch": bool(existing_branch),
            "resumed_pr_rejected": pr_rejected,
            "pr_thread_ids_to_resolve": pr_thread_ids_to_resolve,
        }

    subprocess.run(["git", "-C", target_repo_dir, "checkout", base_branch])
    if not existing_branch:
        # Misma razon que en run_coding_agent_local: una rama RETOMADA no se
        # borra aca solo porque ESTE intento puntual no sumo commits nuevos
        # -- podria tener trabajo real de una corrida anterior.
        subprocess.run(["git", "-C", target_repo_dir, "branch", "-D", branch])
    # Ya NO se borra el conversation_file aca (antes si) -- _deliver()
    # necesita poder reintentar con el feedback real del juez cuando el
    # primer intento no aplico nada, y para eso necesita continuar la MISMA
    # conversacion (investigacion ya hecha) en vez de repagarla de cero. El
    # llamador que no reintenta simplemente lo deja huerfano en /tmp, mismo
    # criterio que el resto de los conversation_file de esta corrida.
    return {
        "applied": False, "branch": None, "base_branch": base_branch, "backend": agent_backend,
        "conversation_file": conversation_file, "self_review": None,
    }


@task(retries=0, name="coding-agent-local-real-retry")
def retry_coding_agent_local_real(
    ticket_id: str, feedback_text: str, target_repo_dir: str, conversation_file: str | None = None
) -> dict:
    """Segundo pase de Camino B1 tras un veredicto FLAGGED retryable --
    reusa la rama que el primer intento ya dejo checked out (NO crea una
    rama nueva). Si conversation_file esta disponible, CONTINUA la misma
    conversacion (resume_messages/resume_state leidos de ese archivo, que
    se borra despues de usarse) en vez de mandar el ticket completo de
    nuevo -- evita repagar la investigacion ya hecha. Sin conversation_file
    (corrida vieja o el primer intento no la genero), cae al comportamiento
    anterior: manda el prompt original + feedback desde cero.
    """
    payload = {"ticket_id": ticket_id, "target_repo_dir": target_repo_dir}
    if conversation_file and Path(conversation_file).exists():
        conversation_state = json.loads(Path(conversation_file).read_text(encoding="utf-8"))
        payload["sanitized_prompt"] = feedback_text
        payload["resume_messages"] = conversation_state.get("messages", [])
        payload["resume_state"] = {
            "has_investigated": conversation_state.get("has_investigated", False),
            "has_run_verification": conversation_state.get("has_run_verification", False),
            "initial_plan": conversation_state.get("initial_plan"),
        }
        Path(conversation_file).unlink(missing_ok=True)
    else:
        payload["sanitized_prompt"] = feedback_text

    # Checkpoint de HEAD ANTES del turno -- mismo motivo que
    # run_coding_agent_local_real: el modelo puede commitear el mismo via
    # run_shell_command, dejando git status limpio aunque haya un commit
    # real nuevo. Aca no hay un base_branch fijo (se reusa la rama del
    # primer intento), asi que el checkpoint es el HEAD real justo antes de
    # este turno puntual.
    checkpoint = subprocess.run(
        ["git", "-C", target_repo_dir, "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()

    payload_file = SCRIPT_DIR / "logs" / f".coding_agent_retry_payload_{ticket_id}_{int(time.time())}.json"
    payload_file.parent.mkdir(parents=True, exist_ok=True)
    payload_file.write_text(json.dumps(payload), encoding="utf-8")
    try:
        result = subprocess.run(
            ["python3", str(SCRIPT_DIR / "coding_agent.py"), str(payload_file)],
            stdout=subprocess.PIPE,
            text=True,
            cwd=target_repo_dir,
        )
    finally:
        payload_file.unlink(missing_ok=True)

    run_logger = get_run_logger()
    if result.returncode != 0:
        run_logger.warning(f"coding_agent.py (segundo intento) salio con returncode={result.returncode} para {ticket_id}.")

    backend = None
    retry_conversation_file = None
    self_review = None
    try:
        agent_result = json.loads(result.stdout)
        run_logger.info(f"Resultado del segundo intento: {agent_result.get('status')} — {agent_result.get('summary')}")
        backend = agent_result.get("_meta", {}).get("backend")
        retry_conversation_file = agent_result.get("_conversation_file")
        self_review = agent_result.get("self_review")
    except json.JSONDecodeError:
        run_logger.warning(f"El agente no devolvio un JSON valido en stdout en el segundo intento para {ticket_id}.")

    status = subprocess.run(
        ["git", "-C", target_repo_dir, "status", "--porcelain"], capture_output=True, text=True
    ).stdout

    head_now = subprocess.run(
        ["git", "-C", target_repo_dir, "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    has_own_commits = bool(checkpoint) and head_now != checkpoint

    if not status.strip() and not has_own_commits:
        return {"applied": False, "backend": backend, "self_review": self_review, "conversation_file": retry_conversation_file}

    if status.strip():
        subprocess.run(["git", "-C", target_repo_dir, "add", "-A"], check=True)
        subprocess.run(
            ["git", "-C", target_repo_dir, "commit", "-m", f"Coding agent retry for {ticket_id} (feedback del juez)"], check=True
        )
    # Ya no se borra aca -- el llamador decide: _retry_local_diff es siempre
    # el ultimo intento de esa cadena y limpia el archivo el mismo; el modo
    # epica secuencial (_deliver_epic_sequential) en cambio necesita este
    # conversation_file intacto para pasarselo como continuacion al proximo
    # hijo de la epica.
    return {"applied": True, "backend": backend, "self_review": self_review, "conversation_file": retry_conversation_file}


@task(retries=0, name="output-guard")
def run_output_guard(diff_text: str, jira_context: dict) -> dict:
    """El AI Firewall (firewall_proxy.py) audita el prompt que ENTRA al
    coding agent, pero nadie volvia a auditar el diff que efectivamente
    produce. Corre las MISMAS reglas (output_guard.py, que reusa
    firewall_proxy._redact()/_check_jailbreak() directo) sobre el diff real,
    antes del testing agent -- si encuentra algo, es tan serio como un test
    fallido: bloquea, y el juez ni se llama.
    """
    return output_guard.scan_diff(diff_text, jira_context)


@task(retries=0, name="testing-agent")
def run_tests(target_repo_dir: str) -> dict:
    if shutil.which("docker") is None:
        # Gap real de observabilidad (Prefect): esto devuelve passed=True
        # sin correr NINGUN test real -- Prefect mostraba esta tarea como
        # "Completed" identica a una corrida real que si paso, sin forma de
        # distinguir un salto silencioso de una validacion real.
        get_run_logger().warning(f"testing-agent omitido para {target_repo_dir}: docker no disponible en el host -- NO se corrio ningun test real.")
        return {"passed": True, "output": "(testing agent omitido: docker no disponible en el host)"}

    # Docker-outside-of-Docker: cuando orchestration.py corre DENTRO de un
    # contenedor (poc-ai-agents-testrunner, con /var/run/docker.sock
    # montado), el docker run ANIDADO que dispara run_module_tests.sh lo
    # ejecuta el daemon real del HOST -- que no puede montar target_repo_dir
    # (un path que solo existe DENTRO de este contenedor). HOST_TARGET_REPO_DIR
    # (opcional, seteada solo en corridas via Docker-outside-of-Docker) le
    # pasa el path real y visible para el host, para el -v del docker run
    # anidado. Sin ella (host real, no DooD), cae al mismo target_repo_dir.
    host_target_repo_dir = os.environ.get("HOST_TARGET_REPO_DIR", target_repo_dir)
    result = subprocess.run(
        ["bash", str(SCRIPT_DIR / "scripts" / "run_module_tests.sh"), target_repo_dir, host_target_repo_dir],
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    if result.returncode != 0:
        # Gap real de observabilidad (Prefect): la salida real de
        # run_module_tests.sh (que test/lint fallo, con que error) solo
        # viajaba dentro del dict que se le pasa al juez -- si los tests
        # fallan, el juez ni se llama (PipelineBlocked), asi que esa salida
        # real quedaba completamente invisible en Prefect Y en Jira.
        get_run_logger().warning(f"testing-agent: los tests reales fallaron para {target_repo_dir}:\n{output[-4000:]}")
    return {"passed": result.returncode == 0, "output": output}


@task(retries=1, retry_delay_seconds=5, name="fetch-epic")
def fetch_epic(epic_key: str, known_components: list | None = None) -> dict:
    return jira_client.fetch_epic_with_children(epic_key, known_repos=set(known_components) if known_components else None)


@task(retries=0, name="plan-epic")
def plan_epic_task(epic: dict, children: list) -> dict:
    """Reordena las historias hijas por dependencia real en vez del orden
    mecanico que devolvio el JQL (epic_planner.py, best-effort: si no hay
    backend disponible o falla, epic_planner.plan_epic ya cae sola al orden
    mecanico original -- nunca bloquea el modo epica).
    """
    return asyncio.run(epic_planner.plan_epic(epic, children))


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


def _check_not_epic(ticket: dict) -> None:
    """Pure: raises PipelineBlocked si el ticket (modo normal, NO --epic) es
    en realidad una Epica -- fetch_ticket_live() no le resuelve hijos, asi
    que procesarla como ticket normal la trata como una story vacia. Antes
    de esto no habia forma de que el pipeline lo supiera (issuetype nunca se
    pedia a la API de Jira).
    """
    if (ticket.get("issue_type") or "").lower() == "epic":
        raise PipelineBlocked(
            f"{ticket['ticket_id']} es una Epica -- correla con --epic {ticket['ticket_id']} en vez de "
            "como ticket normal, o el pipeline no le va a resolver los hijos."
        )


def _format_conflicts_section(conflicts: list) -> str:
    """Pure: convierte la lista de conflictos que epic_planner.py detecto
    (antes se calculaba y se descartaba sin que nadie los viera) en la
    seccion de texto que ve el coding agent en el prompt de la epica.
    "" si no hay conflictos -- no agrega una seccion vacia al prompt.
    """
    if not conflicts:
        return ""
    return "\n--- Conflictos detectados por el planificador ---\n" + "\n".join(f"- {c}" for c in conflicts) + "\n"


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
    query = f"MATCH (n:Service) WHERE n.name IN [{quoted}] RETURN n.name + '|' + coalesce(n.repo_url, '') AS row"
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
def run_judge(
    ticket: dict, firewall_result: dict, change_source: str, change_description: str, test_summary: str,
    self_review: dict | None = None, falco_since: str | None = None, conflicts: list | None = None,
    new_sonar_issues: list | None = None,
) -> dict:
    # Se busca Y se postea Falco ANTES de invocar al juez (no despues, como
    # hacia antes check_falco_correlation() llamado por el caller) -- asi la
    # evidencia de runtime real de ESTA corrida puede llegar al payload del
    # juez que la audita, en vez de solo a un comentario de Jira que un
    # humano lee despues de que el veredicto ya se decidio.
    falco_summary = None
    if falco_since:
        falco_summary = get_falco_summary(falco_since)
        if falco_summary:
            check_falco_correlation(falco_since, ticket.get("ticket_id", "UNKNOWN"), summary=falco_summary)

    payload = {
        "ticket": ticket,
        "firewall": firewall_result,
        "change_source": change_source,
        "change_description": change_description,
        "test_summary": test_summary,
        "self_review": self_review,
        "falco_summary": falco_summary,
        "conflicts": conflicts,
        "new_sonar_issues": new_sonar_issues,
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
    # Gap real de observabilidad (Prefect): con returncode==0, result.stderr
    # se descartaba siempre en silencio -- ahi es donde judge_agent.py loguea
    # cosas como "juez: usando backend 'X'", warnings de modelo no
    # descargado, o errores de tool-calls individuales que el loop ya
    # atrapo y siguio (ej. un CypherSyntaxError real de una query mal
    # armada) -- info real que nunca llegaba a los logs de Prefect.
    if result.stderr.strip():
        get_run_logger().info(f"judge_agent.py stderr:\n{result.stderr.strip()[-4000:]}")
    return json.loads(result.stdout)


@task(retries=0, name="falco-summary")
def get_falco_summary(since_iso: str) -> dict | None:
    """Pure fetch (no posting anywhere): runs scripts/check_falco_alerts.py
    and returns the parsed summary ({"count":..., "alerts":[...]}), or None
    if there's nothing to report or the script failed. check_falco_correlation()
    and run_judge() both build on this so the same window is never queried
    twice for the same run.
    """
    result = subprocess.run(
        ["python3", str(SCRIPT_DIR / "scripts" / "check_falco_alerts.py"), since_iso, str(SCRIPT_DIR / "logs" / "falco_alerts.jsonl")],
        capture_output=True,
        text=True,
    )
    # Gap real de observabilidad (Prefect): result.stderr se descartaba
    # SIEMPRE, incluso cuando el script realmente fallaba (returncode!=0) --
    # esta tarea nunca lanzaba, asi que Prefect la mostraba "Completed" sin
    # ningun rastro de que el chequeo de Falco ni siquiera corrio.
    if result.returncode != 0:
        get_run_logger().warning(f"check_falco_alerts.py fallo (returncode={result.returncode}): {result.stderr.strip()[-2000:]}")
        return None
    if not result.stdout.strip():
        return None
    try:
        summary = json.loads(result.stdout)
    except json.JSONDecodeError:
        get_run_logger().warning(f"check_falco_alerts.py no devolvio JSON valido: {result.stdout.strip()[:500]!r}")
        return None
    if not summary.get("count"):
        return None
    return summary


@task(retries=0, name="falco-correlation")
def check_falco_correlation(since_iso: str, ticket_id: str, summary: dict | None = None):
    """Correlates logs/falco_alerts.jsonl (Falco already writes these in real
    time, see falco/custom_rules.yaml) with this run's time window. Advisory
    only -- never blocks the flow, just surfaces what Falco saw via a Jira
    comment and, if configured, a webhook POST. If `summary` isn't passed
    (already fetched by a caller, e.g. run_judge()), fetches it here.
    """
    if summary is None:
        summary = get_falco_summary(since_iso)
    if not summary:
        return

    count = summary.get("count", 0)
    alert_lines = " ".join(f"- [{a['priority']}] {a['rule']}: {a['output']}" for a in summary["alerts"])
    comment_jira(
        f"🚨 Falco (monitoreo a nivel de sistema, automatizado, Prefect): se detectaron {count} alerta(s) durante esta corrida — {alert_lines}",
        ticket_key=ticket_id,
    )

    post_alert_webhook(f"🚨 Falco detecto {count} alerta(s) en la corrida de {ticket_id}: {alert_lines}")


@task(retries=1, name="record-run-in-graph")
def record_run_in_graph(payload: dict) -> dict:
    """Writes this run into the Neo4j knowledge graph (graph_writer.py) --
    Story/Epic, Run, one Decision per stage, and a Risk node if the judge
    cited a real policy_reference. Best-effort like graph_writer.record_run()
    itself: it never raises for a connection failure, only for a malformed
    payload (a real bug worth surfacing), so this task failing means a
    programming error here, not a Neo4j hiccup.
    """
    return graph_writer.record_run(payload)


def _build_graph_payload(
    ticket_id: str,
    summary: str,
    components: list,
    firewall_result: dict,
    tests_status: str,
    tests_reason: str | None,
    judge_verdict: dict | None,
    branch: str | None,
    backend: str | None,
    is_epic: bool = False,
    child_ticket_keys: list | None = None,
    output_guard_status: str = "SKIPPED",
    output_guard_reason: str | None = None,
) -> dict:
    judge_status = "SKIPPED"
    judge_reason = None
    policy_reference = None
    if judge_verdict is not None:
        judge_status = judge_verdict.get("verdict", "SKIPPED")
        judge_reason = judge_verdict.get("reasoning")
        policy_reference = judge_verdict.get("policy_reference")

    return {
        "run_id": f"{ticket_id}-{int(time.time())}",
        "ticket_key": ticket_id,
        "ticket_summary": summary,
        "is_epic": is_epic,
        "child_ticket_keys": child_ticket_keys or [],
        "components": components,
        "branch": branch,
        "backend": backend,
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "decisions": [
            {"stage": "firewall", "status": firewall_result.get("status"), "reason": firewall_result.get("reason"), "policy_reference": None},
            {"stage": "output_guard", "status": output_guard_status, "reason": output_guard_reason, "policy_reference": None},
            {"stage": "tests", "status": tests_status, "reason": tests_reason, "policy_reference": None},
            {"stage": "judge", "status": judge_status, "reason": judge_reason, "policy_reference": policy_reference},
        ],
    }


def _post_judge_verdict_artifact(jira_context: dict, verdict: dict) -> None:
    """Gap real de observabilidad (Prefect): el veredicto completo del juez
    (reasoning, policy_reference, los *_assessment) solo vivia en el dict
    que devuelve la tarea -- nunca aparecia en la UI de Prefect como algo
    navegable, solo si se lee el resultado crudo de la tarea. Un artifact
    markdown lo deja ahi mismo, ligado a esta corrida especifica -- mismo
    punto de entrada para TODOS los caminos que llaman al juez
    (_run_judge_safe es compartido). Best-effort: nunca bloquea la corrida
    real si Prefect no puede crear el artifact.
    """
    ticket_id = str(jira_context.get("ticket_id") or "desconocido")
    try:
        body = (
            f"**Ticket:** {ticket_id}\n\n"
            f"**Veredicto:** {verdict.get('verdict')}\n\n"
            f"**Policy reference:** {verdict.get('policy_reference')}\n\n"
            f"**Firewall assessment:** {verdict.get('firewall_assessment')}\n\n"
            f"**Change assessment:** {verdict.get('change_assessment')}\n\n"
            f"**Reasoning:**\n\n{verdict.get('reasoning')}"
        )
        safe_key = re.sub(r"[^a-z0-9-]", "-", ticket_id.lower()).strip("-") or "ticket"
        create_markdown_artifact(
            key=f"judge-verdict-{safe_key}",
            markdown=body,
            description=f"Veredicto real del juez para {ticket_id}",
        )
    except Exception as exc:
        get_run_logger().warning(f"No se pudo crear el artifact del veredicto del juez para {ticket_id}: {exc}")


def _run_judge_safe(*args, **kwargs) -> dict | None:
    """Same tolerance as run_poc_loop.sh: if the judge can't run at all (no
    ANTHROPIC_API_KEY, no reachable Ollama, network failure), the pipeline
    continues without a verdict instead of failing the whole flow.
    """
    try:
        verdict = run_judge(*args, **kwargs)
    except Exception as exc:
        get_run_logger().warning(f"El juez no pudo evaluar esta corrida (revisa ANTHROPIC_API_KEY, Ollama local, o conectividad): {exc}")
        return None
    jira_context = args[0] if args else {}
    _post_judge_verdict_artifact(jira_context, verdict)
    return verdict


def _evaluate_new_diff_after_retry(
    ticket_id: str,
    branch: str,
    base_branch: str,
    backend: str | None,
    self_review: dict | None,
    target_repo_dir: str,
    jira_context: dict,
    firewall_result: dict,
    components: list,
    summary: str,
    is_epic: bool,
    child_ticket_keys: list | None,
    falco_since: str,
    label: str,
    conflicts: list | None = None,
) -> dict | None:
    """Guardia de salida + tests + juez sobre un diff nuevo producido por un
    reintento -- comun a _retry_local_diff (reintento tras FLAGGED con un
    diff ya aplicado) y _retry_after_no_changes (reintento tras "el agente
    no aplico nada"). label identifica el intento en los mensajes de Jira
    (ej. "segundo intento") para que quede claro cual paso fallo.
    """
    diff_text = _run(["git", "-C", target_repo_dir, "diff", f"{base_branch}..{branch}"])

    guard_result = run_output_guard(diff_text, jira_context)
    if not guard_result["clean"]:
        reason = f"redacciones={guard_result['redactions_applied']}, jailbreak={guard_result['jailbreak_reason']}"
        _comment_all(
            f"🛡️ Guardia de salida (Prefect): el diff del {label} contiene evidencia real de fuga de "
            f"datos o manipulacion ({reason}). Bloqueado antes del testing agent.",
            ticket_id, is_epic, child_ticket_keys,
        )
        post_alert_webhook(f"🛡️ Guardia de salida BLOCKED en {ticket_id} ({label}): {reason}")
        _transition_all(JIRA_BLOCKED_STATUS, ticket_id, is_epic, child_ticket_keys)
        check_falco_correlation(falco_since, ticket_id)
        record_run_in_graph(
            _build_graph_payload(
                ticket_id, summary, components, firewall_result,
                tests_status="SKIPPED", tests_reason=None,
                judge_verdict=None, branch=branch, backend=backend,
                is_epic=is_epic, child_ticket_keys=child_ticket_keys,
                output_guard_status="FAILED", output_guard_reason=reason,
            )
        )
        raise PipelineBlocked(f"guardia de salida bloqueo el {label}: {reason}")

    test_result = run_tests(target_repo_dir)
    if not test_result["passed"]:
        _comment_all(f"🧪 Testing agent (Prefect): los tests reales FALLARON en el {label} de '{branch}'.", ticket_id, is_epic, child_ticket_keys)
        post_alert_webhook(f"🧪 Testing agent BLOCKED en {ticket_id} ({label}): los tests reales fallaron en '{branch}'.")
        _transition_all(JIRA_BLOCKED_STATUS, ticket_id, is_epic, child_ticket_keys)
        check_falco_correlation(falco_since, ticket_id)
        record_run_in_graph(
            _build_graph_payload(
                ticket_id, summary, components, firewall_result,
                tests_status="FAILED", tests_reason=f"el test suite real fallo ({label})",
                judge_verdict=None, branch=branch, backend=backend,
                is_epic=is_epic, child_ticket_keys=child_ticket_keys,
            )
        )
        raise PipelineBlocked(f"tests reales fallaron en el {label}")

    new_sonar_issues = rescan_sonar(target_repo_dir, components[0]) if components else []

    return _run_judge_safe(
        jira_context, firewall_result, "local_diff", diff_text, test_result["output"],
        self_review=self_review, falco_since=falco_since, conflicts=conflicts,
        new_sonar_issues=new_sonar_issues,
    )


INADEQUATE_TESTS_FEEDBACK = (
    "Tu propia autocritica (self_review.tests_adequate) al terminar el intento anterior "
    "fue 'false' -- vos mismo marcaste que lo que corriste no cubre genuinamente este "
    "cambio. Antes de terminar de nuevo, agregá un test real (unitario o E2E, el que "
    "corresponda al stack real del sub-proyecto que tocaste) que cubra el comportamiento "
    "nuevo que introdujiste -- correr solo la suite existente sin sumar cobertura no "
    "alcanza, aunque esa suite pase entera."
)


def _retry_for_inadequate_tests(ticket_id: str, target_repo_dir: str, agent_result: dict) -> dict:
    """Confirmado real (KAN-15, KAN-5): el coding agent se autoevalua con
    honestidad (self_review.tests_adequate=False) pero termina igual con
    status "done" sin agregar ningun test nuevo -- y el juez, en corridas
    reales, repetidas veces lo paso por alto ("tests_adequate era false
    pero la funcionalidad igual sirve", visto en vivo en KAN-5). En vez de
    confiar solo en que el juez lo note, se le da al coding agent un turno
    mas, real, ANTES de correr tests/juez -- mismo mecanismo de
    retry_coding_agent_local_real ya usado para el feedback de PR/juez. Si
    el reintento no aplica nada nuevo, se sigue con el agent_result
    original (nunca bloquea la corrida solo por esto).
    """
    retried = retry_coding_agent_local_real(
        ticket_id, INADEQUATE_TESTS_FEEDBACK, target_repo_dir, conversation_file=agent_result.get("conversation_file")
    )
    if not retried.get("applied"):
        return agent_result
    # retry_coding_agent_local_real() no devuelve "branch"/"base_branch"
    # (reusa la rama que el primer intento ya dejo checked out, nunca crea
    # una nueva) -- hay que preservarlos del agent_result original o el
    # resto de _deliver/_deliver_epic_sequential pierde la referencia a la
    # rama real donde esta el commit nuevo.
    merged = dict(agent_result)
    merged.update(retried)
    return merged


def _retry_local_diff(
    ticket_id: str,
    sanitized: str,
    target_repo_dir: str,
    agent_result: dict,
    judge_verdict: dict,
    jira_context: dict,
    firewall_result: dict,
    components: list,
    summary: str,
    is_epic: bool,
    child_ticket_keys: list | None,
    falco_since: str,
    conflicts: list | None = None,
) -> dict | None:
    """Le da al coding agent un segundo (y ultimo) intento cuando el primer
    veredicto fue FLAGGED con un policy_reference retryable -- reusa la
    misma rama, le agrega el feedback del juez al prompt, y si produce
    cambios nuevos vuelve a correr tests + juez. Devuelve el veredicto
    nuevo (final) si hubo un segundo intento con resultado, o None si no
    hubo cambios nuevos (el llamador se queda con el veredicto original).
    Si los tests fallan en el reintento, bloquea directo (mismo criterio
    que el primer intento) en vez de devolver un veredicto.
    """
    reasoning = judge_verdict.get("reasoning", "")
    feedback_text = f"--- FEEDBACK DEL JUEZ (corregir antes de continuar) ---\n{reasoning}"
    # Si no hay conversation_file, retry_coding_agent_local_real() cae sola
    # a mandar sanitized + feedback desde cero (compatibilidad con corridas
    # que no lo generaron).
    if not agent_result.get("conversation_file"):
        feedback_text = f"{sanitized}\n\n{feedback_text}"

    retry_result = retry_coding_agent_local_real(
        ticket_id, feedback_text, target_repo_dir, conversation_file=agent_result.get("conversation_file")
    )
    # _retry_local_diff es siempre el ultimo intento de esta cadena (no hay
    # un tercero) -- a diferencia del modo epica secuencial, nadie mas va a
    # continuar esta conversacion, se limpia apenas se consume.
    if retry_result.get("conversation_file"):
        Path(retry_result["conversation_file"]).unlink(missing_ok=True)
    if not retry_result["applied"]:
        print("El segundo intento no produjo cambios nuevos -- se mantiene el veredicto FLAGGED original.")
        return None

    branch = agent_result["branch"]
    return _evaluate_new_diff_after_retry(
        ticket_id, branch, agent_result["base_branch"], retry_result.get("backend"), retry_result.get("self_review"),
        target_repo_dir, jira_context, firewall_result, components, summary, is_epic, child_ticket_keys, falco_since,
        "segundo intento", conflicts=conflicts,
    )


def _retry_after_no_changes(
    ticket_id: str,
    sanitized: str,
    target_repo_dir: str,
    agent_result: dict,
    judge_verdict: dict,
    jira_context: dict,
    firewall_result: dict,
    components: list,
    summary: str,
    is_epic: bool,
    child_ticket_keys: list | None,
    falco_since: str,
    conflicts: list | None = None,
) -> dict | None:
    """Mismo espiritu que _retry_local_diff, pero para cuando el PRIMER
    intento no aplico NINGUN cambio real (agent_result["applied"] es
    False) -- antes, un veredicto FLAGGED en ese caso (ej. el juez
    sugiriendo una accion concreta como "implementa las paginas de error")
    se comentaba en Jira y ahi quedaba, sin que esa guia volviera nunca al
    coding agent (confirmado real esta sesion contra KAN-15). Reintenta
    SIEMPRE que el juez marco FLAGGED aca -- a diferencia de
    _retry_local_diff, no se gatea por RETRYABLE_POLICY_REFERENCES (esos
    policy_reference estan pensados para evaluar un diff real; aca todavia
    no hay ningun diff, asi que no aplican -- confirmado real: el juez le
    asigna "other"). Como el primer intento no dejo ninguna rama viva (se
    borro), este reintento crea una rama nueva antes de continuar la
    conversacion.
    """
    reasoning = judge_verdict.get("reasoning", "")
    feedback_text = (
        f"{sanitized}\n\n--- FEEDBACK DEL JUEZ (tu intento anterior NO aplico ningun cambio real -- "
        f"actua sobre este feedback en vez de volver a rendirte) ---\n{reasoning}"
    )

    base_branch = agent_result.get("base_branch") or "main"
    branch = f"copilot/{ticket_id}-{int(time.time())}"
    subprocess.run(["git", "-C", target_repo_dir, "checkout", "-b", branch], check=True)

    retry_result = retry_coding_agent_local_real(
        ticket_id, feedback_text, target_repo_dir, conversation_file=agent_result.get("conversation_file")
    )
    if retry_result.get("conversation_file"):
        Path(retry_result["conversation_file"]).unlink(missing_ok=True)

    if not retry_result["applied"]:
        print("El segundo intento tampoco aplico ningun cambio -- se mantiene el veredicto FLAGGED original.")
        subprocess.run(["git", "-C", target_repo_dir, "checkout", base_branch])
        subprocess.run(["git", "-C", target_repo_dir, "branch", "-D", branch])
        return None

    _comment_all(
        f"🤖 Copilot (Prefect): segundo intento con el feedback del juez -- esta vez SI aplico un cambio "
        f"en la rama '{branch}' de {target_repo_dir}, pendiente de revision humana.",
        ticket_id, is_epic, child_ticket_keys,
    )
    # Mutamos agent_result (dict, pasado por referencia) para que _deliver()
    # vea la rama nueva -- a diferencia de _retry_local_diff, aca la rama no
    # existia todavia cuando el llamador arranco (el primer intento no
    # aplico nada, no dejo ninguna rama viva), asi que no hay otra forma de
    # que _deliver() se entere sin este side-effect explicito.
    agent_result["branch"] = branch
    agent_result["applied"] = True
    agent_result["backend"] = retry_result.get("backend") or agent_result.get("backend")
    return _evaluate_new_diff_after_retry(
        ticket_id, branch, base_branch, retry_result.get("backend"), retry_result.get("self_review"),
        target_repo_dir, jira_context, firewall_result, components, summary, is_epic, child_ticket_keys, falco_since,
        "segundo intento (tras no aplicar nada)", conflicts=conflicts,
    )


def _handle_rejected(
    ticket_id: str, jira_context: dict, firewall_result: dict, is_epic: bool = False, child_ticket_keys: list | None = None
):
    """Shared by ticket mode and epic mode: the firewall said REJECTED, the
    judge gets a chance to flag a possible false positive (advisory only,
    never overrides the firewall), then the flow stops.
    """
    _comment_all(f"🛡️ AI Firewall (Prefect): RECHAZADA. Motivo: {firewall_result['reason']}.", ticket_id, is_epic, child_ticket_keys)

    judge_verdict = _run_judge_safe(
        jira_context, firewall_result, "firewall_rejected", firewall_result["reason"],
        "sin tests corridos para esta corrida",
    )
    if judge_verdict is not None:
        if judge_verdict["verdict"] == "FLAGGED":
            _comment_all(
                f"🧑‍⚖️ Agente juez (Prefect): el rechazo del firewall podria ser incorrecto — "
                f"{judge_verdict['reasoning']} La solicitud SIGUE RECHAZADA (el juez no puede "
                "revertir al firewall); revision humana recomendada.",
                ticket_id, is_epic, child_ticket_keys,
            )
        else:
            _comment_all(
                f"🧑‍⚖️ Agente juez (Prefect): OK, el rechazo del firewall fue correcto. {judge_verdict['reasoning']}",
                ticket_id, is_epic, child_ticket_keys,
            )

    # Antes esto no transicionaba nada -- un ticket rechazado se quedaba
    # donde estaba, sin senal visible en Jira de que el pipeline ya lo
    # proceso y lo freno.
    _transition_all(JIRA_BLOCKED_STATUS, ticket_id, is_epic, child_ticket_keys)

    record_run_in_graph(
        _build_graph_payload(
            ticket_id,
            jira_context.get("summary", ""),
            (jira_context.get("repository_origen") or "").split(","),
            firewall_result,
            tests_status="SKIPPED",
            tests_reason=None,
            judge_verdict=judge_verdict,
            branch=None,
            backend=None,
            is_epic=is_epic,
            child_ticket_keys=child_ticket_keys,
        )
    )

    raise PipelineBlocked(firewall_result["reason"])


def _deliver(
    ticket_id: str,
    summary: str,
    firewall_result: dict,
    jira_context: dict,
    target_repo_dir: str,
    is_epic: bool = False,
    child_ticket_keys: list | None = None,
    conflicts: list | None = None,
) -> dict:
    """Etapa 5 en adelante: coding agent (nube o fallback local), testing
    agent, juez, correlacion de Falco. Compartido por ticket mode y epic
    mode -- ambos llegan aca con el firewall ya en APPROVED y jira_context
    armado a su manera (un ticket, o una epica + sus hijos combinados).
    conflicts (solo modo epica): lo que epic_planner.py detecto entre
    historias hijas -- se le manda al juez ademas de al coding agent, para
    que verifique si el diff real los tuvo en cuenta.
    """
    sanitized = firewall_result["sanitized_prompt"]
    _transition_all(JIRA_IN_PROGRESS_STATUS, ticket_id, is_epic, child_ticket_keys)
    falco_since = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    components = (jira_context.get("repository_origen") or "").split(",")

    if GITHUB_REPO:
        agent_result = run_coding_agent_cloud(ticket_id, summary, sanitized)
        _comment_all(
            f"🤖 Coding agent (Prefect): issue {agent_result['issue_url']} creado, "
            f"asignado={agent_result['assigned']}. El agente trabaja en la nube y abrira un PR.",
            ticket_id, is_epic, child_ticket_keys,
        )
        judge_verdict = _run_judge_safe(
            jira_context, firewall_result, "issue_only", sanitized,
            "sin tests (coding agent en la nube, el PR aun no existe en el momento de esta corrida)",
            falco_since=falco_since, conflicts=conflicts,
        )
        tests_status, tests_reason, branch, backend = "SKIPPED", None, None, "cloud"
    else:
        if _local_coding_agent_backend_available():
            agent_result = run_coding_agent_local_real(ticket_id, sanitized, target_repo_dir)
        else:
            agent_result = run_coding_agent_local(ticket_id, sanitized, target_repo_dir)

        branch = agent_result.get("branch")
        backend = agent_result.get("backend")

        if agent_result["applied"]:
            if (agent_result.get("self_review") or {}).get("tests_adequate") is False:
                _comment_all(
                    "🧪 El coding agent se autoevaluo con tests_adequate=false -- dandole un turno mas "
                    "para agregar un test real antes de correr el testing agent.",
                    ticket_id, is_epic, child_ticket_keys,
                )
                agent_result = _retry_for_inadequate_tests(ticket_id, target_repo_dir, agent_result)
                backend = agent_result.get("backend") or backend

            resumed_note = ""
            if agent_result.get("resumed_branch"):
                resumed_note = " (retoma una rama existente de una corrida anterior de este ticket, no empieza de cero)"
                if agent_result.get("resumed_pr_rejected"):
                    resumed_note += " -- OJO: la PR anterior de esta rama fue RECHAZADA/cerrada sin mergear, revisar si el codigo previo sigue siendo valido"
            _comment_all(
                f"🤖 Copilot (Prefect): AI Firewall aprobo la solicitud (redacciones: {firewall_result['redactions_applied']}). "
                f"Copilot aplico un cambio en la rama '{agent_result['branch']}' de {target_repo_dir}{resumed_note}, "
                f"pendiente de revision humana (no en '{agent_result['base_branch']}').",
                ticket_id, is_epic, child_ticket_keys,
            )
            pr_thread_ids_to_resolve = agent_result.get("pr_thread_ids_to_resolve")
            if pr_thread_ids_to_resolve:
                _resolve_pr_threads(target_repo_dir, agent_result["branch"], pr_thread_ids_to_resolve)
                _comment_all(
                    f"💬 Se intento atender {len(pr_thread_ids_to_resolve)} comentario(s) de revision real de la PR "
                    "abierta en este mismo commit -- revisar si la correccion fue suficiente.",
                    ticket_id, is_epic, child_ticket_keys,
                )
            diff_text = _run([
                "git", "-C", target_repo_dir, "diff", f"{agent_result['base_branch']}..{agent_result['branch']}"
            ])

            guard_result = run_output_guard(diff_text, jira_context)
            if not guard_result["clean"]:
                reason = f"redacciones={guard_result['redactions_applied']}, jailbreak={guard_result['jailbreak_reason']}"
                _comment_all(
                    f"🛡️ Guardia de salida (Prefect): el diff generado por el coding agent contiene evidencia "
                    f"real de fuga de datos o manipulacion ({reason}). Bloqueado antes del testing agent.",
                    ticket_id, is_epic, child_ticket_keys,
                )
                post_alert_webhook(f"🛡️ Guardia de salida BLOCKED en {ticket_id}: {reason}")
                _transition_all(JIRA_BLOCKED_STATUS, ticket_id, is_epic, child_ticket_keys)
                check_falco_correlation(falco_since, ticket_id)
                record_run_in_graph(
                    _build_graph_payload(
                        ticket_id, summary, components, firewall_result,
                        tests_status="SKIPPED", tests_reason=None,
                        judge_verdict=None, branch=branch, backend=backend,
                        is_epic=is_epic, child_ticket_keys=child_ticket_keys,
                        output_guard_status="FAILED", output_guard_reason=reason,
                    )
                )
                raise PipelineBlocked(f"guardia de salida bloqueo el diff generado: {reason}")

            test_result = run_tests(target_repo_dir)
            if not test_result["passed"]:
                _comment_all(f"🧪 Testing agent (Prefect): los tests reales FALLARON en '{agent_result['branch']}'.", ticket_id, is_epic, child_ticket_keys)
                post_alert_webhook(f"🧪 Testing agent BLOCKED en {ticket_id}: los tests reales fallaron en '{agent_result['branch']}'.")
                _transition_all(JIRA_BLOCKED_STATUS, ticket_id, is_epic, child_ticket_keys)
                check_falco_correlation(falco_since, ticket_id)
                record_run_in_graph(
                    _build_graph_payload(
                        ticket_id, summary, components, firewall_result,
                        tests_status="FAILED", tests_reason="el test suite real fallo",
                        judge_verdict=None, branch=branch, backend=backend,
                        is_epic=is_epic, child_ticket_keys=child_ticket_keys,
                    )
                )
                raise PipelineBlocked("tests reales fallaron")

            new_sonar_issues = rescan_sonar(target_repo_dir, components[0]) if components else []

            judge_verdict = _run_judge_safe(
                jira_context, firewall_result, "local_diff", diff_text, test_result["output"],
                self_review=agent_result.get("self_review"), falco_since=falco_since, conflicts=conflicts,
                new_sonar_issues=new_sonar_issues,
            )
            tests_status, tests_reason = "PASSED", None

            if (
                judge_verdict is not None
                and judge_verdict.get("verdict") == "FLAGGED"
                and judge_verdict.get("policy_reference") in RETRYABLE_POLICY_REFERENCES
            ):
                print(
                    f"🔁 El juez marco un problema potencialmente corregible "
                    f"({judge_verdict.get('policy_reference')}) -- dandole al coding agent un segundo intento."
                )
                retried_verdict = _retry_local_diff(
                    ticket_id, sanitized, target_repo_dir, agent_result, judge_verdict,
                    jira_context, firewall_result, components, summary,
                    is_epic, child_ticket_keys, falco_since, conflicts=conflicts,
                )
                if retried_verdict is not None:
                    judge_verdict = retried_verdict
        else:
            _comment_all(
                "🤖 Copilot (Prefect): AI Firewall aprobo la solicitud "
                f"(redacciones: {firewall_result['redactions_applied']}). Copilot no aplico ningun cambio en esta corrida.",
                ticket_id, is_epic, child_ticket_keys,
            )
            judge_verdict = _run_judge_safe(
                jira_context, firewall_result, "issue_only", sanitized, "sin cambios aplicados",
                self_review=agent_result.get("self_review"), falco_since=falco_since, conflicts=conflicts,
            )
            tests_status, tests_reason = "SKIPPED", None

            if judge_verdict is not None and judge_verdict.get("verdict") == "FLAGGED":
                # Real: antes esto solo comentaba en Jira ("che, todavia no
                # aplicaste nada") y ahi quedaba -- el feedback del juez
                # (a veces una accion bien concreta, ej. "implementa las
                # paginas de error") nunca volvia al coding agent. Un solo
                # reintento real, con ese feedback como parte del prompt.
                print("🔁 El agente no aplico nada y el juez lo marco FLAGGED -- dandole un segundo intento con su feedback.")
                retried_verdict = _retry_after_no_changes(
                    ticket_id, sanitized, target_repo_dir, agent_result, judge_verdict,
                    jira_context, firewall_result, components, summary,
                    is_epic, child_ticket_keys, falco_since, conflicts=conflicts,
                )
                if retried_verdict is not None:
                    judge_verdict = retried_verdict
                    # _retry_after_no_changes muto agent_result (branch
                    # nuevo, applied=True) -- branch/backend locales tienen
                    # que reflejarlo para que push_and_open_pr() mas abajo
                    # use la rama correcta.
                    branch = agent_result.get("branch")
                    backend = agent_result.get("backend") or backend

    pr_result = None
    if judge_verdict is None:
        # Confirmado real (KAN-2, epica KAN-4): antes esto solo hacia
        # print() -- ni comentario en Jira ni transicion de status -- y el
        # ticket quedaba clavado en JIRA_IN_PROGRESS_STATUS para siempre,
        # indistinguible de una corrida que sigue en curso de verdad.
        # "sin veredicto" es evidencia insuficiente para dar el cambio por
        # bueno (mismo criterio que ya aplica el prompt del juez), asi que
        # se trata como bloqueado, no como en curso.
        _comment_all(
            "🧑‍⚖️ Agente juez (Prefect): no pudo evaluar esta corrida -- continua sin veredicto.",
            ticket_id, is_epic, child_ticket_keys,
        )
        _transition_all(JIRA_BLOCKED_STATUS, ticket_id, is_epic, child_ticket_keys)
    elif judge_verdict["verdict"] == "FLAGGED":
        _comment_all(f"🧑‍⚖️ Agente juez (Prefect): FLAGGED. {judge_verdict['reasoning']}", ticket_id, is_epic, child_ticket_keys)
        post_alert_webhook(f"🧑‍⚖️ Juez FLAGGED en {ticket_id}: {judge_verdict['reasoning']}")
        _transition_all(JIRA_BLOCKED_STATUS, ticket_id, is_epic, child_ticket_keys)
    else:
        _comment_all(f"🧑‍⚖️ Agente juez (Prefect): OK. {judge_verdict['reasoning']}", ticket_id, is_epic, child_ticket_keys)

        # Solo con veredicto OK real (nunca con FLAGGED, que ya deja la rama
        # marcada BLOCKED BY JUDGE) tiene sentido push+PR+avanzar el ticket
        # a un estado de review -- antes esto no existia, un ticket exitoso
        # se quedaba en In Progress con la rama sin pushear.
        if branch:
            pr_result = push_and_open_pr(
                target_repo_dir, branch, agent_result.get("base_branch"), ticket_id, summary, sanitized
            )
            if pr_result["pr_url"]:
                _comment_all(f"🔀 PR listo para review: {pr_result['pr_url']}", ticket_id, is_epic, child_ticket_keys)
            elif pr_result["pushed"]:
                _comment_all(
                    f"🔀 Rama '{branch}' pusheada, pero no se pudo abrir el PR automaticamente "
                    f"({pr_result['reason']}) — abrilo a mano.",
                    ticket_id, is_epic, child_ticket_keys,
                )
            else:
                _comment_all(
                    f"🔀 El cambio quedo en la rama local '{branch}' — pusheala y abri el PR a mano "
                    f"({pr_result['reason']}).",
                    ticket_id, is_epic, child_ticket_keys,
                )
        _transition_all(JIRA_REVIEW_STATUS, ticket_id, is_epic, child_ticket_keys)

    technical_report_evidence = {
        "ticket": ticket_id,
        "es_epica_combinada": is_epic,
        "backend_usado": backend or "ninguno",
        "modelo_ollama_coding_agent": os.environ.get("CODING_AGENT_OLLAMA_MODEL") or os.environ.get("OLLAMA_MODEL", ""),
        "modelo_ollama_juez": os.environ.get("JUDGE_OLLAMA_MODEL") or os.environ.get("OLLAMA_MODEL", ""),
        "tests_status": tests_status,
        "veredicto_juez": (judge_verdict or {}).get("verdict", "sin veredicto"),
        "razonamiento_juez": (judge_verdict or {}).get("reasoning", "no disponible"),
        "rama_git": branch or "ninguna",
    }
    # Confirmado real (usuario): pr_result ya estaba disponible aca (rama del
    # veredicto OK) pero nunca se le pasaba al comprobante tecnico -- la
    # documentacion del ticket no mencionaba la PR real aunque ya existiera.
    if pr_result and pr_result.get("pr_url"):
        technical_report_evidence["pr_url"] = pr_result["pr_url"]
    technical_report = generate_technical_report(technical_report_evidence)
    if technical_report:
        _comment_all(technical_report, ticket_id, is_epic, child_ticket_keys)

    # Ya no hace falta un check_falco_correlation() aca -- run_judge() ya lo
    # hizo internamente (fetch + post) ANTES de invocar al juez, para que la
    # evidencia de Falco de esta misma corrida pudiera llegar a su payload
    # en vez de solo a un comentario posterior al veredicto.
    record_run_in_graph(
        _build_graph_payload(
            ticket_id, summary, components, firewall_result,
            tests_status=tests_status, tests_reason=tests_reason,
            judge_verdict=judge_verdict, branch=branch, backend=backend,
            is_epic=is_epic, child_ticket_keys=child_ticket_keys,
        )
    )

    return {"firewall": firewall_result, "agent": agent_result, "judge": judge_verdict}


def _deliver_epic_sequential(
    epic_key: str,
    epic: dict,
    ordered_children: list,
    target_repo_dir: str,
    graph_parts: list,
    sonar_parts: list,
    sonar_errors: list,
    coordination_notes: str,
    conflicts: list,
) -> dict:
    """Camino B1 real (backend local disponible, sin GITHUB_REPO) para
    --epic: procesa cada historia hija en su propio turno real -- su propio
    chequeo de firewall, su propio diff incremental, sus propios
    tests/juez/comentario/transicion -- en vez de un solo prompt combinado
    con las N historias adentro. Confirmado en esta sesion que un prompt
    combinado (12 hijos de KAN-4 en un solo texto) es demasiado para que un
    modelo local devuelva el JSON esperado -- termino en
    {"status":"blocked","summary":""} sin aplicar nada.

    Todas las historias comparten UNA sola rama/PR al final -- el turno de
    cada hijo continua la MISMA conversacion del anterior via
    conversation_file (retry_coding_agent_local_real, el mismo mecanismo que
    ya usaba el reintento del juez en _retry_local_diff, solo que encadenado
    historia tras historia en vez de una sola vez).

    Un rechazo del firewall en un hijo puntual no frena a sus hermanos (se
    salta, sigue con el proximo) -- pero un guardia de salida/tests/juez
    FLAGGED SI corta el loop ahi: los hijos restantes en el orden no se
    tocan, y no reciben ningun comentario que diga "procesado" porque no lo
    fueron.
    """
    conflicts_section = _format_conflicts_section(conflicts)
    coordination_section = f"\n--- Notas de coordinacion del planificador ---\n{coordination_notes}" if coordination_notes else ""
    graph_text = "\n".join(graph_parts)
    sonar_text = "\n".join(sonar_parts)

    branch = None
    base_branch = None
    conversation_file = None
    backend = None
    last_firewall_result = {"status": "APPROVED", "reason": None}
    last_judge_verdict = None
    completed = []
    blocked_at = None

    for child_index, child in enumerate(ordered_children):
        child_id = child["ticket_id"]
        # Confirmado real: un humano mirando SOLO el ticket hijo bloqueado no
        # tenia forma de saber si el resto de la epica sigue o se corta --
        # esa info solo vivia en el comentario-resumen final de la epica.
        # Se calcula aca para poder incluirla en el comentario de bloqueo de
        # ESTE hijo, en cada uno de los 3 puntos de corte de abajo.
        siblings_remaining = [c["ticket_id"] for c in ordered_children[child_index + 1:]]
        siblings_remaining_text = ", ".join(siblings_remaining) if siblings_remaining else "ninguna"
        child_prompt = (
            f"Historia {child_id} de la epica {epic_key} ({child['repository_origen']}): {child['summary']}\n"
            f"{child['description']}\n"
            f"{coordination_section}"
            f"{conflicts_section}"
            f"--- Grafo de impacto (por componente) ---\n{graph_text}\n"
            f"--- Hallazgos Sonar (reales, por componente) ---\n{sonar_text}"
        )
        jira_context = {
            "ticket_id": child_id,
            "summary": child["summary"],
            "description": child["description"],
            "repository_origen": child["repository_origen"],
        }
        components = [child["repository_origen"]]

        firewall_result = evaluate_firewall(child_prompt, jira_context, sonar_errors)
        last_firewall_result = firewall_result
        if firewall_result["status"] == "REJECTED":
            logger.warning(f"{child_id}: rechazado por el firewall, se salta -- {firewall_result['reason']}")
            try:
                _handle_rejected(child_id, jira_context, firewall_result)
            except PipelineBlocked:
                pass  # un rechazo puntual no frena a los hermanos
            continue

        transition_jira(JIRA_IN_PROGRESS_STATUS, ticket_key=child_id)
        sanitized = firewall_result["sanitized_prompt"]
        falco_since = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        checkpoint = _run(["git", "-C", target_repo_dir, "rev-parse", "HEAD"]).strip() if branch else None

        if branch is None:
            agent_result = run_coding_agent_local_real(epic_key, sanitized, target_repo_dir)
            branch = agent_result.get("branch")
            base_branch = agent_result.get("base_branch")
        else:
            agent_result = retry_coding_agent_local_real(
                epic_key, sanitized, target_repo_dir, conversation_file=conversation_file
            )
        conversation_file = agent_result.get("conversation_file")
        backend = agent_result.get("backend") or backend

        if not agent_result["applied"]:
            comment_jira(
                f"🤖 Copilot (Prefect, modo epica secuencial): AI Firewall aprobo {child_id} "
                f"(redacciones: {firewall_result['redactions_applied']}). Copilot no aplico ningun cambio para esta historia.",
                ticket_key=child_id,
            )
            _post_child_technical_report(epic_key, child_id, backend, "no-op -- el agente no aplico ningun cambio")
            completed.append({"ticket_id": child_id, "outcome": "no-op"})
            continue

        if (agent_result.get("self_review") or {}).get("tests_adequate") is False:
            comment_jira(
                f"🧪 El coding agent se autoevaluo con tests_adequate=false en {child_id} -- dandole un turno "
                "mas para agregar un test real antes de correr el testing agent.",
                ticket_key=child_id,
            )
            agent_result = _retry_for_inadequate_tests(epic_key, target_repo_dir, agent_result)
            backend = agent_result.get("backend") or backend

        diff_text = _run(["git", "-C", target_repo_dir, "diff", f"{checkpoint}..HEAD"]) if checkpoint \
            else _run(["git", "-C", target_repo_dir, "diff", f"{base_branch}..HEAD"])

        resumed_note = ""
        if agent_result.get("resumed_branch"):
            resumed_note = " (retoma una rama existente de una corrida anterior de esta epica, no empieza de cero)"
            if agent_result.get("resumed_pr_rejected"):
                resumed_note += " -- OJO: la PR anterior de esta rama fue RECHAZADA/cerrada sin mergear, revisar si el codigo previo sigue siendo valido"
        comment_jira(
            f"🤖 Copilot (Prefect, modo epica secuencial): AI Firewall aprobo {child_id} "
            f"(redacciones: {firewall_result['redactions_applied']}). Copilot aplico un cambio en la rama '{branch}'{resumed_note}, "
            "pendiente de revision humana.",
            ticket_key=child_id,
        )
        pr_thread_ids_to_resolve = agent_result.get("pr_thread_ids_to_resolve")
        if pr_thread_ids_to_resolve:
            _resolve_pr_threads(target_repo_dir, branch, pr_thread_ids_to_resolve)
            comment_jira(
                f"💬 Se intento atender {len(pr_thread_ids_to_resolve)} comentario(s) de revision real de la PR "
                "abierta en este mismo commit -- revisar si la correccion fue suficiente.",
                ticket_key=child_id,
            )

        guard_result = run_output_guard(diff_text, jira_context)
        if not guard_result["clean"]:
            reason = f"redacciones={guard_result['redactions_applied']}, jailbreak={guard_result['jailbreak_reason']}"
            comment_jira(
                f"🛡️ Guardia de salida (Prefect): diff de {child_id} bloqueado ({reason}). Esto corta el "
                f"procesamiento de la epica -- historias hermanas sin tocar: {siblings_remaining_text}.",
                ticket_key=child_id,
            )
            post_alert_webhook(f"🛡️ Guardia de salida BLOCKED en {child_id} (modo epica secuencial): {reason}")
            transition_jira(JIRA_BLOCKED_STATUS, ticket_key=child_id)
            check_falco_correlation(falco_since, child_id)
            _post_child_technical_report(epic_key, child_id, backend, f"bloqueada por la guardia de salida ({reason})")
            blocked_at = child_id
            break

        test_result = run_tests(target_repo_dir)
        if not test_result["passed"]:
            comment_jira(
                f"🧪 Testing agent (Prefect): los tests reales FALLARON procesando {child_id}. Esto corta el "
                f"procesamiento de la epica -- historias hermanas sin tocar: {siblings_remaining_text}.",
                ticket_key=child_id,
            )
            post_alert_webhook(f"🧪 Testing agent BLOCKED en {child_id} (modo epica secuencial): tests reales fallaron.")
            transition_jira(JIRA_BLOCKED_STATUS, ticket_key=child_id)
            check_falco_correlation(falco_since, child_id)
            _post_child_technical_report(epic_key, child_id, backend, "bloqueada -- los tests reales fallaron", {"salida_tests": test_result["output"][:2000]})
            blocked_at = child_id
            break

        new_sonar_issues = rescan_sonar(target_repo_dir, child["repository_origen"])
        judge_verdict = _run_judge_safe(
            jira_context, firewall_result, "local_diff", diff_text, test_result["output"],
            self_review=agent_result.get("self_review"), falco_since=falco_since,
            conflicts=conflicts, new_sonar_issues=new_sonar_issues,
        )

        if (
            judge_verdict is not None
            and judge_verdict.get("verdict") == "FLAGGED"
            and judge_verdict.get("policy_reference") in RETRYABLE_POLICY_REFERENCES
        ):
            print(f"🔁 {child_id}: el juez marco {judge_verdict.get('policy_reference')} -- segundo intento.")
            retried_verdict = _retry_local_diff(
                child_id, sanitized, target_repo_dir, agent_result, judge_verdict,
                jira_context, firewall_result, components, child["summary"],
                False, None, falco_since, conflicts=conflicts,
            )
            if retried_verdict is not None:
                judge_verdict = retried_verdict

        last_judge_verdict = judge_verdict

        if judge_verdict is None:
            comment_jira(f"🧑‍⚖️ Agente juez (Prefect): no pudo evaluar {child_id} -- continua sin veredicto.", ticket_key=child_id)
            # Confirmado real (KAN-2): antes esto no transicionaba nada -- el
            # hijo quedaba clavado en JIRA_IN_PROGRESS_STATUS para siempre,
            # indistinguible de un hijo que sigue en curso de verdad. "sin
            # veredicto" es evidencia insuficiente para dar el cambio por
            # bueno, se trata como bloqueado (necesita revision humana).
            transition_jira(JIRA_BLOCKED_STATUS, ticket_key=child_id)
            _post_child_technical_report(epic_key, child_id, backend, "sin veredicto del juez", {"rama_git": branch})
            completed.append({"ticket_id": child_id, "outcome": "no-verdict"})
        elif judge_verdict["verdict"] == "FLAGGED":
            comment_jira(
                f"🧑‍⚖️ Agente juez (Prefect): FLAGGED en {child_id}. {judge_verdict['reasoning']} Esto corta el "
                f"procesamiento de la epica -- historias hermanas sin tocar: {siblings_remaining_text}.",
                ticket_key=child_id,
            )
            post_alert_webhook(f"🧑‍⚖️ Juez FLAGGED en {child_id} (modo epica secuencial): {judge_verdict['reasoning']}")
            transition_jira(JIRA_BLOCKED_STATUS, ticket_key=child_id)
            _post_child_technical_report(epic_key, child_id, backend, "bloqueada -- el juez marco FLAGGED", {"veredicto_juez": judge_verdict["reasoning"], "rama_git": branch})
            blocked_at = child_id
            break
        else:
            comment_jira(f"🧑‍⚖️ Agente juez (Prefect): OK en {child_id}. {judge_verdict['reasoning']}", ticket_key=child_id)
            transition_jira(JIRA_REVIEW_STATUS, ticket_key=child_id)
            _post_child_technical_report(epic_key, child_id, backend, "OK -- listo para revision humana", {"veredicto_juez": judge_verdict["reasoning"], "rama_git": branch})
            completed.append({"ticket_id": child_id, "outcome": "ok"})

    pr_result = None
    if branch and any(c["outcome"] in ("ok", "no-verdict") for c in completed):
        # Bug real confirmado (usuario, PR #238): antes esto pasaba
        # epic["description"] tal cual -- el texto ORIGINAL/crudo de la
        # epica (a veces un prompt de varios miles de caracteres), sin
        # ninguna relacion con lo que esta rama puntual realmente cambio.
        # Se arma un body real describiendo que historias se procesaron y
        # con que resultado, no el pedido original completo.
        children_by_id_for_pr = {c["ticket_id"]: c for c in ordered_children}
        pr_body_lines = [f"Cambios aplicados por el pipeline de agentes de IA para la epica {epic_key} ({epic['summary']})."]
        pr_body_lines.append("\nHistorias incluidas en esta rama:")
        for c in completed:
            child = children_by_id_for_pr.get(c["ticket_id"])
            title = f" -- {child['summary']}" if child else ""
            pr_body_lines.append(f"- {c['ticket_id']}{title} ({c['outcome']})")
        pr_body = "\n".join(pr_body_lines)
        pr_result = push_and_open_pr(target_repo_dir, branch, base_branch, epic_key, epic["summary"], pr_body)

    processed_ids = {c["ticket_id"] for c in completed} | ({blocked_at} if blocked_at else set())
    remaining = [c["ticket_id"] for c in ordered_children if c["ticket_id"] not in processed_ids]
    summary_lines = [f"🧩 Modo epica secuencial (Prefect): {len(completed)}/{len(ordered_children)} historias procesadas."]
    if blocked_at:
        summary_lines.append(f"Se corto en {blocked_at} -- sin tocar: {', '.join(remaining) if remaining else 'ninguna'}.")
    if pr_result and pr_result.get("pr_url"):
        summary_lines.append(f"PR: {pr_result['pr_url']}")
    summary_text = " ".join(summary_lines)
    comment_jira(summary_text, ticket_key=epic_key)
    # Confirmado real (usuario): la linea "PR: ..." solo se posteaba a la
    # epica -- las historias que realmente aportaron a esa rama/PR (outcome
    # "ok"/"no-verdict") no tenian la URL real en su propia documentacion.
    for c in completed:
        if c["outcome"] in ("ok", "no-verdict"):
            comment_jira(summary_text, ticket_key=c["ticket_id"])

    # Confirmado real (KAN-4): antes esto no existia -- la epica se
    # transicionaba a JIRA_IN_PROGRESS_STATUS al arrancar y nunca mas, asi
    # que quedaba clavada en "En curso" para siempre sin importar el
    # resultado real (12/12 OK o cortada en la primera historia). El modo
    # combinado (_deliver con is_epic=True) ya transicionaba la epica junto
    # con los hijos via _transition_all -- esto lleva el modo secuencial al
    # mismo criterio: bloqueada si se corto en algun punto o si nada quedo
    # listo para revision humana, en revision si al menos una historia lo
    # esta (mismo criterio que la condicion de arriba para abrir la PR).
    has_reviewable_work = branch and any(c["outcome"] in ("ok", "no-verdict") for c in completed)
    if blocked_at or not has_reviewable_work:
        transition_jira(JIRA_BLOCKED_STATUS, ticket_key=epic_key)
    else:
        transition_jira(JIRA_REVIEW_STATUS, ticket_key=epic_key)

    if conversation_file:
        Path(conversation_file).unlink(missing_ok=True)

    record_run_in_graph(
        _build_graph_payload(
            epic_key, epic["summary"], list({c["repository_origen"] for c in ordered_children}),
            last_firewall_result,
            tests_status="PASSED" if completed else "SKIPPED", tests_reason=None,
            judge_verdict=last_judge_verdict, branch=branch, backend=backend,
            is_epic=True, child_ticket_keys=[c["ticket_id"] for c in ordered_children],
        )
    )

    return {"completed": completed, "blocked_at": blocked_at, "branch": branch, "pr": pr_result}


@flow(name="poc-ai-agents-pipeline", log_prints=True)
def run_pipeline():
    logger = get_run_logger()

    target_repo_dir = detect_target_repo()
    check_dirty_tree(target_repo_dir)
    logger.info(f"Repo objetivo detectado: {target_repo_dir}")

    known_components = discover_known_components()

    ticket = fetch_jira_ticket(known_components)
    _check_not_epic(ticket)
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

    known_components = discover_known_components()

    epic_data = fetch_epic(epic_key, known_components)
    epic = epic_data["epic"]
    children = epic_data["children"]
    logger.info(f"Epica {epic_key} — {len(children)} hijos")

    if not children:
        raise PipelineBlocked(
            f"La epica {epic_key} no tiene hijos segun el JQL configurado (JIRA_EPIC_LINK_JQL). "
            "Si tu proyecto Jira es 'company-managed', probablemente necesites el campo custom 'Epic Link' en vez de 'parent'. "
            "Si la epica todavia no tiene historias hijas creadas, usa prompts/decompose_epic_with_rovo.md "
            "(Claude Code + Rovo) para descomponerla en historias reales antes de reintentar."
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

    plan_result = plan_epic_task(epic, children)
    children_by_id = {c["ticket_id"]: c for c in children}
    ordered_children = [children_by_id[cid] for cid in plan_result["ordered_children"] if cid in children_by_id]
    if len(ordered_children) != len(children):
        logger.warning("plan-epic devolvio un orden incompleto -- se usa el orden original")
        ordered_children = children
    coordination_notes = plan_result.get("coordination_notes") or ""
    conflicts = plan_result.get("conflicts") or []
    if conflicts:
        logger.warning(f"plan-epic detecto {len(conflicts)} conflicto(s) potencial(es) entre historias hijas: {conflicts}")

    # Camino B1 real (backend local disponible, sin GITHUB_REPO): procesa
    # cada hijo en su propio turno real en vez de un prompt combinado -- ver
    # _deliver_epic_sequential. El camino cloud y el fallback de un solo
    # tiro (sin backend real) no tienen concepto de "continuar la misma
    # conversacion" turno a turno, asi que siguen con el prompt combinado
    # de mas abajo.
    if not GITHUB_REPO and _local_coding_agent_backend_available():
        return _deliver_epic_sequential(
            epic_key, epic, ordered_children, target_repo_dir,
            graph_parts, sonar_parts, sonar_errors, coordination_notes, conflicts,
        )

    children_text = "\n".join(
        f"- {c['ticket_id']} ({c['repository_origen']}): {c['summary']}\n  {c['description']}" for c in ordered_children
    )
    coordination_section = f"\n--- Notas de coordinacion del planificador ---\n{coordination_notes}" if coordination_notes else ""
    conflicts_section = _format_conflicts_section(conflicts)
    prompt = (
        f"ESTO ES UNA EPICA con {len(children)} historias hijas. Resolvelas todas juntas, "
        "coordinando los cambios entre los componentes que toca cada una.\n\n"
        f"Epica {epic_key}: {epic['summary']}\n{epic['description']}\n\n"
        f"--- Historias hijas (orden sugerido por el planificador de epicas) ---\n{children_text}\n"
        f"{coordination_section}"
        f"{conflicts_section}"
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
    child_ticket_keys = [c["ticket_id"] for c in children]

    if firewall_result["status"] == "REJECTED":
        logger.warning(f"Rechazado por el firewall: {firewall_result['reason']}")
        _handle_rejected(epic_key, jira_context, firewall_result, is_epic=True, child_ticket_keys=child_ticket_keys)

    # _deliver ya espeja cada comentario/transicion real (firewall, coding
    # agent, tests, juez, PR) hacia cada hijo a medida que ocurre -- ya no
    # hace falta un comentario generico aparte al final.
    return _deliver(
        epic_key, epic["summary"], firewall_result, jira_context, target_repo_dir,
        is_epic=True, child_ticket_keys=child_ticket_keys, conflicts=plan_result.get("conflicts"),
    )


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
