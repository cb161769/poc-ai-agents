"""Real local coding agent -- Camino B1 of run_poc_loop.sh/orchestration.py.

Unlike `gh copilot suggest` (Camino B2, a one-shot suggestion with no memory
or reasoning loop), this is an actual multi-turn agent: it reads/writes
files, lists directories, greps the codebase, runs shell commands, and can
query the same MCP tools the judge has (Neo4j-cypher, Qdrant-rag) -- all
confined to the target repo it was handed, already checked out on a fresh
branch by the caller (never the base branch).

Every write_file/run_shell_command call requires human confirmation before
it happens (printed + input() prompt) -- this agent has real reasoning but
never acts unsupervised, same safety spirit gh copilot suggest already had.

Uses the same dual backend as judge_agent.py (agent_loop.py): Anthropic API
first, local Ollama as a free/offline fallback. The caller (run_poc_loop.sh/
orchestration.py) only invokes this when a backend is actually reachable --
falls back to gh copilot suggest otherwise.

Reads its payload from a JSON FILE passed as the first CLI argument (not
stdin -- stdin has to stay free for the interactive confirmations). All
narration and confirmation prompts go to stderr; stdout carries ONLY the
final JSON result, so the caller can capture stdout for the structured
result while the user still sees (and answers) confirmations live on the
terminal.

Usage: python3 coding_agent.py <payload.json>
  payload.json: {"ticket_id": "...", "sanitized_prompt": "...", "target_repo_dir": "..."}

Prints to stdout: {"status": "done"|"blocked", "summary": "...",
  "files_changed": [...], "_meta": {backend, latency_seconds, tokens, cost}}
Every call is appended to logs/coding_agent_runs.jsonl.
"""
import asyncio
import difflib
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import AsyncExitStack
from pathlib import Path

import httpx
from dotenv import load_dotenv
from mcp import StdioServerParameters

import sonar_client
from agent_loop import (
    ANTHROPIC_MODEL,
    OLLAMA_MODEL,
    _call_mcp_tool,
    _call_model_turn,
    _connect_mcp_servers,
    _estimate_cost_usd,
    _final_text_with_json_retry,
    _normalize_tool_schema,
    _select_backend,
    call_with_fallback,
    compact_old_tool_results,
    init_ollama_model_state,
    maybe_switch_ollama_model,
    parse_ollama_model_candidates,
    warn_if_context_large,
)
from log_utils import get_logger

load_dotenv()

logger = get_logger(__name__)

LOG_DIR = Path(__file__).resolve().parent / "logs"
RUN_LOG = LOG_DIR / "coding_agent_runs.jsonl"

MAX_TOOL_TURNS = int(os.environ.get("CODING_AGENT_MAX_TURNS", "15"))

# Modelo(s) Ollama propios para este agente -- escribir codigo/usar tools
# correctamente es mas exigente que solo evaluar texto (judge_agent.py), asi
# que puede valer la pena un modelo mas fuerte aca (ej. qwen2.5-coder:7b) sin
# afectar al juez. Coma-separado = lista de candidatos por prioridad (ver
# parse_ollama_model_candidates); un solo valor (o sin setear, cae al
# generico OLLAMA_MODEL) se comporta igual que antes de esto.
CODING_AGENT_OLLAMA_MODELS = parse_ollama_model_candidates(os.environ.get("CODING_AGENT_OLLAMA_MODEL", ""), OLLAMA_MODEL)

# Limites defensivos de las tools locales: un archivo/salida/listado gigante
# no debe reventar el contexto del modelo ni colgar el loop. _MAX_READ_BYTES
# bajo de 200_000 a 60_000 (~15k tokens estimados) -- el valor anterior
# permitia que UN SOLO read_file consumiera ~50k tokens antes de truncar,
# la mayor fuga individual de contexto de todas las tools. Configurable
# para quien necesite leer archivos mas grandes a proposito.
_MAX_READ_BYTES = int(os.environ.get("CODING_AGENT_MAX_READ_BYTES", "60000"))
_MAX_GREP_FILE_BYTES = 2_000_000
_MAX_GREP_FILES_SCANNED = 5_000
_MAX_LIST_ENTRIES = 500
_MAX_SHELL_OUTPUT_CHARS = 10_000

# Tools de solo lectura cuyos resultados viejos se compactan
# (agent_loop.compact_old_tool_results) despues de unos turnos -- nunca
# incluye write_file/edit_file/run_shell_command, esos representan efectos
# reales que el modelo tiene que seguir recordando con precision.
_READ_ONLY_TOOL_NAMES = {
    "read_file",
    "list_directory",
    "grep_search",
    "git_diff",
    "git_log",
    "detect_project_stack",
    "query_sonar",
}

# Variables de entorno que nunca deben llegar a un comando de shell que
# corre el agente -- el proceso de coding_agent.py las tiene cargadas
# (load_dotenv()) para hablarle a Jira/Sonar/Figma/etc, pero un comando
# como "env" o un script de debug no deberia poder filtrarlas.
_SENSITIVE_ENV_VARS = {
    "ANTHROPIC_API_KEY",
    "JIRA_API_TOKEN",
    "AZURE_DEVOPS_PAT",
    "SONAR_TOKEN",
    "SONAR_NEW_ADMIN_PASSWORD",
    "FIGMA_API_TOKEN",
    "FIREWALL_API_KEY",
    "NEO4J_PASSWORD",
    "GITHUB_TOKEN",
    "GH_TOKEN",
}


def _sanitized_subprocess_env() -> dict:
    return {k: v for k, v in os.environ.items() if k not in _SENSITIVE_ENV_VARS}

VERIFY_BEFORE_DONE_MESSAGE = (
    "Antes de terminar, corré un comando de verificacion real (tests, build, o lint del proyecto) "
    "con run_shell_command y contame el resultado en el summary."
)

SELF_REVIEW_NUDGE_MESSAGE = (
    'Tu respuesta final necesita el campo "self_review" completo -- {"scope_matches_ticket": bool, '
    '"no_secrets_introduced": bool, "tests_adequate": bool} -- respondé de nuevo con el JSON completo, '
    "contestando cada campo con honestidad segun tu propio cambio."
)

# Confirmado real esta sesion: algunos modelos locales anuncian en texto
# narrativo que van a crear/editar un archivo o correr un comando, pero
# nunca llegan a emitir la tool-call real -- en vez de eso explican que
# "no pueden sin confirmacion humana" (aunque write_file/edit_file/
# run_shell_command YA piden esa confirmacion solas al ser llamadas). Sin
# este nudge especifico, el flujo caia directo al reintento generico de
# "dame JSON valido", que no corrige la causa real (el modelo sigue sin
# animarse a llamar la tool, solo reformatea su negativa como JSON).
_TOOL_CALL_REFUSAL_PATTERN = re.compile(
    r"sin confirmaci[oó]n|necesito (?:confirmaci[oó]n|permiso)|no puedo (?:crear|escribir|editar|ejecutar|modificar)|"
    r"requiere confirmaci[oó]n humana|no puedo hacer(?:lo)? sin",
    re.IGNORECASE,
)

# Confirmado real (epica KAN-4, qwen2.5-coder:7b, 12/12 historias bloqueadas):
# cuando el modelo se confunde a mitad de la investigacion (no llama una tool
# ni devuelve JSON), el reintento de _final_text_with_json_retry() le pide
# JSON valido -- Y LO CONSIGUE, pero con status "blocked" y un summary que
# literalmente repite el mensaje del reintento ("la respuesta anterior no
# fue JSON valido"). Como el JSON es sintacticamente valido, ese resultado
# nunca llegaba al chequeo de maybe_switch_ollama_model (que solo dispara
# cuando el reintento ADEMAS falla en parsear) -- se aceptaba como "blocked"
# final sin darle a otro modelo la chance real de investigar. Distinto de un
# bloqueo legitimo por ticket ambiguo (esos nunca mencionan JSON, hablan de
# rutas/specs faltantes) -- este patron es especificamente el modelo
# citando su propia confusion de formato, no una decision real sobre el ticket.
_JSON_CONFUSION_BLOCKED_PATTERN = re.compile(r"json", re.IGNORECASE)

TOOL_CALL_NUDGE_MESSAGE = (
    "Ya podes hacerlo -- la confirmacion humana la pide automaticamente la herramienta misma "
    "(write_file/edit_file/run_shell_command) apenas la llamas, vos no necesitas pedir permiso en texto "
    "ni explicar de nuevo por que no podes. Llama a la tool correspondiente AHORA MISMO, con el "
    "contenido o comando real -- no respondas con otra explicacion."
)

TOOL_CALL_NUDGE_MESSAGE_NEEDS_INVESTIGATION = (
    "Ya podes hacerlo -- la confirmacion humana la pide automaticamente la herramienta misma, vos no "
    "necesitas pedir permiso en texto. Pero primero confirma la ruta EXACTA con list_directory (no "
    "adivines un nombre de archivo/carpeta) -- si de verdad no existe, esa es la señal correcta para "
    "crearlo con write_file en la ubicacion real del proyecto, no para bloquearte. Segui esos pasos "
    "AHORA, no respondas con otra explicacion."
)

# Gap real (usuario, "hay gaps en el coding agent"): antes, has_run_verification
# (-> self_verified) se marcaba True con CUALQUIER llamado a run_shell_command,
# incluido uno trivial como "ls" o "echo listo" -- el modelo podia satisfacer
# el empujon de verificacion sin correr nada que realmente pruebe el cambio.
# Exige que el comando se PAREZCA a una verificacion real (test/build/lint/
# compilacion), mismas palabras clave que los comandos sugeridos por
# _STACK_MARKERS (npm test, mvn test, go test, cargo test, bundle exec rspec,
# pytest, dotnet test).
_VERIFICATION_COMMAND_PATTERN = re.compile(r"\b(test|rspec|lint|build|compile)\b", re.IGNORECASE)

# Bug real confirmado en vivo (PR real de esta sesion, epica KAN-4): el
# coding agent corrio "ionic start my-app" dentro de frontend/ SIN revisar
# antes si ya habia un src/ real ahi -- resultado: dos arboles src/
# desconectados (frontend/src/ real + frontend/my-app/src/ nuevo) en el
# mismo PR. Estos comandos generan estructura de proyecto NUEVA -- antes de
# dejarlos correr, se exige que el agente haya listado (list_directory) el
# directorio donde va a scaffoldear, para que tenga evidencia real de lo
# que ya existe ahi en vez de asumir que esta vacio.
_SCAFFOLD_COMMAND_PATTERN = re.compile(
    r"\b(ionic start|ng new|create-react-app|npm create|npx create-|yarn create|vue create|create-next-app)\b",
    re.IGNORECASE,
)

# Gap real identificado en una auditoria de arquitectura previa (chat.py,
# "sin regla de seguridad especifica para comandos de shell peligrosos"):
# HOY el unico gate es el [s/n] generico de _confirm() -- sin ninguna señal
# extra de "este comando en particular es especialmente riesgoso" antes de
# aprobarlo. No bloquea nada nuevo (mismo humano en el medio de siempre),
# solo hace mas dificil aprobar uno de estos distraidamente.
_DANGEROUS_COMMAND_PATTERN = re.compile(
    r"(rm\s+-[a-z]*r[a-z]*f|rm\s+-[a-z]*f[a-z]*r|git\s+reset\s+--hard|git\s+push\s+.*--force"
    r"|git\s+clean\s+-[a-z]*f|drop\s+table|drop\s+database|>\s*/etc/|:\(\)\s*\{\s*:\|:\s*&\s*\}\s*;\s*:)",
    re.IGNORECASE,
)


def _normalize_listed_dir(path: str) -> str:
    return (path or ".").strip().rstrip("/") or "."

_SELF_REVIEW_FIELDS = ("scope_matches_ticket", "no_secrets_introduced", "tests_adequate")


def _has_valid_self_review(result: dict) -> bool:
    self_review = result.get("self_review")
    if not isinstance(self_review, dict):
        return False
    return all(isinstance(self_review.get(field), bool) for field in _SELF_REVIEW_FIELDS)

# Esquema real de la respuesta final esperada (ver CODING_AGENT_SYSTEM_PROMPT
# mas abajo, "esquema exacto") -- se le pasa a Ollama via el parametro
# "format" (JSON Schema real, no solo el string "json") SOLO en el reintento
# de correccion (_final_text_with_json_retry pasa tools=[]), restringiendo el
# decoding a este esquema exacto en vez de "cualquier JSON valido"
# (docs.ollama.com/capabilities/structured-outputs).
CODING_AGENT_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["done", "blocked"]},
        "summary": {"type": "string"},
        "files_changed": {"type": "array", "items": {"type": "string"}},
        "self_review": {
            "type": "object",
            "properties": {field: {"type": "boolean"} for field in _SELF_REVIEW_FIELDS},
        },
    },
    "required": ["status", "summary", "files_changed"],
}

CODING_AGENT_SYSTEM_PROMPT = """Sos un agente de codigo real, trabajando sobre un repositorio git real que \
ya esta parado en una rama nueva (nunca la rama base) creada especificamente para este cambio.

Tenes herramientas reales: leer archivos, listar directorios, buscar texto/patrones en el codigo, \
editar un fragmento especifico de un archivo, escribir/crear archivos, correr comandos de shell, ver tu \
propio diff y el historial de commits, detectar el stack del proyecto, y consultar hallazgos reales de \
Sonar -- todas confinadas a este repo, nunca fuera de el. Tambien tenes acceso al grafo de dependencias \
(Neo4j) y a codigo/historico indexado (Qdrant) si estan disponibles, para verificar el impacto real de \
tu cambio antes de aplicarlo.

Antes de escribir el cambio, usa ese acceso para contexto histórico real, no solo impacto estructural: \
consulta Qdrant-rag por incidentes o código similar ya resuelto en este repo, y consulta el grafo por \
nodos :Risk que ya afectaron a este componente (`MATCH (svc:Service {name: "X"})<-[:AFFECTS]-(r:Risk) \
RETURN r`, cambiando X por tu componente) -- si este componente ya causó un `scope-mismatch` o \
`insufficient-test-coverage` en una corrida anterior, es información real para no repetir el mismo error.

Preferi edit_file sobre write_file cuando modifiques un archivo EXISTENTE -- edit_file cambia solo el \
fragmento exacto que indiques (como un str_replace), en vez de reescribir el archivo entero; reserva \
write_file para crear archivos nuevos o reescrituras genuinamente completas. Usa detect_project_stack \
antes de asumir un comando de test/build, y query_sonar si necesitas mas detalle de un hallazgo puntual \
en vez de confiar solo en lo que ya te dieron. Usa git_diff antes de declararte "done" para revisar tu \
propio cambio de punta a punta.

NUNCA le pases a read_file una ruta que estas ADIVINANDO -- si no sabes la ruta EXACTA de un archivo, \
usa list_directory primero (empezando por la carpeta del sub-proyecto real, ver detect_project_stack) \
para confirmarla, o grep_search si sabes un fragmento de contenido pero no la ubicacion. Un read_file \
con una ruta inventada solo te da un error y quema un turno -- list_directory te da la estructura real \
del proyecto para no tener que adivinar nada.

IMPORTANTE -- no confundas "no encontre X existente" con "no puedo hacer nada": muchos tickets piden \
CREAR algo que todavia no existe en el repo (un archivo, un componente, una pagina, una funcionalidad \
nueva). Si investigaste (list_directory/read_file/grep_search) y confirmaste que de verdad no existe \
todavia lo que el ticket pide, esa es la señal correcta para usar write_file y CREARLO -- seguí el estilo \
y las convenciones que ya veas en archivos vecinos del mismo proyecto/sub-proyecto. Solo declarate \
"blocked" si el ticket es genuinamente ambiguo sobre que crear (no simplemente porque el archivo no \
existia todavia), y explicá en el summary exactamente que ambigüedad te frena.

Si detect_project_stack devuelve "monorepo detectado" (varios sub-proyectos, cada uno en su propia \
subcarpeta), SIEMPRE pasa el parametro `cwd` de run_shell_command con la subcarpeta del sub-proyecto real \
que estas tocando -- sin eso el comando corre en la raiz del repo y falla (ahi no esta el package.json/ \
pom.xml/etc real). Ejemplo: si detect_project_stack dice "frontend/: Node/TS -- npm test", corre \
run_shell_command con command="npm test" y cwd="frontend".

REGLA OBLIGATORIA: no podes usar write_file, edit_file, ni run_shell_command hasta haber usado al menos \
una vez read_file, list_directory, o grep_search en esta corrida -- si lo intentas antes, la herramienta \
va a rechazar el llamado y vas a perder un turno. Investiga primero, siempre.

Cada escritura, edicion, o comando de shell YA tiene su propia confirmacion humana incorporada -- se \
pregunta automaticamente cuando llamas a write_file/edit_file/run_shell_command, vos NUNCA tenes que \
pedir permiso en texto ni bloquearte con "no puedo sin confirmacion humana": eso ya esta resuelto por la \
herramienta misma, simplemente llamala. Si el usuario rechaza uno, no insistas con el mismo cambio, \
ajusta tu plan.

Antes de tu primer llamado a write_file/edit_file/run_shell_command, tu respuesta de texto tiene que \
incluir un bloque corto "Plan: " con los pasos concretos que vas a seguir -- no es opcional, es lo que \
te obliga a pensar el cambio antes de tocar el repo, y queda auditado junto al resultado final.

Hace el cambio MAS CHICO Y SEGURO que resuelva el ticket. No inventes archivos ni asumas estructura que \
no verificaste -- si justificas un cambio citando el codigo existente, citalo con ruta:linea real, no de memoria.

Si una herramienta falla o no te devuelve datos utiles (un comando de shell que tira error, una consulta \
al grafo/Sonar que no responde, un archivo que no se puede leer), NO sigas como si no hubiera pasado nada \
-- registralo explicitamente en tu razonamiento/summary y ajusta tu plan en consecuencia, en vez de asumir \
que "sin evidencia de un problema" significa "todo esta bien".

Compara tu cambio explicitamente contra los requisitos CONCRETOS del ticket (si tiene criterios Gherkin \
Given/When/Then o una lista de requisitos, cubrilos uno por uno) antes de declararte "done" -- "hice algo \
plausible relacionado con el ticket" no alcanza, tiene que resolver especificamente lo que el ticket pide.

No hagas MAS de lo que el ticket pide -- si en el camino ves algo que tocarias en otro contexto (refactor, \
limpieza, otra funcionalidad), NO lo toques: cerra el cambio apenas cubras lo que el ticket pide, y \
mencionalo en el summary como fuera de alcance si te parece relevante, no lo implementes vos.

No inventes arquitectura, capas, ni archivos de configuracion/estructura que no viste con tus propias \
herramientas (list_directory/read_file/grep_search) -- si necesitas asumir algo sobre como esta organizado \
el proyecto para poder avanzar, verificalo primero, no lo improvises.

Bug real confirmado en vivo (un PR real de este pipeline): un comando como "ionic start"/"ng new"/ \
"create-react-app" corrio dentro de un sub-proyecto que YA tenia un src/ real (con componentes/servicios \
reales) sin revisarlo antes -- el resultado fueron DOS arboles src/ desconectados en el mismo PR. Antes de \
scaffoldear un proyecto/estructura nueva, o de crear un archivo cuyo nombre ya existe con otra extension en \
el mismo directorio (ej. Header.tsx cuando ya existe Header.ts), list_directory ESE directorio puntual \
primero -- si ya hay estructura real ahi, extendela/edita lo existente, no dupliques creando algo paralelo.

Antes de declararte "done", corré algo que verifique tu cambio de verdad con run_shell_command (los tests \
del proyecto si existen, o al menos una compilacion/lint) -- terminar sin haber corrido nada es aceptable \
solo si genuinamente no hay como verificar (explicalo en el summary si es el caso). Ese comando tiene que \
ser el REAL del sub-proyecto que tocaste (el que te dio detect_project_stack para ESA subcarpeta, con el \
`cwd` correspondiente) -- correr un comando generico desde la raiz, o el de un sub-proyecto distinto al \
que modificaste, no cuenta como verificacion real del cambio.

Si tu cambio agrega comportamiento nuevo visible para el usuario o el sistema (una pagina, un endpoint, una \
funcion, una validacion) que los tests EXISTENTES no cubren, agrega vos mismo un test nuevo (unitario o \
E2E, el que corresponda al stack real del sub-proyecto) que lo verifique -- correr solo la suite existente \
sin sumar cobertura para lo nuevo NO cuenta como "tests_adequate": true, aunque esa suite pase entera. Ese \
test nuevo tiene que incluir al menos UN caso negativo/de error (entrada invalida, recurso inexistente, \
permiso denegado, lo que corresponda al cambio real) cuando el cambio lo amerite -- cubrir solo el camino \
feliz tampoco cuenta como "tests_adequate": true. Si genuinamente no hay forma de agregar un test (herramienta \
no disponible, cambio no testeable en este contexto), explicalo en el summary y marca "tests_adequate": false \
con honestidad -- no te declares "done" dandole al ticket por resuelto sin dejar constancia explicita de ese gap.

Cuando termines (con exito o porque no podes seguir), respondé con texto plano que sea UNICAMENTE un \
objeto JSON, sin texto antes ni despues, con este esquema exacto: {"status": "done" o "blocked", \
"summary": "que hiciste, que verificaste, o por que no pudiste", "files_changed": ["ruta1", "ruta2", ...], \
"self_review": {"scope_matches_ticket": true o false, "no_secrets_introduced": true o false, \
"tests_adequate": true o false}}. self_review es tu propia autocritica ANTES de declararte "done" -- \
contestala con honestidad, no la completes con true por defecto: "scope_matches_ticket" es false si tu \
diff toca algo que el ticket no pidio, "no_secrets_introduced" es false si escribiste algo que se parezca \
a un secreto real, "tests_adequate" es false si lo que corriste no cubre genuinamente el cambio."""

MCP_SERVERS = {
    "neo4j-cypher": StdioServerParameters(
        command="uvx",
        args=["mcp-neo4j-cypher"],
        env={
            "NEO4J_URI": os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            "NEO4J_USERNAME": os.environ.get("NEO4J_USERNAME", "neo4j"),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD", "test_password_local"),
        },
    ),
    "qdrant-rag": StdioServerParameters(
        command="uvx",
        args=["mcp-server-qdrant"],
        env={
            "QDRANT_URL": os.environ.get("QDRANT_URL", "http://localhost:6333"),
            "COLLECTION_NAME": "sample_repo_code",
            "EMBEDDING_MODEL": "sentence-transformers/all-MiniLM-L6-v2",
        },
    ),
}

_SKIP_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv", "dist", "build"}


def _safe_path(target_repo_dir: str, relative_path: str) -> Path:
    """Resolves relative_path against target_repo_dir and REJECTS any
    attempt to escape the repo root (../.. traversal, absolute paths
    outside the repo) -- the model's tool calls are untrusted input.
    """
    root = Path(target_repo_dir).resolve()
    candidate = (root / relative_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        raise ValueError(f"ruta fuera del repo objetivo, rechazada: {relative_path}")
    return candidate


def _is_inside_git_dir(full_path: Path, target_repo_dir: str) -> bool:
    """write_file nunca debe poder tocar .git/ -- un hook, el config, o los
    refs modificados por el modelo podrian comprometer el repo entero, no
    solo el cambio que se supone que esta haciendo.
    """
    git_dir = (Path(target_repo_dir).resolve() / ".git").resolve()
    try:
        full_path.relative_to(git_dir)
        return True
    except ValueError:
        return False


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... (truncado, {len(text) - limit} caracteres omitidos)"


# Gap real identificado en auditoria ("orquestacion... necesita cache y
# algoritmos"): en una epica secuencial, cada historia resume la MISMA
# conversacion (ver conversation_memory.py) -- si la historia #5 lee un
# archivo que la #2 ya leyo y nadie toco desde entonces, el contenido
# completo se re-embebia en el prompt de nuevo, inflando tokens y
# enterrando al modelo en contenido duplicado en vez de dejarlo razonar
# sobre lo nuevo. Sembrado desde resume_state.get("read_file_hashes")
# (persistido en conversation_file entre historias, mismo mecanismo que
# listed_dirs/consulted_risk_graph) y devuelto de vuelta en _finalize()
# para que la PROXIMA historia lo siga usando.
_read_file_hashes: dict = {}


def tool_read_file(target_repo_dir: str, path: str) -> str:
    try:
        full_path = _safe_path(target_repo_dir, path)
    except ValueError as exc:
        return f"error: {exc}"
    if not full_path.exists():
        return f"error: no existe {path}"
    if not full_path.is_file():
        return f"error: {path} no es un archivo"
    try:
        size = full_path.stat().st_size
    except OSError as exc:
        return f"error leyendo {path}: {exc}"
    if size > _MAX_READ_BYTES:
        return (
            f"error: {path} es demasiado grande ({size} bytes, limite {_MAX_READ_BYTES}) -- "
            "usa grep_search para buscar partes especificas en vez de leerlo entero"
        )
    try:
        content = full_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return f"error leyendo {path}: {exc}"

    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    # Clave = path ABSOLUTO resuelto (no el relativo que pasa el modelo) --
    # con la clave relativa, dos target_repo_dir distintos (ej. dos tests,
    # o dos repos objetivo reales en corridas separadas del mismo proceso)
    # podrian compartir un nombre de archivo relativo (ej. "hello.txt") y
    # colisionar en el cache, devolviendo el puntero "ya leiste esto" para
    # un archivo que en realidad nunca se leyo en ESE repo.
    cache_key = str(full_path)
    if _read_file_hashes.get(cache_key) == content_hash:
        return f"(ya leiste este archivo antes en esta conversacion, sin cambios desde entonces -- hash={content_hash[:12]})"
    _read_file_hashes[cache_key] = content_hash
    return content


def tool_list_directory(target_repo_dir: str, path: str = ".") -> str:
    try:
        full_path = _safe_path(target_repo_dir, path)
    except ValueError as exc:
        return f"error: {exc}"
    if not full_path.is_dir():
        return f"error: {path} no es un directorio"
    entries = sorted(p.name + ("/" if p.is_dir() else "") for p in full_path.iterdir() if p.name not in _SKIP_DIRS)
    if not entries:
        return "(directorio vacio)"
    if len(entries) > _MAX_LIST_ENTRIES:
        shown = entries[:_MAX_LIST_ENTRIES]
        return "\n".join(shown) + f"\n... ({len(entries) - _MAX_LIST_ENTRIES} entradas mas, omitidas)"
    return "\n".join(entries)


def tool_grep_search(target_repo_dir: str, pattern: str, path: str = ".") -> str:
    import re

    try:
        full_path = _safe_path(target_repo_dir, path)
    except ValueError as exc:
        return f"error: {exc}"
    try:
        regex = re.compile(pattern)
    except re.error as exc:
        return f"error: patron invalido: {exc}"

    matches = []
    files_scanned = 0
    for file_path in full_path.rglob("*"):
        if not file_path.is_file() or any(part in _SKIP_DIRS for part in file_path.parts):
            continue
        try:
            if file_path.stat().st_size > _MAX_GREP_FILE_BYTES:
                continue
        except OSError:
            continue

        files_scanned += 1
        if files_scanned > _MAX_GREP_FILES_SCANNED:
            matches.append(f"... (se alcanzo el limite de {_MAX_GREP_FILES_SCANNED} archivos escaneados, resultado parcial)")
            break

        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if regex.search(line):
                rel = file_path.relative_to(Path(target_repo_dir).resolve())
                matches.append(f"{rel}:{lineno}: {line.strip()}")
                if len(matches) >= 200:
                    return "\n".join(matches) + "\n... (resultado truncado a 200 lineas)"
    return "\n".join(matches) if matches else "(sin resultados)"


def _confirm(prompt_text: str) -> bool:
    print(prompt_text, file=sys.stderr)
    answer = input().strip().lower()
    return answer == "s"


_COMMON_CODE_EXTENSIONS = (".ts", ".tsx", ".js", ".jsx")


def _sibling_duplicate_warning(full_path: Path) -> str:
    """Bug real confirmado en vivo (PR real de esta sesion, epica KAN-4): el
    coding agent creo Header.ts Y Header.tsx (mismo componente, dos
    extensiones) en el mismo directorio -- nunca reviso si ya existia una
    version con otra extension antes de escribir una nueva. Best-effort: no
    bloquea (podria haber falsos positivos legitimos), solo avisa fuerte en
    el resultado de la tool para que el modelo pueda autocorregirse.
    """
    if full_path.suffix not in _COMMON_CODE_EXTENSIONS:
        return ""
    for ext in _COMMON_CODE_EXTENSIONS:
        if ext == full_path.suffix:
            continue
        sibling = full_path.with_name(full_path.stem + ext)
        if sibling.exists():
            return (
                f"\n\nADVERTENCIA: ya existe '{sibling.name}' en este mismo directorio -- "
                "¿es este archivo un duplicado del mismo componente con otra extension? Si es asi, "
                "edita el archivo existente en vez de crear uno nuevo, y borra el que sobra."
            )
    return ""


def tool_write_file(target_repo_dir: str, path: str, content: str) -> str:
    try:
        full_path = _safe_path(target_repo_dir, path)
    except ValueError as exc:
        return f"error: {exc}"
    if _is_inside_git_dir(full_path, target_repo_dir):
        return f"error: no se puede escribir dentro de .git/ ({path})"

    is_new_file = not full_path.exists()
    if full_path.exists() and full_path.is_file():
        try:
            old_content = full_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            old_content = ""
        diff_lines = list(
            difflib.unified_diff(
                old_content.splitlines(keepends=True),
                content.splitlines(keepends=True),
                fromfile=f"{path} (actual)",
                tofile=f"{path} (nuevo)",
            )
        )
        print(f"\nEl agente quiere modificar '{path}':", file=sys.stderr)
        print("---", file=sys.stderr)
        print("".join(diff_lines) if diff_lines else "(sin cambios de contenido)", file=sys.stderr)
        print("---", file=sys.stderr)
    else:
        print(f"\nEl agente quiere crear '{path}':", file=sys.stderr)
        print("---", file=sys.stderr)
        print(content, file=sys.stderr)
        print("---", file=sys.stderr)

    if not _confirm("¿Aplicar este cambio? [s/n]: "):
        return "el usuario rechazo este cambio, no se escribio nada"

    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.write_text(content, encoding="utf-8")
    warning = _sibling_duplicate_warning(full_path) if is_new_file else ""
    return f"escrito ok: {path}{warning}"


def tool_edit_file(target_repo_dir: str, path: str, old_string: str, new_string: str) -> str:
    """str_replace-style edit: old_string must match EXACTLY ONCE in the
    file, same discipline as the Edit tool this session already uses --
    cheaper in tokens and produces a much more reviewable diff than
    rewriting the whole file via write_file for a small change.
    """
    try:
        full_path = _safe_path(target_repo_dir, path)
    except ValueError as exc:
        return f"error: {exc}"
    if _is_inside_git_dir(full_path, target_repo_dir):
        return f"error: no se puede escribir dentro de .git/ ({path})"
    if not full_path.exists() or not full_path.is_file():
        return f"error: no existe {path} -- usa write_file para crear un archivo nuevo"

    try:
        current = full_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return f"error leyendo {path}: {exc}"

    count = current.count(old_string)
    if count == 0:
        return f"error: old_string no se encontro en {path} -- verifica que coincida exactamente (incluido whitespace)"
    if count > 1:
        return f"error: old_string aparece {count} veces en {path} -- agrega mas contexto para que sea unico"

    new_content = current.replace(old_string, new_string, 1)
    diff_lines = list(
        difflib.unified_diff(
            current.splitlines(keepends=True),
            new_content.splitlines(keepends=True),
            fromfile=f"{path} (actual)",
            tofile=f"{path} (nuevo)",
        )
    )
    print(f"\nEl agente quiere editar '{path}':", file=sys.stderr)
    print("---", file=sys.stderr)
    print("".join(diff_lines) if diff_lines else "(sin cambios)", file=sys.stderr)
    print("---", file=sys.stderr)
    if not _confirm("¿Aplicar este cambio? [s/n]: "):
        return "el usuario rechazo este cambio, no se edito nada"

    full_path.write_text(new_content, encoding="utf-8")
    return f"editado ok: {path}"


def tool_git_diff(target_repo_dir: str, path: str = "") -> str:
    """Solo lectura -- deja que el agente revise su propio cambio antes de
    declararse "done", en vez de confiar de memoria en lo que escribio.
    """
    cmd = ["git", "-C", target_repo_dir, "diff"]
    if path:
        try:
            _safe_path(target_repo_dir, path)
        except ValueError as exc:
            return f"error: {exc}"
        cmd.extend(["--", path])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return "error: git diff supero el timeout de 30s"
    if result.returncode != 0:
        return f"error corriendo git diff: {result.stderr.strip()}"
    return _truncate(result.stdout, _MAX_SHELL_OUTPUT_CHARS) or "(sin cambios)"


def tool_git_log(target_repo_dir: str, n: int = 10) -> str:
    try:
        n = max(1, min(int(n), 50))
    except (TypeError, ValueError):
        n = 10
    try:
        result = subprocess.run(
            ["git", "-C", target_repo_dir, "log", f"-{n}", "--oneline"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        return "error: git log supero el timeout de 30s"
    if result.returncode != 0:
        return f"error corriendo git log: {result.stderr.strip()}"
    return result.stdout or "(sin commits)"


# Mismo orden de deteccion que scripts/run_module_tests.sh -- de mas
# especifico a mas generico, package.json al final porque es el marcador
# mas generico (cualquier sabor de Node/TS).
_STACK_MARKERS = [
    ("pom.xml", "Maven/Java", "mvn -B -q test"),
    ("go.mod", "Go", "go test ./..."),
    ("Gemfile", "Ruby", "bundle exec rspec"),
    ("Cargo.toml", "Rust", "cargo test"),
    ("Pipfile", "Python (Pipenv)", "pipenv run pytest -q"),
    ("requirements.txt", "Python (pip)", "pytest -q"),
    ("package.json", "Node/TS", "npm test"),
]


def _detect_stack_at(dir_path: Path) -> tuple | None:
    for marker, stack, suggested_cmd in _STACK_MARKERS:
        if (dir_path / marker).exists():
            return stack, suggested_cmd
    if list(dir_path.glob("*.csproj")) or list(dir_path.glob("*.sln")):
        return ".NET", "dotnet test"
    return None


def tool_detect_project_stack(target_repo_dir: str) -> str:
    """Puramente informativo -- no ejecuta nada, solo le dice al agente que
    stack detecto y que comando de verificacion sugiere, para que no tenga
    que adivinarlo. El agente sigue usando run_shell_command (con
    confirmacion) para correrlo de verdad.

    Antes solo miraba la RAIZ del repo -- en un monorepo real (confirmado
    contra ai-agents-code: auth-service/pom.xml, frontend/package.json,
    data-worker/Pipfile, ninguno en la raiz) siempre devolvia "no se
    detecto ningun marcador", y el agente se rendia ahi en vez de seguir
    investigando. Ahora, si la raiz no tiene marcador, escanea las
    subcarpetas de primer nivel (no recursivo completo -- evita bajar a
    node_modules/.git/etc via _SKIP_DIRS) y reporta CADA sub-proyecto
    encontrado con su propio comando de verificacion.
    """
    root = Path(target_repo_dir).resolve()

    root_hit = _detect_stack_at(root)
    if root_hit:
        stack, suggested_cmd = root_hit
        return f"stack detectado: {stack} (por marcador en la raiz) -- comando de verificacion sugerido: {suggested_cmd}"

    sub_hits = []
    for sub in sorted(p for p in root.iterdir() if p.is_dir() and p.name not in _SKIP_DIRS and not p.name.startswith(".")):
        hit = _detect_stack_at(sub)
        if hit:
            stack, suggested_cmd = hit
            sub_hits.append((sub.name, stack, suggested_cmd))

    if not sub_hits:
        return (
            "no se detecto ningun marcador de stack conocido ni en la raiz ni en subcarpetas de primer nivel "
            "-- inspecciona manualmente con list_directory/read_file"
        )

    lines = [f"- {name}/: {stack} -- comando de verificacion sugerido: {cmd}" for name, stack, cmd in sub_hits]
    return (
        f"monorepo detectado con {len(sub_hits)} sub-proyecto(s) (nada en la raiz):\n"
        + "\n".join(lines)
        + "\nUsa list_directory/read_file dentro del sub-proyecto real que vayas a tocar antes de escribir, "
        "y corre el comando de verificacion de ESE sub-proyecto puntual, no uno solo para todo el repo."
    )


def tool_query_sonar(target_repo_dir: str, component: str) -> str:
    """Los hallazgos de Sonar le llegan precomputados una sola vez en el
    prompt inicial -- esto le permite volver a consultarlos en vivo
    (reusando sonar_client.py real, mismo cache) si necesita mas detalle.
    """
    try:
        result = sonar_client.get_issues(component)
    except Exception as exc:
        return f"error consultando Sonar para '{component}': {exc}"
    issues = result.get("issues", [])
    if not issues:
        return f"sin hallazgos abiertos para '{component}'"
    lines = [f"- [{i['severity']}] {i['rule']}: {i['message']} (linea {i['line']})" for i in issues]
    return "\n".join(lines)


def _has_dotnet_project_file(work_dir: Path) -> bool:
    return bool(list(work_dir.glob("*.csproj")) or list(work_dir.glob("*.sln")))


# binario -> (chequeo del marcador real de ese stack en work_dir, imagen
# Docker real) -- MISMO mapeo que scripts/run_module_tests.sh::detect_stack()
# ya usa para el testing-agent real, mantenido en sync a mano (tabla chica y
# estable, no amerita un bridge bash/Python como pipeline_shared.py).
#
# Gap real confirmado en vivo (epica KAN-4/KAN-6, dias de corridas): el
# coding agent corre sus propios comandos de verificacion (npm install/npm
# test, mvn test, etc, via run_shell_command) DIRECTO en el contenedor donde
# corre el propio agente (poc-ai-agents-testrunner) -- deliberadamente
# minimo (Python + git + jq + cypher-shell, ver Dockerfile.testrunner), NO
# trae Node/Maven/Go/Ruby/Rust/.NET. 'npm install' fallaba SIEMPRE con "npm:
# not found", indistinguible para el agente (y para el juez, que solo ve el
# resultado final) de un fallo real del codigo que genero. El testing-agent
# real (run_tests()/scripts/run_module_tests.sh) ya resuelve exactamente
# este problema corriendo cada stack en su propia imagen via
# Docker-outside-of-Docker -- _resolve_stack_image_if_needed aplica el MISMO
# patron a los comandos ad-hoc del coding agent.
_STACK_RUNTIME_IMAGES = {
    "mvn": (lambda d: (d / "pom.xml").exists(), "maven:3.9-eclipse-temurin-17"),
    "mvnw": (lambda d: (d / "pom.xml").exists(), "maven:3.9-eclipse-temurin-17"),
    "dotnet": (_has_dotnet_project_file, "mcr.microsoft.com/dotnet/sdk:8.0"),
    "go": (lambda d: (d / "go.mod").exists(), "golang:1.22"),
    "bundle": (lambda d: (d / "Gemfile").exists(), "ruby:3.3"),
    "rspec": (lambda d: (d / "Gemfile").exists(), "ruby:3.3"),
    "cargo": (lambda d: (d / "Cargo.toml").exists(), "rust:1.78"),
    "pipenv": (lambda d: (d / "Pipfile").exists(), "python:3.10-slim"),
    # Bug real confirmado en vivo (epica KAN-4, historia de bootstrap Ionic/
    # Angular/Capacitor): v1.44.1-jammy trae Node 20.13.1 -- @capacitor/cli
    # y @angular/cli modernos ya piden Node >=22, asi que cualquier scaffold
    # nuevo de esos frameworks fallaba ANTES de poder escribir nada.
    # v1.61.1-noble (confirmado real: `docker run ... node --version` ->
    # v24.17.0) sigue siendo una tag versionada/pinneada, mismo criterio de
    # reproducibilidad que la anterior.
    "npm": (lambda d: (d / "package.json").exists(), "mcr.microsoft.com/playwright:v1.61.1-noble"),
    "npx": (lambda d: (d / "package.json").exists(), "mcr.microsoft.com/playwright:v1.61.1-noble"),
    "node": (lambda d: (d / "package.json").exists(), "mcr.microsoft.com/playwright:v1.61.1-noble"),
    "yarn": (lambda d: (d / "package.json").exists(), "mcr.microsoft.com/playwright:v1.61.1-noble"),
    "pnpm": (lambda d: (d / "package.json").exists(), "mcr.microsoft.com/playwright:v1.61.1-noble"),
}


def _resolve_stack_image_if_needed(command: str, work_dir: Path) -> str | None:
    """None si el comando puede (o debe) correr nativo -- el binario que
    necesita ya esta disponible en este contenedor, no hay Docker para
    containerizar, o no se reconoce ningun runtime de stack en el comando.
    Solo devuelve una imagen cuando containerizar es realmente lo que
    destraba el comando (el binario falta LOCALMENTE pero el marcador real
    del stack SI esta en work_dir), para no pagar overhead de contenedor en
    comandos genericos (git, ls, cat, python3 -- este ultimo YA disponible
    en testrunner, que es python:3.10-slim).
    """
    try:
        first_token = shlex.split(command)[0]
    except (ValueError, IndexError):
        return None
    if shutil.which(first_token):
        return None
    hit = _STACK_RUNTIME_IMAGES.get(first_token)
    if not hit:
        return None
    has_marker, image = hit
    if shutil.which("docker") is None or not has_marker(work_dir):
        return None
    return image


def _run_command_in_stack_container(image: str, command: str, target_repo_dir: str, work_dir: Path) -> str:
    """Docker-outside-of-Docker, mismo patron que orchestration.py::run_tests()
    ya usa: HOST_TARGET_REPO_DIR (seteada solo en corridas via DooD, ver ese
    comentario) es el path real y visible para el daemon del HOST -- el
    daemon no puede montar un path que solo existe DENTRO de este
    contenedor. Sin ella (host real, no DooD), cae a target_repo_dir tal
    cual. Timeout mas generoso que el camino nativo (600s vs 120s): un
    'npm install' real (mas 'npx playwright install --with-deps') puede
    tardar bastante mas que un comando nativo cualquiera.
    """
    target_repo_root = Path(target_repo_dir).resolve()
    rel = work_dir.relative_to(target_repo_root)
    host_target_repo_dir = os.environ.get("HOST_TARGET_REPO_DIR", target_repo_dir)
    host_work_dir = f"{host_target_repo_dir}/{rel.as_posix()}" if str(rel) != "." else host_target_repo_dir

    print(
        f"(el binario que este comando necesita no esta disponible en este contenedor -- "
        f"delegando a la imagen real '{image}' via Docker-outside-of-Docker)",
        file=sys.stderr,
    )
    try:
        result = subprocess.run(
            ["docker", "run", "--rm", "-v", f"{host_work_dir}:/work", "-w", "/work", image, "sh", "-c", command],
            capture_output=True, text=True, timeout=600,
        )
        stdout = _truncate(result.stdout, _MAX_SHELL_OUTPUT_CHARS)
        stderr = _truncate(result.stderr, _MAX_SHELL_OUTPUT_CHARS)
        return f"exit_code={result.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}"
    except subprocess.TimeoutExpired:
        return "error: el comando (containerizado, imagen real) supero el timeout de 600s"


def tool_run_shell_command(target_repo_dir: str, command: str, cwd: str = "") -> str:
    """cwd (opcional, relativo a target_repo_dir): en que subcarpeta correr
    el comando -- sin esto, siempre corria en la RAIZ del repo, lo que
    rompe cualquier comando (npm test, mvn test, pytest) en un monorepo con
    sub-proyectos reales (confirmado contra ai-agents-code: 'npm test'
    fallaba desde la raiz porque el package.json real esta en frontend/,
    no en la raiz). Usa detect_project_stack primero para saber que
    subcarpeta corresponde a cada sub-proyecto.
    """
    try:
        work_dir = _safe_path(target_repo_dir, cwd) if cwd else Path(target_repo_dir).resolve()
    except ValueError as exc:
        return f"error: {exc}"
    if not work_dir.is_dir():
        return f"error: '{cwd}' no es un directorio dentro del repo objetivo"

    print(f"\nEl agente quiere correr en {work_dir}:", file=sys.stderr)
    print(f"  $ {command}", file=sys.stderr)
    prompt = "¿Ejecutar este comando? [s/n]: "
    if _DANGEROUS_COMMAND_PATTERN.search(command):
        print("⚠️  COMANDO POTENCIALMENTE DESTRUCTIVO ⚠️  (ej. borra archivos, reescribe historial de git, o similar)", file=sys.stderr)
        prompt = "¿Ejecutar este comando POTENCIALMENTE DESTRUCTIVO igual? [s/n]: "
    if not _confirm(prompt):
        return "el usuario rechazo ejecutar este comando"

    stack_image = _resolve_stack_image_if_needed(command, work_dir)
    if stack_image:
        return _run_command_in_stack_container(stack_image, command, target_repo_dir, work_dir)

    try:
        result = subprocess.run(
            command,
            shell=True,
            cwd=str(work_dir),
            capture_output=True,
            text=True,
            timeout=120,
            env=_sanitized_subprocess_env(),
        )
        stdout = _truncate(result.stdout, _MAX_SHELL_OUTPUT_CHARS)
        stderr = _truncate(result.stderr, _MAX_SHELL_OUTPUT_CHARS)
        return f"exit_code={result.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}"
    except subprocess.TimeoutExpired:
        return "error: el comando supero el timeout de 120s"


LOCAL_TOOLS = {
    "read_file": {
        "description": "Lee el contenido de un archivo dentro del repo objetivo.",
        "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]},
        "fn": tool_read_file,
    },
    "list_directory": {
        "description": "Lista archivos y subdirectorios de una carpeta del repo objetivo.",
        "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": []},
        "fn": tool_list_directory,
    },
    "grep_search": {
        "description": "Busca un patron (regex) en los archivos del repo objetivo, devuelve archivo:linea: contenido.",
        "input_schema": {
            "type": "object",
            "properties": {"pattern": {"type": "string"}, "path": {"type": "string"}},
            "required": ["pattern"],
        },
        "fn": tool_grep_search,
    },
    "write_file": {
        "description": "Escribe (crea o sobreescribe) un archivo del repo objetivo. Requiere confirmacion humana.",
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string"}, "content": {"type": "string"}},
            "required": ["path", "content"],
        },
        "fn": tool_write_file,
    },
    "run_shell_command": {
        "description": (
            "Corre un comando de shell dentro del repo objetivo (tests, build, etc). Requiere confirmacion "
            "humana. En un monorepo (varios sub-proyectos, ver detect_project_stack), pasa 'cwd' con la "
            "subcarpeta del sub-proyecto real -- sin esto corre en la RAIZ del repo, donde comandos como "
            "'npm test'/'mvn test' van a fallar si el package.json/pom.xml real esta en una subcarpeta."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {"type": "string"},
                "cwd": {"type": "string", "description": "Subcarpeta relativa al repo donde correr el comando (opcional, default: raiz del repo)"},
            },
            "required": ["command"],
        },
        "fn": tool_run_shell_command,
    },
    "edit_file": {
        "description": (
            "Reemplaza old_string por new_string en un archivo existente del repo objetivo. old_string debe "
            "matchear EXACTAMENTE UNA VEZ (agrega contexto si no es unico). Preferir esto sobre write_file "
            "para cambios chicos en archivos existentes. Requiere confirmacion humana."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "old_string": {"type": "string"},
                "new_string": {"type": "string"},
            },
            "required": ["path", "old_string", "new_string"],
        },
        "fn": tool_edit_file,
    },
    "git_diff": {
        "description": "Muestra el diff real (git diff) del repo objetivo, opcionalmente acotado a un path. Solo lectura.",
        "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": []},
        "fn": tool_git_diff,
    },
    "git_log": {
        "description": "Muestra los ultimos N commits (git log --oneline) del repo objetivo. Solo lectura.",
        "input_schema": {"type": "object", "properties": {"n": {"type": "integer"}}, "required": []},
        "fn": tool_git_log,
    },
    "detect_project_stack": {
        "description": (
            "Detecta el stack del repo objetivo (Maven/Go/Ruby/Rust/Python/Node/.NET) por su archivo de "
            "proyecto y sugiere el comando de test/build -- no ejecuta nada, solo informa. Si no hay marcador "
            "en la raiz, escanea subcarpetas de primer nivel y reporta cada sub-proyecto de un monorepo por "
            "separado. Solo lectura."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
        "fn": tool_detect_project_stack,
    },
    "query_sonar": {
        "description": "Consulta hallazgos REALES y actuales de SonarQube para un componente (mismo cliente que alimenta el pipeline). Solo lectura.",
        "input_schema": {"type": "object", "properties": {"component": {"type": "string"}}, "required": ["component"]},
        "fn": tool_query_sonar,
    },
}


def _local_tools_to_anthropic_format() -> list:
    return [
        {"name": name, "description": spec["description"], "input_schema": spec["input_schema"]}
        for name, spec in LOCAL_TOOLS.items()
    ]


def _build_user_prompt(ticket_id: str, sanitized_prompt: str, target_repo_dir: str) -> str:
    # No se precarga el listado de la raiz del repo (antes se pagaba ese
    # costo en TODAS las corridas, aunque el ticket no lo necesitara) -- el
    # modelo puede llamar list_directory(".") el mismo si le hace falta, lo
    # que ya cuenta como investigacion para la regla obligatoria.
    return f"""Ticket: {ticket_id}
Repo objetivo: {target_repo_dir}

{sanitized_prompt}"""


async def run_coding_agent(
    ticket_id: str,
    sanitized_prompt: str,
    target_repo_dir: str,
    resume_messages: list = None,
    resume_state: dict = None,
) -> dict:
    """Si resume_messages viene seteado (reintento tras un veredicto FLAGGED
    retryable del juez -- ver retry_coding_agent_with_feedback() en
    run_poc_loop.sh / _retry_local_diff() en orchestration.py), la
    conversacion CONTINUA en vez de arrancar de cero: sanitized_prompt pasa
    a ser solo el feedback del juez (un turno de usuario nuevo), no el
    ticket completo -- evita repagar la investigacion (listar el repo, leer
    archivos) ya hecha en el primer intento. resume_state siembra
    has_investigated/has_run_verification con lo ya alcanzado.
    """
    backend = _select_backend()
    logger.info(f"coding agent: usando backend '{backend}'")
    if backend == "none":
        return {"status": "blocked", "summary": "ni ANTHROPIC_API_KEY ni Ollama disponibles", "files_changed": [], "_meta": {}}
    # El probe real (GET /api/tags) solo tiene sentido si el backend elegido
    # es Ollama -- con Anthropic como backend inicial, ollama_model_state
    # igual se arma (call_with_fallback puede caer a Ollama EN VIVO durante
    # el loop si Anthropic falla a mitad de corrida) pero sin gastar una
    # llamada de red de arranque que no aporta nada en el caso comun.
    if backend == "ollama":
        ollama_model_state = init_ollama_model_state(CODING_AGENT_OLLAMA_MODELS, logger, "coding agent")
    else:
        ollama_model_state = {"active": CODING_AGENT_OLLAMA_MODELS[0], "tried": set(), "switch_used": False}

    start_time = time.monotonic()
    total_input_tokens = 0
    total_output_tokens = 0
    resume_state = resume_state or {}
    has_investigated = bool(resume_state.get("has_investigated"))
    has_run_verification = bool(resume_state.get("has_run_verification"))
    initial_plan = resume_state.get("initial_plan")
    consulted_risk_graph = bool(resume_state.get("consulted_risk_graph"))
    listed_dirs = set(resume_state.get("listed_dirs") or [])
    # _read_file_hashes es un dict a nivel de modulo (no una variable local
    # como listed_dirs) porque tool_read_file() se llama via el dict
    # LOCAL_TOOLS con una firma fija (target_repo_dir, path), sin acceso al
    # closure de esta funcion. .clear() primero evita que corridas previas
    # DENTRO DEL MISMO PROCESO (ej. tests, o un futuro caller que reuse el
    # proceso) filtren hashes de un target_repo_dir/conversacion distinta --
    # en produccion cada corrida real ya es un subproceso nuevo (python3
    # coding_agent.py <payload>), asi que esto es defensivo, no el caso comun.
    _read_file_hashes.clear()
    _read_file_hashes.update(resume_state.get("read_file_hashes") or {})
    verification_nudge_given = False
    self_review_nudge_given = False
    tool_call_nudge_given = False
    consecutive_eof_errors = 0

    def _finalize(result: dict) -> dict:
        result["self_verified"] = has_run_verification
        result["initial_plan"] = initial_plan
        result["consulted_risk_graph"] = consulted_risk_graph
        result["_meta"] = {
            "backend": backend,
            "latency_seconds": round(time.monotonic() - start_time, 2),
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "estimated_cost_usd": round(_estimate_cost_usd(backend, ANTHROPIC_MODEL, total_input_tokens, total_output_tokens), 6),
        }
        # Se guarda en un archivo temporal (no en stdout/el log JSONL) para
        # no inflar logs/coding_agent_runs.jsonl con la conversacion
        # completa en cada corrida normal -- solo un reintento la necesita.
        conversation_state = {
            "messages": messages,
            "has_investigated": has_investigated,
            "has_run_verification": has_run_verification,
            "initial_plan": initial_plan,
            "consulted_risk_graph": consulted_risk_graph,
            "listed_dirs": sorted(listed_dirs),
            "read_file_hashes": dict(_read_file_hashes),
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", prefix="coding_agent_conversation_", delete=False, encoding="utf-8"
        ) as f:
            json.dump(conversation_state, f, ensure_ascii=False)
            result["_conversation_file"] = f.name
        return result

    async with AsyncExitStack() as stack:
        sessions = await _connect_mcp_servers(stack, MCP_SERVERS, label="coding agent")

        tools = list(_local_tools_to_anthropic_format())
        for name, session in sessions.items():
            try:
                listed = await session.list_tools()
                tools.extend(_normalize_tool_schema(name, listed.tools))
            except Exception as exc:
                logger.warning(f"coding agent: no se pudieron listar tools de '{name}': {exc}")

        if resume_messages:
            messages = list(resume_messages)
            messages.append({"role": "user", "content": sanitized_prompt})
        else:
            messages = [{"role": "user", "content": _build_user_prompt(ticket_id, sanitized_prompt, target_repo_dir)}]

        async with httpx.AsyncClient() as client:
            for _ in range(MAX_TOOL_TURNS):
                content, stop_reason, usage, backend = await call_with_fallback(
                    client, messages, tools, CODING_AGENT_SYSTEM_PROMPT,
                    ollama_model=ollama_model_state["active"], force_json=True,
                )
                total_input_tokens += usage.get("input_tokens", 0)
                total_output_tokens += usage.get("output_tokens", 0)
                messages.append({"role": "assistant", "content": content})

                if stop_reason != "tool_use":
                    final_text = next((b["text"] for b in content if b.get("type") == "text"), "")

                    if (
                        tool_call_nudge_given
                        and _TOOL_CALL_REFUSAL_PATTERN.search(final_text)
                        and maybe_switch_ollama_model(
                            ollama_model_state, backend, CODING_AGENT_OLLAMA_MODELS, logger,
                            "coding agent", "se nego a llamar la tool incluso tras el nudge",
                        )
                    ):
                        continue

                    if not tool_call_nudge_given and _TOOL_CALL_REFUSAL_PATTERN.search(final_text):
                        # El modelo anuncio una negativa a llamar la tool en
                        # vez de llamarla -- un nudge especifico ataca la
                        # causa real, a diferencia del reintento generico de
                        # "dame JSON valido" (que solo le pide reformatear la
                        # misma negativa como JSON, no que actue). No
                        # depende de has_investigated -- confirmado real que
                        # el modelo puede rendirse ANTES de investigar con
                        # exito (ej. adivino mal una ruta), y en ese caso el
                        # nudge tiene que mandarlo a investigar bien primero,
                        # no solo repetirle "llama la tool ya".
                        tool_call_nudge_given = True
                        nudge = TOOL_CALL_NUDGE_MESSAGE if has_investigated else TOOL_CALL_NUDGE_MESSAGE_NEEDS_INVESTIGATION
                        messages.append({"role": "user", "content": nudge})
                        continue

                    try:
                        result = _extract_json(final_text)
                        if result.get("status") not in ("done", "blocked"):
                            # Real: ornith:9b devolvio JSON sintacticamente
                            # valido pero con el esquema equivocado
                            # ({"plan": "..."} en vez de {"status":...,
                            # "summary":...}) -- sin este chequeo, el parseo
                            # "exitoso" hacia que este resultado ambiguo (sin
                            # status) se aceptara tal cual como final,
                            # dejando el ticket en un estado indefinido en
                            # vez de disparar el mismo reintento de
                            # correccion que ya existe para JSON invalido.
                            raise json.JSONDecodeError("falta 'status' valido en el JSON", final_text, 0)
                    except json.JSONDecodeError:
                        # Un solo reintento acotado antes de degradar a blocked.
                        retry_text, retry_usage = await _final_text_with_json_retry(
                            client, backend, messages, tools, CODING_AGENT_SYSTEM_PROMPT,
                            ollama_model=ollama_model_state["active"], json_schema=CODING_AGENT_RESULT_SCHEMA,
                        )
                        total_input_tokens += retry_usage.get("input_tokens", 0)
                        total_output_tokens += retry_usage.get("output_tokens", 0)
                        try:
                            result = _extract_json(retry_text)
                            if result.get("status") not in ("done", "blocked"):
                                raise json.JSONDecodeError("falta 'status' valido en el JSON", retry_text, 0)
                        except json.JSONDecodeError:
                            if maybe_switch_ollama_model(
                                ollama_model_state, backend, CODING_AGENT_OLLAMA_MODELS, logger,
                                "coding agent", "JSON invalido incluso tras el reintento de correccion",
                            ):
                                continue
                            return _finalize({"status": "blocked", "summary": retry_text[:500], "files_changed": []})

                    if (
                        result.get("status") == "blocked"
                        and _JSON_CONFUSION_BLOCKED_PATTERN.search(result.get("summary", ""))
                        and maybe_switch_ollama_model(
                            ollama_model_state, backend, CODING_AGENT_OLLAMA_MODELS, logger,
                            "coding agent",
                            "reporto 'blocked' citando su propia confusion sobre el formato JSON, no una ambiguedad real del ticket",
                        )
                    ):
                        continue

                    if result.get("status") == "done" and not has_run_verification and not verification_nudge_given:
                        # Un solo empujon -- si en el turno extra tampoco
                        # verifica, se acepta igual (self_verified queda en
                        # false, trazado en el log), no se bloquea infinito.
                        verification_nudge_given = True
                        messages.append({"role": "user", "content": VERIFY_BEFORE_DONE_MESSAGE})
                        continue

                    if result.get("status") == "done" and not _has_valid_self_review(result) and not self_review_nudge_given:
                        # Mismo criterio de un solo empujon que la verificacion:
                        # si tampoco completa self_review la segunda vez, se
                        # acepta igual (queda trazado como faltante en el log,
                        # no se bloquea infinito).
                        self_review_nudge_given = True
                        messages.append({"role": "user", "content": SELF_REVIEW_NUDGE_MESSAGE})
                        continue

                    return _finalize(result)

                if initial_plan is None:
                    plan_text = next((b["text"] for b in content if b.get("type") == "text" and b.get("text", "").strip()), None)
                    if plan_text:
                        initial_plan = plan_text

                tool_results = []
                for block in content:
                    if block.get("type") != "tool_use":
                        continue
                    name = block["name"]
                    tool_input = block.get("input", {})
                    try:
                        if name in ("write_file", "edit_file", "run_shell_command") and not has_investigated:
                            output = (
                                "Todavia no investigaste el repo. Usa read_file, list_directory, o "
                                "grep_search antes de escribir o ejecutar algo."
                            )
                        elif (
                            name == "run_shell_command"
                            and _SCAFFOLD_COMMAND_PATTERN.search(str(tool_input.get("command", "")))
                            and _normalize_listed_dir(tool_input.get("cwd", "")) not in listed_dirs
                        ):
                            scaffold_dir = tool_input.get("cwd") or "."
                            output = (
                                f"Este comando crea estructura de proyecto NUEVA (scaffolding) en '{scaffold_dir}' -- "
                                f"antes de correrlo, usa list_directory con path=\"{scaffold_dir}\" para confirmar si "
                                "ya existe estructura real ahi (ej. un src/ existente) que deberias extender en vez "
                                "de duplicar."
                            )
                        elif name in LOCAL_TOOLS:
                            output = LOCAL_TOOLS[name]["fn"](target_repo_dir, **tool_input)
                            if name in ("read_file", "list_directory", "grep_search") and not str(output).startswith("error:"):
                                has_investigated = True
                            if name == "list_directory":
                                listed_dirs.add(_normalize_listed_dir(tool_input.get("path", ".")))
                            if name == "run_shell_command" and _VERIFICATION_COMMAND_PATTERN.search(str(tool_input.get("command", ""))):
                                has_run_verification = True
                        else:
                            if name.startswith("neo4j-cypher__"):
                                consulted_risk_graph = True
                            output = await _call_mcp_tool(sessions, name, tool_input)
                    except EOFError:
                        # Bug real confirmado en vivo (operacion de esta
                        # noche): sin stdin conectada (corrida detached),
                        # CADA confirmacion interactiva (_confirm) lanza
                        # EOFError -- el catch generico de abajo lo
                        # convertia en un tool-result mas ("error llamando a
                        # la herramienta: EOF when reading a line"), y el
                        # modelo interpretaba eso como un bug de la tool
                        # (intentaba heredocs/escapes distintos durante
                        # ~15 turnos) en vez de que el sistema reconociera
                        # "no hay terminal interactiva" y bloqueara limpio.
                        consecutive_eof_errors += 1
                        if consecutive_eof_errors >= 2:
                            return _finalize({
                                "status": "blocked",
                                "summary": (
                                    "No hay una terminal interactiva conectada para confirmar cambios (EOF al "
                                    "pedir confirmacion) -- correr con stdin conectado (ej. 'yes s | docker run "
                                    "-i ...' o -it) si se espera que el agente escriba archivos."
                                ),
                                "files_changed": [],
                            })
                        output = "error: no se pudo leer la confirmacion (EOF) -- reintentando"
                    except Exception as exc:
                        output = f"error llamando a la herramienta: {exc}"
                    tool_results.append({"type": "tool_result", "tool_use_id": block["id"], "content": str(output)})
                messages.append({"role": "user", "content": tool_results})
                compact_old_tool_results(messages, _READ_ONLY_TOOL_NAMES)
                warn_if_context_large(messages, logger, "coding agent", backend=backend, system_prompt=CODING_AGENT_SYSTEM_PROMPT)

    return _finalize({"status": "blocked", "summary": "se agotaron los turnos de herramientas sin terminar", "files_changed": []})


def _extract_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


def log_run(ticket_id: str, result: dict):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    meta = result.pop("_meta", {})
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ticket_id": ticket_id,
        **result,
        **meta,
    }
    with RUN_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "usage: coding_agent.py <payload.json>"}), file=sys.stderr)
        sys.exit(1)

    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    ticket_id = payload["ticket_id"]

    try:
        result = asyncio.run(
            run_coding_agent(
                ticket_id,
                payload["sanitized_prompt"],
                payload["target_repo_dir"],
                resume_messages=payload.get("resume_messages"),
                resume_state=payload.get("resume_state"),
            )
        )
    except KeyError as exc:
        print(json.dumps({"error": f"missing_env_var:{exc.args[0]}"}), file=sys.stderr)
        sys.exit(1)
    except (httpx.HTTPError, RuntimeError) as exc:
        print(json.dumps({"error": "coding_agent_call_failed", "detail": str(exc)}), file=sys.stderr)
        sys.exit(1)

    log_run(ticket_id, dict(result))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
