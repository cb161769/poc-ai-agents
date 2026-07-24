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

MAX_TOOL_TURNS = int(os.environ.get("CODING_AGENT_MAX_TURNS", "20"))
# Gap real confirmado en vivo (epica KAN-4, esta sesion): un cambio de
# modelo (maybe_switch_ollama_model) pasa a un backend distinto
# PRECISAMENTE porque el actual esta fallando -- pero el loop de turnos
# seguia contando desde el MISMO contador compartido, asi que si el cambio
# pasaba tarde (ej. turno 10 de 15), el modelo nuevo solo tenia 5 turnos
# para investigar y resolver un ticket entero desde cero. Este piso
# garantiza que un cambio de modelo real siempre deja un numero razonable
# de turnos utilizables, sin importar cuantos ya se gastaron antes del
# cambio.
MIN_TURNS_AFTER_MODEL_SWITCH = int(os.environ.get("CODING_AGENT_MIN_TURNS_AFTER_SWITCH", "8"))

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
# Mucho mas chico que _MAX_READ_BYTES a proposito -- esto se repite en CADA
# intento fallido de edit_file (no es un read_file explicito), asi que tiene
# que ser barato en contexto, no una copia completa del archivo.
_EDIT_ERROR_CONTEXT_CHARS = 4000
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

# Gap real confirmado en vivo (epica KAN-4, qwen3:8b, conversacion continuada
# entre historias de una epica): el modelo reporto "done" con files_changed
# listando archivos especificos y una narrativa detallada de lo que "creo",
# sin llamar write_file/edit_file NI UNA VEZ en todo el turno -- una
# fabricacion, no una escritura fallida. La red de seguridad de
# orchestration.py (confia en git, nunca en este status autoreportado) evito
# que se mergeara, pero el turno se desperdicio y el comentario de Jira que
# SI se postea repite la narrativa fabricada, pudiendo confundir a un humano
# que no revisa el diff real.
NO_REAL_WRITE_NUDGE_MESSAGE = (
    "Dijiste que creaste o modificaste archivos (campo files_changed), pero no llamaste a write_file ni "
    "edit_file en este turno -- ni una sola vez. Si el cambio ya esta hecho de verdad, llama a la tool "
    "correspondiente AHORA MISMO con el contenido real. Si en realidad no pudiste hacerlo, respondé de "
    'nuevo con {"status": "blocked", ...} explicando honestamente por que, en vez de reportar "done".'
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

Bug real confirmado en vivo (epica KAN-4, qwen3:8b): un ticket pedia un componente de boton reutilizable con \
CINCO estados nombrados (Primary, Secondary, Outline, Disabled, Loading) -- implementaste (y testeaste) SOLO \
Primary, y te declaraste "done" igual. El juez lo marco scope-mismatch por incompleto, y ni el reintento lo \
corrigio (misma omision, otra vez). Si el ticket enumera VARIOS items nombrados (estados, variantes, casos, \
endpoints, campos, tipos de error) que tu cambio tiene que cubrir, tu "Plan:" inicial tiene que listarlos TODOS \
explicitamente como una checklist -- y antes de declararte "done", releé esa checklist contra tu diff real \
(git_diff) y confirma UNO POR UNO que cada item tiene codigo Y test real, no solo el primero de la lista. Si \
genuinamente no llegas a cubrir todos por falta de tiempo/turnos, marcá "blocked" (o "done" con "tests_adequate": \
false) explicando EXACTAMENTE que items faltan -- nunca te declares "done" sin resolver dejando items de la \
lista sin tocar y sin decirlo.

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

Bug real confirmado en vivo (epica KAN-4, qwen3:8b): un comando de scaffolding ("ionic start", "ng new", \
"create-react-app", etc.) rechaza correr dentro de un directorio que ya tiene archivos -- eso es intencional \
del CLI, NO un problema a resolver borrando esos archivos. Confirmado real: tras varios rechazos asi, el \
modelo corrio "rm -rf frontend && ionic start frontend ..." -- un comando destructivo que se auto-aprobo (sin \
un humano mirando en vivo) y borro un sub-proyecto real entero (tests, config, codigo ya commiteado) sin \
volver a crear nada en su lugar cuando la segunda mitad del comando fallo. NUNCA uses "rm -rf" (ni ningun \
comando destructivo) sobre un directorio con codigo real existente para "resolver" que un scaffolder se niegue \
a correr ahi -- si el directorio ya tiene estructura real, esa es la señal de que NO hace falta (o no se puede) \
scaffoldear de cero: investigá con list_directory/read_file qué hay, y agregá/editá lo que falta a mano \
(package.json, tsconfig.json, capacitor.config.json, los archivos de Angular/Ionic que necesites) en vez de \
borrar para volver a empezar. Si genuinamente creés que hace falta borrar algo real, marcá "blocked" y \
explicá por qué en el summary -- nunca lo ejecutes vos mismo sin que quede como una decisión explícita y \
revisable, no como un intento mas de una cadena de comandos que fueron fallando.

Bug real confirmado en vivo (epica KAN-4, qwen3:8b): editaste package.json para agregar una dependencia nueva \
("@capacitor/firebase-auth") que en realidad no existe en npm con ese nombre exacto -- el nombre real de un \
paquete (sobre todo plugins de nicho de Capacitor/Ionic) no es algo que puedas confiar de memoria, y un nombre \
inventado rompe "npm install" para TODO el sub-proyecto, no solo tu cambio puntual. Antes de agregar CUALQUIER \
dependencia nueva a package.json (no antes de editar una version YA presente), usa la tool verify_npm_package \
para confirmar que existe de verdad ANTES de escribir la edicion -- MISMO criterio para cualquier otro \
manejador de paquetes: verify_pip_package (requirements.txt/Pipfile), verify_maven_package (pom.xml), \
verify_go_module (go.mod), verify_gem_package (Gemfile), verify_cargo_crate (Cargo.toml), verify_nuget_package \
(.csproj) -- todas consultan el registro REAL del ecosistema correspondiente, nunca asumas que un nombre/version \
suena bien solo porque el formato es correcto. Si la verificacion dice que no existe o no encontras el nombre \
exacto, no adivines una alternativa parecida: marcá "blocked" y explicá que dependencia necesitabas y por que \
no pudiste confirmarla.

Segundo bug real confirmado en vivo, mismo mecanismo (epica KAN-4, corrida siguiente): agregaste \
"@ionic/cli": "^6.30.0" -- el PAQUETE existe (verify_npm_package hubiera dicho que si), pero esa VERSION \
especifica nunca se publico (las versiones reales de @ionic/cli saltan de 6.20.9 a 7.0.0 directo) -- "npm \
install" rompe igual de fuerte con un rango de version inventado que con un nombre inventado. verify_npm_package \
te devuelve la lista REAL de versiones publicadas -- antes de escribir CUALQUIER numero de version en \
package.json (no solo el nombre del paquete), confirma que ese numero exacto (o un rango que genuinamente \
cubra una version real de la lista) aparece ahi. No asumas que una version "suena razonable" solo porque el \
formato es correcto.

Bug real confirmado en vivo (epica KAN-4, scaffold Angular/Ionic): un archivo con decoradores de Angular \
(@NgModule, @Component, @Injectable) sin un tsconfig.json real en ese sub-proyecto con \
"experimentalDecorators": true (y "emitDecoratorMetadata": true) rompe el test runner con un error de \
sintaxis al importar ese archivo ("Invalid or unexpected token") -- el decorador no es JS/TS valido sin esa \
config. Si vos mismo creaste/escribiste el primer archivo con un decorador de Angular en un sub-proyecto \
nuevo, usa la tool check_tsconfig_decorators sobre el tsconfig.json de ESE sub-proyecto ANTES de darte por \
"done" -- no asumas que el scaffold de Ionic/Angular ya lo dejo armado.

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

Bug real confirmado en vivo (epica KAN-4, KAN-5, dos corridas seguidas con el mismo veredicto): un test que \
EXISTE pero no cumple estos dos puntos igual cuenta como "tests_adequate": false -- (1) toda dependencia \
externa/inyectada (servicios, HTTP, storage, guards) que tu codigo nuevo usa tiene que estar mockeada \
explicitamente en el test (jest.fn(), spyOn, o el equivalente real del framework), nunca invocada de verdad; \
(2) todo metodo async/Promise que tu codigo nuevo introduce tiene que verificarse con await/resolves/rejects \
(o fakeAsync/tick si es Angular), nunca asumido sincronico ni dejado sin awaitear en el test. Un test que solo \
llama la funcion y chequea que no tira error, sin mockear sus dependencias ni esperar su resultado async, NO \
alcanza aunque pase en verde.

Cuando termines (con exito o porque no podes seguir), respondé con texto plano que sea UNICAMENTE un \
objeto JSON, sin texto antes ni despues, con este esquema exacto: {"status": "done" o "blocked", \
"summary": "que hiciste, que verificaste, o por que no pudiste", "files_changed": ["ruta1", "ruta2", ...], \
"self_review": {"scope_matches_ticket": true o false, "no_secrets_introduced": true o false, \
"tests_adequate": true o false}}. self_review es tu propia autocritica ANTES de declararte "done" -- \
contestala con honestidad, no la completes con true por defecto: "scope_matches_ticket" es false si tu \
diff toca algo que el ticket no pidio, "no_secrets_introduced" es false si escribiste algo que se parezca \
a un secreto real, "tests_adequate" es false si lo que corriste no cubre genuinamente el cambio."""

# Gap real confirmado en vivo (epica KAN-4, KAN-5, dos corridas seguidas con
# el mismo veredicto insufficient-test-coverage): el reintento del coding
# agent CONTINUA la misma conversacion larga (investigacion + implementacion
# + intentos previos de test) -- para cuando llega a escribir el test, viene
# arrastrando el peso de esa conversacion. Este prompt es para una
# conversacion NUEVA, sin esa carga, cuyo UNICO trabajo es escribir/mejorar
# tests para un diff que YA existe -- mismo mecanismo de tools/gates que
# CODING_AGENT_SYSTEM_PROMPT (investigar antes de escribir, confirmacion
# humana, JSON final), pero enfocado en un objetivo mucho mas chico. Ver
# run_test_writer_subagent().
TEST_WRITER_SYSTEM_PROMPT = """Sos un especialista en testing, trabajando sobre un repositorio git real que ya \
tiene un cambio de codigo REAL aplicado (ver el diff en el prompt de abajo) -- ese cambio ya fue implementado \
por otro agente, tu UNICO trabajo es escribir o mejorar los tests que lo cubran. NO toques el codigo de \
produccion (el diff ya aplicado) salvo que sea estrictamente necesario para poder testearlo (ej. exportar algo \
que no estaba exportado) -- tu cambio deberia ser casi enteramente en archivos de test.

Tenes las mismas herramientas reales que un coding agent: leer/listar/buscar archivos, editar/escribir, correr \
comandos de shell (con confirmacion humana), ver tu propio diff, detectar el stack, verificar si un paquete \
npm existe de verdad (verify_npm_package), y confirmar que un tsconfig.json soporta decoradores de Angular si \
hace falta (check_tsconfig_decorators).

Antes de escribir nada, usa read_file/git_diff para ver EXACTAMENTE que cambio de codigo real tenes que \
cubrir -- no asumas el contenido de memoria del prompt.

Reglas concretas para que un test cuente como adecuado (la misma vara que usa el juez que evalua esto despues, \
no una formalidad): (1) toda dependencia externa/inyectada (servicios, HTTP, storage, guards, cualquier cosa \
que el codigo nuevo importe e invoque) tiene que estar mockeada explicitamente (jest.fn(), spyOn, o el \
equivalente real del framework) -- nunca invocada de verdad en un test unitario; (2) todo metodo async/Promise \
que el codigo nuevo introduce tiene que verificarse con await/resolves/rejects (o fakeAsync/tick si es \
Angular), nunca asumido sincronico; (3) al menos UN caso negativo/de error (entrada invalida, recurso \
inexistente, permiso denegado, lo que corresponda) ademas del camino feliz; (4) el assert tiene que validar un \
comportamiento real (una rama condicional, un error, un cambio de estado) -- nunca solo "no tira excepcion".

REGLA OBLIGATORIA: no podes usar write_file, edit_file, ni run_shell_command hasta haber usado al menos una \
vez read_file, list_directory, o grep_search en esta corrida.

El diff que te pasan puede incluir cambios de intentos anteriores que NO son tu problema (ej. un bump de \
version de una dependencia en package.json/pom.xml/requirements.txt). NO evalues si esos cambios son una buena \
idea, NO los cuestiones, NO te bloquees por ellos -- tu unico trabajo es escribir tests para el codigo de \
aplicacion nuevo o modificado. Si un cambio en el diff no tiene codigo de aplicacion testeable (solo config o \
dependencias), ignoralo y segui con el resto.

Antes de declararte "done", corré los tests reales con run_shell_command (el comando del sub-proyecto real, \
con el `cwd` correspondiente) y confirma que pasan -- un test que no corriste no cuenta como agregado.

Cuando termines, respondé con texto plano que sea UNICAMENTE un objeto JSON, sin texto antes ni despues, con \
este esquema exacto: {"status": "done" o "blocked", "summary": "que tests agregaste/mejoraste, que verificaste, \
o por que no pudiste", "files_changed": ["ruta1", "ruta2", ...], "self_review": {"scope_matches_ticket": true, \
"no_secrets_introduced": true o false, "tests_adequate": true o false}}. "scope_matches_ticket" queda fijo en \
true (tu alcance ES escribir tests, no hay otro alcance que evaluar) -- "tests_adequate" es tu propia \
autocritica honesta contra las 4 reglas de arriba, no la completes con true por defecto."""

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


def _json_validation_error(path: str, content: str) -> str | None:
    """Si 'path' termina en .json, confirma que 'content' parsea como JSON \
    valido -- None si esta bien (o si 'path' no es .json, nada que validar).

    Gap real confirmado en vivo (epica KAN-4, qwen3:8b): un edit_file sobre \
    package.json dejo JSON invalido (una llave de cierre "}" en el medio del \
    objeto, seguida de mas propiedades sueltas) -- recien se detecto varios \
    turnos despues, cuando el testing agent real corrio "npm install" y \
    rompio con EJSONPARSE. Para ese momento el turno donde se corrompio el \
    archivo ya habia quedado atras, sin feedback inmediato -- el reintento \
    con el feedback del juez volvio a fallar porque nunca vio el error real \
    de sintaxis, solo "los tests fallaron". Rechazar la escritura ACA (antes \
    de aplicarla) le da al modelo la chance de corregirlo en el MISMO turno.
    """
    if not path.endswith(".json"):
        return None

    def _reject_duplicates(pairs):
        seen = {}
        for key, value in pairs:
            if key in seen:
                raise ValueError(f"clave duplicada '{key}' dentro del mismo objeto")
            seen[key] = value
        return seen

    try:
        # object_pairs_hook (no el default object_hook) ve CADA par
        # key/value en el orden real en que aparecen en el texto, antes de
        # que el dict final descarte duplicados en silencio -- necesario
        # para detectar el segundo gap real de abajo.
        json.loads(content, object_pairs_hook=_reject_duplicates)
    except json.JSONDecodeError as exc:
        return (
            f"error: ese contenido para '{path}' NO es JSON valido ({exc}) -- no se escribio nada. "
            "Revisa llaves/comas/corchetes con cuidado (un error comun: dejar una '}' de cierre vieja en el "
            "medio del objeto al insertar propiedades nuevas) y reintenta con el JSON completo bien formado."
        )
    except ValueError as exc:
        # Gap real confirmado en vivo (epica KAN-4, qwen3:8b, MISMO ticket,
        # reintentos sucesivos sobre package.json): cada intento de
        # edit_file re-insertaba el bloque "devDependencies" ENTERO en vez
        # de reemplazar el existente -- el resultado (5 copias del mismo
        # bloque, una por reintento) es JSON TECNICAMENTE VALIDO (json.loads
        # normal lo acepta sin quejarse -- claves duplicadas son legales en
        # JSON, la ultima gana) asi que el chequeo de sintaxis de arriba no
        # lo atrapaba -- recien se notaba al leer el archivo a simple vista.
        # object_pairs_hook detecta la clave repetida en el momento, antes
        # de escribir nada.
        return (
            f"error: ese contenido para '{path}' tiene una {exc} -- es JSON tecnicamente valido (las claves "
            "duplicadas no rompen el parseo, la ultima gana) pero es evidencia real de que reinsertaste un "
            "bloque que ya existia en vez de editar el existente. No se escribio nada -- usa read_file para ver "
            "el contenido REAL actual antes de reintentar, y edita/reemplaza el bloque existente en vez de "
            "agregar uno nuevo al lado."
        )
    return None


_KNOWN_EXTENSIONLESS_FILENAMES = {
    "dockerfile", "makefile", "license", "readme", "gemfile", "rakefile",
    "procfile", "vagrantfile", "gruntfile", "gulpfile", "berksfile",
}


def _looks_like_directory_placeholder(path: str, content: str) -> bool:
    """Gap real confirmado en vivo (epica KAN-4, KAN-2 y KAN-10, dos
    corridas distintas): el modelo "crea un directorio" llamando a
    write_file con el path del directorio mismo (ej.
    'frontend/src/components') y contenido descriptivo (ej. "Directory
    for reusable components") en vez de mkdir -- el resultado es un
    ARCHIVO real con ese nombre, que despues bloquea cualquier intento
    real de escribir archivos DENTRO de esa ruta (confirmado: rompio la
    implementacion Y el test-writer sub-agent en tickets distintos).
    Heuristica: sin extension real, contenido de una sola linea corta que
    lee como descripcion (sin puntuacion de codigo).
    """
    basename = path.rsplit("/", 1)[-1]
    if "." in basename or basename.lower() in _KNOWN_EXTENSIONLESS_FILENAMES:
        return False
    stripped = content.strip()
    if not stripped or len(stripped) > 200 or "\n" in stripped:
        return False
    return not re.search(r"[{}();=<>]", stripped)


def tool_write_file(target_repo_dir: str, path: str, content: str) -> str:
    try:
        full_path = _safe_path(target_repo_dir, path)
    except ValueError as exc:
        return f"error: {exc}"
    if _is_inside_git_dir(full_path, target_repo_dir):
        return f"error: no se puede escribir dentro de .git/ ({path})"
    json_error = _json_validation_error(path, content)
    if json_error:
        return json_error

    is_new_file = not full_path.exists()
    if is_new_file and _looks_like_directory_placeholder(path, content):
        return (
            f"error: '{path}' no tiene extension y el contenido ('{content.strip()}') parece una descripcion, "
            "no codigo real -- esto crea un ARCHIVO con ese nombre, que despues bloquea cualquier intento real "
            f"de escribir archivos DENTRO de esa ruta (confirmado real: rompio tickets anteriores de esta forma). "
            f"Si necesitas la carpeta '{path}' para poner archivos reales adentro, no hace falta crearla aparte -- "
            f"llama a write_file directamente con la ruta completa del archivo real (ej. '{path}/index.ts') y se "
            "crea el directorio automaticamente. Si de verdad necesitas un directorio vacio sin archivos adentro, "
            f"usa run_shell_command con 'mkdir -p {path}'."
        )
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
        # Gap real confirmado en vivo (epica KAN-4, KAN-7, qwen3:8b): en
        # conversaciones largas (varios reintentos con feedback del juez
        # sobre el MISMO archivo, cada uno editandolo), el modelo pierde de
        # vista el contenido real actual y old_string deja de coincidir --
        # el error anterior solo decia "verifica que coincida", empujando al
        # modelo a un read_file aparte que, bajo presion de contexto, no
        # siempre hacia antes de reintentar (confirmado: "sin acceso directo
        # al contenido del archivo, no puedo realizar la edicion"). Devolver
        # el contenido actual (truncado) directo en el error ahorra ese
        # round-trip y le da la unica fuente de verdad que necesita para el
        # proximo intento, sin esperar a que decida pedirlo por su cuenta.
        preview = current if len(current) <= _EDIT_ERROR_CONTEXT_CHARS else (
            current[:_EDIT_ERROR_CONTEXT_CHARS] + f"\n... (truncado, {len(current)} chars en total -- usa read_file para verlo entero)"
        )
        return (
            f"error: old_string no se encontro en {path} -- verifica que coincida exactamente (incluido whitespace). "
            f"Contenido REAL actual de {path}:\n---\n{preview}\n---"
        )
    if count > 1:
        return f"error: old_string aparece {count} veces en {path} -- agrega mas contexto para que sea unico"

    new_content = current.replace(old_string, new_string, 1)
    json_error = _json_validation_error(path, new_content)
    if json_error:
        return json_error
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


# Gap real confirmado en vivo (epica KAN-4, qwen3:8b): el modelo edito
# package.json agregando "@capacitor/firebase-auth" -- un nombre de paquete
# que no existe en npm -- rompiendo "npm install" para todo el sub-proyecto.
# Antes de esto, la unica defensa era una linea en el prompt pidiendole que
# corriera "npm view" el mismo con run_shell_command -- funciona, pero
# depende de que se acuerde de hacerlo (y de armar el comando bien). Un tool
# dedicado, de solo lectura, sin necesitar confirmacion humana (no escribe
# nada), es una verificacion determinista que no depende de que el modelo
# la recuerde en el momento justo -- se puede ademas nudgear directamente
# desde el prompt como el paso obligatorio antes de tocar package.json.
def _semver_sort_key(version_str: str):
    """Ordena versiones por sus componentes numericos reales, no como texto
    plano -- un sort de strings pone '1.9.0' DESPUES de '1.10.0' (compara
    caracter por caracter: '9' > '1'), lo que le muestra al agente una
    version vieja como si fuera la mas nueva. Componentes no numericos
    (ej. '1.0.0-beta') se comparan como texto solo dentro del mismo prefijo
    numerico, no rompen el sort si algo no matchea el patron esperado.
    """
    parts = re.split(r"[.\-+]", version_str)
    key = []
    for part in parts:
        if part.isdigit():
            key.append((0, int(part)))
        else:
            key.append((1, part))
    return key


def tool_verify_npm_package(target_repo_dir: str, package_name: str, version: str = "") -> str:
    """Verifica si un paquete (y opcionalmente una version exacta) existe de
    verdad en el registro real de npm (npm view, sin instalar nada) -- solo
    lectura, no requiere confirmacion humana. Corre nativo si npm esta
    disponible en este contenedor, o delega a la imagen real de Node
    (Docker-outside-of-Docker, mismo patron que run_shell_command) si no lo
    esta.
    """
    npm_available = shutil.which("npm") is not None
    cmd = ["npm", "view", package_name, "versions", "--json"]
    try:
        if npm_available:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        elif shutil.which("docker") is not None:
            image = "mcr.microsoft.com/playwright:v1.61.1-noble"
            result = subprocess.run(
                ["docker", "run", "--rm", image, *cmd],
                capture_output=True, text=True, timeout=60,
            )
        else:
            return "error: ni npm ni docker estan disponibles para verificar este paquete -- no se puede confirmar, no asumas que existe"
    except subprocess.TimeoutExpired:
        return f"error: timeout verificando '{package_name}' -- no se pudo confirmar, no asumas que existe"

    stderr = (result.stderr or "").strip()
    if result.returncode != 0 or "404" in stderr or "E404" in stderr:
        return f"'{package_name}' NO existe en el registro de npm (verificado con npm view) -- no lo agregues a package.json con este nombre. Detalle: {stderr[:300]}"
    stdout = (result.stdout or "").strip()
    # npm devuelve el array de versiones en orden cronologico ASCENDENTE (la
    # mas vieja primero) -- confirmado real contra el registro publico
    # (registry.npmjs.org/react: ['0.0.1', '0.0.2', ..., mas reciente al
    # final]). Truncar el string crudo con [:1000] se queda con las
    # versiones mas viejas y corta las mas recientes, que son justo las que
    # un agente necesita verificar (una version recien pineada en
    # package.json). Parseamos el JSON real y devolvemos las ultimas 20
    # (mas nuevas) en vez de los primeros 1000 caracteres del string crudo.
    try:
        all_versions = json.loads(stdout)
        if isinstance(all_versions, str):
            all_versions = [all_versions]
    except (ValueError, TypeError):
        all_versions = None
    if all_versions is None:
        return f"'{package_name}' SI existe en npm, pero no se pudo parsear la lista de versiones -- no asumas que una version especifica existe sin confirmarlo de otra forma."
    if version:
        exists = version in all_versions
        return (
            f"'{package_name}@{version}' {'SI' if exists else 'NO'} existe en npm."
            + ("" if exists else f" Versiones reales disponibles (ultimas 20):\n{', '.join(all_versions[-20:])}")
        )
    return f"'{package_name}' SI existe en npm. Versiones reales disponibles (ultimas 20):\n{', '.join(all_versions[-20:])}"


# Gap real confirmado en vivo (epica KAN-4, TRES corridas seguidas):
# verify_npm_package existia SOLO para npm -- el mismo patron real
# (nombre/version de dependencia inventada rompiendo la instalacion entera)
# puede pasar en CUALQUIER stack que este pipeline soporta (ver
# _STACK_MARKERS), no solo Node. Estos 5 tools cubren el resto via las
# APIs REALES y publicas de cada registro -- HTTPS directo, sin necesitar
# el CLI nativo del lenguaje instalado en este contenedor (a diferencia de
# npm, que si necesita el binario o Docker-outside-of-Docker).
_REGISTRY_REQUEST_TIMEOUT_SECONDS = 15


def tool_verify_pip_package(target_repo_dir: str, package_name: str, version: str = "") -> str:
    """Verifica si un paquete (y opcionalmente una version exacta) existe
    de verdad en PyPI -- API real (pypi.org/pypi/<paquete>/json), solo
    lectura. Usa esto antes de agregar algo a requirements.txt/Pipfile.
    """
    try:
        resp = httpx.get(f"https://pypi.org/pypi/{package_name}/json", timeout=_REGISTRY_REQUEST_TIMEOUT_SECONDS)
    except httpx.HTTPError as exc:
        return f"error: no se pudo consultar PyPI para '{package_name}' ({exc}) -- no se pudo confirmar, no asumas que existe"
    if resp.status_code == 404:
        return f"'{package_name}' NO existe en PyPI -- no lo agregues a requirements.txt/Pipfile con este nombre."
    if resp.status_code != 200:
        return f"error: PyPI devolvio {resp.status_code} consultando '{package_name}' -- no se pudo confirmar"
    try:
        releases = list((resp.json() or {}).get("releases", {}).keys())
    except ValueError:
        return f"error: PyPI devolvio una respuesta no-JSON consultando '{package_name}' -- no se pudo confirmar, no asumas que existe"
    if version:
        exists = version in releases
        return (
            f"'{package_name}=={version}' {'SI' if exists else 'NO'} existe en PyPI."
            + ("" if exists else f" Versiones reales disponibles:\n{', '.join(sorted(releases, key=_semver_sort_key)[-20:])}")
        )
    return f"'{package_name}' SI existe en PyPI. Versiones reales disponibles (ultimas 20):\n{', '.join(sorted(releases, key=_semver_sort_key)[-20:])}"


def tool_verify_maven_package(target_repo_dir: str, group_id: str, artifact_id: str, version: str = "") -> str:
    """Verifica si una dependencia Maven (groupId:artifactId, opcionalmente
    una version exacta) existe de verdad en Maven Central -- API real
    (search.maven.org), solo lectura. Usa esto antes de agregar algo al
    pom.xml.
    """
    # Escapa comillas embebidas en group_id/artifact_id -- un nombre
    # hallucinado con un '"' literal rompe la sintaxis de la query Solr y
    # produce un falso "NO existe" (0 resultados) en vez de un error real.
    safe_group = group_id.replace('"', '\\"')
    safe_artifact = artifact_id.replace('"', '\\"')
    query = f'g:"{safe_group}" AND a:"{safe_artifact}"'
    try:
        resp = httpx.get(
            "https://search.maven.org/solrsearch/select",
            params={"q": query, "core": "gav", "rows": 20, "wt": "json"},
            timeout=_REGISTRY_REQUEST_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        return f"error: no se pudo consultar Maven Central para '{group_id}:{artifact_id}' ({exc}) -- no se pudo confirmar, no asumas que existe"
    try:
        docs = ((resp.json() or {}).get("response") or {}).get("docs", [])
    except ValueError:
        return f"error: Maven Central devolvio una respuesta no-JSON consultando '{group_id}:{artifact_id}' -- no se pudo confirmar, no asumas que existe"
    if not docs:
        return f"'{group_id}:{artifact_id}' NO existe en Maven Central (0 resultados) -- no lo agregues al pom.xml con este groupId/artifactId."
    versions = sorted({d.get("v") for d in docs if d.get("v")}, key=_semver_sort_key)
    if version:
        exists = version in versions
        return f"'{group_id}:{artifact_id}:{version}' {'SI' if exists else 'NO'} existe en Maven Central." + ("" if exists else f" Versiones reales disponibles:\n{', '.join(versions)}")
    return f"'{group_id}:{artifact_id}' SI existe en Maven Central. Versiones reales disponibles:\n{', '.join(versions)}"


def tool_verify_go_module(target_repo_dir: str, module_path: str, version: str = "") -> str:
    """Verifica si un modulo Go (y opcionalmente una version exacta) existe
    de verdad -- API real del proxy oficial de modulos (proxy.golang.org),
    solo lectura. Usa esto antes de agregar algo al go.mod.
    """
    module_path = module_path.strip()
    try:
        if version:
            resp = httpx.get(f"https://proxy.golang.org/{module_path}/@v/{version}.info", timeout=_REGISTRY_REQUEST_TIMEOUT_SECONDS)
            if resp.status_code == 404:
                versions_resp = httpx.get(f"https://proxy.golang.org/{module_path}/@v/list", timeout=_REGISTRY_REQUEST_TIMEOUT_SECONDS)
                real_versions = versions_resp.text.strip() if versions_resp.status_code == 200 else "(no se pudo listar)"
                return f"'{module_path}@{version}' NO existe. Versiones reales disponibles:\n{real_versions}"
            resp.raise_for_status()
            return f"'{module_path}@{version}' SI existe (confirmado via proxy.golang.org)."
        resp = httpx.get(f"https://proxy.golang.org/{module_path}/@v/list", timeout=_REGISTRY_REQUEST_TIMEOUT_SECONDS)
    except httpx.HTTPError as exc:
        return f"error: no se pudo consultar el proxy de modulos Go para '{module_path}' ({exc}) -- no se pudo confirmar, no asumas que existe"
    if resp.status_code == 404 or not resp.text.strip():
        return f"'{module_path}' NO existe (o no tiene versiones publicadas) en proxy.golang.org -- no lo agregues al go.mod con este path."
    return f"'{module_path}' SI existe. Versiones reales disponibles:\n{resp.text.strip()}"


def tool_verify_gem_package(target_repo_dir: str, gem_name: str, version: str = "") -> str:
    """Verifica si una gema Ruby (y opcionalmente una version exacta) existe
    de verdad en RubyGems -- API real (rubygems.org), solo lectura. Usa
    esto antes de agregar algo al Gemfile.
    """
    try:
        resp = httpx.get(f"https://rubygems.org/api/v1/versions/{gem_name}.json", timeout=_REGISTRY_REQUEST_TIMEOUT_SECONDS)
    except httpx.HTTPError as exc:
        return f"error: no se pudo consultar RubyGems para '{gem_name}' ({exc}) -- no se pudo confirmar, no asumas que existe"
    if resp.status_code == 404:
        return f"'{gem_name}' NO existe en RubyGems -- no lo agregues al Gemfile con este nombre."
    if resp.status_code != 200:
        return f"error: RubyGems devolvio {resp.status_code} consultando '{gem_name}' -- no se pudo confirmar"
    try:
        payload = resp.json()
    except ValueError:
        return f"error: RubyGems devolvio una respuesta no-JSON consultando '{gem_name}' -- no se pudo confirmar, no asumas que existe"
    versions = [v.get("number") for v in (payload or []) if v.get("number")]
    if version:
        exists = version in versions
        return f"'{gem_name} {version}' {'SI' if exists else 'NO'} existe en RubyGems." + ("" if exists else f" Versiones reales disponibles:\n{', '.join(versions[:20])}")
    return f"'{gem_name}' SI existe en RubyGems. Versiones reales disponibles (mas recientes primero):\n{', '.join(versions[:20])}"


def tool_verify_cargo_crate(target_repo_dir: str, crate_name: str, version: str = "") -> str:
    """Verifica si un crate Rust (y opcionalmente una version exacta) existe
    de verdad en crates.io -- API real, solo lectura. Usa esto antes de
    agregar algo al Cargo.toml. crates.io exige un User-Agent real
    (bloquea pedidos sin uno).
    """
    try:
        resp = httpx.get(
            f"https://crates.io/api/v1/crates/{crate_name}",
            headers={"User-Agent": "poc-ai-agents-coding-agent (verificacion de dependencias)"},
            timeout=_REGISTRY_REQUEST_TIMEOUT_SECONDS,
        )
    except httpx.HTTPError as exc:
        return f"error: no se pudo consultar crates.io para '{crate_name}' ({exc}) -- no se pudo confirmar, no asumas que existe"
    if resp.status_code == 404:
        return f"'{crate_name}' NO existe en crates.io -- no lo agregues al Cargo.toml con este nombre."
    if resp.status_code != 200:
        return f"error: crates.io devolvio {resp.status_code} consultando '{crate_name}' -- no se pudo confirmar"
    try:
        payload = resp.json()
    except ValueError:
        return f"error: crates.io devolvio una respuesta no-JSON consultando '{crate_name}' -- no se pudo confirmar, no asumas que existe"
    versions = [v.get("num") for v in (payload or {}).get("versions", []) if v.get("num")]
    if version:
        exists = version in versions
        return f"'{crate_name} {version}' {'SI' if exists else 'NO'} existe en crates.io." + ("" if exists else f" Versiones reales disponibles:\n{', '.join(versions[:20])}")
    return f"'{crate_name}' SI existe en crates.io. Versiones reales disponibles (mas recientes primero):\n{', '.join(versions[:20])}"


def tool_verify_nuget_package(target_repo_dir: str, package_id: str, version: str = "") -> str:
    """Verifica si un paquete NuGet (.NET, y opcionalmente una version
    exacta) existe de verdad -- API real (api.nuget.org), solo lectura. Usa
    esto antes de agregar algo al .csproj.
    """
    try:
        resp = httpx.get(
            f"https://api.nuget.org/v3-flatcontainer/{package_id.lower()}/index.json",
            timeout=_REGISTRY_REQUEST_TIMEOUT_SECONDS,
        )
    except httpx.HTTPError as exc:
        return f"error: no se pudo consultar NuGet para '{package_id}' ({exc}) -- no se pudo confirmar, no asumas que existe"
    if resp.status_code == 404:
        return f"'{package_id}' NO existe en NuGet -- no lo agregues al .csproj con este nombre."
    if resp.status_code != 200:
        return f"error: NuGet devolvio {resp.status_code} consultando '{package_id}' -- no se pudo confirmar"
    try:
        payload = resp.json()
    except ValueError:
        return f"error: NuGet devolvio una respuesta no-JSON consultando '{package_id}' -- no se pudo confirmar, no asumas que existe"
    versions = (payload or {}).get("versions", [])
    if version:
        exists = version in versions
        return f"'{package_id} {version}' {'SI' if exists else 'NO'} existe en NuGet." + ("" if exists else f" Versiones reales disponibles:\n{', '.join(versions[-20:])}")
    return f"'{package_id}' SI existe en NuGet. Versiones reales disponibles (ultimas 20):\n{', '.join(versions[-20:])}"


def tool_check_tsconfig_decorators(target_repo_dir: str, path: str) -> str:
    """Confirma si un tsconfig.json real tiene experimentalDecorators +
    emitDecoratorMetadata en true -- gap real confirmado en vivo (epica
    KAN-4): un archivo con @NgModule/@Component/@Injectable sin esto en el
    tsconfig.json de ese sub-proyecto rompe el test runner con un error de
    sintaxis, indistinguible a simple vista de un error real de codigo.
    Solo lectura -- 'path' es la ruta al tsconfig.json (ej.
    'frontend/tsconfig.json'), no al sub-proyecto.
    """
    try:
        full_path = _safe_path(target_repo_dir, path)
    except ValueError as exc:
        return f"error: {exc}"
    if not full_path.exists():
        return f"'{path}' no existe -- si vas a escribir codigo con decoradores de Angular (@NgModule/@Component/@Injectable), creá este archivo con \"compilerOptions\": {{\"experimentalDecorators\": true, \"emitDecoratorMetadata\": true, ...}} ANTES de terminar."
    try:
        raw = full_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return f"error leyendo {path}: {exc}"
    # tsconfig.json real permite comentarios (JSONC) -- json.loads estricto
    # rompe con archivos reales de Angular CLI que los incluyen. Chequeo por
    # regex tolerante a comentarios/formato en vez de un parser JSONC
    # completo (no vale la pena la dependencia nueva para esto).
    has_experimental = bool(re.search(r'"experimentalDecorators"\s*:\s*true', raw))
    has_metadata = bool(re.search(r'"emitDecoratorMetadata"\s*:\s*true', raw))
    if has_experimental and has_metadata:
        return f"'{path}': experimentalDecorators y emitDecoratorMetadata estan en true -- OK para usar decoradores de Angular."
    missing = []
    if not has_experimental:
        missing.append('"experimentalDecorators": true')
    if not has_metadata:
        missing.append('"emitDecoratorMetadata": true')
    return f"'{path}' existe pero le falta {', '.join(missing)} en compilerOptions -- agregalo antes de declararte done si este sub-proyecto usa decoradores de Angular."


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
    # Bug real confirmado en vivo (epica KAN-4, historia de bootstrap Ionic):
    # "ionic" no estaba en este mapeo -- _resolve_stack_image_if_needed
    # nunca lo reconocia como un comando de stack (ni siquiera intentaba
    # containerizarlo), asi que corria NATIVO en el contenedor minimo del
    # propio agente (sin Node/npm) y fallaba con "ionic: command not
    # found", indistinguible para el modelo de un fallo real del codigo.
    # La imagen trae Node pero NO el CLI de Ionic (no es parte de Node) --
    # _run_command_in_stack_container lo instala antes de correr el
    # comando real (ver ese comentario).
    "ionic": (lambda d: (d / "package.json").exists(), "mcr.microsoft.com/playwright:v1.61.1-noble"),
    # Mismo gap real, misma causa (epica KAN-4, historia siguiente): "ng"
    # (Angular CLI) tampoco estaba reconocido -- "ng test"/"ng build"
    # fallaban con "ng: command not found" en el contenedor minimo del
    # agente. Mismo tratamiento que "ionic": _run_command_in_stack_container
    # instala el CLI global antes de correr el comando real.
    "ng": (lambda d: (d / "package.json").exists(), "mcr.microsoft.com/playwright:v1.61.1-noble"),
    # Mismo gap real, misma causa (epica KAN-4, corrida de esta noche): el
    # CLI de Capacitor ("capacitor"/su alias corto "cap") tampoco estaba
    # reconocido -- "capacitor sync"/"cap add" fallaban con "command not
    # found" en el contenedor minimo del agente, indistinguible de un fallo
    # real. Mismo tratamiento que "ionic"/"ng".
    "capacitor": (lambda d: (d / "package.json").exists(), "mcr.microsoft.com/playwright:v1.61.1-noble"),
    "cap": (lambda d: (d / "package.json").exists(), "mcr.microsoft.com/playwright:v1.61.1-noble"),
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
    # Bug real confirmado en vivo (epica KAN-4): la imagen trae Node pero NO
    # los CLI globales de Ionic/Angular -- no son parte de Node/npm base,
    # hay que instalarlos aparte. Se instalan solo cuando el comando REAL
    # los necesita (no en cada corrida de npm/npx genericos), en el mismo
    # contenedor efimero antes de correr el comando pedido -- ninguna
    # instalacion persiste entre corridas (contenedor --rm), asi que esto
    # tiene que repetirse cada vez que se necesita, no es un costo unico.
    try:
        first_token = shlex.split(command)[0]
    except (ValueError, IndexError):
        first_token = ""
    global_cli_installs = {"ionic": "@ionic/cli", "ng": "@angular/cli", "capacitor": "@capacitor/cli", "cap": "@capacitor/cli"}
    setup_steps = []
    if first_token in global_cli_installs:
        setup_steps.append(f"npm install -g {global_cli_installs[first_token]} >/dev/null 2>&1")
    # Segundo bug real confirmado en vivo, mismo patron (historia siguiente
    # de la misma epica): "vitest"/"npx vitest" fallaba con "no esta
    # instalado como devDependency" -- a diferencia del CLI global de
    # arriba, esto es una dependencia LOCAL real (package.json/package-lock.json
    # ya la declaran), pero cada `docker run --rm` es un contenedor nuevo, y
    # el modelo no siempre se acuerda de correr "npm install" ANTES del
    # comando real en la misma llamada. work_dir esta bind-mounteado (no es
    # estado efimero del contenedor), asi que node_modules generado por un
    # "npm install" real SI persiste entre llamadas -- pero solo si alguna
    # llamada lo corrio. Si el marcador real (node_modules/) todavia no
    # existe y el comando no es ya un "npm install" en si mismo, se antepone
    # uno real antes de intentar el comando pedido.
    try:
        second_token = shlex.split(command)[1] if first_token == "npm" else ""
    except (ValueError, IndexError):
        second_token = ""
    is_already_an_install_command = first_token == "npm" and second_token in ("install", "ci")
    if (
        not is_already_an_install_command
        and not (work_dir / "node_modules").is_dir()
        and (work_dir / "package.json").is_file()
    ):
        setup_steps.append("npm install --silent >/dev/null 2>&1")
    effective_command = "; ".join(setup_steps + [command]) if setup_steps else command
    try:
        result = subprocess.run(
            ["docker", "run", "--rm", "-v", f"{host_work_dir}:/work", "-w", "/work", image, "sh", "-c", effective_command],
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
    "verify_npm_package": {
        "description": (
            "Confirma si un paquete (y opcionalmente una version exacta) existe DE VERDAD en el registro "
            "real de npm (npm view, sin instalar nada) -- usalo SIEMPRE antes de agregar una dependencia "
            "nueva a package.json, el nombre exacto de un paquete (sobre todo plugins de nicho) no es algo "
            "que puedas confiar de memoria. Solo lectura."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"package_name": {"type": "string"}, "version": {"type": "string"}},
            "required": ["package_name"],
        },
        "fn": tool_verify_npm_package,
    },
    "check_tsconfig_decorators": {
        "description": (
            "Confirma si un tsconfig.json real tiene experimentalDecorators+emitDecoratorMetadata en true -- "
            "usalo antes de declararte done si escribiste un archivo con decoradores de Angular "
            "(@NgModule/@Component/@Injectable), sin esto el test runner rompe con un error de sintaxis. "
            "'path' es la ruta al tsconfig.json (ej. 'frontend/tsconfig.json'). Solo lectura."
        ),
        "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]},
        "fn": tool_check_tsconfig_decorators,
    },
    "verify_pip_package": {
        "description": (
            "Confirma si un paquete Python (y opcionalmente una version exacta) existe DE VERDAD en PyPI -- "
            "usalo SIEMPRE antes de agregar una dependencia nueva a requirements.txt/Pipfile. Solo lectura."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"package_name": {"type": "string"}, "version": {"type": "string"}},
            "required": ["package_name"],
        },
        "fn": tool_verify_pip_package,
    },
    "verify_maven_package": {
        "description": (
            "Confirma si una dependencia Maven (groupId:artifactId, y opcionalmente una version exacta) "
            "existe DE VERDAD en Maven Central -- usalo SIEMPRE antes de agregar algo nuevo al pom.xml. Solo lectura."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"group_id": {"type": "string"}, "artifact_id": {"type": "string"}, "version": {"type": "string"}},
            "required": ["group_id", "artifact_id"],
        },
        "fn": tool_verify_maven_package,
    },
    "verify_go_module": {
        "description": (
            "Confirma si un modulo Go (y opcionalmente una version exacta) existe DE VERDAD (proxy.golang.org) "
            "-- usalo SIEMPRE antes de agregar algo nuevo al go.mod. Solo lectura."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"module_path": {"type": "string"}, "version": {"type": "string"}},
            "required": ["module_path"],
        },
        "fn": tool_verify_go_module,
    },
    "verify_gem_package": {
        "description": (
            "Confirma si una gema Ruby (y opcionalmente una version exacta) existe DE VERDAD en RubyGems -- "
            "usalo SIEMPRE antes de agregar algo nuevo al Gemfile. Solo lectura."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"gem_name": {"type": "string"}, "version": {"type": "string"}},
            "required": ["gem_name"],
        },
        "fn": tool_verify_gem_package,
    },
    "verify_cargo_crate": {
        "description": (
            "Confirma si un crate Rust (y opcionalmente una version exacta) existe DE VERDAD en crates.io -- "
            "usalo SIEMPRE antes de agregar algo nuevo al Cargo.toml. Solo lectura."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"crate_name": {"type": "string"}, "version": {"type": "string"}},
            "required": ["crate_name"],
        },
        "fn": tool_verify_cargo_crate,
    },
    "verify_nuget_package": {
        "description": (
            "Confirma si un paquete NuGet .NET (y opcionalmente una version exacta) existe DE VERDAD "
            "(api.nuget.org) -- usalo SIEMPRE antes de agregar algo nuevo al .csproj. Solo lectura."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"package_id": {"type": "string"}, "version": {"type": "string"}},
            "required": ["package_id"],
        },
        "fn": tool_verify_nuget_package,
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
    system_prompt: str = CODING_AGENT_SYSTEM_PROMPT,
) -> dict:
    """Si resume_messages viene seteado (reintento tras un veredicto FLAGGED
    retryable del juez -- ver retry_coding_agent_with_feedback() en
    run_poc_loop.sh / _retry_local_diff() en orchestration.py), la
    conversacion CONTINUA en vez de arrancar de cero: sanitized_prompt pasa
    a ser solo el feedback del juez (un turno de usuario nuevo), no el
    ticket completo -- evita repagar la investigacion (listar el repo, leer
    archivos) ya hecha en el primer intento. resume_state siembra
    has_investigated/has_run_verification con lo ya alcanzado.

    system_prompt: swappable -- default CODING_AGENT_SYSTEM_PROMPT, pero
    run_test_writer_subagent() (mas abajo) reusa este MISMO loop entero
    (gate de investigacion, confirmacion humana, esquema JSON, self_review)
    con TEST_WRITER_SYSTEM_PROMPT en su lugar -- esos mecanismos son
    genericos (no dependen del texto del prompt), asi que no hace falta
    duplicar el loop para un agente con un objetivo mas acotado.
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
    # Gap real confirmado en vivo (epica KAN-4, qwen3:8b): has_run_verification
    # es un booleano que, una vez True, se queda True para el resto de la
    # conversacion -- pero verificar ANTES de escribir un archivo nuevo no
    # dice nada sobre ESE archivo. Confirmado real: el turno anterior corrio
    # "npm test" de verdad (has_run_verification=True, correctamente), el
    # turno siguiente escribio un test NUEVO (ionic-version.test.ts, con un
    # error real: "expect is not defined", import faltante) y declaro "done"
    # sin volver a correr nada -- has_run_verification seguia en True de
    # ANTES, asi que el gate de verificacion no se disparaba. El testing
    # agent real lo atrapo despues, pero el coding agent nunca se entero.
    # dirty_since_verification: True apenas una escritura exitosa invalida
    # la ultima verificacion real -- se limpia solo cuando corre un comando
    # de verificacion de verdad DESPUES de esa escritura.
    dirty_since_verification = False
    # Gap real confirmado en vivo (epica KAN-4, qwen3:8b): 11 historias
    # seguidas devolvieron status="done" con files_changed listando
    # archivos especificos y narrativas detalladas de lo que "crearon" --
    # sin llamar write_file/edit_file NI UNA VEZ (cero prompts de
    # confirmacion reales en el log). No es un problema de escritura
    # fallida: el modelo nunca lo intento. wrote_any_file trackea si en
    # ALGUN turno de esta corrida realmente corrio una escritura exitosa,
    # para poder atrapar esta inconsistencia antes de aceptar "done".
    wrote_any_file = False
    no_write_nudge_given = False

    def _finalize(result: dict) -> dict:
        result["self_verified"] = has_run_verification
        result["initial_plan"] = initial_plan
        result["consulted_risk_graph"] = consulted_risk_graph
        result["wrote_any_file"] = wrote_any_file
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
            # while + contador mutable (turn_budget), no "for _ in range(MAX_TOOL_TURNS)":
            # un cambio real de modelo (maybe_switch_ollama_model) extiende
            # turn_budget en el momento -- ver MIN_TURNS_AFTER_MODEL_SWITCH
            # arriba y cada sitio que llama a maybe_switch_ollama_model mas
            # abajo. range() no se puede extender una vez arrancado el loop.
            turn_budget = MAX_TOOL_TURNS
            turns_used = 0
            while turns_used < turn_budget:
                turns_used += 1
                content, stop_reason, usage, backend = await call_with_fallback(
                    client, messages, tools, system_prompt,
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
                        turn_budget = max(turn_budget, turns_used + MIN_TURNS_AFTER_MODEL_SWITCH)
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
                            client, backend, messages, tools, system_prompt,
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
                                turn_budget = max(turn_budget, turns_used + MIN_TURNS_AFTER_MODEL_SWITCH)
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
                        turn_budget = max(turn_budget, turns_used + MIN_TURNS_AFTER_MODEL_SWITCH)
                        continue

                    if (
                        result.get("status") == "done"
                        and result.get("files_changed")
                        and not wrote_any_file
                        and not no_write_nudge_given
                    ):
                        # Un solo empujon, mismo criterio que los demas --
                        # si tampoco escribe nada real la segunda vez, se
                        # acepta igual (files_changed queda trazado tal
                        # cual, sin verificar), no se bloquea infinito. La
                        # red de seguridad real (git status/commits_ahead en
                        # orchestration.py) sigue siendo quien decide si
                        # "applied" es verdad, esto solo evita desperdiciar
                        # el turno entero en una fabricacion sin intentar
                        # corregirla.
                        no_write_nudge_given = True
                        messages.append({"role": "user", "content": NO_REAL_WRITE_NUDGE_MESSAGE})
                        continue

                    if (
                        result.get("status") == "done"
                        and (not has_run_verification or dirty_since_verification)
                        and not verification_nudge_given
                    ):
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
                            if name in ("write_file", "edit_file") and not str(output).startswith("error:"):
                                wrote_any_file = True
                                # Una escritura NUEVA invalida cualquier
                                # verificacion anterior -- si ya se habia
                                # gastado el empujon de verificacion en una
                                # vuelta previa, esta escritura merece su
                                # propia oportunidad real, no queda tapada
                                # por un empujon ya usado para otro archivo.
                                dirty_since_verification = True
                                verification_nudge_given = False
                            if name == "list_directory":
                                listed_dirs.add(_normalize_listed_dir(tool_input.get("path", ".")))
                            if name == "run_shell_command" and _VERIFICATION_COMMAND_PATTERN.search(str(tool_input.get("command", ""))):
                                has_run_verification = True
                                dirty_since_verification = False
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
                warn_if_context_large(messages, logger, "coding agent", backend=backend, system_prompt=system_prompt)

    return _finalize({"status": "blocked", "summary": "se agotaron los turnos de herramientas sin terminar", "files_changed": []})


_LOCKFILE_BASENAMES = {
    "package-lock.json", "npm-shrinkwrap.json", "yarn.lock", "pnpm-lock.yaml",
    "Gemfile.lock", "poetry.lock", "Pipfile.lock", "Cargo.lock", "go.sum", "composer.lock",
}


def _strip_lockfile_hunks_from_diff(diff_text: str) -> str:
    """Gap real confirmado en vivo (epica KAN-4, KAN-10): el test-writer
    sub-agent recibe el diff COMPLETO de la rama (git diff base..rama), no
    solo lo del ticket actual -- si un intento anterior en la misma rama
    modifico un lockfile (confirmado real: 3322 lineas insertadas en
    package-lock.json en un solo commit), ese ruido diluye/empuja fuera de
    contexto el diff real que el modelo necesita ver, y termina
    alucinando "no tengo suficiente informacion" en vez de escribir el
    test. Los lockfiles no tienen codigo de aplicacion testeable -- se
    omiten enteros, dejando una marca explicita de que se omitieron (no
    silenciosamente, para que el modelo no asuma que el diff esta
    incompleto por error).
    """
    if not diff_text.strip():
        return diff_text
    file_diffs = re.split(r"(?=^diff --git )", diff_text, flags=re.MULTILINE)
    kept = []
    omitted = []
    for chunk in file_diffs:
        if not chunk.strip():
            continue
        header_match = re.match(r"^diff --git a/(\S+) b/(\S+)", chunk)
        path = header_match.group(2) if header_match else ""
        basename = path.rsplit("/", 1)[-1] if path else ""
        if basename in _LOCKFILE_BASENAMES:
            omitted.append(path)
        else:
            kept.append(chunk)
    result = "".join(kept)
    if omitted:
        result += "\n[diff de " + ", ".join(omitted) + " omitido a proposito -- son lockfiles sin codigo de aplicacion testeable]\n"
    return result


async def run_test_writer_subagent(ticket_id: str, diff_text: str, ticket_summary: str, target_repo_dir: str) -> dict:
    """Conversacion NUEVA (sin resume_messages/resume_state -- arranca de
    cero, a proposito) enfocada UNICAMENTE en escribir/mejorar tests para un
    diff que ya existe. Reusa run_coding_agent entero (mismos gates, mismas
    tools, mismo esquema de respuesta) con TEST_WRITER_SYSTEM_PROMPT en vez
    del prompt general -- ver el comentario ahi arriba para el porque real
    (KAN-5: la MISMA conversacion larga que investigo+implemento+reintento
    llega a escribir el test ya cargada, un contexto fresco enfocado
    unicamente en testing tiene mas chance de aplicar bien las reglas de
    mocking/async).
    """
    prompt = (
        f"Historia: {ticket_id} -- {ticket_summary}\n\n"
        "El siguiente diff ya fue aplicado a este repo (rama actual) -- escribi o mejora los tests que lo "
        "cubran, siguiendo las reglas del prompt de arriba:\n\n"
        f"```diff\n{_strip_lockfile_hunks_from_diff(diff_text)}\n```"
    )
    return await run_coding_agent(
        ticket_id, prompt, target_repo_dir, system_prompt=TEST_WRITER_SYSTEM_PROMPT,
    )


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
        # mode="test_writer": orchestration.py::run_test_writer_subagent_task
        # arma este payload cuando el juez marca insufficient-test-coverage
        # -- conversacion NUEVA (nunca resume_messages/resume_state, a
        # proposito, ver el docstring de run_test_writer_subagent) enfocada
        # solo en escribir/mejorar tests para un diff que ya existe.
        if payload.get("mode") == "test_writer":
            result = asyncio.run(
                run_test_writer_subagent(
                    ticket_id, payload["diff_text"], payload.get("ticket_summary", ""), payload["target_repo_dir"],
                )
            )
        else:
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
