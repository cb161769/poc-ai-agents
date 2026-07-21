#!/usr/bin/env bash
# Validates every moving part before run_poc_loop.sh touches them, so a
# broken demo fails fast with a clear message instead of a cryptic curl
# error halfway through the pipeline.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${ROOT_DIR}/.env"
  set +a
fi

PASS="✅"
FAIL="❌"
WARN="⚠️"
overall_ok=1

check() {
  local label="$1"
  local ok="$2"
  local detail="${3:-}"
  if [ "${ok}" = "0" ]; then
    printf "  %s  %s\n" "${PASS}" "${label}"
  else
    printf "  %s  %s %s\n" "${FAIL}" "${label}" "${detail}"
    overall_ok=0
  fi
}

warn() {
  printf "  %s  %s\n" "${WARN}" "$1"
}

# Consulta GraphQL real (Repository.suggestedActors con capability
# CAN_BE_ASSIGNED) para saber si el Copilot coding agent esta realmente
# habilitado y asignable en el repo -- antes esto solo se descubria DESPUES
# de crear un issue y que la asignacion fallara (prueba y error). Devuelve
# por stdout uno de: "yes" (el login aparece como asignable), "no" (la
# query corrio bien pero no aparece -- no habilitado o sin permisos),
# "unknown" (la query en si fallo -- token sin scope, red, etc). Nunca
# hace fallar el script que la llama.
check_copilot_assignable() {
  local repo="$1" assignee="$2"
  local owner name response
  owner="${repo%%/*}"
  name="${repo#*/}"

  if ! response=$(gh api graphql -f query='
    query($owner: String!, $name: String!) {
      repository(owner: $owner, name: $name) {
        suggestedActors(capabilities: [CAN_BE_ASSIGNED], first: 100) {
          nodes { login }
        }
      }
    }' -f owner="${owner}" -f name="${name}" 2>/dev/null); then
    echo "unknown"
    return 0
  fi

  if echo "${response}" | jq -e --arg login "${assignee}" '.data.repository.suggestedActors.nodes[]? | select(.login == $login)' >/dev/null 2>&1; then
    echo "yes"
  else
    echo "no"
  fi
}

echo "== Verificando prerequisitos de la PoC =="

# --- Neo4j ---
if command -v cypher-shell >/dev/null 2>&1; then
  cypher-shell -a "${NEO4J_URI:-bolt://localhost:7687}" -u "${NEO4J_USERNAME:-neo4j}" -p "${NEO4J_PASSWORD:-test_password_local}" "RETURN 1" >/dev/null 2>&1
  check "Neo4j alcanzable en ${NEO4J_URI:-bolt://localhost:7687}" "$?"
else
  curl -sf "http://localhost:7474" >/dev/null 2>&1
  check "Neo4j (HTTP 7474) alcanzable" "$?" "(cypher-shell no esta en PATH, se probo HTTP)"
fi

# --- SonarQube ---
curl -sf "${SONAR_URL:-http://localhost:9000}/api/system/status" 2>/dev/null | grep -q '"status":"UP"'
check "SonarQube UP en ${SONAR_URL:-http://localhost:9000}" "$?"

if [ -n "${SONAR_TOKEN:-}" ]; then
  curl -sf -u "${SONAR_TOKEN}:" "${SONAR_URL:-http://localhost:9000}/api/authentication/validate" 2>/dev/null | grep -q '"valid":true'
  check "SONAR_TOKEN valido" "$?"
else
  check "SONAR_TOKEN definido en .env" "1" "(falta variable)"
fi

# --- AI Firewall ---
curl -sf "${FIREWALL_URL:-http://localhost:8080}/health" >/dev/null 2>&1
check "AI Firewall alcanzable en ${FIREWALL_URL:-http://localhost:8080}" "$?"

if [ -n "${FIREWALL_API_KEY:-}" ]; then
  # Best-effort: confirma que /evaluate realmente exige el header sin la key,
  # no solo que la variable este definida en .env.
  UNAUTH_CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${FIREWALL_URL:-http://localhost:8080}/evaluate" \
    -H "Content-Type: application/json" -d '{"prompt":"","jira_context":{},"sonar_errors":[]}' 2>/dev/null)
  if [ "${UNAUTH_CODE}" = "401" ]; then
    printf "  %s  %s\n" "${PASS}" "FIREWALL_API_KEY exigida por /evaluate (pedido sin header -> 401)"
  else
    printf "  ⚠️  /evaluate no devolvio 401 sin header (devolvio %s) — verifica que ai-firewall se haya reconstruido con la key nueva\n" "${UNAUTH_CODE:-sin respuesta}"
  fi
else
  printf "  ⚠️  FIREWALL_API_KEY no definida (opcional): /evaluate queda abierto, cualquiera que llegue a FIREWALL_URL puede pegarle directo\n"
fi

# --- Qdrant ---
curl -sf "${QDRANT_URL:-http://localhost:6333}/readyz" >/dev/null 2>&1
check "Qdrant listo en ${QDRANT_URL:-http://localhost:6333}" "$?"

# --- Prefect server ---
# Gap real confirmado esta sesion (recurrente varias veces): poc-prefect-server
# se cae solo periodicamente -- sin este chequeo, la corrida real recien
# fallaba a mitad de camino con un httpx.ConnectError/ConnectTimeout dificil
# de distinguir de un bug de codigo real.
PREFECT_HEALTH_URL="${PREFECT_API_URL:-http://localhost:4200/api}"
curl -sf "${PREFECT_HEALTH_URL%/api}/api/health" >/dev/null 2>&1
check "Prefect server alcanzable en ${PREFECT_HEALTH_URL%/api}" "$?" "(correr 'docker compose up -d prefect-server')"

# --- Jira ---
if [ -n "${JIRA_URL:-}" ] && [ -n "${JIRA_EMAIL:-}" ] && [ -n "${JIRA_API_TOKEN:-}" ]; then
  curl -sf -u "${JIRA_EMAIL}:${JIRA_API_TOKEN}" "${JIRA_URL%/}/rest/api/3/myself" >/dev/null 2>&1
  check "Credenciales Jira validas (${JIRA_URL:-})" "$?"
else
  check "JIRA_URL / JIRA_EMAIL / JIRA_API_TOKEN definidos en .env" "1" "(falta alguna variable)"
fi

if [ -n "${JIRA_TICKET_KEY:-}" ]; then
  check "JIRA_TICKET_KEY definido (${JIRA_TICKET_KEY})" "0"
else
  check "JIRA_TICKET_KEY definido en .env" "1" "(falta variable)"
fi

# --- Azure DevOps ---
if [ -n "${AZURE_DEVOPS_ORG_URL:-}" ] && [ -n "${AZURE_DEVOPS_PAT:-}" ]; then
  curl -sf -u ":${AZURE_DEVOPS_PAT}" "${AZURE_DEVOPS_ORG_URL%/}/_apis/projects?api-version=7.1" >/dev/null 2>&1
  check "Credenciales Azure DevOps validas (${AZURE_DEVOPS_ORG_URL:-})" "$?"
else
  check "AZURE_DEVOPS_ORG_URL / AZURE_DEVOPS_PAT definidos en .env" "1" "(falta alguna variable)"
fi

# --- gh copilot ---
if command -v gh >/dev/null 2>&1; then
  gh copilot --help >/dev/null 2>&1
  check "gh copilot disponible" "$?" "(gh extension install github/gh-copilot)"
else
  check "gh CLI instalado" "1" "(instala https://cli.github.com)"
fi

# --- GitHub Copilot coding agent (opcional, agente real en la nube) ---
if [ -n "${GITHUB_REPO:-}" ]; then
  if command -v gh >/dev/null 2>&1 && gh repo view "${GITHUB_REPO}" >/dev/null 2>&1; then
    printf "  %s  %s\n" "${PASS}" "GITHUB_REPO accesible (${GITHUB_REPO}) — run_poc_loop.sh usara el coding agent real"

    # Antes esto solo se sabia DESPUES de crear un issue y que la
    # asignacion fallara -- ahora se verifica de antemano si el bot
    # realmente aparece como asignable en el repo.
    assignable=$(check_copilot_assignable "${GITHUB_REPO}" "${GITHUB_COPILOT_ASSIGNEE:-copilot-swe-agent}")
    case "${assignable}" in
      yes)
        printf "  %s  %s\n" "${PASS}" "Copilot coding agent habilitado y asignable en ${GITHUB_REPO}"
        ;;
      no)
        warn "el repo es accesible pero '${GITHUB_COPILOT_ASSIGNEE:-copilot-swe-agent}' no aparece como asignable — revisa Settings > Copilot > Coding agent en GitHub, y que el plan sea Business/Enterprise"
        ;;
      *)
        warn "no se pudo confirmar si Copilot coding agent esta habilitado en ${GITHUB_REPO} (verificalo manualmente)"
        ;;
    esac
  else
    check "GITHUB_REPO accesible (${GITHUB_REPO})" "1" "(verifica el nombre owner/repo y que gh este autenticado con acceso)"
  fi
else
  printf "  ⚠️  GITHUB_REPO no definido (opcional): sin el, run_poc_loop.sh usa el fallback local (gh copilot suggest), no un agente real\n"
fi

# --- Agente juez (opcional pero recomendado, con poder de bloqueo) ---
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
  curl -sf -H "x-api-key: ${ANTHROPIC_API_KEY}" -H "anthropic-version: 2023-06-01" \
    -H "content-type: application/json" \
    -d '{"model":"'"${ANTHROPIC_MODEL:-claude-sonnet-5}"'","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}' \
    https://api.anthropic.com/v1/messages >/dev/null 2>&1
  check "ANTHROPIC_API_KEY valida (agente juez, backend Anthropic)" "$?"
elif curl -sf "${OLLAMA_URL:-http://localhost:11434}/api/tags" >/dev/null 2>&1; then
  printf "  %s  %s\n" "${PASS}" "Ollama local alcanzable (agente juez, backend fallback) — verifica que '${OLLAMA_MODEL:-llama3.1}' este descargado: docker exec poc-ollama ollama pull ${OLLAMA_MODEL:-llama3.1}"
else
  printf "  ⚠️  Ni ANTHROPIC_API_KEY ni Ollama local disponibles: el agente juez se omite, ninguna corrida tendra segunda opinion\n"
fi

if command -v uvx >/dev/null 2>&1; then
  printf "  %s  %s\n" "${PASS}" "uvx disponible — el juez (si corre) podra conectarse a mcp-neo4j-cypher / mcp-server-qdrant"
else
  printf "  ⚠️  uvx no encontrado: el juez (si corre) va a razonar sin herramientas MCP, solo sobre texto (instala uv: https://docs.astral.sh/uv)\n"
fi

# --- Repo objetivo real (donde estas parado, no sample-repo/) ---
# run_poc_loop.sh/orchestration.py operan sobre el repo git en el que el
# usuario esta parado al invocarlos, no sobre una carpeta fija — el mismo
# chequeo que hace el pipeline antes de crear una rama. Argumento posicional
# opcional ($1): path de un clon especifico a chequear en vez de "donde
# estoy parado" -- usado por run_epic_dood.sh para validar el clon real
# ANTES de arrancar el contenedor, sin necesitar que el usuario este parado
# ahi.
if [ -n "${1:-}" ]; then
  TARGET_REPO_DIR_CHECK="$1"
else
  TARGET_REPO_DIR_CHECK="$(git rev-parse --show-toplevel 2>/dev/null)"
fi
if [ -n "${TARGET_REPO_DIR_CHECK}" ] && git -C "${TARGET_REPO_DIR_CHECK}" rev-parse --show-toplevel >/dev/null 2>&1; then
  printf "  %s  %s\n" "${PASS}" "Parado dentro de un repo git real (${TARGET_REPO_DIR_CHECK})"
  if [ -z "$(git -C "${TARGET_REPO_DIR_CHECK}" status --porcelain)" ]; then
    printf "  %s  %s\n" "${PASS}" "Working tree limpio (sin cambios sin commitear)"
  else
    check "Working tree limpio (sin cambios sin commitear)" "1" "(hace commit o 'git stash' antes de correr run_poc_loop.sh)"
  fi
  # Gap real confirmado en vivo (operacion de esta noche): un clon fresco
  # (ej. Docker-outside-of-Docker con un target-repo recien clonado) puede
  # no tener user.name/user.email configurados localmente -- el primer 'git
  # commit' real que intenta el coding agent tira CalledProcessError
  # ("Author identity unknown") recien A MITAD de una corrida real.
  #
  # Segundo bug real confirmado esta sesion (epica KAN-4, qwen3:8b): 'git
  # config --get' sin --local tambien resuelve contra el config GLOBAL del
  # HOST (~/.gitconfig) -- este chequeo daba PASS (usando el fallback
  # global del host) mientras el contenedor real (que no monta
  # ~/.gitconfig) seguia sin identidad local, y el commit fallaba de
  # verdad mas adelante pese al check verde. --local hace que el chequeo
  # refleje lo que el contenedor realmente va a ver.
  if [ -n "$(git -C "${TARGET_REPO_DIR_CHECK}" config --local --get user.name 2>/dev/null)" ] \
     && [ -n "$(git -C "${TARGET_REPO_DIR_CHECK}" config --local --get user.email 2>/dev/null)" ]; then
    printf "  %s  %s\n" "${PASS}" "Identidad de git configurada (user.name/user.email)"
  else
    check "Identidad de git configurada (user.name/user.email)" "1" \
      "(correr: git -C '${TARGET_REPO_DIR_CHECK}' config user.name '...' && git -C '${TARGET_REPO_DIR_CHECK}' config user.email '...')"
  fi
else
  check "Parado dentro de un repo git real" "1" "(cd a tu proyecto real antes de correr check_prereqs.sh/run_poc_loop.sh — ya no se usa sample-repo/ por defecto)"
fi

echo "=========================================="
if [ "${overall_ok}" = "1" ]; then
  echo "Todo listo. Puedes correr ./run_poc_loop.sh"
  exit 0
else
  echo "Hay prerequisitos sin cumplir arriba (❌). Resuelvelos antes de correr run_poc_loop.sh."
  exit 1
fi
