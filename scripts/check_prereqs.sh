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

# --- Qdrant ---
curl -sf "${QDRANT_URL:-http://localhost:6333}/readyz" >/dev/null 2>&1
check "Qdrant listo en ${QDRANT_URL:-http://localhost:6333}" "$?"

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

# --- sample-repo como repo git (para poder aplicar sugerencias, opcional) ---
if [ -d "${ROOT_DIR}/sample-repo/.git" ]; then
  printf "  %s  %s\n" "${PASS}" "sample-repo/ es un repo git (Copilot puede aplicar cambios en una rama)"
else
  printf "  ⚠️  sample-repo/ NO es un repo git todavia (opcional): git -C sample-repo init && git -C sample-repo add -A && git -C sample-repo commit -m baseline\n"
fi

echo "=========================================="
if [ "${overall_ok}" = "1" ]; then
  echo "Todo listo. Puedes correr ./run_poc_loop.sh"
  exit 0
else
  echo "Hay prerequisitos sin cumplir arriba (❌). Resuelvelos antes de correr run_poc_loop.sh."
  exit 1
fi
