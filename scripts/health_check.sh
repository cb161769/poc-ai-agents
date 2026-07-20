#!/usr/bin/env bash
# Monitoreo continuo de servicios, independiente de una corrida del
# pipeline -- extrae la misma logica de chequeo de infraestructura que ya
# tiene check_prereqs.sh (Neo4j, SonarQube, Qdrant, AI Firewall, Ollama) en
# una forma pensada para correr en cron/systemd timer, no solo como
# preflight antes de run_poc_loop.sh.
#
# Uso:
#   ./scripts/health_check.sh --json   # imprime {"ok": bool, "checks": [...]}
#   ./scripts/health_check.sh          # salida humana (igual que check_prereqs.sh, pero solo infra)
#
# Si ALERT_WEBHOOK_URL (o el alias FALCO_ALERT_WEBHOOK_URL) esta configurada
# y algun chequeo falla, postea un resumen ahi -- mismo formato que usa
# run_poc_loop.sh/orchestration.py para alertas del juez/testing agent.
#
# Gap real confirmado esta sesion: este script no estaba conectado a NADA --
# ni cron real, ni systemd timer. Ejemplo de crontab (cada 15 minutos, con
# salida acumulada en su propio log):
#   */15 * * * * cd /ruta/al/repo && ./scripts/health_check.sh --json >> logs/health_check.jsonl 2>&1
# (Windows/WSL: registrar el mismo comando en el Programador de tareas o en
# el cron de la distro WSL -- no se instala automaticamente desde aca,
# depende demasiado del entorno del operador.)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${ROOT_DIR}/.env"
  set +a
fi

JSON_MODE=false
[ "${1:-}" = "--json" ] && JSON_MODE=true

CHECKS_JSON="[]"
overall_ok=true

add_check() {
  local name="$1" ok="$2" detail="${3:-}"
  CHECKS_JSON=$(jq -n --argjson checks "${CHECKS_JSON}" --arg name "${name}" --argjson ok "${ok}" --arg detail "${detail}" \
    '$checks + [{name: $name, ok: $ok, detail: $detail}]')
  [ "${ok}" = "false" ] && overall_ok=false
}

# --- Neo4j ---
if command -v cypher-shell >/dev/null 2>&1; then
  if cypher-shell -a "${NEO4J_URI:-bolt://localhost:7687}" -u "${NEO4J_USERNAME:-neo4j}" -p "${NEO4J_PASSWORD:-test_password_local}" "RETURN 1" >/dev/null 2>&1; then
    add_check "neo4j" true ""
  else
    add_check "neo4j" false "no alcanzable en ${NEO4J_URI:-bolt://localhost:7687}"
  fi
else
  if curl -sf "http://localhost:7474" >/dev/null 2>&1; then
    add_check "neo4j" true "(cypher-shell no esta en PATH, se probo HTTP)"
  else
    add_check "neo4j" false "no alcanzable (HTTP 7474)"
  fi
fi

# --- SonarQube ---
if curl -sf "${SONAR_URL:-http://localhost:9000}/api/system/status" 2>/dev/null | grep -q '"status":"UP"'; then
  add_check "sonarqube" true ""
else
  add_check "sonarqube" false "no UP en ${SONAR_URL:-http://localhost:9000}"
fi

# --- Qdrant ---
if curl -sf "${QDRANT_URL:-http://localhost:6333}/readyz" >/dev/null 2>&1; then
  add_check "qdrant" true ""
else
  add_check "qdrant" false "no listo en ${QDRANT_URL:-http://localhost:6333}"
fi

# --- AI Firewall ---
if curl -sf "${FIREWALL_URL:-http://localhost:8080}/health" >/dev/null 2>&1; then
  add_check "ai_firewall" true ""
else
  add_check "ai_firewall" false "no alcanzable en ${FIREWALL_URL:-http://localhost:8080}"
fi

# --- Ollama (opcional -- solo si no hay ANTHROPIC_API_KEY) ---
if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  if curl -sf "${OLLAMA_URL:-http://localhost:11434}/api/tags" >/dev/null 2>&1; then
    add_check "ollama" true ""
  else
    add_check "ollama" false "no alcanzable en ${OLLAMA_URL:-http://localhost:11434} (sin ANTHROPIC_API_KEY, el juez se omite sin esto)"
  fi
fi

# --- Credenciales Jira / Azure DevOps (gap real confirmado esta sesion) ---
# check_prereqs.sh ya validaba esto, pero solo AL MOMENTO de arrancar una
# corrida -- el JIRA_API_TOKEN de esta sesion se vencio A MITAD de la
# operacion sin que nada lo detectara hasta una verificacion manual. Con
# esto en un cron de health_check.sh, un token vencido se detecta en
# minutos, no cuando falla una corrida real (o peor, en silencio).
if [ -n "${JIRA_URL:-}" ] && [ -n "${JIRA_EMAIL:-}" ] && [ -n "${JIRA_API_TOKEN:-}" ]; then
  if curl -sf -u "${JIRA_EMAIL}:${JIRA_API_TOKEN}" "${JIRA_URL%/}/rest/api/3/myself" >/dev/null 2>&1; then
    add_check "jira_credentials" true ""
  else
    add_check "jira_credentials" false "JIRA_API_TOKEN invalida o vencida (${JIRA_URL})"
  fi
fi

if [ -n "${AZURE_DEVOPS_ORG_URL:-}" ] && [ -n "${AZURE_DEVOPS_PAT:-}" ]; then
  if curl -sf -u ":${AZURE_DEVOPS_PAT}" "${AZURE_DEVOPS_ORG_URL%/}/_apis/projects?api-version=7.1" >/dev/null 2>&1; then
    add_check "azure_devops_credentials" true ""
  else
    add_check "azure_devops_credentials" false "AZURE_DEVOPS_PAT invalido o vencido (${AZURE_DEVOPS_ORG_URL})"
  fi
fi

RESULT_JSON=$(jq -n --argjson ok "${overall_ok}" --argjson checks "${CHECKS_JSON}" --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{ts: $ts, ok: $ok, checks: $checks}')

if [ "${overall_ok}" = "false" ]; then
  webhook_url="${ALERT_WEBHOOK_URL:-${FALCO_ALERT_WEBHOOK_URL:-}}"
  if [ -n "${webhook_url}" ]; then
    failed_summary=$(echo "${CHECKS_JSON}" | jq -r '[.[] | select(.ok == false) | "\(.name): \(.detail)"] | join(", ")')
    curl -s -X POST "${webhook_url}" \
      -H "Content-Type: application/json" \
      -d "$(jq -n --arg text "🩺 health_check.sh: servicios caidos -- ${failed_summary}" '{text: $text}')" \
      >/dev/null 2>&1 || true
  fi
fi

if [ "${JSON_MODE}" = "true" ]; then
  echo "${RESULT_JSON}"
else
  echo "== Health check de infraestructura =="
  echo "${CHECKS_JSON}" | jq -r '.[] | if .ok then "  ✅  \(.name)" else "  ❌  \(.name) \(.detail)" end'
fi

# Rotacion oportunista: cada corrida de health_check.sh (via cron/timer) de
# paso mantiene logs/*.jsonl bajo control, sin necesitar un segundo cron
# separado solo para eso. Nunca afecta el exit code de health_check.sh --
# un fallo de rotacion no es un fallo de infraestructura real.
bash "${SCRIPT_DIR}/rotate_logs.sh" >/dev/null 2>&1 || true

[ "${overall_ok}" = "true" ]
