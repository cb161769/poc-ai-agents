#!/usr/bin/env bash
# End-to-end orchestrator: real Jira ticket -> real Neo4j graph impact ->
# real SonarQube findings -> AI Firewall -> real gh copilot suggest, applied
# on a review branch. The scenario (clean vs. malicious) is decided by the
# real content of the Jira ticket in JIRA_TICKET_KEY, not by a flag: edit
# the ticket in Jira to break the flow, then re-run this script.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "${SCRIPT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${SCRIPT_DIR}/.env"
  set +a
fi

NEO4J_URI="${NEO4J_URI:-bolt://localhost:7687}"
NEO4J_USERNAME="${NEO4J_USERNAME:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-test_password_local}"
FIREWALL_URL="${FIREWALL_URL:-http://localhost:8080}"
CONTRIBUTION_LOG="${SCRIPT_DIR}/logs/copilot_contribution.jsonl"

fail() {
  echo "ERROR: $1" >&2
  exit 1
}

# Appends one line to logs/copilot_contribution.jsonl so
# scripts/report_sprint_metrics.py can measure how much Copilot actually
# collaborated on this sprint (not just whether the firewall let it through).
log_contribution() {
  local status="$1" redactions="$2" suggested="$3" applied="$4" branch="$5"
  mkdir -p "${SCRIPT_DIR}/logs"
  jq -n \
    --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg ticket_id "${TICKET_ID:-UNKNOWN}" \
    --arg component "${REPO_ORIGEN:-UNKNOWN}" \
    --arg firewall_status "${status}" \
    --argjson redactions_applied "${redactions:-0}" \
    --argjson copilot_suggested "${suggested}" \
    --argjson copilot_applied "${applied}" \
    --arg branch "${branch}" \
    '{ts:$ts, ticket_id:$ticket_id, component:$component, firewall_status:$firewall_status, redactions_applied:$redactions_applied, copilot_suggested:$copilot_suggested, copilot_applied:$copilot_applied, branch:$branch}' \
    >> "${CONTRIBUTION_LOG}"
}

# Posts an audit comment on the real Jira ticket so anyone reviewing the
# ticket sees, directly in Jira's own comment history, what the firewall
# decided and what Copilot did — no need to go dig through local log files.
post_jira_comment() {
  local text="$1"
  python3 "${SCRIPT_DIR}/jira_client.py" comment "${text}" >/dev/null 2>&1 \
    || echo "(no se pudo dejar el comentario de auditoria en Jira — revisa credenciales)"
}

command -v jq >/dev/null 2>&1 || fail "jq no esta instalado (requerido para parsear JSON). Instalalo y reintenta."
command -v python3 >/dev/null 2>&1 || fail "python3 no esta instalado."

echo "=================================================================="
echo " ETAPA 1/5 — Lectura del ticket Jira real"
echo "=================================================================="
JIRA_JSON="$(python3 "${SCRIPT_DIR}/jira_client.py")" || fail "no se pudo leer el ticket de Jira. Revisa .env y check_prereqs.sh"

TICKET_ID=$(echo "${JIRA_JSON}" | jq -r '.ticket_id')
SUMMARY=$(echo "${JIRA_JSON}" | jq -r '.summary')
DESCRIPTION=$(echo "${JIRA_JSON}" | jq -r '.description')
REPO_ORIGEN=$(echo "${JIRA_JSON}" | jq -r '.repository_origen')
CACHE_HIT=$(echo "${JIRA_JSON}" | jq -r '._cache.hit')

echo "Ticket:              ${TICKET_ID}"
echo "Summary:              ${SUMMARY}"
echo "Componente afectado:  ${REPO_ORIGEN}"
echo "(servido desde cache: ${CACHE_HIT})"

echo
echo "=================================================================="
echo " ETAPA 2/5 — Consulta real de impacto en el grafo Neo4j"
echo "=================================================================="
GRAPH_RESULT=$(cypher-shell -a "${NEO4J_URI}" -u "${NEO4J_USERNAME}" -p "${NEO4J_PASSWORD}" --format plain \
  "MATCH (origin {name: '${REPO_ORIGEN}'})<-[:DEPENDS_ON]-(dependent) RETURN dependent.name AS servicio, dependent.language AS lenguaje" \
  2>/dev/null) || fail "no se pudo consultar Neo4j. Revisa que 'docker compose up' este corriendo."

if [ -z "$(echo "${GRAPH_RESULT}" | tail -n +2)" ]; then
  echo "ALERTA: ningun servicio depende de '${REPO_ORIGEN}' segun el grafo actual."
else
  echo "ALERTA: los siguientes servicios se veran afectados aguas abajo por cambios en ${REPO_ORIGEN}:"
  echo "${GRAPH_RESULT}"
fi

echo
echo "=================================================================="
echo " ETAPA 3/5 — Hallazgos reales de SonarQube para ${REPO_ORIGEN}"
echo "=================================================================="
SONAR_JSON="$(python3 "${SCRIPT_DIR}/sonar_client.py" "${REPO_ORIGEN}")" || fail "no se pudo consultar SonarQube. Corre scripts/bootstrap_sonar.sh primero."

SONAR_ISSUES=$(echo "${SONAR_JSON}" | jq -r '.issues[] | "- [\(.severity)] \(.rule): \(.message) (linea \(.line))"')
SONAR_CACHE_HIT=$(echo "${SONAR_JSON}" | jq -r '._cache.hit')

if [ -z "${SONAR_ISSUES}" ]; then
  echo "Sin hallazgos abiertos para ${REPO_ORIGEN}."
else
  echo "${SONAR_ISSUES}"
fi
echo "(servido desde cache: ${SONAR_CACHE_HIT})"

echo
echo "=================================================================="
echo " ETAPA 4/5 — Composicion del prompt y envio al AI Firewall"
echo "=================================================================="
PROMPT=$(cat <<EOF
${SUMMARY}
${DESCRIPTION}
--- Grafo de impacto ---
${GRAPH_RESULT}
--- Hallazgos Sonar (reales) ---
${SONAR_ISSUES}
EOF
)

JIRA_CONTEXT=$(echo "${JIRA_JSON}" | jq '{ticket_id, summary, description, repository_origen}')
SONAR_ERRORS_ARRAY=$(echo "${SONAR_JSON}" | jq '[.issues[].message]')

PAYLOAD=$(jq -n \
  --arg prompt "${PROMPT}" \
  --argjson jira_context "${JIRA_CONTEXT}" \
  --argjson sonar_errors "${SONAR_ERRORS_ARRAY}" \
  '{prompt: $prompt, jira_context: $jira_context, sonar_errors: $sonar_errors}')

RESPONSE=$(curl -s -w '\n%{http_code}' -X POST "${FIREWALL_URL}/evaluate" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}")

HTTP_CODE=$(echo "${RESPONSE}" | tail -n1)
BODY=$(echo "${RESPONSE}" | sed '$d')

STATUS=$(echo "${BODY}" | jq -r '.status')

echo "HTTP ${HTTP_CODE} — status: ${STATUS}"

if [ "${STATUS}" = "REJECTED" ]; then
  REASON=$(echo "${BODY}" | jq -r '.reason')
  echo
  echo "🛑 EL AI FIREWALL RECHAZO LA PETICION"
  echo "Razon: ${REASON}"
  echo "gh copilot NO fue invocado."
  post_jira_comment "🛡️ AI Firewall (automatizado): solicitud RECHAZADA. Motivo: ${REASON}. gh copilot no fue invocado."
  log_contribution "REJECTED" 0 false false ""
  exit 1
fi

if [ "${STATUS}" = "APPROVED" ]; then
  SANITIZED=$(echo "${BODY}" | jq -r '.sanitized_prompt')
  REDACTIONS=$(echo "${BODY}" | jq -r '.redactions_applied')

  echo
  echo "✅ APROBADO — redacciones aplicadas: ${REDACTIONS}"
  echo
  echo "--- PROMPT ORIGINAL --------------------------------------------"
  echo "${PROMPT}"
  echo "--- PROMPT SANEADO (lo que recibe el agente) --------------------"
  echo "${SANITIZED}"
  echo "-------------------------------------------------------------"

  echo
  echo "=================================================================="
  echo " ETAPA 5/5 — Copilot sugiere y aplica el cambio (rama de revision)"
  echo "=================================================================="

  APPLIED=false
  BRANCH=""

  if [ ! -d "${SCRIPT_DIR}/sample-repo/.git" ]; then
    echo "sample-repo/ no es un repositorio git todavia. Corre:"
    echo "  git -C sample-repo init && git -C sample-repo add -A && git -C sample-repo commit -m 'baseline'"
    echo "para poder aplicar sugerencias en una rama de revision. Por ahora, solo se pedira la sugerencia."
    gh copilot suggest -t shell "${SANITIZED}"
    post_jira_comment "🤖 Copilot (automatizado): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Copilot sugirio un cambio, pero sample-repo/ todavia no es un repo git — no se aplico nada a una rama."
    log_contribution "APPROVED" "${REDACTIONS}" true false ""
    exit 0
  fi

  BRANCH="copilot/${TICKET_ID}-$(date +%s)"
  git -C "${SCRIPT_DIR}/sample-repo" checkout -b "${BRANCH}" >/dev/null

  echo "Copilot va a sugerir un comando para resolver ${TICKET_ID}. Se te pedira confirmar antes de ejecutar nada."
  gh copilot suggest -t shell "${SANITIZED}"
  SUGGEST_EXIT=$?

  if [ "${SUGGEST_EXIT}" -eq 0 ] && [ -n "$(git -C "${SCRIPT_DIR}/sample-repo" status --porcelain)" ]; then
    git -C "${SCRIPT_DIR}/sample-repo" add -A
    git -C "${SCRIPT_DIR}/sample-repo" commit -m "Copilot suggestion for ${TICKET_ID}" >/dev/null
    APPLIED=true
    echo "Cambio aplicado y commiteado en la rama '${BRANCH}' de sample-repo/ — NO en main."
    echo "Revisalo con: git -C sample-repo diff main..${BRANCH}"
    post_jira_comment "🤖 Copilot (automatizado): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Copilot aplico un cambio en la rama '${BRANCH}' de sample-repo/, pendiente de revision humana antes de mergear."
  else
    git -C "${SCRIPT_DIR}/sample-repo" checkout - >/dev/null 2>&1
    git -C "${SCRIPT_DIR}/sample-repo" branch -D "${BRANCH}" >/dev/null 2>&1
    echo "No hubo cambios que aplicar (Copilot no ejecuto ningun comando, o el comando no modifico archivos)."
    post_jira_comment "🤖 Copilot (automatizado): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Copilot no aplico ningun cambio en esta corrida."
    BRANCH=""
  fi

  log_contribution "APPROVED" "${REDACTIONS}" true "${APPLIED}" "${BRANCH}"
  exit 0
fi

fail "respuesta inesperada del firewall: ${BODY}"
