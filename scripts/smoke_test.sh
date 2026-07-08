#!/usr/bin/env bash
# End-to-end smoke test: creates a real, disposable Jira ticket, seeds a
# throwaway git repo, and runs the REAL run_poc_loop.sh against them
# (SMOKE_TEST_MODE=true stops it right after the firewall decision +
# Jira comment/transition -- see the comment in run_poc_loop.sh for why
# stage 5, the coding agent, is intentionally out of scope here: gh copilot
# suggest is an interactive TUI, and the cloud coding agent needs a real
# GitHub repo and opens its PR async, neither fits an automated smoke test).
#
# Validates the deterministic majority of the pipeline (real Jira read,
# real Neo4j graph query, real Sonar query, real firewall evaluation, real
# Jira comment/transition) with nothing mocked -- same spirit as the rest
# of this project. Cleans up after itself: the temp repo is deleted and the
# synthetic ticket is transitioned to a closed state (not deleted -- Jira
# usually requires admin rights for that).
#
# Usage: ./scripts/smoke_test.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -f "${POC_ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${POC_ROOT}/.env"
  set +a
fi

FIREWALL_URL="${FIREWALL_URL:-http://localhost:8080}"
CHECKS_FAILED=0
TMP_REPO=""
SMOKE_TICKET_KEY=""

check() {
  local label="$1" ok="$2"
  if [ "${ok}" = "0" ]; then
    echo "  ✅ ${label}"
  else
    echo "  ❌ ${label}"
    CHECKS_FAILED=$((CHECKS_FAILED + 1))
  fi
}

cleanup() {
  if [ -n "${SMOKE_TICKET_KEY}" ]; then
    echo
    echo "Cerrando el ticket sintetico ${SMOKE_TICKET_KEY}..."
    JIRA_TICKET_KEY="${SMOKE_TICKET_KEY}" python3 "${POC_ROOT}/jira_client.py" transition \
      "${JIRA_SMOKE_TEST_DONE_STATUS:-Done}" >/dev/null 2>&1 \
      || echo "(no se pudo cerrar ${SMOKE_TICKET_KEY} automaticamente — ajusta JIRA_SMOKE_TEST_DONE_STATUS o cerralo a mano)"
  fi
  if [ -n "${TMP_REPO}" ] && [ -d "${TMP_REPO}" ]; then
    rm -rf "${TMP_REPO}"
  fi
}
trap cleanup EXIT

echo "== Smoke test end-to-end (etapas 1-4, ver run_poc_loop.sh para el porque de la 5) =="

command -v cypher-shell >/dev/null 2>&1 || { echo "ERROR: cypher-shell no esta en PATH."; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "ERROR: jq no esta instalado."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 no esta instalado."; exit 1; }
command -v git >/dev/null 2>&1 || { echo "ERROR: git no esta instalado."; exit 1; }

curl -sf "${FIREWALL_URL}/health" >/dev/null 2>&1
check "AI Firewall alcanzable en ${FIREWALL_URL}" "$?"
if [ "${CHECKS_FAILED}" -gt 0 ]; then
  echo "El firewall no esta arriba, no tiene sentido seguir. Corre 'docker compose up -d' primero."
  exit 1
fi

# --- Repo temporal, real, limpio ---
TMP_REPO=$(mktemp -d)
cat > "${TMP_REPO}/requirements.txt" <<'EOF'
pytest
EOF
cat > "${TMP_REPO}/test_smoke.py" <<'EOF'
def test_smoke():
    assert True
EOF
git -C "${TMP_REPO}" init -q
git -C "${TMP_REPO}" -c user.email="smoke-test@local" -c user.name="smoke-test" add -A
git -C "${TMP_REPO}" -c user.email="smoke-test@local" -c user.name="smoke-test" commit -q -m "smoke test baseline"
check "Repo temporal creado y limpio en ${TMP_REPO}" "0"

# --- Componente sintetico: primero de JIRA_KNOWN_COMPONENTS, funciona con
# el default de .env.example sin configuracion extra ---
COMPONENT=$(echo "${JIRA_KNOWN_COMPONENTS:-AuthService,Frontend,DataWorker}" | cut -d, -f1)

# --- Ticket real descartable ---
CREATE_STDERR_FILE=$(mktemp)
CREATE_STDOUT=$(python3 "${POC_ROOT}/jira_client.py" create-smoke-ticket "${COMPONENT}" 2>"${CREATE_STDERR_FILE}")
CREATE_EXIT=$?
CREATE_STDERR=$(cat "${CREATE_STDERR_FILE}")
rm -f "${CREATE_STDERR_FILE}"
if [ "${CREATE_EXIT}" -ne 0 ]; then
  echo "ERROR: no se pudo crear el ticket sintetico: ${CREATE_STDERR}${CREATE_STDOUT}"
  exit 1
fi
SMOKE_TICKET_KEY=$(echo "${CREATE_STDOUT}" | jq -r '.ticket_id')
check "Ticket real creado: ${SMOKE_TICKET_KEY} (componente ${COMPONENT})" "0"

# --- Corre el run_poc_loop.sh REAL, parado en el repo temporal, forzando
# Camino B (GITHUB_REPO vacio -- un smoke test no debe tocar tu repo GitHub
# real) y parada en etapa 4. El ticket va como argumento posicional, NO como
# prefijo de variable de entorno: run_poc_loop.sh sourcea .env DESPUES de
# arrancar, así que un JIRA_TICKET_KEY=... de prefijo quedaria pisado por lo
# que sea que tengas en tu .env real. GITHUB_REPO="" como prefijo es
# inofensivo aca porque SMOKE_TEST_MODE corta el script antes de llegar a
# ese chequeo, pero lo dejamos igual por las dudas.
echo
echo "Corriendo run_poc_loop.sh contra el ticket sintetico..."
(
  cd "${TMP_REPO}" \
  && GITHUB_REPO="" SMOKE_TEST_MODE=true "${POC_ROOT}/run_poc_loop.sh" "${SMOKE_TICKET_KEY}"
)
PIPELINE_EXIT=$?
check "run_poc_loop.sh termino con exit 0" "$([ "${PIPELINE_EXIT}" -eq 0 ] && echo 0 || echo 1)"

# --- Valida efectos reales ---
LAST_AUDIT_LINE=$(grep "\"ticket_id\": \"${SMOKE_TICKET_KEY}\"" "${POC_ROOT}/logs/firewall_audit.jsonl" 2>/dev/null | tail -n1)
if [ -n "${LAST_AUDIT_LINE}" ] && [ "$(echo "${LAST_AUDIT_LINE}" | jq -r '.status')" = "APPROVED" ]; then
  check "logs/firewall_audit.jsonl tiene un APPROVED real para ${SMOKE_TICKET_KEY}" "0"
else
  check "logs/firewall_audit.jsonl tiene un APPROVED real para ${SMOKE_TICKET_KEY}" "1"
fi

JIRA_URL_CLEAN="${JIRA_URL%/}"
AUTH=$(printf '%s:%s' "${JIRA_EMAIL}" "${JIRA_API_TOKEN}" | base64 | tr -d '\n')
COMMENTS_JSON=$(curl -sf -H "Authorization: Basic ${AUTH}" -H "Accept: application/json" \
  "${JIRA_URL_CLEAN}/rest/api/3/issue/${SMOKE_TICKET_KEY}/comment" 2>/dev/null)
if echo "${COMMENTS_JSON}" | jq -e '.comments[] | select(.body.content[0].content[0].text | test("Smoke test"))' >/dev/null 2>&1; then
  check "El ticket real tiene el comentario de auditoria del smoke test" "0"
else
  check "El ticket real tiene el comentario de auditoria del smoke test" "1"
fi

echo
if [ "${CHECKS_FAILED}" -eq 0 ]; then
  echo "== Smoke test OK — etapas 1-4 validadas de punta a punta contra infraestructura real =="
  exit 0
else
  echo "== Smoke test FALLO — ${CHECKS_FAILED} check(s) en rojo, revisa arriba =="
  exit 1
fi
