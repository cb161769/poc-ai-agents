#!/usr/bin/env bash
# Re-escanea el repo objetivo DESPUES de que el coding agent aplico su
# cambio -- sonar_client.py (consultado ANTES de esta corrida, como
# contexto para el prompt) solo LEE analisis ya existentes; esto es lo
# unico en el pipeline que realmente dispara un scan nuevo, sobre el diff
# real. Best-effort SIEMPRE: si el repo objetivo no tiene Sonar configurado,
# o el scanner/polling fallan, se omite sin bloquear -- nunca es un gate
# duro (misma filosofia que check_falco_correlation / output_guard sobre
# Camino A).
#
# Usage: rescan_sonar.sh <target_repo_dir> <component_name>
# Salida (stdout, JSON): {"scanned": bool, "new_issues": [...], "reason": str|null}
set -uo pipefail

TARGET_REPO_DIR="${1:?usage: rescan_sonar.sh <target_repo_dir> <component_name>}"
COMPONENT="${2:?usage: rescan_sonar.sh <target_repo_dir> <component_name>}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

skip() {
  echo "{\"scanned\": false, \"new_issues\": [], \"reason\": \"$1\"}"
  exit 0
}

if [ ! -f "${TARGET_REPO_DIR}/sonar-project.properties" ] && ! grep -qE 'sonar' "${TARGET_REPO_DIR}/pom.xml" 2>/dev/null; then
  skip "el repo objetivo no tiene sonar-project.properties ni plugin de Sonar en pom.xml -- se omite el re-scan"
fi

if [ -z "${SONAR_TOKEN:-}" ]; then
  skip "SONAR_TOKEN no esta configurado -- se omite el re-scan"
fi

SONAR_HOST_URL="${SONAR_URL:-http://localhost:9000}"

PRE_ISSUES_JSON=$(python3 "${SCRIPT_DIR}/sonar_client.py" "${COMPONENT}" 2>/dev/null) || PRE_ISSUES_JSON='{"issues": []}'

echo "Re-escaneando ${TARGET_REPO_DIR} (componente ${COMPONENT}) con sonar-scanner..." >&2
SCAN_OUTPUT=$(docker run --rm \
  -v "${TARGET_REPO_DIR}:/usr/src" \
  -e SONAR_HOST_URL="${SONAR_HOST_URL}" \
  -e SONAR_TOKEN="${SONAR_TOKEN}" \
  sonarsource/sonar-scanner-cli \
  -Dsonar.projectBaseDir=/usr/src -Dsonar.projectKey="${COMPONENT}" 2>&1)
SCAN_EXIT=$?

if [ "${SCAN_EXIT}" -ne 0 ]; then
  skip "sonar-scanner fallo: $(echo "${SCAN_OUTPUT}" | tail -c 300 | tr '\n' ' ' | tr '"' "'")"
fi

# Sondeo acotado (~60s) del Compute Engine task -- best-effort: si no
# confirma a tiempo, se sigue igual (el analisis puede seguir procesandose
# server-side; simplemente no se alcanza a comparar issues nuevos esta vez).
TASK_ID=$(echo "${SCAN_OUTPUT}" | grep -oE "task\?id=[A-Za-z0-9_-]+" | head -1 | cut -d= -f2)
if [ -n "${TASK_ID}" ]; then
  for _ in $(seq 1 12); do
    STATUS=$(curl -s -u "${SONAR_TOKEN}:" "${SONAR_HOST_URL}/api/ce/task?id=${TASK_ID}" 2>/dev/null | jq -r '.task.status // "UNKNOWN"' 2>/dev/null)
    if [ "${STATUS}" = "SUCCESS" ]; then
      break
    fi
    if [ "${STATUS}" = "FAILED" ] || [ "${STATUS}" = "CANCELED" ]; then
      skip "el analisis de Sonar termino en estado ${STATUS}"
    fi
    sleep 5
  done
fi

POST_ISSUES_JSON=$(python3 "${SCRIPT_DIR}/sonar_client.py" "${COMPONENT}" live 2>/dev/null) || POST_ISSUES_JSON='{"issues": []}'

jq -n --argjson pre "${PRE_ISSUES_JSON}" --argjson post "${POST_ISSUES_JSON}" '
  ($pre.issues // []) as $pre_issues |
  ($post.issues // []) as $post_issues |
  ($pre_issues | map(.rule + ":" + (.line|tostring))) as $pre_keys |
  {
    scanned: true,
    new_issues: [
      $post_issues[]
      | select(($pre_keys | index(.rule + ":" + (.line|tostring))) == null)
      | "[\(.severity)] \(.rule): \(.message) (linea \(.line))"
    ],
    reason: null
  }'
