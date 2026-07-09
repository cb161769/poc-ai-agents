#!/usr/bin/env bash
# End-to-end orchestrator: real Jira ticket -> real Neo4j graph impact ->
# real SonarQube findings -> AI Firewall -> real gh copilot suggest, applied
# on a review branch OF YOUR REAL REPO. The scenario (clean vs. malicious)
# is decided by the real content of the Jira ticket, not by a flag: edit
# the ticket in Jira to break the flow, then re-run this script.
#
# Which ticket: pass it as the first argument (./run_poc_loop.sh JIRA-123)
# to work any ticket someone hands to Copilot without touching .env. Without
# an argument, falls back to JIRA_TICKET_KEY from .env (the original
# behavior, still handy for repeatedly testing the same ticket).
#
# Epic mode: ./run_poc_loop.sh --epic EPIC-123 fetches the epic and ALL its
# children, and runs ONE combined prompt through the whole pipeline instead
# of processing children one by one. Only works if every child's component
# resolves to the SAME repo_url in the Neo4j graph -- this pipeline is built
# around one repo per run (TARGET_REPO_DIR), so if the epic's children
# genuinely live in different repos, it refuses instead of guessing.
#
# IMPORTANT: this operates on whatever git repo you are standing in when you
# invoke it (cd to your real project first) — it does NOT use sample-repo/
# by default. sample-repo/ stays in this project only as a reference of what
# project file each stack needs for scripts/run_module_tests.sh to
# auto-detect it.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "${SCRIPT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${SCRIPT_DIR}/.env"
  set +a
fi

EPIC_MODE=false
EPIC_KEY=""
if [ "${1:-}" = "--epic" ]; then
  EPIC_MODE=true
  EPIC_KEY="${2:?usage: run_poc_loop.sh --epic <EPIC_KEY>}"
elif [ -n "${1:-}" ]; then
  JIRA_TICKET_KEY="$1"
fi

NEO4J_URI="${NEO4J_URI:-bolt://localhost:7687}"
NEO4J_USERNAME="${NEO4J_USERNAME:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-test_password_local}"
FIREWALL_URL="${FIREWALL_URL:-http://localhost:8080}"
CONTRIBUTION_LOG="${SCRIPT_DIR}/logs/copilot_contribution.jsonl"
# Historias hijas de la epica actual, para record_run_in_graph() -- solo se
# llena en modo --epic (run_epic_etapas()); en modo ticket normal queda "[]".
CHILD_TICKET_KEYS_JSON="[]"
# Path al archivo temporal con la conversacion del coding agent (Camino B1),
# para que retry_coding_agent_with_feedback() pueda continuarla en vez de
# re-investigar desde cero. Vacio si todavia no corrio el coding agent.
CONVERSATION_FILE=""

fail() {
  echo "ERROR: $1" >&2
  exit 1
}

# Bounded retry (2 reintentos, backoff 1s/2s) para comandos que le pegan a
# servicios reales (curl al firewall, cypher-shell a Neo4j) -- mismo criterio
# que ya usa orchestration.py por task de Prefect y retry_utils.py del lado
# Python. Antes de esto, run_poc_loop.sh no reintentaba nada: un solo blip
# transitorio (Neo4j reiniciando, una conexion que se corta) tiraba abajo
# toda la corrida. "$@" es el comando completo a ejecutar.
retry_cmd() {
  local max_retries=2 backoff=(1 2) attempt=0
  while true; do
    if "$@"; then
      return 0
    fi
    if [ "${attempt}" -ge "${max_retries}" ]; then
      return 1
    fi
    sleep "${backoff[${attempt}]}"
    attempt=$((attempt + 1))
  done
}

# Circuit breaker simple por servicio: si Neo4j ya fallo (tras agotar los
# reintentos de retry_cmd) una vez en esta corrida, las siguientes consultas
# al grafo fallan rapido con un mensaje claro en vez de volver a intentar y
# esperar el mismo timeout de nuevo.
NEO4J_DOWN=false

# Wrapper de cypher-shell con retry + circuit breaker. Usage:
#   cypher_query "<query cypher>" -> stdout con el resultado, o falla.
cypher_query() {
  local query="$1"
  if [ "${NEO4J_DOWN}" = "true" ]; then
    echo "ERROR: Neo4j ya fallo en esta corrida, se omite este intento (circuit breaker abierto)." >&2
    return 1
  fi
  if ! retry_cmd cypher-shell -a "${NEO4J_URI}" -u "${NEO4J_USERNAME}" -p "${NEO4J_PASSWORD}" --format plain "${query}"; then
    NEO4J_DOWN=true
    echo "ERROR: no se pudo consultar Neo4j tras reintentar -- se omiten mas consultas al grafo en esta corrida." >&2
    return 1
  fi
}

# Detects the real repo you're standing in (the cwd you ran this script
# from, not SCRIPT_DIR where the tool itself lives) and refuses to continue
# if it's dirty — the pipeline is about to create a branch and commit to it,
# and we don't want to sweep up your in-progress work into Copilot's branch.
TARGET_REPO_DIR="$(git rev-parse --show-toplevel 2>/dev/null)" \
  || fail "No estas parado dentro de un repositorio git. cd a tu proyecto real (el que corresponde al ticket) antes de correr run_poc_loop.sh — ya no se usa sample-repo/ por defecto."

if [ -n "$(git -C "${TARGET_REPO_DIR}" status --porcelain)" ]; then
  fail "El repo en ${TARGET_REPO_DIR} tiene cambios sin commitear. Hace commit o 'git stash' antes de correr esto — el pipeline va a crear una rama nueva y no queremos mezclar tu trabajo en progreso con lo que haga Copilot."
fi

echo "Repo objetivo detectado: ${TARGET_REPO_DIR}"

# Appends one line to logs/copilot_contribution.jsonl so
# scripts/report_sprint_metrics.py can measure how much Copilot actually
# collaborated on this sprint (not just whether the firewall let it through).
log_contribution() {
  local status="$1" redactions="$2" suggested="$3" applied="$4" branch="$5" tests_passed="${6:-null}"
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
    --argjson tests_passed "${tests_passed}" \
    '{ts:$ts, ticket_id:$ticket_id, component:$component, firewall_status:$firewall_status, redactions_applied:$redactions_applied, copilot_suggested:$copilot_suggested, copilot_applied:$copilot_applied, branch:$branch, tests_passed:$tests_passed}' \
    >> "${CONTRIBUTION_LOG}"
}

# Posts an audit comment on the real Jira ticket so anyone reviewing the
# ticket sees, directly in Jira's own comment history, what the firewall
# decided and what Copilot did — no need to go dig through local log files.
# In epic mode TICKET_ID is the epic key, so this naturally comments on the
# epic; run_epic_etapas() also comments on each child individually.
post_jira_comment() {
  local text="$1"
  JIRA_TICKET_KEY="${TICKET_ID:-${JIRA_TICKET_KEY:-}}" python3 "${SCRIPT_DIR}/jira_client.py" comment "${text}" >/dev/null 2>&1 \
    || echo "(no se pudo dejar el comentario de auditoria en Jira — revisa credenciales)"
}

# Posts a plain-text alert to ALERT_WEBHOOK_URL (formato compatible con un
# incoming webhook de Slack: {"text": "..."}) si esta configurado -- antes
# solo Falco tenia esta capacidad (FALCO_ALERT_WEBHOOK_URL); ahora tambien
# se usa cuando el juez marca FLAGGED o el testing agent bloquea una corrida,
# asi alguien recibe una alerta activa en vez de tener que leer Jira/JSONL.
# Retrocompatible: si ALERT_WEBHOOK_URL no esta seteada, cae a
# FALCO_ALERT_WEBHOOK_URL para no romper a quien ya la tenia configurada.
post_alert_webhook() {
  local text="$1"
  local webhook_url="${ALERT_WEBHOOK_URL:-${FALCO_ALERT_WEBHOOK_URL:-}}"
  [ -z "${webhook_url}" ] && return 0
  curl -s -X POST "${webhook_url}" \
    -H "Content-Type: application/json" \
    -d "$(jq -n --arg text "${text}" '{text: $text}')" \
    >/dev/null 2>&1 || echo "(no se pudo postear al webhook de alertas)"
}

# Moves the real ticket in Jira to JIRA_IN_PROGRESS_STATUS (default
# "In Progress") so its status reflects that an agent is working on it,
# without a human having to drag it across the board by hand. Best-effort:
# if the workflow doesn't have that exact transition name, it just warns.
transition_jira_ticket() {
  local target_status="${JIRA_IN_PROGRESS_STATUS:-In Progress}"
  JIRA_TICKET_KEY="${TICKET_ID:-${JIRA_TICKET_KEY:-}}" python3 "${SCRIPT_DIR}/jira_client.py" transition "${target_status}" >/dev/null 2>&1 \
    || echo "(no se pudo mover el ticket a '${target_status}' — puede que tu workflow use otro nombre; revisa JIRA_IN_PROGRESS_STATUS)"
}

# Registra esta corrida en el grafo de conocimiento de Neo4j (graph_writer.py)
# -- Story/Epic, Run, una Decision por etapa (firewall/output_guard/tests/judge),
# y un Risk si el juez cito un policy_reference real (evals/JUDGE_POLICY.md).
# Best-effort SIEMPRE: graph_writer.py ya maneja fallos de conexion
# internamente y nunca devuelve exit != 0 por eso, pero igual no dejamos que
# esto frene la corrida si algo mas raro pasa (jq roto, etc).
record_run_in_graph() {
  local branch="$1" backend="$2" tests_status="$3" tests_reason="$4" judge_status="$5" judge_reason="$6" judge_policy_ref="$7"
  local og_status="${8:-SKIPPED}" og_reason="${9:-}"

  local components_json is_epic_json decisions_json payload run_id
  components_json=$(echo "${REPO_ORIGEN}" | tr ',' '\n' | jq -R . | jq -s .)
  if [ -n "${EPIC_KEY}" ]; then
    is_epic_json="true"
  else
    is_epic_json="false"
  fi
  run_id="${TICKET_ID}-$(date +%s)"

  decisions_json=$(jq -n \
    --arg fw_status "${STATUS}" --arg fw_reason "${REASON:-}" \
    --arg og_status "${og_status}" --arg og_reason "${og_reason}" \
    --arg tests_status "${tests_status}" --arg tests_reason "${tests_reason}" \
    --arg judge_status "${judge_status}" --arg judge_reason "${judge_reason}" \
    --arg judge_policy_ref "${judge_policy_ref}" \
    '[
      {stage: "firewall", status: $fw_status, reason: (if $fw_reason == "" then null else $fw_reason end), policy_reference: null},
      {stage: "output_guard", status: $og_status, reason: (if $og_reason == "" then null else $og_reason end), policy_reference: null},
      {stage: "tests", status: $tests_status, reason: (if $tests_reason == "" then null else $tests_reason end), policy_reference: null},
      {stage: "judge", status: $judge_status, reason: (if $judge_reason == "" then null else $judge_reason end), policy_reference: (if $judge_policy_ref == "" or $judge_policy_ref == "null" then null else $judge_policy_ref end)}
    ]')

  payload=$(jq -n \
    --arg run_id "${run_id}" \
    --arg ticket_key "${TICKET_ID}" \
    --arg ticket_summary "${SUMMARY:-}" \
    --argjson is_epic "${is_epic_json}" \
    --argjson child_ticket_keys "${CHILD_TICKET_KEYS_JSON:-[]}" \
    --argjson components "${components_json}" \
    --arg branch "${branch}" \
    --arg backend "${backend}" \
    --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson decisions "${decisions_json}" \
    '{run_id: $run_id, ticket_key: $ticket_key, ticket_summary: $ticket_summary, is_epic: $is_epic, child_ticket_keys: $child_ticket_keys, components: $components, branch: (if $branch == "" then null else $branch end), backend: (if $backend == "" then null else $backend end), ts: $ts, decisions: $decisions}')

  echo "${payload}" | python3 "${SCRIPT_DIR}/graph_writer.py" >/dev/null 2>&1 || true
}

# Criterios de policy_reference que el juez puede marcar y que el coding
# agent tiene chance real de corregir -- debe mantenerse sincronizado con
# RETRYABLE_POLICY_REFERENCES en judge_agent.py. Deliberadamente NO incluye
# data-leak-evidence/jailbreak-evidence/firewall-false-negative/other: esos
# son de seguridad o ambiguos, nunca se reintentan automaticamente.
RETRYABLE_POLICY_REFS=(scope-mismatch insufficient-test-coverage graph-impact-unverified)

# Borra el archivo temporal de conversacion del coding agent (Camino B1) si
# todavia esta ahi -- se llama en cada punto donde una corrida llega a un
# resultado final y nadie mas lo va a consumir (si ya se consumio en un
# reintento, el archivo ya no existe y esto es un no-op).
cleanup_conversation_file() {
  if [ -n "${CONVERSATION_FILE:-}" ] && [ -f "${CONVERSATION_FILE}" ]; then
    rm -f "${CONVERSATION_FILE}"
    CONVERSATION_FILE=""
  fi
}

# Le da al coding agent un segundo intento (UNA sola vez) cuando el juez
# marco un problema potencialmente corregible. Reusa la MISMA rama (no crea
# una nueva), le agrega el feedback del juez al prompt original, y si
# produce cambios nuevos vuelve a correr tests + juez una vez mas
# (run_judge con retry_count=1, que ya no vuelve a intentar). Devuelve 0 si
# el segundo intento produjo algun resultado manejado (tests fallidos o un
# nuevo veredicto), 1 si no hubo cambios nuevos -- en ese caso el llamador
# se queda con el veredicto FLAGGED original.
retry_coding_agent_with_feedback() {
  local branch="$1" reasoning="$2"
  local retry_payload_file retry_agent_result_json retry_diff_text retry_tests_gate_result
  local feedback_text="--- FEEDBACK DEL JUEZ (corregir antes de continuar) ---
${reasoning}"

  retry_payload_file=$(mktemp)
  if [ -n "${CONVERSATION_FILE:-}" ] && [ -f "${CONVERSATION_FILE}" ]; then
    # Continua la MISMA conversacion en vez de arrancar de cero -- evita
    # repagar la investigacion (listar el repo, leer archivos) que el
    # primer intento ya hizo. sanitized_prompt pasa a ser solo el feedback,
    # no el ticket completo de nuevo.
    jq -n --arg ticket_id "${TICKET_ID}" --arg feedback "${feedback_text}" --arg repo "${TARGET_REPO_DIR}" \
      --slurpfile conv "${CONVERSATION_FILE}" \
      '{ticket_id: $ticket_id, sanitized_prompt: $feedback, target_repo_dir: $repo,
        resume_messages: $conv[0].messages,
        resume_state: {has_investigated: $conv[0].has_investigated, has_run_verification: $conv[0].has_run_verification}}' \
      > "${retry_payload_file}"
    rm -f "${CONVERSATION_FILE}"
  else
    # Sin conversacion previa disponible (corrida vieja, o el primer intento
    # no llego a generarla) -- cae al comportamiento anterior, prompt desde cero.
    local augmented_prompt="${SANITIZED}

${feedback_text}"
    jq -n --arg ticket_id "${TICKET_ID}" --arg sanitized "${augmented_prompt}" --arg repo "${TARGET_REPO_DIR}" \
      '{ticket_id: $ticket_id, sanitized_prompt: $sanitized, target_repo_dir: $repo}' > "${retry_payload_file}"
  fi

  retry_agent_result_json=$(python3 "${SCRIPT_DIR}/coding_agent.py" "${retry_payload_file}")
  rm -f "${retry_payload_file}"
  echo "Resultado del segundo intento: $(echo "${retry_agent_result_json}" | jq -r '.status') — $(echo "${retry_agent_result_json}" | jq -r '.summary')"
  AGENT_BACKEND=$(echo "${retry_agent_result_json}" | jq -r '._meta.backend // "unknown"')
  CONVERSATION_FILE=$(echo "${retry_agent_result_json}" | jq -r '._conversation_file // ""')

  if [ -z "$(git -C "${TARGET_REPO_DIR}" status --porcelain)" ]; then
    echo "El segundo intento no produjo cambios nuevos."
    return 1
  fi

  git -C "${TARGET_REPO_DIR}" add -A
  git -C "${TARGET_REPO_DIR}" commit -m "Coding agent retry for ${TICKET_ID} (feedback del juez)" >/dev/null

  retry_diff_text=$(git -C "${TARGET_REPO_DIR}" diff "${base_branch}..${branch}")

  if ! run_output_guard "${retry_diff_text}" "${branch}"; then
    return 0
  fi

  run_tests_gate "${REPO_ORIGEN}" "${branch}"
  retry_tests_gate_result=$?
  log_contribution "APPROVED" "${REDACTIONS}" true true "${branch}" "${TEST_PASSED}"

  if [ "${retry_tests_gate_result}" -eq 0 ]; then
    run_judge "local_diff" "${retry_diff_text}" "rama '${branch}' de ${TARGET_REPO_DIR} (segundo intento)" "${branch}" "" "${TEST_OUTPUT}" 1
  fi
  return 0
}

# Independent judge (Claude, no gh copilot) que revisa la decision del
# firewall + el cambio real (o el issue, si el coding agent en la nube
# todavia no genero PR) + la corrida completa. Si marca FLAGGED, tiene poder
# de bloqueo real: reabre/transiciona el ticket a un estado bloqueado, deja
# un comentario fuerte, y si hay una rama local, la marca inequivocamente en
# el propio historial de git.
#
# Ultimo parametro (retry_count, default 0): si el veredicto es FLAGGED con
# un policy_reference retryable, change_source="local_diff", y todavia no se
# reintento en esta corrida, le da al coding agent un segundo intento
# (retry_coding_agent_with_feedback) en vez de bloquear directo. El segundo
# llamado a run_judge se hace con retry_count=1, así no hay un tercer intento.
run_judge() {
  local change_source="$1" change_description="$2" location_label="$3" branch="${4:-}" issue_url="${5:-}" test_summary="${6:-sin tests corridos para esta corrida}" retry_count="${7:-0}"

  echo
  echo "=================================================================="
  echo " ETAPA 7 — Agente juez (segunda opinion independiente, con poder de bloqueo)"
  echo "=================================================================="

  local judge_payload verdict_json verdict reasoning policy_ref
  local tests_status_for_graph="SKIPPED"
  if [ "${TEST_PASSED:-}" = "true" ]; then
    tests_status_for_graph="PASSED"
  elif [ "${TEST_PASSED:-}" = "false" ]; then
    tests_status_for_graph="FAILED"
  fi

  judge_payload=$(jq -n \
    --argjson ticket "${JIRA_CONTEXT}" \
    --arg status "${STATUS}" \
    --arg reason "${REASON:-}" \
    --argjson redactions "${REDACTIONS:-0}" \
    --arg change_source "${change_source}" \
    --arg change_description "${change_description}" \
    --arg test_summary "${test_summary}" \
    '{ticket: $ticket, firewall: {status: $status, reason: $reason, redactions_applied: $redactions}, change_source: $change_source, change_description: $change_description, test_summary: $test_summary}')

  if ! verdict_json=$(echo "${judge_payload}" | python3 "${SCRIPT_DIR}/judge_agent.py" 2>/dev/null); then
    echo "El juez no pudo evaluar esta corrida (revisa ANTHROPIC_API_KEY, Ollama local, o conectividad). Continua sin veredicto."
    record_run_in_graph "${branch}" "${AGENT_BACKEND:-}" "${tests_status_for_graph}" "" "SKIPPED" "sin backend de modelo disponible" ""
    cleanup_conversation_file
    return
  fi

  verdict=$(echo "${verdict_json}" | jq -r '.verdict')
  reasoning=$(echo "${verdict_json}" | jq -r '.reasoning')
  policy_ref=$(echo "${verdict_json}" | jq -r '.policy_reference // ""')

  echo "Veredicto del juez: ${verdict}"
  echo "Razonamiento: ${reasoning}"

  if [ "${verdict}" = "FLAGGED" ] && [ "${change_source}" = "local_diff" ] && [ "${retry_count}" = "0" ]; then
    local ref is_retryable=false
    for ref in "${RETRYABLE_POLICY_REFS[@]}"; do
      [ "${ref}" = "${policy_ref}" ] && is_retryable=true
    done
    if [ "${is_retryable}" = "true" ]; then
      echo
      echo "🔁 El juez marco un problema potencialmente corregible (${policy_ref}) -- dandole al coding agent un segundo intento con el feedback."
      if retry_coding_agent_with_feedback "${branch}" "${reasoning}"; then
        return
      fi
      echo "El segundo intento no produjo cambios -- se bloquea con el veredicto original del juez."
    fi
  fi

  if [ "${verdict}" = "FLAGGED" ] && [ "${change_source}" = "firewall_rejected" ]; then
    # El juez audita un rechazo, nunca lo revierte: el firewall sigue siendo
    # la ultima palabra en seguridad. Esto es solo una alerta de posible
    # falso positivo para revision humana, no un desbloqueo.
    echo
    echo "⚠️  EL JUEZ SOSPECHA QUE EL RECHAZO DEL FIREWALL FUE UN FALSO POSITIVO"
    post_jira_comment "🧑‍⚖️ Agente juez (automatizado): el rechazo del firewall podria ser incorrecto — ${reasoning} La solicitud SIGUE RECHAZADA (el juez no puede revertir al firewall); revision humana recomendada."

  elif [ "${verdict}" = "FLAGGED" ]; then
    echo
    echo "🚫 EL JUEZ MARCO ESTA CORRIDA COMO PROBLEMATICA — ${location_label}"

    post_jira_comment "🧑‍⚖️ Agente juez (automatizado): FLAGGED. ${reasoning} — ${location_label} bloqueado, requiere revision humana antes de continuar."
    post_alert_webhook "🧑‍⚖️ Juez FLAGGED en ${TICKET_ID:-UNKNOWN} (${location_label}): ${reasoning}"

    JIRA_TICKET_KEY="${TICKET_ID:-${JIRA_TICKET_KEY:-}}" python3 "${SCRIPT_DIR}/jira_client.py" transition "${JIRA_BLOCKED_STATUS:-Blocked}" >/dev/null 2>&1 \
      || echo "(no se pudo mover el ticket a '${JIRA_BLOCKED_STATUS:-Blocked}' — ajusta JIRA_BLOCKED_STATUS a un nombre real de tu workflow)"

    if [ -n "${branch}" ]; then
      git -C "${TARGET_REPO_DIR}" commit --allow-empty -m "BLOCKED BY JUDGE: ${reasoning}" >/dev/null 2>&1
      echo "Rama '${branch}' marcada como bloqueada en su propio historial de git — no la mergees sin revision."
    fi

    if [ -n "${issue_url}" ]; then
      gh issue edit "${issue_url}" --remove-assignee "${GITHUB_COPILOT_ASSIGNEE:-copilot-swe-agent}" >/dev/null 2>&1 || true
      gh issue comment "${issue_url}" --body "🧑‍⚖️ Agente juez: FLAGGED. ${reasoning}. Se retiro la asignacion al coding agent hasta revision humana." >/dev/null 2>&1 || true
      echo "Se intento retirar la asignacion del coding agent en ${issue_url} (mejor esfuerzo)."
    fi
  elif [ "${change_source}" = "firewall_rejected" ]; then
    post_jira_comment "🧑‍⚖️ Agente juez (automatizado): OK, el rechazo del firewall fue correcto. ${reasoning}"
  else
    post_jira_comment "🧑‍⚖️ Agente juez (automatizado): OK. ${reasoning}"
  fi

  record_run_in_graph "${branch}" "${AGENT_BACKEND:-}" "${tests_status_for_graph}" "" "${verdict}" "${reasoning}" "${policy_ref}"

  # Este veredicto es el final para esta corrida (no hay mas reintentos
  # despues de esto) -- limpiamos el archivo temporal de conversacion si
  # todavia esta ahi (nadie mas lo va a consumir).
  cleanup_conversation_file
}

# Guardia de salida: el AI Firewall (firewall_proxy.py) audita el prompt que
# ENTRA al coding agent, pero nadie volvia a auditar el diff que efectivamente
# produce -- si el agente escribia un secreto real al "arreglar" algo, nada lo
# atrapaba hasta Sonar (si cubria ese patron) o el juez (si lo notaba). Esto
# corre las MISMAS reglas del firewall (output_guard.py, que reusa
# firewall_proxy._redact()/_check_jailbreak() directo) sobre el diff real,
# ANTES del testing agent -- si encuentra algo, bloquea con la misma severidad
# que un test fallido: comentario fuerte, transicion a bloqueado, commit
# marcador, y el juez ni se llama (no tiene sentido pedirle opinion a algo
# que ya se bloqueo por evidencia dura).
run_output_guard() {
  local diff_text="$1" branch="$2"
  local guard_payload guard_result redactions jailbreak_reason

  guard_payload=$(jq -n --arg diff "${diff_text}" --argjson ticket "${JIRA_CONTEXT}" '{diff_text: $diff, jira_context: $ticket}')
  guard_result=$(echo "${guard_payload}" | python3 "${SCRIPT_DIR}/output_guard.py" 2>/dev/null) || guard_result='{"clean": true}'

  redactions=$(echo "${guard_result}" | jq -r '.redactions_applied // 0')
  jailbreak_reason=$(echo "${guard_result}" | jq -r '.jailbreak_reason // empty')

  if [ "${redactions}" = "0" ] && [ -z "${jailbreak_reason}" ]; then
    return 0
  fi

  echo
  echo "🚫 LA GUARDIA DE SALIDA DETECTO UN PROBLEMA EN EL DIFF GENERADO — rama '${branch}'"
  echo "Redacciones: ${redactions} — Jailbreak: ${jailbreak_reason:-ninguno}"

  local og_reason="redacciones=${redactions}, jailbreak=${jailbreak_reason:-ninguno}"
  post_jira_comment "🛡️ Guardia de salida (automatizada): el diff generado por el coding agent contiene evidencia real de fuga de datos o manipulacion (${og_reason}). Bloqueado antes de llegar al testing agent — requiere revision humana."
  post_alert_webhook "🛡️ Guardia de salida BLOCKED en ${TICKET_ID:-UNKNOWN}: ${og_reason}"

  JIRA_TICKET_KEY="${TICKET_ID:-${JIRA_TICKET_KEY:-}}" python3 "${SCRIPT_DIR}/jira_client.py" transition "${JIRA_BLOCKED_STATUS:-Blocked}" >/dev/null 2>&1 \
    || echo "(no se pudo mover el ticket a '${JIRA_BLOCKED_STATUS:-Blocked}' — ajusta JIRA_BLOCKED_STATUS a un nombre real de tu workflow)"

  if [ -n "${branch}" ]; then
    git -C "${TARGET_REPO_DIR}" commit --allow-empty -m "BLOCKED BY OUTPUT GUARD: ${og_reason}" >/dev/null 2>&1
    echo "Rama '${branch}' marcada como bloqueada en su propio historial de git — no la mergees sin revision."
  fi

  record_run_in_graph "${branch}" "${AGENT_BACKEND:-}" "SKIPPED" "" "SKIPPED" "no se llego a llamar al juez, la guardia de salida bloqueo antes" "" "FAILED" "${og_reason}"
  cleanup_conversation_file

  return 1
}

# Testing agent (deterministic, not an LLM): corre el test suite real del
# modulo afectado en un contenedor descartable (scripts/run_module_tests.sh)
# contra la rama que el fallback local acaba de commitear. Es un gate: si
# fallan los tests, se bloquea aca mismo y el juez ni se llama.
run_tests_gate() {
  local component="$1" branch="$2"
  TEST_OUTPUT=""
  TEST_PASSED=false

  echo
  echo "=================================================================="
  echo " ETAPA 6 — Testing agent (build/test real, gate antes del juez)"
  echo "=================================================================="

  if ! command -v docker >/dev/null 2>&1; then
    echo "docker no disponible — se omite el testing agent para esta corrida."
    TEST_OUTPUT="(testing agent omitido: docker no disponible en el host)"
    TEST_PASSED=true
    return 0
  fi

  if TEST_OUTPUT=$("${SCRIPT_DIR}/scripts/run_module_tests.sh" "${TARGET_REPO_DIR}" 2>&1); then
    TEST_PASSED=true
    echo "${TEST_OUTPUT}"
    return 0
  fi

  TEST_PASSED=false
  echo "${TEST_OUTPUT}"
  echo
  echo "🚫 EL TESTING AGENT MARCO ESTA CORRIDA COMO FALLIDA — rama '${branch}'"

  post_jira_comment "🧪 Testing agent (automatizado): los tests reales de '${component}' FALLARON en la rama '${branch}'. Bloqueado antes de llegar al juez — requiere revision humana."
  post_alert_webhook "🧪 Testing agent BLOCKED en ${TICKET_ID:-UNKNOWN}: los tests reales de '${component}' fallaron en la rama '${branch}'."

  JIRA_TICKET_KEY="${TICKET_ID:-${JIRA_TICKET_KEY:-}}" python3 "${SCRIPT_DIR}/jira_client.py" transition "${JIRA_BLOCKED_STATUS:-Blocked}" >/dev/null 2>&1 \
    || echo "(no se pudo mover el ticket a '${JIRA_BLOCKED_STATUS:-Blocked}' — ajusta JIRA_BLOCKED_STATUS a un nombre real de tu workflow)"

  if [ -n "${branch}" ]; then
    git -C "${TARGET_REPO_DIR}" commit --allow-empty -m "BLOCKED BY TESTS: el test suite real fallo" >/dev/null 2>&1
    echo "Rama '${branch}' marcada como bloqueada en su propio historial de git — no la mergees sin revision."
  fi

  record_run_in_graph "${branch}" "${AGENT_BACKEND:-}" "FAILED" "el test suite real fallo" "SKIPPED" "no se llego a llamar al juez, los tests bloquearon antes" ""
  cleanup_conversation_file

  return 1
}

# Correlaciona logs/falco_alerts.jsonl (que Falco ya viene escribiendo en
# tiempo real, ver falco/custom_rules.yaml) con la ventana de esta corrida
# del pipeline. Puramente informativo: nunca bloquea la corrida, solo la
# deja en evidencia en Jira y, si esta configurado, en un webhook.
check_falco_correlation() {
  local since="$1"
  local result count
  result=$(python3 "${SCRIPT_DIR}/scripts/check_falco_alerts.py" "${since}" "${SCRIPT_DIR}/logs/falco_alerts.jsonl" 2>/dev/null)
  count=$(echo "${result}" | jq -r '.count // 0' 2>/dev/null)

  if [ -z "${count}" ] || [ "${count}" = "0" ]; then
    return 0
  fi

  echo
  echo "🚨 Falco registro ${count} alerta(s) durante esta corrida:"
  echo "${result}" | jq -r '.alerts[] | "  [\(.priority)] \(.rule): \(.output)"'

  local summary
  summary=$(echo "${result}" | jq -r '.alerts[] | "- [\(.priority)] \(.rule): \(.output)"' | tr '\n' ' ')
  post_jira_comment "🚨 Falco (monitoreo a nivel de sistema, automatizado): se detectaron ${count} alerta(s) durante esta corrida — ${summary}"
  post_alert_webhook "🚨 Falco detecto ${count} alerta(s) en la corrida de ${TICKET_ID:-UNKNOWN}: ${summary}"
}

# Dado un listado (uno por linea) de nombres de componente distintos, chequea
# en el grafo real de Neo4j que todos tengan repo_url seteado y que sea el
# MISMO para todos. Fail-safe a proposito: sin dato, o con datos distintos,
# nunca asume que es un solo repo -- deja EPIC_REJECT_REASON con el motivo y
# retorna 1. Si todos coinciden, deja EPIC_REPO_URL seteado y retorna 0.
check_epic_single_repo() {
  local component_names="$1"
  local quoted_list query rows expected_count found_count
  quoted_list=$(echo "${component_names}" | sed "s/.*/'&'/" | paste -sd, -)

  query="MATCH (n:Service) WHERE n.name IN [${quoted_list}] RETURN n.name + '|' + coalesce(n.repo_url, '') AS row"
  rows=$(cypher_query "${query}" 2>/dev/null | tail -n +2 | tr -d '"')

  expected_count=$(echo "${component_names}" | grep -c . || true)
  found_count=$(echo "${rows}" | grep -c . || true)
  if [ -z "${rows}" ] || [ "${found_count}" -ne "${expected_count}" ]; then
    EPIC_REJECT_REASON="No se pudieron resolver todos los componentes de la epica en el grafo Neo4j (esperados: ${expected_count}, encontrados: ${found_count:-0}). Componentes: $(echo "${component_names}" | tr '\n' ' ')"
    return 1
  fi

  local line name repo_url missing_list="" repo_url_list=""
  while IFS= read -r line; do
    [ -z "${line}" ] && continue
    name=$(echo "${line}" | cut -d'|' -f1)
    repo_url=$(echo "${line}" | cut -d'|' -f2-)
    if [ -z "${repo_url}" ]; then
      missing_list="${missing_list}${name}, "
    fi
    repo_url_list="${repo_url_list}${repo_url}
"
  done <<< "${rows}"

  if [ -n "${missing_list}" ]; then
    EPIC_REJECT_REASON="Estos componentes no tienen repo_url seteado en el grafo, asi que no se puede confirmar si viven en el mismo repo: ${missing_list%, }. Agregaselo (ver prompts/sync_graph_from_azure_devops.md) antes de reintentar."
    return 1
  fi

  local distinct_count
  distinct_count=$(echo "${repo_url_list}" | sort -u | grep -c . || true)
  if [ "${distinct_count}" -ne 1 ]; then
    EPIC_REJECT_REASON="Los componentes de esta epica viven en repos distintos segun el grafo: $(echo "${repo_url_list}" | sort -u | tr '\n' ' '). No se puede procesar una epica que toca mas de un repo en una sola corrida."
    return 1
  fi

  EPIC_REPO_URL=$(echo "${repo_url_list}" | sort -u | grep -v '^$' | head -n1)
  return 0
}

# Etapas 1-4 para el modo --epic: trae la epica + TODOS sus hijos, confirma
# que viven en un solo repo (o se niega, ver check_epic_single_repo), y arma
# UN prompt combinado con el contexto de la epica y cada historia hija. A
# partir de ahi se une al mismo flujo que un ticket normal
# (run_pipeline_delivery), sin duplicar coding agent/testing agent/juez.
run_epic_etapas() {
  echo "=================================================================="
  echo " MODO EPICA — Lectura de ${EPIC_KEY} y sus hijos"
  echo "=================================================================="

  local epic_json child_count unresolved distinct_components
  epic_json=$(python3 "${SCRIPT_DIR}/jira_client.py" fetch-epic "${EPIC_KEY}") \
    || fail "no se pudo leer la epica ${EPIC_KEY} de Jira. Revisa que la key exista y JIRA_EPIC_LINK_JQL si tu proyecto es company-managed."

  TICKET_ID="${EPIC_KEY}"
  SUMMARY=$(echo "${epic_json}" | jq -r '.epic.summary')
  local epic_description
  epic_description=$(echo "${epic_json}" | jq -r '.epic.description')

  child_count=$(echo "${epic_json}" | jq '.children | length')
  echo "Hijos encontrados: ${child_count}"
  if [ "${child_count}" -eq 0 ]; then
    fail "la epica ${EPIC_KEY} no tiene hijos segun el JQL configurado (JIRA_EPIC_LINK_JQL). Si tu proyecto Jira es 'company-managed', probablemente necesites el campo custom 'Epic Link' en vez de 'parent' -- ver README."
  fi

  CHILD_TICKET_KEYS_JSON=$(echo "${epic_json}" | jq '[.children[].ticket_id]')

  unresolved=$(echo "${epic_json}" | jq -r '[.children[] | select(.repository_origen == null) | .ticket_id] | join(", ")')
  if [ -n "${unresolved}" ]; then
    EPIC_REJECT_REASON="Estos hijos de ${EPIC_KEY} no tienen un componente resuelto (el campo Components/label no matchea ningun nodo conocido del grafo): ${unresolved}."
    post_jira_comment "🚫 Modo epica (automatizado): no se pudo procesar ${EPIC_KEY} — ${EPIC_REJECT_REASON}"
    fail "${EPIC_REJECT_REASON}"
  fi

  distinct_components=$(echo "${epic_json}" | jq -r '[.children[].repository_origen] | unique | .[]')

  if ! check_epic_single_repo "${distinct_components}"; then
    post_jira_comment "🚫 Modo epica (automatizado): no se pudo procesar ${EPIC_KEY} — ${EPIC_REJECT_REASON}"
    fail "${EPIC_REJECT_REASON}"
  fi
  echo "Todos los componentes de ${EPIC_KEY} confirmados en el mismo repo: ${EPIC_REPO_URL}"

  local origin_url
  origin_url=$(git -C "${TARGET_REPO_DIR}" remote get-url origin 2>/dev/null || echo "")
  if [ -n "${origin_url}" ] && [ "${origin_url}" != "${EPIC_REPO_URL}" ]; then
    echo "⚠️  El remote 'origin' de ${TARGET_REPO_DIR} (${origin_url}) no coincide exactamente con repo_url del grafo (${EPIC_REPO_URL}) — puede ser solo ssh vs https, pero confirma que estas parado en el repo correcto para esta epica."
  fi

  REPO_ORIGEN=$(echo "${distinct_components}" | paste -sd, -)

  echo
  echo "Consultando grafo/Sonar reales por cada componente de la epica..."
  GRAPH_RESULT=""
  SONAR_ISSUES=""
  SONAR_ERRORS_ARRAY="[]"
  local component g s
  while IFS= read -r component; do
    [ -z "${component}" ] && continue
    g=$(cypher_query "MATCH (origin {name: '${component}'})<-[:DEPENDS_ON]-(dependent) RETURN dependent.name AS servicio, dependent.language AS lenguaje" 2>/dev/null)
    GRAPH_RESULT="${GRAPH_RESULT}
--- ${component} ---
${g}"
    s=$(python3 "${SCRIPT_DIR}/sonar_client.py" "${component}" 2>/dev/null)
    if [ -n "${s}" ]; then
      SONAR_ISSUES="${SONAR_ISSUES}
--- ${component} ---
$(echo "${s}" | jq -r '.issues[] | "- [\(.severity)] \(.rule): \(.message) (linea \(.line))"')"
      SONAR_ERRORS_ARRAY=$(jq -s 'add' <(echo "${SONAR_ERRORS_ARRAY}") <(echo "${s}" | jq '[.issues[].message]'))
    fi
  done <<< "${distinct_components}"

  # Planificador de epicas (epic_planner.py, best-effort): reordena las
  # historias hijas por dependencia real en vez del orden mecanico que
  # devolvio el JQL, y suma notas de coordinacion si el modelo encuentra
  # algo relevante. Si no hay backend disponible o falla, cae al orden
  # mecanico de siempre -- nunca bloquea el modo epica por esto.
  local children_text coordination_notes="" planner_payload planner_result ordered_children_json
  children_text=$(echo "${epic_json}" | jq -r '.children[] | "- \(.ticket_id) (\(.repository_origen)): \(.summary)\n  \(.description)"')

  planner_payload=$(echo "${epic_json}" | jq '{epic: .epic, children: .children}')
  if planner_result=$(echo "${planner_payload}" | python3 "${SCRIPT_DIR}/epic_planner.py" 2>/dev/null) \
    && ordered_children_json=$(echo "${planner_result}" | jq -e '.ordered_children' 2>/dev/null); then
    coordination_notes=$(echo "${planner_result}" | jq -r '.coordination_notes // ""')
    children_text=$(jq -n --argjson children "$(echo "${epic_json}" | jq '.children')" --argjson order "${ordered_children_json}" -r '
      ($children | map({(.ticket_id): .}) | add) as $by_id |
      $order | map($by_id[.]) | map("- \(.ticket_id) (\(.repository_origen)): \(.summary)\n  \(.description)") | join("\n")
    ')
    echo "Planificador de epicas: orden real aplicado ($(echo "${ordered_children_json}" | jq -r 'join(", ")'))."
  else
    echo "Planificador de epicas no disponible o sin resultado valido -- se usa el orden mecanico original."
  fi

  PROMPT=$(cat <<EOF
ESTO ES UNA EPICA con ${child_count} historias hijas. Resolvelas todas juntas, coordinando los cambios entre los componentes que toca cada una.

Epica ${EPIC_KEY}: ${SUMMARY}
${epic_description}

--- Historias hijas (orden sugerido por el planificador de epicas) ---
${children_text}
$([ -n "${coordination_notes}" ] && echo "--- Notas de coordinacion del planificador ---
${coordination_notes}")
--- Grafo de impacto (por componente) ---
${GRAPH_RESULT}
--- Hallazgos Sonar (reales, por componente) ---
${SONAR_ISSUES}
EOF
)

  JIRA_CONTEXT=$(jq -n --arg id "${EPIC_KEY}" --arg summary "${SUMMARY}" --arg desc "${epic_description}" --arg repo "${REPO_ORIGEN}" \
    '{ticket_id: $id, summary: $summary, description: $desc, repository_origen: $repo}')

  echo "Prompt combinado armado para ${EPIC_KEY}: ${child_count} hijos, componentes $(echo "${distinct_components}" | tr '\n' ' ')."

  echo "${epic_json}" | jq -r '.children[].ticket_id' | while IFS= read -r child_key; do
    [ -z "${child_key}" ] && continue
    JIRA_TICKET_KEY="${child_key}" python3 "${SCRIPT_DIR}/jira_client.py" comment \
      "🧩 Modo epica (automatizado): esta historia se proceso como parte de una corrida combinada de la epica ${EPIC_KEY}. Revisa el resultado en ${EPIC_KEY}." \
      >/dev/null 2>&1 || true
  done
}

# Etapas 5 en adelante: composicion del prompt final al firewall, camino A/B
# del coding agent, testing agent, juez y correlacion de Falco. Comun tanto
# al modo ticket normal como al modo epica -- ambos llegan aca habiendo
# seteado PROMPT/JIRA_CONTEXT/SONAR_ERRORS_ARRAY/TICKET_ID/SUMMARY/REPO_ORIGEN
# de la forma que corresponda.
run_pipeline_delivery() {
  echo
  echo "=================================================================="
  echo " ETAPA 4/5 — Envio al AI Firewall"
  echo "=================================================================="

  local payload
  payload=$(jq -n \
    --arg prompt "${PROMPT}" \
    --argjson jira_context "${JIRA_CONTEXT}" \
    --argjson sonar_errors "${SONAR_ERRORS_ARRAY}" \
    '{prompt: $prompt, jira_context: $jira_context, sonar_errors: $sonar_errors}')

  local firewall_auth_header=()
  if [ -n "${FIREWALL_API_KEY:-}" ]; then
    firewall_auth_header=(-H "X-Firewall-Key: ${FIREWALL_API_KEY}")
  fi

  local response http_code body
  # curl solo devuelve exit != 0 si no pudo hablar con el firewall (host
  # caido, timeout, conexion rechazada) -- un 401/403 real sigue siendo
  # exit 0 con body, asi que retry_cmd solo reintenta fallos de red, nunca
  # una decision legitima del firewall.
  _call_firewall() {
    response=$(curl -s -w '\n%{http_code}' -X POST "${FIREWALL_URL}/evaluate" \
      -H "Content-Type: application/json" \
      "${firewall_auth_header[@]}" \
      -d "${payload}")
  }
  retry_cmd _call_firewall || fail "no se pudo contactar al AI Firewall en ${FIREWALL_URL} tras reintentar. Revisa que 'docker compose up' este corriendo."

  http_code=$(echo "${response}" | tail -n1)
  body=$(echo "${response}" | sed '$d')

  STATUS=$(echo "${body}" | jq -r '.status')

  echo "HTTP ${http_code} — status: ${STATUS}"

  if [ "${STATUS}" = "REJECTED" ]; then
    REASON=$(echo "${body}" | jq -r '.reason')
    echo
    echo "🛑 EL AI FIREWALL RECHAZO LA PETICION"
    echo "Razon: ${REASON}"
    echo "gh copilot NO fue invocado."
    post_jira_comment "🛡️ AI Firewall (automatizado): solicitud RECHAZADA. Motivo: ${REASON}. gh copilot no fue invocado."
    log_contribution "REJECTED" 0 false false ""
    AGENT_BACKEND=""
    run_judge "firewall_rejected" "${REASON}" "solicitud rechazada por el firewall"
    exit 1
  fi

  if [ "${STATUS}" = "APPROVED" ]; then
    SANITIZED=$(echo "${body}" | jq -r '.sanitized_prompt')
    REDACTIONS=$(echo "${body}" | jq -r '.redactions_applied')

    echo
    echo "✅ APROBADO — redacciones aplicadas: ${REDACTIONS}"
    echo
    echo "--- PROMPT ORIGINAL --------------------------------------------"
    echo "${PROMPT}"
    echo "--- PROMPT SANEADO (lo que recibe el agente) --------------------"
    echo "${SANITIZED}"
    echo "-------------------------------------------------------------"

    echo
    echo "Moviendo el ticket ${TICKET_ID} a '${JIRA_IN_PROGRESS_STATUS:-In Progress}'..."
    transition_jira_ticket

    # scripts/smoke_test.sh setea esto para validar las etapas 1-4 (Jira real,
    # grafo real, Sonar real, firewall real) sin tocar la etapa 5: gh copilot
    # suggest es una TUI interactiva (no automatizable sin volverse fragil) y
    # el coding agent en la nube abre el PR async — ninguno de los dos entra
    # en un smoke test corto y automatico. Sin esta variable, cero cambios.
    if [ "${SMOKE_TEST_MODE:-false}" = "true" ]; then
      echo
      echo "SMOKE_TEST_MODE=true — deteniendo aca a proposito (etapas 1-4 validadas)."
      echo "La etapa 5 (coding agent) requiere confirmacion interactiva de 'gh copilot suggest' o un repo GitHub real — fuera del alcance de un smoke test automatizado."
      post_jira_comment "🧪 Smoke test (automatizado): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Etapas 1-4 validadas de punta a punta; se detiene antes de la etapa 5 por SMOKE_TEST_MODE."
      exit 0
    fi

    local falco_since_ts
    falco_since_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    # --- Camino A: agente real. Si hay un repo real configurado, se crea un
    # Issue con el contexto ya armado (ticket + impacto de grafo + hallazgos
    # Sonar, todo pre-computado localmente) y se asigna al GitHub Copilot
    # coding agent, que corre en la nube de GitHub con su propio loop de
    # razonamiento y herramientas — no una sugerencia de un solo tiro.
    if [ -n "${GITHUB_REPO:-}" ]; then
      echo
      echo "=================================================================="
      echo " ETAPA 5/5 — Asignando el ticket a GitHub Copilot coding agent (${GITHUB_REPO})"
      echo "=================================================================="

      local issue_body issue_url
      issue_body=$(cat <<EOF
${SANITIZED}

---
Generado automaticamente por poc-ai-agents desde el ticket Jira ${TICKET_ID}.
EOF
)

      issue_url=$(gh issue create --repo "${GITHUB_REPO}" --title "${SUMMARY}" --body "${issue_body}") \
        || fail "no se pudo crear el issue en ${GITHUB_REPO}. Revisa que el repo exista y tengas permisos."

      echo "Issue creado: ${issue_url}"

      if gh issue edit "${issue_url}" --add-assignee "${GITHUB_COPILOT_ASSIGNEE:-copilot-swe-agent}" >/dev/null 2>&1; then
        echo "Asignado a ${GITHUB_COPILOT_ASSIGNEE:-copilot-swe-agent}. El agente trabaja de forma asincronica en la nube y va a abrir un PR."
        post_jira_comment "🤖 GitHub Copilot coding agent (automatizado): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Se creo y asigno ${issue_url} — el agente trabaja en la nube y va a abrir un PR."
        log_contribution "APPROVED" "${REDACTIONS}" true true "issue:${issue_url}"
        AGENT_BACKEND="cloud"
        # El PR todavia no existe (el coding agent trabaja async): el juez solo
        # puede evaluar la decision del firewall + el planteo del issue por ahora.
        run_judge "issue_only" "${issue_body}" "issue ${issue_url}" "" "${issue_url}"
      else
        echo "No se pudo asignar a '${GITHUB_COPILOT_ASSIGNEE:-copilot-swe-agent}'. Verifica que el coding agent este habilitado en ${GITHUB_REPO} y que el login del assignee sea correcto."
        post_jira_comment "🤖 GitHub Copilot coding agent (automatizado): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Se creo ${issue_url} pero no se pudo asignar al coding agent — revisar configuracion del repo."
        log_contribution "APPROVED" "${REDACTIONS}" true false "issue:${issue_url}"
        record_run_in_graph "" "cloud" "SKIPPED" "" "SKIPPED" "no se pudo asignar el issue al coding agent" ""
      fi
      check_falco_correlation "${falco_since_ts}"
      exit 0
    fi

    # --- Camino B: sin repo real configurado. B1 (agente real local, con
    # loop de razonamiento y confirmacion antes de aplicar) si hay backend
    # de modelo disponible; si no, B2 (gh copilot suggest, sugerencia de un
    # solo tiro, sin loop) como fallback — ver PLAN.md.
    echo
    echo "=================================================================="
    echo " ETAPA 5/5 — Coding agent local (sin GITHUB_REPO)"
    echo "=================================================================="

    local applied=false branch base_branch diff_text tests_gate_result
    base_branch=$(git -C "${TARGET_REPO_DIR}" rev-parse --abbrev-ref HEAD)
    branch="copilot/${TICKET_ID}-$(date +%s)"
    git -C "${TARGET_REPO_DIR}" checkout -b "${branch}" >/dev/null

    local backend_available=false
    if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
      backend_available=true
    elif curl -sf "${OLLAMA_URL:-http://localhost:11434}/api/tags" >/dev/null 2>&1; then
      backend_available=true
    fi

    if [ "${backend_available}" = "true" ]; then
      # --- Camino B1: agente real local (coding_agent.py). Razona en varios
      # pasos, puede leer/escribir/listar/grep el repo y consultar los MCP
      # que ya tiene el juez (Neo4j/Qdrant) — cada escritura/comando pide
      # confirmacion antes de aplicarse, se ve y se responde en la terminal.
      echo "Backend de modelo disponible — usando el agente de codigo local real (coding_agent.py)."
      local payload_file agent_result_json
      payload_file=$(mktemp)
      jq -n --arg ticket_id "${TICKET_ID}" --arg sanitized "${SANITIZED}" --arg repo "${TARGET_REPO_DIR}" \
        '{ticket_id: $ticket_id, sanitized_prompt: $sanitized, target_repo_dir: $repo}' > "${payload_file}"

      agent_result_json=$(python3 "${SCRIPT_DIR}/coding_agent.py" "${payload_file}")
      rm -f "${payload_file}"
      echo "Resultado del agente: $(echo "${agent_result_json}" | jq -r '.status') — $(echo "${agent_result_json}" | jq -r '.summary')"
      AGENT_BACKEND=$(echo "${agent_result_json}" | jq -r '._meta.backend // "unknown"')
      # Path al archivo temporal con la conversacion completa (mensajes +
      # estado de investigacion/verificacion) -- retry_coding_agent_with_feedback()
      # lo usa para continuar la conversacion en vez de re-investigar desde
      # cero si el juez pide un segundo intento. "" si el agente no lo genero
      # (ej. sin backend disponible).
      CONVERSATION_FILE=$(echo "${agent_result_json}" | jq -r '._conversation_file // ""')
    else
      CONVERSATION_FILE=""
      # --- Camino B2 (fallback): gh copilot suggest, sugerencia de un solo
      # tiro sin loop ni acceso a los MCP — ver PLAN.md.
      echo "Sin ANTHROPIC_API_KEY ni Ollama alcanzable — usando gh copilot suggest como fallback (sugerencia puntual, no un agente)."
      echo "Copilot va a sugerir un comando para resolver ${TICKET_ID} en ${TARGET_REPO_DIR}. Se te pedira confirmar antes de ejecutar nada."
      gh copilot suggest -t shell "${SANITIZED}"
      AGENT_BACKEND="gh_copilot_suggest"
    fi

    if [ -n "$(git -C "${TARGET_REPO_DIR}" status --porcelain)" ]; then
      git -C "${TARGET_REPO_DIR}" add -A
      git -C "${TARGET_REPO_DIR}" commit -m "Copilot suggestion for ${TICKET_ID}" >/dev/null
      applied=true
      echo "Cambio aplicado y commiteado en la rama '${branch}' de ${TARGET_REPO_DIR} — NO en '${base_branch}'."
      echo "Revisalo con: git -C \"${TARGET_REPO_DIR}\" diff ${base_branch}..${branch}"
      post_jira_comment "🤖 Copilot (automatizado, fallback local): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Copilot aplico un cambio en la rama '${branch}' de tu repo, pendiente de revision humana antes de mergear."
      diff_text=$(git -C "${TARGET_REPO_DIR}" diff "${base_branch}..${branch}")

      if run_output_guard "${diff_text}" "${branch}"; then
        run_tests_gate "${REPO_ORIGEN}" "${branch}"
        tests_gate_result=$?
        log_contribution "APPROVED" "${REDACTIONS}" true "${applied}" "${branch}" "${TEST_PASSED}"
        if [ "${tests_gate_result}" -eq 0 ]; then
          run_judge "local_diff" "${diff_text}" "rama '${branch}' de ${TARGET_REPO_DIR}" "${branch}" "" "${TEST_OUTPUT}"
        fi
      else
        log_contribution "APPROVED" "${REDACTIONS}" true "${applied}" "${branch}" "null"
      fi
    else
      git -C "${TARGET_REPO_DIR}" checkout "${base_branch}" >/dev/null 2>&1
      git -C "${TARGET_REPO_DIR}" branch -D "${branch}" >/dev/null 2>&1
      echo "No hubo cambios que aplicar (Copilot no ejecuto ningun comando, o el comando no modifico archivos)."
      post_jira_comment "🤖 Copilot (automatizado, fallback local): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Copilot no aplico ningun cambio en esta corrida."
      branch=""
      log_contribution "APPROVED" "${REDACTIONS}" true "${applied}" "${branch}" "null"
      run_judge "issue_only" "${SANITIZED}" "sin cambios aplicados"
    fi
    check_falco_correlation "${falco_since_ts}"
    exit 0
  fi

  fail "respuesta inesperada del firewall: ${body}"
}

command -v jq >/dev/null 2>&1 || fail "jq no esta instalado (requerido para parsear JSON). Instalalo y reintenta."
command -v python3 >/dev/null 2>&1 || fail "python3 no esta instalado."

# Deriva JIRA_KNOWN_COMPONENTS de los nombres de nodo que ya existen en el
# grafo real de Neo4j, en vez de depender de que la lista estatica de .env
# se mantenga sincronizada a mano. Best-effort: si Neo4j no esta disponible
# o el grafo esta vacio, se deja lo que ya haya en .env sin tocar nada.
if command -v cypher-shell >/dev/null 2>&1; then
  DISCOVERED_COMPONENTS=$(cypher_query "MATCH (n:Service) RETURN DISTINCT n.name AS name" 2>/dev/null | tail -n +2 | tr -d '"' | paste -sd, -)
  if [ -n "${DISCOVERED_COMPONENTS}" ]; then
    export JIRA_KNOWN_COMPONENTS="${DISCOVERED_COMPONENTS}"
    echo "Componentes conocidos derivados del grafo Neo4j: ${JIRA_KNOWN_COMPONENTS}"
  fi
fi

if [ "${EPIC_MODE}" = "true" ]; then
  run_epic_etapas
  run_pipeline_delivery
  exit 0
fi

echo "=================================================================="
echo " ETAPA 1/5 — Lectura del ticket Jira real"
echo "=================================================================="
JIRA_JSON="$(python3 "${SCRIPT_DIR}/jira_client.py")" || fail "no se pudo leer el ticket de Jira. Revisa .env y check_prereqs.sh"

TICKET_ID=$(echo "${JIRA_JSON}" | jq -r '.ticket_id')
SUMMARY=$(echo "${JIRA_JSON}" | jq -r '.summary')
DESCRIPTION=$(echo "${JIRA_JSON}" | jq -r '.description')
REPO_ORIGEN=$(echo "${JIRA_JSON}" | jq -r '.repository_origen')
CACHE_HIT=$(echo "${JIRA_JSON}" | jq -r '._cache.hit')
HAS_ATTACHMENTS=$(echo "${JIRA_JSON}" | jq -r '.has_attachments')
ATTACHMENT_CONTEXT=$(echo "${JIRA_JSON}" | jq -r '.attachment_context')

echo "Ticket:              ${TICKET_ID}"
echo "Summary:              ${SUMMARY}"
echo "Componente afectado:  ${REPO_ORIGEN}"
echo "(servido desde cache: ${CACHE_HIT})"

if [ "${HAS_ATTACHMENTS}" = "true" ]; then
  echo
  echo "Adjuntos detectados en el ticket:"
  echo "${ATTACHMENT_CONTEXT}"
  if echo "${ATTACHMENT_CONTEXT}" | grep -q "Requiere revision humana"; then
    post_jira_comment "🛑 Pipeline (automatizado): el ticket tiene adjuntos sin descripcion de Rovo todavia. Se detuvo antes de llegar al firewall/agente — requiere revision humana."
    fail "el ticket tiene adjuntos pero Rovo aun no genero una descripcion en los comentarios. Esperá a que Rovo la genere, o revisalo a mano antes de reintentar."
  fi
fi

# La deteccion del bug sigue siendo 100% humana (nadie monitorea logs de los
# microservicios por su cuenta) — esto es un chequeo estructural, no un
# heuristico de palabras: Jira marca cualquier "insert code" del editor como
# un nodo codeBlock explicito en el ADF, asi que has_log_evidence es
# deterministico (si el reportante pego el log como bloque de codigo, se
# detecta siempre; si lo escribio como texto plano, no cuenta como evidencia
# y se le pide que lo reformatee como bloque de codigo).
HAS_LOG_EVIDENCE=$(echo "${JIRA_JSON}" | jq -r '.has_log_evidence')
if [ "${HAS_LOG_EVIDENCE}" != "true" ]; then
  echo
  echo "AVISO: la descripcion del ticket no trae un bloque de codigo con logs/stack trace."
  post_jira_comment "📋 Pipeline (automatizado): para diagnosticar este bug en '${REPO_ORIGEN}' con precision, pega el log o stack trace real del servicio como bloque de codigo (no como texto plano) en la descripcion."
fi

echo
echo "=================================================================="
echo " ETAPA 2/5 — Consulta real de impacto en el grafo Neo4j"
echo "=================================================================="
GRAPH_RESULT=$(cypher_query "MATCH (origin {name: '${REPO_ORIGEN}'})<-[:DEPENDS_ON]-(dependent) RETURN dependent.name AS servicio, dependent.language AS lenguaje") \
  || fail "no se pudo consultar Neo4j tras reintentar. Revisa que 'docker compose up' este corriendo."

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
echo " ETAPA 3.5/5 — Specs reales de Figma (si el ticket trae un link)"
echo "=================================================================="
FIGMA_LINK=$(echo "${JIRA_JSON}" | jq -c '.figma_link // empty')
FIGMA_SPECS_TEXT=""
if [ -n "${FIGMA_LINK}" ]; then
  if [ -z "${FIGMA_API_TOKEN:-}" ]; then
    echo "El ticket trae un link de Figma pero falta FIGMA_API_TOKEN — se omite esa seccion del prompt."
  else
    FIGMA_FILE_KEY=$(echo "${FIGMA_LINK}" | jq -r '.file_key')
    FIGMA_NODE_ID=$(echo "${FIGMA_LINK}" | jq -r '.node_id')
    if FIGMA_JSON=$(python3 "${SCRIPT_DIR}/figma_client.py" "${FIGMA_FILE_KEY}" "${FIGMA_NODE_ID}" 2>/dev/null); then
      if [ "$(echo "${FIGMA_JSON}" | jq -r '.found')" = "true" ]; then
        FIGMA_SPECS_TEXT=$(echo "${FIGMA_JSON}" | jq '.summary')
        echo "Specs de Figma obtenidas para el nodo ${FIGMA_NODE_ID} del archivo ${FIGMA_FILE_KEY}."
      else
        echo "El nodo ${FIGMA_NODE_ID} no se encontro en el archivo ${FIGMA_FILE_KEY} de Figma — se omite esa seccion del prompt."
      fi
    else
      echo "No se pudo consultar Figma (revisa FIGMA_API_TOKEN o conectividad) — se omite esa seccion del prompt."
    fi
  fi
else
  echo "El ticket no trae un link de Figma — se omite esta etapa."
fi

PROMPT=$(cat <<EOF
${SUMMARY}
${DESCRIPTION}
--- Adjuntos (descritos por Rovo) ---
${ATTACHMENT_CONTEXT}
--- Grafo de impacto ---
${GRAPH_RESULT}
--- Hallazgos Sonar (reales) ---
${SONAR_ISSUES}
EOF
)
if [ -n "${FIGMA_SPECS_TEXT}" ]; then
  PROMPT="${PROMPT}
--- Specs reales de Figma ---
${FIGMA_SPECS_TEXT}"
fi

JIRA_CONTEXT=$(echo "${JIRA_JSON}" | jq '{ticket_id, summary, description, repository_origen}')
SONAR_ERRORS_ARRAY=$(echo "${SONAR_JSON}" | jq '[.issues[].message]')

run_pipeline_delivery
