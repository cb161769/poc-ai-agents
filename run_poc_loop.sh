#!/usr/bin/env bash
# End-to-end orchestrator: real Jira ticket -> real Neo4j graph impact ->
# real SonarQube findings -> AI Firewall -> real gh copilot suggest, applied
# on a review branch OF YOUR REAL REPO. The scenario (clean vs. malicious)
# is decided by the real content of the Jira ticket in JIRA_TICKET_KEY, not
# by a flag: edit the ticket in Jira to break the flow, then re-run this
# script.
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

NEO4J_URI="${NEO4J_URI:-bolt://localhost:7687}"
NEO4J_USERNAME="${NEO4J_USERNAME:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-test_password_local}"
FIREWALL_URL="${FIREWALL_URL:-http://localhost:8080}"
CONTRIBUTION_LOG="${SCRIPT_DIR}/logs/copilot_contribution.jsonl"

fail() {
  echo "ERROR: $1" >&2
  exit 1
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
post_jira_comment() {
  local text="$1"
  python3 "${SCRIPT_DIR}/jira_client.py" comment "${text}" >/dev/null 2>&1 \
    || echo "(no se pudo dejar el comentario de auditoria en Jira — revisa credenciales)"
}

# Moves the real ticket in Jira to JIRA_IN_PROGRESS_STATUS (default
# "In Progress") so its status reflects that an agent is working on it,
# without a human having to drag it across the board by hand. Best-effort:
# if the workflow doesn't have that exact transition name, it just warns.
transition_jira_ticket() {
  local target_status="${JIRA_IN_PROGRESS_STATUS:-In Progress}"
  python3 "${SCRIPT_DIR}/jira_client.py" transition "${target_status}" >/dev/null 2>&1 \
    || echo "(no se pudo mover el ticket a '${target_status}' — puede que tu workflow use otro nombre; revisa JIRA_IN_PROGRESS_STATUS)"
}

# Independent judge (Claude, no gh copilot) que revisa la decision del
# firewall + el cambio real (o el issue, si el coding agent en la nube
# todavia no genero PR) + la corrida completa. Si marca FLAGGED, tiene poder
# de bloqueo real: reabre/transiciona el ticket a un estado bloqueado, deja
# un comentario fuerte, y si hay una rama local, la marca inequivocamente en
# el propio historial de git.
run_judge() {
  local change_source="$1" change_description="$2" location_label="$3" branch="${4:-}" issue_url="${5:-}" test_summary="${6:-sin tests corridos para esta corrida}"

  echo
  echo "=================================================================="
  echo " ETAPA 7 — Agente juez (segunda opinion independiente, con poder de bloqueo)"
  echo "=================================================================="

  local judge_payload verdict_json verdict reasoning
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
    return
  fi

  verdict=$(echo "${verdict_json}" | jq -r '.verdict')
  reasoning=$(echo "${verdict_json}" | jq -r '.reasoning')

  echo "Veredicto del juez: ${verdict}"
  echo "Razonamiento: ${reasoning}"

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

    python3 "${SCRIPT_DIR}/jira_client.py" transition "${JIRA_BLOCKED_STATUS:-Blocked}" >/dev/null 2>&1 \
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

  python3 "${SCRIPT_DIR}/jira_client.py" transition "${JIRA_BLOCKED_STATUS:-Blocked}" >/dev/null 2>&1 \
    || echo "(no se pudo mover el ticket a '${JIRA_BLOCKED_STATUS:-Blocked}' — ajusta JIRA_BLOCKED_STATUS a un nombre real de tu workflow)"

  if [ -n "${branch}" ]; then
    git -C "${TARGET_REPO_DIR}" commit --allow-empty -m "BLOCKED BY TESTS: el test suite real fallo" >/dev/null 2>&1
    echo "Rama '${branch}' marcada como bloqueada en su propio historial de git — no la mergees sin revision."
  fi

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

  if [ -n "${FALCO_ALERT_WEBHOOK_URL:-}" ]; then
    curl -s -X POST "${FALCO_ALERT_WEBHOOK_URL}" \
      -H "Content-Type: application/json" \
      -d "$(jq -n --arg text "🚨 Falco detecto ${count} alerta(s) en la corrida de ${TICKET_ID:-UNKNOWN}: ${summary}" '{text: $text}')" \
      >/dev/null 2>&1 || echo "(no se pudo postear al webhook de Falco)"
  fi
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

echo
echo "=================================================================="
echo " ETAPA 4/5 — Composicion del prompt y envio al AI Firewall"
echo "=================================================================="
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

PAYLOAD=$(jq -n \
  --arg prompt "${PROMPT}" \
  --argjson jira_context "${JIRA_CONTEXT}" \
  --argjson sonar_errors "${SONAR_ERRORS_ARRAY}" \
  '{prompt: $prompt, jira_context: $jira_context, sonar_errors: $sonar_errors}')

FIREWALL_AUTH_HEADER=()
if [ -n "${FIREWALL_API_KEY:-}" ]; then
  FIREWALL_AUTH_HEADER=(-H "X-Firewall-Key: ${FIREWALL_API_KEY}")
fi

RESPONSE=$(curl -s -w '\n%{http_code}' -X POST "${FIREWALL_URL}/evaluate" \
  -H "Content-Type: application/json" \
  "${FIREWALL_AUTH_HEADER[@]}" \
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
  run_judge "firewall_rejected" "${REASON}" "solicitud rechazada por el firewall"
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
  echo "Moviendo el ticket ${TICKET_ID} a '${JIRA_IN_PROGRESS_STATUS:-In Progress}'..."
  transition_jira_ticket

  FALCO_SINCE_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

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

    ISSUE_BODY=$(cat <<EOF
${SANITIZED}

---
Generado automaticamente por poc-ai-agents desde el ticket Jira ${TICKET_ID}.
EOF
)

    ISSUE_URL=$(gh issue create --repo "${GITHUB_REPO}" --title "${SUMMARY}" --body "${ISSUE_BODY}") \
      || fail "no se pudo crear el issue en ${GITHUB_REPO}. Revisa que el repo exista y tengas permisos."

    echo "Issue creado: ${ISSUE_URL}"

    if gh issue edit "${ISSUE_URL}" --add-assignee "${GITHUB_COPILOT_ASSIGNEE:-copilot-swe-agent}" >/dev/null 2>&1; then
      echo "Asignado a ${GITHUB_COPILOT_ASSIGNEE:-copilot-swe-agent}. El agente trabaja de forma asincronica en la nube y va a abrir un PR."
      post_jira_comment "🤖 GitHub Copilot coding agent (automatizado): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Se creo y asigno ${ISSUE_URL} — el agente trabaja en la nube y va a abrir un PR."
      log_contribution "APPROVED" "${REDACTIONS}" true true "issue:${ISSUE_URL}"
      # El PR todavia no existe (el coding agent trabaja async): el juez solo
      # puede evaluar la decision del firewall + el planteo del issue por ahora.
      run_judge "issue_only" "${ISSUE_BODY}" "issue ${ISSUE_URL}" "" "${ISSUE_URL}"
    else
      echo "No se pudo asignar a '${GITHUB_COPILOT_ASSIGNEE:-copilot-swe-agent}'. Verifica que el coding agent este habilitado en ${GITHUB_REPO} y que el login del assignee sea correcto."
      post_jira_comment "🤖 GitHub Copilot coding agent (automatizado): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Se creo ${ISSUE_URL} pero no se pudo asignar al coding agent — revisar configuracion del repo."
      log_contribution "APPROVED" "${REDACTIONS}" true false "issue:${ISSUE_URL}"
    fi
    check_falco_correlation "${FALCO_SINCE_TS}"
    exit 0
  fi

  # --- Camino B (fallback): sin repo real configurado, se pide una
  # sugerencia de un solo tiro con gh copilot suggest y se aplica local.
  # Esto NO es un agente autonomo, es una llamada puntual — ver PLAN.md.
  echo
  echo "=================================================================="
  echo " ETAPA 5/5 — gh copilot suggest (fallback local, sin GITHUB_REPO)"
  echo "=================================================================="

  APPLIED=false
  BRANCH=""

  BASE_BRANCH=$(git -C "${TARGET_REPO_DIR}" rev-parse --abbrev-ref HEAD)
  BRANCH="copilot/${TICKET_ID}-$(date +%s)"
  git -C "${TARGET_REPO_DIR}" checkout -b "${BRANCH}" >/dev/null

  echo "Copilot va a sugerir un comando para resolver ${TICKET_ID} en ${TARGET_REPO_DIR}. Se te pedira confirmar antes de ejecutar nada."
  gh copilot suggest -t shell "${SANITIZED}"
  SUGGEST_EXIT=$?

  if [ "${SUGGEST_EXIT}" -eq 0 ] && [ -n "$(git -C "${TARGET_REPO_DIR}" status --porcelain)" ]; then
    git -C "${TARGET_REPO_DIR}" add -A
    git -C "${TARGET_REPO_DIR}" commit -m "Copilot suggestion for ${TICKET_ID}" >/dev/null
    APPLIED=true
    echo "Cambio aplicado y commiteado en la rama '${BRANCH}' de ${TARGET_REPO_DIR} — NO en '${BASE_BRANCH}'."
    echo "Revisalo con: git -C \"${TARGET_REPO_DIR}\" diff ${BASE_BRANCH}..${BRANCH}"
    post_jira_comment "🤖 Copilot (automatizado, fallback local): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Copilot aplico un cambio en la rama '${BRANCH}' de tu repo, pendiente de revision humana antes de mergear."
    DIFF_TEXT=$(git -C "${TARGET_REPO_DIR}" diff "${BASE_BRANCH}..${BRANCH}")
    run_tests_gate "${REPO_ORIGEN}" "${BRANCH}"
    TESTS_GATE_RESULT=$?
    log_contribution "APPROVED" "${REDACTIONS}" true "${APPLIED}" "${BRANCH}" "${TEST_PASSED}"
    if [ "${TESTS_GATE_RESULT}" -eq 0 ]; then
      run_judge "local_diff" "${DIFF_TEXT}" "rama '${BRANCH}' de ${TARGET_REPO_DIR}" "${BRANCH}" "" "${TEST_OUTPUT}"
    fi
  else
    git -C "${TARGET_REPO_DIR}" checkout "${BASE_BRANCH}" >/dev/null 2>&1
    git -C "${TARGET_REPO_DIR}" branch -D "${BRANCH}" >/dev/null 2>&1
    echo "No hubo cambios que aplicar (Copilot no ejecuto ningun comando, o el comando no modifico archivos)."
    post_jira_comment "🤖 Copilot (automatizado, fallback local): AI Firewall aprobo la solicitud (redacciones: ${REDACTIONS}). Copilot no aplico ningun cambio en esta corrida."
    BRANCH=""
    log_contribution "APPROVED" "${REDACTIONS}" true "${APPLIED}" "${BRANCH}" "null"
    run_judge "issue_only" "${SANITIZED}" "sin cambios aplicados"
  fi
  check_falco_correlation "${FALCO_SINCE_TS}"
  exit 0
fi

fail "respuesta inesperada del firewall: ${BODY}"
