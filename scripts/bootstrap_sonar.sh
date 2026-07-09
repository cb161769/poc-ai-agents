#!/bin/sh
# Runs once inside the sonar-scanner container. Rotates the default
# admin/admin credential (SonarQube 10.x requires this before the API works),
# mints an analysis token, persists it for reuse, and scans the three
# sample-repo modules against the real SonarQube server.
set -eu

SONAR_HOST_URL="${SONAR_HOST_URL:-http://sonarqube:9000}"
STATE_DIR="/state"
TOKEN_FILE="${STATE_DIR}/sonar_token"

if [ -f /scripts/.env ]; then
  # shellcheck disable=SC1091
  . /scripts/.env
fi

SONAR_NEW_ADMIN_PASSWORD="${SONAR_NEW_ADMIN_PASSWORD:-LocalPoc_Admin_2026!}"

echo "[bootstrap_sonar] esperando a que SonarQube este UP..."
# sonarsource/sonar-scanner-cli no trae wget (si curl, ya usado mas abajo) --
# con wget este loop fallaba en silencio en cada iteracion y nunca detectaba
# que SonarQube estaba arriba, esperando para siempre.
until curl -sf "${SONAR_HOST_URL}/api/system/status" 2>/dev/null | grep -q '"status":"UP"'; do
  sleep 3
done

if [ ! -f "${TOKEN_FILE}" ]; then
  echo "[bootstrap_sonar] rotando password admin por defecto..."
  # If this is the first run, admin/admin is still active and forces a
  # password change; if it already changed (re-run), this call fails
  # harmlessly and we fall through to token generation with the new password.
  curl -s -u admin:admin -X POST \
    "${SONAR_HOST_URL}/api/users/change_password" \
    --data-urlencode "login=admin" \
    --data-urlencode "previousPassword=admin" \
    --data-urlencode "password=${SONAR_NEW_ADMIN_PASSWORD}" \
    >/dev/null 2>&1 || true

  # Revoca cualquier token viejo con el mismo nombre antes de generar --
  # idempotente ante un re-run parcial (ej. la generacion ya habia
  # funcionado antes pero el guardado a disco fallo, dejando un token
  # huerfano en SonarQube que hace fallar la proxima generacion con el
  # mismo nombre). Best-effort: si no existe, revoke no hace nada.
  curl -s -u "admin:${SONAR_NEW_ADMIN_PASSWORD}" -X POST \
    "${SONAR_HOST_URL}/api/user_tokens/revoke" \
    --data-urlencode "login=admin" \
    --data-urlencode "name=poc-scanner-token" \
    >/dev/null 2>&1 || true

  echo "[bootstrap_sonar] generando token de analisis..."
  TOKEN_JSON=$(curl -s -u "admin:${SONAR_NEW_ADMIN_PASSWORD}" -X POST \
    "${SONAR_HOST_URL}/api/user_tokens/generate" \
    --data-urlencode "name=poc-scanner-token")
  TOKEN=$(echo "${TOKEN_JSON}" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')

  if [ -z "${TOKEN}" ]; then
    echo "[bootstrap_sonar] ERROR: no se pudo generar el token. Respuesta: ${TOKEN_JSON}" >&2
    exit 1
  fi

  echo "${TOKEN}" > "${TOKEN_FILE}"
  echo "[bootstrap_sonar] token generado y guardado en ${TOKEN_FILE}"
else
  TOKEN=$(cat "${TOKEN_FILE}")
  echo "[bootstrap_sonar] reutilizando token existente"
fi

for module in auth-service frontend data-worker; do
  echo "[bootstrap_sonar] escaneando /usr/src/${module}..."
  # sonar.working.directory explicito: /usr/src esta montado :ro, y el
  # scanner por default intenta crear .scannerwork DENTRO del projectBaseDir
  # -- fallaba con "Read-only file system". /tmp si es escribible.
  sonar-scanner \
    -Dproject.settings="/usr/src/${module}/sonar-project.properties" \
    -Dsonar.host.url="${SONAR_HOST_URL}" \
    -Dsonar.token="${TOKEN}" \
    -Dsonar.projectBaseDir="/usr/src/${module}" \
    -Dsonar.working.directory="/tmp/.scannerwork-${module}"
done

echo "[bootstrap_sonar] listo. Consulta http://localhost:9000 (admin / ${SONAR_NEW_ADMIN_PASSWORD})"
echo "[bootstrap_sonar] usa este mismo token como SONAR_TOKEN en tu .env: ${TOKEN}"
