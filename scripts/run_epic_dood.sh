#!/usr/bin/env bash
# Corrida real de --epic <KEY> vía Docker-outside-of-Docker (Dockerfile.testrunner
# + /var/run/docker.sock montado), encapsulando lo que en la operacion de
# esta noche tuvo que reconstruirse a mano a partir de logs viejos: mounts,
# red de docker-compose, traduccion HOST_TARGET_REPO_DIR, pipe de
# confirmaciones, y clonado del repo objetivo con la credencial pasada de
# forma segura (nunca embebida en la URL -- eso git la persiste en texto
# plano en .git/config).
#
# Uso:
#   ./scripts/run_epic_dood.sh <EPIC_KEY> [TARGET_REPO_GIT_URL]
#
# TARGET_REPO_GIT_URL es opcional -- si no se pasa, se usa
# TARGET_REPO_GIT_URL de .env. El clon se reusa entre corridas (mismo
# directorio, ${TARGET_REPO_CLONE_DIR:-${ROOT_DIR}/.dood-target-repo}) --
# si ya existe, se hace fetch + reset --hard al trunk en vez de reclonar.
#
# Variables de entorno relevantes (todas opcionales, ver .env.example):
#   TARGET_REPO_CLONE_DIR   directorio donde vive el clon persistente
#   TRUNK_BRANCH            rama trunk del repo objetivo (default: main)
#   GIT_AUTHOR_NAME/EMAIL   identidad de git para el clon (default: poc-ai-agents/poc@local)
#   NO_AUTO_CONFIRM=1       no pipea 'yes s' -- deja las confirmaciones para responder a mano
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

fail() {
  echo "ERROR: $1" >&2
  exit 1
}

if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${ROOT_DIR}/.env"
  set +a
fi

EPIC_KEY="${1:-}"
[ -n "${EPIC_KEY}" ] || fail "uso: $0 <EPIC_KEY> [TARGET_REPO_GIT_URL]"
REPO_URL="${2:-${TARGET_REPO_GIT_URL:-}}"
[ -n "${REPO_URL}" ] || fail "falta TARGET_REPO_GIT_URL -- pasalo como segundo argumento o seteala en .env"

TARGET_REPO_CLONE_DIR="${TARGET_REPO_CLONE_DIR:-${ROOT_DIR}/.dood-target-repo}"
TRUNK_BRANCH="${TRUNK_BRANCH:-main}"

echo "== 1. Clonando/actualizando el repo objetivo =="
# Bug real confirmado esta noche: una credencial embebida en la URL de 'git
# clone' queda persistida en texto plano en .git/config -- se usa -c
# http.extraheader en vez de eso (nunca se guarda en el repo), tanto para el
# clone inicial COMO para cada 'fetch' posterior -- sin re-pasarlo en el
# fetch, una corrida repetida contra un repo privado falla con
# "Authentication failed" porque no quedo NADA de credencial persistida
# (confirmado real probando este mismo script).
AUTH_ARGS=()
if [[ "${REPO_URL}" == *"dev.azure.com"* ]] && [ -n "${AZURE_DEVOPS_PAT:-}" ]; then
  AUTH_HEADER="Authorization: Basic $(printf ':%s' "${AZURE_DEVOPS_PAT}" | base64 -w0)"
  AUTH_ARGS=(-c "http.extraheader=${AUTH_HEADER}")
fi

if [ -d "${TARGET_REPO_CLONE_DIR}/.git" ]; then
  echo "Ya existe un clon en ${TARGET_REPO_CLONE_DIR} -- actualizando en vez de reclonar."
  git "${AUTH_ARGS[@]}" -C "${TARGET_REPO_CLONE_DIR}" fetch origin || fail "no se pudo hacer fetch en el clon existente"
  git -C "${TARGET_REPO_CLONE_DIR}" checkout "${TRUNK_BRANCH}" || fail "no se pudo hacer checkout de '${TRUNK_BRANCH}'"
  git -C "${TARGET_REPO_CLONE_DIR}" reset --hard "origin/${TRUNK_BRANCH}" || fail "no se pudo resetear a origin/${TRUNK_BRANCH}"
  git -C "${TARGET_REPO_CLONE_DIR}" clean -fd || true
else
  git "${AUTH_ARGS[@]}" -c core.autocrlf=false clone -q "${REPO_URL}" "${TARGET_REPO_CLONE_DIR}" \
    || fail "no se pudo clonar ${REPO_URL}"
fi
unset AUTH_HEADER

# Bug real confirmado en vivo (operacion de esta noche): un clon fresco sin
# identidad de git configurada crashea el primer 'git commit' real a mitad
# de la corrida ("Author identity unknown").
if [ -z "$(git -C "${TARGET_REPO_CLONE_DIR}" config --get user.name 2>/dev/null)" ]; then
  git -C "${TARGET_REPO_CLONE_DIR}" config user.name "${GIT_AUTHOR_NAME:-poc-ai-agents}"
fi
if [ -z "$(git -C "${TARGET_REPO_CLONE_DIR}" config --get user.email 2>/dev/null)" ]; then
  git -C "${TARGET_REPO_CLONE_DIR}" config user.email "${GIT_AUTHOR_EMAIL:-poc@local}"
fi

echo "== 2. Preflight (Prefect, credenciales, identidad de git en el clon) =="
bash "${SCRIPT_DIR}/check_prereqs.sh" "${TARGET_REPO_CLONE_DIR}" || fail "el preflight encontro problemas arriba -- resolvelos antes de gastar tiempo en una corrida real."

echo "== 3. Imagen del testrunner =="
if ! docker image inspect poc-ai-agents-testrunner >/dev/null 2>&1; then
  echo "poc-ai-agents-testrunner no existe todavia -- construyendo (puede tardar varios minutos la primera vez)."
  docker build -f "${ROOT_DIR}/Dockerfile.testrunner" -t poc-ai-agents-testrunner "${ROOT_DIR}" || fail "fallo el build de la imagen"
fi

echo "== 4. Traduciendo paths del HOST para el daemon (Docker-outside-of-Docker) =="
# MSYS_NO_PATHCONV=1 (mas abajo) es necesario para que git-bash NO reescriba
# los paths DEL CONTENEDOR (/repo, /target-repo dentro del -lc) como si
# fueran paths de Windows -- pero eso tambien apaga la traduccion automatica
# de los paths reales DEL HOST (ROOT_DIR/TARGET_REPO_CLONE_DIR, en formato
# posix /c/Users/... de git-bash), que el docker.exe nativo de Windows no
# entiende. Bug real confirmado: "docker: open /c/Users/.../.env: El sistema
# no puede encontrar la ruta especificada." -- se traducen a mano ambos,
# igual que ya se hacia solo para HOST_TARGET_REPO_DIR.
if command -v cygpath >/dev/null 2>&1; then
  HOST_TARGET_REPO_DIR="$(cygpath -w "${TARGET_REPO_CLONE_DIR}")"
  HOST_ROOT_DIR="$(cygpath -w "${ROOT_DIR}")"
else
  # Host no-Windows: el daemon real y este script ven el mismo path.
  HOST_TARGET_REPO_DIR="${TARGET_REPO_CLONE_DIR}"
  HOST_ROOT_DIR="${ROOT_DIR}"
fi
echo "HOST_TARGET_REPO_DIR=${HOST_TARGET_REPO_DIR}"

echo "== 5. Corriendo --epic ${EPIC_KEY} =="
if [ "${NO_AUTO_CONFIRM:-0}" != "1" ]; then
  echo "Confirmaciones interactivas auto-aprobadas (NO_AUTO_CONFIRM=1 para responder a mano)."
fi

DOCKER_RUN=(docker run -i --rm --name "epic-run-${EPIC_KEY,,}-$(date +%s)"
  -v "${HOST_ROOT_DIR}:/repo"
  -v "${HOST_TARGET_REPO_DIR}:/target-repo"
  -v /var/run/docker.sock:/var/run/docker.sock
  --env-file "${HOST_ROOT_DIR}/.env"
  -e "HOST_TARGET_REPO_DIR=${HOST_TARGET_REPO_DIR}"
  # .env apunta a localhost:PUERTO para uso desde el host -- dentro de este
  # contenedor (en poc-ai-agents_poc-net) "localhost" es el propio
  # contenedor, no docker-compose. Se sobreescriben con los container_name
  # reales del docker-compose, resolubles por DNS en la misma red.
  -e "NEO4J_URI=bolt://poc-neo4j:7687"
  -e "SONAR_URL=http://poc-sonarqube:9000"
  -e "QDRANT_URL=http://poc-qdrant:6333"
  -e "FIREWALL_URL=http://poc-ai-firewall:8080"
  -e "OLLAMA_URL=http://poc-ollama:11434"
  -e "PREFECT_API_URL=http://poc-prefect-server:4200/api"
  --network poc-ai-agents_poc-net
  -w /target-repo
  poc-ai-agents-testrunner
  bash -lc "cd /target-repo && python3 /repo/orchestration.py --epic ${EPIC_KEY}"
)

if [ "${NO_AUTO_CONFIRM:-0}" != "1" ]; then
  # Bug real confirmado: con 'set -o pipefail' (arriba), cuando docker run
  # termina (incluso OK) 'yes' sigue escribiendo al pipe ya cerrado y muere
  # con SIGPIPE (141) -- pipefail promueve ESE codigo a la salida de todo
  # el pipeline, reportando "fallo" aunque la corrida real haya terminado
  # bien. El codigo real que importa es el de docker run, no el de yes.
  yes s | MSYS_NO_PATHCONV=1 "${DOCKER_RUN[@]}"
  docker_exit="${PIPESTATUS[1]}"
else
  MSYS_NO_PATHCONV=1 "${DOCKER_RUN[@]}"
  docker_exit=$?
fi
exit "${docker_exit}"
