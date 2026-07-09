#!/usr/bin/env bash
# One-shot setup: colapsa los pasos manuales 1-3 del Quick Start del README
# (crear .env, levantar la infraestructura, copiar el SONAR_TOKEN a mano y
# reiniciar ai-firewall, esperar el modelo de Ollama) en un solo comando.
# Cada paso es idempotente -- correr esto de nuevo no rompe nada si ya se
# hizo antes.
#
# NO reemplaza editar .env con tus credenciales reales de Jira/Azure DevOps
# -- eso sigue siendo manual a proposito (son secretos reales, nadie mas
# deberia poder generarlos por vos).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

fail() {
  echo "ERROR: $1" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "Docker no esta instalado."
command -v jq >/dev/null 2>&1 || fail "jq no esta instalado (lo requiere check_prereqs.sh)."

echo "=================================================================="
echo " 1/4 — Credenciales (.env)"
echo "=================================================================="
if [ ! -f .env ]; then
  cp .env.example .env
  echo "Se creo .env desde .env.example con valores de ejemplo."
  echo "IMPORTANTE: editalo con tus credenciales reales de Jira/Azure DevOps antes de correr run_poc_loop.sh."
else
  echo ".env ya existe, no se toca."
fi

echo
echo "=================================================================="
echo " 2/4 — Levantando infraestructura (docker compose up -d --build)"
echo "=================================================================="
docker compose up -d --build

echo
echo "=================================================================="
echo " 3/4 — Esperando el token real de SonarQube (sonar-scanner puede tardar 1-2 min la primera vez)"
echo "=================================================================="
SONAR_TOKEN_VALUE=""
for _ in $(seq 1 60); do
  SONAR_TOKEN_VALUE=$(docker compose exec -T sonar-scanner cat /state/sonar_token 2>/dev/null || true)
  [ -n "${SONAR_TOKEN_VALUE}" ] && break
  sleep 5
done

if [ -z "${SONAR_TOKEN_VALUE}" ]; then
  echo "AVISO: no se pudo leer el token de Sonar todavia. Revisa 'docker compose logs sonar-scanner' y corre este script de nuevo, o segui el paso manual del README (§2)."
else
  if grep -q '^SONAR_TOKEN=' .env; then
    sed -i.bak "s|^SONAR_TOKEN=.*|SONAR_TOKEN=${SONAR_TOKEN_VALUE}|" .env && rm -f .env.bak
  else
    echo "SONAR_TOKEN=${SONAR_TOKEN_VALUE}" >> .env
  fi
  echo "SONAR_TOKEN aplicado a .env — reiniciando ai-firewall para que lo tome..."
  docker compose restart ai-firewall >/dev/null
fi

echo
echo "=================================================================="
echo " 4/4 — Esperando el modelo de Ollama (servicio ollama-pull)"
echo "=================================================================="
for _ in $(seq 1 60); do
  status=$(docker compose ps -a --format '{{.Service}} {{.State}}' 2>/dev/null | awk '$1=="ollama-pull" {print $2}')
  [ "${status}" = "exited" ] && break
  sleep 5
done
if [ "${status:-}" != "exited" ]; then
  echo "AVISO: no se pudo confirmar que ollama-pull haya terminado. Revisa 'docker compose logs ollama-pull', o bajalo a mano: docker exec poc-ollama ollama pull \${OLLAMA_MODEL:-llama3.1}"
else
  echo "Modelo de Ollama listo."
fi

echo
echo "=================================================================="
echo " Verificando prerequisitos"
echo "=================================================================="
"${SCRIPT_DIR}/check_prereqs.sh"
