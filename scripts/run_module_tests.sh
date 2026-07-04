#!/usr/bin/env bash
# Testing agent (deterministic gate, not an LLM): runs the REAL test suite of
# the affected module against whatever is currently checked out in
# sample-repo/ (the branch the coding-agent fallback just committed to),
# inside a throwaway container — nothing installed on the host, nothing left
# behind. Prints the test output and exits non-zero on failure so
# run_poc_loop.sh can gate the judge on it.
#
# Usage: run_module_tests.sh <AuthService|Frontend|DataWorker>
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPONENT="${1:?usage: run_module_tests.sh <AuthService|Frontend|DataWorker>}"

case "${COMPONENT}" in
  AuthService)
    MODULE_DIR="auth-service"
    IMAGE="maven:3.9-eclipse-temurin-17"
    TEST_CMD="mvn -B -q test"
    ;;
  Frontend)
    MODULE_DIR="frontend"
    # Imagen oficial de Playwright: trae Node + navegadores headless con
    # todas las dependencias del sistema (node:alpine no sirve para esto,
    # los binarios de Chromium/Firefox necesitan glibc, no musl).
    IMAGE="mcr.microsoft.com/playwright:v1.44.1-jammy"
    TEST_CMD="npm install --silent && npm test && npx playwright test"
    ;;
  DataWorker)
    MODULE_DIR="data-worker"
    IMAGE="python:3.10-slim"
    TEST_CMD="pip install --quiet pipenv && pipenv install --dev --skip-lock --quiet && pipenv run pytest -q"
    ;;
  *)
    echo "{\"passed\": false, \"component\": \"${COMPONENT}\", \"output\": \"componente desconocido, no se sabe que test suite correr\"}"
    exit 1
    ;;
esac

MODULE_PATH="${ROOT_DIR}/sample-repo/${MODULE_DIR}"

if [ ! -d "${MODULE_PATH}" ]; then
  echo "{\"passed\": false, \"component\": \"${COMPONENT}\", \"output\": \"no existe ${MODULE_PATH}\"}"
  exit 1
fi

OUTPUT=$(docker run --rm \
  -v "${MODULE_PATH}:/work" \
  -w /work \
  "${IMAGE}" \
  sh -c "${TEST_CMD}" 2>&1)
EXIT_CODE=$?

if [ "${EXIT_CODE}" -eq 0 ]; then
  echo "${OUTPUT}"
  echo
  echo "✅ Tests reales de ${COMPONENT} (${MODULE_DIR}) pasaron."
else
  echo "${OUTPUT}"
  echo
  echo "❌ Tests reales de ${COMPONENT} (${MODULE_DIR}) fallaron (exit ${EXIT_CODE})."
fi

exit "${EXIT_CODE}"
