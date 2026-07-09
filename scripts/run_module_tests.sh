#!/usr/bin/env bash
# Testing agent (deterministic gate, not an LLM): runs the REAL test suite of
# whatever is currently checked out in the target repo (the branch the
# coding-agent fallback just committed to), inside a throwaway container —
# nothing installed on the host, nothing left behind. Prints the test output
# and exits non-zero on failure so run_poc_loop.sh / orchestration.py can
# gate the judge on it.
#
# Auto-detects the stack by looking at which project file exists at the
# repo root — it does NOT hardcode a fixed list of components. This is what
# makes "backends in different languages/frameworks" (.NET, NestJS, Angular,
# Ionic, Expo, React Native, Go, Ruby, Rust, plain Node...) work without
# editing this script every time a new one gets connected: as long as the
# repo has the standard project file for its ecosystem, the right image +
# command get picked automatically.
#
# Usage: run_module_tests.sh <path-to-repo>
set -uo pipefail

MODULE_PATH="${1:?usage: run_module_tests.sh <path-to-repo>}"

fail_json() {
  echo "{\"passed\": false, \"repo\": \"${MODULE_PATH}\", \"output\": \"$1\"}"
  exit 1
}

if [ ! -d "${MODULE_PATH}" ]; then
  fail_json "no existe el directorio ${MODULE_PATH}"
fi

# --- Auto-detectar el stack por archivo de proyecto presente ---
# Orden: de mas especifico a mas generico. package.json queda ultimo a
# proposito porque es el marcador mas generico (Node/TS de cualquier sabor:
# NestJS, Angular, Ionic, Expo, React Native, Vitest/Jest plano, etc.) — así
# NestJS/Angular/Ionic/Expo/React Native comparten una sola rama en vez de
# necesitar un caso por framework.
IMAGE=""
TEST_CMD=""
# LINT_CMD queda vacio si no se detecta config de lint en el repo objetivo --
# es un gate ADVISORY (nunca afecta EXIT_CODE, ver mas abajo), asi que solo
# corre cuando hay algo real que ejecutar. go vet/cargo clippy son la
# excepcion: son parte del toolchain, no necesitan config, asi que corren
# siempre.
LINT_CMD=""

if [ -f "${MODULE_PATH}/pom.xml" ]; then
  IMAGE="maven:3.9-eclipse-temurin-17"
  TEST_CMD="mvn -B -q test"
  if grep -qE 'checkstyle|spotless' "${MODULE_PATH}/pom.xml" 2>/dev/null; then
    LINT_CMD="mvn -B -q verify -DskipTests"
  fi

elif [ -n "$(find "${MODULE_PATH}" -maxdepth 2 -name '*.csproj' -print -quit 2>/dev/null)" ] || [ -n "$(find "${MODULE_PATH}" -maxdepth 1 -name '*.sln' -print -quit 2>/dev/null)" ]; then
  IMAGE="mcr.microsoft.com/dotnet/sdk:8.0"
  TEST_CMD="dotnet test"
  LINT_CMD="dotnet format --verify-no-changes"

elif [ -f "${MODULE_PATH}/go.mod" ]; then
  IMAGE="golang:1.22"
  TEST_CMD="go test ./..."
  LINT_CMD="go vet ./..."

elif [ -f "${MODULE_PATH}/Gemfile" ]; then
  IMAGE="ruby:3.3"
  TEST_CMD="bundle install --quiet && bundle exec rspec"
  if [ -f "${MODULE_PATH}/.rubocop.yml" ]; then
    LINT_CMD="bundle exec rubocop"
  fi

elif [ -f "${MODULE_PATH}/Cargo.toml" ]; then
  IMAGE="rust:1.78"
  TEST_CMD="cargo test"
  LINT_CMD="cargo clippy --quiet -- -D warnings"

elif [ -f "${MODULE_PATH}/Pipfile" ]; then
  IMAGE="python:3.10-slim"
  TEST_CMD="pip install --quiet pipenv && pipenv install --dev --skip-lock --quiet && pipenv run pytest -q"
  if [ -f "${MODULE_PATH}/ruff.toml" ] || grep -q '\[tool.ruff\]' "${MODULE_PATH}/pyproject.toml" 2>/dev/null; then
    LINT_CMD="pip install --quiet ruff && ruff check ."
  fi

elif [ -f "${MODULE_PATH}/requirements.txt" ]; then
  IMAGE="python:3.10-slim"
  TEST_CMD="pip install --quiet -r requirements.txt && pip install --quiet pytest && pytest -q"
  if [ -f "${MODULE_PATH}/ruff.toml" ] || grep -q '\[tool.ruff\]' "${MODULE_PATH}/pyproject.toml" 2>/dev/null; then
    LINT_CMD="pip install --quiet ruff && ruff check ."
  fi

elif [ -f "${MODULE_PATH}/package.json" ]; then
  # Imagen oficial de Playwright: trae Node + navegadores headless con
  # dependencias de sistema completas (glibc) — cubre tanto tests unitarios
  # (Jest/Vitest/Karma, sea NestJS, Angular, Ionic, Expo o React Native)
  # como tests de UI real si el repo trae tests/*.spec.ts de Playwright.
  IMAGE="mcr.microsoft.com/playwright:v1.44.1-jammy"
  if grep -q '"test"' "${MODULE_PATH}/package.json" 2>/dev/null; then
    TEST_CMD="npm install --silent && npm test"
  else
    TEST_CMD="npm install --silent && echo 'sin script \"test\" en package.json, se omite'"
  fi
  if [ -d "${MODULE_PATH}/tests" ] || [ -f "${MODULE_PATH}/playwright.config.ts" ]; then
    TEST_CMD="${TEST_CMD} && npx playwright test"
  fi
  if [ -f "${MODULE_PATH}/.eslintrc.json" ] || [ -f "${MODULE_PATH}/.eslintrc.js" ] || [ -f "${MODULE_PATH}/.eslintrc.cjs" ] || [ -f "${MODULE_PATH}/eslint.config.js" ] || [ -f "${MODULE_PATH}/eslint.config.mjs" ]; then
    LINT_CMD="npx eslint ."
  fi

else
  fail_json "no se pudo detectar el stack en ${MODULE_PATH} (sin pom.xml/*.csproj/go.mod/Gemfile/Cargo.toml/Pipfile/requirements.txt/package.json). Agrega el archivo de proyecto estandar de tu ecosistema, o un caso manual en este script."
fi

echo "Stack detectado en ${MODULE_PATH}: imagen=${IMAGE}"

# Lint es ADVISORY -- nunca decide EXIT_CODE. Se captura el exit code de
# TEST_CMD explicitamente adentro del contenedor, se corre el lint (si hay
# uno detectado) despues, y se re-emite el exit code de TEST_CMD al final --
# asi un fallo de lint (deuda tecnica preexistente del repo objetivo, no
# necesariamente causada por este cambio) nunca bloquea la corrida, pero su
# output SI queda en el mismo texto que ya llega al juez como test_summary.
if [ -n "${LINT_CMD}" ]; then
  COMBINED_CMD="${TEST_CMD}; __test_exit=\$?; echo; echo '--- LINT (advisory, no bloquea) ---'; ${LINT_CMD} || echo '(lint fallo o encontro hallazgos -- no bloquea la corrida)'; exit \${__test_exit}"
else
  COMBINED_CMD="${TEST_CMD}"
fi

OUTPUT=$(docker run --rm \
  -v "${MODULE_PATH}:/work" \
  -w /work \
  "${IMAGE}" \
  sh -c "${COMBINED_CMD}" 2>&1)
EXIT_CODE=$?

if [ "${EXIT_CODE}" -eq 0 ]; then
  echo "${OUTPUT}"
  echo
  echo "✅ Tests reales de ${MODULE_PATH} pasaron."
else
  echo "${OUTPUT}"
  echo
  echo "❌ Tests reales de ${MODULE_PATH} fallaron (exit ${EXIT_CODE})."
fi

exit "${EXIT_CODE}"
