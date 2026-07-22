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
# Monorepo-aware (confirmado real contra ai-agents-code: auth-service/pom.xml,
# frontend/package.json, data-worker/Pipfile, NINGUNO en la raiz): si la raiz
# no tiene ningun marcador reconocido, escanea subcarpetas de primer nivel
# (mismo criterio que coding_agent.py::tool_detect_project_stack) y corre
# tests para CADA sub-proyecto detectado, no solo uno -- un ticket puntual
# puede tocar mas de uno.
#
# Docker-outside-of-Docker: cuando este script corre DENTRO de un contenedor
# (ej. poc-ai-agents-testrunner, con /var/run/docker.sock montado), el path
# del repo que ve ESTE script (MODULE_PATH, ej. /target-repo) no es el mismo
# path que el daemon de Docker real (que vive en el HOST) puede montar -- el
# daemon monta paths del HOST, no de un contenedor sibling. Por eso el
# segundo argumento opcional HOST_MODULE_PATH: el path real, visible para el
# HOST, que se usa SOLO para el -v del docker run anidado. Sin DooD (corriendo
# esto nativo en un host real), se omite y HOST_MODULE_PATH cae a MODULE_PATH.
#
# Usage: run_module_tests.sh <path-to-repo> [<host-path-for-docker-mount>]
set -uo pipefail

MODULE_PATH="${1:?usage: run_module_tests.sh <path-to-repo> [<host-path-for-docker-mount>]}"
HOST_MODULE_PATH="${2:-$MODULE_PATH}"

fail_json() {
  echo "{\"passed\": false, \"repo\": \"${MODULE_PATH}\", \"output\": \"$1\"}"
  exit 1
}

if [ ! -d "${MODULE_PATH}" ]; then
  fail_json "no existe el directorio ${MODULE_PATH}"
fi

_SKIP_DIRS_TESTS=".git node_modules __pycache__ .venv venv dist build"

# --- Auto-detectar el stack por archivo de proyecto presente en DIR ---
# Orden: de mas especifico a mas generico. package.json queda ultimo a
# proposito porque es el marcador mas generico (Node/TS de cualquier sabor:
# NestJS, Angular, Ionic, Expo, React Native, Vitest/Jest plano, etc.) — así
# NestJS/Angular/Ionic/Expo/React Native comparten una sola rama en vez de
# necesitar un caso por framework. Setea IMAGE/TEST_CMD/LINT_CMD (globales,
# se resetean en cada llamado); devuelve 1 si no reconoce nada en DIR.
detect_stack() {
  local dir="$1"
  IMAGE=""
  TEST_CMD=""
  # LINT_CMD queda vacio si no se detecta config de lint en el repo objetivo
  # -- es un gate ADVISORY (nunca afecta el exit code), asi que solo corre
  # cuando hay algo real que ejecutar. go vet/cargo clippy son la excepcion:
  # son parte del toolchain, no necesitan config, asi que corren siempre.
  LINT_CMD=""

  if [ -f "${dir}/pom.xml" ]; then
    IMAGE="maven:3.9-eclipse-temurin-17"
    TEST_CMD="mvn -B -q test"
    if grep -qE 'checkstyle|spotless' "${dir}/pom.xml" 2>/dev/null; then
      LINT_CMD="mvn -B -q verify -DskipTests"
    fi

  elif [ -n "$(find "${dir}" -maxdepth 1 -name '*.csproj' -print -quit 2>/dev/null)" ] || [ -n "$(find "${dir}" -maxdepth 1 -name '*.sln' -print -quit 2>/dev/null)" ]; then
    IMAGE="mcr.microsoft.com/dotnet/sdk:8.0"
    TEST_CMD="dotnet test"
    LINT_CMD="dotnet format --verify-no-changes"

  elif [ -f "${dir}/go.mod" ]; then
    IMAGE="golang:1.22"
    TEST_CMD="go test ./..."
    LINT_CMD="go vet ./..."

  elif [ -f "${dir}/Gemfile" ]; then
    IMAGE="ruby:3.3"
    TEST_CMD="bundle install --quiet && bundle exec rspec"
    if [ -f "${dir}/.rubocop.yml" ]; then
      LINT_CMD="bundle exec rubocop"
    fi

  elif [ -f "${dir}/Cargo.toml" ]; then
    IMAGE="rust:1.78"
    TEST_CMD="cargo test"
    LINT_CMD="cargo clippy --quiet -- -D warnings"

  elif [ -f "${dir}/Pipfile" ]; then
    IMAGE="python:3.10-slim"
    TEST_CMD="pip install --quiet pipenv && pipenv install --dev --skip-lock --quiet && pipenv run pytest -q"
    if [ -f "${dir}/ruff.toml" ] || grep -q '\[tool.ruff\]' "${dir}/pyproject.toml" 2>/dev/null; then
      LINT_CMD="pip install --quiet ruff && ruff check ."
    fi

  elif [ -f "${dir}/requirements.txt" ]; then
    IMAGE="python:3.10-slim"
    TEST_CMD="pip install --quiet -r requirements.txt && pip install --quiet pytest && pytest -q"
    if [ -f "${dir}/ruff.toml" ] || grep -q '\[tool.ruff\]' "${dir}/pyproject.toml" 2>/dev/null; then
      LINT_CMD="pip install --quiet ruff && ruff check ."
    fi

  elif [ -f "${dir}/package.json" ]; then
    # Imagen oficial de Playwright: trae Node + navegadores headless con
    # dependencias de sistema completas (glibc) — cubre tanto tests unitarios
    # (Jest/Vitest/Karma, sea NestJS, Angular, Ionic, Expo o React Native)
    # como tests de UI real si el repo trae tests/*.spec.ts de Playwright.
    # v1.61.1-noble trae Node 24 (v1.44.1-jammy solo traia Node 20, insuficiente
    # para @capacitor/cli/@angular/cli modernos -- bug real confirmado en vivo).
    IMAGE="mcr.microsoft.com/playwright:v1.61.1-noble"
    if grep -q '"test"' "${dir}/package.json" 2>/dev/null; then
      TEST_CMD="npm install --silent && npm test"
    else
      TEST_CMD="npm install --silent && echo 'sin script \"test\" en package.json, se omite'"
    fi
    if [ -d "${dir}/tests" ] || [ -f "${dir}/playwright.config.ts" ]; then
      # Bug real confirmado en una corrida real (KAN-15, frontend/): la
      # imagen de arriba trae una version de Chromium fija (v1.44.1), pero
      # package.json suele declarar un rango semver (ej. "^1.44.1") -- npm
      # install real resolvio 1.61.1, y el Chromium viejo pre-instalado no
      # coincide ("Executable doesn't exist... Looks like Playwright was
      # just updated"). En vez de pinnear/parsear una version exacta
      # (fragil, se desincroniza en cuanto package.json suba de version),
      # se descargan los navegadores reales que coincidan con la version
      # YA instalada en node_modules -- funciona sin importar que version
      # resuelva npm install, siempre.
      TEST_CMD="${TEST_CMD} && npx playwright install --with-deps chromium && npx playwright test"
    fi
    if [ -f "${dir}/.eslintrc.json" ] || [ -f "${dir}/.eslintrc.js" ] || [ -f "${dir}/.eslintrc.cjs" ] || [ -f "${dir}/eslint.config.js" ] || [ -f "${dir}/eslint.config.mjs" ]; then
      LINT_CMD="npx eslint ."
    fi

  else
    return 1
  fi
  return 0
}

# run_stack_tests DIR HOST_DIR LABEL -- corre TEST_CMD/LINT_CMD (ya seteados
# por detect_stack) dentro de un contenedor throwaway, imprime el resultado,
# y devuelve el exit code real de TEST_CMD (lint es advisory, nunca decide
# el exit code -- se captura explicitamente adentro del contenedor y se
# re-emite al final).
run_stack_tests() {
  local dir="$1" host_dir="$2" label="$3"
  echo "Stack detectado en ${label}: imagen=${IMAGE}"

  local combined_cmd="${TEST_CMD}"
  if [ -n "${LINT_CMD}" ]; then
    combined_cmd="${TEST_CMD}; __test_exit=\$?; echo; echo '--- LINT (advisory, no bloquea) ---'; ${LINT_CMD} || echo '(lint fallo o encontro hallazgos -- no bloquea la corrida)'; exit \${__test_exit}"
  fi

  local output exit_code
  output=$(docker run --rm -v "${host_dir}:/work" -w /work "${IMAGE}" sh -c "${combined_cmd}" 2>&1)
  exit_code=$?

  echo "${output}"
  echo
  if [ "${exit_code}" -eq 0 ]; then
    echo "✅ Tests reales de ${label} pasaron."
  else
    echo "❌ Tests reales de ${label} fallaron (exit ${exit_code})."
  fi
  return "${exit_code}"
}

if detect_stack "${MODULE_PATH}"; then
  run_stack_tests "${MODULE_PATH}" "${HOST_MODULE_PATH}" "${MODULE_PATH}"
  exit $?
fi

# Sin marcador en la raiz -- monorepo real. Escanea subcarpetas de primer
# nivel y corre tests para CADA sub-proyecto detectado.
FOUND_ANY=0
OVERALL_EXIT=0
for subdir in "${MODULE_PATH}"/*/; do
  subdir="${subdir%/}"
  name="$(basename "${subdir}")"
  case " ${_SKIP_DIRS_TESTS} " in
    *" ${name} "*) continue ;;
  esac
  case "${name}" in .*) continue ;; esac

  if detect_stack "${subdir}"; then
    FOUND_ANY=1
    run_stack_tests "${subdir}" "${HOST_MODULE_PATH}/${name}" "${name}/"
    sub_exit=$?
    if [ "${sub_exit}" -ne 0 ]; then
      OVERALL_EXIT=1
    fi
  fi
done

if [ "${FOUND_ANY}" -eq 0 ]; then
  fail_json "no se pudo detectar el stack en ${MODULE_PATH} ni en sus subcarpetas de primer nivel (sin pom.xml/*.csproj/go.mod/Gemfile/Cargo.toml/Pipfile/requirements.txt/package.json). Agrega el archivo de proyecto estandar de tu ecosistema, o un caso manual en este script."
fi

exit "${OVERALL_EXIT}"
