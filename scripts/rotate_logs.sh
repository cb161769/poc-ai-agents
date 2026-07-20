#!/usr/bin/env bash
# Rotacion de logs/*.jsonl + limpieza de archivos temporales de conversacion
# huerfanos -- gap real confirmado esta sesion: logs/falco_alerts.jsonl
# crecio a 59.902+ lineas (73MB) sin ningun limite, y ningun otro *.jsonl
# (coding_agent_runs, judge_verdicts, firewall_audit) tiene rotacion.
#
# Uso:
#   ./scripts/rotate_logs.sh              # rota lo que supere el umbral
#   LOG_ROTATE_MAX_LINES=500 ./scripts/rotate_logs.sh   # umbral custom (ej. para probar)
#
# Idempotente y seguro de correr con el pipeline/Falco activos: usa el
# patron "copytruncate" clasico de logrotate (copiar + truncar en el MISMO
# path/inode), no 'mv'. Confirmado real probando esto: Falco escribe
# logs/falco_alerts.jsonl con un file handle abierto de forma persistente
# (no reabre el path en cada escritura) -- un 'mv' hace que Falco siga
# escribiendo para siempre al archivo RENOMBRADO (que despues se comprime y
# se abandona), y el archivo nuevo en el path original nunca vuelve a
# recibir nada. copytruncate evita esto: el path/inode original nunca
# cambia, solo se vacia su contenido -- cualquier escritor con el archivo
# ya abierto sigue escribiendo ahi sin enterarse.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOGS_DIR="${ROOT_DIR}/logs"
ARCHIVE_DIR="${LOGS_DIR}/archive"

LOG_ROTATE_MAX_LINES="${LOG_ROTATE_MAX_LINES:-10000}"
LOG_ROTATE_KEEP="${LOG_ROTATE_KEEP:-5}"
ORPHAN_MAX_AGE_HOURS="${ORPHAN_MAX_AGE_HOURS:-24}"

mkdir -p "${ARCHIVE_DIR}"

echo "== Rotando logs/*.jsonl (umbral: ${LOG_ROTATE_MAX_LINES} lineas) =="
for log_file in "${LOGS_DIR}"/*.jsonl; do
  [ -f "${log_file}" ] || continue
  name="$(basename "${log_file}")"
  line_count="$(wc -l < "${log_file}" 2>/dev/null || echo 0)"

  if [ "${line_count}" -le "${LOG_ROTATE_MAX_LINES}" ]; then
    echo "  ${name}: ${line_count} lineas -- ok, no hace falta rotar"
    continue
  fi

  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  rotated_path="${ARCHIVE_DIR}/${name%.jsonl}.${timestamp}.jsonl"
  cp "${log_file}" "${rotated_path}"
  # Ventana de carrera minima aceptada (mismo tradeoff que logrotate
  # copytruncate): una escritura que ocurra justo entre el 'cp' y el
  # truncate de abajo se pierde -- preferible a la alternativa real
  # (mv) que puede perder TODAS las escrituras futuras silenciosamente.
  : > "${log_file}"
  gzip -f "${rotated_path}"
  echo "  ${name}: ${line_count} lineas -- rotado a ${rotated_path}.gz"

  # Conserva como maximo LOG_ROTATE_KEEP archivos comprimidos por log.
  mapfile -t old_archives < <(ls -1t "${ARCHIVE_DIR}/${name%.jsonl}."*.jsonl.gz 2>/dev/null | tail -n +$((LOG_ROTATE_KEEP + 1)))
  for old in "${old_archives[@]:-}"; do
    [ -n "${old}" ] || continue
    rm -f "${old}"
    echo "    borrado archivo viejo: $(basename "${old}")"
  done
done

echo "== Limpiando archivos temporales de conversacion huerfanos (mas de ${ORPHAN_MAX_AGE_HOURS}hs) =="
# .coding_agent_payload_*.json / .coding_agent_retry_payload_*.json: se
# borran normalmente en un 'finally' apenas termina el subprocess real
# (orchestration.py) -- solo sobreviven si ese proceso murio a mitad de
# camino (kill -9, crash del contenedor). Confirmado real: 1 archivo asi
# encontrado en esta sesion.
found_any=0
while IFS= read -r -d '' orphan; do
  found_any=1
  rm -f "${orphan}"
  echo "  borrado: $(basename "${orphan}")"
done < <(find "${LOGS_DIR}" -maxdepth 1 -name ".coding_agent_*.json" -type f -mmin "+$((ORPHAN_MAX_AGE_HOURS * 60))" -print0 2>/dev/null)
[ "${found_any}" = "1" ] || echo "  ninguno encontrado"
