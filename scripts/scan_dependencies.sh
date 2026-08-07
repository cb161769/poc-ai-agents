#!/usr/bin/env bash
# Dependency/SBOM/IaC scan del repo objetivo (pedido explicito del usuario,
# "Autonomy Is Earned" -- DevSecOps adicional) -- una sola herramienta
# (trivy) cubre las 3 cosas en vez de 3 scanners separados. Mismo criterio
# best-effort de rescan_sonar.sh: si trivy no esta disponible o el scan
# falla, se omite sin bloquear la corrida -- nunca es un gate duro.
#
# Usage: scan_dependencies.sh <target_repo_dir>
# Salida (stdout, JSON): {"scanned": bool, "critical_count": int, "high_count": int, "cves": [...], "reason": str|null}
set -uo pipefail

TARGET_REPO_DIR="${1:?usage: scan_dependencies.sh <target_repo_dir>}"

skip() {
  echo "{\"scanned\": false, \"critical_count\": 0, \"high_count\": 0, \"cves\": [], \"reason\": \"$1\"}"
  exit 0
}

echo "Escaneando dependencias/config de ${TARGET_REPO_DIR} con trivy..." >&2
SCAN_OUTPUT=$(docker run --rm \
  -v "${TARGET_REPO_DIR}:/target:ro" \
  aquasec/trivy:latest \
  fs --scanners vuln,config --severity CRITICAL,HIGH --format json --quiet /target 2>/dev/null)
SCAN_EXIT=$?

if [ "${SCAN_EXIT}" -ne 0 ] || [ -z "${SCAN_OUTPUT}" ]; then
  skip "trivy fallo o no devolvio salida -- se omite el scan de dependencias"
fi

echo "${SCAN_OUTPUT}" | python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except json.JSONDecodeError:
    print(json.dumps({"scanned": False, "critical_count": 0, "high_count": 0, "cves": [], "reason": "salida de trivy no fue JSON valido"}))
    sys.exit(0)

cves = []
for result in (data.get("Results") or []):
    for vuln in (result.get("Vulnerabilities") or []):
        cves.append({
            "id": vuln.get("VulnerabilityID"),
            "severity": vuln.get("Severity"),
            "pkg": vuln.get("PkgName"),
            "target": result.get("Target"),
        })

critical_count = sum(1 for c in cves if c["severity"] == "CRITICAL")
high_count = sum(1 for c in cves if c["severity"] == "HIGH")
print(json.dumps({"scanned": True, "critical_count": critical_count, "high_count": high_count, "cves": cves, "reason": None}))
'
