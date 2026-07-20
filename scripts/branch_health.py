"""CLI delgado que expone la logica REAL de salud de rama que ya vive en
orchestration.py (ensure_on_trunk_branch/_find_open_branch_for_ticket/
_check_pr_rejected_for_branch/_has_duplicate_project_scaffolding/run_tests)
a run_poc_loop.sh -- mismo patron que pipeline_shared.py ya usa para que
bash consuma logica Python real en vez de reimplementarla y arriesgar drift
entre dos implementaciones de lo mismo.

Bug real confirmado esta sesion: run_poc_loop.sh (el orquestador bash,
paralelo a orchestration.py) nunca reusaba una rama abierta ni chequeaba
trunk-branch/PR-rechazada/scaffolding-duplicado antes de crear una rama
nueva a ciegas -- la MISMA causa raiz que genero PRs #240/#241 apuntando
entre si en la epica KAN-4, todavia viva en el camino bash.

Uso:
    python3 scripts/branch_health.py resolve <TICKET_ID> <TARGET_REPO_DIR>

Imprime a stdout, formato KEY=VALUE (una linea por variable, parseable con
'eval' en bash):
    BRANCH=copilot/KAN-15-1234567890
    BASE_BRANCH=main
    RESUMED=true|false
    PR_REJECTED=true|false
    ABANDON_REASON=... (vacio si no aplica)
"""
import logging
import subprocess
import sys
import time
from pathlib import Path

from prefect.exceptions import MissingContextError

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

import orchestration  # noqa: E402
from orchestration import (  # noqa: E402
    _branch_diff_has_vendor_pollution,
    _check_pr_rejected_for_branch,
    _find_open_branch_for_ticket,
    _has_duplicate_project_scaffolding,
    ensure_on_trunk_branch,
    run_tests,
)

# Se llama con .fn() (bypassea el task-runner de Prefect) en vez de dentro
# de un @flow real -- probado ambas formas: envolver esto en un @flow real
# crashea con un bug de compatibilidad anyio/Python 3.13 en este entorno
# (TypeError: Can't instantiate abstract class GatherTaskGroup), ajeno a
# esta logica. Sin flow/task run activo, get_run_logger() (que run_tests()
# llama internamente si los tests fallan) lanza MissingContextError -- se
# reemplaza por un logger de modulo comun mientras dure este proceso CLI.
def _fallback_get_run_logger():
    try:
        from prefect import get_run_logger
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger("branch_health")


orchestration.get_run_logger = _fallback_get_run_logger


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def resolve(ticket_id: str, target_repo_dir: str) -> dict:
    base_branch = ensure_on_trunk_branch.fn(target_repo_dir)

    existing_branch = _find_open_branch_for_ticket(target_repo_dir, ticket_id, base_branch)
    pr_rejected = False
    abandon_reason = None

    if existing_branch:
        pr_rejected = _check_pr_rejected_for_branch(target_repo_dir, existing_branch)
        branch = existing_branch
        subprocess.run(["git", "-C", target_repo_dir, "checkout", branch], check=True)

        if pr_rejected:
            abandon_reason = "la PR previa de esta rama fue rechazada/cerrada sin mergear"
        else:
            vendor_pollution = _branch_diff_has_vendor_pollution(target_repo_dir, existing_branch, base_branch)
            if vendor_pollution:
                abandon_reason = vendor_pollution
            else:
                health_check = run_tests.fn(target_repo_dir)
                if not health_check["passed"]:
                    abandon_reason = "los tests reales YA fallan en esta rama antes de aplicar ningun cambio nuevo"
                else:
                    structural_issue = _has_duplicate_project_scaffolding(target_repo_dir)
                    if structural_issue:
                        abandon_reason = f"estructura duplicada detectada: {structural_issue}"

        if abandon_reason:
            subprocess.run(["git", "-C", target_repo_dir, "checkout", base_branch])
            existing_branch = None
            pr_rejected = False
            branch = f"copilot/{ticket_id}-{int(time.time())}"
            subprocess.run(["git", "-C", target_repo_dir, "checkout", "-b", branch], check=True)
    else:
        branch = f"copilot/{ticket_id}-{int(time.time())}"
        subprocess.run(["git", "-C", target_repo_dir, "checkout", "-b", branch], check=True)

    return {
        "branch": branch,
        "base_branch": base_branch,
        "resumed": bool(existing_branch),
        "pr_rejected": pr_rejected,
        "abandon_reason": abandon_reason or "",
    }


def main():
    if len(sys.argv) != 4 or sys.argv[1] != "resolve":
        print("usage: branch_health.py resolve <TICKET_ID> <TARGET_REPO_DIR>", file=sys.stderr)
        sys.exit(1)

    _, _, ticket_id, target_repo_dir = sys.argv
    result = resolve(ticket_id, target_repo_dir)

    print(f"BRANCH={_shell_quote(result['branch'])}")
    print(f"BASE_BRANCH={_shell_quote(result['base_branch'])}")
    print(f"RESUMED={'true' if result['resumed'] else 'false'}")
    print(f"PR_REJECTED={'true' if result['pr_rejected'] else 'false'}")
    print(f"ABANDON_REASON={_shell_quote(result['abandon_reason'])}")


if __name__ == "__main__":
    main()
