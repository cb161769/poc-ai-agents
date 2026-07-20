"""Shared, pure constants/logic used by BOTH orchestrators (orchestration.py
and run_poc_loop.sh) so they can't drift out of sync silently.

Born from a real bug this session: RETRYABLE_POLICY_REFERENCES existed in
THREE places (judge_agent.py, a Python set duplicated in orchestration.py,
and a bash array duplicated in run_poc_loop.sh) -- the Python duplicate had
a test guarding it against drift, the bash one didn't, until it was added.
This module is the single source of truth judge_agent.py/orchestration.py
import directly; run_poc_loop.sh reads it via the CLI below instead of
hand-maintaining its own copy.

Usage from bash:
    python3 pipeline_shared.py retryable-policy-references
    # prints one value per line
"""
import sys
from dataclasses import dataclass
from enum import Enum


class ErrorCategory(str, Enum):
    """Categoria real de POR QUE un policy_reference bloquea -- gap
    identificado en la misma auditoria que TicketState: antes de esto,
    seguridad/alcance/evidencia-insuficiente se resolvian todas igual (via
    policy_reference como string suelto), sin ninguna separacion explicita
    entre "esto es un problema de seguridad real" vs. "esto es evidencia
    insuficiente que un segundo intento puede corregir".
    """
    SECURITY = "security"                  # fuga de datos, jailbreak, firewall que dejo pasar algo real
    SCOPE = "scope"                         # el cambio no corresponde al alcance del ticket
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"  # cobertura de tests/impacto en grafo sin verificar
    OTHER = "other"


@dataclass(frozen=True)
class PolicyRule:
    policy_reference: str
    category: ErrorCategory
    retryable: bool
    description: str


# Fuente unica real para lo que antes vivia repartido: judge_agent.py
# (JUDGE_POLICY_IDS, la lista de ids validos), orchestration.py (que decide
# si reintentar segun RETRYABLE_POLICY_REFERENCES), y evals/JUDGE_POLICY.md
# (las descripciones para humanos). Las descripciones de abajo son las
# MISMAS que evals/JUDGE_POLICY.md ya documenta -- un test de sincronia
# (tests/test_pipeline_shared.py) confirma que las claves de este dict
# coinciden exactamente con judge_agent.JUDGE_POLICY_IDS, asi que agregar un
# policy_reference nuevo al juez sin registrarlo aca falla un test en vez de
# quedar en silencio.
POLICY_REGISTRY: dict = {
    "data-leak-evidence": PolicyRule(
        "data-leak-evidence", ErrorCategory.SECURITY, retryable=False,
        description="El cambio o el prompt exponen (o casi exponen) un secreto real que el firewall no redacto del todo.",
    ),
    "jailbreak-evidence": PolicyRule(
        "jailbreak-evidence", ErrorCategory.SECURITY, retryable=False,
        description="El ticket o el diff contienen evidencia de un intento de manipular al agente que el firewall no capturo.",
    ),
    "scope-mismatch": PolicyRule(
        "scope-mismatch", ErrorCategory.SCOPE, retryable=True,
        description="El cambio aplicado no corresponde al alcance descrito en el ticket.",
    ),
    "insufficient-test-coverage": PolicyRule(
        "insufficient-test-coverage", ErrorCategory.INSUFFICIENT_EVIDENCE, retryable=True,
        description="Los tests que pasaron no cubren razonablemente el cambio real.",
    ),
    "graph-impact-unverified": PolicyRule(
        "graph-impact-unverified", ErrorCategory.INSUFFICIENT_EVIDENCE, retryable=True,
        description="El cambio afecta a un componente con dependientes reales en el grafo, y no hay evidencia de que se haya considerado ese impacto.",
    ),
    "firewall-false-negative": PolicyRule(
        "firewall-false-negative", ErrorCategory.SECURITY, retryable=False,
        description="El firewall aprobo algo que, revisado con mas contexto, deberia haber sido rechazado.",
    ),
    "other": PolicyRule(
        "other", ErrorCategory.OTHER, retryable=False,
        description="Cualquier otro problema real y concreto no cubierto arriba.",
    ),
}

# Retrocompatible: mismo valor exacto de hoy, ahora DERIVADO del registry en
# vez de mantenido a mano por separado -- callers existentes
# (orchestration.py, judge_agent.py, run_poc_loop.sh via el CLI de abajo)
# no necesitan cambiar nada.
RETRYABLE_POLICY_REFERENCES = {ref for ref, rule in POLICY_REGISTRY.items() if rule.retryable}


class TicketState(str, Enum):
    """Maquina de estados explicita por ticket -- gap real identificado en
    una auditoria previa (orchestration_architecture_gaps_mvp, memoria de
    sesion): antes de esto, cada rama de orchestration.py armaba su propio
    string suelto de "outcome" (ok/no-op/already-completed/no-verdict) sin
    ningun tipo/contrato formal detras. Vive aca (no en orchestration.py)
    para que cualquier otro consumidor (evals, graph_writer, un dashboard
    futuro) tenga una sola fuente de verdad de que estados existen.
    """
    PENDING = "pending"
    RUNNING = "running"
    BLOCKED_POLICY = "blocked_policy"      # firewall/juez rechazo por politica real
    BLOCKED_INFRA = "blocked_infra"        # Jira/Azure/Neo4j/git no disponible o fallo real
    BLOCKED_AGENT = "blocked_agent"        # el coding agent no pudo aplicar nada (no-op, EOF, etc)
    PARTIAL_FAILURE = "partial_failure"    # se aplico un cambio real (commit/PR) pero un paso posterior no se completo
    DONE = "done"


# Mapeo 1 a 1 de los strings de "outcome" ad-hoc que _deliver_epic_sequential
# (orchestration.py) ya arma hoy -- no cambia esos strings (varios tests
# existentes los comparan por igualdad exacta), solo les da una categoria
# tipada para quien la necesite (el resumen de la epica, el grafo, etc).
OUTCOME_TO_TICKET_STATE = {
    "already-completed": TicketState.DONE,
    "ok": TicketState.DONE,
    "no-op": TicketState.BLOCKED_AGENT,
    "no-verdict": TicketState.PARTIAL_FAILURE,
}


def outcome_to_state(outcome: str) -> TicketState:
    """Best-effort: un outcome no reconocido (nunca deberia pasar en
    produccion, pero un caller nuevo puede agregar un string sin registrarlo
    aca) cae a PENDING en vez de lanzar -- mismo criterio de
    graceful-degradation que el resto de este modulo."""
    return OUTCOME_TO_TICKET_STATE.get(outcome, TicketState.PENDING)


def main():
    if len(sys.argv) != 2 or sys.argv[1] != "retryable-policy-references":
        print("usage: pipeline_shared.py retryable-policy-references", file=sys.stderr)
        sys.exit(1)
    for ref in sorted(RETRYABLE_POLICY_REFERENCES):
        print(ref)


if __name__ == "__main__":
    main()
