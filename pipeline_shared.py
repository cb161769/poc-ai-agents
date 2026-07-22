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
    python3 pipeline_shared.py lock-acquire <TICKET_ID>
    # prints ACQUIRED=true|false, exit 0 if acquired / 1 if not
    python3 pipeline_shared.py lock-release <TICKET_ID>
"""
import os
import sys
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


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

# Gap real confirmado en vivo (epica KAN-4, KAN-5 con qwen3:8b): el
# reintento le pasaba al coding agent el "reasoning" libre del juez mas la
# evidencia deterministica de que archivos no tienen test -- pero cuando el
# problema NO es "falta el archivo de test" sino "el test que ya existe es
# de baja calidad" (mocks ausentes, solo valida un valor fijo, no verifica
# comportamiento asincronico), esa evidencia deterministica dice "el diff SI
# incluye test(s)" y no aporta nada accionable. Confirmado real: el segundo
# intento de KAN-5 agrego codigo real pero NO corrigio los mocks, y el juez
# volvio a marcar FLAGGED por la misma razon exacta. Estos hints dan
# instrucciones concretas y especificas por policy_reference -- no
# reemplazan el reasoning real del juez (que sigue yendo primero en el
# feedback), lo complementan con una guia accionable que no depende de que
# el juez la haya redactado bien esa vez.
REMEDIATION_HINTS: dict = {
    "insufficient-test-coverage": (
        "Guia concreta: si el archivo de test YA EXISTE pero te marcaron esto de nuevo, "
        "el problema no es ausencia de test sino calidad -- revisa en particular: (1) "
        "toda dependencia externa/inyectada (servicios, HTTP, storage) tiene que estar "
        "mockeada explicitamente (jest.fn(), spyOn, o el equivalente del framework real "
        "del stack), nunca invocada de verdad en un test unitario; (2) todo metodo "
        "async/Promise tiene que verificarse con await/resolves/rejects (o fakeAsync/tick "
        "si es Angular), no asumido sincronico; (3) el assert tiene que validar un "
        "comportamiento real (una rama condicional, un error, un cambio de estado), no "
        "solo que la funcion devuelva un valor fijo sin importar el input."
    ),
    "scope-mismatch": (
        "Guia concreta: releé el alcance exacto del ticket y elimina o revierte cualquier "
        "archivo tocado que no corresponda a lo que el ticket pide -- si tocaste un "
        "archivo compartido por necesidad real, dejalo pero explicalo en el mensaje de "
        "commit/self_review, no lo quites en silencio."
    ),
    "graph-impact-unverified": (
        "Guia concreta: antes de este intento no se registro evidencia de haber revisado "
        "el grafo de dependencias -- consulta el impacto real del componente que tocaste "
        "(quien depende de el) y deja esa consulta reflejada en tu self_review antes de "
        "terminar."
    ),
}


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


# Gap real identificado en auditoria ("gaps en los flujos de jira"): ni
# orchestration.py ni run_poc_loop.sh tenian ninguna arbitracion entre
# procesos sobre el MISMO ticket -- un humano re-corriendo run_poc_loop.sh a
# mano mientras un flow de Prefect disparado por webhook todavia esta
# corriendo (o dos corridas agendadas solapadas) podian transicionar/
# comentar el mismo ticket en paralelo, con resultados intercalados y una
# condicion de carrera real en transition_ticket() (GET de transiciones
# disponibles, despues POST -- no atomico). Lock de archivo simple (no
# fcntl/flock: tiene que funcionar igual desde bash y desde Python, y
# portable entre el contenedor Linux real y una corrida local en Windows).
LOCK_DIR = Path(__file__).resolve().parent / "locks"
# Generoso a proposito: una epica real de 12 historias puede tardar bastante
# mas que un ticket individual -- mejor un lock stale que tarda en liberarse
# solo que uno que se roba en medio de una corrida real todavia viva.
DEFAULT_LOCK_STALE_SECONDS = 2 * 60 * 60


def _ticket_lock_path(ticket_id: str) -> Path:
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    return LOCK_DIR / f"{ticket_id}.lock"


def acquire_ticket_lock(ticket_id: str, stale_after_seconds: int = DEFAULT_LOCK_STALE_SECONDS) -> bool:
    """Creacion atomica de archivo (O_CREAT|O_EXCL) como lock -- si ya existe
    y es reciente, otro proceso esta trabajando este ticket ahora mismo:
    devuelve False (el caller decide degradar, nunca bloquea indefinidamente
    esperando). Si existe pero es mas vieja que stale_after_seconds, se
    asume que el proceso dueno crasheo sin liberar el lock (ej. el
    contenedor se mato a mitad de una corrida) y se la pisa -- best-effort,
    no perfectamente atomico contra otro robo concurrente exactamente en
    esa ventana, pero esta pensado para la concurrencia de baja frecuencia y
    ritmo humano de este pipeline, no para un lock de alta contencion.
    """
    path = _ticket_lock_path(ticket_id)
    payload = f"{os.getpid()}:{time.time()}"
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, payload.encode("utf-8"))
        os.close(fd)
        return True
    except FileExistsError:
        try:
            age_seconds = time.time() - path.stat().st_mtime
        except FileNotFoundError:
            return acquire_ticket_lock(ticket_id, stale_after_seconds)  # se libero justo entre el exists y el stat
        if age_seconds <= stale_after_seconds:
            return False
        path.write_text(payload)
        return True


def release_ticket_lock(ticket_id: str) -> None:
    _ticket_lock_path(ticket_id).unlink(missing_ok=True)


def main():
    if len(sys.argv) == 2 and sys.argv[1] == "retryable-policy-references":
        for ref in sorted(RETRYABLE_POLICY_REFERENCES):
            print(ref)
        return

    if len(sys.argv) == 3 and sys.argv[1] == "lock-acquire":
        acquired = acquire_ticket_lock(sys.argv[2])
        print(f"ACQUIRED={'true' if acquired else 'false'}")
        sys.exit(0 if acquired else 1)

    if len(sys.argv) == 3 and sys.argv[1] == "lock-release":
        release_ticket_lock(sys.argv[2])
        return

    print(
        "usage: pipeline_shared.py retryable-policy-references | lock-acquire <TICKET_ID> | lock-release <TICKET_ID>",
        file=sys.stderr,
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
