"""Unit tests for pipeline_shared.py -- la fuente unica de constantes/logica
compartida entre orchestration.py, run_poc_loop.sh, y judge_agent.py.
"""
import judge_agent
from pipeline_shared import (
    POLICY_REGISTRY,
    RETRYABLE_POLICY_REFERENCES,
    ErrorCategory,
    TicketState,
    outcome_to_state,
)


def test_outcome_to_state_maps_each_known_outcome():
    assert outcome_to_state("already-completed") == TicketState.DONE
    assert outcome_to_state("ok") == TicketState.DONE
    assert outcome_to_state("no-op") == TicketState.BLOCKED_AGENT
    assert outcome_to_state("no-verdict") == TicketState.PARTIAL_FAILURE


def test_outcome_to_state_falls_back_to_pending_for_unknown_outcome():
    assert outcome_to_state("some-new-outcome-nobody-registered") == TicketState.PENDING


def test_ticket_state_values_are_stable_strings():
    """TicketState se persiste en Neo4j (run.state) y se compara en Jira/
    logs -- confirma que el .value de cada miembro es el string esperado,
    no un detalle de implementacion del Enum que pueda cambiar solo."""
    assert TicketState.PENDING.value == "pending"
    assert TicketState.RUNNING.value == "running"
    assert TicketState.BLOCKED_POLICY.value == "blocked_policy"
    assert TicketState.BLOCKED_INFRA.value == "blocked_infra"
    assert TicketState.BLOCKED_AGENT.value == "blocked_agent"
    assert TicketState.PARTIAL_FAILURE.value == "partial_failure"
    assert TicketState.DONE.value == "done"


def test_retryable_policy_references_unaffected_by_ticket_state_addition():
    """No regression: agregar TicketState a este modulo no debe tocar el
    valor real de RETRYABLE_POLICY_REFERENCES que orchestration.py/
    judge_agent.py/run_poc_loop.sh ya consumen."""
    assert RETRYABLE_POLICY_REFERENCES == {"scope-mismatch", "insufficient-test-coverage", "graph-impact-unverified"}


def test_policy_registry_synced_with_judge_policy_ids():
    """Contrato formal real: si alguien agrega un policy_reference nuevo al
    juez (judge_agent.JUDGE_POLICY_IDS) sin registrarlo en POLICY_REGISTRY,
    este test falla en vez de quedar en silencio (el gap real que la
    auditoria de arquitectura identifico: 'sin versionado ni validacion
    cruzada formal', antes la unica proteccion era 'se importa de un solo
    lugar')."""
    assert set(POLICY_REGISTRY.keys()) == set(judge_agent.JUDGE_POLICY_IDS)


def test_retryable_policy_references_derived_correctly_from_registry():
    derived = {ref for ref, rule in POLICY_REGISTRY.items() if rule.retryable}
    assert derived == RETRYABLE_POLICY_REFERENCES


def test_policy_registry_security_references_are_never_retryable():
    for ref, rule in POLICY_REGISTRY.items():
        if rule.category == ErrorCategory.SECURITY:
            assert rule.retryable is False, f"{ref} es de seguridad pero quedo marcado retryable=True"
