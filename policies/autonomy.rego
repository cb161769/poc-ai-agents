# Policy as Code ("Autonomy Is Earned", pedido explicito del usuario) --
# misma logica exacta que decide_policy() en orchestration.py, expresada en
# Rego para que los umbrales de autonomia sean auditables/cambiables sin un
# redeploy de codigo. orchestration.py llama a esto via HTTP primero (ver
# OPA_URL), con su propio decide_policy() en Python como fallback si OPA no
# responde -- mismo criterio best-effort que ya rige Falco/MCP en todo este
# pipeline.
#
# Input esperado (POST /v1/data/autonomy/decision, body {"input": {...}}):
# {
#   "risk_score": 0.0-1.0,
#   "blast_radius": entero (ver _blast_radius en orchestration.py),
#   "trust": 0.0-1.0 (compute_trust_score),
#   "consecutive_ok": entero (consecutive_ok_runs),
#   "judge_verdict": {"verdict": "OK"|"FLAGGED"|null, "policy_reference": str|null} | null,
#   "retryable_policy_references": [str, ...] (RETRYABLE_POLICY_REFERENCES real de pipeline_shared.py)
# }
package autonomy

import future.keywords.if
import future.keywords.in

is_flagged_terminal if {
	input.judge_verdict.verdict == "FLAGGED"
	not input.judge_verdict.policy_reference in input.retryable_policy_references
}

# Mismo orden de precedencia que decide_policy(): el veto del juez primero,
# despues blast radius critico (SIEMPRE gana sobre trust), despues trust/
# riesgo bajos fuerzan revision, recien despues se evalua AUTO_SHIP.
decision := "BLOCK" if is_flagged_terminal
else := "HUMAN_APPROVAL" if input.blast_radius >= 9
else := "REQUIRES_REVIEW" if input.trust < 0.5
else := "REQUIRES_REVIEW" if input.risk_score > 0.5
else := "AUTO_SHIP" if {
	input.trust >= 0.85
	input.risk_score < 0.2
	input.consecutive_ok >= 20
}
else := "AUTO_WITH_CHECKS"
