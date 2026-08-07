"""Trust ledger for the "autonomy is earned, not granted" policy engine.

Every historia that goes through _deliver_epic_sequential (orchestration.py)
already produces all the raw signal this module needs: a judge verdict
(judge_agent.py), a self_review from the coding agent (coding_agent.py), a
retry count, and a Falco correlation result. Nothing here adds new
instrumentation -- it just rolls up what's already logged into one number per
component, the same way judge_agent.py:log_verdict rolls up a single verdict
into logs/judge_verdicts.jsonl.

record_trust_signal() is best-effort by design (same criterion as
check_falco_correlation/MCP connect elsewhere in this pipeline): a failure to
write the ledger must never block or fail a real historia -- it's read back
later by compute_trust_score() to inform (not gate outright) the policy
engine's decisions.
"""
import json
import time
from pathlib import Path

from log_utils import get_logger

logger = get_logger(__name__)

LOG_DIR = Path(__file__).resolve().parent / "logs"
TRUST_LOG = LOG_DIR / "trust_scores.jsonl"

# Pesos de la formula documentada en el artefacto "Autonomy Is Earned" (esta
# sesion) -- suman 1.0. rollback_availability se fija en 1.0 para toda
# corrida de este pipeline (cada historia vive en su propia rama
# copilot/<ticket>-<ts>, nunca toca trunk directo -- ver run_coding_agent_local_real),
# asi que ese termino no necesita leerse del log, es una constante real de
# como esta construido este pipeline, no un valor inventado.
_WEIGHT_SUCCESS_RATE = 0.25
_WEIGHT_VERIFICATION_PASS_RATE = 0.20
_WEIGHT_RETRY = 0.15
_WEIGHT_ROLLBACK = 0.15
_WEIGHT_COVERAGE = 0.10
_WEIGHT_INCIDENT = 0.10
_WEIGHT_TOOL_RELIABILITY = 0.05

_ROLLBACK_AVAILABILITY = 1.0

_TRUST_WINDOW = 50


def record_trust_signal(
    component: str,
    ticket_id: str,
    change_type: str,
    verification: dict,
    judge_verdict: dict | None,
    retries_used: int,
    tool_call_failed: bool = False,
) -> None:
    """Appends one line to logs/trust_scores.jsonl -- mismo patron exacto que
    judge_agent.py:log_verdict (mkdir + un JSON por linea, nunca lanza).

    verification: {"output_guard_clean": bool, "tests_passed": bool,
      "new_sonar_issues": int, "coding_agent_review_verdict": str|None,
      "tests_adequate": bool, "falco_correlated": bool}
    judge_verdict: el dict crudo devuelto por run_judge/_run_judge_safe, o
      None si el juez no pudo evaluar (se trata como no-exito, no como dato
      faltante -- mismo criterio que _deliver_epic_sequential ya usa para
      "sin veredicto" = bloqueado).
    """
    try:
        TRUST_LOG.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "ticket_id": ticket_id,
            "component": component,
            "change_type": change_type,
            "judge_verdict": (judge_verdict or {}).get("verdict"),
            "verification": verification,
            "retries_used": retries_used,
            "tool_call_failed": tool_call_failed,
        }
        with TRUST_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError as exc:
        logger.warning(f"record_trust_signal: no se pudo escribir en {TRUST_LOG} -- {exc}")


def _recent_signals(component: str, limit: int = _TRUST_WINDOW) -> list:
    if not TRUST_LOG.exists():
        return []
    signals = []
    with TRUST_LOG.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if entry.get("component") == component:
                signals.append(entry)
    return signals[-limit:]


def compute_trust_score(component: str) -> float:
    """Sin corridas previas para este componente: 0.5 (prior neutral, tier
    Unproven en el policy engine -- ver decide_policy en orchestration.py).
    Nunca lanza -- un log corrupto/inaccesible degrada al mismo prior neutral,
    mismo criterio best-effort que el resto de este modulo.
    """
    signals = _recent_signals(component)
    if not signals:
        return 0.5

    success_rate = sum(1 for s in signals if s.get("judge_verdict") == "OK") / len(signals)

    def _verification_clean(s: dict) -> bool:
        v = s.get("verification") or {}
        return bool(v.get("output_guard_clean")) and bool(v.get("tests_passed")) and not v.get("new_sonar_issues")

    verification_pass_rate = sum(1 for s in signals if _verification_clean(s)) / len(signals)
    retry_rate = sum(1 for s in signals if (s.get("retries_used") or 0) > 0) / len(signals)
    coverage_rate = sum(1 for s in signals if (s.get("verification") or {}).get("tests_adequate")) / len(signals)
    incident_rate = sum(1 for s in signals if (s.get("verification") or {}).get("falco_correlated")) / len(signals)
    tool_fail_rate = sum(1 for s in signals if s.get("tool_call_failed")) / len(signals)

    score = (
        _WEIGHT_SUCCESS_RATE * success_rate
        + _WEIGHT_VERIFICATION_PASS_RATE * verification_pass_rate
        + _WEIGHT_RETRY * (1 - retry_rate)
        + _WEIGHT_ROLLBACK * _ROLLBACK_AVAILABILITY
        + _WEIGHT_COVERAGE * coverage_rate
        + _WEIGHT_INCIDENT * (1 - incident_rate)
        + _WEIGHT_TOOL_RELIABILITY * (1 - tool_fail_rate)
    )
    return max(0.0, min(1.0, score))


def consecutive_ok_runs(component: str) -> int:
    """Cuenta desde el registro MAS RECIENTE hacia atras hasta el primer
    verdict no-OK -- usado por decide_policy() para el umbral de AUTO_SHIP
    (20 corridas limpias seguidas, ver el artefacto "Autonomy Is Earned" §07)."""
    signals = _recent_signals(component, limit=_TRUST_WINDOW)
    count = 0
    for s in reversed(signals):
        if s.get("judge_verdict") != "OK":
            break
        count += 1
    return count
