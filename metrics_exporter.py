"""Observability exporter ("Autonomy Is Earned", pedido explicito del
usuario) -- expone /metrics en formato Prometheus para los 4 logs JSONL
append-only que este pipeline ya escribe (logs/judge_verdicts.jsonl,
logs/coding_agent_runs.jsonl, logs/firewall_audit.jsonl,
logs/trust_scores.jsonl), sin depender de Loki/Promtail.

A diferencia de firewall_proxy.py (que mantiene sus contadores EN MEMORIA,
actualizados en vivo -- tiene sentido ahi porque es un servidor HTTP de
vida larga), este modulo relee los JSONL desde cero en cada scrape:
judge_agent.py/coding_agent.py/trust_score.py son subprocesos de una sola
corrida, no procesos de larga vida que puedan mantener contadores propios,
asi que el archivo en disco ES la unica fuente de verdad real.

Uso: uvicorn metrics_exporter:app --host 0.0.0.0 --port 9105
"""
import json
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest

from log_utils import get_logger

app = FastAPI(title="poc-ai-agents metrics exporter")
logger = get_logger(__name__)

LOG_DIR = Path("/app/logs") if Path("/app").exists() else Path(__file__).resolve().parent / "logs"
JUDGE_LOG = LOG_DIR / "judge_verdicts.jsonl"
CODING_AGENT_LOG = LOG_DIR / "coding_agent_runs.jsonl"
FIREWALL_LOG = LOG_DIR / "firewall_audit.jsonl"
TRUST_LOG = LOG_DIR / "trust_scores.jsonl"


def _read_jsonl(path: Path) -> list:
    """Best-effort: un log ausente/corrupto no debe tumbar /metrics -- mismo
    criterio que trust_score.py usa para leer su propio log."""
    if not path.exists():
        return []
    entries = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def _component_trust_scores(trust_entries: list) -> dict:
    """Ultimas _TRUST_WINDOW senales por componente, misma formula EXACTA
    que trust_score.compute_trust_score (via _score_from_signals, una sola
    fuente de verdad) -- separado de esa funcion porque ella esta pensada
    para un solo componente por llamada (relee el archivo cada vez), y esto
    necesita TODOS los componentes de una sola lectura por scrape."""
    from trust_score import _TRUST_WINDOW, _score_from_signals

    by_component: dict = {}
    for entry in trust_entries:
        component = entry.get("component")
        if not component:
            continue
        by_component.setdefault(component, []).append(entry)

    return {
        component: _score_from_signals(signals[-_TRUST_WINDOW:])
        for component, signals in by_component.items()
        if signals
    }


def render_metrics() -> bytes:
    """Separado de la ruta FastAPI para poder testearlo sin un TestClient
    real -- arma un registry NUEVO en cada llamada (evita el problema real
    de series duplicadas si este modulo se recarga en el mismo proceso, ver
    firewall_proxy.py:_get_or_create_counter para el mismo gap ya resuelto
    ahi de otra forma)."""
    registry = CollectorRegistry()

    judge_entries = _read_jsonl(JUDGE_LOG)
    judge_verdicts_total = Gauge(
        "judge_verdicts_total", "Veredictos reales del juez, por resultado", ["verdict"], registry=registry
    )
    verdict_counts: dict = {}
    for entry in judge_entries:
        verdict_counts[entry.get("verdict", "UNKNOWN")] = verdict_counts.get(entry.get("verdict", "UNKNOWN"), 0) + 1
    for verdict, count in verdict_counts.items():
        judge_verdicts_total.labels(verdict=verdict).set(count)

    coding_agent_entries = _read_jsonl(CODING_AGENT_LOG)
    coding_agent_runs_total = Gauge(
        "coding_agent_runs_total", "Corridas reales del coding agent, por status", ["status"], registry=registry
    )
    status_counts: dict = {}
    for entry in coding_agent_entries:
        status_counts[entry.get("status", "unknown")] = status_counts.get(entry.get("status", "unknown"), 0) + 1
    for status, count in status_counts.items():
        coding_agent_runs_total.labels(status=status).set(count)

    firewall_entries = _read_jsonl(FIREWALL_LOG)
    firewall_redactions_total = Gauge(
        "firewall_redactions_total_from_audit_log",
        "Redacciones reales del AI Firewall (leido de firewall_audit.jsonl -- ver tambien /metrics del propio firewall)",
        registry=registry,
    )
    firewall_redactions_total.set(sum(e.get("redactions_applied", 0) for e in firewall_entries))

    trust_entries = _read_jsonl(TRUST_LOG)
    trust_score_gauge = Gauge(
        "trust_score", "Trust score real por componente (Autonomy Is Earned)", ["component"], registry=registry
    )
    for component, score in _component_trust_scores(trust_entries).items():
        trust_score_gauge.labels(component=component).set(score)

    return generate_latest(registry)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(render_metrics(), media_type=CONTENT_TYPE_LATEST)
