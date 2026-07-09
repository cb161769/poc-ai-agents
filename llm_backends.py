"""Registry of LLM backends -- the extension point for adding a new model
provider (OpenAI, Gemini, etc.) without editing agent_loop.py's call sites.

Today only "anthropic" and "ollama" are registered (the only two backends
actually implemented and exercised end-to-end, consistent with this
project's no-mocks philosophy: a backend only gets added here once it's
been wired up and tested against the real provider, not speculatively).
Adding a third backend means adding a new entry to BACKEND_PRIORITY_DEFAULT
and a pricing entry here, plus the actual request-building branch in
agent_loop.py::_call_model_turn() -- this module doesn't own the HTTP
request shape (that stays in agent_loop.py, which already normalizes every
backend's response to the same Anthropic-shaped content blocks), it owns
backend *selection order* and *pricing*, the two things that were
previously hardcoded as a fixed if/elif and a single-provider constant.

Every backend registered here automatically gets agent_loop.py's shared
JSON-correction retry (_final_text_with_json_retry) for free -- a model
that returns malformed JSON on its final answer gets one bounded retry
with a correction message, regardless of which backend answered. No
backend needs to reimplement that. It also gets live fallback
(agent_loop.py::call_with_fallback) for free: if the backend fails after
exhausting its own RETRY_POLICY_PER_BACKEND, the next backend in
get_backend_priority() picks up the same turn instead of killing the run.
"""
import json
import os
import time
from pathlib import Path

# Orden de preferencia por defecto -- Anthropic primero (mejor calidad),
# Ollama como fallback gratuito/local. Configurable via LLM_BACKEND_PRIORITY
# ("anthropic,ollama" por defecto, mismo comportamiento que antes de este
# refactor) para poder reordenar sin tocar codigo.
BACKEND_PRIORITY_DEFAULT = ["anthropic", "ollama"]


def get_backend_priority() -> list:
    raw = os.environ.get("LLM_BACKEND_PRIORITY", "")
    if not raw.strip():
        return list(BACKEND_PRIORITY_DEFAULT)
    return [name.strip() for name in raw.split(",") if name.strip()]


# Precios aproximados por millon de tokens (USD), por backend -- solo para
# estimar costo en evals/logs, no son precios contractuales. Ollama es
# gratis/local (tabla vacia -> _estimate_cost_usd siempre da 0.0 para
# cualquier modelo de ese backend).
PRICING_PER_MILLION = {
    "anthropic": {
        "claude-sonnet-5": {"input": 3.0, "output": 15.0},
        "claude-opus-4-8": {"input": 15.0, "output": 75.0},
        "claude-haiku-4-5-20251001": {"input": 0.8, "output": 4.0},
    },
    "ollama": {},
}


def estimate_cost_usd(backend: str, model: str, input_tokens: int, output_tokens: int) -> float:
    pricing = PRICING_PER_MILLION.get(backend, {}).get(model)
    if not pricing:
        return 0.0
    return (input_tokens * pricing["input"] + output_tokens * pricing["output"]) / 1_000_000


# Politica de retry transitorio por backend -- antes era un solo set global
# en agent_loop.py que no incluia el 529 ("overloaded") especifico de
# Anthropic, asi que ni el unico backend que existia se reintentaba bien
# ante ese caso real. Cada backend nuevo trae su propia entrada aca en vez
# de heredar una lista generica que puede no aplicarle.
RETRY_POLICY_PER_BACKEND = {
    "anthropic": {"retryable_status_codes": {429, 500, 502, 503, 504, 529}, "max_retries": 2, "backoff_seconds": [1, 2]},
    "ollama": {"retryable_status_codes": {429, 500, 502, 503, 504}, "max_retries": 2, "backoff_seconds": [1, 2]},
}

# max_tokens por backend/modelo -- antes hardcodeado (1536) solo en la rama
# Anthropic de _call_model_turn, sin equivalente para Ollama. NO incluye
# chequeo real de ventana de contexto (requeriria un tokenizer por
# proveedor) -- es una limitacion conocida, no una validacion que finge
# existir.
MODEL_LIMITS = {
    "anthropic": {"max_tokens": 1536},
    "ollama": {"max_tokens": 1536},
}

_LOG_DIR = Path(__file__).resolve().parent / "logs"
_SPEND_LOG_FILES = ["judge_verdicts.jsonl", "coding_agent_runs.jsonl"]


def spend_today(backend: str) -> float:
    """Suma estimated_cost_usd de las entradas de HOY (fecha UTC) para ese
    backend, leyendo los logs que judge_agent.py/coding_agent.py YA
    escriben (logs/judge_verdicts.jsonl, logs/coding_agent_runs.jsonl) --
    no crea un store de gasto nuevo, reusa la auditoria que ya existe.
    Best-effort: un log ausente o una linea corrupta no rompe el calculo,
    simplemente no suma esa entrada.
    """
    today = time.strftime("%Y-%m-%d", time.gmtime())
    total = 0.0
    for filename in _SPEND_LOG_FILES:
        path = _LOG_DIR / filename
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if entry.get("backend") != backend:
                continue
            if not str(entry.get("ts", "")).startswith(today):
                continue
            total += entry.get("estimated_cost_usd", 0) or 0
    return total


def is_within_budget(backend: str) -> bool:
    """True si no hay LLM_DAILY_BUDGET_USD seteada (sin limite, default), o
    si el gasto de hoy para este backend todavia no lo alcanzo.
    """
    budget_raw = os.environ.get("LLM_DAILY_BUDGET_USD", "")
    if not budget_raw.strip():
        return True
    try:
        budget = float(budget_raw)
    except ValueError:
        return True
    return spend_today(backend) < budget
