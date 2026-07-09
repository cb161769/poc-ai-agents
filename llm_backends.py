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
backend needs to reimplement that.
"""
import os

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
