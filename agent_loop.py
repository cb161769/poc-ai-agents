"""Shared dual-backend (Anthropic / local Ollama), MCP-capable tool-calling
machinery -- used by judge_agent.py (independent auditor) and coding_agent.py
(the real local coding agent, Camino B1 of run_poc_loop.sh/orchestration.py).

Neither caller reimplements backend switching, message-format adapters, or
MCP plumbing: they each bring their own system prompt, their own tools
(MCP and/or local), and their own loop-termination logic (a judge verdict
vs. a coding agent's done/blocked status), and call _call_model_turn() /
_connect_mcp_servers() / _call_mcp_tool() from here.

Model backend, in order of preference (same for both callers):
  1. Anthropic API (ANTHROPIC_API_KEY set) — best quality.
  2. Local Ollama container (OLLAMA_URL reachable) — free, offline fallback
     with a tool-calling capable model (OLLAMA_MODEL, default "llama3.1").
  3. Neither available — caller decides what "none" means for it (the judge
     skips the run; the coding agent falls back to gh copilot suggest).
"""
import asyncio
import json
import os
from contextlib import AsyncExitStack

import httpx
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from llm_backends import (
    MODEL_LIMITS,
    RETRY_POLICY_PER_BACKEND,
    estimate_cost_usd,
    get_backend_priority,
    is_within_budget,
)
from log_utils import get_logger

load_dotenv()

logger = get_logger(__name__)

JSON_CORRECTION_MESSAGE = (
    "Tu respuesta anterior no fue JSON valido. Respondé de nuevo usando "
    "UNICAMENTE el JSON exacto pedido, sin texto antes ni despues."
)

ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1")
# CPU-only inference con el system prompt grande de coding_agent.py/
# judge_agent.py puede tardar varios minutos -- confirmado en corrida real
# (el juez supero 120s y crasheo el subproceso entero via ReadTimeout antes
# de este cambio). Configurable si tu hardware es mas lento/rapido.
OLLAMA_TIMEOUT_SECONDS = float(os.environ.get("OLLAMA_TIMEOUT_SECONDS", "300"))

# Pricing y orden de preferencia viven en llm_backends.py (el registro de
# backends) -- _estimate_cost_usd() se mantiene aca como wrapper fino para
# que coding_agent.py/judge_agent.py no tengan que cambiar su import.
def _estimate_cost_usd(backend: str, model: str, input_tokens: int, output_tokens: int) -> float:
    return estimate_cost_usd(backend, model, input_tokens, output_tokens)


def _backend_available(backend: str) -> bool:
    """Chequeo real de si este backend puede atender una llamada ahora:
    credenciales/alcanzabilidad (mismo criterio que _select_backend ya
    usaba) MAS presupuesto diario si LLM_DAILY_BUDGET_USD esta seteada
    (llm_backends.is_within_budget -- sin la env var, siempre True).
    """
    reachable = False
    if backend == "anthropic":
        reachable = bool(os.environ.get("ANTHROPIC_API_KEY"))
    elif backend == "ollama":
        try:
            httpx.get(f"{OLLAMA_URL}/api/tags", timeout=3.0)
            reachable = True
        except httpx.HTTPError:
            reachable = False
    if not reachable:
        return False
    return is_within_budget(backend)


def _select_backend() -> str:
    """Recorre get_backend_priority() (default: Anthropic primero, Ollama
    como fallback gratuito/local si esta alcanzable; configurable via
    LLM_BACKEND_PRIORITY) y devuelve el primero disponible (alcanzable Y
    dentro de presupuesto), o "none" si ninguno lo esta -- el caller decide
    que significa eso para el.
    """
    for backend in get_backend_priority():
        if _backend_available(backend):
            return backend
    return "none"


def _tools_to_ollama_format(tools: list) -> list:
    return [
        {
            "type": "function",
            "function": {"name": t["name"], "description": t["description"], "parameters": t["input_schema"]},
        }
        for t in tools
    ]


def _messages_to_ollama(messages: list) -> list:
    """Flattens our Anthropic-shaped message history (content = string, or a
    list of text/tool_use/tool_result blocks) into Ollama's chat format.
    """
    out = []
    for m in messages:
        content = m["content"]
        if isinstance(content, str):
            out.append({"role": m["role"], "content": content})
            continue

        if m["role"] == "assistant":
            text_parts = [b["text"] for b in content if b.get("type") == "text"]
            tool_calls = [
                {"function": {"name": b["name"], "arguments": b.get("input", {})}}
                for b in content
                if b.get("type") == "tool_use"
            ]
            msg = {"role": "assistant", "content": "\n".join(text_parts)}
            if tool_calls:
                msg["tool_calls"] = tool_calls
            out.append(msg)
        else:
            for b in content:
                out.append({"role": "tool", "content": b.get("content", "")})
    return out


def _ollama_response_to_blocks(message: dict) -> tuple:
    blocks = []
    text = message.get("content") or ""
    if text:
        blocks.append({"type": "text", "text": text})

    tool_calls = message.get("tool_calls") or []
    for i, tc in enumerate(tool_calls):
        fn = tc.get("function", {})
        arguments = fn.get("arguments", {})
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                arguments = {}
        blocks.append({"type": "tool_use", "id": f"ollama_call_{i}", "name": fn.get("name"), "input": arguments})

    stop_reason = "tool_use" if tool_calls else "end_turn"
    return blocks, stop_reason


async def _post_with_retry(client: httpx.AsyncClient, backend: str, url: str, **kwargs) -> httpx.Response:
    """POSTs with a bounded retry for transient failures, using the retry
    policy for this specific backend (llm_backends.RETRY_POLICY_PER_BACKEND)
    instead of one set of constants shared by every backend -- e.g. only
    Anthropic's policy includes 529 ("overloaded"), which doesn't apply to
    Ollama. On the final attempt, a still-retryable status code just falls
    through to raise_for_status() so the caller gets the real error, not a
    synthetic one.
    """
    policy = RETRY_POLICY_PER_BACKEND[backend]
    max_retries = policy["max_retries"]
    backoff = policy["backoff_seconds"]
    retryable_status_codes = policy["retryable_status_codes"]

    for attempt in range(max_retries + 1):
        try:
            resp = await client.post(url, **kwargs)
        except (httpx.TimeoutException, httpx.ConnectError):
            if attempt < max_retries:
                await asyncio.sleep(backoff[attempt])
                continue
            raise

        if resp.status_code in retryable_status_codes and attempt < max_retries:
            await asyncio.sleep(backoff[attempt])
            continue

        resp.raise_for_status()
        return resp


async def _call_model_turn(
    client: httpx.AsyncClient, backend: str, messages: list, tools: list, system_prompt: str,
    anthropic_model: str = None, ollama_model: str = None,
) -> tuple:
    """Returns (content_blocks, stop_reason, usage) normalized to the
    Anthropic content-block shape regardless of which backend answered.
    usage = {"input_tokens": int, "output_tokens": int} (0s for Ollama,
    which is free/local so cost tracking doesn't apply the same way).

    anthropic_model/ollama_model: override por-llamada (cada agente puede
    pedir su propio modelo -- ej. CODING_AGENT_OLLAMA_MODEL vs
    JUDGE_OLLAMA_MODEL) -- caen a las constantes globales si no se pasan,
    asi que un caller que no los usa no nota diferencia.
    """
    anthropic_model = anthropic_model or ANTHROPIC_MODEL
    ollama_model = ollama_model or OLLAMA_MODEL

    if backend == "anthropic":
        # Prompt caching: system_prompt y tools son estaticos dentro de una
        # corrida (se repiten identicos en cada uno de los hasta
        # MAX_TOOL_TURNS turnos), asi que se marcan como prefijo cacheable
        # -- Anthropic cachea todo hasta el ultimo breakpoint marcado, asi
        # que el cache_control en el ultimo elemento de tools cubre
        # system+tools juntos. Anthropic exige un minimo (~1024 tokens para
        # Sonnet/Opus) para que el cache aplique -- el system prompt solo
        # puede quedar justo debajo de eso en algun agente, pero el bloque
        # combinado con tools normalmente lo supera. La rama ollama no tiene
        # un mecanismo de caching equivalente en la API que ya se usa, asi
        # que queda sin cambios.
        request_body = {
            "model": anthropic_model,
            "max_tokens": MODEL_LIMITS["anthropic"]["max_tokens"],
            "system": [{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}],
            "messages": messages,
        }
        if tools:
            tools_with_cache = [dict(t) for t in tools]
            tools_with_cache[-1] = {**tools_with_cache[-1], "cache_control": {"type": "ephemeral"}}
            request_body["tools"] = tools_with_cache

        resp = await _post_with_retry(
            client,
            "anthropic",
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": os.environ["ANTHROPIC_API_KEY"],
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=request_body,
            timeout=60.0,
        )
        data = resp.json()
        usage = data.get("usage", {})
        return (
            data["content"],
            data.get("stop_reason", "end_turn"),
            {
                "input_tokens": usage.get("input_tokens", 0),
                "output_tokens": usage.get("output_tokens", 0),
                "cache_read_input_tokens": usage.get("cache_read_input_tokens", 0),
                "cache_creation_input_tokens": usage.get("cache_creation_input_tokens", 0),
            },
        )

    if backend == "ollama":
        ollama_messages = [{"role": "system", "content": system_prompt}] + _messages_to_ollama(messages)
        request_body = {
            "model": ollama_model,
            "messages": ollama_messages,
            "stream": False,
            "options": {"num_predict": MODEL_LIMITS["ollama"]["max_tokens"]},
        }
        if tools:
            request_body["tools"] = _tools_to_ollama_format(tools)

        resp = await _post_with_retry(
            client, "ollama", f"{OLLAMA_URL}/api/chat", json=request_body, timeout=OLLAMA_TIMEOUT_SECONDS
        )
        data = resp.json()
        blocks, stop_reason = _ollama_response_to_blocks(data.get("message", {}))
        usage = {"input_tokens": data.get("prompt_eval_count", 0), "output_tokens": data.get("eval_count", 0)}
        return blocks, stop_reason, usage

    raise RuntimeError("ni ANTHROPIC_API_KEY ni un Ollama local disponible")


async def call_with_fallback(
    client: httpx.AsyncClient, messages: list, tools: list, system_prompt: str, exclude: set = None,
    anthropic_model: str = None, ollama_model: str = None,
) -> tuple:
    """Fallback EN VIVO entre backends -- a diferencia de _select_backend()
    (que elige un backend una sola vez al arrancar la corrida), esto se
    llama en CADA turno: si el backend actual falla de verdad (agoto sus
    propios reintentos via _post_with_retry), prueba el siguiente backend
    disponible de get_backend_priority() para ESE MISMO turno, en vez de
    matar la corrida entera.

    anthropic_model/ollama_model: mismo override por-agente que
    _call_model_turn -- se reenvian tal cual.

    Devuelve (content_blocks, stop_reason, usage, backend_used) -- un
    elemento mas que _call_model_turn (que backend realmente respondio),
    para que el caller seek al loop de turnos siga usando ese backend en
    el proximo turno en vez de recalcular desde "none".

    Con un solo backend disponible (el caso mas comun), el comportamiento
    es identico a llamar _call_model_turn directo: no hay a donde caer, y
    la excepcion real del unico backend se re-lanza tal cual.
    """
    exclude = exclude or set()
    last_exc = None
    tried_any = False

    for backend in get_backend_priority():
        if backend in exclude or not _backend_available(backend):
            continue
        tried_any = True
        try:
            blocks, stop_reason, usage = await _call_model_turn(
                client, backend, messages, tools, system_prompt,
                anthropic_model=anthropic_model, ollama_model=ollama_model,
            )
            return blocks, stop_reason, usage, backend
        except Exception as exc:
            logger.warning(f"backend '{backend}' fallo, probando el siguiente disponible: {exc}")
            last_exc = exc
            continue

    if last_exc is not None:
        raise last_exc
    if not tried_any:
        raise RuntimeError("ningun backend disponible (ni alcanzable ni dentro de presupuesto)")
    raise RuntimeError("ningun backend pudo atender esta llamada")


async def _final_text_with_json_retry(
    client: httpx.AsyncClient, backend: str, messages: list, tools: list, system_prompt: str,
    anthropic_model: str = None, ollama_model: str = None,
) -> tuple:
    """Called when a model's final answer wasn't valid JSON: appends a
    correction request and makes ONE more model call (bounded, no loop).
    Mutates messages in place (appends the correction request + the retry's
    assistant reply, same as the normal turn loop would). Returns
    (final_text, usage) -- caller decides what to do if this also isn't
    valid JSON.
    """
    messages.append({"role": "user", "content": JSON_CORRECTION_MESSAGE})
    content, _stop_reason, usage = await _call_model_turn(
        client, backend, messages, tools, system_prompt,
        anthropic_model=anthropic_model, ollama_model=ollama_model,
    )
    messages.append({"role": "assistant", "content": content})
    final_text = next((b["text"] for b in content if b.get("type") == "text"), "")
    return final_text, usage


_COMPACTED_TOOL_RESULT_TEMPLATE = (
    "[resultado de '{tool_name}' colapsado por limite de contexto -- volvé a llamarla si lo necesitás de nuevo]"
)


def compact_old_tool_results(messages: list, read_only_tool_names: set, keep_last_n_turns: int = 3) -> None:
    """Cada turno reenvia TODO el historial acumulado -- sin esto, un loop
    largo (coding_agent.py llega a 15 turnos) paga de nuevo, en cada turno,
    el costo completo de cada read_file/grep_search/etc ya hecho. Esto muta
    `messages` in-place: cualquier tool_result que corresponda (por
    tool_use_id) a una tool de SOLO LECTURA en `read_only_tool_names`, y que
    quede a mas de `keep_last_n_turns` turnos de asistente del final, se
    reemplaza por un placeholder corto. Los resultados de tools de
    escritura (write_file/edit_file/run_shell_command) NUNCA se tocan --
    representan efectos reales que el modelo tiene que recordar con
    precision, no contexto descartable.
    """
    assistant_indices = [i for i, m in enumerate(messages) if m.get("role") == "assistant"]
    if len(assistant_indices) <= keep_last_n_turns:
        return

    boundary_index = assistant_indices[-keep_last_n_turns]

    tool_name_by_id = {}
    for i in assistant_indices:
        for block in messages[i].get("content") or []:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                tool_name_by_id[block.get("id")] = block.get("name")

    for i, message in enumerate(messages):
        if i >= boundary_index or message.get("role") != "user":
            continue
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_result":
                continue
            tool_name = tool_name_by_id.get(block.get("tool_use_id"))
            if tool_name in read_only_tool_names:
                block["content"] = _COMPACTED_TOOL_RESULT_TEMPLATE.format(tool_name=tool_name)


async def _connect_mcp_servers(stack: AsyncExitStack, mcp_servers: dict, label: str = "agente") -> dict:
    """Best-effort: a server that's unreachable (uvx missing, Neo4j/Qdrant
    down) is skipped, not fatal — the caller just has fewer tools that run.
    """
    sessions = {}
    for name, params in mcp_servers.items():
        try:
            read, write = await stack.enter_async_context(stdio_client(params))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            sessions[name] = session
        except Exception as exc:
            logger.warning(f"{label}: no se pudo conectar al MCP '{name}', se omite: {exc}")
    return sessions


def _normalize_tool_schema(server_name: str, tools) -> list:
    """Normaliza tools listadas por un servidor MCP al formato interno
    compartido (bloques con "name"/"description"/"input_schema") -- el
    mismo formato que ya usan los bloques text/tool_use/tool_result en
    todo este modulo. Anthropic es uno de los backends que lo consume tal
    cual; Ollama pasa por _tools_to_ollama_format() para adaptarlo -- ambos
    son adaptadores simetricos de este formato neutral, no hay un backend
    "nativo" y otro "adaptado".
    """
    return [
        {
            "name": f"{server_name}__{t.name}",
            "description": t.description or "",
            "input_schema": t.inputSchema,
        }
        for t in tools
    ]


_MCP_TOOL_TIMEOUT_SECONDS = 30


async def _call_mcp_tool(sessions: dict, qualified_name: str, tool_input: dict) -> str:
    """Bounded by a timeout -- if an MCP server (Neo4j/Qdrant) hangs, the
    whole agent loop shouldn't hang with it. Returns an error string on
    timeout instead of raising, matching what callers already do with
    other tool failures (they wrap this in a broad try/except anyway).
    """
    server_name, tool_name = qualified_name.split("__", 1)
    session = sessions[server_name]
    try:
        result = await asyncio.wait_for(session.call_tool(tool_name, tool_input), timeout=_MCP_TOOL_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        return f"error: la herramienta MCP '{qualified_name}' no respondio en {_MCP_TOOL_TIMEOUT_SECONDS}s"
    return "\n".join(getattr(c, "text", str(c)) for c in result.content)
