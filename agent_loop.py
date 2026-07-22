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
import re
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
    "UNICAMENTE el JSON exacto pedido, sin texto antes ni despues. En este "
    "mensaje NO hay ninguna herramienta disponible -- no intentes llamar "
    "ninguna (ni inventes una que no se te ofrecio, como una tool de shell "
    "generica). Si crees que necesitarias una herramienta para responder "
    "con seguridad, decilo explicitamente en el campo de texto que "
    "corresponda (reasoning/summary) en vez de emitir una llamada de "
    "herramienta."
)

ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
# Mismo criterio que OLLAMA_TEMPERATURE: 0.0 (deterministico) para tareas de
# verificacion (coding_agent.py/judge_agent.py esperan JSON estricto en la
# respuesta final), no el default del modelo pensado para charla natural.
ANTHROPIC_TEMPERATURE = float(os.environ.get("ANTHROPIC_TEMPERATURE", "0.0"))
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1")
# CPU-only inference con el system prompt grande de coding_agent.py/
# judge_agent.py puede tardar varios minutos -- confirmado en corrida real
# (el juez supero 120s y crasheo el subproceso entero via ReadTimeout antes
# de este cambio). Configurable si tu hardware es mas lento/rapido.
OLLAMA_TIMEOUT_SECONDS = float(os.environ.get("OLLAMA_TIMEOUT_SECONDS", "300"))
# Ventana de contexto de ENTRADA -- distinto de num_predict (limite de
# tokens de SALIDA, en MODEL_LIMITS). Sin esto, Ollama usa el default del
# modelo (2048-4096 en muchos casos), y con los prompts largos de este
# pipeline (grafo + Sonar + el esquema JSON pedido al final del system
# prompt) la parte final -- justo donde esta el esquema -- se puede estar
# truncando en silencio. Confirmado como sospechoso real esta sesion: con
# Ollama como unico backend (Anthropic sin credito), el patron recurrente
# fue JSON vacio/invalido en las respuestas finales.
OLLAMA_NUM_CTX = int(os.environ.get("OLLAMA_NUM_CTX", "8192"))
# Sin esto, Ollama usa la temperatura default del modelo (tipicamente ~0.7-0.8,
# pensada para charla natural) para TODO -- incluida la respuesta final que
# coding_agent.py/judge_agent.py esperan como JSON estricto. Mas creatividad
# ahi es mas chance de narrar en vez de devolver el esquema pedido, o de
# "inventar" hechos en vez de usar las tools reales para confirmarlos. 0.0
# (deterministico) es el default correcto para tareas de verificacion, no
# de generacion creativa.
OLLAMA_TEMPERATURE = float(os.environ.get("OLLAMA_TEMPERATURE", "0.0"))
# Modelos Ollama documentados con soporte real de "thinking" (razonamiento
# interno antes de responder, expuesto aparte en message.thinking -- no se
# mezcla con message.content/tool_calls, asi que activarlo no rompe el
# parseo existente): qwen3, gpt-oss, deepseek-r1, deepseek-v3.1
# (docs.ollama.com/capabilities/thinking). Activarlo en un modelo que no lo
# soporta no esta documentado como seguro -- se gatea por nombre en vez de
# mandarlo siempre, para no arriesgar una corrida real con un modelo nuevo
# que no fue verificado.
OLLAMA_THINKING_ENABLED = os.environ.get("OLLAMA_THINKING_ENABLED", "true").strip().lower() not in ("0", "false", "no")
_OLLAMA_THINKING_MODEL_PATTERN = re.compile(r"qwen3|gpt-oss|deepseek-r1|deepseek-v3", re.IGNORECASE)


def _ollama_model_supports_thinking(model: str) -> bool:
    return bool(_OLLAMA_THINKING_MODEL_PATTERN.search(model or ""))

# Pricing y orden de preferencia viven en llm_backends.py (el registro de
# backends) -- _estimate_cost_usd() se mantiene aca como wrapper fino para
# que coding_agent.py/judge_agent.py no tengan que cambiar su import.
def _estimate_cost_usd(backend: str, model: str, input_tokens: int, output_tokens: int) -> float:
    return estimate_cost_usd(backend, model, input_tokens, output_tokens)


# Bug real confirmado en vivo (operacion de esta noche): _backend_available
# solo chequea bool(ANTHROPIC_API_KEY) (que la variable este seteada, no que
# sea VALIDA) -- con una key vencida/revocada, call_with_fallback reintenta
# Anthropic en CADA turno, agotando un 401 real cada vez antes de recien
# ahi caer a Ollama (confirmado: el mismo '401 Unauthorized' se repitio en
# casi todos los turnos de dos corridas reales completas). Este set vive a
# nivel de modulo -- cada corrida real de coding_agent.py/judge_agent.py/
# epic_planner.py es su propio subprocess de Python, asi que esto ya scopea
# correctamente "por corrida" sin necesitar limpieza explicita entre
# corridas distintas.
_backends_failed_hard_this_run: set = set()


def _is_hard_auth_failure(exc: Exception) -> bool:
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (401, 403)


def _backend_available(backend: str) -> bool:
    """Chequeo real de si este backend puede atender una llamada ahora:
    credenciales/alcanzabilidad (mismo criterio que _select_backend ya
    usaba) MAS presupuesto diario si LLM_DAILY_BUDGET_USD esta seteada
    (llm_backends.is_within_budget -- sin la env var, siempre True).
    """
    if backend in _backends_failed_hard_this_run:
        return False
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


def parse_ollama_model_candidates(raw_value: str, default: str) -> list:
    """CODING_AGENT_OLLAMA_MODEL=qwen2.5-coder:7b,llama3.1,mistral -- coma-
    separado, orden = prioridad. Sin coma (o sin setear) = comportamiento
    identico a hoy (una lista de un solo elemento). default es el fallback
    si raw_value viene vacio.
    """
    candidates = [m.strip() for m in (raw_value or "").split(",") if m.strip()]
    if not candidates:
        candidates = [default]
    seen = set()
    return [m for m in candidates if not (m in seen or seen.add(m))]


def _pulled_ollama_model_names():
    """None = servidor inalcanzable (distinto de "sin modelos descargados").
    Unico punto que llama GET /api/tags para chequeos de modelo -- evita N
    requests HTTP para N candidatos.
    """
    try:
        resp = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=3.0)
        return {m.get("name") for m in resp.json().get("models", [])}
    except httpx.HTTPError:
        return None


def _ollama_model_available(model: str) -> bool:
    """Best-effort: _backend_available("ollama") ya confirma que el
    servidor responde -- esto ademas confirma que el modelo PEDIDO esta
    realmente descargado. Sin esto, un nombre mal escrito o un modelo
    nunca 'ollama pull'-eado (ej. probando CODING_AGENT_OLLAMA_MODEL/
    JUDGE_OLLAMA_MODEL por primera vez) recien fallaba a mitad de una
    corrida real, sin ningun aviso previo.
    """
    names = _pulled_ollama_model_names()
    if names is None:
        return False
    return model in names or any(n.startswith(f"{model}:") for n in names)


def resolve_ollama_model(candidates: list, exclude: set = None):
    """Primer candidato (en orden de prioridad) que este realmente 'ollama
    pull'-eado y no este en exclude (modelos ya probados/descartados en
    esta corrida). None si el servidor no responde o ningun candidato
    restante esta descargado -- el caller decide como degradar (mismo
    criterio que _ollama_model_available ya usaba: solo un warning, no un
    bloqueo duro, porque el pedido puede fallar mas adelante o el nombre
    puede resultar correcto igual si el listado de /api/tags fallo por
    otra razon).
    """
    names = _pulled_ollama_model_names()
    if names is None:
        return None
    exclude = exclude or set()
    for model in candidates:
        if model in exclude:
            continue
        if model in names or any(n.startswith(f"{model}:") for n in names):
            return model
    return None


def maybe_switch_ollama_model(model_state: dict, backend: str, candidates: list, log, agent_label: str, reason: str) -> bool:
    """Se llama en el punto en que un agente esta por RENDIRSE ante una
    alucinacion (JSON invalido tras su reintento acotado, negativa a tool
    que persiste tras el nudge, veredicto auto-contradictorio que persiste
    tras el nudge). Devuelve True si cambio de modelo (el caller debe
    reintentar el turno con model_state["active"]), False si no hay otro
    candidato disponible o ya se uso el cambio de modelo en esta corrida
    (el caller se rinde como hacia antes de este cambio). Presupuesto: UN
    cambio de modelo por corrida por agente -- mismo criterio de "un
    empujon acotado, nunca loop infinito" que ya rige los nudges de
    re-prompting existentes en cada agente.
    """
    if backend != "ollama" or model_state["switch_used"]:
        return False
    model_state["tried"].add(model_state["active"])
    next_model = resolve_ollama_model(candidates, exclude=model_state["tried"])
    if next_model is None:
        return False
    log.warning(
        f"{agent_label}: el modelo '{model_state['active']}' alucino ({reason}) -- "
        f"cambiando a '{next_model}' (candidatos: {candidates})"
    )
    model_state["active"] = next_model
    model_state["switch_used"] = True
    return True


def init_ollama_model_state(candidates: list, log, agent_label: str) -> dict:
    """Resuelve el modelo activo inicial de la lista de candidatos (el
    primero realmente descargado) y arma el dict de estado que
    maybe_switch_ollama_model() espera. Si ningun candidato aparece en
    'ollama list' (servidor caido, o ninguno pulled todavia), cae al
    primero de la lista igual -- mismo comportamiento best-effort que ya
    tenia _ollama_model_available: se deja que la corrida real falle mas
    adelante con un error concreto, en vez de bloquear aca sobre un
    chequeo que puede tener falsos negativos.
    """
    active = resolve_ollama_model(candidates)
    if active is None:
        active = candidates[0]
        log.warning(
            f"{agent_label}: ninguno de los modelos candidatos {candidates} aparece en 'ollama list' -- "
            "probablemente falta 'ollama pull', la corrida va a fallar mas adelante si es asi."
        )
    return {"active": active, "tried": set(), "switch_used": False}


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


def _text_as_fallback_tool_call(text: str, offered_tool_names: set) -> dict | None:
    """Algunos modelos locales (confirmado con qwen2.5-coder:7b contra
    Ollama real) nunca completan message.tool_calls -- en vez de eso,
    escriben la llamada como JSON plano en el campo content, con forma
    {"name": "...", "arguments": {...}}. Sin este fallback, agent_loop trata
    ese texto como la respuesta final del modelo (no una tool-call real):
    coding_agent.py/judge_agent.py esperan un esquema distinto ahi
    ({"status":...}/{"verdict":...}), el parseo de JSON falla, y la corrida
    entera termina en "blocked" sin haber intentado ni una tool.

    Sin tools ofrecidas (offered_tool_names vacio -- el caso real de
    _final_text_with_json_retry, que pasa tools=[] a proposito porque pide
    texto corregido, no otra tool-call) NINGUNA forma se reconoce como
    tool-call: no hay ninguna tool real que se pudiera estar invocando, asi
    que cualquier JSON con esta forma es, como mucho, una alucinacion sobre
    una tool inexistente (confirmado real: el juez, con parable/fable,
    devolvio {"tool": "Bash", ...} en el reintento aunque nunca se le
    ofrecio "Bash") -- debe tratarse como texto final (y fallar la
    validacion de forma explicita, no silenciosa) igual que hoy.
    """
    if not offered_tool_names:
        return None

    stripped = text.strip()
    if not (stripped.startswith("{") and stripped.endswith("}")):
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None

    name = parsed.get("name")
    arguments_key = "arguments"
    if not (isinstance(name, str) and "arguments" in parsed):
        # Confirmado real (juez, KAN-5, parable/fable): otro modelo narra su
        # intento de tool-call con claves distintas -- {"tool": "...",
        # "input": {...}} en vez de {"name": ..., "arguments": ...}. Mismo
        # problema (message.tool_calls nunca se completa), forma distinta.
        name = parsed.get("tool")
        arguments_key = "input"
        if not (isinstance(name, str) and arguments_key in parsed):
            return None

    if name not in offered_tool_names:
        return None
    arguments = parsed.get(arguments_key)
    return {"name": name, "arguments": arguments if isinstance(arguments, dict) else {}}


def _ollama_response_to_blocks(message: dict, offered_tool_names: set | None = None) -> tuple:
    blocks = []
    text = message.get("content") or ""

    tool_calls = list(message.get("tool_calls") or [])
    fallback_call = None
    if not tool_calls and text:
        fallback_call = _text_as_fallback_tool_call(text, offered_tool_names or set())

    if text and fallback_call is None:
        blocks.append({"type": "text", "text": text})

    for i, tc in enumerate(tool_calls):
        fn = tc.get("function", {})
        arguments = fn.get("arguments", {})
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                # Antes esto caia a {} en silencio -- una tool real
                # (write_file/edit_file) terminaba llamada con argumentos
                # vacios sin ningun rastro de que el parseo fallo.
                logger.warning(
                    f"ollama: argumentos de tool-call '{fn.get('name')}' no son JSON valido, "
                    f"se llama con {{}} -- crudo: {arguments[:300]!r}"
                )
                arguments = {}
        blocks.append({"type": "tool_use", "id": f"ollama_call_{i}", "name": fn.get("name"), "input": arguments})

    if fallback_call is not None:
        logger.warning(
            f"ollama: '{fallback_call['name']}' llegó como JSON plano en content en vez de "
            "message.tool_calls -- usando fallback en vez de tratarlo como respuesta final."
        )
        blocks.append({"type": "tool_use", "id": "ollama_call_fallback_0", "name": fallback_call["name"], "input": fallback_call["arguments"]})

    stop_reason = "tool_use" if (tool_calls or fallback_call is not None) else "end_turn"
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
    anthropic_model: str = None, ollama_model: str = None, force_json: bool = False,
    json_schema: dict | None = None,
) -> tuple:
    """Returns (content_blocks, stop_reason, usage) normalized to the
    Anthropic content-block shape regardless of which backend answered.
    usage = {"input_tokens": int, "output_tokens": int} (0s for Ollama,
    which is free/local so cost tracking doesn't apply the same way).

    anthropic_model/ollama_model: override por-llamada (cada agente puede
    pedir su propio modelo -- ej. CODING_AGENT_OLLAMA_MODEL vs
    JUDGE_OLLAMA_MODEL) -- caen a las constantes globales si no se pasan,
    asi que un caller que no los usa no nota diferencia.

    force_json: en Ollama fuerza la decodificacion a JSON valido por
    gramatica (parametro real de /api/chat), SOLO cuando no hay tools
    ofrecidas -- forzar JSON en un turno donde tambien se ofrecen tools
    empuja al modelo a responder ya en texto en vez de usar el mecanismo
    real de tool-calling (confirmado real esta sesion con qwen2.5-coder:7b
    y ornith:9b). Mismo criterio en Anthropic: se usa la tecnica de
    "prefill" documentada por Anthropic (arrancar la respuesta del
    asistente con "{"), tambien SOLO cuando no hay tools ofrecidas
    (prefillear texto le impide a Claude emitir un tool_use en ese turno).
    Usado siempre por _final_text_with_json_retry() (que ya pasa
    tools=[]) -- y opcionalmente por el loop principal de
    coding_agent.py/judge_agent.py desde el primer turno (sin efecto
    mientras el loop siga ofreciendo tools reales; empieza a aplicar recien
    en el turno final, sin tools, donde se espera la respuesta JSON).
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
        # force_json en Anthropic: no existe un parametro nativo tipo
        # format:"json" de Ollama -- la tecnica real y documentada por
        # Anthropic es "prefill": arrancar la respuesta del asistente con
        # "{" para que el modelo SOLO pueda continuar como JSON. Solo se
        # aplica cuando no hay tools ofrecidas (prefillear texto le impide
        # a Claude emitir un bloque tool_use en ese turno -- coherente con
        # como ya se usa esto hoy, siempre con tools=[] desde
        # _final_text_with_json_retry o el turno final sin mas tools).
        anthropic_messages = messages
        use_prefill = force_json and not tools
        if use_prefill:
            anthropic_messages = messages + [{"role": "assistant", "content": "{"}]

        request_body = {
            "model": anthropic_model,
            "max_tokens": MODEL_LIMITS["anthropic"]["max_tokens"],
            "temperature": ANTHROPIC_TEMPERATURE,
            "system": [{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}],
            "messages": anthropic_messages,
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
        content = data["content"]
        stop_reason = data.get("stop_reason", "end_turn")
        if use_prefill and content and content[0].get("type") == "text":
            # La respuesta de Anthropic continua DESDE el prefill, no lo
            # repite -- hay que reponer el "{" para que el JSON quede
            # completo y parseable.
            content = [{**content[0], "text": "{" + content[0]["text"]}] + content[1:]

        if tools and stop_reason != "tool_use":
            # Mismo chequeo que ya existe para Ollama: se ofrecieron tools
            # reales y el modelo no las uso, y lo que devolvio tampoco es
            # JSON valido -- ni tool-call ni respuesta final utilizable.
            raw_text = next((b.get("text", "") for b in content if b.get("type") == "text"), "")
            try:
                json.loads(raw_text.strip())
            except (json.JSONDecodeError, ValueError):
                logger.warning(
                    f"anthropic ({anthropic_model}): se ofrecieron {len(tools)} tool(s) y no se uso "
                    "ninguna, y la respuesta tampoco es JSON valido -- posible alucinacion sin verificar con tools."
                )

        return (
            content,
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
            "options": {
                "num_predict": MODEL_LIMITS["ollama"]["max_tokens"],
                "num_ctx": OLLAMA_NUM_CTX,
                "temperature": OLLAMA_TEMPERATURE,
            },
        }
        if tools:
            request_body["tools"] = _tools_to_ollama_format(tools)
        # Mismo criterio que use_prefill en la rama Anthropic: forzar
        # format:"json" cuando TAMBIEN se ofrecen tools empuja al modelo a
        # "contestar ya" en JSON de texto en vez de usar el mecanismo real
        # de tool-calling (message.tool_calls) -- confirmado real esta
        # sesion: con format:"json" siempre activo desde el primer turno,
        # tanto qwen2.5-coder:7b como ornith:9b devolvian el tool-call (o un
        # JSON con el esquema equivocado) como texto plano en vez de
        # investigar primero. Solo forzar JSON cuando NO hay tools que
        # llamar (la respuesta final, o el reintento de correccion que
        # siempre pasa tools=[]).
        if force_json and not tools:
            # Real: format:"json" solo garantiza JSON valido, CUALQUIERA --
            # no el esquema que coding_agent.py/judge_agent.py realmente
            # esperan. Confirmado real (epica KAN-4): el reintento de
            # correccion producia JSON sintacticamente valido pero con
            # status:"blocked" citando su propia confusion, o directamente
            # con las claves equivocadas. Ollama soporta pasarle un JSON
            # Schema real a "format" (no solo el string "json"), que
            # restringe el decoding a ESE esquema exacto -- mucho mas fuerte
            # que "algo de JSON" (docs.ollama.com/capabilities/structured-outputs).
            # Los callers que conocen su esquema exacto lo pasan via
            # json_schema; sin eso, se mantiene el comportamiento anterior.
            request_body["format"] = json_schema if json_schema is not None else "json"
        if OLLAMA_THINKING_ENABLED and _ollama_model_supports_thinking(ollama_model):
            # "thinking" es un campo de respuesta APARTE de content/tool_calls
            # (docs.ollama.com/capabilities/thinking) -- activarlo no cambia
            # el parseo existente de _ollama_response_to_blocks(), solo le da
            # al modelo espacio real para razonar antes de decidir "llamo una
            # tool" vs "respondo ya", que es exactamente el paso donde
            # qwen2.5-coder:7b/ornith:9b se confundian mas seguido.
            request_body["think"] = True

        resp = await _post_with_retry(
            client, "ollama", f"{OLLAMA_URL}/api/chat", json=request_body, timeout=OLLAMA_TIMEOUT_SECONDS
        )
        data = resp.json()
        offered_tool_names = {t["name"] for t in tools} if tools else set()
        blocks, stop_reason = _ollama_response_to_blocks(data.get("message", {}), offered_tool_names)
        usage = {"input_tokens": data.get("prompt_eval_count", 0), "output_tokens": data.get("eval_count", 0)}

        if offered_tool_names and stop_reason == "end_turn":
            raw_text = next((b["text"] for b in blocks if b.get("type") == "text"), "")
            try:
                json.loads(raw_text.strip())
                looks_like_valid_answer = True
            except (json.JSONDecodeError, ValueError):
                looks_like_valid_answer = False
            if not looks_like_valid_answer:
                # Se le ofrecieron tools reales (para investigar antes de
                # actuar) y no las uso, Y lo que devolvio tampoco es un JSON
                # valido -- ni tool-call ni respuesta final utilizable. Senal
                # real de que el modelo esta "alucinando" en vez de verificar
                # con las herramientas disponibles.
                logger.warning(
                    f"ollama ({ollama_model}): se ofrecieron {len(offered_tool_names)} tool(s) y no se uso "
                    "ninguna, y la respuesta tampoco es JSON valido -- posible alucinacion sin verificar con tools."
                )
                # Gap real confirmado en vivo (epica KAN-4, qwen3:8b): este
                # patron se repitio 11 veces seguidas en una conversacion
                # continuada entre historias -- el campo "thinking" (cuando
                # el modelo lo soporta) llega en la respuesta pero antes
                # nunca se inspeccionaba ni se logueaba, asi que no habia
                # forma de saber QUE estaba "pensando" el modelo en el
                # momento de fabricar la respuesta sin reproducirlo a
                # ciegas. Se trunca (no es para auditoria estructurada,
                # solo diagnostico puntual de un incidente real).
                thinking = (data.get("message", {}) or {}).get("thinking")
                if thinking:
                    logger.warning(f"ollama ({ollama_model}): contenido de 'thinking' en ese turno: {thinking[:500]!r}")

        return blocks, stop_reason, usage

    raise RuntimeError("ni ANTHROPIC_API_KEY ni un Ollama local disponible")


async def call_with_fallback(
    client: httpx.AsyncClient, messages: list, tools: list, system_prompt: str, exclude: set = None,
    anthropic_model: str = None, ollama_model: str = None, force_json: bool = False,
    json_schema: dict | None = None,
) -> tuple:
    """Fallback EN VIVO entre backends -- a diferencia de _select_backend()
    (que elige un backend una sola vez al arrancar la corrida), esto se
    llama en CADA turno: si el backend actual falla de verdad (agoto sus
    propios reintentos via _post_with_retry), prueba el siguiente backend
    disponible de get_backend_priority() para ESE MISMO turno, en vez de
    matar la corrida entera.

    anthropic_model/ollama_model: mismo override por-agente que
    _call_model_turn -- se reenvian tal cual. force_json: idem (ver
    docstring de _call_model_turn) -- coding_agent.py/judge_agent.py lo
    pasan en True incluso en el turno inicial (no solo en el reintento de
    correccion), porque ambos esperan JSON estricto en la respuesta final y
    dejarlo sin forzar hasta el reintento le da al modelo una vuelta libre
    para responder en texto narrativo (o alucinar) antes de corregirse.

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
                anthropic_model=anthropic_model, ollama_model=ollama_model, force_json=force_json,
                json_schema=json_schema,
            )
            return blocks, stop_reason, usage, backend
        except Exception as exc:
            if _is_hard_auth_failure(exc):
                # No-transitorio -- reintentar este backend en el PROXIMO
                # turno de la misma corrida solo repetiria el mismo 401/403
                # (confirmado real: se repitio en casi todos los turnos de
                # dos corridas completas). Se recuerda para el resto de esta
                # corrida en vez de re-probarlo turno a turno.
                _backends_failed_hard_this_run.add(backend)
                logger.warning(f"backend '{backend}' fallo con auth invalida ({exc}) -- se descarta para el resto de esta corrida.")
            else:
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
    anthropic_model: str = None, ollama_model: str = None, json_schema: dict | None = None,
) -> tuple:
    """Called when a model's final answer wasn't valid JSON: appends a
    correction request and makes ONE more model call (bounded, no loop).
    Mutates messages in place (appends the correction request + the retry's
    assistant reply, same as the normal turn loop would). Returns
    (final_text, usage) -- caller decides what to do if this also isn't
    valid JSON.
    """
    messages.append({"role": "user", "content": JSON_CORRECTION_MESSAGE})
    # Ya sabemos que el texto anterior no fue JSON valido -- acá le pedimos
    # texto corregido, no otra tool-call, asi que no se reenvian los tools
    # originales; force_json fuerza la gramatica JSON en Ollama (ignorado en
    # Anthropic, ver docstring de _call_model_turn).
    content, _stop_reason, usage = await _call_model_turn(
        client, backend, messages, [], system_prompt,
        anthropic_model=anthropic_model, ollama_model=ollama_model, force_json=True,
        json_schema=json_schema,
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


# Gap real (usuario, "hay gaps en el context window"): nada en el pipeline
# media cuanto pesa realmente la conversacion acumulada contra el limite
# real del backend (OLLAMA_NUM_CTX para Ollama) -- total_input_tokens/
# total_output_tokens se acumulan solo para costo, nunca se comparan contra
# nada. Esto es una ESTIMACION honesta por caracteres (~4 caracteres/token,
# heuristica conocida, NO un conteo exacto de tokens por backend real -- eso
# requeriria un tokenizer distinto por modelo, desproporcionado para esto),
# etiquetada como tal en el mensaje de warning. Nunca trunca nada -- solo
# visibilidad donde hoy no habia ninguna.
#
# Bug real confirmado esta sesion (segunda vuelta del mismo gap): este
# umbral era fijo (siempre basado en OLLAMA_NUM_CTX), sin importar que
# backend estuviera realmente sirviendo el turno -- una corrida en
# Anthropic (ventana real ~200k tokens) recibia el mismo umbral chico
# pensado para Ollama, generando warnings falsos-positivos con muchisimo
# margen real de sobra. CONTEXT_SIZE_WARNING_CHARS (env var) sigue
# funcionando como override manual absoluto si esta seteada; sin ella, el
# umbral default ahora se deriva del backend real de MODEL_LIMITS.
_CONTEXT_SIZE_WARNING_CHARS_OVERRIDE = os.environ.get("CONTEXT_SIZE_WARNING_CHARS")


def context_warning_threshold_chars(backend: str) -> int:
    """Estimacion honesta por caracteres (~4/token) de cuanto entra en la
    ventana de contexto REAL del backend dado -- reusada por
    conversation_memory.py para alinear su umbral de compactacion a esto
    mismo (antes tenia un umbral fijo propio, mas alto que lo que realmente
    entra en OLLAMA_NUM_CTX, asi que la compactacion nunca llegaba a
    activarse antes de que el contexto real ya se hubiera desbordado --
    bug real confirmado en vivo, epica KAN-4: 11 historias seguidas
    devolvieron "done" fabricado, sin una sola tool call, una vez que la
    conversacion continuada acumulo mas texto del que el modelo realmente
    podia ver)."""
    if _CONTEXT_SIZE_WARNING_CHARS_OVERRIDE:
        return int(_CONTEXT_SIZE_WARNING_CHARS_OVERRIDE)
    context_tokens = MODEL_LIMITS.get(backend, {}).get("context_window_tokens") or OLLAMA_NUM_CTX
    return context_tokens * 4


def _estimate_message_chars(messages: list) -> int:
    return sum(len(str(m.get("content", ""))) for m in messages)


def warn_if_context_large(messages: list, logger, label: str, backend: str = "ollama", system_prompt: str = "") -> None:
    """Best-effort: loguea UN warning real (no trunca, no lanza) cuando el
    historial acumulado de esta conversacion (MAS el system_prompt, que se
    manda en CADA turno igual que los messages -- bug real confirmado esta
    sesion: antes se excluia del calculo por completo, subestimando la
    presion real sobre la ventana de contexto en varios miles de tokens
    fijos por turno) supera el umbral real de este backend. Llamar despues
    de compact_old_tool_results() para medir lo que efectivamente se va a
    reenviar en el proximo turno.
    """
    threshold = context_warning_threshold_chars(backend)
    estimated_chars = _estimate_message_chars(messages) + len(system_prompt)
    if estimated_chars > threshold:
        logger.warning(
            f"{label}: el historial acumulado de esta conversacion (incluido el system prompt) es de "
            f"~{estimated_chars} caracteres (~{estimated_chars // 4} tokens estimados a 4 caracteres/token, "
            f"umbral real para backend '{backend}': {threshold} caracteres) -- riesgo real de truncamiento o "
            "rechazo del backend."
        )


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


# Confirmado real esta sesion (juez, corrida real contra KAN-15): el paquete
# de terceros mcp-neo4j-cypher tiene un bug real en su propia implementacion
# de get_neo4j_schema -- arma la query interna con el None de Python en vez
# del null de Cypher ("CALL apoc.meta.schema({sample: None}) YIELD value
# RETURN value"), lo que SIEMPRE tira CypherSyntaxError y quema turnos hasta
# agotar MAX_TOOL_TURNS (confirmado: la corrida completa termino en "el juez
# agoto los turnos... " por esto). No es algo que un prompt mas fuerte pueda
# arreglar -- es codigo del servidor MCP externo, no algo que el modelo
# escribe -- asi que se excluye directamente de las tools ofrecidas, para
# ambos agentes (coding agent y juez comparten este mismo servidor MCP).
_BROKEN_MCP_TOOLS = {"neo4j-cypher__get_neo4j_schema"}


def _normalize_tool_schema(server_name: str, tools) -> list:
    """Normaliza tools listadas por un servidor MCP al formato interno
    compartido (bloques con "name"/"description"/"input_schema") -- el
    mismo formato que ya usan los bloques text/tool_use/tool_result en
    todo este modulo. Anthropic es uno de los backends que lo consume tal
    cual; Ollama pasa por _tools_to_ollama_format() para adaptarlo -- ambos
    son adaptadores simetricos de este formato neutral, no hay un backend
    "nativo" y otro "adaptado". Filtra _BROKEN_MCP_TOOLS -- ver comentario
    arriba.
    """
    return [
        {
            "name": f"{server_name}__{t.name}",
            "description": t.description or "",
            "input_schema": t.inputSchema,
        }
        for t in tools
        if f"{server_name}__{t.name}" not in _BROKEN_MCP_TOOLS
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
