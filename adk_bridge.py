"""Puente entre Google ADK (google-adk) y la capa de backends de agent_loop.py.

ADK aporta el modelo de agente (LlmAgent + Runner + sesiones + tools); el
modelo en si lo sirve AgentLoopLlm, un BaseLlm propio que delega en
agent_loop.call_with_fallback() -- asi un agente ADK obtiene gratis el
fallback en vivo entre backends (anthropic -> claude_cli -> ollama), el
presupuesto diario, el circuit breaker de auth y el cambio de modelo Ollama
que ya existen, en vez de reimplementarlos contra otra API de modelo.

  adk_generate()          -- agente ADK de un solo turno, sin tools (clasificadores,
                             generadores de texto).
  python_function_tool()  -- funcion Python real -> FunctionTool de ADK.
  McpSessionTool/mcp_tools() -- tools MCP (sesiones vivas) como BaseTool de ADK.
  build_adk_agent()       -- LlmAgent respaldado por AgentLoopLlm con esos tools.
"""
import asyncio
import contextvars
import inspect
import re
import uuid
from typing import Any, AsyncGenerator, Callable

import httpx
from google.adk.agents import LlmAgent
from google.adk.events import Event
from google.adk.agents.invocation_context import LlmCallsLimitExceededError
from google.adk.agents.run_config import RunConfig
from google.adk.models import BaseLlm, LlmRequest, LlmResponse
from google.adk.runners import InMemoryRunner
from google.adk.tools import BaseTool, FunctionTool
from google.genai import types

from agent_loop import _COMPACTED_TOOL_RESULT_TEMPLATE, _call_mcp_tool, call_with_fallback, warn_if_context_large
from log_utils import get_logger

logger = get_logger(__name__)

_APP_NAME = "poc_ai_agents"
_ADK_IDENTITY_LINE = re.compile(r'\n*You are an agent\. Your internal name is "[^"]*"\.\s*$')


def _contents_to_messages(contents: list) -> list:
    """ADK/genai Content -> historial estilo Anthropic (el formato interno
    que call_with_fallback espera: content = str o bloques text/tool_use/
    tool_result)."""
    messages = []
    for content in contents:
        role = "assistant" if content.role == "model" else "user"
        blocks = []
        for part in content.parts or []:
            if part.text and not getattr(part, "thought", False):
                blocks.append({"type": "text", "text": part.text})
            elif part.function_call:
                fc = part.function_call
                blocks.append({"type": "tool_use", "id": fc.id or f"adk_{fc.name}", "name": fc.name, "input": dict(fc.args or {})})
            elif part.function_response:
                fr = part.function_response
                response = fr.response or {}
                text = response.get("result", response) if isinstance(response, dict) else response
                blocks.append({"type": "tool_result", "tool_use_id": fr.id or f"adk_{fr.name}", "content": str(text)})
        if not blocks:
            continue
        # tool_result siempre va en un mensaje 'user' (formato Anthropic).
        if any(b["type"] == "tool_result" for b in blocks):
            role = "user"
        if len(blocks) == 1 and blocks[0]["type"] == "text":
            messages.append({"role": role, "content": blocks[0]["text"]})
        else:
            messages.append({"role": role, "content": blocks})
    return messages


def _messages_to_contents(messages: list, agent_name: str) -> list:
    """Inverso de _contents_to_messages: -> [(author, Content)] listos para
    append_event. Los resultados de tools van con author=agente (como los
    genera ADK) para que el Runner los empareje con su function_call por id."""
    tool_name_by_id = {}
    out = []
    for m in messages:
        content = m["content"]
        blocks = [{"type": "text", "text": content}] if isinstance(content, str) else content
        parts = []
        has_tool_result = False
        for b in blocks:
            kind = b.get("type")
            if kind == "text":
                parts.append(types.Part(text=b["text"]))
            elif kind == "tool_use":
                tool_name_by_id[b.get("id")] = b.get("name")
                parts.append(types.Part(function_call=types.FunctionCall(id=b.get("id"), name=b["name"], args=b.get("input", {}))))
            elif kind == "tool_result":
                has_tool_result = True
                parts.append(types.Part(function_response=types.FunctionResponse(
                    id=b.get("tool_use_id"), name=tool_name_by_id.get(b.get("tool_use_id"), "unknown"),
                    response={"result": b.get("content", "")},
                )))
        if not parts:
            continue
        if m["role"] == "assistant":
            out.append((agent_name, types.Content(role="model", parts=parts)))
        else:
            out.append((agent_name if has_tool_result else "user", types.Content(role="user", parts=parts)))
    return out


def _declarations_to_tools(config: types.GenerateContentConfig | None) -> list:
    tools = []
    for tool in (config.tools if config else None) or []:
        for decl in tool.function_declarations or []:
            if decl.parameters_json_schema is not None:
                schema = decl.parameters_json_schema
            elif decl.parameters is not None:
                schema = _lowercase_types(decl.parameters.model_dump(exclude_none=True, mode="json"))
            else:
                schema = {"type": "object", "properties": {}}
            tools.append({"name": decl.name, "description": decl.description or "", "input_schema": schema})
    return tools


def _lowercase_types(node: Any) -> Any:
    """genai.Schema serializa los tipos en MAYUSCULAS ("STRING"); JSON Schema
    (y los backends de este pipeline) los espera en minuscula."""
    if isinstance(node, dict):
        return {k: (v.lower() if k == "type" and isinstance(v, str) else _lowercase_types(v)) for k, v in node.items()}
    if isinstance(node, list):
        return [_lowercase_types(v) for v in node]
    return node


def _blocks_to_content(blocks: list) -> types.Content:
    parts = []
    for b in blocks:
        if b.get("type") == "text" and b.get("text"):
            parts.append(types.Part(text=b["text"]))
        elif b.get("type") == "tool_use":
            parts.append(types.Part(function_call=types.FunctionCall(id=b.get("id"), name=b["name"], args=b.get("input", {}))))
    return types.Content(role="model", parts=parts or [types.Part(text="")])


class AgentLoopLlm(BaseLlm):
    """BaseLlm de ADK cuyo "modelo" es la cadena de backends de agent_loop."""

    model: str = "agent-loop"
    anthropic_model: str | None = None
    ollama_model: str | None = None
    force_json: bool = False
    json_schema: dict | None = None
    exclude_backends: set = set()
    # Modo "respuesta final": no se ofrecen tools en ESTE turno (el reintento de
    # JSON/los nudges piden texto corregido, no otra tool-call) -- permite que
    # el backend fuerce la gramatica JSON (Ollama format / prefill Anthropic),
    # que solo aplica sin tools. Lo prende/apaga el caller entre turnos.
    tools_disabled: bool = False
    # Ultimo backend que realmente atendio un turno / uso acumulado -- el
    # caller (ej. coding_agent) lo necesita para auditoria y costo.
    last_backend: str | None = None
    calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0

    @property
    def capabilities(self):
        from google.adk.models.base_llm import LlmCapabilities
        return LlmCapabilities(output_schema_and_tools=False)

    async def generate_content_async(self, llm_request: LlmRequest, stream: bool = False) -> AsyncGenerator[LlmResponse, None]:
        config = llm_request.config
        system_prompt = ""
        if config and config.system_instruction:
            instruction = config.system_instruction
            system_prompt = instruction if isinstance(instruction, str) else "\n".join(
                p.text for p in getattr(instruction, "parts", []) or [] if getattr(p, "text", None)
            )
        # ADK agrega siempre una linea de identidad al system prompt
        # ("You are an agent. Your internal name is ..."); los prompts de este
        # pipeline son exactos y auditados, no se les agrega texto.
        system_prompt = _ADK_IDENTITY_LINE.sub("", system_prompt)
        messages = _contents_to_messages(llm_request.contents)
        tools = [] if self.tools_disabled else _declarations_to_tools(config)

        # Solo se pasan los parametros que difieren del default: asi un
        # agente que no usa force_json/json_schema/etc. llama a
        # call_with_fallback con la misma firma minima que siempre uso.
        extra = {}
        if self.ollama_model:
            extra["ollama_model"] = self.ollama_model
        if self.force_json:
            extra["force_json"] = True
        if self.json_schema is not None:
            extra["json_schema"] = self.json_schema
        if self.exclude_backends:
            extra["exclude"] = self.exclude_backends
        if self.anthropic_model:
            extra["anthropic_model"] = self.anthropic_model
        async with httpx.AsyncClient() as client:
            blocks, _stop_reason, usage, backend_used = await call_with_fallback(
                client, messages, tools, system_prompt,
                **extra,
            )
        self.last_backend = backend_used
        self.calls += 1
        self.total_input_tokens += usage.get("input_tokens", 0)
        self.total_output_tokens += usage.get("output_tokens", 0)
        yield LlmResponse(
            content=_blocks_to_content(blocks),
            usage_metadata=types.GenerateContentResponseUsageMetadata(
                prompt_token_count=usage.get("input_tokens", 0),
                candidates_token_count=usage.get("output_tokens", 0),
            ),
        )


# Observabilidad opcional: quien corre un agente (ej. la GUI, chat_server.py)
# setea un callback sync(dict) en este ContextVar y recibe cada tool_call /
# tool_result / texto del modelo a medida que ocurren. Sin callback, no hace nada.
event_sink: contextvars.ContextVar = contextvars.ContextVar("adk_event_sink", default=None)


def _emit_event(event) -> None:
    sink = event_sink.get()
    if sink is None or not event.content or not event.content.parts:
        return
    for part in event.content.parts:
        if part.function_call:
            sink({"type": "tool_call", "name": part.function_call.name, "args": dict(part.function_call.args or {})})
        elif part.function_response:
            response = part.function_response.response or {}
            output = response.get("result", response) if isinstance(response, dict) else response
            sink({"type": "tool_result", "name": part.function_response.name, "output": str(output)[:4000]})
        elif part.text and not getattr(part, "thought", False):
            sink({"type": "model_text", "text": part.text, "final": bool(event.is_final_response())})


_NO_BIND = object()


class _NullAsyncContext:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False


def python_function_tool(
    fn: Callable, *, name: str | None = None, bind_first_arg: Any = _NO_BIND, description: str | None = None,
    lock: asyncio.Lock | None = None, pre_hook: Callable | None = None, post_hook: Callable | None = None,
    on_exception: Callable[[Exception], str] | None = None,
) -> FunctionTool:
    """Funcion Python REAL -> FunctionTool de ADK (ADK deriva el schema de la
    firma + type hints + docstring de la funcion, no de un dict a mano).

    name: nombre expuesto al modelo (default: el de la funcion).
    bind_first_arg: varias tools del pipeline (coding_agent.tool_*) reciben
    target_repo_dir como primer parametro -- el modelo no debe verlo ni
    pasarlo. Se envuelve en una funcion real con la firma SIN ese parametro
    (conserva __name__/anotaciones; sync o async).
    lock: ADK ejecuta los tool-calls de un mismo turno en PARALELO
    (asyncio.gather) -- las tools que piden confirmacion por stdin o dependen
    del estado que dejo la anterior (gates de coding_agent) deben compartir
    un lock para correr en orden, como en el loop manual.
    pre_hook(name, args) -> str | None: si devuelve texto, ESA es la salida de
    la tool y la funcion no se ejecuta (gates). Corre dentro del lock.
    post_hook(name, args, output): efectos laterales tras una ejecucion que
    no levanto excepcion (tracking de escrituras/verificaciones).
    on_exception(exc) -> str: convierte una excepcion de la tool en la salida
    que ve el modelo (puede re-levantar para abortar la corrida); sin esto,
    "error llamando a la herramienta: ..." (igual que los loops viejos).
    """
    bound = () if bind_first_arg is _NO_BIND else (bind_first_arg,)
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())[len(bound):]
    exposed_name = name or fn.__name__

    async def target(**kwargs):
        async with (lock if lock is not None else _NullAsyncContext()):
            if pre_hook is not None:
                blocked = pre_hook(exposed_name, kwargs)
                if blocked is not None:
                    return blocked
            try:
                result = fn(*bound, **kwargs)
                if inspect.isawaitable(result):
                    result = await result
            except Exception as exc:
                if on_exception is not None:
                    return on_exception(exc)
                return f"error llamando a la herramienta: {exc}"
            if post_hook is not None:
                post_hook(exposed_name, kwargs, result)
            return result

    target.__name__ = exposed_name
    target.__doc__ = description or fn.__doc__
    target.__signature__ = sig.replace(parameters=params)
    first = next(iter(sig.parameters)) if bound else None
    target.__annotations__ = {k: v for k, v in fn.__annotations__.items() if k != first}
    return FunctionTool(target)


class McpSessionTool(BaseTool):
    """Tool de ADK respaldada por una sesion MCP VIVA (las que abre
    agent_loop._connect_mcp_servers, con su timeout/probe de conexion). Se
    usa esto en vez de google.adk McpToolset porque este ultimo exige un
    `mcp` mas nuevo que el pineado (1.12.4, atado a prefect/anyio) y
    reemplazaria la logica de conexion endurecida con incidentes reales.
    spec = {"name": "server__tool", "description", "input_schema"} (lo que
    devuelve agent_loop._normalize_tool_schema)."""

    def __init__(self, spec: dict, sessions: dict):
        super().__init__(name=spec["name"], description=spec["description"])
        self._spec = spec
        self._sessions = sessions

    def _get_declaration(self) -> types.FunctionDeclaration:
        return types.FunctionDeclaration(
            name=self._spec["name"], description=self._spec["description"],
            parameters_json_schema=self._spec["input_schema"],
        )

    async def run_async(self, *, args: dict[str, Any], tool_context) -> Any:
        try:
            return {"result": await _call_mcp_tool(self._sessions, self.name, args)}
        except Exception as exc:
            return {"result": f"error llamando a la herramienta: {exc}"}


def mcp_tools(specs: list, sessions: dict) -> list:
    return [McpSessionTool(spec, sessions) for spec in specs]


def context_guard(llm: AgentLoopLlm, label: str, system_prompt: str, read_only_tool_names: set, keep_last_n_turns: int = 3):
    """before_model_callback de ADK: mismo rol que agent_loop.compact_old_tool_results
    + warn_if_context_large en los loops manuales -- colapsa resultados
    viejos de tools de SOLO LECTURA (nunca los de escritura) y avisa si el
    historial se acerca a la ventana real del backend."""
    def callback(callback_context, llm_request: LlmRequest):
        contents = llm_request.contents
        model_indices = [i for i, c in enumerate(contents) if c.role == "model"]
        if len(model_indices) > keep_last_n_turns:
            boundary = model_indices[-keep_last_n_turns]
            for content in contents[:boundary]:
                for part in content.parts or []:
                    fr = part.function_response
                    if fr is not None and fr.name in read_only_tool_names:
                        fr.response = {"result": _COMPACTED_TOOL_RESULT_TEMPLATE.format(tool_name=fr.name)}
        system = llm_request.config.system_instruction if llm_request.config else ""
        warn_if_context_large(
            _contents_to_messages(contents), logger, label,
            backend=llm.last_backend or "ollama", system_prompt=system if isinstance(system, str) else system_prompt,
        )
        return None
    return callback


def build_adk_agent(
    name: str, system_prompt: str, llm: AgentLoopLlm, tools: list | None = None,
    read_only_tool_names: set | None = None, after_tool_callback=None, after_model_callback=None,
) -> LlmAgent:
    """tools: lista de BaseTool de ADK (python_function_tool(...), mcp_tools(...)).
    read_only_tool_names: activa la compactacion de contexto para esas tools.
    after_tool_callback(tool, args, tool_context, tool_response): hook de ADK."""
    kwargs = {}
    if read_only_tool_names:
        kwargs["before_model_callback"] = context_guard(llm, name, system_prompt, read_only_tool_names)
    if after_tool_callback is not None:
        kwargs["after_tool_callback"] = after_tool_callback
    if after_model_callback is not None:
        kwargs["after_model_callback"] = after_model_callback
    # instruction como callable: ADK interpola {nombre} en los strings
    # (variables de estado), y estos system prompts traen JSON de ejemplo con
    # llaves -- un InstructionProvider se usa tal cual, sin templating.
    return LlmAgent(name=name, model=llm, instruction=lambda _ctx: system_prompt, tools=list(tools or []), **kwargs)


class AgentTurnsExhausted(RuntimeError):
    """El agente llego a max_llm_calls sin una respuesta final."""


def _unwrap_adk_error(exc: Exception) -> Exception:
    """ADK 2.x envuelve los fallos del nodo del agente en DynamicNodeFailError;
    los callers (main() de cada agente) esperan las excepciones originales
    (httpx.HTTPError, RuntimeError...). Limite de llamadas -> RuntimeError."""
    inner = getattr(exc, "error", None)
    while isinstance(inner, Exception) and hasattr(inner, "error") and inner is not exc:
        inner = inner.error
    original = inner if isinstance(inner, Exception) else exc
    if isinstance(original, LlmCallsLimitExceededError):
        return AgentTurnsExhausted(f"el agente agoto los turnos de herramientas sin una respuesta final: {original}")
    return original


class AdkConversation:
    """Conversacion multi-turno sobre UNA sesion de ADK: cada say() agrega un
    mensaje de usuario y corre el Runner hasta la respuesta final del agente
    (el Runner ejecuta el loop de tools solo). Los follow-ups (reintento de
    JSON, nudges de consistencia) ven todo el historial previo."""

    def __init__(self, agent: LlmAgent):
        self._agent = agent
        self._runner = InMemoryRunner(agent=agent, app_name=_APP_NAME)
        self._session_id = uuid.uuid4().hex
        self._created = False

    async def _ensure_session(self):
        if not self._created:
            await self._runner.session_service.create_session(
                app_name=_APP_NAME, user_id="pipeline", session_id=self._session_id,
            )
            self._created = True

    async def seed(self, messages: list) -> None:
        """Precarga historial estilo Anthropic (ej. la conversacion que un
        reintento anterior guardo en disco) como eventos de la sesion, para
        que el proximo say() continue esa conversacion."""
        await self._ensure_session()
        session = await self._runner.session_service.get_session(
            app_name=_APP_NAME, user_id="pipeline", session_id=self._session_id,
        )
        for author, content in _messages_to_contents(messages, self._agent.name):
            await self._runner.session_service.append_event(
                session, Event(author=author, invocation_id=uuid.uuid4().hex, content=content),
            )

    async def export_messages(self) -> list:
        """Historial actual de la sesion en formato Anthropic (lo que
        _finalize de coding_agent persiste para reintentos)."""
        await self._ensure_session()
        session = await self._runner.session_service.get_session(
            app_name=_APP_NAME, user_id="pipeline", session_id=self._session_id,
        )
        return _contents_to_messages([e.content for e in session.events if e.content and e.content.parts])

    async def say(self, text: str, max_llm_calls: int | None = None) -> str:
        """Devuelve el texto final. Con max_llm_calls (el equivalente de
        MAX_TOOL_TURNS) un agente que nunca deja de llamar tools levanta
        RuntimeError en vez de iterar sin limite."""
        await self._ensure_session()
        message = types.Content(role="user", parts=[types.Part(text=text)])
        final_text = ""
        try:
            async for event in self._runner.run_async(
                user_id="pipeline", session_id=self._session_id, new_message=message,
                run_config=RunConfig(max_llm_calls=max_llm_calls) if max_llm_calls else None,
            ):
                _emit_event(event)
                if event.is_final_response() and event.content and event.content.parts:
                    final_text = "".join(p.text for p in event.content.parts if p.text and not getattr(p, "thought", False))
        except Exception as exc:
            raise _unwrap_adk_error(exc)
        return final_text


async def run_adk_agent(agent: LlmAgent, user_text: str, max_llm_calls: int | None = None) -> str:
    """Un solo mensaje -> respuesta final (ver AdkConversation.say)."""
    return await AdkConversation(agent).say(user_text, max_llm_calls)


async def adk_generate(
    name: str, system_prompt: str, user_text: str, *, ollama_model: str | None = None,
    force_json: bool = False, json_schema: dict | None = None,
) -> tuple[str, str | None]:
    """Agente ADK de un turno sin tools. Devuelve (texto, backend_usado)."""
    llm = AgentLoopLlm(ollama_model=ollama_model, force_json=force_json, json_schema=json_schema)
    agent = build_adk_agent(name, system_prompt, llm)
    text = await run_adk_agent(agent, user_text)
    return text, llm.last_backend
