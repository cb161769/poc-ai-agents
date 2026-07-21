"""Epic planning agent -- run_epic_etapas() (run_poc_loop.sh) and
run_epic_pipeline() (orchestration.py) today concatenate an epic's child
stories mechanically, in whatever order Jira's JQL happened to return them.
This agent reasons about real order/coordination before that prompt gets
built: given the epic + its children, it can query the same Neo4j graph the
judge already uses (mcp-neo4j-cypher, real DEPENDS_ON relationships between
the components each child touches) and returns an execution order plus
coordination notes -- so the combined prompt reflects real dependency
structure instead of whatever order the ticket search returned.

Same dual backend as judge_agent.py/coding_agent.py (agent_loop.py,
Anthropic first, Ollama fallback). Best-effort like the judge: if no
backend is reachable or the call fails, the caller (run_poc_loop.sh/
orchestration.py) falls back to the mechanical order that already exists
today -- this never blocks epic mode.

Reads a single JSON blob from stdin:
  {"epic": {"key": "...", "summary": "...", "description": "..."},
   "children": [{"ticket_id": "...", "summary": "...", "description": "...",
                 "repository_origen": "..."}, ...]}

Prints a single JSON result to stdout:
  {"ordered_children": ["JIRA-1", "JIRA-2", ...],
   "coordination_notes": "...", "conflicts": ["..."]}
"""
import asyncio
import json
import os
import sys
from contextlib import AsyncExitStack

import httpx
from dotenv import load_dotenv
from mcp import StdioServerParameters

from agent_loop import (
    OLLAMA_MODEL,
    _call_mcp_tool,
    _connect_mcp_servers,
    _final_text_with_json_retry,
    _normalize_tool_schema,
    _select_backend,
    call_with_fallback,
    compact_old_tool_results,
    init_ollama_model_state,
    maybe_switch_ollama_model,
    parse_ollama_model_candidates,
    warn_if_context_large,
)
from log_utils import get_logger

load_dotenv()

logger = get_logger(__name__)

MAX_TOOL_TURNS = 4

# Modelo(s) Ollama propios para el planificador -- mismo patron que
# CODING_AGENT_OLLAMA_MODEL/JUDGE_OLLAMA_MODEL: override opcional
# coma-separado (lista de candidatos por prioridad), cae al generico
# OLLAMA_MODEL si no se setea.
EPIC_PLANNER_OLLAMA_MODELS = parse_ollama_model_candidates(os.environ.get("EPIC_PLANNER_OLLAMA_MODEL", ""), OLLAMA_MODEL)


def _extract_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())

EPIC_PLANNER_SYSTEM_PROMPT = """Sos un planificador de épicas para un pipeline que resuelve tickets de Jira \
con un agente de código autónomo. Te dan una épica real y sus historias hijas reales -- tu trabajo es \
ordenarlas por dependencia real, no alfabético ni el orden en que llegaron.

Hay DOS tipos distintos de dependencia real a considerar, con evidencia distinta cada una -- no las confundas:

1) **Dependencia entre COMPONENTES YA EXISTENTES** (grafo Neo4j, vía MCP): usá la tool para consultar si los \
componentes que tocan las historias hijas tienen relaciones DEPENDS_ON reales entre sí (por ejemplo: `MATCH \
(a:Service)-[:DEPENDS_ON]->(b:Service) WHERE a.name IN [...] OR b.name IN [...] RETURN a.name, b.name`). \
Una historia que toca un componente del que otro depende debería resolverse antes, si es razonable. No \
inventes relaciones acá que no puedas verificar con la tool -- si el grafo no tiene datos suficientes, no \
asumas una dependencia de este tipo.

2) **Dependencia de ANDAMIAJE (scaffolding), evidente del TEXTO, sin necesitar el grafo**: una historia puede \
describir literalmente crear/montar/inicializar la estructura base de un proyecto o framework (ej. "montar la \
arquitectura del proyecto Angular/Ionic/Capacitor", "inicializar el monorepo", "configurar el build") -- y \
otra historia puede asumir que esa estructura YA EXISTE para poder trabajar dentro de ella (ej. "crear el \
componente Header", "agregar un botón reutilizable", cualquier cosa que necesite `src/app/...` o rutas \
similares ya creadas). ESTO SÍ hace falta inferirlo del texto de la historia, no del grafo -- el grafo modela \
dependencias entre componentes ya existentes, nunca "quién crea la estructura que otra historia necesita", \
porque esa estructura todavía no existe como nodo cuando estás planificando. Gap real confirmado en una \
corrida real: una historia que agregaba un componente a un framework se intentó ANTES que la historia que \
montaba ese framework, porque nunca se buscó este tipo de dependencia -- terminó bloqueada porque la \
estructura esperada literalmente no existía todavía. Si detectás este patrón, la historia de andamiaje va \
primero, y decilo explícitamente en coordination_notes (ej. "KAN-5 monta el proyecto base, por eso va antes \
que KAN-9/KAN-10/KAN-11 que agregan componentes dentro de él").

También señalá (sin bloquear -- esto es información, no un gate) si dos historias parecen pisarse: mismo \
componente, cambios que suenan contradictorios, o alcance solapado.

Si una historia trae info de sprint, es solo contexto informativo -- NUNCA la uses para excluir o bloquear \
una historia del orden (una historia fuera del sprint activo puede seguir siendo parte real de esta corrida). \
Usala solo si te ayuda a explicar coordination_notes (ej. "las historias del Sprint 12 se agrupan primero").

Cuando termines, respondé con texto plano que sea ÚNICAMENTE un objeto JSON, sin texto antes ni después, \
con este esquema exacto: {"ordered_children": ["<ticket_id>", ...] (TODOS los ticket_id de las historias \
hijas que te di, sin omitir ninguna), "coordination_notes": "...", "conflicts": ["..."]}"""

MCP_SERVERS = {
    "neo4j-cypher": StdioServerParameters(
        command="uvx",
        args=["mcp-neo4j-cypher"],
        env={
            "NEO4J_URI": os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            "NEO4J_USERNAME": os.environ.get("NEO4J_USERNAME", "neo4j"),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD", "test_password_local"),
        },
    ),
}


def _format_sprint_suffix(sprint: dict | None) -> str:
    if not sprint or not sprint.get("name"):
        return ""
    state = f", {sprint['state']}" if sprint.get("state") else ""
    return f" (sprint: {sprint['name']}{state})"


def _build_user_prompt(epic: dict, children: list) -> str:
    children_text = "\n".join(
        f"- {c.get('ticket_id')} ({c.get('repository_origen')}){_format_sprint_suffix(c.get('sprint'))}: "
        f"{c.get('summary')}\n  {c.get('description')}"
        for c in children
    )
    return f"""Épica {epic.get('key')}: {epic.get('summary')}
{epic.get('description')}

--- Historias hijas ({len(children)}) ---
{children_text}"""


def _fallback_result(children: list) -> dict:
    """Mismo orden mecanico que el caller ya usaba antes de este agente --
    se usa si el modelo no devuelve una lista completa/valida.
    """
    return {
        "ordered_children": [c.get("ticket_id") for c in children],
        "coordination_notes": "",
        "conflicts": [],
    }


def _validate_result(result: dict, children: list) -> dict:
    """Best-effort: si el modelo omite o inventa ticket_ids, no confiamos en
    un orden parcial -- caemos al orden mecanico original en vez de arriesgar
    perder una historia del prompt combinado.
    """
    expected_ids = {c.get("ticket_id") for c in children}
    ordered = result.get("ordered_children")
    if not isinstance(ordered, list) or set(ordered) != expected_ids:
        fallback = _fallback_result(children)
        fallback["coordination_notes"] = str(result.get("coordination_notes") or "")
        fallback["conflicts"] = result.get("conflicts") if isinstance(result.get("conflicts"), list) else []
        return fallback
    return {
        "ordered_children": ordered,
        "coordination_notes": str(result.get("coordination_notes") or ""),
        "conflicts": result.get("conflicts") if isinstance(result.get("conflicts"), list) else [],
    }


async def plan_epic(epic: dict, children: list) -> dict:
    if not children:
        return _fallback_result(children)

    backend = _select_backend()
    if backend == "none":
        logger.warning("epic planner: sin backend disponible, cae al orden mecanico")
        return _fallback_result(children)

    logger.info(f"epic planner: usando backend '{backend}'")
    # Mismo criterio que coding_agent.py/judge_agent.py: el probe real solo
    # tiene sentido si el backend elegido es Ollama.
    if backend == "ollama":
        ollama_model_state = init_ollama_model_state(EPIC_PLANNER_OLLAMA_MODELS, logger, "epic planner")
    else:
        ollama_model_state = {"active": EPIC_PLANNER_OLLAMA_MODELS[0], "tried": set(), "switch_used": False}

    async with AsyncExitStack() as stack:
        sessions = await _connect_mcp_servers(stack, MCP_SERVERS, label="epic planner")

        tools = []
        for name, session in sessions.items():
            try:
                listed = await session.list_tools()
                tools.extend(_normalize_tool_schema(name, listed.tools))
            except Exception as exc:
                logger.warning(f"epic planner: no se pudieron listar tools de '{name}': {exc}")

        # Gap real (usuario, "hay gaps en el context window"): la unica tool
        # que se le ofrece al planificador es neo4j-cypher (solo lectura),
        # asi que compact_old_tool_results() puede compactar cualquiera.
        read_only_tool_names = {t["name"] for t in tools}

        messages = [{"role": "user", "content": _build_user_prompt(epic, children)}]

        async with httpx.AsyncClient() as client:
            for _ in range(MAX_TOOL_TURNS):
                content, stop_reason, usage, backend = await call_with_fallback(
                    client, messages, tools, EPIC_PLANNER_SYSTEM_PROMPT,
                    ollama_model=ollama_model_state["active"], force_json=True,
                )
                messages.append({"role": "assistant", "content": content})

                if stop_reason != "tool_use":
                    final_text = next((b["text"] for b in content if b.get("type") == "text"), "")
                    try:
                        result = _extract_json(final_text)
                    except json.JSONDecodeError:
                        try:
                            retry_text, _usage = await _final_text_with_json_retry(
                                client, backend, messages, tools, EPIC_PLANNER_SYSTEM_PROMPT,
                                ollama_model=ollama_model_state["active"],
                            )
                            result = _extract_json(retry_text)
                        except json.JSONDecodeError:
                            if maybe_switch_ollama_model(
                                ollama_model_state, backend, EPIC_PLANNER_OLLAMA_MODELS, logger,
                                "epic planner", "JSON invalido incluso tras el reintento de correccion",
                            ):
                                continue
                            logger.warning("epic planner: el modelo no devolvio JSON valido, cae al orden mecanico")
                            return _fallback_result(children)
                    return _validate_result(result, children)

                tool_results = []
                for block in content:
                    if block.get("type") != "tool_use":
                        continue
                    try:
                        output = await _call_mcp_tool(sessions, block["name"], block.get("input", {}))
                    except Exception as exc:
                        output = f"error llamando a la herramienta: {exc}"
                    tool_results.append({"type": "tool_result", "tool_use_id": block["id"], "content": str(output)})
                messages.append({"role": "user", "content": tool_results})
                compact_old_tool_results(messages, read_only_tool_names)
                warn_if_context_large(messages, logger, "epic planner", backend=backend, system_prompt=EPIC_PLANNER_SYSTEM_PROMPT)

    logger.warning("epic planner: agoto los turnos sin un resultado final, cae al orden mecanico")
    return _fallback_result(children)


def main():
    try:
        payload = json.loads(sys.stdin.read())
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": f"invalid_json_payload: {exc}"}), file=sys.stderr)
        sys.exit(1)

    epic = payload.get("epic", {})
    children = payload.get("children", [])

    try:
        result = asyncio.run(plan_epic(epic, children))
    except Exception as exc:
        logger.warning(f"epic planner: fallo inesperado, cae al orden mecanico: {exc}")
        result = _fallback_result(children)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
