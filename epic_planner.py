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
    _ollama_model_available,
    _select_backend,
    call_with_fallback,
)
from log_utils import get_logger

load_dotenv()

logger = get_logger(__name__)

MAX_TOOL_TURNS = 4


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

Tenés acceso al grafo real de dependencias (Neo4j, vía MCP) -- usalo para consultar si los componentes que \
tocan las historias hijas tienen relaciones DEPENDS_ON reales entre sí (por ejemplo: `MATCH \
(a:Service)-[:DEPENDS_ON]->(b:Service) WHERE a.name IN [...] OR b.name IN [...] RETURN a.name, b.name`). \
Una historia que toca un componente del que otro depende debería resolverse antes, si es razonable.

También señalá (sin bloquear -- esto es información, no un gate) si dos historias parecen pisarse: mismo \
componente, cambios que suenan contradictorios, o alcance solapado.

No inventes relaciones que no puedas verificar con la tool -- si el grafo no tiene datos suficientes, \
ordená por el criterio que tengas (ej. mantené el orden original) y decilo en coordination_notes en vez de \
inventar una dependencia.

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


def _build_user_prompt(epic: dict, children: list) -> str:
    children_text = "\n".join(
        f"- {c.get('ticket_id')} ({c.get('repository_origen')}): {c.get('summary')}\n  {c.get('description')}"
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
    if backend == "ollama" and not _ollama_model_available(OLLAMA_MODEL):
        logger.warning(
            f"epic planner: el modelo '{OLLAMA_MODEL}' no aparece en 'ollama list' -- "
            "probablemente falta 'ollama pull', cae al orden mecanico si falla mas adelante."
        )

    async with AsyncExitStack() as stack:
        sessions = await _connect_mcp_servers(stack, MCP_SERVERS, label="epic planner")

        tools = []
        for name, session in sessions.items():
            try:
                listed = await session.list_tools()
                tools.extend(_normalize_tool_schema(name, listed.tools))
            except Exception as exc:
                logger.warning(f"epic planner: no se pudieron listar tools de '{name}': {exc}")

        messages = [{"role": "user", "content": _build_user_prompt(epic, children)}]

        async with httpx.AsyncClient() as client:
            for _ in range(MAX_TOOL_TURNS):
                content, stop_reason, usage, backend = await call_with_fallback(
                    client, messages, tools, EPIC_PLANNER_SYSTEM_PROMPT, force_json=True,
                )
                messages.append({"role": "assistant", "content": content})

                if stop_reason != "tool_use":
                    final_text = next((b["text"] for b in content if b.get("type") == "text"), "")
                    try:
                        result = _extract_json(final_text)
                    except json.JSONDecodeError:
                        try:
                            retry_text, _usage = await _final_text_with_json_retry(
                                client, backend, messages, tools, EPIC_PLANNER_SYSTEM_PROMPT
                            )
                            result = _extract_json(retry_text)
                        except json.JSONDecodeError:
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
