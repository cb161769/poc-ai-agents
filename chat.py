"""Chat interactivo real contra el mismo dual-backend (Anthropic/Ollama) que
usa el resto de la PoC -- a diferencia de `docker exec -it poc-ollama ollama
run llama3.1` (Ollama pelado, sin nada de este proyecto), esto reusa
exactamente la misma infraestructura que ya prueban coding_agent.py/
judge_agent.py: seleccion de backend con fallback en vivo, y acceso real a
TODAS las tools que ya existen -- LOCAL_TOOLS completo de coding_agent.py
(leer/escribir/editar archivos, listar directorios, grep, git diff/log,
detectar stack, correr comandos de shell, consultar Sonar) + los mismos MCP
reales (Neo4j-cypher, Qdrant-rag).

write_file/edit_file/run_shell_command siguen pidiendo confirmacion humana
[s/n] antes de actuar (misma funcion _confirm de coding_agent.py) -- ese
guardrail funciona sin problema aca porque este script lo corre el usuario
en su propia terminal con TTY real.

Uso:
  python chat.py [target_repo_dir]   # default: directorio actual
  escribi "salir" / "exit" / "quit" para cortar, o Ctrl+C.

No es parte del pipeline auditado: no escribe en logs/*.jsonl, no exige un
JSON final -- es charla libre para explorar el repo/la PoC. Cualquier
write_file/run_shell_command que se confirme SI tiene efectos reales.
"""
import asyncio
import os
import sys
from contextlib import AsyncExitStack
from pathlib import Path

import httpx
from dotenv import load_dotenv
from mcp import StdioServerParameters

from agent_loop import (
    ANTHROPIC_MODEL,
    _call_mcp_tool,
    _connect_mcp_servers,
    _estimate_cost_usd,
    _normalize_tool_schema,
    _select_backend,
    call_with_fallback,
)
from coding_agent import LOCAL_TOOLS

load_dotenv()

MAX_TOOL_TURNS = int(os.environ.get("CHAT_MAX_TOOL_TURNS", "10"))

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
    "qdrant-rag": StdioServerParameters(
        command="uvx",
        args=["mcp-server-qdrant"],
        env={
            "QDRANT_URL": os.environ.get("QDRANT_URL", "http://localhost:6333"),
            "COLLECTION_NAME": "sample_repo_code",
            "EMBEDDING_MODEL": "sentence-transformers/all-MiniLM-L6-v2",
        },
    ),
}

CHAT_SYSTEM_PROMPT = """Sos el asistente interactivo de esta PoC de pipeline de agentes de codigo \
(Jira real + firewall + coding agent + testing agent + juez). Hablas directo con un humano en una \
terminal, no con un pipeline automatizado -- no hay un JSON final que devolver, respondes en texto \
libre.

Tenes acceso real a: leer/listar/buscar archivos del repo objetivo, editar/escribir archivos (pide \
confirmacion humana antes de aplicarse -- avisale al usuario que va a aparecer ese prompt), correr \
comandos de shell (mismo criterio), ver el diff/historial de git, detectar el stack del proyecto, \
consultar hallazgos reales de SonarQube, y consultar el grafo de dependencias real en Neo4j y \
codigo/tickets/hallazgos indexados en Qdrant via MCP.

Cuando uses una herramienta, contale al usuario que encontraste concretamente (que devolvio la \
query, que archivo leiste) -- no digas solo "consulte el grafo", di que viste ahi. Si no tenes \
Neo4j/Qdrant conectados (pueden no estar disponibles), decilo en vez de inventar una respuesta."""


def _local_tools_to_anthropic_format() -> list:
    return [
        {"name": name, "description": spec["description"], "input_schema": spec["input_schema"]}
        for name, spec in LOCAL_TOOLS.items()
    ]


async def _run_chat(target_repo_dir: str):
    backend = _select_backend()
    if backend == "none":
        print("Ni ANTHROPIC_API_KEY ni Ollama estan disponibles -- no hay con que chatear.")
        return
    print(f"Backend activo: {backend}. Repo objetivo: {target_repo_dir}")
    print("Escribi 'salir' (o Ctrl+C) para cortar.\n")

    async with AsyncExitStack() as stack:
        sessions = await _connect_mcp_servers(stack, MCP_SERVERS, label="chat")

        tools = list(_local_tools_to_anthropic_format())
        for name, session in sessions.items():
            try:
                listed = await session.list_tools()
                tools.extend(_normalize_tool_schema(name, listed.tools))
            except Exception as exc:
                print(f"(no se pudieron listar tools de '{name}': {exc})")

        messages = []
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    user_text = input("vos> ").strip()
                except (EOFError, KeyboardInterrupt):
                    print("\nChau.")
                    return
                if not user_text:
                    continue
                if user_text.lower() in ("salir", "exit", "quit"):
                    print("Chau.")
                    return

                messages.append({"role": "user", "content": user_text})
                turn_input_tokens = 0
                turn_output_tokens = 0
                current_backend = backend

                for _ in range(MAX_TOOL_TURNS):
                    content, stop_reason, usage, current_backend = await call_with_fallback(
                        client, messages, tools, CHAT_SYSTEM_PROMPT
                    )
                    turn_input_tokens += usage.get("input_tokens", 0)
                    turn_output_tokens += usage.get("output_tokens", 0)
                    messages.append({"role": "assistant", "content": content})

                    if stop_reason != "tool_use":
                        final_text = "\n".join(b["text"] for b in content if b.get("type") == "text")
                        print(f"\nasistente ({current_backend})> {final_text}")
                        cost = _estimate_cost_usd(current_backend, ANTHROPIC_MODEL, turn_input_tokens, turn_output_tokens)
                        print(f"[tokens: {turn_input_tokens} in / {turn_output_tokens} out -- costo estimado: ${cost:.6f}]\n")
                        break

                    tool_results = []
                    for block in content:
                        if block.get("type") != "tool_use":
                            continue
                        name = block["name"]
                        tool_input = block.get("input", {})
                        try:
                            if name in LOCAL_TOOLS:
                                output = LOCAL_TOOLS[name]["fn"](target_repo_dir, **tool_input)
                            else:
                                output = await _call_mcp_tool(sessions, name, tool_input)
                        except Exception as exc:
                            output = f"error llamando a la herramienta: {exc}"
                        tool_results.append({"type": "tool_result", "tool_use_id": block["id"], "content": str(output)})
                    messages.append({"role": "user", "content": tool_results})
                else:
                    print("\n(se alcanzo el limite de turnos de herramientas para este mensaje)\n")


def main():
    target_repo_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    target_repo_dir = str(Path(target_repo_dir).resolve())
    asyncio.run(_run_chat(target_repo_dir))


if __name__ == "__main__":
    main()
