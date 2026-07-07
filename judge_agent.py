"""Independent judge agent for run_poc_loop.sh — with real MCP tool access.

Unlike the GitHub Copilot coding agent (which runs in GitHub's cloud and
can't reach localhost), this judge runs on the same machine as
run_poc_loop.sh, so it CAN reach the local MCP servers over stdio:

  - neo4j-cypher: verify the graph-impact claim itself, instead of trusting
    the precomputed GRAPH_RESULT text handed to it.
  - qdrant-rag: pull related historical code/tickets/Sonar findings to check
    if this looks like a known-safe pattern or a repeat problem.

Model backend, in order of preference:
  1. Anthropic API (ANTHROPIC_API_KEY set) — best quality.
  2. Local Ollama container (OLLAMA_URL reachable, e.g. the `ollama` service
     in docker-compose.yml) — free, offline fallback with a tool-calling
     capable model (OLLAMA_MODEL, default "llama3.1").
  3. Neither available — the judge is skipped for this run (same as before).

It reviews three things about a single pipeline run:
  1. The firewall's verdict (APPROVED/REJECTED) — was it the right call?
  2. The change Copilot proposed/applied (a local diff, or the issue text
     handed to the cloud coding agent when no diff exists yet).
  3. The run as a whole, end to end.

Reads a single JSON blob from stdin:
  {
    "ticket": {...},              # jira_client.py ticket dict
    "firewall": {status, reason, redactions_applied},
    "change_description": "...",  # diff text, or issue body if no diff yet
    "change_source": "local_diff" | "issue_only"
  }

Prints a single JSON verdict to stdout:
  {"verdict": "OK"|"FLAGGED", "firewall_assessment": "...",
   "change_assessment": "...", "reasoning": "..."}

Every call is appended to logs/judge_verdicts.jsonl regardless of verdict.
If the local MCP servers aren't reachable (uvx missing, Neo4j/Qdrant down),
the judge falls back to reasoning over the text it was given, same as before.
"""
import asyncio
import json
import os
import sys
import time
from contextlib import AsyncExitStack
from pathlib import Path

import httpx
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

load_dotenv()

LOG_DIR = Path(__file__).resolve().parent / "logs"
VERDICT_LOG = LOG_DIR / "judge_verdicts.jsonl"

ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1")
MAX_TOOL_TURNS = 6

JUDGE_SYSTEM_PROMPT = """Sos un revisor de seguridad y calidad, independiente y escéptico, \
para un pipeline que conecta tickets de Jira con un agente de código autónomo. \
Tu trabajo es encontrar problemas, no confirmar que todo estuvo bien.

Tenés acceso a herramientas reales sobre el grafo de dependencias (Neo4j) y \
sobre código/histórico indexado (Qdrant) — usalas para VERIFICAR las afirmaciones \
del contexto que te dan, no des por cierto lo que dice el texto sin chequearlo \
cuando tengas dudas razonables. Por ejemplo: si el contexto dice que un cambio \
en un servicio no afecta a otros, consultá el grafo vos mismo antes de confiarlo.

Evaluás tres cosas: (1) si la decisión del firewall (APPROVED/REJECTED) fue \
correcta dado el contexto real del ticket, (2) si el cambio de código \
propuesto/aplicado resuelve el problema del ticket sin introducir riesgos \
nuevos — considerando tanto el diff como el resultado del testing agent que \
ya corrió antes que vos (un test que pasa no prueba que el alcance de esos \
tests sea el correcto para este cambio) —, y (3) si la corrida completa \
tiene sentido de principio a fin. Si algo te genera duda razonable, marcá \
FLAGGED — el costo de un falso bloqueo es mucho menor que dejar pasar algo mal.

Cuando termines de investigar (con o sin herramientas), respondé con texto \
plano que sea ÚNICAMENTE un objeto JSON, sin texto antes ni después, con este \
esquema exacto: {"verdict": "OK" o "FLAGGED", "firewall_assessment": "...", \
"change_assessment": "...", "reasoning": "..."}"""

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


def _extract_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


async def _connect_mcp_servers(stack: AsyncExitStack) -> dict:
    """Best-effort: a server that's unreachable (uvx missing, Neo4j/Qdrant
    down) is skipped, not fatal — the judge just has fewer tools that run.
    """
    sessions = {}
    for name, params in MCP_SERVERS.items():
        try:
            read, write = await stack.enter_async_context(stdio_client(params))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            sessions[name] = session
        except Exception as exc:
            print(f"(juez: no se pudo conectar al MCP '{name}', se omite: {exc})", file=sys.stderr)
    return sessions


def _mcp_tools_to_anthropic_format(server_name: str, tools) -> list:
    return [
        {
            "name": f"{server_name}__{t.name}",
            "description": t.description or "",
            "input_schema": t.inputSchema,
        }
        for t in tools
    ]


async def _call_mcp_tool(sessions: dict, qualified_name: str, tool_input: dict) -> str:
    server_name, tool_name = qualified_name.split("__", 1)
    session = sessions[server_name]
    result = await session.call_tool(tool_name, tool_input)
    return "\n".join(getattr(c, "text", str(c)) for c in result.content)


def _build_user_prompt(payload: dict) -> str:
    return f"""Ticket: {payload['ticket'].get('ticket_id')} — {payload['ticket'].get('summary')}
Descripcion: {payload['ticket'].get('description')}
Componente: {payload['ticket'].get('repository_origen')}

Decision del firewall: {payload['firewall'].get('status')}
Motivo (si rechazo): {payload['firewall'].get('reason')}
Redacciones aplicadas: {payload['firewall'].get('redactions_applied')}

Fuente del cambio: {payload.get('change_source')}
Contenido del cambio (diff real, o el texto del issue si el agente en la nube \
todavia no genero un PR):
{payload.get('change_description')}

Resultado del testing agent (build/test real del modulo, ya corrio ANTES que \
vos — si llego a esta etapa es porque paso, pero fijate si el alcance de los \
tests es suficiente para el cambio real, no asumas que "paso" significa \
"esta bien probado"):
{payload.get('test_summary', 'sin tests corridos para esta corrida')}"""


def _select_backend() -> str:
    """Anthropic first (best quality); local Ollama as a free/offline
    fallback if reachable; otherwise the judge is skipped entirely — the
    same behavior as before this got a fallback.
    """
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    try:
        httpx.get(f"{OLLAMA_URL}/api/tags", timeout=3.0)
        return "ollama"
    except httpx.HTTPError:
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


# Precios aproximados por millon de tokens (USD) — solo para tener una
# estimacion de costo en los evals/logs, no son precios contractuales.
# Ajustar si cambian o si se usa otro modelo.
ANTHROPIC_PRICING_PER_MILLION = {
    "claude-sonnet-5": {"input": 3.0, "output": 15.0},
    "claude-opus-4-8": {"input": 15.0, "output": 75.0},
    "claude-haiku-4-5-20251001": {"input": 0.8, "output": 4.0},
}


def _estimate_cost_usd(model: str, input_tokens: int, output_tokens: int) -> float:
    pricing = ANTHROPIC_PRICING_PER_MILLION.get(model)
    if not pricing:
        return 0.0
    return (input_tokens * pricing["input"] + output_tokens * pricing["output"]) / 1_000_000


async def _call_model_turn(client: httpx.AsyncClient, backend: str, messages: list, tools: list) -> tuple:
    """Returns (content_blocks, stop_reason, usage) normalized to the
    Anthropic content-block shape regardless of which backend answered.
    usage = {"input_tokens": int, "output_tokens": int} (0s for Ollama,
    which is free/local so cost tracking doesn't apply the same way).
    """
    if backend == "anthropic":
        request_body = {
            "model": ANTHROPIC_MODEL,
            "max_tokens": 1536,
            "system": JUDGE_SYSTEM_PROMPT,
            "messages": messages,
        }
        if tools:
            request_body["tools"] = tools

        resp = await client.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": os.environ["ANTHROPIC_API_KEY"],
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=request_body,
            timeout=60.0,
        )
        resp.raise_for_status()
        data = resp.json()
        usage = data.get("usage", {})
        return (
            data["content"],
            data.get("stop_reason", "end_turn"),
            {"input_tokens": usage.get("input_tokens", 0), "output_tokens": usage.get("output_tokens", 0)},
        )

    if backend == "ollama":
        ollama_messages = [{"role": "system", "content": JUDGE_SYSTEM_PROMPT}] + _messages_to_ollama(messages)
        request_body = {"model": OLLAMA_MODEL, "messages": ollama_messages, "stream": False}
        if tools:
            request_body["tools"] = _tools_to_ollama_format(tools)

        resp = await client.post(f"{OLLAMA_URL}/api/chat", json=request_body, timeout=120.0)
        resp.raise_for_status()
        data = resp.json()
        blocks, stop_reason = _ollama_response_to_blocks(data.get("message", {}))
        usage = {"input_tokens": data.get("prompt_eval_count", 0), "output_tokens": data.get("eval_count", 0)}
        return blocks, stop_reason, usage

    raise RuntimeError("ni ANTHROPIC_API_KEY ni un Ollama local disponible — el juez no puede correr")


async def judge_with_tools(payload: dict) -> dict:
    backend = _select_backend()
    print(f"(juez: usando backend '{backend}')", file=sys.stderr)

    start_time = time.monotonic()
    total_input_tokens = 0
    total_output_tokens = 0

    def _finalize(verdict: dict) -> dict:
        verdict["_meta"] = {
            "backend": backend,
            "latency_seconds": round(time.monotonic() - start_time, 2),
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "estimated_cost_usd": (
                round(_estimate_cost_usd(ANTHROPIC_MODEL, total_input_tokens, total_output_tokens), 6)
                if backend == "anthropic"
                else 0.0
            ),
        }
        return verdict

    async with AsyncExitStack() as stack:
        sessions = await _connect_mcp_servers(stack)

        tools = []
        for name, session in sessions.items():
            try:
                listed = await session.list_tools()
                tools.extend(_mcp_tools_to_anthropic_format(name, listed.tools))
            except Exception as exc:
                print(f"(juez: no se pudieron listar tools de '{name}': {exc})", file=sys.stderr)

        messages = [{"role": "user", "content": _build_user_prompt(payload)}]

        async with httpx.AsyncClient() as client:
            for _ in range(MAX_TOOL_TURNS):
                content, stop_reason, usage = await _call_model_turn(client, backend, messages, tools)
                total_input_tokens += usage.get("input_tokens", 0)
                total_output_tokens += usage.get("output_tokens", 0)
                messages.append({"role": "assistant", "content": content})

                if stop_reason != "tool_use":
                    final_text = next((b["text"] for b in content if b.get("type") == "text"), "")
                    return _finalize(_extract_json(final_text))

                tool_results = []
                for block in content:
                    if block.get("type") == "tool_use":
                        try:
                            output = await _call_mcp_tool(sessions, block["name"], block.get("input", {}))
                        except Exception as exc:
                            output = f"error llamando a la herramienta: {exc}"
                        tool_results.append(
                            {"type": "tool_result", "tool_use_id": block["id"], "content": output}
                        )
                messages.append({"role": "user", "content": tool_results})

    raise RuntimeError("el juez agoto los turnos de herramientas sin dar un veredicto final")


def log_verdict(ticket_id: str, verdict: dict):
    """Flattens _meta (backend/latency/tokens/cost) into the top level of
    the log entry so evals/run_judge_evals.py and report_sprint_metrics.py
    can aggregate them with plain jq, no nested-field gymnastics.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    meta = verdict.pop("_meta", {})
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ticket_id": ticket_id,
        **verdict,
        **meta,
    }
    with VERDICT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def main():
    payload = json.loads(sys.stdin.read())

    try:
        verdict = asyncio.run(judge_with_tools(payload))
    except KeyError as exc:
        print(json.dumps({"error": f"missing_env_var:{exc.args[0]}"}), file=sys.stderr)
        sys.exit(1)
    except (httpx.HTTPError, json.JSONDecodeError, RuntimeError) as exc:
        print(json.dumps({"error": "judge_call_failed", "detail": str(exc)}), file=sys.stderr)
        sys.exit(1)

    log_verdict(payload["ticket"].get("ticket_id", "UNKNOWN"), verdict)
    print(json.dumps(verdict, ensure_ascii=False))


if __name__ == "__main__":
    main()
