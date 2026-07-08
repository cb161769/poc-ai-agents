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
import copy
import json
import os
import sys
import time
from contextlib import AsyncExitStack
from pathlib import Path

import httpx
from dotenv import load_dotenv
from mcp import StdioServerParameters

from agent_loop import (  # noqa: F401 -- _messages_to_ollama/_ollama_response_to_blocks/_estimate_cost_usd
    ANTHROPIC_MODEL,       # re-exported so existing imports/tests of judge_agent keep working
    _call_mcp_tool,
    _call_model_turn,
    _connect_mcp_servers,
    _estimate_cost_usd,
    _final_text_with_json_retry,
    _messages_to_ollama,
    _mcp_tools_to_anthropic_format,
    _ollama_response_to_blocks,
    _select_backend,
)
from firewall_proxy import _redact

load_dotenv()

LOG_DIR = Path(__file__).resolve().parent / "logs"
VERDICT_LOG = LOG_DIR / "judge_verdicts.jsonl"

MAX_TOOL_TURNS = 6

JUDGE_SYSTEM_PROMPT = """Sos un revisor de seguridad y calidad, independiente y escéptico, \
para un pipeline que conecta tickets de Jira con un agente de código autónomo. \
Tu trabajo es encontrar problemas, no confirmar que todo estuvo bien.

Tenés acceso a herramientas reales sobre el grafo de dependencias (Neo4j) y \
sobre código/histórico indexado (Qdrant) — usalas para VERIFICAR las afirmaciones \
del contexto que te dan, no des por cierto lo que dice el texto sin chequearlo \
cuando tengas dudas razonables. Por ejemplo: si el contexto dice que un cambio \
en un servicio no afecta a otros, consultá el grafo vos mismo antes de confiarlo. \
Cuando SÍ uses una herramienta, citá en tu "reasoning" el resultado concreto que \
obtuviste (qué devolvió el grafo, qué encontraste en Qdrant) — no alcanza con decir \
"verifiqué el grafo", decí qué viste ahí exactamente.

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
        sessions = await _connect_mcp_servers(stack, MCP_SERVERS, label="juez")

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
                content, stop_reason, usage = await _call_model_turn(client, backend, messages, tools, JUDGE_SYSTEM_PROMPT)
                total_input_tokens += usage.get("input_tokens", 0)
                total_output_tokens += usage.get("output_tokens", 0)
                messages.append({"role": "assistant", "content": content})

                if stop_reason != "tool_use":
                    final_text = next((b["text"] for b in content if b.get("type") == "text"), "")
                    try:
                        return _finalize(_extract_json(final_text))
                    except json.JSONDecodeError:
                        # Un solo reintento acotado -- si el modelo tampoco
                        # devuelve JSON valido esta vez, se deja propagar y
                        # main() lo maneja como error, sin loop infinito.
                        retry_text, retry_usage = await _final_text_with_json_retry(
                            client, backend, messages, tools, JUDGE_SYSTEM_PROMPT
                        )
                        total_input_tokens += retry_usage.get("input_tokens", 0)
                        total_output_tokens += retry_usage.get("output_tokens", 0)
                        return _finalize(_extract_json(retry_text))

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


def _redact_payload_for_logging(payload: dict) -> dict:
    """payload["ticket"]["description"] and payload["change_description"]
    never went through the firewall's egress redaction (that only redacts
    the composed prompt before it reaches the coding agent, not the raw
    ticket or the diff Copilot ends up producing) -- they already reach the
    judge unredacted via the API call. Persisting them to disk for the
    review/promote-to-evals workflow is a NEW exposure that call didn't
    have, so apply the same redaction firewall_proxy already uses before
    writing anything to judge_verdicts.jsonl.
    """
    redacted = copy.deepcopy(payload)
    description = (redacted.get("ticket") or {}).get("description")
    if description:
        redacted["ticket"]["description"], _ = _redact(description)
    change_description = redacted.get("change_description")
    if change_description:
        redacted["change_description"], _ = _redact(change_description)
    return redacted


def log_verdict(ticket_id: str, verdict: dict, payload: dict):
    """Flattens _meta (backend/latency/tokens/cost) into the top level of
    the log entry so evals/run_judge_evals.py and report_sprint_metrics.py
    can aggregate them with plain jq, no nested-field gymnastics.

    Also persists the (redacted) input payload -- scripts/review_judge_verdicts.py
    and scripts/promote_reviews_to_evals.py need it to turn a real run into a
    new evals/judge_eval_cases.jsonl case once a human confirms or corrects
    the verdict.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    meta = verdict.pop("_meta", {})
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ticket_id": ticket_id,
        **verdict,
        **meta,
        "payload": _redact_payload_for_logging(payload),
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

    log_verdict(payload["ticket"].get("ticket_id", "UNKNOWN"), verdict, payload)
    print(json.dumps(verdict, ensure_ascii=False))


if __name__ == "__main__":
    main()
