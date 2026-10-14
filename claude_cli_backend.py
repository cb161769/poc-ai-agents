"""Backend "claude_cli": invoca el CLI de Claude Code en modo headless
(`claude -p`) como un modelo mas, usando la sesion/suscripcion ya logueada
del CLI en vez de ANTHROPIC_API_KEY.

El CLI no expone tool-calling "del cliente" (tools que ejecuta ESTE proceso,
no el CLI) -- asi que se emula: con tools ofrecidas se le pide al CLI una
salida estructurada (--json-schema) con la forma
{"text": str, "tool_calls": [{"name", "arguments"}]}, y aca se traduce a los
mismos bloques text/tool_use estilo Anthropic que ya consume el resto del
pipeline (agent_loop._call_model_turn devuelve siempre ese formato). Las
tools built-in del propio CLI se desactivan (--tools "") y no se carga
ningun MCP (--strict-mcp-config sin --mcp-config): el CLI solo razona, las
tools reales las ejecuta el agente que lo llama.
"""
import asyncio
import json
import os
import shutil
import tempfile
import uuid
from pathlib import Path

from log_utils import get_logger

logger = get_logger(__name__)

CLAUDE_CLI_TIMEOUT_SECONDS = float(os.environ.get("CLAUDE_CLI_TIMEOUT_SECONDS", "300"))
# Vacio = el modelo default del CLI (alias o nombre completo, ej. "sonnet").
CLAUDE_CLI_MODEL = os.environ.get("CLAUDE_CLI_MODEL", "")


def find_claude_cli() -> str | None:
    """CLAUDE_CLI_PATH > 'claude' en PATH > binario nativo de la extension
    de VS Code (la instalacion que ya existe en esta maquina). None si no
    se encuentra ninguno.
    """
    explicit = os.environ.get("CLAUDE_CLI_PATH")
    if explicit:
        return explicit if Path(explicit).exists() else None
    on_path = shutil.which("claude")
    if on_path:
        return on_path
    extensions_dir = Path.home() / ".vscode" / "extensions"
    candidates = sorted(extensions_dir.glob("anthropic.claude-code-*/resources/native-binary/claude*"))
    candidates = [c for c in candidates if c.is_file()]
    return str(candidates[-1]) if candidates else None


def claude_cli_available() -> bool:
    return find_claude_cli() is not None


def _flatten_messages(messages: list) -> str:
    """Historial estilo Anthropic (content = str o bloques text/tool_use/
    tool_result) -> transcript de texto plano para stdin del CLI.
    """
    tool_name_by_id = {}
    lines = []
    for m in messages:
        content = m["content"]
        role = "USUARIO" if m["role"] == "user" else "ASISTENTE"
        if isinstance(content, str):
            lines.append(f"[{role}]\n{content}")
            continue
        for b in content:
            kind = b.get("type")
            if kind == "text":
                lines.append(f"[{role}]\n{b['text']}")
            elif kind == "tool_use":
                tool_name_by_id[b.get("id")] = b.get("name")
                lines.append(f"[ASISTENTE llamo a la herramienta '{b.get('name')}']\n{json.dumps(b.get('input', {}), ensure_ascii=False)}")
            elif kind == "tool_result":
                name = tool_name_by_id.get(b.get("tool_use_id"), "?")
                lines.append(f"[RESULTADO de la herramienta '{name}']\n{b.get('content', '')}")
    return "\n\n".join(lines)


_TOOL_PROTOCOL_HEADER = (
    "=== PROTOCOLO DE HERRAMIENTAS (LEER PRIMERO, TIENE PRIORIDAD) ===\n"
    "Todas las herramientas que menciona el prompt que sigue (read_file, list_directory, write_file, ...) son "
    "REALES pero NO existen como tool-use nativo en esta sesion: intentar invocarlas de forma nativa falla "
    "con 'No such tool available'. Se invocan UNICAMENTE escribiendolas en el campo tool_calls de tu respuesta "
    "JSON; quien te invoca las ejecuta de verdad y te devuelve el resultado en el siguiente mensaje. "
    "Cada vez que el prompt diga 'llama/usa/investiga con <herramienta>', eso significa: agregala a tool_calls "
    "en ESTE turno (no te bloquees ni declares que no hay herramientas).\n\n"
)


def _tool_protocol_footer(tools: list) -> str:
    catalog = "\n".join(
        f"- {t['name']}: {t['description']}\n  input_schema: {json.dumps(t['input_schema'], ensure_ascii=False)}"
        for t in tools
    )
    return (
        "\n\n=== HERRAMIENTAS DISPONIBLES (se invocan SOLO via tool_calls) ===\n"
        f"{catalog}\n\n"
        "Respondé SIEMPRE con un objeto JSON {\"text\": str, \"tool_calls\": [...]}. Para usar herramientas, "
        "pone en tool_calls una o mas {\"name\": <nombre exacto>, \"arguments\": {...segun su input_schema}}; "
        "veras sus resultados en el siguiente mensaje. Pasa los argumentos (paths incluidos) tal cual los pide el "
        "prompt/la tarea: NO los reescribas con tu directorio de trabajo, que no tiene relacion con el repo del "
        "agente. Cuando ya no necesites herramientas, deja tool_calls vacio y pone en text tu respuesta final "
        "EXACTAMENTE en el formato que pide el prompt (si pide un JSON, text es ese JSON serializado como string)."
    )


def _tool_call_schema(tools: list) -> dict:
    return {
        "type": "object",
        "properties": {
            "text": {"type": "string"},
            "tool_calls": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "enum": [t["name"] for t in tools]},
                        "arguments": {"type": "object"},
                    },
                    "required": ["name", "arguments"],
                },
            },
        },
        "required": ["text", "tool_calls"],
    }


_PROTOCOL_RETRIES = 2


async def _run_cli(cli: str, full_system: str, transcript: str, schema: dict | None, model: str) -> dict:
    # El system prompt va por archivo y el transcript por stdin: ambos pueden
    # pasar el limite de ~32k caracteres de linea de comandos de Windows.
    with tempfile.TemporaryDirectory(prefix="claude_cli_") as workdir:
        system_file = Path(workdir) / "system.txt"
        system_file.write_text(full_system, encoding="utf-8")
        args = [
            cli, "-p", "--output-format", "json", "--tools", "", "--strict-mcp-config",
            "--no-session-persistence", "--system-prompt-file", str(system_file),
        ]
        if model:
            args += ["--model", model]
        if schema is not None:
            args += ["--json-schema", json.dumps(schema)]

        proc = await asyncio.create_subprocess_exec(
            *args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE, cwd=workdir,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(transcript.encode("utf-8")), timeout=CLAUDE_CLI_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise RuntimeError(f"claude CLI no respondio en {CLAUDE_CLI_TIMEOUT_SECONDS}s")

    if proc.returncode != 0:
        raise RuntimeError(f"claude CLI salio con {proc.returncode}: {stderr.decode('utf-8', 'replace')[:500]}")
    try:
        return json.loads(stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"claude CLI devolvio salida no-JSON: {exc}")


async def call_claude_cli(
    messages: list, tools: list, system_prompt: str, model: str = None,
    force_json: bool = False, json_schema: dict | None = None,
) -> tuple:
    """Devuelve (content_blocks, stop_reason, usage) -- misma forma que el
    resto de los backends de agent_loop._call_model_turn.
    """
    cli = find_claude_cli()
    if cli is None:
        raise RuntimeError("claude CLI no encontrado (CLAUDE_CLI_PATH, PATH o extension de VS Code)")

    full_system = system_prompt
    schema = None
    if tools:
        full_system = _TOOL_PROTOCOL_HEADER + system_prompt + _tool_protocol_footer(tools)
        schema = _tool_call_schema(tools)
    elif force_json:
        schema = json_schema
        if schema is None:
            full_system += "\n\nRespondé UNICAMENTE con un objeto JSON valido, sin texto antes ni despues."

    # El system prompt va por archivo y el transcript por stdin: ambos pueden
    # pasar el limite de ~32k caracteres de linea de comandos de Windows.
    transcript = _flatten_messages(messages)
    data = None
    for attempt in range(1 + _PROTOCOL_RETRIES):
        data = await _run_cli(cli, full_system, transcript, schema, model or CLAUDE_CLI_MODEL)
        if not tools or data.get("structured_output") is not None or attempt == _PROTOCOL_RETRIES:
            break
        # Con tools ofrecidas el CLI tiene que devolver el esquema {text,
        # tool_calls}; si no vino estructurado, el modelo intento invocar las
        # tools de forma nativa (fallan con "No such tool available") y
        # contesto texto libre. Un reintento acotado con recordatorio.
        logger.warning("claude_cli: respuesta sin el esquema de tool_calls (¿tool-use nativo?) -- reintentando con recordatorio")
        transcript += (
            "\n\n[RECORDATORIO DEL SISTEMA] Tu respuesta anterior NO uso el esquema {text, tool_calls}. NO invoques "
            "herramientas de forma nativa (fallan con 'No such tool available'): ponelas en tool_calls y respondé "
            "con ese objeto JSON."
        )
    if data.get("is_error"):
        raise RuntimeError(f"claude CLI reporto error: {str(data.get('result'))[:500]}")

    raw_usage = data.get("usage", {})
    usage = {
        "input_tokens": raw_usage.get("input_tokens", 0),
        "output_tokens": raw_usage.get("output_tokens", 0),
        "cache_read_input_tokens": raw_usage.get("cache_read_input_tokens", 0),
        "cache_creation_input_tokens": raw_usage.get("cache_creation_input_tokens", 0),
    }
    return _cli_result_to_blocks(data, bool(tools)) + (usage,)


def _unwrap_nested_protocol(payload: dict) -> dict:
    """A veces el modelo anida el envoltorio dentro de su propio campo text
    (text = '{"text": ..., "tool_calls": [...]}'). Si text parsea como un
    envoltorio del protocolo, se usa ese."""
    for _ in range(2):
        text = payload.get("text")
        if not (isinstance(text, str) and text.lstrip().startswith("{")):
            break
        try:
            inner = json.loads(text)
        except json.JSONDecodeError:
            break
        if not (isinstance(inner, dict) and ("tool_calls" in inner or set(inner) == {"text"})):
            break
        payload = {"text": inner.get("text", ""), "tool_calls": (payload.get("tool_calls") or []) + (inner.get("tool_calls") or [])}
    return payload


def _cli_result_to_blocks(data: dict, tools_offered: bool) -> tuple:
    structured = data.get("structured_output")
    result_text = data.get("result") or ""

    if tools_offered:
        payload = structured if isinstance(structured, dict) else None
        if payload is None:
            try:
                payload = json.loads(result_text)
            except json.JSONDecodeError:
                payload = None
        if not isinstance(payload, dict):
            # El CLI no respeto el protocolo: se trata como texto final (el
            # caller ya maneja "no es JSON valido" con su propio reintento).
            return ([{"type": "text", "text": result_text}], "end_turn")
        payload = _unwrap_nested_protocol(payload)
        blocks = []
        if payload.get("text"):
            blocks.append({"type": "text", "text": payload["text"]})
        calls = payload.get("tool_calls") or []
        for i, call in enumerate(calls):
            args = call.get("arguments")
            blocks.append({
                "type": "tool_use", "id": f"claude_cli_call_{uuid.uuid4().hex[:12]}", "name": call.get("name"),
                "input": args if isinstance(args, dict) else {},
            })
        return (blocks, "tool_use" if calls else "end_turn")

    if structured is not None:
        result_text = json.dumps(structured, ensure_ascii=False)
    return ([{"type": "text", "text": result_text}], "end_turn")
