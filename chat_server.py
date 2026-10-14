"""GUI web local para interactuar con los agents (coding agent, juez, epic
planner) sobre ADK: arranca una corrida, muestra en vivo cada tool-call /
resultado / texto del modelo y la consola (diffs, comandos), y deja aprobar o
rechazar desde el navegador las confirmaciones [s/n] de write_file / edit_file
/ run_shell_command (el hook `confirm_hook` de coding_agent.run_coding_agent).

Uso:
  python chat_server.py            # http://127.0.0.1:8765
  CHAT_SERVER_PORT=9000 python chat_server.py

Seguridad: es una herramienta LOCAL que puede escribir archivos y correr
comandos en el repo elegido (previa confirmacion humana). Escucha solo en
127.0.0.1 y el WebSocket rechaza Origins que no sean el propio servidor, para
que una pagina web cualquiera no pueda manejarlo desde tu navegador.
"""
import asyncio
import contextlib
import io
import json
import logging
import os
import sys
import uuid
from pathlib import Path
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

import adk_bridge
import agent_loop
from log_utils import get_logger

logger = get_logger(__name__)

HOST = "127.0.0.1"
PORT = int(os.environ.get("CHAT_SERVER_PORT", "8765"))
GUI_DIR = Path(__file__).resolve().parent / "gui"

app = FastAPI(title="poc-ai-agents GUI")


@app.get("/")
async def index():
    return FileResponse(GUI_DIR / "index.html")


@app.get("/api/info")
async def info():
    return {
        "backends": ["auto", "anthropic", "claude_cli", "ollama"],
        "claude_cli_available": agent_loop.claude_cli_available(),
        "default_repo": str(Path.cwd()),
    }


def _origin_allowed(origin: str | None) -> bool:
    if not origin:
        return True  # clientes no-navegador (tests, curl) no mandan Origin
    parsed = urlparse(origin)
    return parsed.hostname in ("127.0.0.1", "localhost") and (parsed.port or 80) == PORT


class _ConsoleTee(io.TextIOBase):
    """Reemplaza sys.stderr durante una corrida: los diffs/comandos que las
    tools imprimen para el humano llegan a la GUI en vez de a la terminal."""

    def __init__(self, emit):
        self._emit = emit

    def write(self, text):
        if text:
            self._emit({"type": "console", "text": text})
        return len(text)

    def flush(self):
        pass


class _LogForwarder(logging.Handler):
    def __init__(self, emit):
        super().__init__(level=logging.INFO)
        self._emit = emit

    def emit(self, record):
        self._emit({"type": "log", "level": record.levelname, "logger": record.name, "text": record.getMessage()})


async def _run_agent(agent: str, params: dict, confirm_hook) -> dict:
    if agent == "coding":
        import coding_agent

        repo = Path(params["repo_dir"]).expanduser()
        if not repo.is_dir():
            raise ValueError(f"repo_dir no existe o no es un directorio: {repo}")
        return await coding_agent.run_coding_agent(
            params.get("ticket_id") or "GUI-1", params["prompt"], str(repo), confirm_hook=confirm_hook,
        )
    if agent == "judge":
        import judge_agent

        return await judge_agent.judge_with_tools(json.loads(params["payload_json"]))
    if agent == "epic":
        import epic_planner

        return await epic_planner.plan_epic(json.loads(params["epic_json"]), json.loads(params["children_json"]))
    raise ValueError(f"agente desconocido: {agent}")


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    if not _origin_allowed(websocket.headers.get("origin")):
        await websocket.close(code=1008)
        return
    await websocket.accept()

    loop = asyncio.get_running_loop()
    outbox: asyncio.Queue = asyncio.Queue()
    pending_confirms: dict = {}
    run_task: asyncio.Task | None = None

    def emit(event: dict) -> None:
        # Thread-safe: las tools sync pueden correr en hilos del executor.
        loop.call_soon_threadsafe(outbox.put_nowait, event)

    async def sender():
        while True:
            await websocket.send_json(await outbox.get())

    async def confirm_hook(prompt_text: str) -> bool:
        confirm_id = uuid.uuid4().hex
        future = loop.create_future()
        pending_confirms[confirm_id] = future
        emit({"type": "confirm", "id": confirm_id, "prompt": prompt_text})
        try:
            return await future
        finally:
            pending_confirms.pop(confirm_id, None)

    async def run(agent: str, params: dict, backend: str):
        previous_priority = os.environ.get("LLM_BACKEND_PRIORITY")
        forwarder = _LogForwarder(emit)
        logging.getLogger().addHandler(forwarder)
        sink_token = adk_bridge.event_sink.set(emit)
        try:
            if backend != "auto":
                os.environ["LLM_BACKEND_PRIORITY"] = backend
            agent_loop._backends_failed_hard_this_run.clear()
            emit({"type": "status", "state": "running", "agent": agent})
            with contextlib.redirect_stderr(_ConsoleTee(emit)):
                result = await _run_agent(agent, params, confirm_hook)
            result.pop("_conversation_file", None)
            emit({"type": "result", "data": result})
            emit({"type": "status", "state": "done"})
        except asyncio.CancelledError:
            emit({"type": "status", "state": "cancelled"})
            raise
        except Exception as exc:
            logger.warning(f"GUI: la corrida de '{agent}' fallo: {exc}")
            emit({"type": "error", "text": f"{type(exc).__name__}: {exc}"})
            emit({"type": "status", "state": "error"})
        finally:
            adk_bridge.event_sink.reset(sink_token)
            logging.getLogger().removeHandler(forwarder)
            if previous_priority is None:
                os.environ.pop("LLM_BACKEND_PRIORITY", None)
            else:
                os.environ["LLM_BACKEND_PRIORITY"] = previous_priority

    sender_task = asyncio.create_task(sender())
    try:
        while True:
            message = await websocket.receive_json()
            action = message.get("action")
            if action == "start":
                if run_task is not None and not run_task.done():
                    emit({"type": "error", "text": "ya hay una corrida en curso"})
                    continue
                run_task = asyncio.create_task(
                    run(message.get("agent", ""), message.get("params", {}), message.get("backend", "auto"))
                )
            elif action == "confirm":
                future = pending_confirms.get(message.get("id"))
                if future is not None and not future.done():
                    future.set_result(bool(message.get("approve")))
            elif action == "cancel" and run_task is not None and not run_task.done():
                run_task.cancel()
    except WebSocketDisconnect:
        pass
    finally:
        # Cerrar la pestana = rechazar lo pendiente y cortar la corrida.
        for future in pending_confirms.values():
            if not future.done():
                future.set_result(False)
        if run_task is not None and not run_task.done():
            run_task.cancel()
        sender_task.cancel()


if __name__ == "__main__":
    print(f"GUI de agents en http://{HOST}:{PORT}", file=sys.stderr)
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
