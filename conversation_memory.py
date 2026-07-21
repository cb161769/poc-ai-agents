"""Compacta conversaciones largas del coding agent para que no crezcan sin
limite a traves de una epica secuencial completa.

Gap real identificado en auditoria ("orquestacion... necesita memoria
acotada"): retry_coding_agent_local_real (orchestration.py) resumia la
MISMA conversacion turno a turno, historia tras historia de una epica, via
conversation_file -- sin ningun tope. Para una epica de 12 historias, el
transcript crudo podia acercarse (o superar) el context window del backend
LOCAL de Ollama (mas chico que el de Anthropic -- ver MODEL_LIMITS en
llm_backends.py), degradando la capacidad del modelo de razonar sobre la
historia ACTUAL en vez de arrastrar todo el historial crudo de las
anteriores.

Best-effort total (mismo criterio que tech_doc_agent.py): si la generacion
del resumen falla o esta apagada, se devuelven los mensajes originales sin
tocar -- esto nunca bloquea ni corrompe una corrida real.
"""
import asyncio
import json
import logging
import os

import httpx

from agent_loop import OLLAMA_MODEL, call_with_fallback, parse_ollama_model_candidates, resolve_ollama_model

logger = logging.getLogger("conversation_memory")

CONVERSATION_SUMMARY_ENABLED = os.environ.get("CONVERSATION_SUMMARY_ENABLED", "true").strip().lower() not in {"0", "false", "no"}
# Umbral en caracteres del JSON serializado de los mensajes -- deliberadamente
# generoso (bien por debajo de un context window real, incluso el mas chico
# de Ollama) para no comprimir conversaciones cortas/normales, solo las que
# genuinamente crecieron mucho a traves de varias historias.
CONVERSATION_SUMMARY_THRESHOLD_CHARS = int(os.environ.get("CONVERSATION_SUMMARY_THRESHOLD_CHARS", "40000"))
# Cuantos mensajes RECIENTES se preservan sin tocar (nunca resumidos) -- el
# turno actual necesita el detalle exacto de lo ultimo que paso, no un resumen.
CONVERSATION_SUMMARY_KEEP_RECENT = int(os.environ.get("CONVERSATION_SUMMARY_KEEP_RECENT", "10"))
CONVERSATION_SUMMARY_OLLAMA_MODELS = parse_ollama_model_candidates(
    os.environ.get("CONVERSATION_SUMMARY_OLLAMA_MODEL", ""), OLLAMA_MODEL
)

_SUMMARY_SYSTEM_PROMPT = (
    "Vas a comprimir el historial de una conversacion tecnica real entre un coding agent y las herramientas que "
    "uso (lectura/escritura de archivos, comandos de shell, tests) mientras trabajaba varias historias de una "
    "epica de Jira, una atras de otra. Genera un resumen COMPACTO en español que preserve, si aparece en el "
    "historial real: (1) que historias/tickets ya se procesaron y el resultado real de cada una (aplico un "
    "cambio real / no aplico nada y por que), (2) que archivos/directorios ya se investigaron, (3) decisiones de "
    "diseno o convenciones del proyecto que el agente ya establecio y debe seguir usando. NO inventes nada que no "
    "este en el historial real -- si algo no aparece, no lo menciones. Se breve: el objetivo es liberar espacio "
    "de contexto, no documentar todo en detalle."
)


def _messages_to_text(messages: list) -> str:
    parts = []
    for m in messages:
        role = m.get("role", "?")
        content = m.get("content")
        if isinstance(content, str):
            parts.append(f"[{role}] {content}")
        elif isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "text":
                    parts.append(f"[{role}] {block.get('text', '')}")
                elif block.get("type") == "tool_use":
                    parts.append(f"[{role}] llamo a la tool '{block.get('name')}' con input {block.get('input')}")
                elif block.get("type") == "tool_result":
                    parts.append(f"[{role}] resultado de tool: {str(block.get('content'))[:500]}")
    return "\n".join(parts)


async def _summarize_async(old_messages_text: str) -> str | None:
    async with httpx.AsyncClient() as client:
        try:
            blocks, _stop_reason, _usage, _backend_used = await call_with_fallback(
                client,
                messages=[{"role": "user", "content": old_messages_text}],
                tools=[],
                system_prompt=_SUMMARY_SYSTEM_PROMPT,
                ollama_model=resolve_ollama_model(CONVERSATION_SUMMARY_OLLAMA_MODELS) or CONVERSATION_SUMMARY_OLLAMA_MODELS[0],
            )
        except Exception as exc:
            logger.warning(f"no se pudo resumir la conversacion: {exc}")
            return None
    text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
    return text or None


def _is_tool_result_message(message: dict) -> bool:
    content = message.get("content")
    return isinstance(content, list) and any(isinstance(b, dict) and b.get("type") == "tool_result" for b in content)


def _safe_cutoff_index(messages: list, target_keep_n: int) -> int:
    """El indice desde el cual "cortar": todo ANTES de este indice se
    resume, todo DESDE este indice en mas se preserva intacto. Nunca puede
    caer en medio de un par tool_use (assistant) / tool_result (user) --
    el backend real (Anthropic) exige que un tool_result siga
    INMEDIATAMENTE a su tool_use, asi que cortar ahi corromperia la
    conversacion para el proximo turno. Retrocede el corte hasta un punto
    seguro (un mensaje que no sea la mitad "result" de un par).
    """
    cutoff = max(0, len(messages) - target_keep_n)
    while cutoff > 0 and _is_tool_result_message(messages[cutoff]):
        cutoff -= 1
    return cutoff


def maybe_compact_conversation(messages: list) -> list:
    """Best-effort: devuelve los mensajes tal cual si el resumen esta
    apagado, si el historial esta por debajo del umbral, si no hay un corte
    seguro, o si la generacion real del resumen falla -- nunca bloquea ni
    corrompe una corrida real, en el peor caso el historial sigue creciendo
    sin comprimir (el comportamiento de antes de este modulo).
    """
    if not CONVERSATION_SUMMARY_ENABLED or not messages:
        return messages

    serialized_size = len(json.dumps(messages, ensure_ascii=False))
    if serialized_size <= CONVERSATION_SUMMARY_THRESHOLD_CHARS:
        return messages

    cutoff = _safe_cutoff_index(messages, CONVERSATION_SUMMARY_KEEP_RECENT)
    if cutoff <= 0:
        return messages

    old_messages, recent_messages = messages[:cutoff], messages[cutoff:]
    old_text = _messages_to_text(old_messages)
    summary = asyncio.run(_summarize_async(old_text))
    if not summary:
        return messages

    summary_message = {
        "role": "user",
        "content": (
            "[RESUMEN DE CONTEXTO PREVIO -- turnos anteriores comprimidos para no exceder el context window, "
            f"generado a partir de {len(old_messages)} mensajes reales]\n\n{summary}"
        ),
    }
    return [summary_message] + recent_messages
