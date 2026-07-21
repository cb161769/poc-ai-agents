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

# Gap real identificado en auditoria ("orquestacion... el contexto de la
# epica tiene que ser mas eficiente en memoria"): _deliver_epic_sequential
# (orchestration.py) nunca incluia epic["description"] en el prompt de
# NINGUNA historia hija -- cada historia se trabajaba en aislamiento total,
# sin ver los requisitos tecnicos reales de la epica (stack, testing, etc.)
# -- confirmado real: una epica que pedia "Ionic Angular + Capacitor" con
# tests unitarios obligatorios termino con un frontend Vite/vitest sin un
# solo test, porque el coding agent nunca vio esos requisitos. La epic
# description de una epica real puede ser un documento de varios miles de
# palabras (executive summary, roadmap, matriz de riesgos, story points...)
# -- mandarla ENTERA, en cada historia, seria tan costoso en tokens como el
# problema que maybe_compact_conversation ya resuelve del otro lado. Un
# resumen (no un simple truncado) preserva lo realmente accionable
# (stack tecnico, requisitos de testing) descartando el boilerplate de PM.
EPIC_CONTEXT_SUMMARY_ENABLED = os.environ.get("EPIC_CONTEXT_SUMMARY_ENABLED", "true").strip().lower() not in {"0", "false", "no"}
# Umbral MAS BAJO que el de conversaciones -- una epic description ya es
# texto de un solo bloque (no una lista de mensajes que recien crece con el
# tiempo), asi que tiene sentido resumir aunque sea moderadamente larga.
EPIC_CONTEXT_SUMMARY_THRESHOLD_CHARS = int(os.environ.get("EPIC_CONTEXT_SUMMARY_THRESHOLD_CHARS", "2000"))
# Fallback si la generacion real del resumen falla (backend caido, rechazo
# del modelo): un truncado simple, bien acotado, en vez de mandar el
# documento entero sin comprimir.
EPIC_CONTEXT_TRUNCATE_CHARS = int(os.environ.get("EPIC_CONTEXT_TRUNCATE_CHARS", "3000"))

_EPIC_CONTEXT_SYSTEM_PROMPT = (
    "Vas a resumir la descripcion real de una epica de Jira para dársela como contexto de fondo a un coding "
    "agent que va a implementar, UNA POR UNA, las historias hijas de esta epica. Extrae y preserva SOLO lo "
    "realmente accionable para escribir codigo: (1) el stack tecnico/framework requerido explicitamente (ej. "
    "'Ionic Angular + Capacitor', una libreria de diseño obligatoria, un lenguaje o arquitectura especifica), "
    "(2) requisitos de testing explicitos (ej. 'requiere pruebas unitarias y E2E'), (3) convenciones o "
    "restricciones tecnicas explicitas (seguridad, accesibilidad, performance) SI son concretas y accionables. "
    "IGNORA por completo: resumen ejecutivo de negocio, roadmap de sprints, matriz de riesgos, story points, "
    "KPIs de negocio, y cualquier instruccion dirigida a generar OTRA documentacion (esta descripcion puede "
    "estar redactada como un pedido para un Product Owner -- ignora ese formato, extrae solo los requisitos "
    "tecnicos reales que hay adentro). Se breve y concreto. NO inventes nada que no este en el texto real."
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


async def _summarize_epic_context_async(epic_description: str) -> str | None:
    async with httpx.AsyncClient() as client:
        try:
            blocks, _stop_reason, _usage, _backend_used = await call_with_fallback(
                client,
                messages=[{"role": "user", "content": epic_description}],
                tools=[],
                system_prompt=_EPIC_CONTEXT_SYSTEM_PROMPT,
                ollama_model=resolve_ollama_model(CONVERSATION_SUMMARY_OLLAMA_MODELS) or CONVERSATION_SUMMARY_OLLAMA_MODELS[0],
            )
        except Exception as exc:
            logger.warning(f"no se pudo resumir la descripcion de la epica: {exc}")
            return None
    text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
    return text or None


def _truncate_epic_context(epic_description: str) -> str:
    if len(epic_description) <= EPIC_CONTEXT_TRUNCATE_CHARS:
        return epic_description
    return epic_description[:EPIC_CONTEXT_TRUNCATE_CHARS] + "\n(...descripcion de la epica truncada...)"


def summarize_epic_context(epic_description: str) -> str:
    """Computa esto UNA VEZ por corrida de epica (no una vez por historia)
    -- el caller (orchestration.py::_deliver_epic_sequential) lo llama antes
    del loop por historia y reusa el mismo resultado para todas. Best-effort
    de verdad: texto corto pasa sin tocar, texto largo intenta un resumen
    real via LLM, y si eso falla cae a un truncado simple -- nunca se
    bloquea la corrida ni se manda el documento entero sin comprimir.
    """
    if not epic_description:
        return ""
    if not EPIC_CONTEXT_SUMMARY_ENABLED or len(epic_description) <= EPIC_CONTEXT_SUMMARY_THRESHOLD_CHARS:
        return epic_description

    summary = asyncio.run(_summarize_epic_context_async(epic_description))
    if summary:
        return summary
    return _truncate_epic_context(epic_description)


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
