"""Comprobante de desarrollo tecnico poblado por el propio backend LLM
(Ollama u otro segun get_backend_priority()) a partir de evidencia REAL de
una corrida del pipeline -- el texto final lo redacta el modelo via
call_with_fallback, no un template Python con placeholders.

Nace de una corrida manual (armar el prompt a mano, pegarle a Ollama con
curl, pegar el resultado en un comentario de Jira) que se automatiza aca
para que CADA corrida real de --epic deje su propio comprobante, sin
intervencion manual.
"""

import asyncio
import logging
import os

import httpx

from agent_loop import OLLAMA_MODEL, call_with_fallback

logger = logging.getLogger("tech_doc_agent")

# Best-effort: esto documenta una corrida, no la ejecuta -- un fallo aca
# nunca debe bloquear ni romper el pipeline real. Se puede apagar del todo
# si el usuario no lo quiere (ej. corridas de humo repetidas sin necesidad
# de un comprobante por cada una).
TECH_DOC_ENABLED = os.environ.get("TECH_DOC_ENABLED", "true").strip().lower() not in {"0", "false", "no"}
# Mismo criterio que CODING_AGENT_OLLAMA_MODEL/JUDGE_OLLAMA_MODEL: override
# opcional por agente, cae al OLLAMA_MODEL generico si no se setea.
TECH_DOC_OLLAMA_MODEL = os.environ.get("TECH_DOC_OLLAMA_MODEL") or OLLAMA_MODEL

_SYSTEM_PROMPT = (
    "Actua como un Ingeniero de Software Principal y Arquitecto de Soluciones Senior. "
    "Vas a generar un Comprobante de Desarrollo Tecnico detallado, riguroso y listo para "
    "auditoria sobre una corrida real de un pipeline de agentes de IA que usa un backend "
    "LLM local (Ollama) o cloud (Anthropic) como fallback. Usa EXCLUSIVAMENTE los datos "
    "reales que se te dan a continuacion -- no inventes metricas, comandos ni resultados "
    "que no esten en los datos provistos. Si algun dato no fue provisto, decilo "
    "explicitamente como 'no medido' en vez de estimarlo. Responde en español, en "
    "Markdown, con esta estructura de secciones: 1. Resumen Ejecutivo y Objetivo. "
    "2. Ficha Tecnica del Modelo y Entorno. 3. Configuracion del Entorno y Variables de "
    "Entorno. 4. Resultado Real de la Corrida. 5. Prueba de Integracion y Validacion "
    "de API. 6. Metricas de Rendimiento y Eficiencia (KPIs). 7. Control de Errores y "
    "Mitigacion de Riesgos."
)


def _format_evidence(evidence: dict) -> str:
    lines = ["DATOS REALES DE ESTA CORRIDA:"]
    for key, value in evidence.items():
        lines.append(f"- {key}: {value}")
    lines.append("\nGenera el Comprobante de Desarrollo Tecnico completo con estos datos reales.")
    return "\n".join(lines)


async def _generate_async(evidence: dict) -> str | None:
    async with httpx.AsyncClient() as client:
        try:
            blocks, _stop_reason, _usage, backend_used = await call_with_fallback(
                client,
                messages=[{"role": "user", "content": _format_evidence(evidence)}],
                tools=[],
                system_prompt=_SYSTEM_PROMPT,
                ollama_model=TECH_DOC_OLLAMA_MODEL,
            )
        except Exception as exc:
            logger.warning(f"tech_doc_agent: no se pudo generar el comprobante tecnico: {exc}")
            return None

    text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
    if not text:
        return None
    return f"_(Comprobante tecnico generado por el backend '{backend_used}', modelo real -- no redactado a mano)_\n\n{text}"


def generate_technical_report(evidence: dict) -> str | None:
    """Sincrono para llamarse directo desde una @task de Prefect (que ya
    corre dentro de su propio flujo sync). Devuelve None (nunca levanta) si
    TECH_DOC_ENABLED esta apagado, no hay backend disponible, o la
    generacion falla por cualquier motivo -- es una mejora de
    documentacion, no una parte critica de la corrida.
    """
    if not TECH_DOC_ENABLED:
        return None
    try:
        return asyncio.run(_generate_async(evidence))
    except Exception as exc:
        logger.warning(f"tech_doc_agent: fallo inesperado generando el comprobante: {exc}")
        return None
