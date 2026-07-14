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
import re

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
    "que no esten en los datos provistos. Responde en español, en Markdown, con esta "
    "estructura de secciones (en este orden si aplican): 1. Resumen Ejecutivo y Objetivo. "
    "2. Ficha Tecnica del Modelo y Entorno. 3. Configuracion del Entorno y Variables de "
    "Entorno. 4. Resultado Real de la Corrida. 5. Prueba de Integracion y Validacion "
    "de API. 6. Metricas de Rendimiento y Eficiencia (KPIs). 7. Control de Errores y "
    "Mitigacion de Riesgos.\n\n"
    "Confirmado real esta sesion: rellenar CADA seccion pase lo que pase produce relleno "
    "generico sin valor real (ej. 'no se proporciona informacion sobre X porque no fue "
    "provista en los datos reales', repetido seccion tras seccion) -- eso es peor que no "
    "escribir la seccion. Regla estricta: si para una seccion de la lista de arriba NO hay "
    "ningun dato real relacionado en la evidencia que se te dio, OMITILA POR COMPLETO -- no "
    "escribas su titulo, ni una frase tipo 'no disponible'/'no medido'/'no se proporciona "
    "informacion'. Un comprobante mas corto con solo las secciones que tienen contenido "
    "real es mejor que uno completo con relleno. Dentro de una seccion que SI escribas, "
    "cada afirmacion tiene que estar respaldada por un dato concreto de la evidencia real -- "
    "si falta un dato puntual DENTRO de una seccion que si tiene otro contenido real, "
    "omiti esa frase puntual en vez de rellenarla."
)


def _format_evidence(evidence: dict) -> str:
    lines = ["DATOS REALES DE ESTA CORRIDA:"]
    for key, value in evidence.items():
        lines.append(f"- {key}: {value}")
    lines.append("\nGenera el Comprobante de Desarrollo Tecnico completo con estos datos reales.")
    return "\n".join(lines)


# Confirmado real esta sesion: pedirle al modelo por prompt que OMITA una
# seccion sin datos reales no alcanza -- modelos locales chicos (ollama)
# igual "completan el patron" de las 7 secciones, rellenando con frases
# tipo "no se proporciona informacion sobre X porque no fue provista en los
# datos reales" en vez de omitir. En vez de confiar en que el modelo
# obedezca, se limpia el resultado con codigo despues: cualquier seccion
# cuyo CUERPO entero sea puro relleno de este tipo se elimina.
_FILLER_PATTERN = re.compile(
    # Confirmado real (KAN-5, segunda vuelta): "no se proporcionAN" (plural)
    # no matcheaba con el patron anterior (solo cubria "proporciona"
    # singular y "proporcion[oó]" pasado) -- se usa el stem del verbo
    # (\w* al final) para cubrir cualquier conjugacion, no una lista fija.
    r"no se (?:proporcion|mencion|especific|incluy)\w*"
    r"|no se midi\w*"
    r"|no fue(?:ron)? provist\w*"
    r"|no (?:esta|est[aá]) disponible"
    r"|sin informaci[oó]n disponible"
    r"|no se puede incluir informaci[oó]n",
    re.IGNORECASE,
)
_SECTION_HEADING_PATTERN = re.compile(r"^(?:#{1,6}\s+.+|\*\*[^\n*]+\*\*)\s*$")
_UNDERLINE_PATTERN = re.compile(r"^[-=]{3,}\s*$")
# Una seccion real (con contenido real) puede mencionar de paso algo no
# medido -- solo se descarta si el cuerpo ES basicamente esa frase, no un
# parrafo largo que la incluya de pasada.
_FILLER_SECTION_MAX_CHARS = 400


def _strip_filler_sections(text: str) -> str:
    lines = text.split("\n")
    heading_indices = [i for i, line in enumerate(lines) if _SECTION_HEADING_PATTERN.match(line.strip())]
    if not heading_indices:
        return text

    boundaries = heading_indices + [len(lines)]
    kept_blocks = []
    for idx, start in enumerate(heading_indices):
        end = boundaries[idx + 1]
        section_lines = lines[start:end]
        body_start = 1
        if len(section_lines) > 1 and _UNDERLINE_PATTERN.match(section_lines[1].strip()):
            body_start = 2
        body_text = "\n".join(section_lines[body_start:]).strip()
        is_pure_filler = bool(body_text) and len(body_text) < _FILLER_SECTION_MAX_CHARS and bool(_FILLER_PATTERN.search(body_text))
        if not is_pure_filler:
            kept_blocks.append(section_lines)

    preamble = lines[: heading_indices[0]]
    out_lines = preamble + [line for block in kept_blocks for line in block]
    result = "\n".join(out_lines).strip()
    return result if result else text  # nunca devolver vacio -- mejor relleno que nada


# Confirmado real esta sesion (KAN-5): a veces el modelo, en vez de rellenar
# secciones, se niega a escribir NADA -- "Lo siento, pero no puedo generar
# un comprobante de desarrollo tecnico que incluya informacion confidencial
# o sensible" -- un falso positivo de seguridad (la evidencia real, ej. un
# nombre de variable de entorno como OPENAI_API_KEY, no es un secreto en si
# mismo). Sin este chequeo, esa negativa se posteaba como si fuera el
# comprobante real. Se detecta por las mismas frases de rechazo tipicas y
# se trata igual que "no genero nada" (None), no como contenido real.
_REFUSAL_PATTERN = re.compile(
    r"^\s*lo siento,?\s+pero\s+no\s+puedo\b"
    r"|^\s*no\s+puedo\s+generar\b"
    r"|^\s*i'?m\s+sorry,?\s+but\s+i\s+can'?t\b",
    re.IGNORECASE,
)


def _looks_like_refusal(text: str) -> bool:
    return bool(_REFUSAL_PATTERN.match(text.strip()))


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
    if _looks_like_refusal(text):
        logger.warning(f"tech_doc_agent: el modelo se nego a generar el comprobante (falso positivo de seguridad): {text[:200]!r}")
        return None
    text = _strip_filler_sections(text)
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
