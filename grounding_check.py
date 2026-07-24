"""Chequeo de "grounding" -- confirma que una afirmacion generada por otro
agente (el "claim") esta de verdad respaldada por la evidencia real que ese
agente tenia enfrente (el "document"), usando bespoke-minicheck
(ollama.com/blog/reduce-hallucinations-with-bespoke-minicheck): un modelo
chico, especializado SOLO en esto (Document/Claim -> Yes/No), no un LLM
generico -- mas barato y mas confiable para esta tarea puntual que pedirle
al mismo modelo que se autoevalue.

Nace de un hallazgo real de esta sesion: gap_classifier.py (con qwen3:8b)
clasifico un bloqueo real correctamente como "infra_bug", pero su propio
"reasoning" citaba un dato especifico FALSO ("la imagen
mcr.microsoft.com/playwright:v1.61.1-noble no esta disponible" -- mentira,
se habia confirmado en vivo que si lo estaba) -- una alucinacion CON
apariencia de evidencia concreta, la mas peligrosa de detectar a simple
vista. judge_agent.py tiene el mismo riesgo estructural: un veredicto
FLAGGED que cita evidencia inventada puede cortar una epica entera por un
motivo que nunca paso de verdad.

Decision de diseno: esto NUNCA revierte ni sobreescribe un veredicto/
clasificacion por su cuenta -- demasiado riesgoso automatizar "el juez se
equivoco, lo ignoro" sin supervision humana. Lo que SI hace: marca
explicitamente cuando una afirmacion no pudo confirmarse contra la
evidencia real, para que quede visible (en el log, en el comentario de
Jira) en vez de pasar desapercibida como si fuera un hecho verificado.
"""
import logging
import os

import httpx

from agent_loop import OLLAMA_URL

logger = logging.getLogger("grounding_check")

GROUNDING_CHECK_ENABLED = os.environ.get("GROUNDING_CHECK_ENABLED", "true").strip().lower() not in {"0", "false", "no"}
GROUNDING_CHECK_MODEL = os.environ.get("GROUNDING_CHECK_MODEL", "bespoke-minicheck")
GROUNDING_CHECK_TIMEOUT_SECONDS = float(os.environ.get("GROUNDING_CHECK_TIMEOUT_SECONDS", "30"))
# bespoke-minicheck es un modelo chico y especializado (no un chat model de
# proposito general) -- su propia model card (ollama.com/library/bespoke-minicheck)
# documenta un limite real de contexto mucho mas chico que los modelos de
# chat que el resto del pipeline usa. Se trunca el documento (nunca el
# claim, siempre mas corto) para no exceder ese limite real.
GROUNDING_CHECK_MAX_DOCUMENT_CHARS = int(os.environ.get("GROUNDING_CHECK_MAX_DOCUMENT_CHARS", "4000"))


def verify_claim_grounded(document: str, claim: str) -> bool | None:
    """True si el claim esta respaldado por el document real (segun
    bespoke-minicheck), False si NO lo esta (posible alucinacion), None si
    no se pudo determinar (backend no disponible, timeout, respuesta
    inesperada) -- best-effort total, nunca lanza, nunca bloquea la corrida
    real. None se trata SIEMPRE como "no se pudo verificar", nunca como
    "esta bien" ni como "esta mal" -- el llamador decide que hacer con la
    incertidumbre real.
    """
    if not GROUNDING_CHECK_ENABLED or not document or not claim:
        return None

    truncated_document = (
        document if len(document) <= GROUNDING_CHECK_MAX_DOCUMENT_CHARS
        else document[:GROUNDING_CHECK_MAX_DOCUMENT_CHARS] + "\n... (truncado)"
    )
    prompt = f"Document: {truncated_document}\nClaim: {claim}"

    try:
        resp = httpx.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": GROUNDING_CHECK_MODEL, "prompt": prompt, "stream": False},
            timeout=GROUNDING_CHECK_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        raw = (resp.json().get("response") or "").strip().lower()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning(f"grounding_check: no se pudo verificar el claim (backend no disponible: {exc}) -- se trata como 'no se pudo determinar'.")
        return None

    if raw.startswith("yes"):
        return True
    if raw.startswith("no"):
        return False
    logger.warning(f"grounding_check: respuesta inesperada de {GROUNDING_CHECK_MODEL} (ni 'yes' ni 'no'): {raw[:100]!r}")
    return None
