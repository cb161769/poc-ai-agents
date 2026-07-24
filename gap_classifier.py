"""Clasificador de "gaps" -- por que se corto una corrida real, para no
tener que leer 200 lineas de log de Prefect a mano cada vez (lo que este
mismo pipeline necesito hacer, repetidamente, durante la epica KAN-4).

Nace de una discusion real sobre arquitectura: la mayoria de los bloqueos
reales de una corrida caen en tres categorias bien distintas, cada una con
una respuesta correcta DISTINTA --

  1. content_gap: el modelo necesita otro intento con mejor feedback -- ya
     automatizado (ver _retry_local_diff/REMEDIATION_HINTS en orchestration.py).
  2. infra_bug: la falla apunta al CODIGO DEL PROPIO PIPELINE (una tool dio
     un error inutil, un chequeo que faltaba, un comando destructivo que se
     auto-aprobo) -- necesita una edicion real de orchestration.py/
     coding_agent.py, NUNCA aplicada por este clasificador.
  3. model_ceiling: gap de contenido que un reintento no va a resolver
     (calidad de mocks, un nombre de paquete alucinado) -- ningun cambio de
     codigo lo arregla, hace falta un modelo mejor o intervencion humana.

Decision de diseno explicita, no negociable: este modulo SOLO clasifica y
redacta una propuesta -- NUNCA edita orchestration.py/coding_agent.py, NUNCA
corre tests, NUNCA hace commit. Un "gap manager" que auto-edita el codigo
del propio pipeline sin supervision tiene un blast radius mucho mayor que
una rama de ticket mala (cada corrida futura hereda el cambio en silencio)
-- la propuesta queda para que un humano (o una sesion como esta) la lea y
decida, exactamente como paso durante toda esta sesion real.
"""
import asyncio
import json
import logging
import os

import httpx

from agent_loop import OLLAMA_MODEL, call_with_fallback, parse_ollama_model_candidates, resolve_ollama_model
from grounding_check import verify_claim_grounded

logger = logging.getLogger("gap_classifier")

# Mismo criterio que TECH_DOC_ENABLED/TEST_PLAN_ENABLED -- best-effort,
# apagable sin afectar el resto del pipeline.
GAP_CLASSIFIER_ENABLED = os.environ.get("GAP_CLASSIFIER_ENABLED", "true").strip().lower() not in {"0", "false", "no"}
GAP_CLASSIFIER_OLLAMA_MODELS = parse_ollama_model_candidates(os.environ.get("GAP_CLASSIFIER_OLLAMA_MODEL", ""), OLLAMA_MODEL)

GAP_CLASSIFIER_SYSTEM_PROMPT = """Sos un diagnosticador tecnico para un pipeline de agentes de IA que resuelve \
tickets de Jira. Te dan evidencia real de por que una corrida se bloqueo -- tu trabajo es clasificarla en \
UNA de tres categorias, cada una con una respuesta correcta distinta:

- "content_gap": el agente de codigo necesita otro intento con mejor feedback especifico -- el problema es \
sobre ESTE cambio puntual, no sobre el pipeline ni sobre una limitacion de fondo del modelo. Ejemplo: el \
agente olvido un caso borde que el feedback del juez ya deja claro como corregir.

- "infra_bug": la evidencia apunta a un defecto REAL en el codigo del pipeline mismo -- una herramienta \
devolvio un error inutil o enganoso, un chequeo de seguridad que falta, un comando destructivo que se \
auto-aprobo, un timeout mal configurado, algo que un cambio de codigo en orchestration.py/coding_agent.py \
resolveria para TODAS las corridas futuras, no solo esta. Ejemplo real: un comando de shell que un CLI \
necesita no estaba disponible en el contenedor, indistinguible para el agente de un fallo real del codigo.

- "model_ceiling": un gap de CONTENIDO que ningun cambio de codigo del pipeline puede resolver -- el modelo \
elegido no tiene el conocimiento/capacidad para esto de forma confiable (ej. escribir mocks async correctos \
para un framework de nicho, o recordar el nombre exacto de un paquete oscuro). Mas reintentos con el MISMO \
modelo no van a cambiar el resultado.

Se especialmente escéptico antes de marcar "infra_bug" -- solo marcalo si podes señalar la causa real en \
terminos de COMPORTAMIENTO DEL PIPELINE (que herramienta/chequeo fallo), no una opinion vaga de que "el \
pipeline deberia ser mejor". Si no estas seguro entre content_gap y model_ceiling, preferi content_gap (menos \
costoso equivocarse: un reintento de mas es barato, un veredicto model_ceiling incorrecto cierra la puerta \
a una solucion real).

Si marcas "infra_bug", tenes que incluir una "proposal" concreta: {"suspected_file": "el archivo real donde \
probablemente esta el bug (ej. orchestration.py, coding_agent.py) o null si no estas seguro", \
"suspected_root_cause": "que comportamiento especifico del pipeline causo esto", "suggested_fix": "que \
cambiarias, en terminos concretos", "regression_test_sketch": "que caso de test confirmaria el fix"}. Para \
content_gap/model_ceiling, "proposal" tiene que ser null.

Cuando termines, respondé con texto plano que sea UNICAMENTE un objeto JSON, sin texto antes ni despues, con \
este esquema exacto: {"bucket": "content_gap" o "infra_bug" o "model_ceiling", "reasoning": "por que, citando \
evidencia concreta de lo que te dieron", "proposal": {...} o null}"""

GAP_CLASSIFIER_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "bucket": {"type": "string", "enum": ["content_gap", "infra_bug", "model_ceiling"]},
        "reasoning": {"type": "string"},
        "proposal": {
            "type": ["object", "null"],
            "properties": {
                "suspected_file": {"type": ["string", "null"]},
                "suspected_root_cause": {"type": "string"},
                "suggested_fix": {"type": "string"},
                "regression_test_sketch": {"type": "string"},
            },
        },
    },
    "required": ["bucket", "reasoning"],
}


def _format_evidence(evidence: dict) -> str:
    lines = ["EVIDENCIA REAL DE POR QUE SE BLOQUEO ESTA CORRIDA:"]
    for key, value in evidence.items():
        lines.append(f"- {key}: {value}")
    lines.append("\nClasifica esta corrida bloqueada segun las tres categorias del prompt.")
    return "\n".join(lines)


async def _classify_async(evidence: dict) -> dict | None:
    async with httpx.AsyncClient() as client:
        try:
            blocks, _stop_reason, _usage, _backend_used = await call_with_fallback(
                client,
                messages=[{"role": "user", "content": _format_evidence(evidence)}],
                tools=[],
                system_prompt=GAP_CLASSIFIER_SYSTEM_PROMPT,
                ollama_model=resolve_ollama_model(GAP_CLASSIFIER_OLLAMA_MODELS) or GAP_CLASSIFIER_OLLAMA_MODELS[0],
                force_json=True,
                json_schema=GAP_CLASSIFIER_RESULT_SCHEMA,
            )
        except Exception as exc:
            logger.warning(f"gap_classifier: no se pudo clasificar el bloqueo: {exc}")
            return None

    text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
    if not text:
        return None
    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        logger.warning(f"gap_classifier: el modelo no devolvio JSON valido: {text[:300]!r}")
        return None
    if result.get("bucket") not in ("content_gap", "infra_bug", "model_ceiling"):
        logger.warning(f"gap_classifier: bucket invalido/faltante en la respuesta: {result!r}")
        return None
    if result.get("bucket") != "infra_bug":
        result["proposal"] = None

    # Gap real confirmado en vivo esta sesion: una clasificacion "infra_bug"
    # correcta en el bucket vino con un "reasoning" que citaba un dato
    # especifico FALSO ("la imagen X no esta disponible" -- ya confirmado en
    # vivo que si lo estaba). bespoke-minicheck (grounding_check.py) confirma
    # si el reasoning esta de verdad respaldado por la evidencia real que se
    # le dio -- best-effort, nunca bloquea: grounded=None (no se pudo
    # verificar) se trata igual que "no marcado", solo grounded=False deja
    # una advertencia explicita en vez de dejar pasar la cita inventada
    # como si fuera un hecho verificado.
    grounded = verify_claim_grounded(_format_evidence(evidence), result.get("reasoning", ""))
    result["reasoning_grounded"] = grounded
    if grounded is False:
        result["reasoning"] = (
            "⚠️ Esta justificacion NO se pudo confirmar contra la evidencia real (posible cita inventada) -- "
            f"tratala con escepticismo: {result.get('reasoning', '')}"
        )
    return result


def classify_gap(evidence: dict) -> dict | None:
    """Best-effort, sincrono (mismo criterio que tech_doc_agent.py para
    llamarse directo desde una @task de Prefect): None si GAP_CLASSIFIER_ENABLED
    esta apagado o la clasificacion falla por cualquier motivo -- esto es
    diagnostico, nunca debe bloquear ni romper la corrida real.
    """
    if not GAP_CLASSIFIER_ENABLED:
        return None
    try:
        return asyncio.run(_classify_async(evidence))
    except Exception as exc:
        logger.warning(f"gap_classifier: fallo inesperado clasificando el bloqueo: {exc}")
        return None
