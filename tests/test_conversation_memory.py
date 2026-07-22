"""Unit tests for conversation_memory.py -- compacta conversaciones largas
del coding agent para que no crezcan sin limite a traves de una epica
secuencial completa. Best-effort total: nunca debe bloquear ni corromper
una corrida real, incluso si la generacion del resumen falla.
"""
import importlib
from unittest.mock import patch

import agent_loop
import conversation_memory as cm


def test_compaction_threshold_derives_from_real_ollama_context_window(monkeypatch):
    """Gap real confirmado en vivo (epica KAN-4, qwen3:8b): el umbral de
    compactacion era un numero fijo (40000 caracteres) sin relacion con
    OLLAMA_NUM_CTX -- mas alto que lo que realmente entra en el contexto
    real de Ollama (8192 tokens ~ 32768 caracteres), asi que la
    compactacion nunca llegaba a activarse antes de que el contexto real ya
    se hubiera desbordado. Ahora tiene que derivarse del mismo calculo real
    que agent_loop.context_warning_threshold_chars("ollama") -- confirmado
    recargando ambos modulos con OLLAMA_NUM_CTX chico y viendo que el
    umbral de conversation_memory baja proporcionalmente (mitad, con margen
    para el system prompt + la respuesta del turno actual).
    """
    monkeypatch.delenv("CONVERSATION_SUMMARY_THRESHOLD_CHARS", raising=False)
    monkeypatch.delenv("CONTEXT_SIZE_WARNING_CHARS", raising=False)
    monkeypatch.setenv("OLLAMA_NUM_CTX", "1000")
    try:
        reloaded_agent_loop = importlib.reload(agent_loop)
        reloaded_cm = importlib.reload(cm)
        # 1000 tokens * 4 chars/token = 4000; conversation_memory se queda
        # con la mitad de eso, no con el fijo de 40000 de antes.
        assert reloaded_cm.CONVERSATION_SUMMARY_THRESHOLD_CHARS == 2000
        assert reloaded_cm.CONVERSATION_SUMMARY_THRESHOLD_CHARS == reloaded_agent_loop.context_warning_threshold_chars("ollama") // 2
        assert reloaded_cm.CONVERSATION_SUMMARY_THRESHOLD_CHARS < 40000
    finally:
        importlib.reload(agent_loop)
        importlib.reload(cm)


def test_messages_to_text_formats_different_block_types():
    messages = [
        {"role": "user", "content": "texto plano"},
        {"role": "assistant", "content": [{"type": "text", "text": "pensando..."}]},
        {"role": "assistant", "content": [{"type": "tool_use", "name": "read_file", "input": {"path": "a.py"}}]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "1", "content": "contenido del archivo"}]},
    ]

    text = cm._messages_to_text(messages)

    assert "texto plano" in text
    assert "pensando..." in text
    assert "read_file" in text and "a.py" in text
    assert "contenido del archivo" in text


def test_is_tool_result_message_true_for_tool_result_content():
    message = {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "1", "content": "x"}]}
    assert cm._is_tool_result_message(message) is True


def test_is_tool_result_message_false_for_plain_text():
    assert cm._is_tool_result_message({"role": "user", "content": "texto normal"}) is False
    assert cm._is_tool_result_message({"role": "assistant", "content": [{"type": "text", "text": "x"}]}) is False


def test_safe_cutoff_index_never_splits_tool_use_tool_result_pair():
    """Gap real: cortar en medio de un par tool_use (assistant) / tool_result
    (user) corromperia la conversacion para el backend real -- Anthropic
    exige que un tool_result siga INMEDIATAMENTE a su tool_use."""
    messages = [
        {"role": "user", "content": "prompt inicial"},                                              # 0
        {"role": "assistant", "content": [{"type": "tool_use", "name": "read_file", "input": {}}]},  # 1
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "1", "content": "x"}]},   # 2 <- candidato de corte "naive"
        {"role": "assistant", "content": [{"type": "text", "text": "listo"}]},                       # 3
    ]

    cutoff = cm._safe_cutoff_index(messages, target_keep_n=2)

    # target_keep_n=2 apuntaria naturalmente al indice 2 (un tool_result) --
    # tiene que retroceder hasta el indice 1 (el tool_use, mensaje seguro).
    assert cutoff == 1
    assert not cm._is_tool_result_message(messages[cutoff])


def test_safe_cutoff_index_returns_zero_when_all_messages_are_pairs():
    messages = [
        {"role": "assistant", "content": [{"type": "tool_use", "name": "x", "input": {}}]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "1", "content": "y"}]},
    ]
    assert cm._safe_cutoff_index(messages, target_keep_n=1) == 0


def test_maybe_compact_conversation_returns_unchanged_when_under_threshold(monkeypatch):
    monkeypatch.setattr(cm, "CONVERSATION_SUMMARY_THRESHOLD_CHARS", 1_000_000)
    messages = [{"role": "user", "content": "prompt corto"}]

    assert cm.maybe_compact_conversation(messages) == messages


def test_maybe_compact_conversation_disabled_returns_unchanged(monkeypatch):
    monkeypatch.setattr(cm, "CONVERSATION_SUMMARY_ENABLED", False)
    monkeypatch.setattr(cm, "CONVERSATION_SUMMARY_THRESHOLD_CHARS", 1)  # estaria sobre el umbral si estuviera prendido
    messages = [{"role": "user", "content": "x" * 100}]

    assert cm.maybe_compact_conversation(messages) == messages


def test_maybe_compact_conversation_empty_messages_returns_unchanged():
    assert cm.maybe_compact_conversation([]) == []


def test_maybe_compact_conversation_compacts_when_over_threshold(monkeypatch):
    """Gap real identificado en auditoria ("orquestacion... necesita
    memoria acotada"): sin esto, retry_coding_agent_local_real arrastraba
    el transcript crudo COMPLETO turno tras turno, historia tras historia,
    de una epica secuencial entera -- podia acercarse al context window del
    backend local de Ollama."""
    monkeypatch.setattr(cm, "CONVERSATION_SUMMARY_THRESHOLD_CHARS", 100)
    monkeypatch.setattr(cm, "CONVERSATION_SUMMARY_KEEP_RECENT", 2)
    monkeypatch.setattr(cm, "_summarize_async", None)  # nunca deberia usarse (se llama via asyncio.run(_summarize_async(...)))

    async def fake_summarize(old_text):
        assert "historia vieja" in old_text
        return "resumen real: se procesaron 3 historias, ninguna aplico cambios."

    monkeypatch.setattr(cm, "_summarize_async", fake_summarize)

    messages = (
        [{"role": "user", "content": f"contexto de historia vieja numero {i}, " + "x" * 30} for i in range(5)]
        + [{"role": "user", "content": "mensaje reciente 1"}, {"role": "assistant", "content": [{"type": "text", "text": "mensaje reciente 2"}]}]
    )

    result = cm.maybe_compact_conversation(messages)

    assert len(result) == 3  # 1 mensaje resumen + 2 recientes preservados
    assert "RESUMEN DE CONTEXTO PREVIO" in result[0]["content"]
    assert "resumen real: se procesaron 3 historias" in result[0]["content"]
    assert result[1]["content"] == "mensaje reciente 1"
    assert result[2] == messages[-1]  # el ultimo mensaje queda intacto, sin tocar


def test_maybe_compact_conversation_returns_unchanged_when_summary_fails(monkeypatch):
    """Best-effort real: si la generacion del resumen falla (backend caido,
    excepcion), esto nunca debe romper la corrida -- se devuelve el
    historial original sin comprimir, mismo comportamiento que antes de
    que este modulo existiera."""
    monkeypatch.setattr(cm, "CONVERSATION_SUMMARY_THRESHOLD_CHARS", 10)
    monkeypatch.setattr(cm, "CONVERSATION_SUMMARY_KEEP_RECENT", 1)

    async def failing_summarize(old_text):
        return None

    monkeypatch.setattr(cm, "_summarize_async", failing_summarize)

    messages = [{"role": "user", "content": "x" * 50} for _ in range(4)]

    result = cm.maybe_compact_conversation(messages)

    assert result == messages


def test_summarize_epic_context_returns_short_description_unchanged(monkeypatch):
    monkeypatch.setattr(cm, "EPIC_CONTEXT_SUMMARY_THRESHOLD_CHARS", 1_000_000)
    description = "Implementar Ionic Angular + Capacitor, con pruebas unitarias obligatorias."

    assert cm.summarize_epic_context(description) == description


def test_summarize_epic_context_empty_returns_empty():
    assert cm.summarize_epic_context("") == ""


def test_summarize_epic_context_disabled_returns_original(monkeypatch):
    monkeypatch.setattr(cm, "EPIC_CONTEXT_SUMMARY_ENABLED", False)
    monkeypatch.setattr(cm, "EPIC_CONTEXT_SUMMARY_THRESHOLD_CHARS", 1)  # estaria sobre el umbral si estuviera prendido
    description = "x" * 100

    assert cm.summarize_epic_context(description) == description


def test_summarize_epic_context_calls_llm_when_over_threshold(monkeypatch):
    """Gap real identificado en auditoria ("orquestacion... el contexto de
    la epica tiene que ser mas eficiente en memoria"): una epic description
    real puede ser un documento de miles de palabras (executive summary,
    roadmap, matriz de riesgos, story points) -- mandarla ENTERA a cada
    historia seria tan costoso como el problema que ya resuelve
    maybe_compact_conversation del otro lado. Confirma que dispara un
    resumen real (no un simple truncado) cuando supera el umbral."""
    monkeypatch.setattr(cm, "EPIC_CONTEXT_SUMMARY_THRESHOLD_CHARS", 50)

    async def fake_summarize(description):
        assert "Ionic Angular" in description
        return "Stack requerido: Ionic Angular + Capacitor. Requiere pruebas unitarias."

    monkeypatch.setattr(cm, "_summarize_epic_context_async", fake_summarize)

    description = "Implementar Ionic Angular + Capacitor. " + "Contexto de negocio irrelevante. " * 10

    result = cm.summarize_epic_context(description)

    assert result == "Stack requerido: Ionic Angular + Capacitor. Requiere pruebas unitarias."


def test_summarize_epic_context_falls_back_to_truncation_when_summary_fails(monkeypatch):
    """Best-effort real: si la generacion del resumen falla (backend caido,
    rechazo del modelo), esto cae a un truncado simple en vez de mandar el
    documento entero sin comprimir -- nunca bloquea la corrida."""
    monkeypatch.setattr(cm, "EPIC_CONTEXT_SUMMARY_THRESHOLD_CHARS", 10)
    monkeypatch.setattr(cm, "EPIC_CONTEXT_TRUNCATE_CHARS", 20)

    async def failing_summarize(description):
        return None

    monkeypatch.setattr(cm, "_summarize_epic_context_async", failing_summarize)

    description = "x" * 100
    result = cm.summarize_epic_context(description)

    assert result.startswith("x" * 20)
    assert "truncada" in result
    assert len(result) < len(description)


def test_truncate_epic_context_passes_through_short_text(monkeypatch):
    monkeypatch.setattr(cm, "EPIC_CONTEXT_TRUNCATE_CHARS", 100)
    assert cm._truncate_epic_context("texto corto") == "texto corto"


def test_maybe_compact_conversation_returns_unchanged_when_no_safe_cutoff(monkeypatch):
    """Si TODO el historial son pares tool_use/tool_result (sin ningun
    punto seguro para cortar), mejor no tocar nada que corromper la
    conversacion."""
    monkeypatch.setattr(cm, "CONVERSATION_SUMMARY_THRESHOLD_CHARS", 10)
    monkeypatch.setattr(cm, "CONVERSATION_SUMMARY_KEEP_RECENT", 1)
    messages = [
        {"role": "assistant", "content": [{"type": "tool_use", "name": "x", "input": {}}]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "1", "content": "y" * 50}]},
    ]

    result = cm.maybe_compact_conversation(messages)

    assert result == messages
