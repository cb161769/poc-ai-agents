"""Unit tests for judge_agent.py's pure helpers: JSON extraction from the
model's reply, the Anthropic<->Ollama message-shape adapters, and the cost
estimator. No network, no MCP servers, no model calls involved.
"""
import pytest

from judge_agent import (
    _estimate_cost_usd,
    _extract_json,
    _messages_to_ollama,
    _ollama_response_to_blocks,
)


def test_extract_json_from_plain_json():
    assert _extract_json('{"verdict": "OK", "reasoning": "todo bien"}') == {
        "verdict": "OK",
        "reasoning": "todo bien",
    }


def test_extract_json_strips_json_fenced_code_block():
    text = '```json\n{"verdict": "FLAGGED", "reasoning": "sospechoso"}\n```'
    assert _extract_json(text) == {"verdict": "FLAGGED", "reasoning": "sospechoso"}


def test_extract_json_strips_plain_fenced_code_block():
    text = '```\n{"verdict": "OK", "reasoning": "ok"}\n```'
    assert _extract_json(text) == {"verdict": "OK", "reasoning": "ok"}


def test_extract_json_with_surrounding_whitespace():
    assert _extract_json('\n  {"verdict": "OK"}  \n') == {"verdict": "OK"}


def test_messages_to_ollama_string_content_passthrough():
    messages = [{"role": "user", "content": "hello"}]
    assert _messages_to_ollama(messages) == [{"role": "user", "content": "hello"}]


def test_messages_to_ollama_assistant_text_and_tool_use():
    messages = [
        {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "pensando"},
                {"type": "tool_use", "name": "neo4j-cypher__read", "input": {"query": "MATCH (n) RETURN n"}},
            ],
        }
    ]
    result = _messages_to_ollama(messages)
    assert result == [
        {
            "role": "assistant",
            "content": "pensando",
            "tool_calls": [{"function": {"name": "neo4j-cypher__read", "arguments": {"query": "MATCH (n) RETURN n"}}}],
        }
    ]


def test_messages_to_ollama_tool_result_becomes_tool_role():
    messages = [{"role": "user", "content": [{"type": "tool_result", "content": "resultado real de la tool"}]}]
    assert _messages_to_ollama(messages) == [{"role": "tool", "content": "resultado real de la tool"}]


def test_ollama_response_to_blocks_text_only():
    blocks, stop_reason = _ollama_response_to_blocks({"content": "todo ok"})
    assert blocks == [{"type": "text", "text": "todo ok"}]
    assert stop_reason == "end_turn"


def test_ollama_response_to_blocks_with_tool_call_dict_arguments():
    message = {
        "content": "voy a chequear el grafo",
        "tool_calls": [{"function": {"name": "neo4j-cypher__read", "arguments": {"query": "MATCH (n) RETURN n"}}}],
    }
    blocks, stop_reason = _ollama_response_to_blocks(message)
    assert stop_reason == "tool_use"
    assert blocks[0] == {"type": "text", "text": "voy a chequear el grafo"}
    assert blocks[1]["type"] == "tool_use"
    assert blocks[1]["name"] == "neo4j-cypher__read"
    assert blocks[1]["input"] == {"query": "MATCH (n) RETURN n"}


def test_ollama_response_to_blocks_with_tool_call_string_arguments():
    message = {"tool_calls": [{"function": {"name": "qdrant-rag__find", "arguments": '{"query": "auth bug"}'}}]}
    blocks, stop_reason = _ollama_response_to_blocks(message)
    assert stop_reason == "tool_use"
    tool_block = next(b for b in blocks if b["type"] == "tool_use")
    assert tool_block["input"] == {"query": "auth bug"}


@pytest.mark.parametrize(
    "input_tokens,output_tokens,expected",
    [
        (1_000_000, 0, 3.0),
        (0, 1_000_000, 15.0),
        (0, 0, 0.0),
    ],
)
def test_estimate_cost_usd_known_model(input_tokens, output_tokens, expected):
    assert _estimate_cost_usd("claude-sonnet-5", input_tokens, output_tokens) == expected


def test_estimate_cost_usd_unknown_model_is_zero():
    assert _estimate_cost_usd("some-model-not-in-the-pricing-table", 1000, 1000) == 0.0
