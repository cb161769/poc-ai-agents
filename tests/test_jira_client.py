"""Unit tests for jira_client.py's pure ADF (Atlassian Document Format)
parsing helpers -- no network involved, these never hit the real Jira API.
"""
from jira_client import _adf_has_code_block, _adf_to_text


def test_adf_to_text_none_is_empty_string():
    assert _adf_to_text(None) == ""


def test_adf_to_text_flattens_nested_paragraphs():
    doc = {
        "type": "doc",
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "Hola"}]},
            {"type": "paragraph", "content": [{"type": "text", "text": "Mundo"}]},
        ],
    }
    result = _adf_to_text(doc)
    assert "Hola" in result
    assert "Mundo" in result


def test_adf_has_code_block_true_when_present_anywhere_nested():
    doc = {
        "type": "doc",
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "intro"}]},
            {"type": "codeBlock", "content": [{"type": "text", "text": "Exception: boom"}]},
        ],
    }
    assert _adf_has_code_block(doc) is True


def test_adf_has_code_block_false_when_only_plain_text():
    doc = {
        "type": "doc",
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "Exception: boom (pegado como texto plano)"}]},
        ],
    }
    assert _adf_has_code_block(doc) is False


def test_adf_has_code_block_false_for_none():
    assert _adf_has_code_block(None) is False
