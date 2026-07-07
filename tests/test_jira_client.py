"""Unit tests for jira_client.py's pure ADF (Atlassian Document Format)
parsing helpers -- no network involved, these never hit the real Jira API.
"""
from jira_client import _adf_has_code_block, _adf_to_text, _extract_figma_link


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


def test_extract_figma_link_from_file_url():
    description = "Arreglar el boton segun https://www.figma.com/file/ABC123xyz/Login-Screen?node-id=12-34 por favor"
    result = _extract_figma_link(description)
    assert result == {
        "file_key": "ABC123xyz",
        "node_id": "12:34",
        "url": "https://www.figma.com/file/ABC123xyz/Login-Screen?node-id=12-34",
    }


def test_extract_figma_link_from_design_url():
    description = "Ver https://www.figma.com/design/ZZ999/App?type=design&node-id=456-789&t=abc123 para las medidas"
    result = _extract_figma_link(description)
    assert result["file_key"] == "ZZ999"
    assert result["node_id"] == "456:789"


def test_extract_figma_link_none_when_no_url():
    assert _extract_figma_link("Arreglar el boton de login, no muestra el spinner de carga") is None


def test_extract_figma_link_none_when_url_has_no_node_id():
    description = "El diseno esta en https://www.figma.com/file/ABC123xyz/Login-Screen sin nodo especifico"
    assert _extract_figma_link(description) is None


def test_extract_figma_link_none_for_empty_description():
    assert _extract_figma_link("") is None
