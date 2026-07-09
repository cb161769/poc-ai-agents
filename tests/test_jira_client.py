"""Unit tests for jira_client.py's pure ADF (Atlassian Document Format)
parsing helpers -- no network involved, these never hit the real Jira API.
"""
from unittest.mock import MagicMock, patch

import jira_client
from jira_client import (
    _adf_has_code_block,
    _adf_to_text,
    _build_smoke_ticket_payload,
    _extract_figma_link,
    fetch_epic_with_children,
)


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


def _fake_issue_response(components: list, labels: list) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {
        "key": "T-1",
        "fields": {
            "summary": "algo",
            "description": None,
            "labels": labels,
            "status": {"name": "Open"},
            "attachment": [],
            "components": [{"name": name} for name in components],
        },
    }
    return resp


@patch("jira_client.httpx.get")
def test_fetch_ticket_live_prefers_native_components_field(mock_get, monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setenv("JIRA_TICKET_KEY", "T-1")
    monkeypatch.setattr(jira_client, "KNOWN_REPOS", {"AuthService", "Frontend"})
    mock_get.return_value = _fake_issue_response(components=["AuthService"], labels=["Frontend"])

    ticket = jira_client.fetch_ticket_live()

    assert ticket["repository_origen"] == "AuthService"


@patch("jira_client.httpx.get")
def test_fetch_ticket_live_falls_back_to_labels_when_no_matching_component(mock_get, monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setenv("JIRA_TICKET_KEY", "T-1")
    monkeypatch.setattr(jira_client, "KNOWN_REPOS", {"Frontend"})
    mock_get.return_value = _fake_issue_response(components=["SomethingNotKnown"], labels=["Frontend"])

    ticket = jira_client.fetch_ticket_live()

    assert ticket["repository_origen"] == "Frontend"


@patch("jira_client.httpx.get")
def test_fetch_ticket_live_ticket_key_param_overrides_env(mock_get, monkeypatch):
    """orchestration.py llama fetch_ticket_live() directo (import, no
    subprocess) pasando ticket_key explicito -- confirma que tiene prioridad
    sobre JIRA_TICKET_KEY, asi ya no hace falta mutar os.environ antes.
    """
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setenv("JIRA_TICKET_KEY", "T-DEL-ENV")
    monkeypatch.setattr(jira_client, "KNOWN_REPOS", {"AuthService"})
    mock_get.return_value = _fake_issue_response(components=["AuthService"], labels=[])

    jira_client.fetch_ticket_live(ticket_key="T-EXPLICITO")

    called_url = mock_get.call_args[0][0]
    assert "T-EXPLICITO" in called_url
    assert "T-DEL-ENV" not in called_url


@patch("jira_client.httpx.get")
def test_fetch_ticket_live_known_repos_param_overrides_module_default(mock_get, monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setattr(jira_client, "KNOWN_REPOS", {"Frontend"})
    mock_get.return_value = _fake_issue_response(components=["DataWorker"], labels=[])

    ticket = jira_client.fetch_ticket_live(ticket_key="T-1", known_repos={"DataWorker"})

    assert ticket["repository_origen"] == "DataWorker"


def test_build_smoke_ticket_payload_shape(monkeypatch):
    monkeypatch.setenv("JIRA_PROJECT_KEY", "POC")
    monkeypatch.setenv("JIRA_SMOKE_TEST_ISSUE_TYPE", "Bug")

    payload = _build_smoke_ticket_payload("AuthService")
    fields = payload["fields"]

    assert fields["project"] == {"key": "POC"}
    assert fields["issuetype"] == {"name": "Bug"}
    assert fields["labels"] == ["smoke-test", "AuthService"]
    assert "[smoke-test]" in fields["summary"]

    code_block = next(c for c in fields["description"]["content"] if c["type"] == "codeBlock")
    assert "SmokeTestException" in code_block["content"][0]["text"]


def test_build_smoke_ticket_payload_default_issue_type(monkeypatch):
    monkeypatch.setenv("JIRA_PROJECT_KEY", "POC")
    monkeypatch.delenv("JIRA_SMOKE_TEST_ISSUE_TYPE", raising=False)

    payload = _build_smoke_ticket_payload("Frontend")

    assert payload["fields"]["issuetype"] == {"name": "Task"}


def _fake_epic_response() -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {
        "key": "EPIC-1",
        "fields": {
            "summary": "Rehacer el login",
            "description": None,
            "labels": [],
            "components": [{"name": "AuthService"}],
        },
    }
    return resp


def _fake_search_response() -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {
        "issues": [
            {
                "key": "T-10",
                "fields": {"summary": "Boton nuevo", "description": None, "labels": [], "components": [{"name": "Frontend"}]},
            },
            {
                "key": "T-11",
                "fields": {"summary": "Validar token", "description": None, "labels": [], "components": [{"name": "AuthService"}]},
            },
        ]
    }
    return resp


@patch("jira_client.httpx.post")
@patch("jira_client.httpx.get")
def test_fetch_epic_with_children_shape(mock_get, mock_post, monkeypatch):
    """La epica en si se sigue trayendo con GET; los hijos se buscan con
    POST /rest/api/3/search/jql -- GET /rest/api/3/search fue dado de baja
    por Atlassian (410 Gone)."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setattr(jira_client, "KNOWN_REPOS", {"AuthService", "Frontend"})
    mock_get.return_value = _fake_epic_response()
    mock_post.return_value = _fake_search_response()

    result = fetch_epic_with_children("EPIC-1")

    assert result["epic"]["ticket_id"] == "EPIC-1"
    assert result["epic"]["repository_origen"] == "AuthService"
    assert len(result["children"]) == 2
    assert result["children"][0] == {
        "ticket_id": "T-10",
        "summary": "Boton nuevo",
        "description": "",
        "repository_origen": "Frontend",
    }
    assert result["children"][1]["repository_origen"] == "AuthService"

    search_call = mock_post.call_args_list[0]
    assert search_call.kwargs["json"]["jql"] == 'parent = "EPIC-1"'


@patch("jira_client.httpx.post")
@patch("jira_client.httpx.get")
def test_fetch_epic_with_children_respects_custom_jql(mock_get, mock_post, monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setenv("JIRA_EPIC_LINK_JQL", 'cf[10014] = "{epic_key}"')
    monkeypatch.setattr(jira_client, "KNOWN_REPOS", {"AuthService"})
    mock_get.return_value = _fake_epic_response()
    mock_post.return_value = _fake_search_response()

    fetch_epic_with_children("EPIC-1")

    search_call = mock_post.call_args_list[0]
    assert search_call.kwargs["json"]["jql"] == 'cf[10014] = "EPIC-1"'
