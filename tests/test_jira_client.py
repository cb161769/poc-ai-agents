"""Unit tests for jira_client.py's pure ADF (Atlassian Document Format)
parsing helpers -- no network involved, these never hit the real Jira API.
"""
from unittest.mock import MagicMock, patch

import httpx

import jira_client
from jira_client import (
    _adf_has_code_block,
    _adf_to_text,
    _build_smoke_ticket_payload,
    _extract_figma_link,
    _has_sufficient_context,
    _markdown_to_adf,
    _parse_sprint_field,
    fetch_epic_with_children,
)


def _fake_comments_page(comments: list, total: int) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"comments": comments, "total": total}
    return resp


def _fake_comment(marker_text: str) -> dict:
    return {"body": {"content": [{"content": [{"text": marker_text, "type": "text"}]}]}}


@patch("jira_client.httpx.get")
def test_comment_already_posted_true_when_marker_on_first_page(mock_get, monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    mock_get.return_value = _fake_comments_page([_fake_comment("[ref: run-1:abc]")], total=1)

    assert jira_client.comment_already_posted("T-1", "[ref: run-1:abc]") is True
    assert mock_get.call_count == 1


@patch("jira_client.httpx.get")
def test_comment_already_posted_false_when_not_found_within_page_limit(mock_get, monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    mock_get.return_value = _fake_comments_page([_fake_comment("otra cosa")], total=1)

    assert jira_client.comment_already_posted("T-1", "[ref: run-1:abc]") is False


@patch("jira_client.httpx.get")
def test_comment_already_posted_paginates_past_first_50_to_find_older_marker(mock_get, monkeypatch):
    """Bug real identificado en auditoria ("gaps en los flujos de jira"): un
    ticket con varias corridas del pipeline (test plan, estado del agente,
    salida de tests, veredicto del juez, link al PR, comprobante tecnico --
    varios comentarios por corrida) hace que un marcador de una corrida
    anterior se corra mas alla de una sola pagina de 50 -- la version
    original (maxResults=50, sin paginar) fallaba en detectarlo. Confirma
    que ahora sigue pidiendo paginas siguientes hasta encontrarlo."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    page_1 = _fake_comments_page([_fake_comment("otra cosa") for _ in range(100)], total=120)
    page_2 = _fake_comments_page([_fake_comment("[ref: run-old:xyz]")] + [_fake_comment("mas") for _ in range(19)], total=120)
    mock_get.side_effect = [page_1, page_2]

    assert jira_client.comment_already_posted("T-1", "[ref: run-old:xyz]") is True
    assert mock_get.call_count == 2
    assert mock_get.call_args_list[1].kwargs["params"]["startAt"] == 100


@patch("jira_client.httpx.get")
def test_comment_already_posted_stops_after_bounded_page_limit(mock_get, monkeypatch):
    """Best-effort acotado, no un scan sin fin -- un ticket con miles de
    comentarios historicos no debe convertir cada comment_jira() en una
    cadena larga de requests HTTP secuenciales."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    mock_get.return_value = _fake_comments_page([_fake_comment("otra cosa") for _ in range(100)], total=10000)

    assert jira_client.comment_already_posted("T-1", "[ref: nunca-esta:zzz]") is False
    assert mock_get.call_count == jira_client._COMMENT_ALREADY_POSTED_MAX_PAGES


@patch("jira_client.httpx.get")
def test_test_plan_already_posted_true_when_present(mock_get, monkeypatch):
    """Gap real identificado en auditoria ("gaps en los flujos de jira",
    KAN-6 real): un ticket bloqueado dias seguidos regeneraba y reposteaba
    practicamente el mismo Test Plan en CADA corrida nueva (confirmado real:
    5 postings identicos en dias distintos), enterrando la señal real de
    por que se bloqueaba entre ruido repetido."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    mock_get.return_value = _fake_comments_page(
        [_fake_comment("🧪 Test Plan (Prefect):\n\nCasos funcionales...")], total=1,
    )

    assert jira_client.test_plan_already_posted("T-1") is True


@patch("jira_client.httpx.get")
def test_test_plan_already_posted_false_when_absent(mock_get, monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    mock_get.return_value = _fake_comments_page([_fake_comment("otro comentario cualquiera")], total=1)

    assert jira_client.test_plan_already_posted("T-1") is False


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


def test_adf_to_text_collapses_consecutive_blank_paragraphs():
    """Auditoria real: parrafos vacios consecutivos (comun en descripciones
    editadas a mano) generaban corridas largas de saltos de linea que se le
    pasaban tal cual al coding agent/juez como ruido en el prompt."""
    doc = {
        "type": "doc",
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": "Antes"}]},
            {"type": "paragraph", "content": []},
            {"type": "paragraph", "content": []},
            {"type": "paragraph", "content": []},
            {"type": "paragraph", "content": [{"type": "text", "text": "Despues"}]},
        ],
    }
    result = _adf_to_text(doc)
    assert "\n\n\n" not in result
    assert "Antes" in result and "Despues" in result


def test_has_sufficient_context_true_for_real_description():
    assert _has_sufficient_context("Fix login bug", "El boton de login no responde en Safari 17.") is True


def test_has_sufficient_context_false_for_empty_description():
    assert _has_sufficient_context("Fix login bug", "") is False


def test_has_sufficient_context_false_for_too_short_description():
    assert _has_sufficient_context("Fix it", "arreglar esto") is False


def test_has_sufficient_context_false_for_empty_summary():
    assert _has_sufficient_context("", "Una descripcion larga y detallada de verdad sobre el problema real.") is False


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


def _fake_issue_response(components: list, labels: list, issue_type: str = "Task") -> MagicMock:
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
            "issuetype": {"name": issue_type},
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
def test_fetch_ticket_live_includes_issue_type(mock_get, monkeypatch):
    """orchestration.py/run_poc_loop.sh necesitan saber si el ticket es una
    Epica antes de procesarlo como ticket normal -- fetch_ticket_live() no
    pedia el campo issuetype a la API hasta este cambio."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setenv("JIRA_TICKET_KEY", "T-1")
    monkeypatch.setattr(jira_client, "KNOWN_REPOS", {"AuthService"})
    mock_get.return_value = _fake_issue_response(components=["AuthService"], labels=[], issue_type="Epic")

    ticket = jira_client.fetch_ticket_live()

    assert ticket["issue_type"] == "Epic"
    requested_fields = mock_get.call_args.kwargs["params"]["fields"]
    assert "issuetype" in requested_fields


@patch("jira_client.httpx.get")
def test_fetch_ticket_live_includes_sprint(mock_get, monkeypatch):
    """Gap real (usuario, "gaps en el workflow"): fetch_ticket_live() no
    pedia el campo Sprint hasta este cambio."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setenv("JIRA_TICKET_KEY", "T-1")
    monkeypatch.setattr(jira_client, "KNOWN_REPOS", {"AuthService"})
    resp = _fake_issue_response(components=["AuthService"], labels=[])
    resp.json.return_value["fields"][jira_client.JIRA_SPRINT_FIELD_ID] = [{"name": "Sprint 12", "state": "active"}]
    mock_get.return_value = resp

    ticket = jira_client.fetch_ticket_live()

    assert ticket["sprint"] == {"name": "Sprint 12", "state": "active"}
    requested_fields = mock_get.call_args.kwargs["params"]["fields"]
    assert jira_client.JIRA_SPRINT_FIELD_ID in requested_fields


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
        "status": None,
        "sprint": None,
    }
    assert result["children"][1]["repository_origen"] == "AuthService"

    search_call = mock_post.call_args_list[0]
    assert search_call.kwargs["json"]["jql"] == 'parent = "EPIC-1"'


@patch("jira_client.httpx.post")
@patch("jira_client.httpx.get")
def test_fetch_epic_with_children_includes_status(mock_get, mock_post, monkeypatch):
    """Gap real (usuario, "encuentra gaps en jira de cara al development
    cycle"): orchestration.py necesita saber si la epica/hijos YA estan en
    Done antes de reprocesarlos (ej. un webhook viejo/duplicado) -- ni la
    epica ni sus hijos traian el campo status hasta este cambio."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setattr(jira_client, "KNOWN_REPOS", {"AuthService", "Frontend"})

    epic_resp = MagicMock()
    epic_resp.raise_for_status.return_value = None
    epic_resp.json.return_value = {
        "key": "EPIC-1",
        "fields": {
            "summary": "Rehacer el login", "description": None, "labels": [],
            "components": [{"name": "AuthService"}], "status": {"name": "Done"},
        },
    }
    mock_get.return_value = epic_resp

    search_resp = MagicMock()
    search_resp.raise_for_status.return_value = None
    search_resp.json.return_value = {
        "issues": [
            {
                "key": "T-10",
                "fields": {
                    "summary": "Boton nuevo", "description": None, "labels": [],
                    "components": [{"name": "Frontend"}], "status": {"name": "Done"},
                },
            },
        ]
    }
    mock_post.return_value = search_resp

    result = fetch_epic_with_children("EPIC-1")

    assert result["epic"]["status"] == "Done"
    assert result["children"][0]["status"] == "Done"
    search_call = mock_post.call_args_list[0]
    assert "status" in search_call.kwargs["json"]["fields"]
    assert "status" in mock_get.call_args.kwargs["params"]["fields"]


def test_parse_sprint_field_none_or_empty_returns_none():
    assert _parse_sprint_field(None) is None
    assert _parse_sprint_field([]) is None
    assert _parse_sprint_field("not a list") is None


def test_parse_sprint_field_modern_dict_shape():
    raw = [{"id": 1, "name": "Sprint 12", "state": "active", "boardId": 5}]
    assert _parse_sprint_field(raw) == {"name": "Sprint 12", "state": "active"}


def test_parse_sprint_field_legacy_string_shape():
    """Instancias viejas de Jira devuelven el campo Sprint como una lista de
    strings serializados estilo Greenhopper, no dicts."""
    raw = [
        "com.atlassian.greenhopper.service.sprint.Sprint@1a2b3c4d[id=1,rapidViewId=5,"
        "state=ACTIVE,name=Sprint 12,startDate=2026-07-01,goal=]"
    ]
    assert _parse_sprint_field(raw) == {"name": "Sprint 12", "state": "active"}


def test_parse_sprint_field_prefers_active_among_multiple():
    raw = [
        {"name": "Sprint 11", "state": "closed"},
        {"name": "Sprint 12", "state": "active"},
    ]
    assert _parse_sprint_field(raw) == {"name": "Sprint 12", "state": "active"}


def test_parse_sprint_field_falls_back_to_last_when_none_active():
    raw = [
        {"name": "Sprint 10", "state": "closed"},
        {"name": "Sprint 11", "state": "closed"},
    ]
    assert _parse_sprint_field(raw) == {"name": "Sprint 11", "state": "closed"}


def test_parse_sprint_field_ignores_malformed_entries():
    raw = [{"id": 1, "state": "active"}, "garbage without brackets", 42]
    assert _parse_sprint_field(raw) is None


@patch("jira_client.httpx.post")
@patch("jira_client.httpx.get")
def test_fetch_epic_with_children_includes_sprint(mock_get, mock_post, monkeypatch):
    """Gap real (usuario, "gaps en el workflow"): ningun fetch de Jira traia
    el campo Sprint -- el pipeline no tenia forma de saber en que sprint
    estaba una historia."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    monkeypatch.setattr(jira_client, "KNOWN_REPOS", {"AuthService", "Frontend"})

    epic_resp = MagicMock()
    epic_resp.raise_for_status.return_value = None
    epic_resp.json.return_value = {
        "key": "EPIC-1",
        "fields": {
            "summary": "Rehacer el login", "description": None, "labels": [],
            "components": [{"name": "AuthService"}],
            jira_client.JIRA_SPRINT_FIELD_ID: [{"name": "Sprint 12", "state": "active"}],
        },
    }
    mock_get.return_value = epic_resp

    search_resp = MagicMock()
    search_resp.raise_for_status.return_value = None
    search_resp.json.return_value = {
        "issues": [
            {
                "key": "T-10",
                "fields": {
                    "summary": "Boton nuevo", "description": None, "labels": [],
                    "components": [{"name": "Frontend"}],
                    jira_client.JIRA_SPRINT_FIELD_ID: [{"name": "Sprint 12", "state": "active"}],
                },
            },
        ]
    }
    mock_post.return_value = search_resp

    result = fetch_epic_with_children("EPIC-1")

    assert result["epic"]["sprint"] == {"name": "Sprint 12", "state": "active"}
    assert result["children"][0]["sprint"] == {"name": "Sprint 12", "state": "active"}
    search_call = mock_post.call_args_list[0]
    assert jira_client.JIRA_SPRINT_FIELD_ID in search_call.kwargs["json"]["fields"]
    assert jira_client.JIRA_SPRINT_FIELD_ID in mock_get.call_args.kwargs["params"]["fields"]


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


def test_markdown_to_adf_converts_hash_heading():
    doc = _markdown_to_adf("## Resultado Real de la Corrida")
    assert doc["type"] == "doc" and doc["version"] == 1
    assert doc["content"][0] == {
        "type": "heading",
        "attrs": {"level": 2},
        "content": [{"type": "text", "text": "Resultado Real de la Corrida"}],
    }


def test_markdown_to_adf_converts_bold_underline_heading_style():
    """Confirmado real: tech_doc_agent.py genera encabezados con el estilo
    "**Titulo**\n----" (no "## "), el mismo que usa el modelo real -- tiene
    que reconocerse como heading tambien, no como parrafo con negrita."""
    text = "**Resumen Ejecutivo y Objetivo**\n-----------------------------\n\nContenido real."
    doc = _markdown_to_adf(text)
    assert doc["content"][0]["type"] == "heading"
    assert doc["content"][0]["content"][0]["text"] == "Resumen Ejecutivo y Objetivo"
    assert doc["content"][1]["type"] == "paragraph"


def test_markdown_to_adf_inline_bold_and_code():
    doc = _markdown_to_adf("El backend fue **Ollama** con el modelo `ornith:9b`.")
    inline = doc["content"][0]["content"]
    bold_node = next(n for n in inline if n["text"] == "Ollama")
    code_node = next(n for n in inline if n["text"] == "ornith:9b")
    assert bold_node["marks"] == [{"type": "strong"}]
    assert code_node["marks"] == [{"type": "code"}]


def test_markdown_to_adf_bullet_list():
    text = "- Backend utilizado: Ollama\n- Modelo Coding Agent: Ornith:9b\n"
    doc = _markdown_to_adf(text)
    assert doc["content"][0]["type"] == "bulletList"
    items = doc["content"][0]["content"]
    assert len(items) == 2
    assert items[0]["type"] == "listItem"


def test_markdown_to_adf_numbered_list():
    text = "1. Primer paso\n2. Segundo paso\n"
    doc = _markdown_to_adf(text)
    assert doc["content"][0]["type"] == "orderedList"
    assert len(doc["content"][0]["content"]) == 2


def test_markdown_to_adf_code_block_with_language():
    text = "```python\nprint('hola')\n```"
    doc = _markdown_to_adf(text)
    assert doc["content"][0] == {
        "type": "codeBlock",
        "attrs": {"language": "python"},
        "content": [{"type": "text", "text": "print('hola')"}],
    }


def test_markdown_to_adf_blockquote():
    doc = _markdown_to_adf("> The diff applies exactly the ticket's requested change.")
    assert doc["content"][0]["type"] == "blockquote"


def test_markdown_to_adf_rule():
    text = "Antes\n\n---\n\nDespues"
    doc = _markdown_to_adf(text)
    types = [node["type"] for node in doc["content"]]
    assert "rule" in types


def test_markdown_to_adf_plain_text_single_paragraph():
    doc = _markdown_to_adf("Copilot aplico un cambio en la rama 'X', pendiente de revision humana.")
    assert doc["content"] == [{
        "type": "paragraph",
        "content": [{"type": "text", "text": "Copilot aplico un cambio en la rama 'X', pendiente de revision humana."}],
    }]


def test_markdown_to_adf_real_comprobante_tecnico_end_to_end():
    """Caso real (recortado) confirmado en vivo esta sesion: el comprobante
    tecnico limpio (post _strip_filler_sections) que efectivamente se
    postea a Jira."""
    text = (
        "**Resumen Ejecutivo y Objetivo**\n"
        "-----------------------------\n\n"
        "La corrida realizada correspondio a la epica KAN-4.\n\n"
        "**Ficha Tecnica del Modelo y Entorno**\n"
        "-----------------------------------\n\n"
        "* **Backend utilizado:** Ollama\n"
        "* **Modelo Ollama Coding Agent:** Ornith:9b\n"
    )
    doc = _markdown_to_adf(text)
    types = [node["type"] for node in doc["content"]]
    assert types == ["heading", "paragraph", "heading", "bulletList"]


@patch("jira_client.httpx.post")
def test_post_audit_comment_sends_real_adf_body(mock_post, monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    mock_post.return_value = MagicMock(json=lambda: {"id": "1"})

    jira_client.post_audit_comment("T-1", "## Titulo\n\nTexto real.")

    sent_body = mock_post.call_args.kwargs["json"]["body"]
    assert sent_body["content"][0]["type"] == "heading"


@patch("jira_client.httpx.post")
def test_post_audit_comment_truncates_oversized_body(mock_post, monkeypatch):
    """Gap real identificado en auditoria ("gaps en los flujos de jira"): un
    reporte tecnico/razonamiento del juez generado por un LLM sin ningun tope
    de longitud podia superar el tamano real que Jira Cloud acepta para un
    comentario -- ese 400 se atrapaba como cualquier otro fallo (best-effort,
    solo WARNING en orchestration.py) y el comentario se perdia entero en
    silencio. Confirma que se trunca ANTES de mandarlo, en vez de dejar que
    Jira lo rechace."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    mock_post.return_value = MagicMock(json=lambda: {"id": "1"})

    huge_text = "x" * (jira_client._COMMENT_MAX_CHARS + 5000)
    jira_client.post_audit_comment("T-1", huge_text)

    sent_body = mock_post.call_args.kwargs["json"]["body"]
    sent_text = _adf_to_text(sent_body)
    assert len(sent_text) < len(huge_text)
    assert "truncado" in sent_text


@patch("jira_client.httpx.put")
def test_set_pipeline_blocked_label_adds_label_when_blocked(mock_put, monkeypatch):
    """Gap real identificado en auditoria ("gaps en los flujos de jira",
    KAN-6 real): este proyecto de Jira solo tiene 4 estados -- no existe un
    estado "Blocked" real, asi que JIRA_BLOCKED_STATUS y JIRA_REVIEW_STATUS
    terminan siendo el MISMO string ("En revision"). Un ticket bloqueado
    dias por output_guard quedaba visualmente identico a uno con un PR real
    listo para revisar. Confirma que se agrega un label distintivo."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    mock_put.return_value = MagicMock(raise_for_status=lambda: None)

    jira_client.set_pipeline_blocked_label("T-1", blocked=True)

    sent = mock_put.call_args.kwargs["json"]
    assert sent == {"update": {"labels": [{"add": "pipeline-blocked"}]}}


@patch("jira_client.httpx.put")
def test_set_pipeline_blocked_label_removes_label_when_not_blocked(mock_put, monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    mock_put.return_value = MagicMock(raise_for_status=lambda: None)

    jira_client.set_pipeline_blocked_label("T-1", blocked=False)

    sent = mock_put.call_args.kwargs["json"]
    assert sent == {"update": {"labels": [{"remove": "pipeline-blocked"}]}}


@patch("jira_client.httpx.put")
def test_set_pipeline_blocked_label_degrades_gracefully_on_failure(mock_put, monkeypatch):
    """Best-effort: una senal visual extra nunca debe tirar abajo la
    corrida real -- mismo criterio que comment_already_posted."""
    monkeypatch.setenv("JIRA_URL", "https://example.atlassian.net")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")

    def raise_error(*a, **k):
        raise httpx.ConnectError("caido")

    mock_put.side_effect = raise_error

    jira_client.set_pipeline_blocked_label("T-1", blocked=True)  # no debe lanzar
