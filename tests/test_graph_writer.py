"""Unit tests for graph_writer.py.

_build_write_params() is pure (validation/normalization/redaction, no I/O)
and tested directly, no mocks needed. record_run() talks to a real Neo4j
driver -- mocked here the same way tests/test_sonar_client.py mocks httpx:
the runtime pipeline never mocks Neo4j (cypher-shell/the real driver), but
the unit test layer always has, for every true external service in this
project.
"""
from unittest.mock import MagicMock, patch

import pytest

import firewall_proxy
import graph_writer


def _base_payload(**overrides):
    payload = {
        "run_id": "JIRA-1-1700000000",
        "ticket_key": "JIRA-1",
        "ticket_summary": "Arreglar el boton de login",
        "components": ["AuthService"],
        "branch": "copilot/JIRA-1-1700000000",
        "backend": "anthropic",
        "ts": "2026-01-01T00:00:00Z",
        "decisions": [
            {"stage": "firewall", "status": "APPROVED", "reason": None, "policy_reference": None},
            {"stage": "tests", "status": "PASSED", "reason": None, "policy_reference": None},
            {"stage": "judge", "status": "OK", "reason": "todo bien", "policy_reference": None},
        ],
    }
    payload.update(overrides)
    return payload


def test_build_write_params_normalizes_defaults():
    params = graph_writer._build_write_params(_base_payload())

    assert params["run_id"] == "JIRA-1-1700000000"
    assert params["ticket_key"] == "JIRA-1"
    assert params["is_epic"] is False
    assert params["child_ticket_keys"] == []
    assert params["components"] == ["AuthService"]


def test_build_write_params_keeps_is_epic_and_children():
    params = graph_writer._build_write_params(
        _base_payload(ticket_key="EPIC-1", is_epic=True, child_ticket_keys=["JIRA-1", "JIRA-2"])
    )

    assert params["is_epic"] is True
    assert params["child_ticket_keys"] == ["JIRA-1", "JIRA-2"]


def test_build_write_params_redacts_free_text_fields():
    payload = _base_payload(
        ticket_summary="la conexion usa password=Sup3rS3cr3t!",
        decisions=[{"stage": "judge", "status": "FLAGGED", "reason": "encontre secret_key=abc123XYZ", "policy_reference": "data-leak-evidence"}],
    )

    params = graph_writer._build_write_params(payload)

    assert "Sup3rS3cr3t" not in params["ticket_summary"]
    assert firewall_proxy.REDACTED_TOKEN in params["ticket_summary"]
    assert "abc123XYZ" not in params["decisions"][0]["reason"]
    assert firewall_proxy.REDACTED_TOKEN in params["decisions"][0]["reason"]
    assert params["decisions"][0]["policy_reference"] == "data-leak-evidence"


def test_build_write_params_leaves_clean_text_untouched():
    params = graph_writer._build_write_params(_base_payload(ticket_summary="Arreglar el boton de login"))
    assert params["ticket_summary"] == "Arreglar el boton de login"


def test_build_write_params_rejects_invalid_stage():
    payload = _base_payload(decisions=[{"stage": "not-a-real-stage", "status": "OK", "reason": None, "policy_reference": None}])
    with pytest.raises(ValueError):
        graph_writer._build_write_params(payload)


def test_build_write_params_requires_run_id_and_ticket_key():
    payload = _base_payload()
    del payload["run_id"]
    with pytest.raises(KeyError):
        graph_writer._build_write_params(payload)


def test_record_run_uses_story_label_when_not_epic():
    fake_session = MagicMock()
    fake_session.__enter__.return_value = fake_session
    fake_session.__exit__.return_value = False
    fake_driver = MagicMock()
    fake_driver.session.return_value = fake_session

    with patch("graph_writer.GraphDatabase.driver", return_value=fake_driver):
        result = graph_writer.record_run(_base_payload())

    assert result["recorded"] is True
    fake_session.execute_write.assert_called_once()
    fake_driver.close.assert_called_once()


def test_record_run_uses_epic_label_when_is_epic():
    fake_session = MagicMock()
    fake_session.__enter__.return_value = fake_session
    fake_session.__exit__.return_value = False
    fake_driver = MagicMock()
    fake_driver.session.return_value = fake_session

    captured_query = {}

    def _fake_execute_write(fn):
        tx = MagicMock()
        fn(tx)
        captured_query["query"] = tx.run.call_args[0][0]

    fake_session.execute_write.side_effect = _fake_execute_write

    with patch("graph_writer.GraphDatabase.driver", return_value=fake_driver):
        graph_writer.record_run(_base_payload(ticket_key="EPIC-1", is_epic=True, child_ticket_keys=["JIRA-1"]))

    assert "MERGE (root:Epic {key: $ticket_key})" in captured_query["query"]


def test_record_run_is_best_effort_on_connection_failure():
    with patch("graph_writer.GraphDatabase.driver", side_effect=ConnectionError("no se pudo conectar")):
        result = graph_writer.record_run(_base_payload())

    assert result == {"recorded": False, "error": "no se pudo conectar"}
