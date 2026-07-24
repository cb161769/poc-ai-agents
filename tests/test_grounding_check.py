"""Unit tests for grounding_check.py -- bespoke-minicheck (mockeado aca,
sin red real) devuelve Yes/No sobre si un claim esta respaldado por un
document real. Estos tests verifican el envoltorio: parseo de la
respuesta, truncado del documento, y que cualquier fallo se trague como
None (best-effort, nunca bloquea la corrida real).
"""
from unittest.mock import MagicMock, patch

import grounding_check


def test_verify_claim_grounded_returns_true_for_yes(monkeypatch):
    monkeypatch.setattr(grounding_check, "GROUNDING_CHECK_ENABLED", True)
    resp = MagicMock()
    resp.json.return_value = {"response": "Yes"}
    with patch("grounding_check.httpx.post", return_value=resp) as mock_post:
        result = grounding_check.verify_claim_grounded("el archivo X existe", "X existe de verdad")

    assert result is True
    assert mock_post.call_args.kwargs["json"]["model"] == "bespoke-minicheck"


def test_verify_claim_grounded_returns_false_for_no(monkeypatch):
    monkeypatch.setattr(grounding_check, "GROUNDING_CHECK_ENABLED", True)
    resp = MagicMock()
    resp.json.return_value = {"response": "No"}
    with patch("grounding_check.httpx.post", return_value=resp):
        result = grounding_check.verify_claim_grounded("el archivo X existe", "Y no existe")

    assert result is False


def test_verify_claim_grounded_returns_none_when_disabled(monkeypatch):
    monkeypatch.setattr(grounding_check, "GROUNDING_CHECK_ENABLED", False)
    with patch("grounding_check.httpx.post") as mock_post:
        result = grounding_check.verify_claim_grounded("doc", "claim")

    assert result is None
    mock_post.assert_not_called()


def test_verify_claim_grounded_returns_none_for_empty_inputs(monkeypatch):
    monkeypatch.setattr(grounding_check, "GROUNDING_CHECK_ENABLED", True)
    with patch("grounding_check.httpx.post") as mock_post:
        assert grounding_check.verify_claim_grounded("", "claim") is None
        assert grounding_check.verify_claim_grounded("doc", "") is None
    mock_post.assert_not_called()


def test_verify_claim_grounded_returns_none_on_backend_failure(monkeypatch):
    monkeypatch.setattr(grounding_check, "GROUNDING_CHECK_ENABLED", True)
    import httpx as real_httpx

    def boom(*a, **k):
        raise real_httpx.ConnectError("ollama no disponible")

    with patch("grounding_check.httpx.post", side_effect=boom):
        result = grounding_check.verify_claim_grounded("doc", "claim")

    assert result is None


def test_verify_claim_grounded_returns_none_on_unexpected_response(monkeypatch):
    monkeypatch.setattr(grounding_check, "GROUNDING_CHECK_ENABLED", True)
    resp = MagicMock()
    resp.json.return_value = {"response": "maybe? unclear"}
    with patch("grounding_check.httpx.post", return_value=resp):
        result = grounding_check.verify_claim_grounded("doc", "claim")

    assert result is None


def test_verify_claim_grounded_truncates_long_document(monkeypatch):
    monkeypatch.setattr(grounding_check, "GROUNDING_CHECK_ENABLED", True)
    monkeypatch.setattr(grounding_check, "GROUNDING_CHECK_MAX_DOCUMENT_CHARS", 50)
    resp = MagicMock()
    resp.json.return_value = {"response": "Yes"}
    long_doc = "x" * 500

    with patch("grounding_check.httpx.post", return_value=resp) as mock_post:
        grounding_check.verify_claim_grounded(long_doc, "claim corto")

    sent_prompt = mock_post.call_args.kwargs["json"]["prompt"]
    assert "(truncado)" in sent_prompt
    assert len(sent_prompt) < 500
