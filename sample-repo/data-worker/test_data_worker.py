"""Minimal real test suite — this is what the testing agent runs (`pytest`)
against the branch a coding agent produces, before the judge ever sees it.
"""
from unittest.mock import MagicMock, patch

from data_worker import DataWorker


def test_fetch_token_returns_the_token():
    worker = DataWorker("https://auth.example.com")
    mock_response = MagicMock()
    mock_response.json.return_value = {"token": "abc123"}

    with patch("data_worker.requests.post", return_value=mock_response) as mock_post:
        token = worker.fetch_token("demo", "demo")

    assert token == "abc123"
    mock_post.assert_called_once()


def test_run_batch_returns_the_pending_count():
    worker = DataWorker("https://auth.example.com")
    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 1}, {"id": 2}, {"id": 3}]

    with patch("data_worker.requests.get", return_value=mock_response):
        count = worker.run_batch("some-token")

    assert count == 3
