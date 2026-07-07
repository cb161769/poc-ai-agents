"""Unit tests for sonar_client.py's response mapping -- mocks httpx.get
(stdlib unittest.mock) instead of hitting a real SonarQube server.
"""
from unittest.mock import MagicMock, patch

import sonar_client


def _fake_response(payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


@patch("sonar_client.httpx.get")
def test_fetch_issues_live_maps_expected_fields(mock_get, monkeypatch):
    monkeypatch.setenv("SONAR_TOKEN", "fake-token")
    mock_get.return_value = _fake_response(
        {
            "total": 1,
            "issues": [
                {
                    "rule": "java:S2068",
                    "severity": "BLOCKER",
                    "message": "Hardcoded credential",
                    "component": "AuthService:src/main/java/AuthService.java",
                    "line": 14,
                    "extra_field_we_dont_care_about": "ignored",
                }
            ],
        }
    )

    result = sonar_client.fetch_issues_live("AuthService")

    assert result["project_key"] == "AuthService"
    assert result["total"] == 1
    assert len(result["issues"]) == 1
    issue = result["issues"][0]
    assert issue == {
        "rule": "java:S2068",
        "severity": "BLOCKER",
        "message": "Hardcoded credential",
        "component": "AuthService:src/main/java/AuthService.java",
        "line": 14,
    }


@patch("sonar_client.httpx.get")
def test_fetch_issues_live_with_no_issues_returns_empty_list(mock_get, monkeypatch):
    monkeypatch.setenv("SONAR_TOKEN", "fake-token")
    mock_get.return_value = _fake_response({"total": 0, "issues": []})

    result = sonar_client.fetch_issues_live("DataWorker")

    assert result["issues"] == []
    assert result["total"] == 0
