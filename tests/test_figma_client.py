"""Unit tests for figma_client.py's node summarization -- mocks httpx.get
(stdlib unittest.mock) instead of hitting the real Figma REST API.
"""
from unittest.mock import MagicMock, patch

import figma_client


def _fake_response(payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


@patch("figma_client.httpx.get")
def test_fetch_node_live_summarizes_dimensions_color_and_nested_text(mock_get, monkeypatch):
    monkeypatch.setenv("FIGMA_API_TOKEN", "fake-token")
    mock_get.return_value = _fake_response(
        {
            "nodes": {
                "12:34": {
                    "document": {
                        "name": "Login Button",
                        "type": "FRAME",
                        "absoluteBoundingBox": {"x": 0, "y": 0, "width": 120.4, "height": 48.2},
                        "fills": [{"type": "SOLID", "color": {"r": 0.121, "g": 0.305, "b": 0.549, "a": 1}}],
                        "children": [
                            {
                                "name": "Label",
                                "type": "TEXT",
                                "characters": "Ingresar",
                                "style": {"fontFamily": "Inter", "fontSize": 16, "fontWeight": 600},
                                "absoluteBoundingBox": {"x": 10, "y": 10, "width": 80, "height": 20},
                            }
                        ],
                    }
                }
            }
        }
    )

    result = figma_client.fetch_node_live("FILEKEY123", "12:34")

    assert result["found"] is True
    assert result["file_key"] == "FILEKEY123"
    assert result["node_id"] == "12:34"

    summary = result["summary"]
    assert summary["name"] == "Login Button"
    assert summary["type"] == "FRAME"
    assert summary["width"] == 120
    assert summary["height"] == 48
    assert summary["fill_color"] == "#1F4E8C"

    child = summary["children"][0]
    assert child["name"] == "Label"
    assert child["type"] == "TEXT"
    assert child["text"] == "Ingresar"
    assert child["font"] == {"family": "Inter", "size": 16, "weight": 600}


@patch("figma_client.httpx.get")
def test_fetch_node_live_not_found_when_node_id_missing(mock_get, monkeypatch):
    monkeypatch.setenv("FIGMA_API_TOKEN", "fake-token")
    mock_get.return_value = _fake_response({"nodes": {}})

    result = figma_client.fetch_node_live("FILEKEY123", "99:99")

    assert result["found"] is False
    assert result["summary"] is None


@patch("figma_client.httpx.get")
def test_fetch_node_live_not_found_when_node_entry_is_null(mock_get, monkeypatch):
    monkeypatch.setenv("FIGMA_API_TOKEN", "fake-token")
    mock_get.return_value = _fake_response({"nodes": {"12:34": None}})

    result = figma_client.fetch_node_live("FILEKEY123", "12:34")

    assert result["found"] is False


def test_color_to_hex_rounds_correctly():
    assert figma_client._color_to_hex({"r": 1.0, "g": 0.0, "b": 0.0}) == "#FF0000"
    assert figma_client._color_to_hex({"r": 0.0, "g": 0.0, "b": 0.0}) == "#000000"
