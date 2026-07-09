"""Real Figma client used by run_poc_loop.sh / orchestration.py (optional
stage, only runs if the Jira ticket's description carries a Figma link).

This is the AUTOMATED counterpart to prompts/build_ui_from_figma.md: that
prompt drives the `figma-dev-mode` MCP (local, needs Figma Desktop open,
no token) in an interactive Copilot Chat session where a human decides what
to build. This module instead calls the real Figma REST API
(api.figma.com, needs FIGMA_API_TOKEN) so the headless pipeline can pull
real specs (colors, dimensions, text) for the exact node the ticket points
to, with no human at Figma Desktop required.

jira_client.py's _extract_figma_link() parses the file_key + node_id out of
the ticket description; this module fetches and summarizes that node.
"""
import json
import os
import sys

import httpx
from dotenv import load_dotenv

from cache_utils import cached_call
from retry_utils import retry_call
from secrets_provider import require_secret

load_dotenv()

# How many levels of children to summarize below the requested node. Figma
# frames can nest deeply; capping this keeps the prompt readable instead of
# dumping the whole subtree.
MAX_SUMMARY_DEPTH = 3


def _figma_url() -> str:
    return "https://api.figma.com/v1"


def _auth_headers() -> dict:
    return {"X-Figma-Token": require_secret("FIGMA_API_TOKEN")}


def _color_to_hex(color: dict) -> str:
    r = round(color.get("r", 0) * 255)
    g = round(color.get("g", 0) * 255)
    b = round(color.get("b", 0) * 255)
    return f"#{r:02X}{g:02X}{b:02X}"


def _summarize_node(node: dict, depth: int = 0) -> dict:
    summary = {"name": node.get("name"), "type": node.get("type")}

    box = node.get("absoluteBoundingBox")
    if box:
        summary["width"] = round(box.get("width", 0))
        summary["height"] = round(box.get("height", 0))

    fills = node.get("fills") or []
    solid_fill = next((f for f in fills if f.get("type") == "SOLID" and f.get("color")), None)
    if solid_fill:
        summary["fill_color"] = _color_to_hex(solid_fill["color"])

    if node.get("type") == "TEXT":
        summary["text"] = node.get("characters", "")
        style = node.get("style") or {}
        if style:
            summary["font"] = {
                "family": style.get("fontFamily"),
                "size": style.get("fontSize"),
                "weight": style.get("fontWeight"),
            }

    children = node.get("children") or []
    if children and depth < MAX_SUMMARY_DEPTH:
        summary["children"] = [_summarize_node(child, depth + 1) for child in children]

    return summary


def fetch_node_live(file_key: str, node_id: str) -> dict:
    def _fetch():
        r = httpx.get(
            f"{_figma_url()}/files/{file_key}/nodes",
            headers=_auth_headers(),
            params={"ids": node_id},
            timeout=15.0,
        )
        r.raise_for_status()
        return r

    resp = retry_call(_fetch)
    data = resp.json()

    node_entry = (data.get("nodes") or {}).get(node_id)
    if not node_entry or not node_entry.get("document"):
        return {"file_key": file_key, "node_id": node_id, "found": False, "summary": None}

    return {
        "file_key": file_key,
        "node_id": node_id,
        "found": True,
        "summary": _summarize_node(node_entry["document"]),
    }


def get_node_summary(file_key: str, node_id: str) -> dict:
    return cached_call(
        namespace="figma_node",
        params={"file_key": file_key, "node_id": node_id},
        fetch_fn=lambda: fetch_node_live(file_key, node_id),
    )


def main():
    if len(sys.argv) != 3:
        print(json.dumps({"error": "usage: figma_client.py <file_key> <node_id>"}), file=sys.stderr)
        sys.exit(1)

    file_key, node_id = sys.argv[1], sys.argv[2]
    try:
        result = get_node_summary(file_key, node_id)
    except KeyError as exc:
        print(json.dumps({"error": f"missing_env_var:{exc.args[0]}"}), file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPStatusError as exc:
        print(
            json.dumps({"error": "figma_http_error", "status_code": exc.response.status_code, "body": exc.response.text}),
            file=sys.stderr,
        )
        sys.exit(1)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
