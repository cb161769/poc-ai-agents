"""Real SonarQube client used by run_poc_loop.sh (stage 3).

Calls the local SonarQube server's Web API for the real issues detected by
sonar-scanner on sample-repo/<component>, filtered to the component that
matches the Jira ticket's repository_origen. Also exposes a
fetch_resolved_history() helper reused by scripts/index_rag_corpus.py to
seed the RAG corpus with real, previously-resolved findings.
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


def _sonar_url() -> str:
    return os.environ.get("SONAR_URL", "http://localhost:9000").rstrip("/")


def _auth():
    token = require_secret("SONAR_TOKEN")
    return (token, "")


def fetch_issues_live(project_key: str) -> dict:
    def _fetch():
        r = httpx.get(
            f"{_sonar_url()}/api/issues/search",
            auth=_auth(),
            params={"componentKeys": project_key, "resolved": "false", "ps": 100},
            timeout=15.0,
        )
        r.raise_for_status()
        return r

    resp = retry_call(_fetch)
    data = resp.json()
    issues = [
        {
            "rule": i.get("rule"),
            "severity": i.get("severity"),
            "message": i.get("message"),
            "component": i.get("component"),
            "line": i.get("line"),
        }
        for i in data.get("issues", [])
    ]
    return {"project_key": project_key, "issues": issues, "total": data.get("total", 0)}


def get_issues(project_key: str) -> dict:
    return cached_call(
        namespace="sonar_issues",
        params={"project_key": project_key, "sonar_url": _sonar_url()},
        fetch_fn=lambda: fetch_issues_live(project_key),
    )


def fetch_resolved_history() -> list:
    """Real, previously-resolved issues across all scanned projects — used to
    seed the sonar_history_resolved Qdrant collection (see index_rag_corpus.py)."""
    def _fetch():
        r = httpx.get(
            f"{_sonar_url()}/api/issues/search",
            auth=_auth(),
            params={"resolved": "true", "ps": 100},
            timeout=15.0,
        )
        r.raise_for_status()
        return r

    resp = retry_call(_fetch)
    data = resp.json()
    return data.get("issues", [])


def main():
    if len(sys.argv) != 2:
        print(json.dumps({"error": "usage: sonar_client.py <project_key>"}), file=sys.stderr)
        sys.exit(1)

    project_key = sys.argv[1]
    try:
        result = get_issues(project_key)
    except KeyError as exc:
        print(json.dumps({"error": f"missing_env_var:{exc.args[0]}"}), file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPStatusError as exc:
        print(
            json.dumps({"error": "sonar_http_error", "status_code": exc.response.status_code, "body": exc.response.text}),
            file=sys.stderr,
        )
        sys.exit(1)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
