"""Unit tests for orchestration.py's pure epic-mode helper: deciding
whether all of an epic's children live in the same repo, given what the
Neo4j graph reports for each component's repo_url. No Prefect flow/task
machinery involved, no network.
"""
from orchestration import _resolve_single_repo


def test_resolve_single_repo_ok_when_all_agree():
    ok, repo_url, reason = _resolve_single_repo({"AuthService": "https://github.com/org/repo", "Frontend": "https://github.com/org/repo"})

    assert ok is True
    assert repo_url == "https://github.com/org/repo"
    assert reason == ""


def test_resolve_single_repo_rejects_when_repo_url_missing():
    ok, repo_url, reason = _resolve_single_repo({"AuthService": "https://github.com/org/repo", "Frontend": None})

    assert ok is False
    assert repo_url is None
    assert "Frontend" in reason


def test_resolve_single_repo_rejects_when_repos_differ():
    ok, repo_url, reason = _resolve_single_repo(
        {"AuthService": "https://github.com/org/repo-a", "DataWorker": "https://github.com/org/repo-b"}
    )

    assert ok is False
    assert repo_url is None
    assert "repo-a" in reason
    assert "repo-b" in reason


def test_resolve_single_repo_ok_with_single_component():
    ok, repo_url, reason = _resolve_single_repo({"AuthService": "https://github.com/org/repo"})

    assert ok is True
    assert repo_url == "https://github.com/org/repo"
