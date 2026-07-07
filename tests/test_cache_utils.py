"""Unit tests for cache_utils.cached_call -- the file-based TTL cache
shared by jira_client.py and sonar_client.py. Runs entirely against
tmp_path, no network involved.
"""
import time

import cache_utils


def test_first_call_is_a_miss_and_calls_fetch_fn(tmp_path, monkeypatch):
    monkeypatch.setattr(cache_utils, "CACHE_DIR", tmp_path)
    calls = []

    def fetch():
        calls.append(1)
        return {"value": 42}

    result = cache_utils.cached_call("ns", {"k": "v"}, fetch, ttl_seconds=300)

    assert result["value"] == 42
    assert result["_cache"]["hit"] is False
    assert len(calls) == 1


def test_second_call_within_ttl_is_a_hit_and_skips_fetch_fn(tmp_path, monkeypatch):
    monkeypatch.setattr(cache_utils, "CACHE_DIR", tmp_path)
    calls = []

    def fetch():
        calls.append(1)
        return {"value": 42}

    cache_utils.cached_call("ns", {"k": "v"}, fetch, ttl_seconds=300)
    result = cache_utils.cached_call("ns", {"k": "v"}, fetch, ttl_seconds=300)

    assert result["_cache"]["hit"] is True
    assert len(calls) == 1


def test_call_after_ttl_expires_is_a_miss_again(tmp_path, monkeypatch):
    monkeypatch.setattr(cache_utils, "CACHE_DIR", tmp_path)
    calls = []

    def fetch():
        calls.append(1)
        return {"value": 42}

    cache_utils.cached_call("ns", {"k": "v"}, fetch, ttl_seconds=0)
    time.sleep(0.01)
    result = cache_utils.cached_call("ns", {"k": "v"}, fetch, ttl_seconds=0)

    assert result["_cache"]["hit"] is False
    assert len(calls) == 2


def test_different_params_get_different_cache_entries(tmp_path, monkeypatch):
    monkeypatch.setattr(cache_utils, "CACHE_DIR", tmp_path)

    result_a = cache_utils.cached_call("ns", {"k": "a"}, lambda: {"value": "a"}, ttl_seconds=300)
    result_b = cache_utils.cached_call("ns", {"k": "b"}, lambda: {"value": "b"}, ttl_seconds=300)

    assert result_a["value"] == "a"
    assert result_b["value"] == "b"
