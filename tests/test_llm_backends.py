"""Unit tests for llm_backends.py: the backend registry (priority order,
pricing, retry policy, model limits) and spend_today()/is_within_budget()
(reading real log files from tmp_path, no mocks needed -- these are pure
file reads).
"""
import json
import time

import llm_backends


def test_get_backend_priority_default(monkeypatch):
    monkeypatch.delenv("LLM_BACKEND_PRIORITY", raising=False)
    assert llm_backends.get_backend_priority() == ["anthropic", "ollama"]


def test_get_backend_priority_custom(monkeypatch):
    monkeypatch.setenv("LLM_BACKEND_PRIORITY", "ollama, anthropic")
    assert llm_backends.get_backend_priority() == ["ollama", "anthropic"]


def test_every_default_backend_has_retry_policy():
    for backend in llm_backends.BACKEND_PRIORITY_DEFAULT:
        assert backend in llm_backends.RETRY_POLICY_PER_BACKEND


def test_every_default_backend_has_model_limits():
    for backend in llm_backends.BACKEND_PRIORITY_DEFAULT:
        assert backend in llm_backends.MODEL_LIMITS
        assert llm_backends.MODEL_LIMITS[backend]["max_tokens"] > 0


def test_anthropic_retry_policy_includes_529_overloaded():
    assert 529 in llm_backends.RETRY_POLICY_PER_BACKEND["anthropic"]["retryable_status_codes"]


def test_estimate_cost_usd_known_backend_and_model():
    assert llm_backends.estimate_cost_usd("anthropic", "claude-sonnet-5", 1_000_000, 0) == 3.0


def test_estimate_cost_usd_ollama_is_always_zero():
    assert llm_backends.estimate_cost_usd("ollama", "llama3.1", 1_000_000, 1_000_000) == 0.0


def _write_log_entry(path, ts, backend, cost):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": ts, "backend": backend, "estimated_cost_usd": cost}) + "\n")


def test_spend_today_sums_matching_backend_and_day(tmp_path, monkeypatch):
    monkeypatch.setattr(llm_backends, "_LOG_DIR", tmp_path)
    today = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    _write_log_entry(tmp_path / "judge_verdicts.jsonl", today, "anthropic", 0.5)
    _write_log_entry(tmp_path / "coding_agent_runs.jsonl", today, "anthropic", 0.25)
    _write_log_entry(tmp_path / "judge_verdicts.jsonl", today, "ollama", 0.0)
    _write_log_entry(tmp_path / "judge_verdicts.jsonl", "2020-01-01T00:00:00Z", "anthropic", 100.0)

    assert llm_backends.spend_today("anthropic") == 0.75
    assert llm_backends.spend_today("ollama") == 0.0


def test_spend_today_ignores_corrupt_lines_and_missing_files(tmp_path, monkeypatch):
    monkeypatch.setattr(llm_backends, "_LOG_DIR", tmp_path)
    today = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    log_path = tmp_path / "judge_verdicts.jsonl"
    log_path.write_text("not json at all\n" + json.dumps({"ts": today, "backend": "anthropic", "estimated_cost_usd": 1.0}) + "\n")

    assert llm_backends.spend_today("anthropic") == 1.0


def test_is_within_budget_true_when_no_budget_set(monkeypatch):
    monkeypatch.delenv("LLM_DAILY_BUDGET_USD", raising=False)
    assert llm_backends.is_within_budget("anthropic") is True


def test_is_within_budget_false_when_spend_exceeds_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(llm_backends, "_LOG_DIR", tmp_path)
    monkeypatch.setenv("LLM_DAILY_BUDGET_USD", "1.0")
    today = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _write_log_entry(tmp_path / "judge_verdicts.jsonl", today, "anthropic", 2.0)

    assert llm_backends.is_within_budget("anthropic") is False


def test_is_within_budget_true_when_spend_under_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(llm_backends, "_LOG_DIR", tmp_path)
    monkeypatch.setenv("LLM_DAILY_BUDGET_USD", "10.0")
    today = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _write_log_entry(tmp_path / "judge_verdicts.jsonl", today, "anthropic", 2.0)

    assert llm_backends.is_within_budget("anthropic") is True
