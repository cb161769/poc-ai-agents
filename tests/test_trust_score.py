import json

import trust_score


def _write_signals(tmp_path, monkeypatch, entries):
    log_file = tmp_path / "trust_scores.jsonl"
    monkeypatch.setattr(trust_score, "TRUST_LOG", log_file)
    with log_file.open("w", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
    return log_file


def _signal(verdict="OK", output_guard_clean=True, tests_passed=True, new_sonar_issues=0,
            tests_adequate=True, falco_correlated=False, retries_used=0, tool_call_failed=False):
    return {
        "component": "Frontend",
        "judge_verdict": verdict,
        "verification": {
            "output_guard_clean": output_guard_clean,
            "tests_passed": tests_passed,
            "new_sonar_issues": new_sonar_issues,
            "tests_adequate": tests_adequate,
            "falco_correlated": falco_correlated,
        },
        "retries_used": retries_used,
        "tool_call_failed": tool_call_failed,
    }


def test_compute_trust_score_no_history_returns_neutral_prior(tmp_path, monkeypatch):
    monkeypatch.setattr(trust_score, "TRUST_LOG", tmp_path / "does_not_exist.jsonl")

    assert trust_score.compute_trust_score("Frontend") == 0.5


def test_compute_trust_score_all_clean_runs_scores_high(tmp_path, monkeypatch):
    _write_signals(tmp_path, monkeypatch, [_signal() for _ in range(10)])

    score = trust_score.compute_trust_score("Frontend")

    assert score > 0.9


def test_compute_trust_score_all_flagged_runs_scores_low(tmp_path, monkeypatch):
    _write_signals(tmp_path, monkeypatch, [
        _signal(verdict="FLAGGED", output_guard_clean=False, tests_passed=False,
                tests_adequate=False, falco_correlated=True, retries_used=2, tool_call_failed=True)
        for _ in range(10)
    ])

    score = trust_score.compute_trust_score("Frontend")

    assert score < 0.2


def test_compute_trust_score_only_uses_last_50_and_only_this_component(tmp_path, monkeypatch):
    old_bad = [_signal(verdict="FLAGGED", output_guard_clean=False, tests_passed=False,
                        tests_adequate=False) for _ in range(60)]
    recent_good = [_signal() for _ in range(50)]
    other_component_bad = [dict(_signal(verdict="FLAGGED"), component="AuthService")]
    _write_signals(tmp_path, monkeypatch, old_bad + recent_good + other_component_bad)

    score = trust_score.compute_trust_score("Frontend")

    assert score > 0.9  # solo ve los 50 mas recientes de Frontend, los FLAGGED viejos no cuentan


def test_compute_trust_score_ignores_corrupt_lines(tmp_path, monkeypatch):
    log_file = tmp_path / "trust_scores.jsonl"
    monkeypatch.setattr(trust_score, "TRUST_LOG", log_file)
    with log_file.open("w", encoding="utf-8") as f:
        f.write("not json at all\n")
        f.write(json.dumps(_signal()) + "\n")

    score = trust_score.compute_trust_score("Frontend")

    assert score > 0.9


def test_consecutive_ok_runs_counts_from_most_recent_backwards(tmp_path, monkeypatch):
    signals = [
        _signal(verdict="FLAGGED"),
        _signal(verdict="OK"),
        _signal(verdict="OK"),
        _signal(verdict="OK"),
    ]
    _write_signals(tmp_path, monkeypatch, signals)

    assert trust_score.consecutive_ok_runs("Frontend") == 3


def test_consecutive_ok_runs_zero_when_most_recent_is_flagged(tmp_path, monkeypatch):
    _write_signals(tmp_path, monkeypatch, [_signal(verdict="OK"), _signal(verdict="FLAGGED")])

    assert trust_score.consecutive_ok_runs("Frontend") == 0


def test_record_trust_signal_writes_valid_json_line(tmp_path, monkeypatch):
    log_file = tmp_path / "nested" / "trust_scores.jsonl"
    monkeypatch.setattr(trust_score, "TRUST_LOG", log_file)

    trust_score.record_trust_signal(
        component="Frontend", ticket_id="KAN-9", change_type="story",
        verification={"output_guard_clean": True, "tests_passed": True, "new_sonar_issues": 0,
                      "coding_agent_review_verdict": "approve", "tests_adequate": True, "falco_correlated": False},
        judge_verdict={"verdict": "OK", "reasoning": "todo bien"},
        retries_used=0,
    )

    assert log_file.exists()
    entry = json.loads(log_file.read_text(encoding="utf-8").strip())
    assert entry["ticket_id"] == "KAN-9"
    assert entry["component"] == "Frontend"
    assert entry["judge_verdict"] == "OK"
    assert entry["retries_used"] == 0


def test_record_trust_signal_handles_no_verdict_gracefully(tmp_path, monkeypatch):
    log_file = tmp_path / "trust_scores.jsonl"
    monkeypatch.setattr(trust_score, "TRUST_LOG", log_file)

    trust_score.record_trust_signal(
        component="Frontend", ticket_id="KAN-9", change_type="story",
        verification={}, judge_verdict=None, retries_used=0,
    )

    entry = json.loads(log_file.read_text(encoding="utf-8").strip())
    assert entry["judge_verdict"] is None


def test_record_trust_signal_never_raises_on_write_failure(monkeypatch):
    class UnwritableParent:
        def mkdir(self, *a, **k):
            raise OSError("disk full")

    class UnwritablePath:
        parent = UnwritableParent()

    monkeypatch.setattr(trust_score, "TRUST_LOG", UnwritablePath())

    trust_score.record_trust_signal(
        component="Frontend", ticket_id="KAN-9", change_type="story",
        verification={}, judge_verdict={"verdict": "OK"}, retries_used=0,
    )  # no debe lanzar
