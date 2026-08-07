import json

import metrics_exporter


def test_render_metrics_empty_logs_returns_valid_prometheus_text(tmp_path, monkeypatch):
    monkeypatch.setattr(metrics_exporter, "JUDGE_LOG", tmp_path / "does_not_exist.jsonl")
    monkeypatch.setattr(metrics_exporter, "CODING_AGENT_LOG", tmp_path / "does_not_exist.jsonl")
    monkeypatch.setattr(metrics_exporter, "FIREWALL_LOG", tmp_path / "does_not_exist.jsonl")
    monkeypatch.setattr(metrics_exporter, "TRUST_LOG", tmp_path / "does_not_exist.jsonl")

    output = metrics_exporter.render_metrics().decode()

    assert "judge_verdicts_total" in output
    assert "trust_score" in output


def test_render_metrics_counts_judge_verdicts_by_outcome(tmp_path, monkeypatch):
    log = tmp_path / "judge_verdicts.jsonl"
    log.write_text(
        "\n".join(json.dumps(e) for e in [{"verdict": "OK"}, {"verdict": "OK"}, {"verdict": "FLAGGED"}]) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(metrics_exporter, "JUDGE_LOG", log)
    monkeypatch.setattr(metrics_exporter, "CODING_AGENT_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "FIREWALL_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "TRUST_LOG", tmp_path / "x.jsonl")

    output = metrics_exporter.render_metrics().decode()

    assert 'judge_verdicts_total{verdict="OK"} 2.0' in output
    assert 'judge_verdicts_total{verdict="FLAGGED"} 1.0' in output


def test_render_metrics_sums_firewall_redactions(tmp_path, monkeypatch):
    log = tmp_path / "firewall_audit.jsonl"
    log.write_text(
        "\n".join(json.dumps(e) for e in [{"redactions_applied": 2}, {"redactions_applied": 3}]) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(metrics_exporter, "JUDGE_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "CODING_AGENT_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "FIREWALL_LOG", log)
    monkeypatch.setattr(metrics_exporter, "TRUST_LOG", tmp_path / "x.jsonl")

    output = metrics_exporter.render_metrics().decode()

    assert "firewall_redactions_total_from_audit_log 5.0" in output


def test_render_metrics_exposes_trust_score_per_component(tmp_path, monkeypatch):
    log = tmp_path / "trust_scores.jsonl"
    entries = [
        {
            "component": "Frontend", "judge_verdict": "OK",
            "verification": {"output_guard_clean": True, "tests_passed": True, "new_sonar_issues": 0, "tests_adequate": True, "falco_correlated": False},
            "retries_used": 0, "tool_call_failed": False,
        }
        for _ in range(5)
    ]
    log.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")
    monkeypatch.setattr(metrics_exporter, "JUDGE_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "CODING_AGENT_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "FIREWALL_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "TRUST_LOG", log)

    output = metrics_exporter.render_metrics().decode()

    assert 'trust_score{component="Frontend"}' in output


def test_render_metrics_ignores_corrupt_lines(tmp_path, monkeypatch):
    log = tmp_path / "judge_verdicts.jsonl"
    log.write_text("not json\n" + json.dumps({"verdict": "OK"}) + "\n", encoding="utf-8")
    monkeypatch.setattr(metrics_exporter, "JUDGE_LOG", log)
    monkeypatch.setattr(metrics_exporter, "CODING_AGENT_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "FIREWALL_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "TRUST_LOG", tmp_path / "x.jsonl")

    output = metrics_exporter.render_metrics().decode()

    assert 'judge_verdicts_total{verdict="OK"} 1.0' in output


def test_health_endpoint():
    from fastapi.testclient import TestClient

    client = TestClient(metrics_exporter.app)
    resp = client.get("/health")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_metrics_endpoint_returns_prometheus_content_type(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setattr(metrics_exporter, "JUDGE_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "CODING_AGENT_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "FIREWALL_LOG", tmp_path / "x.jsonl")
    monkeypatch.setattr(metrics_exporter, "TRUST_LOG", tmp_path / "x.jsonl")

    client = TestClient(metrics_exporter.app)
    resp = client.get("/metrics")

    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
