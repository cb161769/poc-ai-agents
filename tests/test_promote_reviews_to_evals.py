"""Unit tests for scripts/promote_reviews_to_evals.py's pure case-building
logic -- no file I/O involved.
"""
from scripts.promote_reviews_to_evals import _build_eval_case


def test_build_eval_case_from_agreed_review():
    review = {
        "ts": "2026-07-08T10:00:00Z",
        "verdict_ts": "2026-07-08T09:55:00Z",
        "ticket_id": "JIRA-123",
        "human_agreed": True,
        "human_expected_verdict": "OK",
        "human_note": "",
    }
    verdict_entry = {
        "ts": "2026-07-08T09:55:00Z",
        "ticket_id": "JIRA-123",
        "verdict": "OK",
        "reasoning": "el diff resuelve el ticket sin riesgos nuevos",
        "payload": {"ticket": {"ticket_id": "JIRA-123"}, "change_source": "local_diff", "change_description": "diff real"},
    }

    case = _build_eval_case(review, verdict_entry)

    assert case["case_id"] == "real-JIRA-123-2026-07-08T09:55:00Z"
    assert case["expected_verdict"] == "OK"
    assert case["payload"] == verdict_entry["payload"]


def test_build_eval_case_from_corrected_review():
    review = {
        "ts": "2026-07-08T11:00:00Z",
        "verdict_ts": "2026-07-08T10:50:00Z",
        "ticket_id": "JIRA-456",
        "human_agreed": False,
        "human_expected_verdict": "FLAGGED",
        "human_note": "el juez no vio que el diff toca un archivo fuera del alcance del ticket",
    }
    verdict_entry = {
        "ts": "2026-07-08T10:50:00Z",
        "ticket_id": "JIRA-456",
        "verdict": "OK",
        "payload": {"ticket": {"ticket_id": "JIRA-456"}, "change_source": "local_diff", "change_description": "diff real"},
    }

    case = _build_eval_case(review, verdict_entry)

    assert case["expected_verdict"] == "FLAGGED"
    assert case["case_id"] == "real-JIRA-456-2026-07-08T10:50:00Z"
