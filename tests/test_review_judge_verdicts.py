"""Unit tests for scripts/review_judge_verdicts.py's pure filtering logic --
no terminal input, no file I/O involved.
"""
from scripts.review_judge_verdicts import _unreviewed_entries


def test_unreviewed_entries_returns_all_when_no_reviews_yet():
    verdicts = [{"ts": "2026-01-01T00:00:00Z", "ticket_id": "T-1"}, {"ts": "2026-01-02T00:00:00Z", "ticket_id": "T-2"}]
    assert _unreviewed_entries(verdicts, []) == verdicts


def test_unreviewed_entries_excludes_already_reviewed_ones():
    verdicts = [{"ts": "2026-01-01T00:00:00Z", "ticket_id": "T-1"}, {"ts": "2026-01-02T00:00:00Z", "ticket_id": "T-2"}]
    reviews = [{"verdict_ts": "2026-01-01T00:00:00Z", "ticket_id": "T-1", "human_agreed": True}]

    result = _unreviewed_entries(verdicts, reviews)

    assert result == [{"ts": "2026-01-02T00:00:00Z", "ticket_id": "T-2"}]


def test_unreviewed_entries_empty_when_all_reviewed():
    verdicts = [{"ts": "2026-01-01T00:00:00Z", "ticket_id": "T-1"}]
    reviews = [{"verdict_ts": "2026-01-01T00:00:00Z", "ticket_id": "T-1", "human_agreed": False}]

    assert _unreviewed_entries(verdicts, reviews) == []
