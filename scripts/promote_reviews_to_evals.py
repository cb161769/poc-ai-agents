#!/usr/bin/env python3
"""Step 2 of the judge's continuous-improvement loop (dataset curation, NOT
model retraining): turns human-reviewed real runs (logs/judge_reviews.jsonl,
produced by scripts/review_judge_verdicts.py) into new cases in
evals/judge_eval_cases.jsonl.

Both confirmed-correct AND corrected verdicts get promoted -- a confirmation
becomes a regression case (protects that a future JUDGE_SYSTEM_PROMPT change
doesn't break something that works today), a correction becomes the case the
judge should start getting right going forward.

Idempotent: case_id is deterministic (ticket_id + the original run's
timestamp), so already-promoted reviews are skipped on repeat runs.

Usage: python3 scripts/promote_reviews_to_evals.py
"""
import json
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
VERDICT_LOG = LOG_DIR / "judge_verdicts.jsonl"
REVIEW_LOG = LOG_DIR / "judge_reviews.jsonl"
CASES_PATH = Path(__file__).resolve().parent.parent / "evals" / "judge_eval_cases.jsonl"


def _load_jsonl(path: Path) -> list:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _build_eval_case(review: dict, verdict_entry: dict) -> dict:
    return {
        "case_id": f"real-{review['ticket_id']}-{review['verdict_ts']}",
        "expected_verdict": review["human_expected_verdict"],
        "payload": verdict_entry["payload"],
    }


def main():
    reviews = _load_jsonl(REVIEW_LOG)
    verdicts_by_ts = {v["ts"]: v for v in _load_jsonl(VERDICT_LOG)}
    existing_cases = _load_jsonl(CASES_PATH)
    existing_ids = {c["case_id"] for c in existing_cases}

    new_cases = []
    skipped_no_payload = 0
    for review in reviews:
        verdict_entry = verdicts_by_ts.get(review["verdict_ts"])
        if verdict_entry is None or "payload" not in verdict_entry:
            skipped_no_payload += 1
            continue

        case = _build_eval_case(review, verdict_entry)
        if case["case_id"] in existing_ids:
            continue

        new_cases.append(case)
        existing_ids.add(case["case_id"])

    if new_cases:
        CASES_PATH.parent.mkdir(parents=True, exist_ok=True)
        with CASES_PATH.open("a", encoding="utf-8") as f:
            for case in new_cases:
                f.write(json.dumps(case, ensure_ascii=False) + "\n")

    print(f"Revisiones totales: {len(reviews)}")
    print(f"Casos nuevos agregados a {CASES_PATH.name}: {len(new_cases)}")
    print(f"Ya existian (no duplicados): {len(reviews) - len(new_cases) - skipped_no_payload}")
    if skipped_no_payload:
        print(f"Sin el verdict original en logs (no se pudo promover): {skipped_no_payload}")


if __name__ == "__main__":
    main()
