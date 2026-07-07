#!/usr/bin/env python3
"""Interactive human curation of real judge verdicts -- step 1 of the
continuous-improvement loop for the judge (no model retraining involved,
this is dataset curation): for each real run in logs/judge_verdicts.jsonl
that hasn't been reviewed yet, asks a human whether they agree with the
judge's OK/FLAGGED call, and records that in logs/judge_reviews.jsonl.

scripts/promote_reviews_to_evals.py consumes these reviews afterward to
grow evals/judge_eval_cases.jsonl -- both confirmed-correct AND corrected
verdicts get promoted (confirmations become regression cases, corrections
become the cases the judge should start getting right).

Usage: python3 scripts/review_judge_verdicts.py
"""
import json
import sys
import time
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
VERDICT_LOG = LOG_DIR / "judge_verdicts.jsonl"
REVIEW_LOG = LOG_DIR / "judge_reviews.jsonl"


def _load_jsonl(path: Path) -> list:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _unreviewed_entries(verdicts: list, reviews: list) -> list:
    reviewed_ts = {r["verdict_ts"] for r in reviews}
    return [v for v in verdicts if v["ts"] not in reviewed_ts]


def _append_review(review: dict):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with REVIEW_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(review, ensure_ascii=False) + "\n")


def _prompt_for_entry(entry: dict) -> dict | None:
    payload = entry.get("payload", {})
    ticket = payload.get("ticket", {})

    print("\n" + "=" * 70)
    print(f"Ticket: {entry.get('ticket_id')} — {ticket.get('summary', '(sin resumen)')}")
    print(f"Fuente del cambio: {payload.get('change_source')}")
    print(f"Veredicto del juez: {entry.get('verdict')}")
    print(f"Razonamiento: {entry.get('reasoning')}")

    answer = input("\n¿Estas de acuerdo con el veredicto del juez? [s/n/skip] ").strip().lower()
    if answer in ("skip", ""):
        return None

    if answer == "s":
        return {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "verdict_ts": entry["ts"],
            "ticket_id": entry.get("ticket_id"),
            "human_agreed": True,
            "human_expected_verdict": entry.get("verdict"),
            "human_note": "",
        }

    if answer == "n":
        corrected = input("Veredicto correcto segun vos [OK/FLAGGED]: ").strip().upper()
        while corrected not in ("OK", "FLAGGED"):
            corrected = input("Respuesta invalida, escribi OK o FLAGGED: ").strip().upper()
        note = input("Nota (opcional): ").strip()
        return {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "verdict_ts": entry["ts"],
            "ticket_id": entry.get("ticket_id"),
            "human_agreed": False,
            "human_expected_verdict": corrected,
            "human_note": note,
        }

    print("Respuesta no reconocida, se omite esta corrida.")
    return None


def main():
    verdicts = _load_jsonl(VERDICT_LOG)
    reviews = _load_jsonl(REVIEW_LOG)
    pending = _unreviewed_entries(verdicts, reviews)

    if not pending:
        print("No hay corridas del juez pendientes de revision.")
        return

    print(f"{len(pending)} corrida(s) del juez pendientes de revision.")
    reviewed_now = 0
    for entry in pending:
        review = _prompt_for_entry(entry)
        if review is not None:
            _append_review(review)
            reviewed_now += 1

    still_pending = len(pending) - reviewed_now
    print(f"\nRevisadas en esta corrida: {reviewed_now}. Pendientes: {still_pending}.")
    if reviewed_now:
        print("Corre scripts/promote_reviews_to_evals.py para sumar estas revisiones a evals/judge_eval_cases.jsonl.")


if __name__ == "__main__":
    try:
        main()
    except (EOFError, KeyboardInterrupt):
        print("\nInterrumpido, nada se pierde: las revisiones ya guardadas quedan en logs/judge_reviews.jsonl.")
        sys.exit(0)
