"""Aggregates logs/copilot_contribution.jsonl and logs/judge_verdicts.jsonl
into a sprint-level summary: how many tickets Copilot actually touched, how
often the firewall approved vs. rejected, how often a secret had to be
redacted, how often a suggestion was actually applied to a review branch,
what fraction of applied changes passed the real test suite (a proxy for
coding-agent precision — we can't grade "did it solve the ticket correctly"
without a human, but "did it pass the tests it's supposed to pass" is a
real, measurable floor), and the judge's latency/cost per run.

This is the evidence a VP would want for "how much did Copilot collaborate
this sprint, and how much did that cost" — not just whether the PoC ran.

Usage: python3 scripts/report_sprint_metrics.py [--since 2026-07-01]
"""
import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "copilot_contribution.jsonl"
JUDGE_LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "judge_verdicts.jsonl"


def _load_jsonl(path: Path, since_dt) -> list[dict]:
    if not path.exists():
        return []
    entries = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        entry = json.loads(line)
        if since_dt:
            entry_dt = datetime.strptime(entry["ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if entry_dt < since_dt:
                continue
        entries.append(entry)
    return entries


def load_entries(since: str | None) -> list[dict]:
    since_dt = datetime.fromisoformat(since).replace(tzinfo=timezone.utc) if since else None
    return _load_jsonl(LOG_PATH, since_dt)


def load_judge_entries(since: str | None) -> list[dict]:
    since_dt = datetime.fromisoformat(since).replace(tzinfo=timezone.utc) if since else None
    return _load_jsonl(JUDGE_LOG_PATH, since_dt)


def summarize(entries: list[dict]) -> dict:
    total = len(entries)
    tickets = {e["ticket_id"] for e in entries}
    status_counts = Counter(e["firewall_status"] for e in entries)
    suggested = sum(1 for e in entries if e.get("copilot_suggested"))
    applied = sum(1 for e in entries if e.get("copilot_applied"))
    redacted_runs = sum(1 for e in entries if e.get("redactions_applied", 0) > 0)
    total_redactions = sum(e.get("redactions_applied", 0) for e in entries)

    tests_graded = [e for e in entries if e.get("tests_passed") is not None]
    tests_passed = sum(1 for e in tests_graded if e.get("tests_passed") is True)

    return {
        "total_runs": total,
        "unique_tickets": len(tickets),
        "approved": status_counts.get("APPROVED", 0),
        "rejected": status_counts.get("REJECTED", 0),
        "copilot_suggested": suggested,
        "copilot_applied": applied,
        "runs_with_redaction": redacted_runs,
        "total_redactions": total_redactions,
        "changes_tested": len(tests_graded),
        "changes_passed_tests": tests_passed,
    }


def summarize_judge(entries: list[dict]) -> dict:
    total = len(entries)
    flagged = sum(1 for e in entries if e.get("verdict") == "FLAGGED")
    total_cost = sum(e.get("estimated_cost_usd", 0) for e in entries)
    total_latency = sum(e.get("latency_seconds", 0) for e in entries)
    backends = Counter(e.get("backend", "desconocido") for e in entries)

    return {
        "total_verdicts": total,
        "flagged": flagged,
        "avg_latency_seconds": (total_latency / total) if total else 0.0,
        "total_cost_usd": total_cost,
        "backends": dict(backends),
    }


def print_report(summary: dict, judge_summary: dict, since: str | None):
    scope = f"desde {since}" if since else "historico completo"
    print(f"== Metricas de colaboracion de Copilot ({scope}) ==")
    print(f"  Corridas totales:            {summary['total_runs']}")
    print(f"  Tickets unicos tocados:      {summary['unique_tickets']}")
    print(f"  Aprobadas por el firewall:   {summary['approved']}")
    print(f"  Rechazadas por el firewall:  {summary['rejected']}")
    if summary["total_runs"]:
        approval_rate = 100 * summary["approved"] / summary["total_runs"]
        print(f"  Tasa de aprobacion:          {approval_rate:.0f}%")
    print(f"  Copilot sugirio un cambio:   {summary['copilot_suggested']}")
    print(f"  Sugerencia aplicada a rama:  {summary['copilot_applied']}")
    print(f"  Corridas con dato redactado: {summary['runs_with_redaction']}")
    print(f"  Total de redacciones:        {summary['total_redactions']}")

    if summary["changes_tested"]:
        precision_proxy = 100 * summary["changes_passed_tests"] / summary["changes_tested"]
        print(
            f"  Precision del coding agent (proxy, tests reales pasados): "
            f"{summary['changes_passed_tests']}/{summary['changes_tested']} ({precision_proxy:.0f}%)"
        )
    else:
        print("  Precision del coding agent (proxy): sin cambios testeados todavia")

    if judge_summary["total_verdicts"]:
        print(f"\n  Veredictos del juez:         {judge_summary['total_verdicts']}")
        print(f"  Marcados FLAGGED:            {judge_summary['flagged']}")
        print(f"  Latencia promedio del juez:  {judge_summary['avg_latency_seconds']:.1f}s")
        print(f"  Costo estimado total:        ${judge_summary['total_cost_usd']:.4f}")
        print(f"  Backend usado:               {judge_summary['backends']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="Fecha ISO (YYYY-MM-DD) desde la que contar, p. ej. inicio del sprint")
    args = parser.parse_args()

    entries = load_entries(args.since)
    judge_entries = load_judge_entries(args.since)

    if not entries:
        print(f"Sin datos en {LOG_PATH}. Corre run_poc_loop.sh al menos una vez.", file=sys.stderr)
        sys.exit(1)

    print_report(summarize(entries), summarize_judge(judge_entries), args.since)


if __name__ == "__main__":
    main()
