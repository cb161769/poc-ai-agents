"""Aggregates logs/copilot_contribution.jsonl into a sprint-level summary:
how many tickets Copilot actually touched, how often the firewall approved
vs. rejected, how often a secret had to be redacted, and how often a
suggestion was actually applied to a review branch.

This is the evidence a VP would want for "how much did Copilot collaborate
this sprint" — not just whether the PoC ran.

Usage: python3 scripts/report_sprint_metrics.py [--since 2026-07-01]
"""
import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "copilot_contribution.jsonl"


def load_entries(since: str | None) -> list[dict]:
    if not LOG_PATH.exists():
        return []

    entries = []
    since_dt = datetime.fromisoformat(since).replace(tzinfo=timezone.utc) if since else None

    for line in LOG_PATH.read_text(encoding="utf-8").splitlines():
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


def summarize(entries: list[dict]) -> dict:
    total = len(entries)
    tickets = {e["ticket_id"] for e in entries}
    status_counts = Counter(e["firewall_status"] for e in entries)
    suggested = sum(1 for e in entries if e.get("copilot_suggested"))
    applied = sum(1 for e in entries if e.get("copilot_applied"))
    redacted_runs = sum(1 for e in entries if e.get("redactions_applied", 0) > 0)
    total_redactions = sum(e.get("redactions_applied", 0) for e in entries)

    return {
        "total_runs": total,
        "unique_tickets": len(tickets),
        "approved": status_counts.get("APPROVED", 0),
        "rejected": status_counts.get("REJECTED", 0),
        "copilot_suggested": suggested,
        "copilot_applied": applied,
        "runs_with_redaction": redacted_runs,
        "total_redactions": total_redactions,
    }


def print_report(summary: dict, since: str | None):
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="Fecha ISO (YYYY-MM-DD) desde la que contar, p. ej. inicio del sprint")
    args = parser.parse_args()

    entries = load_entries(args.since)
    if not entries:
        print(f"Sin datos en {LOG_PATH}. Corre run_poc_loop.sh al menos una vez.", file=sys.stderr)
        sys.exit(1)

    print_report(summarize(entries), args.since)


if __name__ == "__main__":
    main()
