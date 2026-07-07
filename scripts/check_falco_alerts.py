#!/usr/bin/env python3
"""Correlates Falco alerts (logs/falco_alerts.jsonl) with a pipeline run's
time window. Falco (docker-compose service, falco/custom_rules.yaml) watches
poc-* containers for unexpected shells, writes outside expected paths, and
odd outbound connections in real time -- this script is what actually reads
those alerts and surfaces them, instead of leaving them sitting in a file
nobody opens.

This is advisory only: it never decides to block the pipeline, it just
reports what Falco saw during the run's window. The caller (run_poc_loop.sh /
orchestration.py) decides what to do with that (Jira comment, webhook).

Usage: check_falco_alerts.py <since_iso8601_utc> [log_path]
Prints one JSON line to stdout: {"count": N, "alerts": [...]}.
Exit 0 if no matching alerts (or the log doesn't exist yet), 1 if any found.
"""
import json
import os
import re
import sys
from datetime import datetime

# Falco emits nanosecond-precision fractional seconds (9 digits), but
# datetime.fromisoformat() before Python 3.11 only accepts 0, 3, or 6 digit
# fractions -- and this project's Dockerfile.firewall pins python:3.10-slim.
# Truncate/pad to exactly 6 (microseconds) so this parses on any supported
# Python version, not just whatever happens to be on the host running this.
_FRACTIONAL_SECONDS_RE = re.compile(r"\.(\d+)")


def _parse_iso(ts: str) -> datetime:
    ts = ts.replace("Z", "+00:00")

    def _pad_or_truncate(match: "re.Match[str]") -> str:
        digits = match.group(1)[:6].ljust(6, "0")
        return f".{digits}"

    ts = _FRACTIONAL_SECONDS_RE.sub(_pad_or_truncate, ts, count=1)
    return datetime.fromisoformat(ts)


def main():
    if len(sys.argv) < 2:
        print("usage: check_falco_alerts.py <since_iso8601_utc> [log_path]", file=sys.stderr)
        sys.exit(2)

    since = _parse_iso(sys.argv[1])
    log_path = (
        sys.argv[2]
        if len(sys.argv) > 2
        else os.path.join(os.path.dirname(__file__), "..", "logs", "falco_alerts.jsonl")
    )

    alerts = []
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                ts_raw = entry.get("time") or (entry.get("output_fields") or {}).get("evt.time")
                if not ts_raw:
                    continue
                try:
                    ts = _parse_iso(ts_raw)
                except ValueError:
                    continue

                if ts >= since:
                    alerts.append(
                        {
                            "time": ts_raw,
                            "priority": entry.get("priority"),
                            "rule": entry.get("rule"),
                            "output": entry.get("output"),
                        }
                    )

    print(json.dumps({"count": len(alerts), "alerts": alerts}, ensure_ascii=False))
    sys.exit(1 if alerts else 0)


if __name__ == "__main__":
    main()
