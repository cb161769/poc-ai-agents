"""Structured JSON logging for operational/diagnostic output -- backend
selection, MCP connection failures, startup warnings. Does NOT touch the
audit trails (firewall_audit.jsonl, judge_verdicts.jsonl, etc.) or the
interactive human-confirmation UI in coding_agent.py (diffs/commands shown
before a [s/n] prompt need to stay human-readable, not JSON). This only
replaces plain print(..., file=sys.stderr) diagnostics, so they can be
shipped to a log aggregator (ELK/Loki/CloudWatch) later without a rewrite.
"""
import json
import logging
import sys
import time


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
