"""Shared, pure constants/logic used by BOTH orchestrators (orchestration.py
and run_poc_loop.sh) so they can't drift out of sync silently.

Born from a real bug this session: RETRYABLE_POLICY_REFERENCES existed in
THREE places (judge_agent.py, a Python set duplicated in orchestration.py,
and a bash array duplicated in run_poc_loop.sh) -- the Python duplicate had
a test guarding it against drift, the bash one didn't, until it was added.
This module is the single source of truth judge_agent.py/orchestration.py
import directly; run_poc_loop.sh reads it via the CLI below instead of
hand-maintaining its own copy.

Usage from bash:
    python3 pipeline_shared.py retryable-policy-references
    # prints one value per line
"""
import sys

# Criterios de policy_reference que el juez puede marcar y que el coding
# agent tiene chance real de corregir. Deliberadamente NO incluye
# data-leak-evidence/jailbreak-evidence/firewall-false-negative/other: esos
# son de seguridad o ambiguos, nunca se reintentan automaticamente.
RETRYABLE_POLICY_REFERENCES = {"scope-mismatch", "insufficient-test-coverage", "graph-impact-unverified"}


def main():
    if len(sys.argv) != 2 or sys.argv[1] != "retryable-policy-references":
        print("usage: pipeline_shared.py retryable-policy-references", file=sys.stderr)
        sys.exit(1)
    for ref in sorted(RETRYABLE_POLICY_REFERENCES):
        print(ref)


if __name__ == "__main__":
    main()
