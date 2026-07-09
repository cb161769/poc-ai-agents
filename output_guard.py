"""Output guard -- the AI Firewall (firewall_proxy.py) audits the composed
prompt going INTO the coding agent, but nothing used to audit the diff
coming OUT of it. If the coding agent wrote a real secret while "fixing"
something, or a jailbreak-shaped string ended up in a comment, nothing
caught it until Sonar (if it happened to cover that pattern) or the judge
(if it happened to notice).

This closes that gap by running the SAME rules (firewall/policies.yaml, via
firewall_proxy._redact()/_check_jailbreak() -- reused directly, not
reimplemented) against the real diff before it's allowed to reach the
testing agent/judge.

Called the same way as graph_writer.py: reads a single JSON payload from
stdin, prints a JSON result to stdout. Unlike graph_writer.py (which is
best-effort and never blocks), a real finding here is meant to block --
that's the entire point, it's the same firewall applied to the other end
of the pipeline.
"""
import json
import sys

from firewall_proxy import _check_jailbreak, _redact


def scan_diff(diff_text: str, jira_context: dict = None) -> dict:
    jira_context = jira_context or {}
    sanitized, redactions_applied = _redact(diff_text or "")
    jailbreak_reason = _check_jailbreak(diff_text or "", jira_context)
    return {
        "redactions_applied": redactions_applied,
        "jailbreak_reason": jailbreak_reason,
        "clean": redactions_applied == 0 and jailbreak_reason is None,
    }


def main():
    try:
        payload = json.loads(sys.stdin.read())
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": f"invalid_json_payload: {exc}"}), file=sys.stderr)
        sys.exit(1)

    result = scan_diff(payload.get("diff_text", ""), payload.get("jira_context", {}))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
