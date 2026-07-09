"""Writes real pipeline runs into the Neo4j knowledge graph -- extends the
existing dependency graph (:Service + DEPENDS_ON, seeded by seed/seed.cypher
or synced from Azure DevOps) with real execution evidence: which ticket a
run was for, what it touched, what each stage (firewall/tests/judge)
decided, and which documented risk (evals/JUDGE_POLICY.md policy_reference)
it triggered, if any.

Called the same way jira_client.py/sonar_client.py are: reads a single JSON
payload from stdin, prints a JSON result to stdout. run_poc_loop.sh calls it
as a subprocess; orchestration.py imports record_run() directly (it's
already Python).

Deliberately does NOT store large/sensitive content (full diffs, raw ticket
descriptions, full judge reasoning) in the graph -- that already lives in
logs/judge_verdicts.jsonl, logs/coding_agent_runs.jsonl, and the real git
branch (Run.branch). Only short structured fields go in, and any free text
that IS written (ticket summary, decision reason) is redacted with the same
firewall_proxy._redact() used for judge_verdicts.jsonl, so the graph never
becomes a second, unredacted copy of anything the firewall already guards.

Best-effort like the judge or the Falco correlation: if Neo4j isn't
reachable, this logs a warning and returns {"recorded": false, "error":
...} with exit 0 -- it never blocks the pipeline.
"""
import json
import os
import sys

from dotenv import load_dotenv
from neo4j import GraphDatabase

from firewall_proxy import _redact
from log_utils import get_logger

load_dotenv()

logger = get_logger(__name__)

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "test_password_local")

_VALID_STAGES = {"firewall", "tests", "judge"}

# The root ticket node is either a :Story (normal ticket run) or an :Epic
# (--epic mode: the ticket BEING delivered IS the epic itself, combining all
# its children into one prompt -- see run_epic_etapas()/run_epic_pipeline()).
# It would be wrong to model that as "Story PART_OF Epic" with the same key
# pointing at itself, so the root label is picked in Python (from a fixed
# 2-value allowlist, never from request text) and baked into the query
# template -- Cypher can't parametrize a label directly in MERGE.
_ROOT_LABEL_BY_MODE = {True: "Epic", False: "Story"}

_RECORD_RUN_QUERY_TEMPLATE = """
MERGE (root:{root_label} {{key: $ticket_key}})
  ON CREATE SET root.summary = $ticket_summary
  ON MATCH SET root.summary = $ticket_summary

WITH root
UNWIND $child_ticket_keys AS child_key
  MERGE (child:Story {{key: child_key}})
  MERGE (child)-[:PART_OF]->(root)

WITH root
UNWIND $components AS component_name
  MERGE (svc:Service {{name: component_name}})
  MERGE (root)-[:AFFECTS]->(svc)

WITH root
MERGE (run:Run {{run_id: $run_id}})
  ON CREATE SET run.ts = $ts, run.branch = $branch, run.backend = $backend
MERGE (run)-[:FOR_TICKET]->(root)

WITH run
UNWIND $components AS component_name
  MATCH (svc:Service {{name: component_name}})
  MERGE (run)-[:TOUCHED]->(svc)

WITH run
UNWIND $decisions AS d
  MERGE (dec:Decision {{run_id: $run_id, stage: d.stage}})
  ON CREATE SET dec.status = d.status, dec.reason = d.reason, dec.policy_reference = d.policy_reference
  MERGE (run)-[:HAS_DECISION]->(dec)

WITH run
UNWIND [d IN $decisions WHERE d.policy_reference IS NOT NULL] AS flagged
  MERGE (risk:Risk {{policy_reference: flagged.policy_reference}})
  MERGE (risk)-[:IDENTIFIED_IN]->(run)
  WITH risk, run
  UNWIND $components AS component_name
    MATCH (svc:Service {{name: component_name}})
    MERGE (risk)-[:AFFECTS]->(svc)
"""


def _redact_text(text):
    if not text:
        return text
    sanitized, _count = _redact(text)
    return sanitized


def _build_write_params(payload: dict) -> dict:
    """Pure: validates/normalizes the payload and redacts free-text fields,
    without touching Neo4j -- so this half of the logic is unit-testable
    without a real database, same split firewall_proxy.py already has
    between _check_jailbreak/_redact (pure) and the FastAPI endpoint (I/O).
    """
    run_id = payload["run_id"]
    ticket_key = payload["ticket_key"]

    decisions = []
    for d in payload.get("decisions", []):
        stage = d.get("stage")
        if stage not in _VALID_STAGES:
            raise ValueError(f"decision stage invalido: {stage!r} (valores validos: {sorted(_VALID_STAGES)})")
        decisions.append(
            {
                "stage": stage,
                "status": d.get("status"),
                "reason": _redact_text(d.get("reason")),
                "policy_reference": d.get("policy_reference"),
            }
        )

    return {
        "run_id": run_id,
        "ticket_key": ticket_key,
        "ticket_summary": _redact_text(payload.get("ticket_summary", "")),
        "is_epic": bool(payload.get("is_epic", False)),
        "child_ticket_keys": payload.get("child_ticket_keys") or [],
        "components": payload.get("components") or [],
        "branch": payload.get("branch"),
        "backend": payload.get("backend"),
        "ts": payload.get("ts") or "",
        "decisions": decisions,
    }


def record_run(payload: dict) -> dict:
    params = _build_write_params(payload)
    is_epic = params.pop("is_epic")
    query = _RECORD_RUN_QUERY_TEMPLATE.format(root_label=_ROOT_LABEL_BY_MODE[is_epic])

    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
        try:
            with driver.session() as session:
                session.execute_write(lambda tx: tx.run(query, **params))
        finally:
            driver.close()
    except Exception as exc:
        logger.warning(f"no se pudo registrar la corrida '{params['run_id']}' en el grafo: {exc}")
        return {"recorded": False, "error": str(exc)}

    return {"recorded": True, "run_id": params["run_id"]}


def main():
    try:
        payload = json.loads(sys.stdin.read())
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": f"invalid_json_payload: {exc}"}), file=sys.stderr)
        sys.exit(1)

    try:
        result = record_run(payload)
    except (KeyError, ValueError) as exc:
        print(json.dumps({"error": f"invalid_payload: {exc}"}), file=sys.stderr)
        sys.exit(1)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
