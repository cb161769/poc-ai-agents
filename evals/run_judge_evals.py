"""Benchmark for the judge agent's PRECISION — not the coding agent's.

Runs judge_agent.judge_with_tools() against a fixed set of hand-labeled
cases (evals/judge_eval_cases.jsonl, each with a known expected_verdict) and
reports a confusion matrix: does the judge agree with a human's call on
whether a run should be OK or FLAGGED?

Treats FLAGGED as the "positive" class (the thing we want the judge to
catch), so:
  - false negative = judge said OK on something a human would flag (the
    dangerous miss — the judge's whole job is to catch these)
  - false positive = judge said FLAGGED on something a human would approve
    (the annoying-but-safe failure mode)

Also reports latency and estimated cost per case, using the _meta the judge
already attaches to every verdict.

Usage: python3 evals/run_judge_evals.py
Requires the same env as judge_agent.py (ANTHROPIC_API_KEY or a reachable
Ollama, .env at the repo root).
"""
import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import judge_agent  # noqa: E402

CASES_PATH = Path(__file__).resolve().parent / "judge_eval_cases.jsonl"
RUNS_LOG = Path(__file__).resolve().parent.parent / "logs" / "eval_judge_runs.jsonl"


def load_cases() -> list:
    return [json.loads(line) for line in CASES_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]


async def run_case(case: dict) -> dict:
    expected_policy_reference = case.get("expected_policy_reference")
    try:
        verdict = await judge_agent.judge_with_tools(case["payload"])
    except Exception as exc:  # noqa: BLE001 — eval harness, we want to record the failure, not crash
        return {
            "case_id": case["case_id"],
            "expected": case["expected_verdict"],
            "actual": "ERROR",
            "correct": False,
            "expected_policy_reference": expected_policy_reference,
            "actual_policy_reference": None,
            "policy_reference_correct": False,
            "reasoning": str(exc),
            "_meta": {},
        }

    actual = verdict.get("verdict", "ERROR")
    actual_policy_reference = verdict.get("policy_reference")
    return {
        "case_id": case["case_id"],
        "expected": case["expected_verdict"],
        "actual": actual,
        "correct": actual == case["expected_verdict"],
        "expected_policy_reference": expected_policy_reference,
        "actual_policy_reference": actual_policy_reference,
        # Solo tiene sentido comparar policy_reference cuando el veredicto en
        # si tambien salio bien -- si el veredicto ya esta mal, la cita de
        # politica no importa (ni se puede evaluar de forma justa).
        "policy_reference_correct": (actual == case["expected_verdict"]) and (actual_policy_reference == expected_policy_reference),
        "reasoning": verdict.get("reasoning", ""),
        "_meta": verdict.get("_meta", {}),
    }


def confusion_matrix(results: list) -> dict:
    tp = fp = tn = fn = 0
    for r in results:
        expected_positive = r["expected"] == "FLAGGED"
        actual_positive = r["actual"] == "FLAGGED"
        if expected_positive and actual_positive:
            tp += 1
        elif not expected_positive and actual_positive:
            fp += 1
        elif not expected_positive and not actual_positive:
            tn += 1
        elif expected_positive and not actual_positive:
            fn += 1

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    accuracy = (tp + tn) / len(results) if results else 0.0

    return {"tp": tp, "fp": fp, "tn": tn, "fn": fn, "precision": precision, "recall": recall, "accuracy": accuracy}


async def main():
    cases = load_cases()
    print(f"Corriendo {len(cases)} casos de eval contra el agente juez...\n")

    results = []
    for case in cases:
        start = time.monotonic()
        result = await run_case(case)
        elapsed = round(time.monotonic() - start, 2)
        status = "✅" if result["correct"] else "❌"
        print(f"{status} {result['case_id']}: esperado={result['expected']} real={result['actual']} ({elapsed}s)")
        if not result["correct"]:
            print(f"   razonamiento del juez: {result['reasoning']}")
        results.append(result)

    RUNS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with RUNS_LOG.open("a", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    matrix = confusion_matrix(results)
    print("\n== Matriz de confusion (FLAGGED = positivo) ==")
    print(f"  Verdaderos positivos (deberia FLAGGED, dijo FLAGGED): {matrix['tp']}")
    print(f"  Falsos positivos     (deberia OK, dijo FLAGGED):      {matrix['fp']}")
    print(f"  Verdaderos negativos (deberia OK, dijo OK):           {matrix['tn']}")
    print(f"  Falsos negativos     (deberia FLAGGED, dijo OK):      {matrix['fn']}  <- el error grave")
    print(f"  Precision: {matrix['precision']:.0%}  Recall: {matrix['recall']:.0%}  Accuracy: {matrix['accuracy']:.0%}")

    # Accuracy de policy_reference: metrica separada de si el veredicto es
    # correcto -- el juez puede acertar OK/FLAGGED y citar el criterio
    # equivocado de evals/JUDGE_POLICY.md, y eso no lo capturaba nada hasta
    # ahora.
    policy_reference_correct = sum(1 for r in results if r["policy_reference_correct"])
    policy_reference_accuracy = policy_reference_correct / len(results) if results else 0.0
    print(f"\n== Accuracy de policy_reference (independiente del veredicto) ==")
    print(f"  {policy_reference_correct}/{len(results)} correctos ({policy_reference_accuracy:.0%})")
    for r in results:
        if not r["policy_reference_correct"] and r["correct"]:
            print(
                f"   ⚠️ {r['case_id']}: veredicto correcto pero policy_reference esperado="
                f"{r['expected_policy_reference']!r} real={r['actual_policy_reference']!r}"
            )

    total_cost = sum(r["_meta"].get("estimated_cost_usd", 0) for r in results)
    total_latency = sum(r["_meta"].get("latency_seconds", 0) for r in results)
    print(f"\nCosto estimado total: ${total_cost:.4f}  ·  Latencia total: {total_latency:.1f}s")

    if matrix["fn"] > 0:
        sys.exit(1)  # el error grave (dejar pasar algo que deberia bloquearse) hace fallar el eval


if __name__ == "__main__":
    asyncio.run(main())
