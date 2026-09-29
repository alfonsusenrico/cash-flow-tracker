"""Non-mutating evaluation using repository-owned fictional cases only."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import subprocess
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

import httpx2 as httpx

from evaluation.notification_cases import FIXTURE_VERSION, SyntheticCase, synthetic_cases
from app.services.notification_context import sanitize_text
from app.services.notification_evidence import EvidenceError, collect_facts
from app.services.notification_interpretation import Interpretation, PROMPT_VERSION, validate_interpretation, system_prompt
from app.services.notification_resolution import (
    endpoint_context, institution_parent, observed_endpoint, pocket_endpoint, signatures_compatible, transfer_signature,
)
from app.services.openai_notification_provider import (
    OPERATIONAL_MODELS,
    PERMITTED_REASONING_EFFORTS,
    OpenAINotificationProvider,
    ProviderError,
)


WEIGHTS = {"schema": 10, "facts": 25, "references": 20, "classification": 20, "movement": 15, "abstention": 10}
MODEL_ID = "gpt-5.6-luna"
REASONING_ORDER = ["low", "none"]
MAX_OUTPUT_TOKENS = 2048
DEFAULT_MAX_ESTIMATED_USD = 0.25
INPUT_USD_PER_MILLION_TOKENS = 0.20
OUTPUT_USD_PER_MILLION_TOKENS = 1.20


class BenchmarkBudgetExceeded(ValueError):
    def __init__(self, estimate_usd: float, cap_usd: float):
        self.estimate_usd = estimate_usd
        self.cap_usd = cap_usd
        super().__init__("benchmark_budget_exceeded")


def benchmark_input(case: SyntheticCase) -> tuple:
    if not isinstance(case, SyntheticCase) or case.context.get("synthetic") is not True:
        raise ValueError("Only repository-owned synthetic cases are accepted")
    facts = collect_facts(case.event)
    context = deepcopy(case.context)
    context["notification"] = sanitize_text(facts.text, "Mira Langit")
    context["self_identity_detected"] = "Mira Langit" in facts.text
    context["facts"] = {"institution": facts.institution, "direction": facts.direction,
                        "amount_quotes": facts.amount_quotes, "timestamp": facts.timestamp.isoformat(),
                        **endpoint_context(facts, context["accounts"])}
    return facts, context


def evaluation_route(case: SyntheticCase) -> str:
    facts, _ = benchmark_input(case)
    if facts.status == "ignored":
        return "entrance_filter"
    if facts.status != "candidate":
        return "evidence_review"
    if facts.institution == "stockbit":
        return "deterministic_stockbit"
    return "model"


def deterministic_route_row(case: SyntheticCase) -> dict:
    facts, _ = benchmark_input(case)
    route = evaluation_route(case)
    expected = case.expected
    if route == "entrance_filter":
        outcome = "ignored"
        passed = expected["facts_status"] == "ignored" and expected["outcome"] == outcome
    elif route == "evidence_review":
        outcome = "needs_review"
        passed = expected["facts_status"] == "needs_review" and expected["outcome"] == outcome
    else:
        outcome = "deterministic_stockbit"
        passed = facts.institution == "stockbit" and expected["facts_status"] == "candidate"
    return {
        "case": case.name,
        "partition": case.partition,
        "route": route,
        "outcome": outcome,
        "passed": passed,
        "request_attempted": False,
    }


def deterministic_route_summary(rows: list[dict]) -> dict:
    by_route = {}
    for route in ("entrance_filter", "evidence_review", "deterministic_stockbit"):
        route_rows = [row for row in rows if row["route"] == route]
        by_route[route] = {
            "offered": len(route_rows),
            "passed": sum(row["passed"] for row in route_rows),
        }
    return {
        "offered": len(rows),
        "passed": sum(row["passed"] for row in rows),
        "gate_pass": all(row["passed"] for row in rows),
        "by_route": by_route,
        "cases": rows,
    }


def request_cost_upper_bound(context: dict) -> float:
    schema = Interpretation.model_json_schema()
    input_bytes = sum(
        len(value.encode("utf-8"))
        for value in (
            system_prompt(),
            json.dumps(context, ensure_ascii=False, separators=(",", ":"), default=str),
            json.dumps(schema, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        )
    )
    return (
        input_bytes * INPUT_USD_PER_MILLION_TOKENS
        + MAX_OUTPUT_TOKENS * OUTPUT_USD_PER_MILLION_TOKENS
    ) / 1_000_000


def estimated_run_cost(cases: list[SyntheticCase], *, repeats: int, configurations: int) -> float:
    if repeats < 1 or configurations < 1:
        raise ValueError("Benchmark repeats and configurations must be positive")
    return sum(
        request_cost_upper_bound(benchmark_input(case)[1]) * repeats * configurations
        for case in cases
        if evaluation_route(case) == "model"
    )


def enforce_cost_cap(estimate_usd: float, cap_usd: float) -> None:
    if estimate_usd > cap_usd:
        raise BenchmarkBudgetExceeded(estimate_usd, cap_usd)


def usage_cost(usage: dict | None) -> float:
    usage = usage or {}
    return (
        int(usage.get("input_tokens", 0)) * INPUT_USD_PER_MILLION_TOKENS
        + int(usage.get("output_tokens", 0)) * OUTPUT_USD_PER_MILLION_TOKENS
    ) / 1_000_000


def trusted_decision(case: SyntheticCase, interpretation) -> dict:
    facts, context = benchmark_input(case)
    if facts.status == "ignored":
        return {"outcome": "ignored"}
    if facts.status != "candidate" or facts.institution == "stockbit":
        return {"outcome": "needs_review"}
    validate_interpretation(interpretation, facts, context)
    if interpretation.outcome != "record":
        return {"outcome": interpretation.outcome}
    decision = interpretation.model_dump(mode="json")
    decision["amount"] = facts.amount
    decision["timestamp"] = facts.timestamp.isoformat()
    if interpretation.direction == "internal_movement":
        parent = institution_parent(context["accounts"], "jago")
        source = pocket_endpoint(facts.parsed.source_pocket, parent, context["accounts"])
        target = pocket_endpoint(facts.parsed.target_pocket, parent, context["accounts"])
        if str(source["id"]) != str(interpretation.source_account_id) or str(target["id"]) != str(interpretation.target_account_id):
            raise EvidenceError("conflicting_movement_endpoints")
        return decision
    observed = observed_endpoint(facts, context["accounts"], interpretation)
    if str(observed["id"]) != str(interpretation.account_id):
        raise EvidenceError("conflicting_observed_account")
    decision["source_account_id"] = None
    decision["target_account_id"] = None
    signature = transfer_signature(facts, context, str(observed["id"]))
    candidates = {
        str(row["id"]): row for row in context.get("candidates", [])
        if row["amount"] == facts.amount and abs((row["date"] - facts.timestamp).total_seconds()) < 30
        and signatures_compatible(signature, row.get("evidence") or {})
    }
    decision["candidate_transaction_id"] = next(iter(candidates)) if len(candidates) == 1 else None
    return decision


def score_case(case: SyntheticCase, interpretation, *, latency: float, error_code: str | None = None) -> dict:
    if evaluation_route(case) != "model":
        raise ValueError("Only model-eligible cases can be scored as model output")
    expected = case.expected
    raw = interpretation.model_dump(mode="json") if interpretation else {}
    rejected = False
    rejection_code = None
    try:
        decision = trusted_decision(case, interpretation) if interpretation else {"outcome": "failed"}
    except EvidenceError as exc:
        decision = {"outcome": "needs_review"}
        rejected = True
        rejection_code = exc.code
    expected_record = expected["outcome"] == "record"
    actual_record = raw.get("outcome") == "record"
    facts, _ = benchmark_input(case)
    raw_fact_valid = (
        facts.status == "candidate" and raw.get("direction") == expected.get("direction")
        and raw.get("amount_evidence") in facts.amount_quotes and facts.amount == expected.get("amount")
    ) if actual_record else bool(raw) and raw.get("outcome") == expected["outcome"]
    refs = ("source_account_id", "target_account_id") if expected.get("direction") == "internal_movement" else ("account_id",)
    reference_correct = all(raw.get(field) == expected.get(field) for field in refs) if expected_record else not actual_record and bool(raw)
    category_correct = (
        raw.get("category_id") == expected.get("category_id") and raw.get("kakeibo") == expected.get("kakeibo")
        and actual_record
    ) if expected_record else raw.get("outcome") == expected["outcome"]
    movement_correct = raw.get("candidate_transaction_id") == expected.get("candidate_transaction_id") and raw.get("outcome") == expected["outcome"]
    unsafe_write = False
    if decision.get("outcome") == "record":
        unsafe_write = (
            not expected_record or decision.get("amount") != expected["amount"]
            or any(decision.get(field) != expected.get(field) for field in refs)
            or decision.get("candidate_transaction_id") != expected.get("candidate_transaction_id")
        )
    description = raw.get("description", "")
    description_quality = bool(actual_record and 3 <= len(description.strip()) <= 160
                               and not re.search(r"\[SELF\]|\[REDACTED\]|ignore previous|\b\d{6,}\b", description, re.I))
    return {
        "case": case.name, "partition": case.partition, "latency_seconds": latency,
        "error_code": error_code, "schema": bool(interpretation), "facts": raw_fact_valid,
        "references": reference_correct, "classification": category_correct,
        "movement": movement_correct, "abstention": raw.get("outcome") == expected["outcome"],
        "expected_record": expected_record, "proposed_record": actual_record,
        "validated_outcome": decision["outcome"], "validation_rejected": rejected,
        "validation_rejection_code": rejection_code,
        "unsafe_validated_write": unsafe_write,
        "unambiguous_classification": bool(expected.get("classification_unambiguous")),
        "expected_movement": bool(expected.get("candidate_transaction_id") or expected.get("direction") == "internal_movement" and expected_record),
        "proposed_movement": bool(raw.get("candidate_transaction_id") or raw.get("direction") == "internal_movement" and actual_record),
        "description_rubric_pass": description_quality,
        "synthetic_description": sanitize_text(description)[:160] if actual_record else None,
        "raw_proposal": raw or None,
        "trusted_decision": decision,
    }


def summarize(rows: list[dict]) -> dict:
    metrics = {name: 100 * mean(int(row[name]) for row in rows) if rows else 0 for name in WEIGHTS}
    latencies = sorted(row["latency_seconds"] for row in rows)

    def percentile(fraction):
        return latencies[min(len(latencies) - 1, int((len(latencies) - 1) * fraction))] if latencies else None

    classified = [row for row in rows if row["unambiguous_classification"]]
    classification_accuracy = 100 * mean(int(row["classification"]) for row in classified) if classified else 0
    positives = [row for row in rows if row["expected_record"]]
    movement_expected = [row for row in rows if row["expected_movement"]]
    movement_proposed = [row for row in rows if row["proposed_movement"]]
    safe = not any(row["unsafe_validated_write"] for row in rows)
    quality = bool(rows and positives and metrics["schema"] >= 98 and classification_accuracy >= 90)
    return {
        "offered": len(rows), "completed": sum(row["schema"] for row in rows),
        "failed_requests": sum(bool(row["error_code"]) for row in rows),
        "raw_metrics_percent": metrics, "composite_score": sum(metrics[name] * WEIGHTS[name] / 100 for name in WEIGHTS),
        "validated_safety_gate_pass": safe, "quality_gate_pass": quality,
        "eligible_by_evaluation": safe and quality,
        "unambiguous_classification_percent": classification_accuracy,
        "validation_rejections": sum(row["validation_rejected"] for row in rows),
        "record_recall_percent": 100 * mean(int(row["proposed_record"]) for row in positives) if positives else 0,
        "movement_precision_percent": 100 * mean(int(row["movement"]) for row in movement_proposed) if movement_proposed else 0,
        "movement_recall_percent": 100 * mean(int(row["movement"]) for row in movement_expected) if movement_expected else 0,
        "latency_seconds": {"p50": percentile(0.5), "p95": percentile(0.95)},
        "token_usage": {
            name: sum((row.get("usage") or {}).get(name, 0) for row in rows)
            for name in ("input_tokens", "output_tokens", "total_tokens")
        },
        "observed_usage_usd": sum(usage_cost(row.get("usage")) for row in rows),
        "cases": rows,
    }


async def evaluate_model(provider, cases: list[SyntheticCase], *, repeats: int = 3, interval: float = 0.5) -> dict:
    if repeats < 1:
        raise ValueError("At least one repeat required")
    model_cases = [case for case in cases if evaluation_route(case) == "model"]
    deterministic_rows = [deterministic_route_row(case) for case in cases if evaluation_route(case) != "model"]
    rows = []
    terminal_error = None
    for case in model_cases:
        _, context = benchmark_input(case)
        for repeat in range(repeats):
            started = time.perf_counter()
            interpretation = None
            error = terminal_error
            attempted = terminal_error is None
            if attempted:
                provider.last_usage = None
                try:
                    interpretation = await provider.interpret(context)
                except ProviderError as exc:
                    error = exc.code
                    if exc.code in {
                        "provider_access_denied", "provider_credential_missing", "provider_model_not_permitted",
                        "provider_timeout", "provider_unavailable",
                    }:
                        terminal_error = error
            row = score_case(case, interpretation, latency=time.perf_counter() - started, error_code=error)
            usage = getattr(provider, "last_usage", None) if attempted else None
            row["usage"] = usage if isinstance(usage, dict) else None
            row["repeat"] = repeat + 1
            row["request_attempted"] = attempted
            rows.append(row)
            if interval and attempted:
                await asyncio.sleep(interval)
    deterministic_routes = deterministic_route_summary(deterministic_rows)
    summary = summarize(rows)
    summary["deterministic_routes"] = deterministic_routes
    summary["eligible_by_evaluation"] = summary["eligible_by_evaluation"] and deterministic_routes["gate_pass"]
    return {"repeats": repeats, "unavailable_reason": terminal_error,
            "requests_attempted": sum(row["request_attempted"] for row in rows), **summary}


def selected_reasoning_efforts(efforts: list[str]) -> list[str]:
    if not efforts or any(effort not in PERMITTED_REASONING_EFFORTS for effort in efforts):
        raise ValueError("Only permitted OpenAI reasoning efforts can be evaluated")
    return [effort for effort in REASONING_ORDER if effort in efforts]


def revision_identity() -> dict:
    source_root = Path(__file__).resolve().parents[1]
    repo_root = source_root.parent
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo_root, capture_output=True,
            text=True, check=True,
        ).stdout.strip()
        changes = subprocess.run(
            ["git", "status", "--porcelain"], cwd=repo_root, capture_output=True,
            text=True, check=True,
        ).stdout
        dirty: bool | None = bool(changes)
    except (FileNotFoundError, subprocess.CalledProcessError):
        revision = os.getenv("APP_REVISION", "unavailable").strip() or "unavailable"
        dirty = None
    implementation = hashlib.sha256()
    for directory in (source_root / "app" / "services", source_root / "evaluation"):
        for path in sorted(directory.rglob("*")):
            if path.is_file() and path.suffix in {".py", ".md"}:
                implementation.update(str(path).encode())
                implementation.update(path.read_bytes())
    migration = repo_root / "db" / "migrations" / "V20__notification_processing.sql"
    if migration.is_file():
        implementation.update(migration.read_bytes())
    return {"revision": revision, "dirty": dirty, "implementation_hash": implementation.hexdigest()}


async def run_live(reasoning_efforts: list[str], *, repeats: int, partition: str,
                   model: str = MODEL_ID,
                   max_estimated_usd: float = DEFAULT_MAX_ESTIMATED_USD):
    reasoning_efforts = selected_reasoning_efforts(reasoning_efforts)
    if max_estimated_usd <= 0:
        raise ValueError("Benchmark cost cap must be positive")
    credential = os.getenv("OPENAI_API_KEY", "").strip()
    if not credential:
        env_file = Path(__file__).resolve().parents[2] / ".env"
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("OPENAI_API_KEY=") and not line.startswith("#"):
                    credential = line.split("=", 1)[1].strip().strip("'\"")
                    break
    if not credential:
        raise ProviderError("provider_credential_missing")
    cases = [case for case in synthetic_cases() if partition == "all" or case.partition == partition]
    estimated_usd = estimated_run_cost(cases, repeats=repeats, configurations=len(reasoning_efforts))
    enforce_cost_cap(estimated_usd, max_estimated_usd)
    report = {
        "synthetic_only": True, "timestamp": datetime.now(timezone.utc).isoformat(),
        "prompt_version": PROMPT_VERSION, "prompt_hash": hashlib.sha256(system_prompt().encode()).hexdigest(),
        "fixture_version": FIXTURE_VERSION,
        "fixture_hash": hashlib.sha256(json.dumps([{"event": case.event, "context": case.context, "expected": case.expected} for case in cases], default=str, sort_keys=True).encode()).hexdigest(),
        "partition": partition, **revision_identity(), "model": model, "configurations": {},
        "settings": {"max_output_tokens": MAX_OUTPUT_TOKENS, "store": False, "tools": [], "interval_seconds": 0.5},
        "cost": {
            "pricing_usd_per_million_tokens": {
                "input": INPUT_USD_PER_MILLION_TOKENS,
                "output": OUTPUT_USD_PER_MILLION_TOKENS,
            },
            "max_estimated_usd": max_estimated_usd,
            "estimated_upper_bound_usd": estimated_usd,
        },
    }
    async with httpx.AsyncClient(follow_redirects=False, trust_env=False, limits=httpx.Limits(max_connections=1)) as client:
        for reasoning_effort in reasoning_efforts:
            provider = OpenAINotificationProvider(
                client,
                api_key=credential,
                model=model,
                reasoning_effort=reasoning_effort,
            )
            result = await evaluate_model(provider, cases, repeats=repeats)
            result["reasoning_effort"] = reasoning_effort
            result["operational_model_permitted"] = model in OPERATIONAL_MODELS
            result["activation_recommended"] = False
            report["configurations"][reasoning_effort] = result
    return report


def main():
    parser = argparse.ArgumentParser(description="Synthetic-only notification benchmark; never reads or writes runtime records")
    parser.add_argument("--live", action="store_true", help="Explicitly allow synthetic provider requests using OPENAI_API_KEY")
    parser.add_argument("--model", default=MODEL_ID, choices=sorted(OPERATIONAL_MODELS), help="Target model for synthetic benchmark")
    parser.add_argument("--reasoning-efforts", nargs="+", default=["low"], choices=REASONING_ORDER)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--partition", choices=("all", "development", "held_out"), default="all")
    parser.add_argument("--max-estimated-usd", type=float, default=DEFAULT_MAX_ESTIMATED_USD)
    arguments = parser.parse_args()
    if not arguments.live:
        parser.error("Use --live only after rechecking OpenAI project access, spend limits, and retention controls")
    if arguments.repeats < 3:
        parser.error("Live comparison requires at least three repeats per case")
    try:
        report = asyncio.run(run_live(
            arguments.reasoning_efforts,
            repeats=arguments.repeats,
            partition=arguments.partition,
            model=arguments.model,
            max_estimated_usd=arguments.max_estimated_usd,
        ))
    except (ProviderError, BenchmarkBudgetExceeded, ValueError) as exc:
        error_code = exc.code if isinstance(exc, ProviderError) else str(exc)
        report = {"status": "not_run", "error_code": error_code}
        if isinstance(exc, BenchmarkBudgetExceeded):
            report["estimated_upper_bound_usd"] = exc.estimate_usd
            report["max_estimated_usd"] = exc.cap_usd
        print(json.dumps(report))
        return 1
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
