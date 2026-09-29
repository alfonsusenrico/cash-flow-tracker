"""Synthetic live check that AI account mapping never auto-accepts a wrong account.

Each case names an account the backend cannot resolve. The model's answer goes through the same
validation and threshold decision as production; any mapping at or above the threshold to an
account outside the case's acceptable set fails the gate.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime

import httpx2 as httpx

from evaluation.notification_benchmark import provider_credential, usage_cost
from evaluation.notification_cases import BASE_CONTEXT, fixture_id
from app.services.notification_context import sanitize_text
from app.services.notification_evidence import EvidenceError, collect_facts
from app.services.notification_interpretation import PROMPT_VERSION, validate_interpretation
from app.services.notification_mapping import mapping_decision
from app.services.notification_resolution import alias_key, endpoint_context, normalize_name
from app.services.openai_notification_provider import OpenAINotificationProvider, ProviderError

POST_TIME = datetime.fromisoformat("2026-09-29T17:00:00+07:00")


@dataclass(frozen=True)
class MappingCase:
    name: str
    package: str
    text: str
    extra_accounts: tuple[dict, ...]
    acceptable_auto: frozenset[str]
    # (account id, bank name, confirmed_by) pairs learned from earlier notifications.
    learned: tuple[tuple[str, str, str], ...] = ()


def jago_pocket(number: int, name: str) -> dict:
    return {"id": fixture_id(number), "name": name, "type": "bank", "parent_id": fixture_id(2), "is_savings": False}


CASES = [
    MappingCase("gopay-tabungan-to-gopay", "com.jago.digitalBanking",
                "Rp1.250.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket.",
                (), frozenset({fixture_id(5)})),
    MappingCase("two-plausible-pockets", "com.jago.digitalBanking",
                "Rp300.000 has been moved from your Main Pocket Pocket to your Tabungan Liburan Pocket.",
                (jago_pocket(21, "Liburan Bali"), jago_pocket(22, "Tabungan Rumah")), frozenset()),
    MappingCase("unrelated-pocket-name", "com.jago.digitalBanking",
                "Rp500.000 has been moved from your Main Pocket Pocket to your Investasi Emas Pocket.",
                (), frozenset()),
    MappingCase("renamed-bills-pocket", "com.jago.digitalBanking",
                "Payment of Rp210.000 to Listrik Fiksi using Tagihan Bulanan Pocket was successful.",
                (jago_pocket(23, "Tagihan"),), frozenset({fixture_id(23)})),
    MappingCase("gopay-with-paylater", "com.gopay.wallet", "QRIS Rp32.000 ke Kedai Awan berhasil",
                ({"id": fixture_id(24), "name": "GoPay Later", "type": "ewallet", "parent_id": None},),
                frozenset({fixture_id(5)})),
    MappingCase("similar-to-owner-confirmed", "com.jago.digitalBanking",
                "Rp200.000 has been moved from your Main Pocket Pocket to your Tabungan GoPay Pocket.",
                (), frozenset({fixture_id(5)}), ((fixture_id(5), "GoPay Tabungan", "owner"),)),
    MappingCase("generic-word-only-learned", "com.jago.digitalBanking",
                "Rp300.000 has been moved from your Main Pocket Pocket to your Tabungan Liburan Pocket.",
                (jago_pocket(21, "Liburan Bali"), jago_pocket(22, "Rumah")), frozenset(),
                ((fixture_id(22), "Tabungan Rumah", "owner"),)),
    MappingCase("similar-to-ai-learned", "com.jago.digitalBanking",
                "Rp200.000 has been moved from your Main Pocket Pocket to your Tabungan GoPay Pocket.",
                (), frozenset({fixture_id(5)}), ((fixture_id(5), "GoPay Tabungan", "ai"),)),
]


def case_input(case: MappingCase) -> tuple:
    facts = collect_facts({"package_name": case.package, "body_text": case.text, "post_time": POST_TIME})
    context = deepcopy(BASE_CONTEXT)
    context["accounts"] += [dict(account) for account in case.extra_accounts]
    for account_id, name, confirmed_by in case.learned:
        account = next(row for row in context["accounts"] if row["id"] == account_id)
        account.setdefault("learned_notification_names", []).append({
            "key": alias_key("jago", normalize_name(name)), "name": name, "institution": "jago", "confirmed_by": confirmed_by,
        })
    context["notification"] = sanitize_text(facts.text)
    context["self_identity_detected"] = False
    context["facts"] = {"institution": facts.institution, "direction": facts.direction,
                        "amount_quotes": facts.amount_quotes, "timestamp": facts.timestamp.isoformat(),
                        "external_counterparty": False, "counterparty_is_owner": False,
                        **endpoint_context(facts, context["accounts"])}
    if not context["facts"].get("unresolved_names"):
        raise ValueError(f"case {case.name} has no unresolved name")
    return facts, context


def classify(case: MappingCase, facts, context: dict, proposal, threshold: float) -> dict:
    try:
        validate_interpretation(proposal, facts, context)
        decision = mapping_decision(proposal, context)
    except EvidenceError as exc:
        return {"outcome": "review", "code": exc.code}
    chosen = [entry["account_id"] for entry in decision["entries"]]
    if decision["confidence"] < threshold:
        return {"outcome": "confirm", "confidence": decision["confidence"], "accounts": chosen}
    wrong = any(account not in case.acceptable_auto for account in chosen)
    return {"outcome": "auto_wrong" if wrong else "auto_ok", "confidence": decision["confidence"], "accounts": chosen}


async def run(model: str, effort: str, repeats: int, threshold: float) -> dict:
    rows, cost = [], 0.0
    async with httpx.AsyncClient(follow_redirects=False, trust_env=False, limits=httpx.Limits(max_connections=1)) as client:
        provider = OpenAINotificationProvider(client, api_key=provider_credential(), model=model, reasoning_effort=effort)
        for case in CASES:
            facts, context = case_input(case)
            for repeat in range(repeats):
                try:
                    proposal = await provider.interpret(context)
                    row = classify(case, facts, context, proposal, threshold)
                except ProviderError as exc:
                    row = {"outcome": "provider_error", "code": exc.code}
                cost += usage_cost(provider.last_usage)
                rows.append({"case": case.name, "repeat": repeat + 1, **row})
                await asyncio.sleep(0.5)
    counts = {}
    for row in rows:
        counts[row["outcome"]] = counts.get(row["outcome"], 0) + 1
    per_case = {}
    for row in rows:
        entry = per_case.setdefault(row["case"], {"outcomes": [], "confidences": []})
        entry["outcomes"].append(row["outcome"])
        if row.get("confidence") is not None:
            entry["confidences"].append(row["confidence"])
    return {"prompt_version": PROMPT_VERSION, "model": model, "reasoning_effort": effort, "threshold": threshold,
            "repeats": repeats, "outcomes": counts, "gate_pass": counts.get("auto_wrong", 0) == 0
            and counts.get("provider_error", 0) == 0, "observed_usage_usd": round(cost, 5),
            "per_case": per_case, "rows": rows}


def main():
    parser = argparse.ArgumentParser(description="Synthetic-only AI account-mapping check (live provider calls)")
    parser.add_argument("--live", action="store_true", help="Explicitly allow synthetic provider requests")
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--reasoning-effort", default="none")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--threshold", type=float, default=0.85)
    args = parser.parse_args()
    if not args.live:
        for case in CASES:
            case_input(case)
        print(json.dumps({"cases": [case.name for case in CASES], "live": False}))
        return
    print(json.dumps(asyncio.run(run(args.model, args.reasoning_effort, args.repeats, args.threshold)), indent=2))


if __name__ == "__main__":
    main()
