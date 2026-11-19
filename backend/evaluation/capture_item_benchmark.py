"""Synthetic live check of how text capture names the item group of new wording (learn-accounts-by-item D3).

Each case is a typed capture whose item group the code cannot know without the model, with the owner's groups.
The model's answer goes through the production check (`validated_item_group`). The gate fails when a capture
joins another existing item (its account would be learned from the wrong payments); a dropped answer only falls
back to the first word. Only synthetic text and group words are sent.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass

import httpx2 as httpx

from evaluation.capture_tag_benchmark import ACCOUNTS, CATEGORIES, SENT_AT
from evaluation.notification_benchmark import provider_credential, usage_cost
from app.services.capture_normalization import (
    InvalidCaptureNormalization, validate_capture_normalization, validated_item_group,
)
from app.services.openai_notification_provider import OpenAINotificationProvider, ProviderError
from app.services.prompt_language import CAPTURE_NORMALIZATION, INDONESIAN_VERSIONS, load_prompt

# The owner's groups with their recorded captures, most used first, as `owner_item_groups` sends them.
ITEM_GROUPS = [
    {"group": "parkir", "count": 14}, {"group": "makan", "count": 10}, {"group": "bensin", "count": 6},
    {"group": "kopi", "count": 4}, {"group": "pulsa", "count": 2}, {"group": "potong rambut", "count": 2},
]
EXISTING = {row["group"] for row in ITEM_GROUPS}


@dataclass(frozen=True)
class ItemCase:
    name: str
    text: str
    amount: int
    # Groups that are right for this text; a group outside this set and inside EXISTING joins the wrong item.
    accepted: frozenset[str]


CASES = [
    ItemCase("parking-new-word", "parkiran mall 5rb", 5_000, frozenset({"parkir"})),
    ItemCase("parking-english", "bayar parking basement 10rb", 10_000, frozenset({"parkir"})),
    ItemCase("fuel-brand", "isi pertalite 30rb", 30_000, frozenset({"bensin"})),
    ItemCase("fuel-station", "spbu pertamina 50rb", 50_000, frozenset({"bensin"})),
    ItemCase("haircut-synonym", "cukur rambut 50rb", 50_000, frozenset({"potong rambut"})),
    ItemCase("haircut-barber", "barbershop 60rb", 60_000, frozenset({"potong rambut"})),
    ItemCase("coffee-drink", "es kopi susu 25rb", 25_000, frozenset({"kopi"})),
    ItemCase("coffee-brand", "janji jiwa 28rb", 28_000, frozenset({"kopi"})),
    ItemCase("phone-credit", "isi kuota telkomsel 25rb", 25_000, frozenset({"pulsa"})),
    ItemCase("meal-dish", "nasi goreng 20rb", 20_000, frozenset({"makan", "nasi goreng"})),
    ItemCase("time-before-item", "kemarin 20.45 parkiran ptc 10rb", 10_000, frozenset({"parkir"})),
    ItemCase("new-umbrella", "beli payung 80rb", 80_000, frozenset({"payung"})),
    ItemCase("new-service", "servis motor 150rb", 150_000, frozenset({"servis motor", "servis"})),
    ItemCase("new-cinema", "tiket bioskop 50rb", 50_000, frozenset({"tiket bioskop", "bioskop", "tiket"})),
    ItemCase("tip-not-haircut", "tip tukang parkir 2rb", 2_000, frozenset({"parkir", "tip"})),
]


def case_context(case: ItemCase) -> dict:
    return {
        "text": case.text,
        "facts": {"kind": None, "kind_guess": "expense", "amount": case.amount, "local_date": SENT_AT[:10],
                  "sent_at": SENT_AT, "sent_weekday": "Monday", "time_phrase": None, "time_day": None,
                  "accounts": []},
        "categories": [dict(row) for row in CATEGORIES],
        "accounts": [dict(row) for row in ACCOUNTS],
        "memory": [],
        "similar_history": [],
        "item_groups": [dict(row) for row in ITEM_GROUPS],
        "tags": [],
    }


def classify(case: ItemCase, group: str | None) -> str:
    if group is None:
        return "fallback_first_word"
    if group in case.accepted:
        return "correct"
    if group in EXISTING:
        return "wrong_existing"
    return "other_new_group"


async def run(model: str, effort: str, repeats: int, language: str) -> dict:
    rows, cost, latencies = [], 0.0, []
    prompt = load_prompt(CAPTURE_NORMALIZATION, language)
    async with httpx.AsyncClient(follow_redirects=False, trust_env=False, limits=httpx.Limits(max_connections=1)) as client:
        provider = OpenAINotificationProvider(client, api_key=provider_credential(), model=model, reasoning_effort=effort)
        for case in CASES:
            context = case_context(case)
            for repeat in range(repeats):
                started = time.perf_counter()
                try:
                    result = await provider.normalize_capture(context, prompt=prompt)
                    validate_capture_normalization(result, context, output_language=language)
                    group = validated_item_group(result.item_group, context)
                    row = {"outcome": classify(case, group), "group": group, "proposed": result.item_group.group}
                except InvalidCaptureNormalization as exc:
                    row = {"outcome": "invalid", "code": str(exc)}
                except ProviderError as exc:
                    row = {"outcome": "provider_error", "code": exc.code}
                latencies.append(time.perf_counter() - started)
                cost += usage_cost(provider.last_usage)
                rows.append({"case": case.name, "repeat": repeat + 1, **row})
                await asyncio.sleep(0.5)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["outcome"]] = counts.get(row["outcome"], 0) + 1
    per_case: dict[str, list[str]] = {}
    for row in rows:
        per_case.setdefault(row["case"], []).append(f"{row['outcome']}:{row.get('group')}")
    latencies.sort()
    return {
        "prompt_version": INDONESIAN_VERSIONS[CAPTURE_NORMALIZATION] if language == "id" else f"{language} rendering",
        "model": model, "reasoning_effort": effort, "language": language, "repeats": repeats, "outcomes": counts,
        "gate_pass": not any(counts.get(key) for key in ("wrong_existing", "invalid", "provider_error")),
        "median_seconds": round(latencies[len(latencies) // 2], 2) if latencies else None,
        "observed_usage_usd": round(cost, 5), "per_case": per_case,
    }


def main():
    parser = argparse.ArgumentParser(description="Synthetic-only capture item-group check (live provider calls)")
    parser.add_argument("--live", action="store_true", help="Explicitly allow synthetic provider requests")
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--reasoning-effort", default="none")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--language", choices=("id", "en"), default="id")
    args = parser.parse_args()
    if not args.live:
        print(json.dumps({"cases": [case.name for case in CASES], "live": False}))
        return
    print(json.dumps(asyncio.run(run(args.model, args.reasoning_effort, args.repeats, args.language)), indent=2))


if __name__ == "__main__":
    main()
