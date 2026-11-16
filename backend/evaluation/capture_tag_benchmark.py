"""Synthetic live check that text capture reuses the owner's tags and creates one only when the owner names it.

Each case is a typed capture with the owner's tags. The model's answer goes through the same checks as production
(`validate_capture_normalization`); the gate fails on any tag the owner did not mark (an unwanted tag) and on a new
tag created where an existing one was meant (a duplicate). Only synthetic text and tag names are sent.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass, field

import httpx2 as httpx

from evaluation.notification_benchmark import provider_credential, usage_cost
from evaluation.notification_cases import fixture_id
from app.services.capture_normalization import InvalidCaptureNormalization, validate_capture_normalization
from app.services.capture_resolution import has_tag_wording
from app.services.openai_notification_provider import OpenAINotificationProvider, ProviderError
from app.services.prompt_language import CAPTURE_NORMALIZATION, INDONESIAN_VERSIONS, load_prompt

SENT_AT = "2026-11-16T20:05+07:00"

TAGS = [
    {"id": fixture_id(101), "name": "fellowship coach"},
    {"id": fixture_id(102), "name": "Malang trip"},
    {"id": fixture_id(103), "name": "Kantor"},
    {"id": fixture_id(104), "name": "Ulang tahun Rani"},
    {"id": fixture_id(105), "name": "Liburan Bali 2026"},
]
CATEGORIES = [
    {"id": fixture_id(201), "name": "Makanan & Minuman", "kind": "expense", "default_kakeibo": "need"},
    {"id": fixture_id(202), "name": "Transportasi", "kind": "expense", "default_kakeibo": "need"},
    {"id": fixture_id(203), "name": "Hadiah & Donasi", "kind": "expense", "default_kakeibo": "culture"},
    {"id": fixture_id(204), "name": "Belanja Rumah", "kind": "expense", "default_kakeibo": "need"},
    {"id": fixture_id(205), "name": "Liburan", "kind": "expense", "default_kakeibo": "want"},
]
ACCOUNTS = [
    {"id": fixture_id(301), "label": "Tunai", "type": "cash", "aliases": ["cash"]},
    {"id": fixture_id(302), "label": "BCA", "type": "bank", "aliases": []},
]


@dataclass(frozen=True)
class TagCase:
    name: str
    text: str
    amount: int
    # Existing tag IDs and new tag names (compared lower-cased) the owner meant; both empty means no tag.
    existing: frozenset[str] = frozenset()
    new: frozenset[str] = frozenset()
    # Words the transaction name must not repeat, because they only mark the tag.
    tag_words: tuple[str, ...] = field(default_factory=tuple)


CASES = [
    TagCase("prefix-shortening", "fellow coach: bayar makan di Kafe Contoh cash jam 19", 45_000,
            frozenset({fixture_id(101)}), tag_words=("fellow coach",)),
    TagCase("prefix-typo", "felowship coach: kopi susu 28rb", 28_000, frozenset({fixture_id(101)}),
            tag_words=("felowship",)),
    TagCase("prefix-word-order", "trip malang: tiket kereta 350rb", 350_000, frozenset({fixture_id(102)}),
            tag_words=("trip malang",)),
    TagCase("hashtag-existing", "isi bensin 150rb #malangtrip", 150_000, frozenset({fixture_id(102)}),
            tag_words=("malangtrip",)),
    TagCase("instruction-existing", "tiket pesawat 1,2jt tag liburan bali", 1_200_000, frozenset({fixture_id(105)})),
    TagCase("instruction-exact-name", "makan malam 85rb tag Fellowship Coach", 85_000, frozenset({fixture_id(101)}),
            tag_words=("fellowship",)),
    TagCase("two-tags", "fellow coach: makan siang 45rb #kantor", 45_000,
            frozenset({fixture_id(101), fixture_id(103)}), tag_words=("fellow coach", "kantor")),
    TagCase("instruction-new", "isi bensin 150rb tag-nya Reuni SMA", 150_000, new=frozenset({"reuni sma"}),
            tag_words=("reuni",)),
    TagCase("hashtag-new", "kado 200rb #nikahandika", 200_000, new=frozenset({"nikahandika"}),
            tag_words=("nikahandika",)),
    TagCase("none-plain", "es teh manis 5rb tunai", 5_000),
    TagCase("none-item-before-colon", "makan siang: nasi padang 25rb", 25_000),
    TagCase("none-merchant-before-colon", "Indomaret: sabun dan sampo 48rb", 48_000),
    TagCase("none-birthday-not-named-as-tag", "kado ulang tahun buat Rani 150rb", 150_000),
]


def case_context(case: TagCase) -> dict:
    """The model input production builds for this text: tags only when the text may mark one."""
    return {
        "text": case.text,
        "facts": {"kind": None, "kind_guess": "expense", "amount": case.amount, "local_date": SENT_AT[:10],
                  "sent_at": SENT_AT, "sent_weekday": "Monday", "time_phrase": None, "time_day": None,
                  "accounts": []},
        "categories": [dict(row) for row in CATEGORIES],
        "accounts": [dict(row) for row in ACCOUNTS],
        "memory": [],
        "similar_history": [],
        "tags": [dict(row) for row in TAGS] if has_tag_wording(case.text) else [],
    }


def classify(case: TagCase, checked: dict) -> dict:
    existing = {tag["tag_id"] for tag in checked["tags"] if tag["tag_id"]}
    new = {tag["name"].lower() for tag in checked["tags"] if not tag["tag_id"]}
    name = (checked["description"] or "").casefold()
    row = {"existing": sorted(existing), "new": sorted(new), "name": checked["description"],
           "name_repeats_tag": any(word.casefold() in name for word in case.tag_words)}
    if existing == case.existing and new == case.new:
        return {"outcome": "correct", **row}
    if new - case.new and case.existing:
        return {"outcome": "duplicate_tag", **row}
    if (existing - case.existing) or (new - case.new):
        return {"outcome": "unwanted_tag", **row}
    return {"outcome": "missed_tag", **row}


async def run(model: str, effort: str, repeats: int, language: str) -> dict:
    rows, cost = [], 0.0
    prompt = load_prompt(CAPTURE_NORMALIZATION, language)
    async with httpx.AsyncClient(follow_redirects=False, trust_env=False, limits=httpx.Limits(max_connections=1)) as client:
        provider = OpenAINotificationProvider(client, api_key=provider_credential(), model=model, reasoning_effort=effort)
        for case in CASES:
            context = case_context(case)
            for repeat in range(repeats):
                try:
                    result = await provider.normalize_capture(context, prompt=prompt)
                    row = classify(case, validate_capture_normalization(result, context, output_language=language))
                except InvalidCaptureNormalization as exc:
                    row = {"outcome": "invalid", "code": str(exc)}
                except ProviderError as exc:
                    row = {"outcome": "provider_error", "code": exc.code}
                cost += usage_cost(provider.last_usage)
                rows.append({"case": case.name, "repeat": repeat + 1, **row})
                await asyncio.sleep(0.5)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["outcome"]] = counts.get(row["outcome"], 0) + 1
    per_case: dict[str, list[str]] = {}
    for row in rows:
        per_case.setdefault(row["case"], []).append(row["outcome"])
    return {
        "prompt_version": INDONESIAN_VERSIONS[CAPTURE_NORMALIZATION] if language == "id" else f"{language} rendering",
        "model": model, "reasoning_effort": effort, "language": language, "repeats": repeats, "outcomes": counts,
        "name_repeats_tag": sum(1 for row in rows if row.get("name_repeats_tag")),
        "gate_pass": not any(counts.get(key) for key in ("unwanted_tag", "duplicate_tag", "invalid", "provider_error")),
        "observed_usage_usd": round(cost, 5), "per_case": per_case, "rows": rows,
    }


def main():
    parser = argparse.ArgumentParser(description="Synthetic-only capture tag check (live provider calls)")
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
