"""Offline simulation of habit learning on a synthetic eight-week history (learn-habits-to-ask-less).

Deterministic and synthetic only: no database, no model, no production data. Each merchant-less payment
is either recorded from its habit or asked; an asked payment gets the owner's true answer, and a wrong
automatic record is corrected by the owner with probability `correction_rate`.

    PYTHONPATH=backend python3 scripts/verify/habit_simulation.py
"""

from __future__ import annotations

import math
import random
import sys
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "backend"))

from app.services import habits  # noqa: E402

START = datetime(2026, 9, 7, tzinfo=timezone.utc)  # a Monday
WEEKS = 8


@dataclass(frozen=True)
class Payer:
    """A synthetic source of merchant-less payments sharing one bank category and account."""

    name: str
    signature: str
    amount: int
    spread: float
    per_week: int
    weekdays: tuple[int, ...]
    hour: int
    truth: tuple[tuple[str, str, float], ...]
    drift_week: int | None = None
    drift_truth: tuple[tuple[str, str, float], ...] = ()


STEADY = (
    Payer("lunch", "bca|expense|groceries", 45_000, 0.12, 5, (1, 2, 3, 4, 5), 12,
          (("makan", "need", 0.95), ("jajan", "want", 0.05))),
    Payer("weekly-shop", "bca|expense|shopping", 250_000, 0.15, 1, (6,), 10, (("belanja", "need", 1.0),)),
    Payer("small-shop", "bca|expense|shopping", 70_000, 0.25, 2, (1, 3, 5, 6, 7), 19,
          (("jajan", "want", 0.55), ("hiburan", "culture", 0.45))),
    Payer("snack", "bca|expense|groceries", 22_000, 0.15, 3, (1, 2, 3, 4, 5), 16,
          (("jajan", "want", 0.85), ("makan", "need", 0.15))),
)
NOISE_SIGNATURES = ("bca|expense|entertainment", "bca|expense|health", "bca|expense|transportation",
                    "bca|expense|groceries", "bca|expense|shopping")
NOISE_CATEGORIES = (("hiburan", "culture"), ("kesehatan", "need"), ("transport", "need"),
                    ("makan", "need"), ("belanja", "need"), ("jajan", "want"))
NOISE_PER_WEEK = 3
HABITUAL = "lunch"


def drifting() -> tuple[Payer, ...]:
    """The lunch habit changes from week 5: the owner now files lunches as snacks."""
    lunch = replace(STEADY[0], drift_week=5, drift_truth=(("jajan", "want", 1.0),))
    return (lunch, *STEADY[1:])


@dataclass(frozen=True)
class Config:
    name: str
    band: str = "ratio25"
    correction_weight: int = habits.CORRECTION_WEIGHT
    window: int = habits.EVIDENCE_WINDOW
    correction_rate: float = 0.9


@dataclass
class Week:
    payments: int = 0
    asked: int = 0
    automatic: int = 0
    wrong: int = 0
    corrected: int = 0


@dataclass
class Result:
    config: Config
    scenario: str
    weeks: list[Week] = field(default_factory=lambda: [Week() for _ in range(WEEKS)])
    payers: dict[str, Week] = field(default_factory=dict)
    habitual: list[Week] = field(default_factory=lambda: [Week() for _ in range(WEEKS)])

    @property
    def automatic(self) -> int:
        return sum(week.automatic for week in self.weeks)

    @property
    def wrong(self) -> int:
        return sum(week.wrong for week in self.weeks)

    @property
    def payments(self) -> int:
        return sum(week.payments for week in self.weeks)

    @property
    def asked(self) -> int:
        return sum(week.asked for week in self.weeks)


def in_band(band: str, amount: int, labelled: int) -> bool:
    if band == "ratio25":
        return habits.within_band(amount, labelled)
    if band == "ratio50":
        return 2 * labelled <= 3 * amount and 2 * amount <= 3 * labelled
    if band == "log2":
        return math.floor(math.log2(amount)) == math.floor(math.log2(labelled))
    raise ValueError(band)


def pick(rng: random.Random, options: tuple[tuple[str, str, float], ...]) -> tuple[str, str]:
    roll, total = rng.random(), 0.0
    for category, pillar, share in options:
        total += share
        if roll < total:
            return category, pillar
    return options[-1][0], options[-1][1]


def payments(rng: random.Random, payers: tuple[Payer, ...]) -> list[dict]:
    """Every payment of the eight weeks in time order, with the owner's true label."""
    generated = []
    for week in range(WEEKS):
        week_start = START + timedelta(weeks=week)
        for payer in payers:
            days = sorted(rng.sample(payer.weekdays, min(payer.per_week, len(payer.weekdays))))
            while len(days) < payer.per_week:
                days.append(rng.choice(payer.weekdays))
            truth = payer.drift_truth if payer.drift_week is not None and week + 1 >= payer.drift_week else payer.truth
            for day in days:
                moment = week_start + timedelta(days=day - 1, hours=payer.hour, minutes=rng.randrange(60))
                amount = round(payer.amount * math.exp(rng.uniform(-payer.spread, payer.spread)), -2)
                generated.append({"week": week, "at": moment, "payer": payer.name, "signature": payer.signature,
                                  "amount": int(amount), "truth": pick(rng, truth)})
        for _ in range(NOISE_PER_WEEK):
            moment = week_start + timedelta(days=rng.randrange(7), hours=rng.randrange(7, 23))
            category, pillar = rng.choice(NOISE_CATEGORIES)
            generated.append({"week": week, "at": moment, "payer": "noise", "signature": rng.choice(NOISE_SIGNATURES),
                              "amount": int(round(math.exp(rng.uniform(math.log(10_000), math.log(1_000_000))), -2)),
                              "truth": (category, pillar)})
    return sorted(generated, key=lambda item: item["at"])


def simulate(config: Config, scenario: str = "steady", seed: int = 20261031) -> Result:
    rng = random.Random(seed)
    payers = STEADY if scenario == "steady" else drifting()
    owner = random.Random(seed + 1)
    result = Result(config=config, scenario=scenario)
    labels: list[tuple[str, habits.Label]] = []
    weight = config.correction_weight

    class WeightedLabel(habits.Label):
        @property
        def weight(self) -> int:
            return weight if self.kind == "correction" else habits.ANSWER_WEIGHT

    for index, payment in enumerate(payments(rng, payers)):
        week = result.weeks[payment["week"]]
        payer = result.payers.setdefault(payment["payer"], Week())
        week.payments += 1
        payer.payments += 1
        habitual = result.habitual[payment["week"]] if payment["payer"] == HABITUAL else Week()
        habitual.payments += 1
        moment = payment["at"]
        evidence = [label for signature, label in labels
                    if signature == payment["signature"] and in_band(config.band, payment["amount"], label.amount)]
        decision = habits.decide(evidence, weekday=moment.isoweekday(), hour=moment.hour, auto_enabled=True,
                                 window=config.window)
        truth = payment["truth"]

        def label(kind: str) -> habits.Label:
            return WeightedLabel(
                transaction_id=f"t{index}", kind=kind, category_id=truth[0], kakeibo=truth[1], notes=None,
                merchant=None, amount=payment["amount"], weekday=moment.isoweekday(), hour=moment.hour,
                created_at=moment,
            )

        if decision and decision.auto:
            week.automatic += 1
            payer.automatic += 1
            if (decision.category_id, decision.kakeibo) != truth:
                week.wrong += 1
                payer.wrong += 1
                if owner.random() < config.correction_rate:
                    week.corrected += 1
                    payer.corrected += 1
                    labels.append((payment["signature"], label("correction")))
        else:
            week.asked += 1
            payer.asked += 1
            habitual.asked += 1
            labels.append((payment["signature"], label("purpose_answer")))
    return result


CONFIGS = (
    Config("chosen: ±25% ratio, correction ×2, latest 10"),
    Config("correction ×1", correction_weight=1),
    Config("no window", window=10_000),
    Config("±50% ratio", band="ratio50"),
    Config("log2 buckets", band="log2"),
    Config("owner corrects every wrong record", correction_rate=1.0),
)


SEEDS = range(20261031, 20261031 + 200)


def percent(part: float, whole: float) -> str:
    return f"{100 * part / whole:.0f}%" if whole else "—"


def averaged(config: Config, scenario: str) -> dict:
    """Question rate per week and wrong automatic records, summed over every seed."""
    runs = [simulate(config, scenario, seed) for seed in SEEDS]
    weeks = [(sum(run.weeks[index].asked for run in runs), sum(run.weeks[index].payments for run in runs))
             for index in range(WEEKS)]
    habitual = [(sum(run.habitual[index].asked for run in runs), sum(run.habitual[index].payments for run in runs))
                for index in range(WEEKS)]
    return {
        "weeks": weeks, "habitual": habitual, "asked": sum(run.asked for run in runs), "payments": sum(run.payments for run in runs),
        "automatic": sum(run.automatic for run in runs), "wrong": sum(run.wrong for run in runs),
    }


def report() -> str:
    lines = ["# Habit learning simulation", "",
             f"Synthetic eight-week history, {len(SEEDS)} seeds from {SEEDS.start}; generated by "
             "`scripts/verify/habit_simulation.py`. Each week has 14 merchant-less payments: a weekday lunch",
             "(95% one category), a weekly shop (100%), an ambiguous small shop (55/45), an afternoon snack",
             "(85/15) and three unrelated payments. The drift scenario changes the lunch category from week 5.",
             "Baseline before this change: every merchant-less payment is asked (100% each week).", ""]
    for scenario in ("steady", "drift"):
        lines += [f"## Scenario: {scenario}", "",
                  "| Configuration | Question rate per week (1→8) | Habitual lunch, per week | Asked | Wrong automatic |",
                  "| --- | --- | --- | --- | --- |"]
        for config in CONFIGS:
            totals = averaged(config, scenario)
            rates = " ".join(percent(asked, payments) for asked, payments in totals["weeks"])
            lunch = " ".join(percent(asked, payments) for asked, payments in totals["habitual"])
            lines.append(
                f"| {config.name} | {rates} | {lunch} | {percent(totals['asked'], totals['payments'])} | "
                f"{percent(totals['wrong'], totals['automatic'])} of automatic, "
                f"{percent(totals['wrong'], totals['payments'])} of payments |"
            )
        chosen = simulate(CONFIGS[0], scenario, SEEDS.start)
        lines += ["", f"Per payer, chosen configuration, seed {SEEDS.start} ({scenario}):", "",
                  "| Payer | Payments | Asked | Automatic | Wrong automatic | Corrected |",
                  "| --- | --- | --- | --- | --- | --- |"]
        for name, totals in chosen.payers.items():
            lines.append(f"| {name} | {totals.payments} | {totals.asked} | {totals.automatic} | {totals.wrong} | "
                         f"{totals.corrected} |")
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    print(report())
