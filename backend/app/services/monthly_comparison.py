"""Whole-period comparison for Laporan and Analisis (compare-whole-months).

A completed period is compared with the whole previous period and with the average of the three most
recent completed, non-partial periods before it. A running period gets progress against the previous
whole period and no verdict. Every amount uses the report rules (reportable expenses, reimbursement
offsets, Kakeibo pillars), so each one equals the ledger summary of its window and filters.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.services.kakeibo import UNCLASSIFIED, pillar_bucket_sql
from app.services.observed_insights import merchant_spending, window_figures
from app.services.observed_periods import ObservedPeriod
from app.services.report_figures import UNCATEGORIZED_NAME, reportable_expense_sql

BASELINE_PERIODS = 3
MIN_BASELINE_PERIODS = 2
HISTORY_COMPLETED = 4
FACT_LIMIT = 7
ABOVE_AVERAGE_LIMIT = 3
# A category is "above its average" from Rp 50.000 and 10% over it; a category without spending in
# the baseline periods only from Rp 100.000 (OD91).
ABOVE_AVERAGE_MIN = 50_000
ABOVE_AVERAGE_RATIO_PERCENT = 10
NEW_CATEGORY_MIN = 100_000
# Pengeluaran is spending without saving; saving-pillar expenses belong to Ditabung & investasi.
SPENDING_PILLARS = ("need", "want", "culture", "unexpected", UNCLASSIFIED)

KakeiboBreakdown = Callable[..., dict[str, Any]]
RecurringInPeriod = Callable[..., dict[str, Any]]


def round_half_up_div(numerator: int, denominator: int) -> int:
    """`numerator / denominator` as a whole number, halves rounded up (both non-negative)."""
    return (2 * numerator + denominator) // (2 * denominator)


def share_percent(amount: int, total: int) -> int | None:
    if total <= 0:
        return None
    return round_half_up_div(100 * max(amount, 0), total)


def _zone() -> ZoneInfo:
    return ZoneInfo(settings.tz)


def _local_start(period: ObservedPeriod) -> date:
    return period.start.astimezone(_zone()).date()


def _local_end(period: ObservedPeriod) -> date:
    return period.last_instant.astimezone(_zone()).date()


@dataclass
class PeriodAmounts:
    period: ObservedPeriod
    partial: bool
    inflow: int
    fresh_saving: int
    # pillar -> category key -> {"category_id", "name", "amount"}
    categories: dict[str, dict[str, dict[str, Any]]]

    def pillar(self, pillar: str) -> int:
        return sum(item["amount"] for item in self.categories.get(pillar, {}).values())

    @property
    def spending(self) -> int:
        return sum(self.pillar(pillar) for pillar in SPENDING_PILLARS)

    def category_totals(self) -> dict[str, dict[str, Any]]:
        """Spending per category over the spending pillars (a category split across pillars summed)."""
        totals: dict[str, dict[str, Any]] = {}
        for pillar in SPENDING_PILLARS:
            for key, item in self.categories.get(pillar, {}).items():
                entry = totals.setdefault(key, {"category_id": item["category_id"], "name": item["name"], "amount": 0})
                entry["amount"] += item["amount"]
        return totals

    def totals(self) -> dict[str, int]:
        spending = self.spending
        return {"spending": spending, "inflow": self.inflow, "fresh_saving": self.fresh_saving,
                "difference": self.inflow - spending}


def _category_pillar_spending(cur, user_id: str, start: datetime, end: datetime, offsets) -> dict[str, dict[str, dict[str, Any]]]:
    """Net spending per (pillar bucket, category), each row's reimbursement share removed and floored at 0."""
    cur.execute(
        f"""
        SELECT {pillar_bucket_sql()} AS pillar, t.category_id, c.name, SUM(t.amount) AS spent
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s AND {reportable_expense_sql()} AND t.date >= %s AND t.date <= %s
        GROUP BY pillar, t.category_id, c.name
        """,
        (user_id, start, end),
    )
    result: dict[str, dict[str, dict[str, Any]]] = {}
    for row in cur.fetchall():
        category_id = str(row["category_id"]) if row["category_id"] else None
        gross = int(row["spent"])
        net = gross - (offsets.for_category_pillar(category_id, row["pillar"]) if category_id else 0)
        key = category_id or "uncategorized"
        result.setdefault(row["pillar"], {})[key] = {
            "category_id": category_id, "name": row["name"] or UNCATEGORIZED_NAME, "amount": max(0, net),
        }
    return result


def _period_amounts(cur, user_id: str, period: ObservedPeriod, partial: bool, kakeibo_breakdown: KakeiboBreakdown) -> PeriodAmounts:
    start, end = period.start, period.last_instant
    figures = window_figures(cur, user_id, start, end)
    offsets = figures["offsets"]
    kakeibo = kakeibo_breakdown(cur, user_id, start, end, figures["inflow"], offsets)
    return PeriodAmounts(
        period=period, partial=partial, inflow=figures["inflow"], fresh_saving=int(kakeibo["saving_spent"]),
        categories=_category_pillar_spending(cur, user_id, start, end, offsets),
    )


def _describe(period: ObservedPeriod, partial: bool, first_day: date | None) -> dict[str, Any]:
    described = period.describe()
    return {
        key: described[key] for key in ("label", "start", "end", "start_date", "end_date", "complete")
    } | {"partial": partial, "tracking_from": first_day.isoformat() if partial and first_day else None}


def _ref(period: ObservedPeriod, **filters: Any) -> dict[str, Any]:
    return {"from": period.start.isoformat(), "to": period.last_instant.isoformat(), **filters}


def _category_ref(period: ObservedPeriod, category_id: str | None, pillar: str | None = None) -> dict[str, Any]:
    filters: dict[str, Any] = {"category_id": category_id} if category_id else {"uncategorized": True}
    if pillar is not None:
        filters["kakeibo_type"] = pillar
    return _ref(period, **filters)


def _mean(values: list[int]) -> int:
    return round_half_up_div(sum(values), len(values))


def _change(current: int, other: int | None, *, allowed: bool) -> int | None:
    return current - other if allowed and other is not None else None


def build_comparison(
    cur, user_id: str, *, basis: str, period: ObservedPeriod, listing: dict[str, Any], now: datetime,
    kakeibo_breakdown: KakeiboBreakdown, recurring_in_period: RecurringInPeriod,
    merchants: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """The whole-period comparison of `period` (design D1–D7); `listing` is `list_periods` of the basis.

    `merchants` is the period's merchant spending, sorted, when the caller already loaded it (the report).
    """
    first_day: date | None = listing["first_day"]
    periods: list[ObservedPeriod] = listing["periods"]  # newest first
    index = next(i for i, item in enumerate(periods) if item.start == period.start)
    earlier = periods[index + 1:]

    def partial(item: ObservedPeriod) -> bool:
        return first_day is not None and first_day > _local_start(item)

    cache: dict[datetime, PeriodAmounts] = {}

    def amounts(item: ObservedPeriod) -> PeriodAmounts:
        if item.start not in cache:
            cache[item.start] = _period_amounts(cur, user_id, item, partial(item), kakeibo_breakdown)
        return cache[item.start]

    running = not period.complete
    current = amounts(period)
    previous_period = earlier[0] if earlier and earlier[0].complete else None
    previous = amounts(previous_period) if previous_period else None
    baseline_periods = [item for item in earlier if item.complete and not partial(item)][:BASELINE_PERIODS]
    baseline_amounts = [amounts(item) for item in baseline_periods] if len(baseline_periods) >= MIN_BASELINE_PERIODS else []

    # A verdict needs a finished period and, against the previous one, a whole previous period.
    previous_verdict = not running and previous is not None and not previous.partial
    baseline_verdict = not running and bool(baseline_amounts)

    totals = current.totals()
    result: dict[str, Any] = {
        "basis": basis,
        "period": _describe(period, current.partial, first_day),
        "running": running,
        "totals": totals,
        "previous": None,
        "baseline": None,
        "progress": None,
    }
    if previous is not None:
        previous_totals = previous.totals()
        result["previous"] = {
            **_describe(previous.period, previous.partial, first_day),
            "totals": previous_totals,
            "change": {key: _change(totals[key], previous_totals[key], allowed=previous_verdict) for key in totals},
        }
    if baseline_amounts:
        baseline_totals = {key: _mean([item.totals()[key] for item in baseline_amounts]) for key in totals}
        result["baseline"] = {
            "period_count": len(baseline_amounts),
            "labels": [item.period.label for item in reversed(baseline_amounts)],
            "totals": baseline_totals,
            "change": {key: _change(totals[key], baseline_totals[key], allowed=baseline_verdict) for key in totals},
        }
    if running:
        whole_previous = previous if previous is not None and not previous.partial else None
        result["progress"] = {
            "previous_label": whole_previous.period.label if whole_previous else None,
            "previous_spending": whole_previous.spending if whole_previous else None,
            "spending": current.spending,
            "share": share_percent(current.spending, whole_previous.spending) if whole_previous else None,
            "pillars": [
                {
                    "pillar": pillar, "amount": current.pillar(pillar),
                    "previous": whole_previous.pillar(pillar) if whole_previous else None,
                    "share": share_percent(current.pillar(pillar), whole_previous.pillar(pillar)) if whole_previous else None,
                }
                for pillar in SPENDING_PILLARS
            ],
        }

    result["pillars"] = _pillars(current, previous, baseline_amounts, previous_verdict, baseline_verdict)
    result["facts"] = _facts(
        cur, user_id, current, previous, baseline_amounts, running=running, now=now,
        previous_verdict=previous_verdict, recurring_in_period=recurring_in_period, basis=basis, merchants=merchants,
    )
    result["history"] = _history(periods, index, amounts, partial)
    return result


def _pillars(current: PeriodAmounts, previous: PeriodAmounts | None, baseline: list[PeriodAmounts],
             previous_verdict: bool, baseline_verdict: bool) -> list[dict[str, Any]]:
    spending = current.spending
    rows = []
    for pillar in SPENDING_PILLARS:
        amount = current.pillar(pillar)
        previous_amount = previous.pillar(pillar) if previous else None
        baseline_amount = _mean([item.pillar(pillar) for item in baseline]) if baseline else None
        categories = []
        for key, item in current.categories.get(pillar, {}).items():
            if item["amount"] <= 0:
                continue
            before = previous.categories.get(pillar, {}).get(key, {}).get("amount", 0) if previous else None
            usual = _mean([b.categories.get(pillar, {}).get(key, {}).get("amount", 0) for b in baseline]) if baseline else None
            categories.append({
                "category_id": item["category_id"], "name": item["name"], "amount": item["amount"],
                "share": share_percent(item["amount"], spending),
                "previous": before, "change": _change(item["amount"], before, allowed=previous_verdict),
                "baseline": usual, "baseline_change": _change(item["amount"], usual, allowed=baseline_verdict),
                "ref": _category_ref(current.period, item["category_id"], pillar),
            })
        categories.sort(key=lambda entry: (-entry["amount"], entry["name"]))
        if amount <= 0 and not categories and not previous_amount:
            continue
        rows.append({
            "pillar": pillar, "amount": amount, "share": share_percent(amount, spending),
            "previous": previous_amount, "change": _change(amount, previous_amount, allowed=previous_verdict),
            "baseline": baseline_amount, "baseline_change": _change(amount, baseline_amount, allowed=baseline_verdict),
            "ref": _ref(current.period, kakeibo_type=pillar),
            "categories": categories,
        })
    return rows


def _facts(cur, user_id: str, current: PeriodAmounts, previous: PeriodAmounts | None, baseline: list[PeriodAmounts], *,
           running: bool, now: datetime, previous_verdict: bool, recurring_in_period: RecurringInPeriod,
           basis: str, merchants: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """"Yang bisa diperbaiki" (D6); a running period gets only the top merchant, recurring and unexpected."""
    period = current.period
    unit = "bulan" if basis == "month" else "siklus"
    facts: list[dict[str, Any]] = []
    if not running and baseline:
        usual: dict[str, int] = {}
        for item in baseline:
            for key, entry in item.category_totals().items():
                usual[key] = usual.get(key, 0) + entry["amount"]
        above = []
        for key, entry in current.category_totals().items():
            average = round_half_up_div(usual.get(key, 0), len(baseline))
            difference = entry["amount"] - average
            if average == 0:
                qualifies = entry["amount"] >= NEW_CATEGORY_MIN
            else:
                qualifies = difference >= ABOVE_AVERAGE_MIN and difference * 100 >= ABOVE_AVERAGE_RATIO_PERCENT * average
            if qualifies:
                above.append((difference, entry, average))
        above.sort(key=lambda item: (-item[0], item[1]["name"]))
        for difference, entry, average in above[:ABOVE_AVERAGE_LIMIT]:
            new = average == 0
            facts.append({
                "kind": "category_new" if new else "category_above_average",
                "label": f"{entry['name']}: kategori baru" if new else f"{entry['name']} di atas rata-rata {len(baseline)} {unit}",
                "amount": entry["amount"], "difference": difference, "baseline": average,
                "ref": _category_ref(period, entry["category_id"]),
            })
    if not running and previous_verdict and previous is not None:
        share_now = share_percent(current.pillar("want"), current.spending)
        share_before = share_percent(previous.pillar("want"), previous.spending)
        if share_now is not None and share_before is not None:
            facts.append({
                "kind": "want_share",
                "label": f"Porsi Keinginan {share_now}% dari pengeluaran ({previous.period.label}: {share_before}%)",
                "amount": current.pillar("want"), "share": share_now, "previous_share": share_before,
                "ref": _ref(period, kakeibo_type="want"),
            })
    if merchants is None:
        merchants = sorted(merchant_spending(cur, user_id, period.start, period.last_instant).values(),
                           key=lambda item: (-item["amount"], item["name"]))
    if merchants:
        top = merchants[0]
        facts.append({
            "kind": "top_merchant", "label": f"Tempat belanja terbesar: {top['name']}", "amount": top["amount"],
            "ref": _ref(period, q=top["search_text"]),
        })
    recurring = recurring_in_period(cur, user_id, period, now=now)
    if recurring["charges"]:
        count = len(recurring["charges"])
        facts.append({
            "kind": "recurring", "label": f"{count} tagihan dan langganan berulang", "count": count,
            "amount": recurring["monthly_equivalent_total"], "ref": None,
        })
    unexpected = current.pillar("unexpected")
    if unexpected > 0:
        count = _unexpected_count(cur, user_id, period)
        facts.append({
            "kind": "unexpected", "label": f"{count} pengeluaran tak terduga", "count": count, "amount": unexpected,
            "ref": _ref(period, kakeibo_type="unexpected"),
        })
    return facts[:FACT_LIMIT]


def _unexpected_count(cur, user_id: str, period: ObservedPeriod) -> int:
    cur.execute(
        f"""SELECT COUNT(*) AS count FROM transactions t LEFT JOIN categories c ON c.id = t.category_id
            WHERE t.user_id = %s AND {reportable_expense_sql()} AND {pillar_bucket_sql()} = 'unexpected'
              AND t.date >= %s AND t.date <= %s""",
        (user_id, period.start, period.last_instant),
    )
    return int(cur.fetchone()["count"])


def _history(periods: list[ObservedPeriod], index: int, amounts: Callable[[ObservedPeriod], PeriodAmounts],
             partial: Callable[[ObservedPeriod], bool]) -> list[dict[str, Any]]:
    """The four most recent completed periods up to the viewed one, plus the running period, oldest first."""
    completed = [item for item in periods[index:] if item.complete][:HISTORY_COMPLETED]
    selected = list(reversed(completed))
    if periods and not periods[0].complete:
        selected.append(periods[0])
    viewed = periods[index].start
    history = []
    for item in selected:
        figures = amounts(item)
        history.append({
            "label": item.label, "start": item.start.isoformat(), "end": item.last_instant.isoformat(),
            "complete": item.complete, "partial": partial(item), "viewed": item.start == viewed,
            "spending": figures.spending,
            "pillars": {pillar: figures.pillar(pillar) for pillar in SPENDING_PILLARS},
        })
    return history
