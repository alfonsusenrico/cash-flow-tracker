"""Observed insights for Analisis: a fair period comparison, cash flow per period and recurring charges.

Every figure is measured from recorded ledger rows over a stated window. Nothing here projects or
averages into the future; the next date of a recurring charge is labelled as an estimate.
"""

from __future__ import annotations

import calendar
import math
import re
import statistics
import unicodedata
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.services.kakeibo import PILLARS
from app.services.observed_periods import ObservedPeriod, load_period_history
from app.services.reimbursements import load_spending_offsets
from app.services.report_figures import (
    cash_flow_totals,
    category_breakdown,
    reportable_expense_sql,
    reportable_income_sql,
)
from app.services.report_windows import shift_month

TOP_CHANGES = 3
RECURRING_LOOKBACK_MONTHS = 6
RECURRING_MIN_OCCURRENCES = 3
WEEKLY_INTERVAL_DAYS = (6, 8)
MONTHLY_INTERVAL_DAYS = (25, 35)
DOMINANT_INTERVAL_SHARE = 2 / 3
MAX_AMOUNT_VARIATION = 0.15
PLAN_AMOUNT_TOLERANCE = 0.15
WEEKS_PER_MONTH = 52 / 12

_DIGITS = re.compile(r"\d+")
_NON_WORD = re.compile(r"[^\w\s]")


def merchant_key(value: str | None) -> str | None:
    """Grouping key for a merchant: case, accents' compatibility forms, digits and punctuation removed."""
    text = unicodedata.normalize("NFKC", value or "").casefold()
    text = _NON_WORD.sub(" ", _DIGITS.sub(" ", text))
    words = text.split()
    return " ".join(words) if words else None


def search_text(name: str) -> str:
    """Ledger search text for a merchant group: its name without reference numbers."""
    return " ".join(_DIGITS.sub(" ", name).split()).strip(" -#*/:") or name


def _window_figures(cur, user_id: str, start: datetime, end: datetime) -> dict[str, Any]:
    cur.execute(
        f"""
        SELECT
            COALESCE(SUM(CASE WHEN {reportable_income_sql()} THEN t.amount ELSE 0 END), 0) AS inflow,
            COALESCE(SUM(CASE WHEN {reportable_expense_sql()} THEN t.amount ELSE 0 END), 0) AS outflow,
            COUNT(*) AS rows
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
        """,
        (user_id, start, end),
    )
    totals = cur.fetchone()
    offsets = load_spending_offsets(cur, user_id, start, end)
    flow = cash_flow_totals(int(totals["inflow"]), int(totals["outflow"]), offsets.applied)
    return {**flow, "row_count": int(totals["rows"]), "offsets": offsets}


def _pillar_spending(cur, user_id: str, start: datetime, end: datetime, offsets) -> dict[str, int]:
    cur.execute(
        f"""
        SELECT COALESCE(t.kakeibo_type, c.kakeibo_type, 'need') AS pillar, SUM(t.amount) AS spent
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s AND {reportable_expense_sql()} AND t.date >= %s AND t.date <= %s
        GROUP BY pillar
        """,
        (user_id, start, end),
    )
    gross = {row["pillar"]: int(row["spent"]) for row in cur.fetchall()}
    return {pillar: max(0, gross.get(pillar, 0) - offsets.for_pillar(pillar)) for pillar in PILLARS}


def _merchant_spending(cur, user_id: str, start: datetime, end: datetime) -> dict[str, dict[str, Any]]:
    """Gross reportable spending per merchant; reimbursements apply to categories, not merchants."""
    cur.execute(
        f"""
        SELECT t.merchant, t.amount, t.date
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s AND {reportable_expense_sql()} AND t.merchant IS NOT NULL
          AND t.date >= %s AND t.date <= %s
        ORDER BY t.date DESC
        """,
        (user_id, start, end),
    )
    merchants: dict[str, dict[str, Any]] = {}
    for row in cur.fetchall():
        key = merchant_key(row["merchant"])
        if not key:
            continue
        item = merchants.setdefault(key, {"name": row["merchant"], "search_text": search_text(row["merchant"]), "amount": 0})
        item["amount"] += int(row["amount"])
    return merchants


def _top_changes(current: dict[str, dict[str, Any]], previous: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    changes = []
    for key in set(current) | set(previous):
        now_amount = current.get(key, {}).get("amount", 0)
        before_amount = previous.get(key, {}).get("amount", 0)
        if now_amount == before_amount:
            continue
        source = current.get(key) or previous.get(key)
        changes.append({
            "key": key, "id": source.get("id"), "name": source["name"], "is_uncategorized": source.get("is_uncategorized", False),
            "search_text": source.get("search_text"),
            "current": now_amount, "previous": before_amount, "change": now_amount - before_amount,
        })
    increases = sorted((item for item in changes if item["change"] > 0), key=lambda item: (-item["change"], item["name"]))
    decreases = sorted((item for item in changes if item["change"] < 0), key=lambda item: (item["change"], item["name"]))
    return {"increases": increases[:TOP_CHANGES], "decreases": decreases[:TOP_CHANGES]}


def _category_amounts(cur, user_id: str, start: datetime, end: datetime, offsets) -> dict[str, dict[str, Any]]:
    rows = category_breakdown(cur, user_id, start, end, offsets)
    return {
        (row["id"] or "uncategorized"): {
            "id": row["id"], "name": row["name"], "amount": int(row["spent"]), "is_uncategorized": row["is_uncategorized"],
        }
        for row in rows if row["spent"] or row["gross_spent"]
    }


def _window_dates(start: datetime, end: datetime) -> dict[str, str]:
    zone = ZoneInfo(settings.tz)
    return {
        "start": start.isoformat(), "end": end.isoformat(),
        "start_date": start.astimezone(zone).date().isoformat(), "end_date": end.astimezone(zone).date().isoformat(),
    }


def period_comparison(
    cur, user_id: str, *, current: tuple[datetime, datetime, str], previous: tuple[datetime, datetime, str] | None,
) -> dict[str, Any]:
    """The current period against the previous one over the same elapsed time from each start."""
    start, end, label = current
    elapsed = end - start
    if previous is None or previous[1] < previous[0]:
        cur_window = (start, end)
        prev_window = None
        compared = elapsed
    else:
        compared = min(elapsed, previous[1] - previous[0])
        cur_window = (start, start + compared)
        prev_window = (previous[0], previous[0] + compared)
    zone = ZoneInfo(settings.tz)
    days = (cur_window[1].astimezone(zone).date() - cur_window[0].astimezone(zone).date()).days + 1
    now_figures = _window_figures(cur, user_id, *cur_window)
    now_pillars = _pillar_spending(cur, user_id, *cur_window, now_figures["offsets"])
    now_categories = _category_amounts(cur, user_id, *cur_window, now_figures["offsets"])
    now_merchants = _merchant_spending(cur, user_id, *cur_window)
    result: dict[str, Any] = {
        "label": label,
        "compared_days": days,
        "current_truncated": cur_window[1] < end,
        "current": {**_window_dates(*cur_window), "inflow": now_figures["inflow"], "outflow": now_figures["outflow"],
                    "net": now_figures["net"], "reimbursement_offset": now_figures["reimbursement_offset"]},
        "previous": None,
        "previous_label": previous[2] if previous else None,
        "previous_has_data": False,
        "pillars": [{"pillar": pillar, "current": now_pillars[pillar], "previous": None, "change": None} for pillar in PILLARS],
        "categories": _top_changes(now_categories, {}),
        "merchants": _top_changes(now_merchants, {}),
    }
    if prev_window is None:
        return result
    before = _window_figures(cur, user_id, *prev_window)
    before_pillars = _pillar_spending(cur, user_id, *prev_window, before["offsets"])
    result["previous"] = {**_window_dates(*prev_window), "inflow": before["inflow"], "outflow": before["outflow"],
                          "net": before["net"], "reimbursement_offset": before["reimbursement_offset"]}
    result["previous_has_data"] = before["row_count"] > 0
    if not result["previous_has_data"]:
        return result
    result["pillars"] = [
        {"pillar": pillar, "current": now_pillars[pillar], "previous": before_pillars[pillar],
         "change": now_pillars[pillar] - before_pillars[pillar]}
        for pillar in PILLARS
    ]
    result["categories"] = _top_changes(now_categories, _category_amounts(cur, user_id, *prev_window, before["offsets"]))
    result["merchants"] = _top_changes(now_merchants, _merchant_spending(cur, user_id, *prev_window))
    return result


def _period_flow(cur, user_id: str, period: ObservedPeriod) -> dict[str, Any]:
    figures = _window_figures(cur, user_id, period.start, period.last_instant)
    return {
        **period.describe(),
        "in_progress": not period.complete,
        "inflow": figures["inflow"], "outflow": figures["outflow"], "net": figures["net"],
        "gross_outflow": figures["gross_outflow"], "reimbursement_offset": figures["reimbursement_offset"],
    }


def cash_flow_history(cur, user_id: str, *, basis: str = "auto", now: datetime | None = None) -> dict[str, Any]:
    history = load_period_history(cur, user_id, now=now, basis=basis)
    periods = [_period_flow(cur, user_id, period) for period in reversed(history.completed)]
    if history.current is not None:
        periods.append(_period_flow(cur, user_id, history.current))
    return {"basis": basis, "cycle_status": history.cycle_status, "periods": periods}


def _interval_band(days: int) -> str | None:
    if WEEKLY_INTERVAL_DAYS[0] <= days <= WEEKLY_INTERVAL_DAYS[1]:
        return "weekly"
    if MONTHLY_INTERVAL_DAYS[0] <= days <= MONTHLY_INTERVAL_DAYS[1]:
        return "monthly"
    return None


def _add_month(day: date) -> date:
    year, month = shift_month(day.year, day.month, 1)
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def detect_charge(rows: list[dict[str, Any]], today: date) -> dict[str, Any] | None:
    """A recurring charge from one merchant's rows (oldest first), or None when it does not qualify.

    Qualifies with at least three occurrences, a dominant weekly (6–8 days) or monthly (25–35 days)
    interval holding at least two thirds of the gaps, and an amount coefficient of variation of at
    most 0.15. A charge not seen for longer than 1.5 times its interval is treated as ended.
    """
    if len(rows) < RECURRING_MIN_OCCURRENCES:
        return None
    days = [row["day"] for row in rows]
    intervals = [(later - earlier).days for earlier, later in zip(days, days[1:])]
    bands = Counter(_interval_band(gap) for gap in intervals)
    cadence = next(
        (band for band in ("monthly", "weekly") if bands[band] >= math.ceil(len(intervals) * DOMINANT_INTERVAL_SHARE)),
        None,
    )
    if cadence is None:
        return None
    amounts = [int(row["amount"]) for row in rows]
    mean = statistics.fmean(amounts)
    variation = statistics.pstdev(amounts) / mean if mean else math.inf
    if variation > MAX_AMOUNT_VARIATION:
        return None
    typical_gap = statistics.median(gap for gap in intervals if _interval_band(gap) == cadence)
    last_day = days[-1]
    if (today - last_day).days > 1.5 * typical_gap:
        return None
    typical = int(statistics.median(amounts[-3:]))
    next_day = _add_month(last_day) if cadence == "monthly" else last_day + timedelta(days=7)
    monthly_equivalent = typical if cadence == "monthly" else int(round(typical * WEEKS_PER_MONTH))
    last = rows[-1]
    return {
        "occurrences": len(rows),
        "cadence": cadence,
        "typical_amount": typical,
        "amount_variation": round(variation, 3),
        "last_amount": amounts[-1],
        "previous_amount": amounts[-2],
        "amount_changed": amounts[-1] != amounts[-2],
        "last_date": last_day.isoformat(),
        "next_expected_date": next_day.isoformat(),
        "next_date_is_estimate": True,
        "monthly_equivalent": monthly_equivalent,
        "account_id": str(last["account_id"]),
        "account_name": last["account_name"],
        "category_id": str(last["category_id"]) if last["category_id"] else None,
        "category_name": last["category_name"],
    }


def _plan_matches(cur, user_id: str) -> tuple[list[dict[str, Any]], dict[str, str]]:
    cur.execute(
        """
        SELECT id, name, amount, source_account_id, schedule_type FROM recurring_rules
        WHERE user_id = %s AND is_active AND type = 'expense'
        """,
        (user_id,),
    )
    plans = cur.fetchall()
    cur.execute(
        """
        SELECT e.transaction_id, e.recurring_rule_id FROM recurring_executions e
        JOIN recurring_rules r ON r.id = e.recurring_rule_id
        WHERE e.user_id = %s AND r.is_active AND e.status IN ('linked', 'succeeded') AND e.transaction_id IS NOT NULL
        UNION
        SELECT t.id, t.recurring_rule_id FROM transactions t
        JOIN recurring_rules r ON r.id = t.recurring_rule_id
        WHERE t.user_id = %s AND r.is_active
        """,
        (user_id, user_id),
    )
    linked = {str(row["transaction_id"]): str(row["recurring_rule_id"]) for row in cur.fetchall()}
    return plans, linked


def _matching_plan(charge: dict[str, Any], key: str, transaction_ids: list[str], plans, linked) -> dict[str, str] | None:
    by_id = {str(plan["id"]): plan for plan in plans}
    for transaction_id in transaction_ids:
        plan = by_id.get(linked.get(transaction_id, ""))
        if plan:
            return {"id": str(plan["id"]), "name": plan["name"]}
    for plan in plans:
        cadence = "weekly" if plan["schedule_type"] == "weekly" else "monthly"
        same_name = merchant_key(plan["name"]) == key
        similar = (
            str(plan["source_account_id"]) == charge["account_id"] and cadence == charge["cadence"]
            and abs(int(plan["amount"]) - charge["typical_amount"]) <= PLAN_AMOUNT_TOLERANCE * charge["typical_amount"]
        )
        if same_name or similar:
            return {"id": str(plan["id"]), "name": plan["name"]}
    return None


def recurring_charges(cur, user_id: str, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    zone = ZoneInfo(settings.tz)
    today = now.astimezone(zone).date()
    year, month = shift_month(today.year, today.month, -RECURRING_LOOKBACK_MONTHS)
    first_day = date(year, month, 1)
    since = datetime.combine(first_day, datetime.min.time(), tzinfo=zone).astimezone(timezone.utc)
    cur.execute(
        f"""
        SELECT t.id, t.amount, t.date, t.merchant, t.notes, t.account_id, a.name AS account_name,
               t.category_id, c.name AS category_name
        FROM transactions t
        JOIN accounts a ON a.id = t.account_id
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s AND t.type = 'expense' AND t.movement_id IS NULL AND t.fee_for_movement_id IS NULL
          AND a.type <> 'investment' AND COALESCE(c.name, '') NOT IN ('Internal Movement', 'Investasi')
          AND t.date >= %s AND t.date <= %s
        ORDER BY t.date, t.id
        """,
        (user_id, since, now),
    )
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in cur.fetchall():
        key = merchant_key(row["merchant"] or row["notes"])
        if key:
            groups.setdefault(key, []).append({**row, "day": row["date"].astimezone(zone).date()})
    plans, linked = _plan_matches(cur, user_id)
    charges = []
    for key, rows in groups.items():
        charge = detect_charge(rows, today)
        if charge is None:
            continue
        name = rows[-1]["merchant"] or rows[-1]["notes"]
        charge.update({
            "key": key, "merchant": name, "search_text": search_text(name),
            "existing_plan": _matching_plan(charge, key, [str(row["id"]) for row in rows], plans, linked),
            "plan_draft": {
                "name": name[:100], "type": "expense", "amount": charge["typical_amount"],
                "source_account_id": charge["account_id"], "category_id": charge["category_id"],
                "schedule_type": "weekly" if charge["cadence"] == "weekly" else "monthly_day",
                "schedule_day": date.fromisoformat(charge["last_date"]).isoweekday()
                if charge["cadence"] == "weekly" else date.fromisoformat(charge["last_date"]).day,
            },
        })
        charges.append(charge)
    charges.sort(key=lambda item: (-item["monthly_equivalent"], item["merchant"]))
    return {
        "window": {"start": since.isoformat(), "end": now.isoformat(), "start_date": first_day.isoformat(),
                   "end_date": today.isoformat(), "months": RECURRING_LOOKBACK_MONTHS},
        "rules": {
            "min_occurrences": RECURRING_MIN_OCCURRENCES,
            "weekly_interval_days": list(WEEKLY_INTERVAL_DAYS),
            "monthly_interval_days": list(MONTHLY_INTERVAL_DAYS),
            "max_amount_variation": MAX_AMOUNT_VARIATION,
        },
        "charges": charges,
        "monthly_equivalent_total": sum(item["monthly_equivalent"] for item in charges),
        "monthly_equivalent_without_plan": sum(item["monthly_equivalent"] for item in charges if not item["existing_plan"]),
    }
