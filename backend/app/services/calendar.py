"""The Analisis calendar: every local day of a period with its spending, markers and plans.

A day's spending is the ledger's reportable spending less the reimbursement offset the daily
apportionment gives that day, the same path as the analytics `daily_cadence`, so the days add up exactly
to the period's Belanja. Waiting rows are counted with the Tinjau badge's SQL and stay in the day's
spending where they are reportable. Heat levels and the upcoming total are computed here, so the page
adds nothing up.
"""

from __future__ import annotations

import math
from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.i18n import language_for_user, t
from app.services.cycle_summary import expected_salary_for_cycle
from app.services.plan_occurrences import plan_occurrences_in_window
from app.services.reimbursements import load_spending_offsets
from app.services.report_figures import daily_offsets, reportable_expense_sql, reportable_income_sql
from app.services.report_windows import (
    cycle_range_label,
    get_timeframe_window,
    shift_month,
)
from app.services.salary_cycles import resolve_cycle
from app.services.salary_receipts import receipts
from app.services.waiting_rows import fetch_waiting_rows

HEAT_PERCENTILES = (0.2, 0.4, 0.6, 0.8)
HEAT_ROUNDING = 1000

INVESTMENT_ACCOUNT_SQL = "({alias}.type = 'investment' OR {alias}.instrument_type IS NOT NULL)"


def heat_thresholds(spends: list[int]) -> list[int]:
    """Nearest-rank 20th, 40th, 60th and 80th percentiles of the days with spending, to Rp 1.000."""
    values = sorted(amount for amount in spends if amount > 0)
    if not values:
        return []
    thresholds = []
    for share in HEAT_PERCENTILES:
        value = values[max(0, math.ceil(share * len(values)) - 1)]
        thresholds.append(int((value + HEAT_ROUNDING // 2) // HEAT_ROUNDING * HEAT_ROUNDING))
    return thresholds


def heat_level(amount: int, thresholds: list[int]) -> int:
    """0 without spending; otherwise 1 plus the number of thresholds the amount is above, at most 5."""
    if amount <= 0:
        return 0
    return min(5, 1 + sum(1 for threshold in thresholds if amount > threshold))


def _grid_end(start_date: date, end_date: date, today: date, open_cycle: bool, expected) -> date:
    """The last day the calendar shows.

    An open salary cycle has no known end: it runs to the day before the expected salary, or for one
    nominal month when that date cannot be estimated. Other windows end where the report window ends.
    """
    if not open_cycle:
        return end_date
    if expected.status == "estimated" and expected.date is not None and expected.date > today:
        return max(end_date, expected.date - timedelta(days=1))
    year, month = shift_month(start_date.year, start_date.month, 1)
    nominal = date(year, month, min(start_date.day, monthrange(year, month)[1])) - timedelta(days=1)
    return max(end_date, nominal)


def build_calendar(conn, user: dict[str, Any], *, timeframe: str, cycle_offset: int, now: datetime) -> dict[str, Any]:
    """The calendar of one period, read through one connection in a constant number of statements."""
    user_id = str(user["id"])
    language = language_for_user(user)
    payday_day = int(user.get("payday_day") or 25)
    zone = ZoneInfo(settings.tz)
    today = now.astimezone(zone).date()

    receipt_rows = receipts(conn, user_id)
    cycle = None
    if timeframe == "cycle":
        cycle = resolve_cycle([row["date"] for row in receipt_rows], offset=cycle_offset, tz_name=settings.tz, as_of=now)
    start_utc, end_utc, start_date, end_date, report_label = get_timeframe_window(
        payday_day, timeframe, cycle_offset=cycle_offset, user_id=user_id, cycle=cycle, language=language
    )
    open_cycle = cycle is not None and cycle.start is not None and cycle.end_exclusive is None
    # An open cycle's report window ends today, a month's at its last day, so both hold today when current.
    is_current = start_date <= today <= end_date
    expected = expected_salary_for_cycle(conn, user_id, open_cycle and cycle_offset == 0, receipt_rows, now)
    grid_end = _grid_end(start_date, end_date, today, open_cycle, expected)
    # The report window's bounds, cut at now when current; days after today have no rows.
    read_start, read_end = start_utc, end_utc

    with conn.cursor() as cur:
        offsets = load_spending_offsets(cur, user_id, read_start, read_end)
        investment_inbound = INVESTMENT_ACCOUNT_SQL.format(alias="a")
        investment_pair = INVESTMENT_ACCOUNT_SQL.format(alias="pair")
        cur.execute(
            f"""
            SELECT (t.date AT TIME ZONE %s)::date AS day,
                   COALESCE(SUM(t.amount) FILTER (WHERE {reportable_expense_sql()}), 0) AS spend,
                   COUNT(*) FILTER (WHERE {reportable_expense_sql()}) AS spend_count,
                   COALESCE(SUM(t.amount) FILTER (WHERE {reportable_income_sql()}), 0) AS income_total,
                   COUNT(*) FILTER (WHERE {reportable_income_sql()}) AS income_count,
                   COALESCE(SUM(t.amount) FILTER (
                       WHERE t.movement_role = 'outbound' AND pair.id IS NOT NULL AND NOT {investment_pair}
                   ), 0) AS movement_total,
                   COUNT(*) FILTER (
                       WHERE t.movement_role = 'outbound' AND pair.id IS NOT NULL AND NOT {investment_pair}
                   ) AS movement_count,
                   COALESCE(SUM(t.amount) FILTER (
                       WHERE t.movement_role = 'inbound' AND {investment_inbound}
                   ), 0) AS topup_total,
                   COUNT(*) FILTER (WHERE t.movement_role = 'inbound' AND {investment_inbound}) AS topup_count
            FROM transactions t
            LEFT JOIN categories c ON c.id = t.category_id
            LEFT JOIN accounts a ON a.id = t.account_id
            LEFT JOIN LATERAL (
                SELECT pa.id, pa.type, pa.instrument_type FROM transactions p
                JOIN accounts pa ON pa.id = p.account_id
                WHERE t.movement_role = 'outbound' AND p.user_id = t.user_id
                  AND p.movement_id = t.movement_id AND p.movement_role = 'inbound'
                LIMIT 1
            ) pair ON TRUE
            WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
            GROUP BY day
            """,
            (settings.tz, user_id, read_start, read_end),
        )
        daily = {row["day"]: row for row in cur.fetchall()}
        day_offsets = daily_offsets(cur, user_id, read_start, read_end, offsets)
        waiting = fetch_waiting_rows(cur, user_id, read_start, read_end)
        plans = plan_occurrences_in_window(
            cur, user_id, payday_day, start_date, grid_end, today, include_resolved=True,
        )

    waiting_by_day: dict[date, list[dict[str, Any]]] = {}
    for row in waiting:
        waiting_by_day.setdefault(row["date"].astimezone(zone).date(), []).append(row)
    plans_by_day: dict[str, list[dict[str, Any]]] = {}
    for line in plans:
        plans_by_day.setdefault(line["due_date"], []).append(
            {"rule_id": line["rule_id"], "label": line["label"], "amount": line["amount"], "status": line["status"]}
        )

    days = []
    day = start_date
    while day <= grid_end:
        row = daily.get(day)
        spend = (int(row["spend"]) if row else 0) - day_offsets.get(day, 0)
        if spend < 0:
            raise RuntimeError("daily apportionment left a negative day")
        day_waiting = waiting_by_day.get(day, [])
        days.append({
            "date": day.isoformat(),
            "spend": spend,
            "spend_count": int(row["spend_count"]) if row else 0,
            "heat": 0,
            "income_total": int(row["income_total"]) if row else 0,
            "income_count": int(row["income_count"]) if row else 0,
            "movement_total": int(row["movement_total"]) if row else 0,
            "movement_count": int(row["movement_count"]) if row else 0,
            "topup_total": int(row["topup_total"]) if row else 0,
            "topup_count": int(row["topup_count"]) if row else 0,
            "waiting_count": len(day_waiting),
            "waiting_amount": sum(int(item["amount"]) for item in day_waiting),
            "waiting": [
                {
                    "transaction_id": str(item["id"]),
                    "section": item["section"],
                    "label": item["merchant"] or item["notes"] or item["category_name"] or t(language, "labels.fallback.undescribed"),
                    "account_name": item["account_name"],
                    "type": item["type"],
                    "amount": int(item["amount"]),
                }
                for item in day_waiting
            ],
            "plans": plans_by_day.get(day.isoformat(), []),
        })
        day += timedelta(days=1)

    thresholds = heat_thresholds([entry["spend"] for entry in days])
    for entry in days:
        entry["heat"] = heat_level(entry["spend"], thresholds)

    gross = sum(int(row["spend"]) for row in daily.values())
    spend_total = sum(entry["spend"] for entry in days)
    if gross - spend_total != sum(day_offsets.values()):
        raise RuntimeError("calendar days do not add up to the period's Belanja")

    upcoming: list[dict[str, Any]] = []
    if is_current:
        upcoming = [
            {"date": line["due_date"], "rule_id": line["rule_id"], "label": line["label"],
             "amount": line["amount"], "status": line["status"]}
            for line in plans
            if line["status"] in ("due", "overdue") and line["due_date"] >= today.isoformat()
        ]
    return {
        "window": {
            "start": start_date.isoformat(),
            "end": grid_end.isoformat(),
            # An open cycle's report label says "berlanjut"; the calendar names the days it shows.
            "label": cycle_range_label(language, start_date, grid_end) if open_cycle else report_label,
            "basis": timeframe,
            "today": today.isoformat(),
            "day_index": (today - start_date).days + 1 if is_current else None,
            "days_total": (grid_end - start_date).days + 1,
            "is_current": is_current,
            "open": open_cycle,
        },
        "expected_salary": {
            "date": expected.date.isoformat() if expected.date else None,
            "available": expected.date is not None and expected.status == "estimated",
            "status": expected.status,
            "reason": expected.reason,
        },
        "totals": {"spend": spend_total, "gross_spend": gross, "offset_applied": gross - spend_total},
        "days": days,
        "upcoming": upcoming,
        "upcoming_total": sum(line["amount"] for line in upcoming),
        "upcoming_basis": "expected_salary" if open_cycle and grid_end != end_date and expected.status == "estimated" else "window_end",
        "heat_thresholds": thresholds,
    }
