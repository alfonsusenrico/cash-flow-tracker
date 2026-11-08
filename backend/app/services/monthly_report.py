"""Laporan Bulanan: one report per calendar month or closed salary cycle (add-monthly-report).

The report composes the shared reporting contract: window figures, the Kakeibo breakdown, the category
breakdown, the whole-period comparison, recurring charges and the essential cost. It adds no calculation rule of
its own, so each figure equals the ledger summary for the same window (design D1).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.i18n import account_language
from app.services.kakeibo import UNCLASSIFIED, pillar_bucket_sql
from app.services.monthly_comparison import build_comparison
from app.services.observed_insights import (
    merchant_spending,
    recurring_charges,
    window_figures,
)
from app.services.observed_periods import (
    ObservedPeriod,
    cycle_label,
    first_recorded_day,
    month_period,
    salary_boundaries,
)
from app.services.report_figures import (
    uncategorized_name,
    category_breakdown,
    possible_duplicate_sql,
    reportable_expense_sql,
    reportable_income_sql,
)
from app.services.report_windows import shift_month
from app.services.seen_twice import seen_twice_count

BASES = ("month", "cycle")
BASIS_PATTERN = "^(month|cycle)$"
CLOSED_PERIOD_LIMIT = 24
TOP_LIMIT = 5
UNEXPECTED_ITEM_LIMIT = 10
SPENDING_PILLARS = ("need", "want", "culture", "unexpected", "saving", UNCLASSIFIED)
FLOW_KEYS = ("inflow", "gross_outflow", "reimbursement_offset", "outflow", "net")

# The Tinjau badge's rules, shared by every figure that counts or lists the rows behind it.
OPEN_QUESTION_JOIN_SQL = """
    LEFT JOIN LATERAL (
        SELECT e.id FROM notification_events e
        WHERE e.user_id = t.user_id AND e.processing_state = 'recorded'
          AND e.active_question ->> 'type' = 'purpose'
          AND (e.active_question ->> 'transaction_id')::uuid = t.id
        LIMIT 1
    ) question ON TRUE
"""
ATTENTION_SQL = "(t.review_status = 'needs_review' OR question.id IS NOT NULL)"
REVIEWABLE_ROW_SQL = (
    "t.type IN ('expense', 'income') AND (t.movement_id IS NULL OR question.id IS NOT NULL)"
)


def waiting_row_sql(tx: str = "t") -> str:
    """The rows behind the badge's `attention`, as one predicate without the question join.

    REVIEWABLE_ROW_SQL AND ATTENTION_SQL reduce to: an expense or income that is an unlinked row needing
    review, or any row with an open purpose question. `GET /api/transactions?waiting=true` uses it, so
    the ledger's waiting filter lists exactly the rows the badge counts.
    """
    open_question = (
        "EXISTS (SELECT 1 FROM notification_events e"
        f" WHERE e.user_id = {tx}.user_id AND e.processing_state = 'recorded'"
        " AND e.active_question ->> 'type' = 'purpose'"
        f" AND (e.active_question ->> 'transaction_id')::uuid = {tx}.id)"
    )
    return (
        f"({tx}.type IN ('expense', 'income')"
        f" AND (({tx}.movement_id IS NULL AND {tx}.review_status = 'needs_review') OR {open_question}))"
    )

# Rows waiting in Tinjau: the badge's "attention" (needs review or an open purpose question) and the
# automatic rows nobody looked at yet. `GET /api/review/summary` and the review queue use this query too.
# A leg of a linked movement is finished and never counts, unless it still carries an open question.
_REVIEW_COUNT_SQL = f"""
    SELECT
        COUNT(*) FILTER (WHERE {ATTENTION_SQL}) AS attention,
        COUNT(*) FILTER (WHERE t.review_status = 'auto' AND question.id IS NULL) AS unreviewed
    FROM transactions t
    {OPEN_QUESTION_JOIN_SQL}
    WHERE t.user_id = %s AND {REVIEWABLE_ROW_SQL}
      AND (t.review_status IN ('auto', 'needs_review') OR question.id IS NOT NULL)
"""


class ReportError(Exception):
    """A request for a period that does not exist or a basis the owner cannot use."""

    def __init__(self, status: int, code: str):
        super().__init__(code)
        self.status = status
        self.code = code


def _zone() -> ZoneInfo:
    return ZoneInfo(settings.tz)


def local_start(period: ObservedPeriod) -> date:
    return period.start.astimezone(_zone()).date()


def local_end(period: ObservedPeriod) -> date:
    return period.last_instant.astimezone(_zone()).date()


def report_path(basis: str, start_date: date) -> str:
    return f"/laporan?basis={basis}&start={start_date.isoformat()}"


def list_periods(cur, user_id: str, basis: str, *, now: datetime) -> dict[str, Any]:
    """The period in progress, then up to 24 closed periods of the basis, newest first.

    Unlike the essential-cost history, the period in which tracking began is listed (and flagged), so a
    user still sees their first, partial month. Salary cycles are listed only when receipts are confirmed.
    """
    first_day = first_recorded_day(cur, user_id)
    boundaries, cycle_status = salary_boundaries(cur, user_id, now)
    cycle_available = bool(boundaries)
    language, _region = account_language(cur, user_id)
    periods: list[ObservedPeriod] = []
    if first_day is not None and basis == "month":
        today = now.astimezone(_zone()).date()
        year, month = today.year, today.month
        for _ in range(CLOSED_PERIOD_LIMIT + 1):
            period = month_period(year, month, now=now, language=language)
            if local_end(period) < first_day:
                break
            periods.append(period)
            year, month = shift_month(year, month, -1)
    elif first_day is not None and basis == "cycle" and boundaries:
        periods.append(ObservedPeriod("cycle", boundaries[-1], now,
                                      cycle_label(boundaries[-1], now, open_cycle=True, language=language), False))
        closed = [
            ObservedPeriod("cycle", start, end,
                           cycle_label(start, end - timedelta(microseconds=1), open_cycle=False, language=language), True)
            for start, end in zip(boundaries[:-1], boundaries[1:])
        ]
        periods.extend(reversed(closed[-CLOSED_PERIOD_LIMIT:]))
    return {
        "periods": periods, "first_day": first_day,
        "cycle_available": cycle_available, "cycle_status": cycle_status, "language": language,
    }


def describe_period(period: ObservedPeriod, basis: str, first_day: date | None) -> dict[str, Any]:
    started_inside = first_day is not None and first_day > local_start(period)
    return {
        **period.describe(),
        "basis": basis,
        "tracking_started_inside": started_inside,
        "first_recorded_date": first_day.isoformat() if started_inside else None,
        "url": report_path(basis, local_start(period)),
    }


def default_period(periods: list[ObservedPeriod]) -> ObservedPeriod | None:
    """The latest closed period, else the one in progress."""
    return next((period for period in periods if period.complete), periods[0] if periods else None)


def resolve_period(listing: dict[str, Any], basis: str, start_date: date | None) -> ObservedPeriod:
    if basis == "cycle" and not listing["cycle_available"]:
        raise ReportError(409, "cycle_unavailable")
    periods = listing["periods"]
    if start_date is None:
        period = default_period(periods)
    else:
        period = next((item for item in periods if local_start(item) == start_date), None)
    if period is None:
        raise ReportError(404, "period_not_found")
    return period


def _previous_window(listing: dict[str, Any], period: ObservedPeriod, basis: str, *, now: datetime) -> ObservedPeriod | None:
    """The period before `period` of the same basis; a month before tracking began still compares (no data)."""
    periods = listing["periods"]
    index = next(i for i, item in enumerate(periods) if item.start == period.start)
    if index + 1 < len(periods):
        return periods[index + 1]
    if basis == "month":
        start = local_start(period)
        year, month = shift_month(start.year, start.month, -1)
        return month_period(year, month, now=now, language=listing["language"])
    return None


def _figures(cur, user_id: str, start: datetime, end: datetime) -> dict[str, Any]:
    figures = window_figures(cur, user_id, start, end)
    return {key: figures[key] for key in FLOW_KEYS}


def _income_sources(cur, user_id: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    cur.execute(
        f"""SELECT t.category_id, c.name, SUM(t.amount) AS amount, COUNT(*) AS count
            FROM transactions t LEFT JOIN categories c ON c.id = t.category_id
            WHERE t.user_id = %s AND {reportable_income_sql()} AND t.date >= %s AND t.date <= %s
            GROUP BY t.category_id, c.name""",
        (user_id, start, end),
    )
    sources = [
        {
            "category_id": str(row["category_id"]) if row["category_id"] else None,
            "name": row["name"] or uncategorized_name(cur, user_id),
            "amount": int(row["amount"]), "count": int(row["count"]),
        }
        for row in cur.fetchall()
    ]
    sources.sort(key=lambda item: (-item["amount"], item["name"]))
    return sources


def _unexpected_items(cur, user_id: str, start: datetime, end: datetime) -> dict[str, Any]:
    condition = f"{reportable_expense_sql()} AND {pillar_bucket_sql()} = 'unexpected' AND t.date >= %s AND t.date <= %s"
    cur.execute(
        f"""SELECT t.id, t.date, t.amount, t.merchant, t.notes, c.name AS category_name
            FROM transactions t LEFT JOIN categories c ON c.id = t.category_id
            WHERE t.user_id = %s AND {condition}
            ORDER BY t.amount DESC, t.date DESC, t.id LIMIT %s""",
        (user_id, start, end, UNEXPECTED_ITEM_LIMIT),
    )
    items = [
        {
            "id": str(row["id"]), "date": row["date"].isoformat(), "amount": int(row["amount"]),
            "merchant": row["merchant"], "notes": row["notes"], "category_name": row["category_name"],
        }
        for row in cur.fetchall()
    ]
    cur.execute(
        f"SELECT COUNT(*) AS count FROM transactions t LEFT JOIN categories c ON c.id = t.category_id WHERE t.user_id = %s AND {condition}",
        (user_id, start, end),
    )
    return {"count": int(cur.fetchone()["count"]), "items": items}


def recurring_in_period(cur, user_id: str, period: ObservedPeriod, *, now: datetime) -> dict[str, Any]:
    as_of = min(now, period.last_instant)
    detected = recurring_charges(cur, user_id, now=as_of)
    first, last = local_start(period), local_end(period)
    charges = [
        {
            key: charge[key]
            for key in ("merchant", "search_text", "cadence", "typical_amount", "last_amount", "amount_changed",
                        "last_date", "monthly_equivalent", "existing_plan", "category_name")
        }
        for charge in detected["charges"]
        if first <= date.fromisoformat(charge["last_date"]) <= last
    ]
    return {"charges": charges, "monthly_equivalent_total": sum(item["monthly_equivalent"] for item in charges)}


def review_counts(cur, user_id: str, start: datetime | None = None, end: datetime | None = None) -> dict[str, int]:
    query, params = _REVIEW_COUNT_SQL, [user_id]
    if start is not None and end is not None:
        query += " AND t.date >= %s AND t.date <= %s"
        params += [start, end]
    cur.execute(query, params)
    row = cur.fetchone()
    # Second observations wait in Tinjau as automatic items, never as needing attention (OD117).
    return {"attention": int(row["attention"]),
            "unreviewed": int(row["unreviewed"]) + seen_twice_count(cur, user_id, start, end)}


def _data_quality(cur, user_id: str, period: ObservedPeriod, kakeibo: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    start, end = period.start, period.last_instant
    reviews = review_counts(cur, user_id, start, end)
    cur.execute(
        f"""SELECT COUNT(*) AS count FROM transactions t LEFT JOIN categories c ON c.id = t.category_id
            WHERE t.user_id = %s AND {reportable_expense_sql()} AND {pillar_bucket_sql()} = %s
              AND t.date >= %s AND t.date <= %s""",
        (user_id, UNCLASSIFIED, start, end),
    )
    unclassified_count = int(cur.fetchone()["count"])
    cur.execute(
        f"""SELECT COUNT(*) AS count FROM transactions t
            WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s AND {possible_duplicate_sql("t")}""",
        (user_id, start, end),
    )
    duplicates = int(cur.fetchone()["count"])
    cur.execute(
        """SELECT DISTINCT (t.date AT TIME ZONE %s)::date AS day FROM transactions t
           WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s""",
        (settings.tz, user_id, start, end),
    )
    recorded = {row["day"] for row in cur.fetchall()}
    today = now.astimezone(_zone()).date()
    day, last = local_start(period), min(local_end(period), today)
    empty_days = []
    while day <= last:
        if day not in recorded:
            empty_days.append(day.isoformat())
        day += timedelta(days=1)
    return {
        "pending_review": reviews["attention"] + reviews["unreviewed"],
        "needs_attention": reviews["attention"],
        "unreviewed": reviews["unreviewed"],
        "unclassified": {"count": unclassified_count, "amount": kakeibo["unclassified_spent"]},
        "possible_duplicates": duplicates,
        "days_without_records": {"count": len(empty_days), "dates": empty_days},
    }


def _budget(breakdown: list[dict[str, Any]], outflow: int, overall_budget: int | None, basis: str) -> dict[str, Any] | None:
    """Budget against actual, or None when the owner set no budget at all (budgets stay hidden)."""
    rows = [
        {
            "category_id": row["id"], "name": row["name"], "budget": int(row["monthly_budget"]),
            "spent": int(row["spent"]), "remaining": int(row["monthly_budget"]) - int(row["spent"]),
        }
        for row in breakdown
        if not row["is_uncategorized"] and not row["is_archived"] and (row["monthly_budget"] or 0) > 0
    ]
    overall = int(overall_budget) if overall_budget and overall_budget > 0 else None
    if not rows and overall is None:
        return None
    rows.sort(key=lambda item: (item["remaining"], item["name"]))
    return {
        # A salary cycle is compared with the monthly amount as is; the interface says so.
        "period_basis": basis,
        "categories": rows,
        "total_budget": sum(item["budget"] for item in rows),
        "total_spent": sum(item["spent"] for item in rows),
        "overall": None if overall is None else {"budget": overall, "spent": outflow, "remaining": overall - outflow},
    }


def load_reflection(cur, user_id: str, basis: str, start_date: date) -> dict[str, Any] | None:
    cur.execute(
        """SELECT note, period_start, period_end, updated_at FROM monthly_reflections
           WHERE user_id = %s AND basis = %s AND period_start = %s""",
        (user_id, basis, start_date),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "note": row["note"], "start_date": row["period_start"].isoformat(),
        "end_date": row["period_end"].isoformat(), "updated_at": row["updated_at"].isoformat(),
    }


def reflection_dates(cur, user_id: str, basis: str) -> set[date]:
    cur.execute("SELECT period_start FROM monthly_reflections WHERE user_id = %s AND basis = %s", (user_id, basis))
    return {row["period_start"] for row in cur.fetchall()}


def build_report(
    cur, user_id: str, *, basis: str, period: ObservedPeriod, listing: dict[str, Any],
    kakeibo_breakdown, overall_budget: int | None, essential: dict[str, Any], now: datetime | None = None,
) -> dict[str, Any]:
    """The four Kakeibo questions, comparison, data quality and budget for one period.

    `kakeibo_breakdown(cur, user_id, start, end, inflow, offsets)` is the dashboard's breakdown, passed in
    so this service reuses it without importing a router. `essential` is the current essential cost
    and coverage, labelled "saat ini" because no balance history exists.
    """
    now = now or datetime.now(timezone.utc)
    start, end = period.start, period.last_instant
    figures = window_figures(cur, user_id, start, end)
    offsets = figures["offsets"]
    kakeibo = kakeibo_breakdown(cur, user_id, start, end, figures["inflow"], offsets)
    breakdown = category_breakdown(cur, user_id, start, end, offsets)
    merchants = sorted(merchant_spending(cur, user_id, start, end).values(), key=lambda item: (-item["amount"], item["name"]))
    pillar_amounts = {
        "need": kakeibo["need_spent"], "want": kakeibo["want_spent"], "culture": kakeibo["culture_spent"],
        "unexpected": kakeibo["unexpected_spent"], "saving": kakeibo["saving_expenses"],
        UNCLASSIFIED: kakeibo["unclassified_spent"],
    }
    previous = _previous_window(listing, period, basis, now=now)
    recurring = recurring_in_period(cur, user_id, period, now=now)
    comparison = build_comparison(
        cur, user_id, basis=basis, period=period, listing=listing, now=now,
        kakeibo_breakdown=kakeibo_breakdown, recurring_in_period=lambda *_args, **_kwargs: recurring,
        merchants=merchants,
    )
    first_day = listing["first_day"]
    previous_reflection = None
    if previous is not None:
        previous_reflection = load_reflection(cur, user_id, basis, local_start(previous))
        if previous_reflection:
            previous_reflection["label"] = previous.label
    return {
        "basis": basis,
        "period": describe_period(period, basis, first_day),
        "flow": {key: figures[key] for key in FLOW_KEYS},
        "income": {"total": figures["inflow"], "sources": _income_sources(cur, user_id, start, end)},
        "savings": {
            "fresh_saving": kakeibo["saving_spent"],
            "saving_expenses": kakeibo["saving_expenses"],
            "net_saving_transfers": kakeibo["net_saving_transfers"],
            "surplus": figures["net"],
        },
        "spending": {
            "total": figures["outflow"],
            "pillars": [{"pillar": pillar, "amount": pillar_amounts[pillar]} for pillar in SPENDING_PILLARS],
            "top_categories": [
                {
                    "category_id": row["id"], "name": row["name"], "amount": row["spent"],
                    "is_uncategorized": row["is_uncategorized"], "kakeibo_type": row["kakeibo_type"],
                }
                for row in sorted((row for row in breakdown if row["spent"] > 0), key=lambda row: (-row["spent"], row["name"]))[:TOP_LIMIT]
            ],
            "top_merchants": [
                {"name": item["name"], "search_text": item["search_text"], "amount": item["amount"]} for item in merchants[:TOP_LIMIT]
            ],
            "unexpected": {"amount": kakeibo["unexpected_spent"], **_unexpected_items(cur, user_id, start, end)},
            "recurring": recurring,
            "essential": essential,
        },
        "comparison": comparison,
        "data_quality": _data_quality(cur, user_id, period, kakeibo, now=now),
        "budget": _budget(breakdown, figures["outflow"], overall_budget, basis),
        "reflection": load_reflection(cur, user_id, basis, local_start(period)),
        "previous_reflection": previous_reflection,
    }


def save_reflection(cur, user_id: str, basis: str, period: ObservedPeriod, note: str) -> dict[str, Any]:
    cur.execute(
        """INSERT INTO monthly_reflections (user_id, basis, period_start, period_end, note)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (user_id, basis, period_start)
           DO UPDATE SET note = EXCLUDED.note, period_end = EXCLUDED.period_end, updated_at = NOW()""",
        (user_id, basis, local_start(period), local_end(period), note),
    )
    return load_reflection(cur, user_id, basis, local_start(period))


def delete_reflection(cur, user_id: str, basis: str, start_date: date) -> bool:
    cur.execute(
        "DELETE FROM monthly_reflections WHERE user_id = %s AND basis = %s AND period_start = %s RETURNING id",
        (user_id, basis, start_date),
    )
    return cur.fetchone() is not None


def latest_readiness(cur, user_id: str, basis: str | None, *, now: datetime) -> dict[str, Any]:
    """The latest closed period's report for the phone reminder, with the pending Tinjau counts."""
    if basis is None:
        basis = "cycle" if list_periods(cur, user_id, "cycle", now=now)["cycle_available"] else "month"
    listing = list_periods(cur, user_id, basis, now=now)
    closed = next((period for period in listing["periods"] if period.complete), None)
    reviews = review_counts(cur, user_id)
    result: dict[str, Any] = {
        "basis": basis, "ready": closed is not None, "period": None, "url": None,
        "pending_review": reviews["attention"], "unreviewed": reviews["unreviewed"],
    }
    if closed is not None:
        described = closed.describe()
        result["period"] = {key: described[key] for key in ("label", "start", "end", "start_date", "end_date")}
        result["url"] = report_path(basis, local_start(closed))
    return result
