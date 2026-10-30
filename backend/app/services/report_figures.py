"""Shared spending figures for reports: what counts as spending, the category breakdown and daily netting.

Every report total, breakdown and series uses `reportable_expense_sql`, so a category list adds up to
the total outflow and a daily series adds up to it too.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from app.core.config import settings
from app.services.apportionment import apportion
from app.services.reimbursements import NON_REPORTABLE_CATEGORY_NAMES, SpendingOffsets

UNCATEGORIZED_NAME = "Tanpa kategori"
UNCATEGORIZED_COLOR = "#64748b"

_NON_REPORTABLE_SQL = ", ".join(f"'{name}'" for name in NON_REPORTABLE_CATEGORY_NAMES)


def reportable_category_sql(category: str = "c") -> str:
    """True for a missing category or one that reports count as spending."""
    return (
        f"(COALESCE({category}.is_excluded_from_budget, FALSE) = FALSE"
        f" AND COALESCE({category}.name, '') NOT IN ({_NON_REPORTABLE_SQL}))"
    )


def reportable_expense_sql(tx: str = "t", category: str = "c") -> str:
    """An expense that is spending: not a movement, not an investment, not in an excluded category.

    Uncategorised expenses are spending; they are reported in the "Tanpa kategori" bucket.
    """
    return f"({tx}.type = 'expense' AND {reportable_category_sql(category)})"


def reportable_income_sql(tx: str = "t", category: str = "c") -> str:
    """Income that reports count: not a movement leg, not an investment, not a reimbursement."""
    return (
        f"({tx}.type = 'income' AND {reportable_category_sql(category)}"
        f" AND COALESCE({category}.offsets_spending, FALSE) = FALSE)"
    )


def cash_flow_totals(inflow: int, gross_outflow: int, reimbursement_offset: int) -> dict[str, int]:
    """Apply a scoped reimbursement offset to reportable inflow and outflow."""
    outflow = max(0, gross_outflow - reimbursement_offset)
    return {
        "inflow": inflow,
        "outflow": outflow,
        "net": inflow - outflow,
        "gross_outflow": gross_outflow,
        "reimbursement_offset": gross_outflow - outflow,
    }


def category_breakdown(cur, user_id: str, start: datetime, end: datetime, offsets: SpendingOffsets) -> list[dict[str, Any]]:
    """Spending per category in a window, net of applied reimbursements, summing to the total outflow.

    Active expense categories are always listed, archived categories only when they hold spending in
    the window, and uncategorised spending as a "Tanpa kategori" bucket with `id` None.
    """
    cur.execute(
        f"""
        SELECT c.id, c.name, c.icon, c.color, c.monthly_budget, c.is_primary, c.kakeibo_type, c.is_archived,
               COALESCE(SUM(t.amount), 0) AS spent
        FROM categories c
        LEFT JOIN transactions t ON t.category_id = c.id AND t.user_id = c.user_id
            AND t.type = 'expense' AND t.date >= %s AND t.date <= %s
        WHERE c.user_id = %s AND {reportable_category_sql("c")}
        GROUP BY c.id, c.name, c.icon, c.color, c.monthly_budget, c.is_primary, c.kakeibo_type, c.is_archived, c.kind
        HAVING (c.kind = 'expense' AND c.is_archived = FALSE) OR COALESCE(SUM(t.amount), 0) > 0
        ORDER BY spent DESC, c.name ASC
        """,
        (start, end, user_id),
    )
    rows = []
    for row in cur.fetchall():
        gross = int(row["spent"])
        net = gross - offsets.for_category(row["id"])
        rows.append({
            "id": str(row["id"]), "name": row["name"], "icon": row["icon"], "color": row["color"],
            "monthly_budget": row["monthly_budget"], "is_primary": bool(row.get("is_primary", True)),
            "kakeibo_type": row.get("kakeibo_type") or "need", "is_archived": bool(row["is_archived"]),
            "is_uncategorized": False, "gross_spent": gross, "spent": net, "reimbursed": gross - net,
        })
    cur.execute(
        """SELECT COALESCE(SUM(amount), 0) AS spent FROM transactions
           WHERE user_id = %s AND type = 'expense' AND category_id IS NULL AND date >= %s AND date <= %s""",
        (user_id, start, end),
    )
    uncategorized = int(cur.fetchone()["spent"] or 0)
    if uncategorized > 0:
        rows.append({
            "id": None, "name": UNCATEGORIZED_NAME, "icon": "tag", "color": UNCATEGORIZED_COLOR,
            "monthly_budget": None, "is_primary": True, "kakeibo_type": "need", "is_archived": False,
            "is_uncategorized": True, "gross_spent": uncategorized, "spent": uncategorized, "reimbursed": 0,
        })
        rows.sort(key=lambda item: (-item["spent"], item["name"]))
    return rows


def daily_offsets(cur, user_id: str, start: datetime, end: datetime, offsets: SpendingOffsets) -> dict[date, int]:
    """The applied reimbursement offsets of a window, attributed to local days.

    Each category's applied offset is spread over the days that category was spent on, in proportion
    to that day's spending in it, so no day goes negative and the days add up to the window's offset.
    """
    if not offsets.applied_by_category:
        return {}
    cur.execute(
        """SELECT t.category_id, (t.date AT TIME ZONE %s)::date AS day, SUM(t.amount) AS spent
           FROM transactions t
           WHERE t.user_id = %s AND t.type = 'expense' AND t.category_id = ANY(%s::uuid[])
             AND t.date >= %s AND t.date <= %s
           GROUP BY t.category_id, day""",
        (settings.tz, user_id, list(offsets.applied_by_category), start, end),
    )
    spending: dict[str, dict[date, int]] = {}
    for row in cur.fetchall():
        spending.setdefault(str(row["category_id"]), {})[row["day"]] = int(row["spent"])
    per_day: dict[date, int] = {}
    for category_id, applied in offsets.applied_by_category.items():
        for day, share in apportion(applied, spending.get(category_id, {})).items():
            per_day[day] = per_day.get(day, 0) + share
    return per_day


def sunday_first_weekday(day: date) -> int:
    """0 = Sunday … 6 = Saturday, as PostgreSQL's DOW."""
    return day.isoweekday() % 7
