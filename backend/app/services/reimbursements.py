"""Reimbursements: money a friend pays back for a shared expense.

A reimbursement is income in a category with `offsets_spending`. It is never income in reports.
Linked to the expenses it repays (`reimbursement_links`), its allocated parts lower each expense's
category and pillar in the window of the expense's date, whatever the reimbursement's date; the part
no link holds is reported as unallocated. Otherwise, when it names the expense category it repays
(`offset_category_id`), it lowers that category's spending in the same report window, floored at
zero after the linked part; the remainder is reported as excess. Without either it is reported as
unallocated and changes no spending figure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from fastapi import HTTPException

from app.core.config import settings
from app.errors import ErrorCode, api_error
from app.services.apportionment import apportion
from app.services.system_categories import non_reportable_sql, reportable_key_sql


def not_reimbursement_sql(category: str = "c") -> str:
    """SQL condition that is true unless the row's category offsets spending."""
    return f"COALESCE({category}.offsets_spending, FALSE) = FALSE"


@dataclass(frozen=True)
class SpendingOffsets:
    """Reimbursement effects on the spending of one report window."""

    applied_by_category: dict[str, int] = field(default_factory=dict)
    applied_by_pillar: dict[str, int] = field(default_factory=dict)
    applied: int = 0
    excess: int = 0
    unallocated: int = 0
    # The same apportionment, kept per (category id, pillar bucket), so a category's rows in one pillar
    # get exactly the share `applied_by_pillar` counted for them.
    applied_by_category_pillar: dict[tuple[str, str], int] = field(default_factory=dict)
    # The linked part of `applied_by_category_pillar`, per local day of the expenses it repays: a link lowers its
    # expense's own day (attribute-linked-reimbursements-to-their-day D1).
    linked_by_category_pillar_day: dict[tuple[str, str], dict[date, int]] = field(default_factory=dict)
    # Unlinked, untagged reimbursements applied at period-total level without altering any category or pillar.
    applied_unlinked: int = 0
    applied_unlinked_spending: int = 0

    def for_category(self, category_id: Any) -> int:
        return self.applied_by_category.get(str(category_id), 0)

    def for_pillar(self, pillar: str) -> int:
        return self.applied_by_pillar.get(pillar, 0)

    def for_category_pillar(self, category_id: Any, pillar: str) -> int:
        return self.applied_by_category_pillar.get((str(category_id), pillar), 0)

    def linked_days(self, category_id: Any, pillar: str | None = None) -> dict[date, int]:
        """The linked offset of a category, in one pillar or all, per local day of the expenses it repays."""
        days: dict[date, int] = {}
        for (linked_category, linked_pillar), amounts in self.linked_by_category_pillar_day.items():
            if linked_category != str(category_id) or (pillar is not None and linked_pillar != pillar):
                continue
            for day, amount in amounts.items():
                days[day] = days.get(day, 0) + amount
        return days

    @property
    def living(self) -> int:
        """Applied offsets of spending pillars plus period-level unlinked reimbursements; saving is set aside, not spent."""
        return sum(amount for pillar, amount in self.applied_by_pillar.items() if pillar != "saving") + self.applied_unlinked_spending

    def summary(self, gross_outflow: int) -> dict[str, int]:
        return {
            "gross_outflow": gross_outflow,
            "reimbursement_offset": self.applied,
            "reimbursement_unallocated": self.unallocated,
            "reimbursement_excess": self.excess,
        }


def load_spending_offsets(cur, user_id: str, start: datetime, end: datetime) -> SpendingOffsets:
    # One statement for the window (pages keep a fixed statement count): rows of three parts.
    # "fallback": reimbursements without links, by the category they name; "linked": linked parts by their
    # expense's category and pillar, for expenses dated in the window; "rest": what linked reimbursements dated in
    # the window hold on no expense (link-reimbursements-to-expenses D3).
    cur.execute(
        f"""
        SELECT 'fallback' AS part, r.offset_category_id, r.reimbursed, oc.kakeibo_type AS category_pillar,
               CASE
                   WHEN oc.id IS NULL
                        OR COALESCE(oc.is_excluded_from_budget, FALSE)
                        OR {non_reportable_sql("oc")}
                   THEN 0
                   ELSE COALESCE((
                       SELECT SUM(e.amount) FROM transactions e
                       WHERE e.user_id = r.user_id AND e.type = 'expense' AND e.category_id = oc.id
                         AND e.date >= %(start)s AND e.date <= %(end)s
                   ), 0)
               END AS gross_spent,
               COALESCE((
                   SELECT jsonb_object_agg(expense_pillars.pillar, expense_pillars.spent)
                   FROM (
                       SELECT COALESCE(e.kakeibo_type, oc.kakeibo_type, 'unclassified') AS pillar,
                              SUM(e.amount) AS spent
                       FROM transactions e
                       WHERE e.user_id = r.user_id AND e.type = 'expense' AND e.category_id = oc.id
                         AND e.date >= %(start)s AND e.date <= %(end)s
                       GROUP BY COALESCE(e.kakeibo_type, oc.kakeibo_type, 'unclassified')
                   ) expense_pillars
               ), '{{}}'::jsonb) AS pillar_spending,
               NULL::text AS pillar, NULL::bigint AS amount, NULL::date AS day
        FROM (
            SELECT t.user_id, t.offset_category_id, SUM(t.amount) AS reimbursed
            FROM transactions t
            JOIN categories rc ON rc.id = t.category_id
            WHERE t.user_id = %(user_id)s AND t.type = 'income' AND rc.offsets_spending
              AND t.date >= %(start)s AND t.date <= %(end)s
              AND NOT EXISTS (SELECT 1 FROM reimbursement_links l WHERE l.reimbursement_id = t.id)
            GROUP BY t.user_id, t.offset_category_id
        ) r
        LEFT JOIN categories oc ON oc.id = r.offset_category_id AND oc.user_id = r.user_id
        UNION ALL
        SELECT 'linked', e.category_id, NULL, NULL, NULL, NULL,
               COALESCE(e.kakeibo_type, c.kakeibo_type, 'unclassified'), SUM(l.amount),
               (e.date AT TIME ZONE %(tz)s)::date
        FROM reimbursement_links l
        JOIN transactions e ON e.id = l.expense_id AND e.user_id = l.user_id
        JOIN categories c ON c.id = e.category_id
        WHERE l.user_id = %(user_id)s AND l.amount > 0 AND e.date >= %(start)s AND e.date <= %(end)s
          AND NOT COALESCE(c.is_excluded_from_budget, FALSE) AND NOT ({non_reportable_sql("c")})
        GROUP BY e.category_id, COALESCE(e.kakeibo_type, c.kakeibo_type, 'unclassified'), (e.date AT TIME ZONE %(tz)s)::date
        UNION ALL
        SELECT 'rest', NULL, NULL, NULL, NULL, NULL, NULL, COALESCE(SUM(t.amount - h.held), 0), NULL
        FROM transactions t
        JOIN categories rc ON rc.id = t.category_id
        JOIN (SELECT reimbursement_id, SUM(amount) AS held FROM reimbursement_links
              WHERE user_id = %(user_id)s GROUP BY reimbursement_id) h ON h.reimbursement_id = t.id
        WHERE t.user_id = %(user_id)s AND t.type = 'income' AND rc.offsets_spending
          AND t.date >= %(start)s AND t.date <= %(end)s
        UNION ALL
        SELECT 'totals', NULL,
               COALESCE(SUM(t.amount), 0),
               NULL,
               COALESCE(SUM(t.amount) FILTER (WHERE COALESCE(t.kakeibo_type, c.kakeibo_type, '') <> 'saving'), 0),
               NULL, NULL, NULL, NULL
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %(user_id)s AND t.type = 'expense'
          AND COALESCE(c.is_excluded_from_budget, FALSE) = FALSE AND {reportable_key_sql("c")}
          AND t.date >= %(start)s AND t.date <= %(end)s
        """,
        {"user_id": user_id, "start": start, "end": end, "tz": settings.tz},
    )
    rows = cur.fetchall()
    linked: dict[tuple[str, str], int] = {}
    linked_by_day: dict[tuple[str, str], dict[date, int]] = {}
    for row in rows:
        if row.get("part") != "linked":
            continue
        key = (str(row["offset_category_id"]), row["pillar"])
        linked[key] = linked.get(key, 0) + int(row["amount"])
        if row.get("day") is not None:
            days = linked_by_day.setdefault(key, {})
            days[row["day"]] = days.get(row["day"], 0) + int(row["amount"])
    fallback_rows = [row for row in rows if row.get("part", "fallback") == "fallback"]
    totals_row = next((row for row in rows if row.get("part") == "totals"), None)
    total_gross_outflow = int(totals_row["reimbursed"] or 0) if totals_row else 0
    total_gross_spending = int(totals_row["gross_spent"] or 0) if totals_row else 0
    by_category: dict[str, int] = {}
    by_pillar: dict[str, int] = {}
    by_category_pillar: dict[tuple[str, str], int] = dict(linked)
    for (category_id, pillar), amount in linked.items():
        by_category[category_id] = by_category.get(category_id, 0) + amount
        by_pillar[pillar] = by_pillar.get(pillar, 0) + amount
    applied = sum(linked.values())
    excess = 0
    unallocated = sum(int(row["amount"] or 0) for row in rows if row.get("part") == "rest")
    unlinked_raw = 0
    for row in fallback_rows:
        reimbursed = int(row["reimbursed"] or 0)
        if row["offset_category_id"] is None:
            unlinked_raw += reimbursed
            continue
        category_id = str(row["offset_category_id"])
        # The category link only reaches what the linked parts left of the category (D3).
        room = max(0, int(row["gross_spent"] or 0) - by_category.get(category_id, 0))
        category_applied = min(room, reimbursed)
        excess += reimbursed - category_applied
        if category_applied <= 0:
            continue
        applied += category_applied
        by_category[category_id] = by_category.get(category_id, 0) + category_applied
        pillar_spending = {
            pillar: max(0, int(amount) - linked.get((category_id, pillar), 0))
            for pillar, amount in row["pillar_spending"].items()
        }
        for pillar, share in apportion(category_applied, pillar_spending).items():
            by_pillar[pillar] = by_pillar.get(pillar, 0) + share
            by_category_pillar[(category_id, pillar)] = by_category_pillar.get((category_id, pillar), 0) + share
    living_category_applied = sum(amount for pillar, amount in by_pillar.items() if pillar != "saving")
    applied_unlinked = min(unlinked_raw, max(0, total_gross_outflow - applied))
    applied_unlinked_spending = min(unlinked_raw, max(0, total_gross_spending - living_category_applied))
    excess += unlinked_raw - applied_unlinked
    applied += applied_unlinked
    return SpendingOffsets(
        by_category, by_pillar, applied, excess, unallocated, by_category_pillar, linked_by_day,
        applied_unlinked, applied_unlinked_spending,
    )


def category_offsets_spending(cur, user_id: str, category_id: str | None) -> bool:
    if not category_id:
        return False
    cur.execute(
        "SELECT offsets_spending FROM categories WHERE user_id = %s AND id = %s",
        (user_id, category_id),
    )
    row = cur.fetchone()
    return bool(row and row["offsets_spending"])


def validate_offset_category(cur, user_id: str, offset_category_id: str) -> None:
    """An offset must name an active, budget-reportable expense category of the same owner.

    Reports leave the movement and investment categories out by key even where the excluded flag was
    never set, so they cannot be offset either.
    """
    cur.execute(
        f"""
        SELECT 1 FROM categories
        WHERE user_id = %s AND id = %s AND kind = 'expense' AND is_archived = FALSE
          AND COALESCE(is_excluded_from_budget, FALSE) = FALSE
          AND {reportable_key_sql("categories")}
        """,
        (user_id, offset_category_id),
    )
    if not cur.fetchone():
        raise api_error(422, ErrorCode.INVALID_OFFSET_CATEGORY)


def offset_not_allowed() -> HTTPException:
    return api_error(422, ErrorCode.OFFSET_NOT_ALLOWED)


def offset_day_shares(applied: int, days: dict[date, int], linked: dict[date, int]) -> dict[date, int]:
    """How much of `applied` each day carries: its linked amounts on their own day, then the rest of the offset
    (unlinked reimbursements naming the category, or what links did not reach) spread over what each day has left.

    A day never carries more than it spent, and the shares add up to `applied` whenever `applied` is at most the
    days' spending (attribute-linked-reimbursements-to-their-day D1, D2).
    """
    on_day = {day: min(amount, days.get(day, 0)) for day, amount in linked.items() if day in days and amount > 0}
    if sum(on_day.values()) > applied:
        # Links larger than the applied offset cannot happen while links are capped at their expense; keep the
        # proportional spread rather than over-subtracting.
        return apportion(applied, days)
    left = {day: amount - on_day.get(day, 0) for day, amount in days.items()}
    shares = dict(on_day)
    for day, share in apportion(applied - sum(on_day.values()), left).items():
        shares[day] = shares.get(day, 0) + share
    return shares
