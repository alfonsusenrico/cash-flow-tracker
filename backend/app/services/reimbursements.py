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
from datetime import datetime
from typing import Any

from fastapi import HTTPException

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

    def for_category(self, category_id: Any) -> int:
        return self.applied_by_category.get(str(category_id), 0)

    def for_pillar(self, pillar: str) -> int:
        return self.applied_by_pillar.get(pillar, 0)

    def for_category_pillar(self, category_id: Any, pillar: str) -> int:
        return self.applied_by_category_pillar.get((str(category_id), pillar), 0)

    @property
    def living(self) -> int:
        """Applied offsets of spending pillars; saving is set aside, not spent."""
        return sum(amount for pillar, amount in self.applied_by_pillar.items() if pillar != "saving")

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
               NULL::text AS pillar, NULL::bigint AS amount
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
               COALESCE(e.kakeibo_type, c.kakeibo_type, 'unclassified'), SUM(l.amount)
        FROM reimbursement_links l
        JOIN transactions e ON e.id = l.expense_id AND e.user_id = l.user_id
        JOIN categories c ON c.id = e.category_id
        WHERE l.user_id = %(user_id)s AND l.amount > 0 AND e.date >= %(start)s AND e.date <= %(end)s
          AND NOT COALESCE(c.is_excluded_from_budget, FALSE) AND NOT ({non_reportable_sql("c")})
        GROUP BY e.category_id, COALESCE(e.kakeibo_type, c.kakeibo_type, 'unclassified')
        UNION ALL
        SELECT 'rest', NULL, NULL, NULL, NULL, NULL, NULL, COALESCE(SUM(t.amount - h.held), 0)
        FROM transactions t
        JOIN categories rc ON rc.id = t.category_id
        JOIN (SELECT reimbursement_id, SUM(amount) AS held FROM reimbursement_links
              WHERE user_id = %(user_id)s GROUP BY reimbursement_id) h ON h.reimbursement_id = t.id
        WHERE t.user_id = %(user_id)s AND t.type = 'income' AND rc.offsets_spending
          AND t.date >= %(start)s AND t.date <= %(end)s
        """,
        {"user_id": user_id, "start": start, "end": end},
    )
    rows = cur.fetchall()
    linked = {(str(row["offset_category_id"]), row["pillar"]): int(row["amount"]) for row in rows if row.get("part") == "linked"}
    fallback_rows = [row for row in rows if row.get("part", "fallback") == "fallback"]
    by_category: dict[str, int] = {}
    by_pillar: dict[str, int] = {}
    by_category_pillar: dict[tuple[str, str], int] = dict(linked)
    for (category_id, pillar), amount in linked.items():
        by_category[category_id] = by_category.get(category_id, 0) + amount
        by_pillar[pillar] = by_pillar.get(pillar, 0) + amount
    applied = sum(linked.values())
    excess = 0
    unallocated = sum(int(row["amount"] or 0) for row in rows if row.get("part") == "rest")
    for row in fallback_rows:
        reimbursed = int(row["reimbursed"] or 0)
        if row["offset_category_id"] is None:
            unallocated += reimbursed
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
    return SpendingOffsets(by_category, by_pillar, applied, excess, unallocated, by_category_pillar)


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
