"""Reimbursements: money a friend pays back for a shared expense.

A reimbursement is income in a category with `offsets_spending`. It is never income in reports.
When it names the expense category it repays (`offset_category_id`), it lowers that category's
spending in the same report window, floored at zero; the remainder is reported as excess. Without
an offset category it is reported as unallocated and changes no spending figure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from fastapi import HTTPException

from app.services.kakeibo import effective_pillar

REIMBURSEMENT_CATEGORY_NAME = "Patungan & Ganti Uang"
# Categories that report totals already leave out by name.
NON_REPORTABLE_CATEGORY_NAMES = ("Internal Movement", "Investasi")


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

    def for_category(self, category_id: Any) -> int:
        return self.applied_by_category.get(str(category_id), 0)

    def for_pillar(self, pillar: str) -> int:
        return self.applied_by_pillar.get(pillar, 0)

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
    cur.execute(
        """
        SELECT r.offset_category_id, r.reimbursed, oc.kakeibo_type AS category_pillar,
               CASE
                   WHEN oc.id IS NULL
                        OR COALESCE(oc.is_excluded_from_budget, FALSE)
                        OR oc.name = ANY(%s)
                   THEN 0
                   ELSE COALESCE((
                       SELECT SUM(e.amount) FROM transactions e
                       WHERE e.user_id = r.user_id AND e.type = 'expense' AND e.category_id = oc.id
                         AND e.date >= %s AND e.date <= %s
                   ), 0)
               END AS gross_spent
        FROM (
            SELECT t.user_id, t.offset_category_id, SUM(t.amount) AS reimbursed
            FROM transactions t
            JOIN categories rc ON rc.id = t.category_id
            WHERE t.user_id = %s AND t.type = 'income' AND rc.offsets_spending
              AND t.date >= %s AND t.date <= %s
            GROUP BY t.user_id, t.offset_category_id
        ) r
        LEFT JOIN categories oc ON oc.id = r.offset_category_id AND oc.user_id = r.user_id
        """,
        (list(NON_REPORTABLE_CATEGORY_NAMES), start, end, user_id, start, end),
    )
    by_category: dict[str, int] = {}
    by_pillar: dict[str, int] = {}
    applied = excess = unallocated = 0
    for row in cur.fetchall():
        reimbursed = int(row["reimbursed"] or 0)
        if row["offset_category_id"] is None:
            unallocated += reimbursed
            continue
        category_applied = min(int(row["gross_spent"] or 0), reimbursed)
        excess += reimbursed - category_applied
        if category_applied <= 0:
            continue
        applied += category_applied
        by_category[str(row["offset_category_id"])] = category_applied
        pillar = effective_pillar("expense", None, row["category_pillar"])
        by_pillar[pillar] = by_pillar.get(pillar, 0) + category_applied
    return SpendingOffsets(by_category, by_pillar, applied, excess, unallocated)


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

    Reports leave "Internal Movement" and "Investasi" out by name even where the excluded flag was
    never set, so they cannot be offset either.
    """
    cur.execute(
        """
        SELECT 1 FROM categories
        WHERE user_id = %s AND id = %s AND kind = 'expense' AND is_archived = FALSE
          AND COALESCE(is_excluded_from_budget, FALSE) = FALSE
          AND NOT (name = ANY(%s))
        """,
        (user_id, offset_category_id, list(NON_REPORTABLE_CATEGORY_NAMES)),
    )
    if not cur.fetchone():
        raise HTTPException(
            status_code=422,
            detail={
                "code": "invalid_offset_category",
                "message": "Pilih kategori pengeluaran aktif milik Anda sebagai kategori yang dikurangi.",
            },
        )


def offset_not_allowed() -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={
            "code": "offset_not_allowed",
            "message": "Hanya pemasukan dalam kategori ganti uang yang dapat mengurangi pengeluaran.",
        },
    )
