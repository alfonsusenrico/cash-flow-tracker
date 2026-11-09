"""Pengeluaran per local day and per category for a window (apply-markas-analisis D31, owner decision 2026-11-08).

Pengeluaran is the figure of Beranda, Transaksi and the whole-period comparison: reportable expenses outside the
Tabungan pillar, each category and pillar net of its applied reimbursement and never below zero. The analytics,
the calendar and the flow return it beside their Belanja-basis fields, so every "Pengeluaran" on Analisis is one
figure. A category and pillar's reimbursement is spread over its days in proportion to their amounts, so the days
and the categories both add up to the total exactly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from app.core.config import settings
from app.services.apportionment import apportion
from app.services.kakeibo import pillar_bucket_sql
from app.services.observed_insights import merchant_spending
from app.services.reimbursements import SpendingOffsets
from app.services.report_figures import reportable_expense_sql
from app.services.saving_flow import saving_pillar_sql


def spending_sql(tx: str = "t", category: str = "c") -> str:
    """A reportable expense that counts as Pengeluaran: not one with the Tabungan pillar."""
    return f"({reportable_expense_sql(tx, category)} AND NOT COALESCE({saving_pillar_sql(tx, category)}, FALSE))"


@dataclass
class SpendingBasis:
    total: int = 0
    # The reimbursements subtracted from Pengeluaran: those applied to the spending pillars' categories.
    reimbursed: int = 0
    by_day: dict[date, int] = field(default_factory=dict)
    # Category id as a string, or None for uncategorised rows.
    by_category: dict[str | None, int] = field(default_factory=dict)

    def day(self, day: date) -> int:
        return self.by_day.get(day, 0)

    def category(self, category_id: Any) -> int:
        return self.by_category.get(str(category_id) if category_id is not None else None, 0)


def spending_basis(cur, user_id: str, start: datetime, end: datetime, offsets: SpendingOffsets) -> SpendingBasis:
    """Pengeluaran of `[start, end]` per local day and per category, from one statement."""
    cur.execute(
        f"""
        SELECT t.category_id, {pillar_bucket_sql()} AS pillar, (t.date AT TIME ZONE %s)::date AS day, SUM(t.amount) AS spent
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s AND {spending_sql()}
        GROUP BY t.category_id, pillar, day
        """,
        (settings.tz, user_id, start, end),
    )
    groups: dict[tuple[str | None, str], dict[date, int]] = {}
    for row in cur.fetchall():
        category_id = str(row["category_id"]) if row["category_id"] else None
        groups.setdefault((category_id, row["pillar"]), {})[row["day"]] = int(row["spent"])

    basis = SpendingBasis()
    for (category_id, pillar), days in sorted(groups.items(), key=lambda item: (item[0][0] or "", item[0][1])):
        gross = sum(days.values())
        # The comparison floors each category and pillar at zero, so at most its own spending is offset.
        applied = min(offsets.for_category_pillar(category_id, pillar), gross) if category_id else 0
        shares = apportion(applied, days)
        for day, amount in days.items():
            net = amount - shares.get(day, 0)
            basis.by_day[day] = basis.by_day.get(day, 0) + net
        basis.by_category[category_id] = basis.by_category.get(category_id, 0) + gross - applied
        basis.total += gross - applied
        basis.reimbursed += applied
    return basis


def spending_merchants(cur, user_id: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Gross Pengeluaran per merchant, largest first, for "Tempat belanja terbesar" (apply-markas-laporan D29).

    The merchant rule of the report, without the expenses that have the Tabungan pillar.
    """
    merchants = merchant_spending(cur, user_id, start, end, expense_sql=spending_sql())
    return sorted(merchants.values(), key=lambda item: (-item["amount"], item["name"]))
