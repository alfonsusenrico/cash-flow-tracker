"""Pengeluaran per local day and per category for a window (apply-markas-analisis D31, owner decision 2026-11-24).

Pengeluaran is the figure of Beranda, Transaksi and the whole-period comparison: reportable expenses outside the
Tabungan pillar, each category and pillar net of its applied reimbursement and never below zero. Each local day,
however, is reported at the full price of its Pengeluaran rows, with no reimbursement subtracted: days add up to
`total + reimbursed`, and each day's pillars and categories add up to that day.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from app.core.config import settings
from app.services.kakeibo import REPORT_BUCKETS, pillar_bucket_sql
from app.services.observed_insights import merchant_spending
from app.services.reimbursements import SpendingOffsets
from app.services.report_figures import reportable_expense_sql
from app.services.saving_flow import saving_pillar_sql

# The pillar buckets Pengeluaran can hold: every report bucket but Tabungan, which is never Pengeluaran.
SPENDING_BUCKETS: tuple[str, ...] = tuple(bucket for bucket in REPORT_BUCKETS if bucket != "saving")


def spending_sql(tx: str = "t", category: str = "c") -> str:
    """A reportable expense that counts as Pengeluaran: not one with the Tabungan pillar."""
    return f"({reportable_expense_sql(tx, category)} AND NOT COALESCE({saving_pillar_sql(tx, category)}, FALSE))"


@dataclass
class SpendingBasis:
    total: int = 0
    # The reimbursements subtracted from Pengeluaran: category/pillar offsets plus period-level unlinked offsets.
    reimbursed: int = 0
    unlinked_reimbursed: int = 0
    by_day: dict[date, int] = field(default_factory=dict)
    # Category id as a string, or None for uncategorised rows.
    by_category: dict[str | None, int] = field(default_factory=dict)
    # Each day's Pengeluaran by pillar bucket and by category id (None for uncategorised); each adds up to `by_day`.
    by_day_pillar: dict[date, dict[str, int]] = field(default_factory=dict)
    by_day_category: dict[date, dict[str | None, int]] = field(default_factory=dict)

    def day(self, day: date) -> int:
        return self.by_day.get(day, 0)

    def category(self, category_id: Any) -> int:
        return self.by_category.get(str(category_id) if category_id is not None else None, 0)

    def day_pillars(self, day: date) -> dict[str, int]:
        """A day's Pengeluaran in every spending bucket, zero where it has none."""
        pillars = self.by_day_pillar.get(day, {})
        return {bucket: pillars.get(bucket, 0) for bucket in SPENDING_BUCKETS}

    def day_categories(self, day: date, names: dict[str | None, str]) -> list[dict[str, Any]]:
        """A day's full-price Pengeluaran per category, largest first, for every category with spending that day.

        `names` maps every category id of the window, and None for uncategorised rows, to its display name.
        """
        rows = [
            {"category_id": category_id, "name": names[category_id], "amount": amount}
            for category_id, amount in self.by_day_category.get(day, {}).items()
            if amount > 0
        ]
        return sorted(rows, key=lambda row: (-row["amount"], row["name"], row["category_id"] or ""))


def spending_basis(cur, user_id: str, start: datetime, end: datetime, offsets: SpendingOffsets) -> SpendingBasis:
    """Pengeluaran of `[start, end]`: full-price per local day (with pillars and categories) and net per category.

    One statement. The per-day splits come from the same full-price rows as `by_day`, so they add up to it exactly.
    """
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
        for day, amount in days.items():
            basis.by_day[day] = basis.by_day.get(day, 0) + amount
            day_pillars = basis.by_day_pillar.setdefault(day, {})
            day_pillars[pillar] = day_pillars.get(pillar, 0) + amount
            day_categories = basis.by_day_category.setdefault(day, {})
            day_categories[category_id] = day_categories.get(category_id, 0) + amount
        basis.by_category[category_id] = basis.by_category.get(category_id, 0) + gross - applied
        basis.total += gross - applied
        basis.reimbursed += applied
    unlinked = min(offsets.applied_unlinked_spending, basis.total)
    basis.total -= unlinked
    basis.reimbursed += unlinked
    basis.unlinked_reimbursed = unlinked
    return basis


def spending_merchants(cur, user_id: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Gross Pengeluaran per merchant, largest first, for "Tempat belanja terbesar" (apply-markas-laporan D29).

    The merchant rule of the report, without the expenses that have the Tabungan pillar.
    """
    merchants = merchant_spending(cur, user_id, start, end, expense_sql=spending_sql())
    return sorted(merchants.values(), key=lambda item: (-item["amount"], item["name"]))
