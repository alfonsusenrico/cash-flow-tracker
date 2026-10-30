"""Essential monthly cost ("Biaya hidup pokok bulanan") and emergency coverage ("Ketahanan Dana").

The cost is the median of observed `need` spending per completed period plus the active debt minimums
once. Debt-linked expenses are left out of the periods so a debt payment is never counted twice.
Unknown inputs stay `None`; no sentinel stands in for them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.services.apportionment import apportion
from app.services.observed_periods import COMPLETED_PERIOD_LIMIT, ObservedPeriod, load_period_history
from app.services.reimbursements import load_spending_offsets
from app.services.report_figures import reportable_category_sql

LIMITED_HISTORY_PERIODS = 3
# Average days per month; turns months of coverage into days.
DAYS_PER_MONTH = 30.4
DEFAULT_TARGET_MONTHS = 6


@dataclass(frozen=True)
class EssentialCost:
    amount: int | None
    living_amount: int | None
    debt_minimum: int
    method: str  # "median_need_periods", "need_budget" or "unknown"
    periods: list[dict[str, Any]] = field(default_factory=list)
    cycle_status: str = "not_configured"

    @property
    def limited_history(self) -> bool:
        return self.method == "median_need_periods" and len(self.periods) < LIMITED_HISTORY_PERIODS

    def describe(self) -> dict[str, Any]:
        return {
            "amount": self.amount,
            "living_amount": self.living_amount,
            "debt_minimum": self.debt_minimum,
            "method": self.method,
            "basis": {"median_need_periods": "history", "need_budget": "budget"}.get(self.method, "unknown"),
            "period_count": len(self.periods),
            "periods_used": self.periods,
            "limited_history": self.limited_history,
            "cycle_status": self.cycle_status,
        }


def median_rupiah(values: list[int]) -> int:
    """Median in whole rupiah; an even count averages the middle pair, rounding half up."""
    if not values:
        raise ValueError("median of no values")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle] + 1) // 2


def period_need_spending(cur, user_id: str, period: ObservedPeriod) -> dict[str, int]:
    """Essential `need` spending of one period, net of its share of applied reimbursements.

    Each category's applied reimbursement is apportioned across all its expenses in the period by
    effective pillar and by whether the expense counts here, so excluded rows keep their own share.
    """
    start, last = period.start, period.last_instant
    cur.execute(
        f"""
        SELECT t.category_id,
               COALESCE(t.kakeibo_type, c.kakeibo_type, 'need') AS pillar,
               ({reportable_category_sql('c')}
                AND t.movement_id IS NULL
                AND a.type <> 'investment'
                AND t.obligation_id IS NULL
                AND NOT EXISTS (SELECT 1 FROM transaction_obligation_allocations x WHERE x.transaction_id = t.id)
               ) AS counted,
               SUM(t.amount) AS spent
        FROM transactions t
        JOIN accounts a ON a.id = t.account_id
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s AND t.type = 'expense' AND t.date >= %s AND t.date <= %s
        GROUP BY t.category_id, pillar, counted
        """,
        (user_id, start, last),
    )
    gross = 0
    weights_by_category: dict[str, dict[tuple[str, bool], int]] = {}
    for row in cur.fetchall():
        spent = int(row["spent"] or 0)
        counted = bool(row["counted"])
        if counted and row["pillar"] == "need":
            gross += spent
        if row["category_id"] is not None:
            weights = weights_by_category.setdefault(str(row["category_id"]), {})
            key = (row["pillar"], counted)
            weights[key] = weights.get(key, 0) + spent
    offsets = load_spending_offsets(cur, user_id, start, last)
    offset = 0
    for category_id, applied in offsets.applied_by_category.items():
        offset += apportion(applied, weights_by_category.get(category_id, {})).get(("need", True), 0)
    return {"gross_need": gross, "reimbursement_offset": offset, "amount": max(0, gross - offset)}


def debt_minimum_total(cur, user_id: str) -> int:
    """Active debts' monthly minimums; a final payment smaller than the minimum counts as itself."""
    cur.execute(
        """
        SELECT COALESCE(SUM(LEAST(COALESCE(minimum_payment, 0), remaining_amount)), 0) AS total
        FROM obligations
        WHERE user_id = %s AND is_archived = FALSE AND remaining_amount > 0
        """,
        (user_id,),
    )
    row = cur.fetchone()
    return int(row["total"] or 0) if row else 0


def need_budget_total(cur, user_id: str) -> int:
    cur.execute(
        f"""
        SELECT COALESCE(SUM(c.monthly_budget), 0) AS total
        FROM categories c
        WHERE c.user_id = %s AND c.kind = 'expense' AND c.is_archived = FALSE
          AND COALESCE(c.kakeibo_type, 'need') = 'need' AND {reportable_category_sql('c')}
        """,
        (user_id,),
    )
    row = cur.fetchone()
    return int(row["total"] or 0) if row else 0


def estimate_essential_cost(cur, user_id: str, *, now: datetime | None = None) -> EssentialCost:
    now = now or datetime.now(timezone.utc)
    history = load_period_history(cur, user_id, now=now, limit=COMPLETED_PERIOD_LIMIT)
    debt_minimum = debt_minimum_total(cur, user_id)
    periods = []
    for period in history.completed:
        figures = period_need_spending(cur, user_id, period)
        periods.append({**period.describe(), **figures})
    if periods:
        living = median_rupiah([item["amount"] for item in periods])
        return EssentialCost(living + debt_minimum, living, debt_minimum, "median_need_periods", periods, history.cycle_status)
    budget = need_budget_total(cur, user_id)
    if budget > 0:
        return EssentialCost(budget + debt_minimum, budget, debt_minimum, "need_budget", [], history.cycle_status)
    return EssentialCost(None, None, debt_minimum, "unknown", [], history.cycle_status)


def coverage(balance: int, essential: EssentialCost, target_months: int | float) -> dict[str, Any]:
    """Months and days of essential cost the balance covers; `None` whenever the cost is unknown or zero."""
    target_months = float(target_months or DEFAULT_TARGET_MONTHS)
    cost = essential.amount
    if cost is None or cost <= 0:
        return {
            "coverage_months": None, "runway_days": None, "target_amount": None, "coverage_pct": None,
            "daily_burn_rate": None, "status": "unknown",
            "coverage_unavailable_reason": "essential_cost_unknown" if cost is None else "essential_cost_zero",
        }
    target_amount = int(round(target_months * cost))
    if balance <= 0:
        months, days, status = 0.0, 0, "zero"
    else:
        months = round(balance / cost, 1)
        days = int(round(balance * DAYS_PER_MONTH / cost))
        if months >= target_months:
            status = "healthy"
        elif months >= target_months / 2:
            status = "moderate"
        else:
            status = "critical"
    return {
        "coverage_months": months,
        "runway_days": days,
        "target_amount": target_amount,
        "coverage_pct": round(max(0, balance) / target_amount * 100, 1),
        "daily_burn_rate": int(round(cost / DAYS_PER_MONTH)),
        "status": status,
        "coverage_unavailable_reason": None,
    }
