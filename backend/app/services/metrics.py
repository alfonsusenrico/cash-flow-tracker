"""
Financial health metric calculations.
All functions are pure — no DB access, fully unit-testable.
"""
from typing import Any


def _status(value: float, warn_threshold: float, critical_threshold: float, higher_is_better: bool = True) -> str:
    if higher_is_better:
        if value >= warn_threshold:
            return "ok"
        if value >= critical_threshold:
            return "warn"
        return "critical"
    else:
        if value <= warn_threshold:
            return "ok"
        if value <= critical_threshold:
            return "warn"
        return "critical"


def safe_to_spend(
    liquid_balance: int,
    committed_allocations: int,
    upcoming_fixed_expenses: int,
) -> dict[str, Any]:
    """
    How much can be spent today without disrupting plans.
    safe = liquid - committed_allocations - upcoming_fixed_expenses
    """
    value = max(0, liquid_balance - committed_allocations - upcoming_fixed_expenses)
    return {
        "value": value,
        "pct": None,
        "status": "ok" if value > 0 else "critical",
        "label": "Safe to Spend",
    }


def emergency_fund_coverage(
    emergency_balance: int,
    avg_monthly_expense: int,
) -> dict[str, Any]:
    """
    How many months of expenses the emergency fund covers.
    Target: >= 6 months. Warn: < 6. Critical: < 3.
    """
    if avg_monthly_expense <= 0:
        return {"value": None, "months": None, "status": "ok", "label": "Emergency Fund Coverage"}
    months = round(emergency_balance / avg_monthly_expense, 1)
    return {
        "value": emergency_balance,
        "months": months,
        "status": _status(months, 6.0, 3.0, higher_is_better=True),
        "label": "Emergency Fund Coverage",
    }


def savings_rate(total_in: int, total_out: int) -> dict[str, Any]:
    """
    (income - expenses) / income. Target: >= 20%. Warn: < 20%. Critical: < 0%.
    """
    if total_in <= 0:
        return {"value": None, "pct": None, "status": "ok", "label": "Savings Rate"}
    net = total_in - total_out
    pct = round(net / total_in * 100, 1)
    return {
        "value": net,
        "pct": pct,
        "status": _status(pct, 20.0, 0.0, higher_is_better=True),
        "label": "Savings Rate",
    }


def investment_rate(invested_this_month: int, total_in: int) -> dict[str, Any]:
    """
    Invested / income. Target: >= 10%. Warn: < 10%. Critical: 0%.
    """
    if total_in <= 0:
        return {"value": None, "pct": None, "status": "ok", "label": "Investment Rate"}
    pct = round(invested_this_month / total_in * 100, 1)
    return {
        "value": invested_this_month,
        "pct": pct,
        "status": _status(pct, 10.0, 0.0, higher_is_better=True),
        "label": "Investment Rate",
    }


def cash_runway(liquid_balance: int, avg_monthly_expense: int) -> dict[str, Any]:
    """
    How many months of expenses can be covered by current liquid balance.
    Target: >= 3 months. Warn: < 3. Critical: < 1.
    """
    if avg_monthly_expense <= 0:
        return {"value": None, "months": None, "days": None, "status": "warn", "label": "Cash Runway"}
    months = round(liquid_balance / avg_monthly_expense, 1)
    days = round(months * 30, 1)
    return {
        "value": liquid_balance,
        "months": months,
        "days": days,
        "status": _status(months, 3.0, 1.0, higher_is_better=True),
        "label": "Cash Runway",
    }


def monthly_drift(planned_spend: int, actual_spend: int) -> dict[str, Any]:
    """
    (actual - planned) / planned. Positive = overspend. Target: <= 5%. Warn: > 5%. Critical: > 20%.
    """
    if planned_spend <= 0:
        return {"value": None, "pct": None, "status": "ok", "label": "Monthly Drift"}
    drift = actual_spend - planned_spend
    pct = round(drift / planned_spend * 100, 1)
    return {
        "value": drift,
        "pct": pct,
        "status": _status(pct, 5.0, 20.0, higher_is_better=False),
        "label": "Monthly Drift",
    }


def goal_feasibility(
    goal_name: str,
    required_monthly: int | None,
    available_monthly: int,
) -> dict[str, Any]:
    """
    Is the required monthly contribution achievable given available savings?
    """
    if required_monthly is None:
        return {"goal": goal_name, "required": None, "available": available_monthly, "feasible": True, "status": "ok"}
    feasible = available_monthly >= required_monthly
    ratio = available_monthly / required_monthly if required_monthly > 0 else 1.0
    return {
        "goal": goal_name,
        "required": required_monthly,
        "available": available_monthly,
        "feasible": feasible,
        "status": "ok" if ratio >= 1.0 else "warn" if ratio >= 0.5 else "critical",
    }
