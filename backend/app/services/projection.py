"""
PMT-style projection math for financial goals.

All functions are pure — no DB access, fully unit-testable.
"""
import math
from datetime import date


def months_until(target_date: date) -> int:
    """Number of full months from today to target_date. Minimum 1."""
    today = date.today()
    months = (target_date.year - today.year) * 12 + (target_date.month - today.month)
    return max(1, months)


def inflation_adjusted_target(target_amount: int, inflation_rate: float, months: int) -> int:
    """
    Adjust target_amount for inflation over `months` months.
    inflation_rate is annual (e.g. 0.05 = 5%).
    """
    monthly_inflation = (1 + inflation_rate) ** (1 / 12) - 1
    return int(math.ceil(target_amount * (1 + monthly_inflation) ** months))


def required_monthly_contribution(
    target_amount: int,
    current_amount: int,
    months: int,
    expected_return: float,
) -> int:
    """
    PMT formula: how much to contribute each month to reach target_amount
    in `months` months, given current_amount already saved and
    expected_return annual rate.

    Returns 0 if already funded.
    """
    if current_amount >= target_amount:
        return 0
    if months <= 0:
        return max(0, target_amount - current_amount)

    remaining = target_amount - current_amount
    monthly_rate = (1 + expected_return) ** (1 / 12) - 1

    if monthly_rate == 0:
        return int(math.ceil(remaining / months))

    # PMT = PV * r / (1 - (1+r)^-n)  where PV = remaining, r = monthly_rate, n = months
    pmt = remaining * monthly_rate / (1 - (1 + monthly_rate) ** (-months))
    return int(math.ceil(pmt))


def goal_projection(
    target_amount: int,
    current_amount: int,
    target_date: date | None,
    inflation_rate: float,
    expected_return: float,
    monthly_contribution: int = 0,
) -> dict:
    """
    Full projection for a goal.

    Returns:
    - inflation_adjusted_target: target in today's money adjusted for inflation
    - months_remaining: months until target_date (None if no date)
    - required_monthly: required monthly contribution to hit target on time
    - progress_pct: current_amount / target as percentage
    - eta_months: estimated months to reach target at current monthly_contribution
    - feasible: True if required_monthly <= monthly_contribution (or no date set)
    - shortfall_per_month: how much more per month is needed
    """
    months = months_until(target_date) if target_date else None
    adj_target = inflation_adjusted_target(target_amount, inflation_rate, months or 0)
    progress_pct = round(current_amount / adj_target * 100, 1) if adj_target > 0 else 0

    required = required_monthly_contribution(adj_target, current_amount, months or 1, expected_return) if months else None

    # ETA at current contribution rate
    eta_months: int | None = None
    if monthly_contribution > 0 and current_amount < adj_target:
        monthly_rate = (1 + expected_return) ** (1 / 12) - 1
        remaining = adj_target - current_amount
        if monthly_rate == 0:
            eta_months = int(math.ceil(remaining / monthly_contribution))
        else:
            # n = log(1 + PV*r/PMT) / log(1+r)
            try:
                eta_months = int(math.ceil(
                    math.log(1 + remaining * monthly_rate / monthly_contribution) / math.log(1 + monthly_rate)
                ))
            except (ValueError, ZeroDivisionError):
                eta_months = None
    elif current_amount >= adj_target:
        eta_months = 0

    feasible = True
    shortfall = 0
    if required is not None and monthly_contribution > 0:
        feasible = monthly_contribution >= required
        shortfall = max(0, required - monthly_contribution)

    return {
        "target_amount": target_amount,
        "inflation_adjusted_target": adj_target,
        "current_amount": current_amount,
        "progress_pct": progress_pct,
        "months_remaining": months,
        "required_monthly": required,
        "eta_months": eta_months,
        "feasible": feasible,
        "shortfall_per_month": shortfall,
    }
