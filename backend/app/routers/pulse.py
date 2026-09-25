import calendar
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends

from app.core.config import settings
from app.db.pool import db_conn
from app.routers.accounts import get_accounts_with_balances, summarize_account_balances
from app.services.auth import get_current_user

router = APIRouter(tags=["Pulse & Insights"])


def get_cycle_window(payday_day: int, ref_date: date | None = None) -> tuple[datetime, datetime, int, int]:
    """
    Returns (cycle_start, cycle_end, elapsed_days, total_days) in UTC.
    The cycle runs from payday_day of previous/current month up to (payday_day - 1) of next month.
    Supports payday_day from 1 to 31 with month-aware clamping.
    """
    tz = ZoneInfo(settings.tz)
    today = ref_date or datetime.now(tz).date()

    target_payday = max(1, min(payday_day, 31))
    max_current = calendar.monthrange(today.year, today.month)[1]
    actual_payday_this_month = min(target_payday, max_current)

    if today.day >= actual_payday_this_month:
        # Started this month
        start_date = date(today.year, today.month, actual_payday_this_month)
        if target_payday == 1:
            end_date = date(today.year, today.month, max_current)
        else:
            if today.month == 12:
                next_month_year = today.year + 1
                next_month = 1
            else:
                next_month_year = today.year
                next_month = today.month + 1
            max_next = calendar.monthrange(next_month_year, next_month)[1]
            actual_payday_next_month = min(target_payday, max_next)
            target_end_day = actual_payday_next_month - 1
            end_date = date(next_month_year, next_month, target_end_day)
    else:
        # Started previous month
        if today.month == 1:
            prev_month_year = today.year - 1
            prev_month = 12
        else:
            prev_month_year = today.year
            prev_month = today.month - 1

        max_prev = calendar.monthrange(prev_month_year, prev_month)[1]
        actual_start_day = min(target_payday, max_prev)
        start_date = date(prev_month_year, prev_month, actual_start_day)

        target_end_day = actual_payday_this_month - 1 if target_payday > 1 else max_prev
        end_date = date(today.year, today.month, target_end_day)

    cycle_start = datetime.combine(start_date, time.min, tzinfo=tz).astimezone(timezone.utc)
    cycle_end = datetime.combine(end_date, time.max, tzinfo=tz).astimezone(timezone.utc)

    total_days = (end_date - start_date).days + 1
    elapsed_days = max(1, (today - start_date).days + 1)

    return cycle_start, cycle_end, elapsed_days, total_days


@router.get("/pulse")
def get_pulse(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    payday_day = current_user.get("payday_day") or 25
    tz = ZoneInfo(settings.tz)
    now_local = datetime.now(tz)
    today_start = datetime.combine(now_local.date(), time.min, tzinfo=tz).astimezone(timezone.utc)
    today_end = datetime.combine(now_local.date(), time.max, tzinfo=tz).astimezone(timezone.utc)

    cycle_start, cycle_end, elapsed_days, total_days = get_cycle_window(payday_day, now_local.date())
    remaining_days = max(1, total_days - elapsed_days + 1)

    with db_conn() as conn:
        with conn.cursor() as cur:
            # 1. Accounts & Liquid Balances
            accs = get_accounts_with_balances(user_id, include_archived=False)
            balance_summary = summarize_account_balances(accs)
            liquid_balance = balance_summary["liquid_balance"]
            investment_balance = balance_summary["investment_balance"]
            total_balance = balance_summary["total_balance"]
            reconciliation_pending = balance_summary["reconciliation_pending"]

            # 2. Today's spent (living expenses only: excludes internal movement & investments)
            cur.execute(
                """
                SELECT COALESCE(SUM(t.amount), 0) AS today_spent
                FROM transactions t
                LEFT JOIN categories c ON t.category_id = c.id
                WHERE t.user_id = %s
                  AND t.type = 'expense'
                  AND (c.is_excluded_from_budget IS FALSE OR c.is_excluded_from_budget IS NULL)
                  AND (c.kakeibo_type IS NULL OR c.kakeibo_type IN ('need', 'want'))
                  AND (c.name IS NULL OR c.name NOT IN ('Internal Movement', 'Investasi'))
                  AND t.date >= %s AND t.date <= %s
                """,
                (user_id, today_start, today_end),
            )
            today_spent = int(cur.fetchone()["today_spent"] or 0)

            # 3. Cycle spending & income (living expenses only for cycle_spent)
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(CASE
                        WHEN t.type = 'expense'
                             AND (c.is_excluded_from_budget IS FALSE OR c.is_excluded_from_budget IS NULL)
                             AND (c.kakeibo_type IS NULL OR c.kakeibo_type IN ('need', 'want'))
                             AND (c.name IS NULL OR c.name NOT IN ('Internal Movement', 'Investasi'))
                        THEN t.amount ELSE 0 END), 0) AS cycle_spent,
                    COALESCE(SUM(CASE
                        WHEN t.type = 'income'
                             AND (c.is_excluded_from_budget IS FALSE OR c.is_excluded_from_budget IS NULL)
                             AND (c.name IS NULL OR c.name NOT IN ('Internal Movement', 'Investasi'))
                        THEN t.amount ELSE 0 END), 0) AS cycle_income
                FROM transactions t
                LEFT JOIN categories c ON t.category_id = c.id
                WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
                """,
                (user_id, cycle_start, cycle_end),
            )
            cycle_tx = cur.fetchone()
            cycle_spent = int(cycle_tx["cycle_spent"] or 0)
            cycle_income = int(cycle_tx["cycle_income"] or 0)

            # 4. Total monthly budget ceiling from user settings or categories
            monthly_spending_budget = current_user.get("monthly_spending_budget")
            cur.execute(
                """
                SELECT COALESCE(SUM(monthly_budget), 0) AS total_budget
                FROM categories
                WHERE user_id = %s AND kind = 'expense' AND is_archived = FALSE AND monthly_budget > 0
                """,
                (user_id,),
            )
            cat_budget = int(cur.fetchone()["total_budget"] or 0)
            effective_budget = monthly_spending_budget if (monthly_spending_budget and monthly_spending_budget > 0) else cat_budget

            if effective_budget > 0:
                remaining_budget = max(0, effective_budget - cycle_spent)
                safe_to_spend_today = remaining_budget // remaining_days
            else:
                safe_to_spend_today = 0
                remaining_budget = 0

            # 5. Today's transactions
            cur.execute(
                """
                SELECT
                    t.id, t.type, t.amount, t.notes, t.date,
                    sa.name AS account_name,
                    c.name AS category_name, c.icon AS category_icon, c.color AS category_color
                FROM transactions t
                JOIN accounts sa ON sa.id = t.account_id
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
                ORDER BY t.date DESC, t.created_at DESC
                """,
                (user_id, today_start, today_end),
            )
            today_rows = cur.fetchall()

            # 6. Recent category chips (top 5 most used categories in last 30 days)
            cur.execute(
                """
                SELECT c.id, c.name, c.icon, c.color, COUNT(t.id) AS usage_count
                FROM categories c
                JOIN transactions t ON t.category_id = c.id
                WHERE c.user_id = %s AND c.kind = 'expense' AND c.is_archived = FALSE
                GROUP BY c.id, c.name, c.icon, c.color
                ORDER BY usage_count DESC
                LIMIT 6
                """,
                (user_id,),
            )
            recent_cats = cur.fetchall()

    # Pace status
    time_elapsed_pct = (elapsed_days / total_days) * 100
    budget_spent_pct = (cycle_spent / effective_budget * 100) if effective_budget > 0 else 0
    if budget_spent_pct <= time_elapsed_pct + 5:
        pace_status = "on_track"
    elif budget_spent_pct <= time_elapsed_pct + 15:
        pace_status = "warning"
    else:
        pace_status = "over_budget"

    return {
        "ok": True,
        "currency": current_user.get("currency", "IDR"),
        "total_liquid_balance": liquid_balance,
        "investment_balance": investment_balance,
        "total_net_worth": total_balance,
        "reconciliation_pending": reconciliation_pending,
        "today_spent": today_spent,
        "safe_to_spend_today": safe_to_spend_today,
        "cycle_spent": cycle_spent,
        "cycle_total_budget": effective_budget,
        "cycle_remaining": remaining_budget,
        "cycle_days_total": total_days,
        "cycle_day_current": elapsed_days,
        "cycle_days_remaining": remaining_days,
        "pace_status": pace_status,
        "today_transactions": [
            {
                "id": str(r["id"]),
                "type": r["type"],
                "amount": r["amount"],
                "notes": r["notes"],
                "date": r["date"].isoformat(),
                "account_name": r["account_name"],
                "category_name": r["category_name"],
                "category_icon": r["category_icon"],
                "category_color": r["category_color"],
                "transfer_target_name": None,
            }
            for r in today_rows
        ],
        "recent_categories": [
            {
                "id": str(c["id"]),
                "name": c["name"],
                "icon": c["icon"],
                "color": c["color"],
            }
            for c in recent_cats
        ],
    }


@router.get("/insights")
def get_insights(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    payday_day = current_user.get("payday_day") or 25
    tz = ZoneInfo(settings.tz)
    now_local = datetime.now(tz)

    cycle_start, cycle_end, elapsed_days, total_days = get_cycle_window(payday_day, now_local.date())

    with db_conn() as conn:
        with conn.cursor() as cur:
            # 1. Total in & out (excluding internal movement & invest)
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(CASE
                        WHEN t.type = 'income'
                             AND (c.is_excluded_from_budget IS FALSE OR c.is_excluded_from_budget IS NULL)
                             AND (c.name IS NULL OR c.name NOT IN ('Internal Movement', 'Investasi'))
                        THEN t.amount ELSE 0 END), 0) AS total_income,
                    COALESCE(SUM(CASE
                        WHEN t.type = 'expense'
                             AND (c.is_excluded_from_budget IS FALSE OR c.is_excluded_from_budget IS NULL)
                             AND (c.name IS NULL OR c.name NOT IN ('Internal Movement', 'Investasi'))
                        THEN t.amount ELSE 0 END), 0) AS total_expense
                FROM transactions t
                LEFT JOIN categories c ON t.category_id = c.id
                WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
                """,
                (user_id, cycle_start, cycle_end),
            )
            totals = cur.fetchone()
            total_income = int(totals["total_income"] or 0)
            total_expense = int(totals["total_expense"] or 0)

            # 2. Category spending vs budgets
            cur.execute(
                """
                SELECT
                    c.id, c.name, c.icon, c.color, c.monthly_budget,
                    COALESCE(SUM(t.amount), 0) AS spent
                FROM categories c
                LEFT JOIN transactions t ON t.category_id = c.id
                    AND t.type = 'expense'
                    AND t.date >= %s AND t.date <= %s
                WHERE c.user_id = %s AND c.kind = 'expense' AND c.is_archived = FALSE
                  AND c.name NOT IN ('Internal Movement', 'Investasi')
                GROUP BY c.id, c.name, c.icon, c.color, c.monthly_budget
                ORDER BY spent DESC, c.name ASC
                """,
                (cycle_start, cycle_end, user_id),
            )
            cat_rows = cur.fetchall()

            # 3. Daily spending histogram
            cur.execute(
                """
                SELECT
                    DATE(t.date AT TIME ZONE %s) AS day_date,
                    COALESCE(SUM(CASE
                        WHEN t.type = 'expense'
                             AND (c.is_excluded_from_budget IS FALSE OR c.is_excluded_from_budget IS NULL)
                             AND (c.name IS NULL OR c.name NOT IN ('Internal Movement', 'Investasi'))
                        THEN t.amount ELSE 0 END), 0) AS day_expense,
                    COALESCE(SUM(CASE
                        WHEN t.type = 'income'
                             AND (c.is_excluded_from_budget IS FALSE OR c.is_excluded_from_budget IS NULL)
                             AND (c.name IS NULL OR c.name NOT IN ('Internal Movement', 'Investasi'))
                        THEN t.amount ELSE 0 END), 0) AS day_income
                FROM transactions t
                LEFT JOIN categories c ON t.category_id = c.id
                WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
                GROUP BY day_date
                ORDER BY day_date ASC
                """,
                (settings.tz, user_id, cycle_start, cycle_end),
            )
            daily_rows = cur.fetchall()

    daily_map = {r["day_date"].isoformat(): r for r in daily_rows}
    histogram = []
    curr_date = cycle_start.astimezone(tz).date()
    end_date = now_local.date()
    while curr_date <= end_date:
        d_str = curr_date.isoformat()
        entry = daily_map.get(d_str)
        histogram.append({
            "date": d_str,
            "expense": int(entry["day_expense"]) if entry else 0,
            "income": int(entry["day_income"]) if entry else 0,
        })
        curr_date += timedelta(days=1)

    categories = []
    for c in cat_rows:
        budget = c["monthly_budget"]
        spent = int(c["spent"])
        pct = round((spent / budget) * 100, 1) if budget and budget > 0 else None
        categories.append({
            "id": str(c["id"]),
            "name": c["name"],
            "icon": c["icon"],
            "color": c["color"],
            "budget": budget,
            "spent": spent,
            "percentage_used": pct,
        })

    return {
        "ok": True,
        "cycle": {
            "start": cycle_start.isoformat(),
            "end": cycle_end.isoformat(),
            "elapsed_days": elapsed_days,
            "total_days": total_days,
        },
        "total_income": total_income,
        "total_expense": total_expense,
        "net_cashflow": total_income - total_expense,
        "categories": categories,
        "daily_histogram": histogram,
    }
