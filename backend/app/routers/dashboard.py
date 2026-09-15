import calendar
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query

from app.core.config import settings
from app.db.pool import db_conn
from app.routers.accounts import get_accounts_with_balances
from app.routers.pulse import get_cycle_window
from app.services.auth import get_current_user

router = APIRouter(tags=["Dashboard"])


def get_timeframe_window(
    payday_day: int,
    timeframe: str = "cycle",
    ref_date: date | None = None,
    cycle_offset: int = 0,
) -> tuple[datetime, datetime, date, date, str]:
    """Returns (start_utc, end_utc, start_date, end_date, label)."""
    tz = ZoneInfo(settings.tz)
    target_date = ref_date or datetime.now(tz).date()

    if timeframe == "30d":
        start_date = target_date - timedelta(days=29)
        end_date = target_date
        label = "30 Hari Terakhir"
    elif timeframe == "90d":
        start_date = target_date - timedelta(days=89)
        end_date = target_date
        label = "90 Hari Terakhir"
    else:  # cycle
        curr = target_date
        step = -1 if cycle_offset < 0 else 1
        for _ in range(abs(cycle_offset)):
            c_start, c_end, _, _ = get_cycle_window(payday_day, curr)
            if step < 0:
                curr = c_start.astimezone(tz).date() - timedelta(days=1)
            else:
                curr = c_end.astimezone(tz).date() + timedelta(days=1)

        c_start, c_end, _, _ = get_cycle_window(payday_day, curr)
        start_date = c_start.astimezone(tz).date()
        end_date = c_end.astimezone(tz).date()
        label = f"Periode {start_date.strftime('%d %b')} - {end_date.strftime('%d %b')}"

    start_utc = datetime.combine(start_date, time.min, tzinfo=tz).astimezone(timezone.utc)
    end_utc = datetime.combine(end_date, time.max, tzinfo=tz).astimezone(timezone.utc)
    return start_utc, end_utc, start_date, end_date, label


def calc_delta_pct(current: int | float, prev: int | float) -> float:
    if prev == 0:
        return 100.0 if current > 0 else (0.0 if current == 0 else -100.0)
    return round(((current - prev) / abs(prev)) * 100, 1)


def format_idr_compact(amount: int) -> str:
    """Format integer into Indonesian currency notation, e.g. 10.000.000"""
    return f"{amount:,}".replace(",", ".")


def generate_financial_narrative(
    current_outflow: int,
    prev_outflow: int,
    outflow_delta_pct: float,
    current_net: int,
    top_driver_name: str | None,
    top_driver_delta_pct: float | None,
) -> tuple[str, str]:
    """
    Generates (narrative_string, trend_status).
    trend_status is 'MENURUN' | 'MENINGKAT' | 'STABIL'.
    """
    if prev_outflow == 0 and current_outflow == 0:
        return "Belum ada transaksi pengeluaran pada periode ini.", "STABIL"

    cashflow_label = "surplus" if current_net >= 0 else "defisit"

    if prev_outflow == 0:
        trend_status = "MENINGKAT"
        return (
            f"Pengeluaran periode ini tercatat Rp {format_idr_compact(current_outflow)}. "
            f"Arus kas tercatat {cashflow_label} Rp {format_idr_compact(abs(current_net))}.",
            trend_status,
        )

    if outflow_delta_pct > 0:
        trend_status = "MENINGKAT"
        direction = f"naik {abs(outflow_delta_pct):.1f}%"
    elif outflow_delta_pct < 0:
        trend_status = "MENURUN"
        direction = f"turun {abs(outflow_delta_pct):.1f}%"
    else:
        trend_status = "STABIL"
        direction = "stabil (0.0%)"

    driver_clause = ""
    if top_driver_name:
        sign = "+" if (top_driver_delta_pct or 0) > 0 else ""
        driver_clause = f", terutama dipengaruhi oleh {top_driver_name} ({sign}{top_driver_delta_pct:.1f}%)"

    narrative = (
        f"Dibanding periode sebelumnya, pengeluaran {direction} menjadi Rp {format_idr_compact(current_outflow)}"
        f"{driver_clause}. Arus kas tercatat {cashflow_label} Rp {format_idr_compact(abs(current_net))}."
    )
    return narrative, trend_status


def get_previous_timeframe_window(
    payday_day: int,
    timeframe: str,
    start_date: date,
    end_date: date,
    cycle_offset: int,
) -> tuple[datetime, datetime, date, date, str]:
    tz = ZoneInfo(settings.tz)
    if timeframe == "30d":
        prev_start_date = start_date - timedelta(days=30)
        prev_end_date = start_date - timedelta(days=1)
        prev_label = "30 Hari Sebelumnya"
        prev_start_utc = datetime.combine(prev_start_date, time.min, tzinfo=tz).astimezone(timezone.utc)
        prev_end_utc = datetime.combine(prev_end_date, time.max, tzinfo=tz).astimezone(timezone.utc)
        return prev_start_utc, prev_end_utc, prev_start_date, prev_end_date, prev_label
    elif timeframe == "90d":
        prev_start_date = start_date - timedelta(days=90)
        prev_end_date = start_date - timedelta(days=1)
        prev_label = "90 Hari Sebelumnya"
        prev_start_utc = datetime.combine(prev_start_date, time.min, tzinfo=tz).astimezone(timezone.utc)
        prev_end_utc = datetime.combine(prev_end_date, time.max, tzinfo=tz).astimezone(timezone.utc)
        return prev_start_utc, prev_end_utc, prev_start_date, prev_end_date, prev_label
    else:
        return get_timeframe_window(payday_day, "cycle", cycle_offset=cycle_offset - 1)


def get_kakeibo_breakdown(
    cur,
    user_id: str,
    start_utc: datetime,
    end_utc: datetime,
    total_inflow: int,
) -> dict[str, Any]:
    # 1. Expenses by pillar (transaction-level classification)
    cur.execute(
        """
        SELECT 
            COALESCE(t.kakeibo_type, 'need') AS pillar,
            COALESCE(SUM(t.amount), 0) AS total_amount
        FROM transactions t
        WHERE t.user_id = %s 
          AND t.type = 'expense' 
          AND t.date >= %s AND t.date <= %s
        GROUP BY pillar
        """,
        (user_id, start_utc, end_utc),
    )
    pillar_map = {r["pillar"]: int(r["total_amount"]) for r in cur.fetchall()}
    need_spent = pillar_map.get("need", 0)
    want_spent = pillar_map.get("want", 0)
    saving_expenses = pillar_map.get("saving", 0)

    # 2. Saving transfers (fresh investments from operational accounts, goal funding, or explicitly marked saving)
    cur.execute(
        """
        SELECT 
            COALESCE(SUM(
                CASE 
                    -- Explicit saving or goal funding (excluding trades)
                    WHEN (t.kakeibo_type = 'saving' OR t.goal_id IS NOT NULL)
                         AND t.notes NOT LIKE '%%lot @%%'
                         AND t.notes NOT LIKE '%%Stockbit%%'
                    THEN t.amount
                    -- Fresh capital: from non-investment operational account to investment account (excluding trades)
                    WHEN (fa.type != 'investment' AND fa.instrument_type IS NULL AND fa.name NOT ILIKE '%%RDN%%')
                         AND (ta.type = 'investment' OR ta.instrument_type IS NOT NULL)
                         AND t.notes NOT LIKE '%%lot @%%'
                         AND t.notes NOT LIKE '%%Stockbit: Stockbit%%'
                    THEN t.amount
                    -- Withdrawals: from investment account back to operational checking/cash
                    WHEN (fa.type = 'investment' OR fa.instrument_type IS NOT NULL)
                         AND (ta.type != 'investment' AND ta.instrument_type IS NULL)
                         AND t.notes NOT LIKE '%%lot @%%'
                         AND t.notes NOT LIKE '%%Stockbit: Stockbit%%'
                    THEN -t.amount
                    ELSE 0
                END
            ), 0) AS net_saving_transfers
        FROM transactions t
        LEFT JOIN accounts fa ON fa.id = t.account_id
        LEFT JOIN accounts ta ON ta.id = t.transfer_target_account_id
        WHERE t.user_id = %s 
          AND t.type = 'transfer' 
          AND t.date >= %s AND t.date <= %s
        """,
        (user_id, start_utc, end_utc),
    )
    st_row = cur.fetchone()
    if st_row:
        net_saving_transfers = int(
            st_row.get("net_saving_transfers")
            or st_row.get("saving_transfers")
            or (st_row[0] if isinstance(st_row, tuple) else 0)
            or 0
        )
    else:
        net_saving_transfers = 0
    saving_spent = max(0, saving_expenses + net_saving_transfers)

    total_allocated = need_spent + want_spent + saving_spent
    base_calc = max(total_inflow, total_allocated)

    if base_calc > 0:
        need_pct = round((need_spent / base_calc) * 100, 1)
        want_pct = round((want_spent / base_calc) * 100, 1)
        saving_pct = round((saving_spent / base_calc) * 100, 1)
    else:
        need_pct = 0.0
        want_pct = 0.0
        saving_pct = 0.0

    want_status = "warning" if want_pct > 30.0 else "healthy"
    kakeibo_status = "warning" if want_pct > 30.0 else ("elevated_needs" if need_pct > 50.0 else "healthy")

    return {
        "need_spent": need_spent,
        "need_pct": need_pct,
        "need_target_pct": 50.0,
        "want_spent": want_spent,
        "want_pct": want_pct,
        "want_target_pct": 30.0,
        "saving_spent": saving_spent,
        "saving_pct": saving_pct,
        "saving_target_pct": 20.0,
        "total_allocated": total_allocated,
        "kakeibo_status": kakeibo_status,
        "want_status": want_status,
    }


def calculate_ketahanan_dana(
    cur,
    user_id: str,
    liquid_total: int,
    accounts: list[dict[str, Any]],
    target_multiplier: int = 6,
) -> dict[str, Any]:
    """
    Calculates Ketahanan Dana (Emergency Fund Coverage):
    Coverage = Flagged Emergency Fund Balance / Monthly Primary Living Expense.
    """
    # 1. Flagged Emergency Fund Balance
    cur.execute(
        """
        SELECT id, name, target_amount, current_amount, is_emergency
        FROM goals
        WHERE user_id = %s AND is_archived = FALSE
        """,
        (user_id,),
    )
    all_goals = cur.fetchall() or []
    g_ids = [str(g["id"]) for g in all_goals]

    g_linked_map: dict[str, list[str]] = {gid: [] for gid in g_ids}
    if g_ids:
        cur.execute(
            "SELECT goal_id, account_id FROM goal_accounts WHERE goal_id = ANY(%s)",
            (g_ids,),
        )
        for ga in cur.fetchall() or []:
            g_linked_map.setdefault(str(ga["goal_id"]), []).append(str(ga["account_id"]))

    acc_balance_map = {str(a["id"]): int(a.get("balance", 0)) for a in accounts}
    acc_parent_map = {str(a["id"]): str(a["parent_id"]) if a.get("parent_id") else None for a in accounts}

    emergency_goals = []
    for g in all_goals:
        gid = str(g["id"])
        linked_aids = g_linked_map.get(gid, [])
        if linked_aids:
            linked_set = set(linked_aids)
            c_amt = sum(
                acc_balance_map.get(aid, 0)
                for aid in linked_aids
                if not acc_parent_map.get(aid) or acc_parent_map.get(aid) not in linked_set
            )
        else:
            c_amt = int(g.get("current_amount", 0))

        g_dict = {
            "id": gid,
            "name": g["name"],
            "current_amount": c_amt,
            "is_emergency": bool(g.get("is_emergency", False)),
        }
        if g_dict["is_emergency"]:
            emergency_goals.append(g_dict)

    # Fallback to name pattern if none explicitly flagged with is_emergency
    if not emergency_goals:
        for g in all_goals:
            name_lower = str(g.get("name", "")).lower()
            if any(k in name_lower for k in ["darurat", "emergency"]):
                gid = str(g["id"])
                linked_aids = g_linked_map.get(gid, [])
                if linked_aids:
                    linked_set = set(linked_aids)
                    c_amt = sum(
                        acc_balance_map.get(aid, 0)
                        for aid in linked_aids
                        if not acc_parent_map.get(aid) or acc_parent_map.get(aid) not in linked_set
                    )
                else:
                    c_amt = int(g.get("current_amount", 0))
                emergency_goals.append({
                    "id": gid,
                    "name": g["name"],
                    "current_amount": c_amt,
                    "is_emergency": True,
                })

    is_flagged = len(emergency_goals) > 0
    if is_flagged:
        emergency_fund_balance = sum(g["current_amount"] for g in emergency_goals)
    else:
        emergency_fund_balance = liquid_total

    # 2. Monthly Primary Living Expense
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    cur.execute(
        """
        SELECT COALESCE(SUM(t.amount), 0) AS primary_spent_30d
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s
          AND t.type = 'expense'
          AND t.date >= %s
          AND COALESCE(c.is_primary, TRUE) = TRUE
        """,
        (user_id, thirty_days_ago),
    )
    row_30d = cur.fetchone() or {}
    primary_spent_30d = int(row_30d.get("primary_spent_30d") or row_30d.get("spent_30d") or 0)

    cur.execute(
        """
        SELECT COALESCE(SUM(monthly_budget), 0) AS primary_budget
        FROM categories
        WHERE user_id = %s AND kind = 'expense' AND is_archived = FALSE AND is_primary = TRUE
        """,
        (user_id,),
    )
    row_budget = cur.fetchone() or {}
    primary_budget = int(row_budget.get("primary_budget") or row_budget.get("total_budget") or 0)

    cur.execute(
        """
        SELECT COALESCE(SUM(minimum_payment), 0) AS monthly_commitments
        FROM obligations
        WHERE user_id = %s AND is_archived = FALSE
        """,
        (user_id,),
    )
    row_ob = cur.fetchone() or {}
    monthly_obligations = int(row_ob.get("monthly_commitments") or 0)

    monthly_primary_expense = (primary_spent_30d if primary_spent_30d > 0 else primary_budget) + monthly_obligations
    daily_primary_burn = round(monthly_primary_expense / 30) if monthly_primary_expense > 0 else 0

    target_months = float(target_multiplier or 6)
    target_amount = int(round(target_months * monthly_primary_expense))

    # 3. Coverage metrics
    if emergency_fund_balance <= 0:
        runway_days = 0
        runway_months = 0.0
        coverage_pct = 0.0
        status = "zero"
    elif monthly_primary_expense <= 0:
        runway_days = 999
        runway_months = 99.9
        coverage_pct = 100.0
        status = "healthy"
    else:
        runway_months = round(emergency_fund_balance / monthly_primary_expense, 1)
        runway_days = round(runway_months * 30.4)
        coverage_pct = round((emergency_fund_balance / target_amount) * 100, 1) if target_amount > 0 else 100.0
        status = "healthy" if runway_months >= target_months else "moderate" if runway_months >= (target_months / 2) else "critical"

    return {
        "daily_burn_rate": daily_primary_burn,
        "runway_days": runway_days,
        "runway_months": runway_months,
        "coverage_months": runway_months,
        "target_months": target_months,
        "target_amount": target_amount,
        "coverage_pct": coverage_pct,
        "status": status,
        "emergency_fund_balance": emergency_fund_balance,
        "monthly_primary_expense": monthly_primary_expense,
        "is_flagged": is_flagged,
        "emergency_goal_names": [g["name"] for g in emergency_goals],
        "primary_spent_30d": primary_spent_30d,
        "monthly_commitments": monthly_obligations,
    }


@router.get("/overview")
def get_dashboard_overview(
    timeframe: str = Query(default="cycle", pattern="^(cycle|30d|90d)$"),
    cycle_offset: int = Query(default=0, ge=-24, le=12),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    payday_day = current_user.get("payday_day", 25)

    start_utc, end_utc, start_date, end_date, timeframe_label = get_timeframe_window(
        payday_day, timeframe, cycle_offset=cycle_offset
    )

    # 1. Accounts & Liquid Balances
    accounts = get_accounts_with_balances(user_id, include_archived=False)
    top_accounts = [a for a in accounts if a.get("parent_id") is None]

    liquid_accounts = [a for a in top_accounts if a.get("type") in ("cash", "bank", "ewallet", "wallet") and not a.get("instrument_type")]
    investment_accounts = [a for a in top_accounts if a.get("type") == "investment" or a.get("instrument_type") is not None]

    liquid_balance = sum(a["balance"] for a in liquid_accounts)
    investment_balance = sum(a["balance"] for a in investment_accounts)
    total_balance = sum(a["balance"] for a in top_accounts)
    liquid_net_worth = liquid_balance

    emergency_multiplier = int(current_user.get("emergency_fund_multiplier") or 6)
    monthly_spending_budget = current_user.get("monthly_spending_budget")

    total_positive_balance = sum(max(0, a["balance"]) for a in top_accounts)
    accounts_liquidity = [
        {
            "id": a["id"],
            "name": a["name"],
            "type": a["type"],
            "color": a.get("color", "#3b82f6"),
            "instrument_type": a.get("instrument_type"),
            "balance": a["balance"],
            "percentage": round((a["balance"] / total_positive_balance) * 100, 1)
            if total_positive_balance > 0 and a["balance"] > 0
            else 0,
        }
        for a in top_accounts
    ]

    with db_conn() as conn:
        with conn.cursor() as cur:
            # 2. Total Inflow & Outflow in timeframe
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN type = 'income' THEN amount ELSE 0 END), 0) AS inflow,
                    COALESCE(SUM(CASE WHEN type = 'expense' THEN amount ELSE 0 END), 0) AS outflow,
                    COUNT(*) AS tx_count
                FROM transactions
                WHERE user_id = %s AND date >= %s AND date <= %s
                """,
                (user_id, start_utc, end_utc),
            )
            totals_row = cur.fetchone()
            total_inflow = int(totals_row["inflow"])
            total_outflow = int(totals_row["outflow"])
            net_cashflow = total_inflow - total_outflow
            savings_rate = (
                round(((total_inflow - total_outflow) / total_inflow) * 100, 1)
                if total_inflow > 0
                else 0
            )

            # 3. Ketahanan Dana / Runway calculation
            runway_info = calculate_ketahanan_dana(cur, user_id, liquid_balance, accounts, target_multiplier=emergency_multiplier)
            daily_burn_rate = runway_info["daily_burn_rate"]
            runway_days = runway_info["runway_days"]

            # 4. Safe to spend today benchmark (from user monthly budget or category budgets)
            cur.execute(
                "SELECT COALESCE(SUM(monthly_budget), 0) AS total_budget FROM categories WHERE user_id = %s AND is_archived = false AND kind = 'expense'",
                (user_id,),
            )
            cat_budget = int(cur.fetchone()["total_budget"] or 0)
            effective_budget = monthly_spending_budget if (monthly_spending_budget and monthly_spending_budget > 0) else cat_budget

            tz = ZoneInfo(settings.tz)
            today_date = datetime.now(tz).date()

            # Query upcoming recurring expenses in this cycle
            cur.execute(
                """
                SELECT COALESCE(SUM(amount), 0) AS upcoming_amount
                FROM recurring_rules
                WHERE user_id = %s 
                  AND is_active = TRUE 
                  AND type = 'expense'
                  AND next_due_date >= %s 
                  AND next_due_date <= %s
                """,
                (user_id, today_date, end_date),
            )
            upcoming_row = cur.fetchone()
            if upcoming_row:
                if isinstance(upcoming_row, dict):
                    upcoming_recurring_expense = int(upcoming_row.get("upcoming_amount") or 0)
                else:
                    upcoming_recurring_expense = int(upcoming_row[0] or 0)
            else:
                upcoming_recurring_expense = 0

            # Compute cycle remaining days
            c_start, c_end, elapsed_days, total_days = get_cycle_window(payday_day)
            remaining_cycle_days = max(1, total_days - elapsed_days)

            if effective_budget > 0:
                safe_discretionary = max(0, effective_budget - total_outflow - upcoming_recurring_expense)
                safe_to_spend_today = max(0, round(safe_discretionary / remaining_cycle_days))
                benchmark_daily = round(effective_budget / total_days)
                has_budget = True
            else:
                safe_to_spend_today = 0
                benchmark_daily = 0
                has_budget = False

            expected_spent = benchmark_daily * elapsed_days
            velocity_ratio = round(total_outflow / expected_spent, 2) if expected_spent > 0 else 1.0
            if velocity_ratio > 1.15:
                velocity_status = "fast"
            elif velocity_ratio < 0.85:
                velocity_status = "frugal"
            else:
                velocity_status = "on_track"

            # 5. Day-by-Day Time Series for Cumulative Trendline & Burn Cadence
            cur.execute(
                """
                SELECT
                    (date AT TIME ZONE %s)::date AS tx_day,
                    COALESCE(SUM(CASE WHEN type = 'income' THEN amount ELSE 0 END), 0) AS income,
                    COALESCE(SUM(CASE WHEN type = 'expense' THEN amount ELSE 0 END), 0) AS expense
                FROM transactions
                WHERE user_id = %s AND date >= %s AND date <= %s
                GROUP BY tx_day
                ORDER BY tx_day ASC
                """,
                (settings.tz, user_id, start_utc, end_utc),
            )
            grouped_days = {r["tx_day"]: r for r in cur.fetchall()}

            # Generate continuous series from start_date to end_date (or today)
            series_end_date = min(end_date, today_date) if timeframe == "cycle" else end_date

            cumulative_trendline = []
            burn_cadence = []
            running_inflow = 0
            running_outflow = 0

            curr = start_date
            while curr <= series_end_date:
                day_data = grouped_days.get(curr)
                day_income = int(day_data["income"]) if day_data else 0
                day_expense = int(day_data["expense"]) if day_data else 0

                running_inflow += day_income
                running_outflow += day_expense

                day_str = curr.isoformat()
                cumulative_trendline.append(
                    {
                        "date": day_str,
                        "label": curr.strftime("%b %d"),
                        "income": day_income,
                        "expense": day_expense,
                        "cumulative_inflow": running_inflow,
                        "cumulative_outflow": running_outflow,
                        "cumulative_net": running_inflow - running_outflow,
                    }
                )

                burn_cadence.append(
                    {
                        "date": day_str,
                        "label": curr.strftime("%b %d"),
                        "expense": day_expense,
                        "benchmark_safe_daily": benchmark_daily,
                    }
                )
                curr += timedelta(days=1)

            # 6. Category Breakdown
            cur.execute(
                """
                SELECT 
                    c.id, c.name, c.icon, c.color, c.monthly_budget, c.is_primary, c.kakeibo_type,
                    COALESCE(SUM(t.amount), 0) AS spent
                FROM categories c
                LEFT JOIN transactions t ON t.category_id = c.id 
                    AND t.type = 'expense' 
                    AND t.date >= %s AND t.date <= %s
                WHERE c.user_id = %s AND c.is_archived = false AND c.kind = 'expense'
                GROUP BY c.id, c.name, c.icon, c.color, c.monthly_budget, c.is_primary, c.kakeibo_type
                ORDER BY spent DESC, c.name ASC
                """,
                (start_utc, end_utc, user_id),
            )
            cat_rows = cur.fetchall()
            categories_data = []
            for r in cat_rows:
                spent = int(r["spent"])
                pct_total = (
                    round((spent / total_outflow) * 100, 1) if total_outflow > 0 else 0
                )
                budget = int(r["monthly_budget"]) if r["monthly_budget"] else None
                pct_used = round((spent / budget) * 100, 1) if budget and budget > 0 else None
                categories_data.append(
                    {
                        "id": str(r["id"]),
                        "name": r["name"],
                        "icon": r["icon"],
                        "color": r["color"],
                        "is_primary": bool(r.get("is_primary", True)),
                        "kakeibo_type": r.get("kakeibo_type") or ("need" if r.get("is_primary", True) else "want"),
                        "spent": spent,
                        "percentage_of_total": pct_total,
                        "budget": budget,
                        "percentage_used": pct_used,
                    }
                )

            # 6b. Period Comparison, Narrative, and Kakeibo
            prev_start_utc, prev_end_utc, prev_start_date, prev_end_date, prev_label = get_previous_timeframe_window(
                payday_day, timeframe, start_date, end_date, cycle_offset
            )

            cur.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN type = 'income' THEN amount ELSE 0 END), 0) AS inflow,
                    COALESCE(SUM(CASE WHEN type = 'expense' THEN amount ELSE 0 END), 0) AS outflow,
                    COUNT(*) AS tx_count
                FROM transactions
                WHERE user_id = %s AND date >= %s AND date <= %s
                """,
                (user_id, prev_start_utc, prev_end_utc),
            )
            prev_totals_row = cur.fetchone()
            prev_inflow = int(prev_totals_row["inflow"]) if prev_totals_row else 0
            prev_outflow = int(prev_totals_row["outflow"]) if prev_totals_row else 0
            prev_net = prev_inflow - prev_outflow
            prev_savings_rate = (
                round(((prev_inflow - prev_outflow) / prev_inflow) * 100, 1)
                if prev_inflow > 0
                else 0.0
            )

            inflow_delta_pct = calc_delta_pct(total_inflow, prev_inflow)
            outflow_delta_pct = calc_delta_pct(total_outflow, prev_outflow)
            net_cashflow_delta_pct = calc_delta_pct(net_cashflow, prev_net)
            savings_rate_delta_pts = round(savings_rate - prev_savings_rate, 1)

            # Query previous category spending for top driver identification
            cur.execute(
                """
                SELECT 
                    c.id, c.name,
                    COALESCE(SUM(t.amount), 0) AS spent
                FROM categories c
                LEFT JOIN transactions t ON t.category_id = c.id 
                    AND t.type = 'expense' 
                    AND t.date >= %s AND t.date <= %s
                WHERE c.user_id = %s AND c.is_archived = false AND c.kind = 'expense'
                GROUP BY c.id, c.name
                """,
                (prev_start_utc, prev_end_utc, user_id),
            )
            prev_cat_map = {str(r["id"]): int(r["spent"]) for r in cur.fetchall()}

            top_driver_name = None
            top_driver_delta = 0.0
            max_impact = -1

            for r in cat_rows:
                cid = str(r["id"])
                cname = r["name"]
                curr_spent = int(r["spent"])
                prev_spent = prev_cat_map.get(cid, 0)
                diff = curr_spent - prev_spent

                if outflow_delta_pct >= 0:
                    if diff > max_impact and curr_spent > 0:
                        max_impact = diff
                        top_driver_name = cname
                        top_driver_delta = calc_delta_pct(curr_spent, prev_spent)
                else:
                    if -diff > max_impact and prev_spent > 0:
                        max_impact = -diff
                        top_driver_name = cname
                        top_driver_delta = calc_delta_pct(curr_spent, prev_spent)

            if not top_driver_name and cat_rows and total_outflow > 0:
                top_cat = max(cat_rows, key=lambda x: int(x["spent"]))
                if int(top_cat["spent"]) > 0:
                    top_driver_name = top_cat["name"]
                    p_spent = prev_cat_map.get(str(top_cat["id"]), 0)
                    top_driver_delta = calc_delta_pct(int(top_cat["spent"]), p_spent)

            narrative_summary, trend_status = generate_financial_narrative(
                current_outflow=total_outflow,
                prev_outflow=prev_outflow,
                outflow_delta_pct=outflow_delta_pct,
                current_net=net_cashflow,
                top_driver_name=top_driver_name,
                top_driver_delta_pct=top_driver_delta,
            )

            kakeibo_data = get_kakeibo_breakdown(cur, user_id, start_utc, end_utc, total_inflow)

            # 7. Goals Glance
            cur.execute(
                """
                SELECT id, name, target_amount, current_amount, target_date, color, icon
                FROM goals
                WHERE user_id = %s AND is_archived = false
                ORDER BY target_date ASC NULLS LAST, created_at ASC
                LIMIT 4
                """,
                (user_id,),
            )
            goals_rows = cur.fetchall()
            goal_ids = [str(g["id"]) for g in goals_rows]

            goal_linked_map: dict[str, list[str]] = {gid: [] for gid in goal_ids}
            if goal_ids:
                cur.execute(
                    "SELECT goal_id, account_id FROM goal_accounts WHERE goal_id = ANY(%s)",
                    (goal_ids,),
                )
                for ga in cur.fetchall():
                    goal_linked_map.setdefault(str(ga["goal_id"]), []).append(str(ga["account_id"]))

            acc_balance_map = {str(a["id"]): int(a["balance"]) for a in accounts}
            acc_parent_map = {str(a["id"]): str(a["parent_id"]) if a.get("parent_id") else None for a in accounts}

            goals_glance = []
            for g in goals_rows:
                gid = str(g["id"])
                linked_aids = goal_linked_map.get(gid, [])
                if linked_aids:
                    linked_set = set(linked_aids)
                    c_amt = sum(
                        acc_balance_map.get(aid, 0)
                        for aid in linked_aids
                        if not acc_parent_map.get(aid) or acc_parent_map.get(aid) not in linked_set
                    )
                else:
                    c_amt = int(g["current_amount"])
                t_amt = int(g["target_amount"])
                pct = min(100.0, round((c_amt / t_amt) * 100, 1)) if t_amt > 0 else 0
                goals_glance.append({
                    "id": gid,
                    "name": g["name"],
                    "target_amount": t_amt,
                    "current_amount": c_amt,
                    "percentage": pct,
                    "target_date": g["target_date"].isoformat() if g["target_date"] else None,
                    "color": g["color"],
                    "icon": g["icon"],
                })

            # 8. Obligations Glance
            cur.execute(
                """
                SELECT id, name, total_amount, remaining_amount, due_date, minimum_payment
                FROM obligations
                WHERE user_id = %s AND is_archived = false
                ORDER BY due_date ASC NULLS LAST, remaining_amount DESC
                LIMIT 4
                """,
                (user_id,),
            )
            obligations_glance = [
                {
                    "id": str(o["id"]),
                    "name": o["name"],
                    "total_amount": int(o["total_amount"]),
                    "remaining_amount": int(o["remaining_amount"]),
                    "payoff_percentage": min(
                        100.0,
                        round(
                            ((int(o["total_amount"]) - int(o["remaining_amount"])) / int(o["total_amount"]))
                            * 100,
                            1,
                        ),
                    )
                    if int(o["total_amount"]) > 0
                    else 0,
                    "due_date": o["due_date"].isoformat() if o["due_date"] else None,
                    "minimum_payment": o["minimum_payment"],
                }
                for o in cur.fetchall()
            ]

            # 9. Recent Transactions (10)
            cur.execute(
                """
                SELECT 
                    t.id, t.account_id, sa.name AS account_name,
                    t.category_id, c.name AS category_name, c.icon AS category_icon, c.color AS category_color,
                    t.type, t.transfer_target_account_id, ta.name AS transfer_target_account_name,
                    t.amount, t.notes, t.date, t.receipt_path
                FROM transactions t
                JOIN accounts sa ON sa.id = t.account_id
                LEFT JOIN accounts ta ON ta.id = t.transfer_target_account_id
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.user_id = %s
                ORDER BY t.date DESC, t.created_at DESC
                LIMIT 10
                """,
                (user_id,),
            )
            recent_txs = [
                {
                    "id": str(r["id"]),
                    "account_id": str(r["account_id"]),
                    "account_name": r["account_name"],
                    "category_id": str(r["category_id"]) if r["category_id"] else None,
                    "category_name": r["category_name"],
                    "category_icon": r["category_icon"],
                    "category_color": r["category_color"],
                    "type": r["type"],
                    "transfer_target_account_id": str(r["transfer_target_account_id"]) if r["transfer_target_account_id"] else None,
                    "transfer_target_account_name": r["transfer_target_account_name"],
                    "amount": r["amount"],
                    "notes": r["notes"],
                    "date": r["date"].isoformat() if r["date"] else None,
                    "receipt_path": r["receipt_path"],
                }
                for r in cur.fetchall()
            ]

    return {
        "ok": True,
        "timeframe": timeframe,
        "timeframe_label": timeframe_label,
        "kpis": {
            "liquid_net_worth": liquid_balance,
            "liquid_balance": liquid_balance,
            "investment_balance": investment_balance,
            "total_balance": total_balance,
            "total_inflow": total_inflow,
            "total_outflow": total_outflow,
            "net_cashflow": net_cashflow,
            "savings_rate": savings_rate,
            "safe_to_spend_today": safe_to_spend_today,
            "has_budget": has_budget,
            "effective_budget": effective_budget,
            "daily_burn_rate": daily_burn_rate,
            "runway_days": runway_days,
            "runway_months": runway_info["runway_months"],
            "target_months": runway_info.get("target_months", 6.0),
            "target_amount": runway_info.get("target_amount", 0),
            "coverage_pct": runway_info.get("coverage_pct", 100.0),
            "runway_status": runway_info["status"],
            "emergency_fund_balance": runway_info["emergency_fund_balance"],
            "monthly_primary_expense": runway_info["monthly_primary_expense"],
            "is_emergency_flagged": runway_info["is_flagged"],
            "emergency_goal_names": runway_info["emergency_goal_names"],
            "benchmark_daily": benchmark_daily,
            "upcoming_recurring_expense": upcoming_recurring_expense,
            "spend_velocity_ratio": velocity_ratio,
            "spend_velocity_status": velocity_status,
        },
        "comparison": {
            "inflow_delta_pct": inflow_delta_pct,
            "outflow_delta_pct": outflow_delta_pct,
            "net_cashflow_delta_pct": net_cashflow_delta_pct,
            "savings_rate_delta_pts": savings_rate_delta_pts,
            "prev_inflow": prev_inflow,
            "prev_outflow": prev_outflow,
            "prev_net_cashflow": prev_net,
            "prev_savings_rate": prev_savings_rate,
            "prev_timeframe_label": prev_label,
        },
        "kakeibo": kakeibo_data,
        "narrative": {
            "summary": narrative_summary,
            "trend_status": trend_status,
            "top_driver": top_driver_name,
            "top_driver_delta_pct": top_driver_delta,
        },
        "spend_velocity": {
            "velocity_ratio": velocity_ratio,
            "velocity_status": velocity_status,
            "benchmark_daily": benchmark_daily,
            "expected_spent": expected_spent,
            "upcoming_recurring_expense": upcoming_recurring_expense,
            "safe_to_spend_today": safe_to_spend_today,
        },
        "cumulative_trendline": cumulative_trendline,
        "burn_cadence": burn_cadence,
        "categories": categories_data,
        "accounts_liquidity": accounts_liquidity,
        "goals_glance": goals_glance,
        "obligations_glance": obligations_glance,
        "recent_transactions": recent_txs,
    }


@router.get("/analytics")
def get_dashboard_analytics(
    timeframe: str = Query(default="cycle", pattern="^(cycle|30d|90d)$"),
    cycle_offset: int = Query(default=0, ge=-24, le=12),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    payday_day = current_user.get("payday_day", 25)

    start_utc, end_utc, start_date, end_date, timeframe_label = get_timeframe_window(
        payday_day, timeframe, cycle_offset=cycle_offset
    )

    with db_conn() as conn:
        with conn.cursor() as cur:
            # 1. Day of Week Heatmap Matrix (0=Sunday to 6=Saturday)
            cur.execute(
                """
                SELECT 
                    EXTRACT(DOW FROM t.date AT TIME ZONE %s)::int AS dow,
                    COUNT(*) AS tx_count,
                    COALESCE(SUM(t.amount), 0) AS total_spent
                FROM transactions t
                WHERE t.user_id = %s AND t.type = 'expense' AND t.date >= %s AND t.date <= %s
                GROUP BY dow
                ORDER BY dow ASC
                """,
                (settings.tz, user_id, start_utc, end_utc),
            )
            dow_rows = {r["dow"]: r for r in cur.fetchall()}

            day_names = ["Minggu", "Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"]
            day_short = ["Min", "Sen", "Sel", "Rab", "Kam", "Jum", "Sab"]
            day_of_week_heatmap = []
            for i in range(7):
                row = dow_rows.get(i)
                spent = int(row["total_spent"]) if row else 0
                count = int(row["tx_count"]) if row else 0
                avg = round(spent / count) if count > 0 else 0
                day_of_week_heatmap.append(
                    {
                        "day_index": i,
                        "day_name": day_names[i],
                        "day_short": day_short[i],
                        "total_spent": spent,
                        "tx_count": count,
                        "average_spent": avg,
                    }
                )

            # 2. Daily Cadence
            cur.execute(
                """
                SELECT 
                    (date AT TIME ZONE %s)::date AS tx_day,
                    COALESCE(SUM(CASE WHEN type = 'expense' THEN amount ELSE 0 END), 0) AS expense,
                    COALESCE(SUM(CASE WHEN type = 'income' THEN amount ELSE 0 END), 0) AS income
                FROM transactions
                WHERE user_id = %s AND date >= %s AND date <= %s
                GROUP BY tx_day
                ORDER BY tx_day ASC
                """,
                (settings.tz, user_id, start_utc, end_utc),
            )
            daily_rows = {r["tx_day"]: r for r in cur.fetchall()}

            daily_cadence = []
            curr = start_date
            while curr <= end_date:
                r = daily_rows.get(curr)
                daily_cadence.append(
                    {
                        "date": curr.isoformat(),
                        "label": curr.strftime("%b %d"),
                        "expense": int(r["expense"]) if r else 0,
                        "income": int(r["income"]) if r else 0,
                    }
                )
                curr += timedelta(days=1)

            # 3. Category Variance Matrix
            cur.execute(
                """
                SELECT 
                    c.id, c.name, c.icon, c.color, c.monthly_budget, c.is_primary, c.kakeibo_type,
                    COALESCE(SUM(t.amount), 0) AS spent
                FROM categories c
                LEFT JOIN transactions t ON t.category_id = c.id 
                    AND t.type = 'expense' 
                    AND t.date >= %s AND t.date <= %s
                WHERE c.user_id = %s AND c.is_archived = false AND c.kind = 'expense'
                GROUP BY c.id, c.name, c.icon, c.color, c.monthly_budget, c.is_primary, c.kakeibo_type
                ORDER BY spent DESC, c.name ASC
                """,
                (start_utc, end_utc, user_id),
            )
            category_variance = []
            for r in cur.fetchall():
                spent = int(r["spent"])
                budget = int(r["monthly_budget"]) if r["monthly_budget"] else None
                pct = round((spent / budget) * 100, 1) if budget and budget > 0 else None
                variance = (budget - spent) if budget else None

                status = "unlimited"
                if budget:
                    if spent > budget:
                        status = "over_budget"
                    elif pct and pct >= 80:
                        status = "warning"
                    else:
                        status = "on_track"

                category_variance.append(
                    {
                        "id": str(r["id"]),
                        "name": r["name"],
                        "icon": r["icon"],
                        "color": r["color"],
                        "is_primary": bool(r.get("is_primary", True)),
                        "kakeibo_type": r.get("kakeibo_type") or ("need" if r.get("is_primary", True) else "want"),
                        "spent": spent,
                        "budget": budget,
                        "variance": variance,
                        "percentage_used": pct,
                        "status": status,
                    }
                )

            # 4. Burn Rate Analytics
            now_utc = datetime.now(timezone.utc)
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN date >= %s THEN amount ELSE 0 END), 0) AS spent_7d,
                    COALESCE(SUM(CASE WHEN date >= %s THEN amount ELSE 0 END), 0) AS spent_30d
                FROM transactions
                WHERE user_id = %s AND type = 'expense' AND date >= %s
                """,
                (now_utc - timedelta(days=7), now_utc - timedelta(days=30), user_id, now_utc - timedelta(days=30)),
            )
            burn_row = cur.fetchone()
            spent_7d = int(burn_row["spent_7d"])
            spent_30d = int(burn_row["spent_30d"])
            burn_7d = round(spent_7d / 7)
            burn_30d = round(spent_30d / 30)
            projected_30d_outflow = spent_30d

            # 5. Inflow / Outflow & Period Comparison
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN type = 'income' THEN amount ELSE 0 END), 0) AS inflow,
                    COALESCE(SUM(CASE WHEN type = 'expense' THEN amount ELSE 0 END), 0) AS outflow
                FROM transactions
                WHERE user_id = %s AND date >= %s AND date <= %s
                """,
                (user_id, start_utc, end_utc),
            )
            totals_row = cur.fetchone() or {}
            total_inflow = int(totals_row.get("inflow", 0))
            total_outflow = int(totals_row.get("outflow", 0))
            net_cashflow = total_inflow - total_outflow
            savings_rate = (
                round(((total_inflow - total_outflow) / total_inflow) * 100, 1)
                if total_inflow > 0
                else 0.0
            )

            # Active Cycle Forecast calculation
            tz_app = ZoneInfo(settings.tz)
            today_local = datetime.now(tz_app).date()
            total_cycle_days = (end_date - start_date).days + 1
            if today_local < start_date:
                cycle_elapsed_days = 1
            elif today_local > end_date:
                cycle_elapsed_days = total_cycle_days
            else:
                cycle_elapsed_days = max(1, (today_local - start_date).days + 1)

            if total_outflow > 0:
                projected_cycle_outflow = round((total_outflow / cycle_elapsed_days) * total_cycle_days)
            else:
                projected_cycle_outflow = 0

            prev_start_utc, prev_end_utc, prev_start_date, prev_end_date, prev_label = get_previous_timeframe_window(
                payday_day, timeframe, start_date, end_date, cycle_offset
            )

            cur.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN type = 'income' THEN amount ELSE 0 END), 0) AS inflow,
                    COALESCE(SUM(CASE WHEN type = 'expense' THEN amount ELSE 0 END), 0) AS outflow
                FROM transactions
                WHERE user_id = %s AND date >= %s AND date <= %s
                """,
                (user_id, prev_start_utc, prev_end_utc),
            )
            prev_totals_row = cur.fetchone() or {}
            prev_inflow = int(prev_totals_row.get("inflow", 0))
            prev_outflow = int(prev_totals_row.get("outflow", 0))
            prev_net = prev_inflow - prev_outflow
            prev_savings_rate = (
                round(((prev_inflow - prev_outflow) / prev_inflow) * 100, 1)
                if prev_inflow > 0
                else 0.0
            )

            inflow_delta_pct = calc_delta_pct(total_inflow, prev_inflow)
            outflow_delta_pct = calc_delta_pct(total_outflow, prev_outflow)
            net_cashflow_delta_pct = calc_delta_pct(net_cashflow, prev_net)
            savings_rate_delta_pts = round(savings_rate - prev_savings_rate, 1)

            # Query previous category spending for top driver identification
            cur.execute(
                """
                SELECT 
                    c.id, c.name,
                    COALESCE(SUM(t.amount), 0) AS spent
                FROM categories c
                LEFT JOIN transactions t ON t.category_id = c.id 
                    AND t.type = 'expense' 
                    AND t.date >= %s AND t.date <= %s
                WHERE c.user_id = %s AND c.is_archived = false AND c.kind = 'expense'
                GROUP BY c.id, c.name
                """,
                (prev_start_utc, prev_end_utc, user_id),
            )
            prev_cat_map = {str(r["id"]): int(r["spent"]) for r in cur.fetchall()}

            top_driver_name = None
            top_driver_delta = 0.0
            max_impact = -1

            for r in category_variance:
                cid = str(r["id"])
                cname = r["name"]
                curr_spent = int(r["spent"])
                prev_spent = prev_cat_map.get(cid, 0)
                diff = curr_spent - prev_spent

                if outflow_delta_pct >= 0:
                    if diff > max_impact and curr_spent > 0:
                        max_impact = diff
                        top_driver_name = cname
                        top_driver_delta = calc_delta_pct(curr_spent, prev_spent)
                else:
                    if -diff > max_impact and prev_spent > 0:
                        max_impact = -diff
                        top_driver_name = cname
                        top_driver_delta = calc_delta_pct(curr_spent, prev_spent)

            if not top_driver_name and category_variance and total_outflow > 0:
                top_cat = max(category_variance, key=lambda x: int(x["spent"]))
                if int(top_cat["spent"]) > 0:
                    top_driver_name = top_cat["name"]
                    p_spent = prev_cat_map.get(str(top_cat["id"]), 0)
                    top_driver_delta = calc_delta_pct(int(top_cat["spent"]), p_spent)

            narrative_summary, trend_status = generate_financial_narrative(
                current_outflow=total_outflow,
                prev_outflow=prev_outflow,
                outflow_delta_pct=outflow_delta_pct,
                current_net=net_cashflow,
                top_driver_name=top_driver_name,
                top_driver_delta_pct=top_driver_delta,
            )

            kakeibo_data = get_kakeibo_breakdown(cur, user_id, start_utc, end_utc, total_inflow)

    return {
        "ok": True,
        "timeframe": timeframe,
        "timeframe_label": timeframe_label,
        "day_of_week_heatmap": day_of_week_heatmap,
        "daily_cadence": daily_cadence,
        "category_variance": category_variance,
        "burn_rate": {
            "daily_burn_7d": burn_7d,
            "daily_burn_30d": burn_30d,
            "projected_cycle_outflow": projected_cycle_outflow,
            "projected_30d_outflow": projected_30d_outflow,
            "cycle_elapsed_days": cycle_elapsed_days,
            "cycle_total_days": total_cycle_days,
            "cycle_end_date": end_date.isoformat(),
        },
        "comparison": {
            "inflow_delta_pct": inflow_delta_pct,
            "outflow_delta_pct": outflow_delta_pct,
            "net_cashflow_delta_pct": net_cashflow_delta_pct,
            "savings_rate_delta_pts": savings_rate_delta_pts,
            "total_inflow": total_inflow,
            "total_outflow": total_outflow,
            "net_cashflow": net_cashflow,
            "savings_rate": savings_rate,
            "prev_inflow": prev_inflow,
            "prev_outflow": prev_outflow,
            "prev_net_cashflow": prev_net,
            "prev_savings_rate": prev_savings_rate,
            "prev_timeframe_label": prev_label,
        },
        "kakeibo": kakeibo_data,
        "narrative": {
            "summary": narrative_summary,
            "trend_status": trend_status,
            "top_driver": top_driver_name,
            "top_driver_delta_pct": top_driver_delta,
        },
    }


@router.get("/net-worth")
def get_dashboard_net_worth(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]

    # 1. Liquid Accounts (Assets)
    accounts = get_accounts_with_balances(user_id, include_archived=False)
    total_assets = sum(a["balance"] for a in accounts if a.get("parent_id") is None)

    with db_conn() as conn:
        with conn.cursor() as cur:
            # 2. Obligations (Liabilities)
            cur.execute(
                """
                SELECT id, name, total_amount, remaining_amount, due_date, minimum_payment, notes
                FROM obligations
                WHERE user_id = %s AND is_archived = false
                ORDER BY remaining_amount DESC
                """,
                (user_id,),
            )
            obligations = [
                {
                    "id": str(r["id"]),
                    "name": r["name"],
                    "total_amount": int(r["total_amount"]),
                    "remaining_amount": int(r["remaining_amount"]),
                    "due_date": r["due_date"].isoformat() if r["due_date"] else None,
                    "minimum_payment": r["minimum_payment"],
                    "notes": r["notes"],
                }
                for r in cur.fetchall()
            ]
            total_liabilities = sum(o["remaining_amount"] for o in obligations)
            net_worth = total_assets - total_liabilities

            # 3. Ketahanan Dana / Runway calculation
            runway_info = calculate_ketahanan_dana(cur, user_id, total_assets, accounts)

    return {
        "ok": True,
        "net_worth": net_worth,
        "total_assets": total_assets,
        "total_liabilities": total_liabilities,
        "accounts": [
            {
                "id": a["id"],
                "name": a["name"],
                "type": a["type"],
                "balance": a["balance"],
                "share": round((a["balance"] / total_assets) * 100, 1) if total_assets > 0 and a["balance"] > 0 else 0,
            }
            for a in accounts
        ],
        "obligations": obligations,
        "runway": runway_info,
    }
