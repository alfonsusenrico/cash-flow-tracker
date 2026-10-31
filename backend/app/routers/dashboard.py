import calendar
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query

from app.core.config import settings
from app.db.pool import db_conn
from app.routers.accounts import get_accounts_with_balances, summarize_account_balances
from app.services.auth import get_current_user
from app.services.essential_cost import DEFAULT_TARGET_MONTHS, EssentialCost, coverage, estimate_essential_cost
from app.services.kakeibo import effective_pillar_sql
from app.services.reimbursements import SpendingOffsets, load_spending_offsets, not_reimbursement_sql
from app.services.report_figures import (
    category_breakdown,
    cash_flow_totals,
    daily_offsets,
    reportable_expense_sql,
    reportable_income_sql,
    sunday_first_weekday,
)
from app.services.observed_insights import cash_flow_history, period_comparison, recurring_charges
from app.services.question_rate import MAX_WEEKS, question_rate
from app.services.report_windows import (
    PERIOD_PATTERN,
    TIMEFRAME_PATTERN,
    get_previous_timeframe_window,
    get_timeframe_window,
)

router = APIRouter(tags=["Dashboard"])

COMMITMENT_WINDOW_DAYS = 30


@router.get("/cash-flow-summary")
def get_cash_flow_summary(
    period: str = Query(default="today", pattern="^(today|month)$"),
    current_user: dict = Depends(get_current_user),
):
    """Observed ledger cash flow through this instant in the application timezone."""
    now = datetime.now(timezone.utc)
    local_now = now.astimezone(ZoneInfo(settings.tz))
    first = local_now.date() if period == "today" else local_now.date().replace(day=1)
    start = datetime.combine(first, time.min, tzinfo=ZoneInfo(settings.tz)).astimezone(timezone.utc)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cur.execute(
                f"""SELECT
                       COALESCE(SUM(CASE WHEN {reportable_income_sql()} THEN t.amount ELSE 0 END), 0) AS inflow,
                       COALESCE(SUM(CASE WHEN {reportable_expense_sql()} THEN t.amount ELSE 0 END), 0) AS outflow
                    FROM transactions t
                    LEFT JOIN categories c ON c.id = t.category_id
                    WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s""",
                (current_user["id"], start, now),
            )
            totals = cur.fetchone()
            offsets = load_spending_offsets(cur, current_user["id"], start, now)
            flow = cash_flow_totals(int(totals["inflow"]), int(totals["outflow"]), offsets.applied)
    return {
        "ok": True,
        "period": period,
        "timezone": settings.tz,
        "start": start.isoformat(),
        "end": now.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "currency": "IDR",
        **flow,
        "reimbursement_unallocated": offsets.unallocated,
        "reimbursement_excess": offsets.excess,
    }


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
    if top_driver_name and top_driver_delta_pct is not None:
        sign = "+" if top_driver_delta_pct > 0 else ""
        driver_clause = f", terutama dipengaruhi oleh {top_driver_name} ({sign}{top_driver_delta_pct:.1f}%)"
    elif top_driver_name:
        driver_clause = f", terutama dipengaruhi oleh {top_driver_name} (belum ada pembanding)"

    narrative = (
        f"Dibanding periode sebelumnya, pengeluaran {direction} menjadi Rp {format_idr_compact(current_outflow)}"
        f"{driver_clause}. Arus kas tercatat {cashflow_label} Rp {format_idr_compact(abs(current_net))}."
    )
    return narrative, trend_status


def get_kakeibo_breakdown(
    cur,
    user_id: str,
    start_utc: datetime,
    end_utc: datetime,
    total_inflow: int,
    offsets: SpendingOffsets | None = None,
) -> dict[str, Any]:
    if offsets is None:
        offsets = load_spending_offsets(cur, user_id, start_utc, end_utc)
    # 1. Expenses by effective pillar, excluding internal movements and other excluded categories
    cur.execute(
        f"""
        SELECT
            {effective_pillar_sql()} AS pillar,
            COALESCE(SUM(t.amount), 0) AS total_amount
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s
          AND {reportable_expense_sql()}
          AND t.date >= %s AND t.date <= %s
        GROUP BY pillar
        """,
        (user_id, start_utc, end_utc),
    )
    # Reimbursements lower the pillar of the category they repay, never below zero.
    pillar_map = {
        r["pillar"]: max(0, int(r["total_amount"]) - offsets.for_pillar(r["pillar"]))
        for r in cur.fetchall()
    }
    need_spent = pillar_map.get("need", 0)
    want_spent = pillar_map.get("want", 0)
    culture_spent = pillar_map.get("culture", 0)
    unexpected_spent = pillar_map.get("unexpected", 0)
    saving_expenses = pillar_map.get("saving", 0)

    # 2. Net fresh saving across durably linked movements.
    cur.execute(
        """
        SELECT
            COALESCE(SUM(
                CASE
                    WHEN NOT (
                        sa.type = 'investment' OR sa.instrument_type IS NOT NULL
                        OR EXISTS (SELECT 1 FROM goal_accounts sga WHERE sga.account_id = sa.id)
                    ) AND (
                        da.type = 'investment' OR da.instrument_type IS NOT NULL
                        OR EXISTS (SELECT 1 FROM goal_accounts dga WHERE dga.account_id = da.id)
                    ) THEN outbound.amount
                    WHEN (
                        sa.type = 'investment' OR sa.instrument_type IS NOT NULL
                        OR EXISTS (SELECT 1 FROM goal_accounts sga WHERE sga.account_id = sa.id)
                    ) AND NOT (
                        da.type = 'investment' OR da.instrument_type IS NOT NULL
                        OR EXISTS (SELECT 1 FROM goal_accounts dga WHERE dga.account_id = da.id)
                    ) THEN -outbound.amount
                    ELSE 0
                END
            ), 0) AS saving_transfers
        FROM transactions outbound
        JOIN transactions inbound
          ON inbound.movement_id = outbound.movement_id
         AND inbound.movement_role = 'inbound'
        JOIN accounts sa ON sa.id = outbound.account_id
        JOIN accounts da ON da.id = inbound.account_id
        WHERE outbound.user_id = %s
          AND outbound.movement_role = 'outbound'
          AND outbound.kakeibo_type = 'saving'
          AND outbound.date >= %s AND outbound.date <= %s
        """,
        (user_id, start_utc, end_utc),
    )
    st_row = cur.fetchone()
    if st_row:
        if isinstance(st_row, dict):
            net_saving_transfers = int(st_row.get("saving_transfers") or st_row.get("net_saving_transfers") or 0)
        else:
            net_saving_transfers = int(st_row[0])
    else:
        net_saving_transfers = 0
    saving_spent = max(0, saving_expenses + net_saving_transfers)

    total_allocated = need_spent + want_spent + culture_spent + unexpected_spent + saving_spent
    base_calc = max(total_inflow, total_allocated)

    def pct(amount: int) -> float:
        return round((amount / base_calc) * 100, 1) if base_calc > 0 else 0.0

    need_pct = pct(need_spent)
    want_pct = pct(want_spent)
    culture_pct = pct(culture_spent)
    unexpected_pct = pct(unexpected_spent)
    saving_pct = pct(saving_spent)
    # The 50/30/20 benchmark is judged on groups: unexpected spending is an essential cost,
    # culture is discretionary lifestyle spending (owner decision, 2026-10-28).
    essentials_pct = pct(need_spent + unexpected_spent)
    lifestyle_pct = pct(want_spent + culture_spent)

    want_status = "warning" if lifestyle_pct > 30.0 else "healthy"
    kakeibo_status = "warning" if lifestyle_pct > 30.0 else ("elevated_needs" if essentials_pct > 50.0 else "healthy")

    return {
        "need_spent": need_spent,
        "need_pct": need_pct,
        "need_target_pct": 50.0,
        "want_spent": want_spent,
        "want_pct": want_pct,
        "want_target_pct": 30.0,
        "culture_spent": culture_spent,
        "culture_pct": culture_pct,
        "unexpected_spent": unexpected_spent,
        "unexpected_pct": unexpected_pct,
        "saving_spent": saving_spent,
        "saving_expenses": saving_expenses,
        "net_saving_transfers": net_saving_transfers,
        "saving_pct": saving_pct,
        "saving_target_pct": 20.0,
        "essentials_pct": essentials_pct,
        "essentials_target_pct": 50.0,
        "lifestyle_pct": lifestyle_pct,
        "lifestyle_target_pct": 30.0,
        "total_allocated": total_allocated,
        "kakeibo_status": kakeibo_status,
        "want_status": want_status,
    }


def emergency_fund_balance(cur, user_id: str, liquid_total: int, accounts: list[dict[str, Any]]) -> dict[str, Any]:
    """The designated emergency reserve, or all liquid cash when none is designated."""
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

    def goal_amount(goal: dict[str, Any]) -> int:
        linked_aids = g_linked_map.get(str(goal["id"]), [])
        if not linked_aids:
            return int(goal.get("current_amount", 0))
        linked_set = set(linked_aids)
        # A linked parent already contains its linked children.
        return sum(
            acc_balance_map.get(aid, 0)
            for aid in linked_aids
            if not acc_parent_map.get(aid) or acc_parent_map.get(aid) not in linked_set
        )

    emergency_goals = [g for g in all_goals if g.get("is_emergency")]
    if not emergency_goals:
        # Goals named as an emergency fund count as designated, as before the explicit flag existed.
        emergency_goals = [
            g for g in all_goals if any(k in str(g.get("name", "")).lower() for k in ("darurat", "emergency"))
        ]
    if emergency_goals:
        return {
            "balance": sum(goal_amount(g) for g in emergency_goals),
            "basis": "emergency_goals",
            "goal_names": [g["name"] for g in emergency_goals],
        }
    return {"balance": liquid_total, "basis": "all_liquid", "goal_names": []}


def calculate_ketahanan_dana(
    cur,
    user_id: str,
    liquid_total: int,
    accounts: list[dict[str, Any]],
    target_multiplier: int = DEFAULT_TARGET_MONTHS,
    *,
    essential: EssentialCost | None = None,
) -> dict[str, Any]:
    """Ketahanan Dana: the emergency balance in months of essential monthly cost, with its basis."""
    reserve = emergency_fund_balance(cur, user_id, liquid_total, accounts)
    essential = essential or estimate_essential_cost(cur, user_id)
    covered = coverage(reserve["balance"], essential, target_multiplier)
    return {
        "daily_burn_rate": covered["daily_burn_rate"],
        "runway_days": covered["runway_days"],
        "runway_months": covered["coverage_months"],
        "coverage_months": covered["coverage_months"],
        "target_months": float(target_multiplier or DEFAULT_TARGET_MONTHS),
        "target_amount": covered["target_amount"],
        "coverage_pct": covered["coverage_pct"],
        "status": covered["status"],
        "coverage_unavailable_reason": covered["coverage_unavailable_reason"],
        "emergency_fund_balance": reserve["balance"],
        "balance_basis": reserve["basis"],
        "monthly_primary_expense": essential.amount,
        "is_flagged": reserve["basis"] == "emergency_goals",
        "emergency_goal_names": reserve["goal_names"],
        # Retired 30-day basis; kept for older clients and never filled with an estimate.
        "primary_spent_30d": None,
        "monthly_commitments": essential.debt_minimum,
        "essential_cost": essential.describe(),
    }


@router.get("/overview")
def get_dashboard_overview(
    timeframe: str = Query(default="cycle", pattern=TIMEFRAME_PATTERN),
    cycle_offset: int = Query(default=0, ge=-24, le=12),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    payday_day = current_user.get("payday_day", 25)

    start_utc, end_utc, start_date, end_date, timeframe_label = get_timeframe_window(
        payday_day, timeframe, cycle_offset=cycle_offset, user_id=user_id
    )

    # 1. Accounts & Liquid Balances
    accounts = get_accounts_with_balances(user_id, include_archived=False)
    top_accounts = [account for account in accounts if account.get("parent_id") is None]
    balance_summary = summarize_account_balances(accounts)
    liquid_balance = balance_summary["liquid_balance"]
    investment_balance = balance_summary["investment_balance"]
    total_balance = balance_summary["total_balance"]
    liquid_net_worth = liquid_balance
    reconciliation_pending = balance_summary["reconciliation_pending"]

    emergency_multiplier = int(current_user.get("emergency_fund_multiplier") or DEFAULT_TARGET_MONTHS)
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
                f"""
                SELECT
                    COALESCE(SUM(CASE WHEN t.type = 'income' AND COALESCE(c.is_excluded_from_budget, false) = false AND COALESCE(c.name, '') NOT IN ('Internal Movement', 'Investasi') AND t.type != 'transfer' AND {not_reimbursement_sql()} THEN t.amount ELSE 0 END), 0) AS inflow,
                    COALESCE(SUM(CASE WHEN {reportable_expense_sql()} THEN t.amount ELSE 0 END), 0) AS outflow,
                    COUNT(*) AS tx_count
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
                """,
                (user_id, start_utc, end_utc),
            )
            totals_row = cur.fetchone()
            total_inflow = int(totals_row["inflow"])
            gross_outflow = int(totals_row["outflow"])
            offsets = load_spending_offsets(cur, user_id, start_utc, end_utc)
            flow = cash_flow_totals(total_inflow, gross_outflow, offsets.applied)
            total_outflow = flow["outflow"]
            net_cashflow = flow["net"]
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
                "SELECT COALESCE(SUM(monthly_budget), 0) AS total_budget FROM categories WHERE user_id = %s AND is_archived = false AND kind = 'expense' AND is_excluded_from_budget = false",
                (user_id,),
            )
            cat_budget = int(cur.fetchone()["total_budget"] or 0)
            effective_budget = monthly_spending_budget if (monthly_spending_budget and monthly_spending_budget > 0) else cat_budget

            tz = ZoneInfo(settings.tz)
            today_date = datetime.now(tz).date()

            # Identified commitments use an explicit horizon: an open salary cycle has no known end.
            cur.execute(
                """
                SELECT r.id, r.name, r.amount, r.next_due_date, o.name AS obligation_name
                FROM recurring_rules r
                LEFT JOIN obligations o ON o.id = r.obligation_id
                WHERE r.user_id = %s AND r.is_active = TRUE AND r.type = 'expense'
                  AND r.next_due_date <= %s
                ORDER BY r.next_due_date, r.name
                """,
                (user_id, today_date + timedelta(days=COMMITMENT_WINDOW_DAYS)),
            )
            upcoming_items = [
                {
                    "id": str(row["id"]),
                    "name": row["name"],
                    "amount": int(row["amount"]),
                    "due_date": row["next_due_date"].isoformat(),
                    "overdue": row["next_due_date"] < today_date,
                    "obligation_name": row["obligation_name"],
                }
                for row in cur.fetchall()
            ]
            upcoming_recurring_expense = sum(item["amount"] for item in upcoming_items)

            # A future salary date is unknown, so no daily allowance or pace is inferred.
            safe_to_spend_today = None
            benchmark_daily = None
            expected_spent = None
            velocity_ratio = None
            velocity_status = None
            has_budget = effective_budget > 0
            # A monthly budget only compares with month-length windows, not 30/90-day rolling ones.
            budget_remaining = effective_budget - total_outflow if has_budget and timeframe in ("month", "cycle") else None

            # 5. Day-by-Day Time Series for Cumulative Trendline & Burn Cadence
            cur.execute(
                f"""
                SELECT
                    (t.date AT TIME ZONE %s)::date AS tx_day,
                    COALESCE(SUM(CASE WHEN t.type = 'income' AND COALESCE(c.is_excluded_from_budget, false) = false AND {not_reimbursement_sql()} THEN t.amount ELSE 0 END), 0) AS income,
                    COALESCE(SUM(CASE WHEN {reportable_expense_sql()} THEN t.amount ELSE 0 END), 0) AS expense
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
                GROUP BY tx_day
                ORDER BY tx_day ASC
                """,
                (settings.tz, user_id, start_utc, end_utc),
            )
            grouped_days = {r["tx_day"]: r for r in cur.fetchall()}
            day_offsets = daily_offsets(cur, user_id, start_utc, end_utc, offsets)

            # Generate continuous series from start_date to end_date (or today)
            series_end_date = min(end_date, today_date) if timeframe in ("cycle", "month") else end_date

            cumulative_trendline = []
            burn_cadence = []
            running_inflow = 0
            running_outflow = 0

            curr = start_date
            while curr <= series_end_date:
                day_data = grouped_days.get(curr)
                day_income = int(day_data["income"]) if day_data else 0
                day_expense = (int(day_data["expense"]) if day_data else 0) - day_offsets.get(curr, 0)

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
            cat_rows = category_breakdown(cur, user_id, start_utc, end_utc, offsets)
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
                        "id": r["id"],
                        "name": r["name"],
                        "icon": r["icon"],
                        "color": r["color"],
                        "is_primary": r["is_primary"],
                        "kakeibo_type": r["kakeibo_type"],
                        "is_archived": r["is_archived"],
                        "is_uncategorized": r["is_uncategorized"],
                        "spent": spent,
                        "gross_spent": r["gross_spent"],
                        "reimbursed": r["gross_spent"] - spent,
                        "percentage_of_total": pct_total,
                        "budget": budget,
                        "percentage_used": pct_used,
                    }
                )

            # 6b. Period Comparison, Narrative, and Kakeibo
            prev_start_utc, prev_end_utc, prev_start_date, prev_end_date, prev_label = get_previous_timeframe_window(
                payday_day, timeframe, start_date, end_date, cycle_offset, user_id=user_id
            )

            cur.execute(
                f"""
                SELECT
                    COALESCE(SUM(CASE WHEN t.type = 'income' AND COALESCE(c.is_excluded_from_budget, false) = false AND COALESCE(c.name, '') NOT IN ('Internal Movement', 'Investasi') AND t.type != 'transfer' AND {not_reimbursement_sql()} THEN t.amount ELSE 0 END), 0) AS inflow,
                    COALESCE(SUM(CASE WHEN {reportable_expense_sql()} THEN t.amount ELSE 0 END), 0) AS outflow,
                    COUNT(*) AS tx_count
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
                """,
                (user_id, prev_start_utc, prev_end_utc),
            )
            prev_totals_row = cur.fetchone()
            prev_offsets = load_spending_offsets(cur, user_id, prev_start_utc, prev_end_utc)
            prev_inflow = int(prev_totals_row["inflow"]) if prev_totals_row else 0
            prev_outflow = (int(prev_totals_row["outflow"]) if prev_totals_row else 0) - prev_offsets.applied
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
                WHERE c.user_id = %s AND c.is_archived = false AND c.kind = 'expense' AND c.is_excluded_from_budget = false
                GROUP BY c.id, c.name
                """,
                (prev_start_utc, prev_end_utc, user_id),
            )
            prev_cat_map = {str(r["id"]): int(r["spent"]) - prev_offsets.for_category(r["id"]) for r in cur.fetchall()}

            top_driver_name = None
            top_driver_delta = 0.0
            top_driver_delta_available = False
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
                        top_driver_delta_available = prev_spent != 0
                else:
                    if -diff > max_impact and prev_spent > 0:
                        max_impact = -diff
                        top_driver_name = cname
                        top_driver_delta = calc_delta_pct(curr_spent, prev_spent)
                        top_driver_delta_available = prev_spent != 0

            if not top_driver_name and cat_rows and total_outflow > 0:
                top_cat = max(cat_rows, key=lambda x: int(x["spent"]))
                if int(top_cat["spent"]) > 0:
                    top_driver_name = top_cat["name"]
                    p_spent = prev_cat_map.get(str(top_cat["id"]), 0)
                    top_driver_delta = calc_delta_pct(int(top_cat["spent"]), p_spent)
                    top_driver_delta_available = p_spent != 0

            narrative_summary, trend_status = generate_financial_narrative(
                current_outflow=total_outflow,
                prev_outflow=prev_outflow,
                outflow_delta_pct=outflow_delta_pct,
                current_net=net_cashflow,
                top_driver_name=top_driver_name,
                top_driver_delta_pct=top_driver_delta if top_driver_delta_available else None,
            )

            kakeibo_data = get_kakeibo_breakdown(cur, user_id, start_utc, end_utc, total_inflow, offsets)

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
                WHERE user_id = %s AND is_archived = false AND remaining_amount > 0
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
                    t.type,
                    t.amount, t.notes, t.date, t.receipt_path,
                    COALESCE(c.is_excluded_from_budget, false) AS is_excluded_from_budget
                FROM transactions t
                JOIN accounts sa ON sa.id = t.account_id
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
                    "transfer_target_account_id": None,
                    "transfer_target_account_name": None,
                    "amount": r["amount"],
                    "notes": r["notes"],
                    "date": r["date"].isoformat() if r["date"] else None,
                    "receipt_path": r["receipt_path"],
                    "is_excluded_from_budget": bool(r.get("is_excluded_from_budget", False)),
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
            "reconciliation_pending": reconciliation_pending,
            "total_inflow": total_inflow,
            "total_outflow": total_outflow,
            **offsets.summary(gross_outflow),
            "net_cashflow": net_cashflow,
            "savings_rate": savings_rate,
            "safe_to_spend_today": safe_to_spend_today,
            "has_budget": has_budget,
            "effective_budget": effective_budget,
            "daily_burn_rate": daily_burn_rate,
            "runway_days": runway_days,
            "runway_months": runway_info["runway_months"],
            "coverage_months": runway_info["coverage_months"],
            "target_months": runway_info["target_months"],
            "target_amount": runway_info["target_amount"],
            "coverage_pct": runway_info["coverage_pct"],
            "runway_status": runway_info["status"],
            "emergency_fund_balance": runway_info["emergency_fund_balance"],
            "emergency_balance_basis": runway_info["balance_basis"],
            "monthly_primary_expense": runway_info["monthly_primary_expense"],
            "essential_cost": runway_info["essential_cost"],
            "is_emergency_flagged": runway_info["is_flagged"],
            "emergency_goal_names": runway_info["emergency_goal_names"],
            "benchmark_daily": benchmark_daily,
            "upcoming_recurring_expense": upcoming_recurring_expense,
            "budget_remaining": budget_remaining,
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
            "top_driver_delta_available": top_driver_delta_available,
        },
        "spend_velocity": {
            "velocity_ratio": velocity_ratio,
            "velocity_status": velocity_status,
            "benchmark_daily": benchmark_daily,
            "expected_spent": expected_spent,
            "upcoming_recurring_expense": upcoming_recurring_expense,
            "safe_to_spend_today": safe_to_spend_today,
        },
        "upcoming_commitments": {
            "window_days": COMMITMENT_WINDOW_DAYS,
            "total": upcoming_recurring_expense,
            "items": upcoming_items,
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
    timeframe: str = Query(default="cycle", pattern=TIMEFRAME_PATTERN),
    cycle_offset: int = Query(default=0, ge=-24, le=12),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    payday_day = current_user.get("payday_day", 25)

    start_utc, end_utc, start_date, end_date, timeframe_label = get_timeframe_window(
        payday_day, timeframe, cycle_offset=cycle_offset, user_id=user_id
    )

    with db_conn() as conn:
        with conn.cursor() as cur:
            analytics_offsets = load_spending_offsets(cur, user_id, start_utc, end_utc)

            # 1. Daily series: reportable spending net of reimbursements attributed to its days
            cur.execute(
                f"""
                SELECT
                    (t.date AT TIME ZONE %s)::date AS tx_day,
                    COALESCE(SUM(CASE WHEN {reportable_expense_sql()} THEN t.amount ELSE 0 END), 0) AS expense,
                    COUNT(*) FILTER (WHERE {reportable_expense_sql()}) AS expense_count,
                    COALESCE(SUM(CASE WHEN {reportable_income_sql()} THEN t.amount ELSE 0 END), 0) AS income
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
                GROUP BY tx_day
                ORDER BY tx_day ASC
                """,
                (settings.tz, user_id, start_utc, end_utc),
            )
            daily_rows = {r["tx_day"]: r for r in cur.fetchall()}
            day_offsets = daily_offsets(cur, user_id, start_utc, end_utc, analytics_offsets)

            daily_cadence = []
            dow_spent = [0] * 7
            dow_count = [0] * 7
            curr = start_date
            while curr <= end_date:
                r = daily_rows.get(curr)
                day_expense = (int(r["expense"]) if r else 0) - day_offsets.get(curr, 0)
                daily_cadence.append(
                    {
                        "date": curr.isoformat(),
                        "label": curr.strftime("%b %d"),
                        "expense": day_expense,
                        "income": int(r["income"]) if r else 0,
                    }
                )
                dow = sunday_first_weekday(curr)
                dow_spent[dow] += day_expense
                dow_count[dow] += int(r["expense_count"]) if r else 0
                curr += timedelta(days=1)

            # 2. Day of Week Heatmap Matrix (0=Sunday to 6=Saturday), from the same net daily figures
            day_names = ["Minggu", "Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"]
            day_short = ["Min", "Sen", "Sel", "Rab", "Kam", "Jum", "Sab"]
            day_of_week_heatmap = [
                {
                    "day_index": i,
                    "day_name": day_names[i],
                    "day_short": day_short[i],
                    "total_spent": dow_spent[i],
                    "tx_count": dow_count[i],
                    "average_spent": round(dow_spent[i] / dow_count[i]) if dow_count[i] > 0 else 0,
                }
                for i in range(7)
            ]

            # 3. Category Variance Matrix
            category_variance = []
            for r in category_breakdown(cur, user_id, start_utc, end_utc, analytics_offsets):
                spent = r["spent"]
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
                        "id": r["id"],
                        "name": r["name"],
                        "icon": r["icon"],
                        "color": r["color"],
                        "is_primary": r["is_primary"],
                        "kakeibo_type": r["kakeibo_type"],
                        "is_archived": r["is_archived"],
                        "is_uncategorized": r["is_uncategorized"],
                        "spent": spent,
                        "gross_spent": r["gross_spent"],
                        "reimbursed": r["reimbursed"],
                        "budget": budget,
                        "variance": variance,
                        "percentage_used": pct,
                        "status": status,
                    }
                )

            # 4. Burn Rate Analytics: rolling windows ending now, net of their own reimbursements
            now_utc = datetime.now(timezone.utc)
            burn_spent = {}
            for days in (7, 30):
                window_start = now_utc - timedelta(days=days)
                cur.execute(
                    f"""
                    SELECT COALESCE(SUM(t.amount), 0) AS spent
                    FROM transactions t
                    LEFT JOIN categories c ON c.id = t.category_id
                    WHERE t.user_id = %s AND {reportable_expense_sql()} AND t.date >= %s AND t.date <= %s
                    """,
                    (user_id, window_start, now_utc),
                )
                gross = int(cur.fetchone()["spent"])
                burn_spent[days] = gross - load_spending_offsets(cur, user_id, window_start, now_utc).applied
            spent_30d = burn_spent[30]
            burn_7d = round(burn_spent[7] / 7)
            burn_30d = round(spent_30d / 30)

            # 5. Inflow / Outflow & Period Comparison
            cur.execute(
                f"""
                SELECT
                    COALESCE(SUM(CASE WHEN {reportable_income_sql()} THEN t.amount ELSE 0 END), 0) AS inflow,
                    COALESCE(SUM(CASE WHEN {reportable_expense_sql()} THEN t.amount ELSE 0 END), 0) AS outflow
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
                """,
                (user_id, start_utc, end_utc),
            )
            totals_row = cur.fetchone() or {}
            total_inflow = int(totals_row.get("inflow", 0))
            gross_outflow = int(totals_row.get("outflow", 0))
            flow = cash_flow_totals(total_inflow, gross_outflow, analytics_offsets.applied)
            total_outflow = flow["outflow"]
            net_cashflow = flow["net"]
            savings_rate = (
                round(((total_inflow - total_outflow) / total_inflow) * 100, 1)
                if total_inflow > 0
                else 0.0
            )

            # Legacy forecast fields remain present but are unavailable.
            cycle_elapsed_days = None
            total_cycle_days = None
            projected_cycle_outflow = None

            prev_start_utc, prev_end_utc, prev_start_date, prev_end_date, prev_label = get_previous_timeframe_window(
                payday_day, timeframe, start_date, end_date, cycle_offset, user_id=user_id
            )

            cur.execute(
                f"""
                SELECT
                    COALESCE(SUM(CASE WHEN {reportable_income_sql()} THEN t.amount ELSE 0 END), 0) AS inflow,
                    COALESCE(SUM(CASE WHEN {reportable_expense_sql()} THEN t.amount ELSE 0 END), 0) AS outflow
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.user_id = %s AND t.date >= %s AND t.date <= %s
                """,
                (user_id, prev_start_utc, prev_end_utc),
            )
            prev_totals_row = cur.fetchone() or {}
            prev_offsets = load_spending_offsets(cur, user_id, prev_start_utc, prev_end_utc)
            prev_inflow = int(prev_totals_row.get("inflow", 0))
            prev_outflow = int(prev_totals_row.get("outflow", 0)) - prev_offsets.applied
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
                WHERE c.user_id = %s AND c.is_archived = false AND c.kind = 'expense' AND c.is_excluded_from_budget = false
                GROUP BY c.id, c.name
                """,
                (prev_start_utc, prev_end_utc, user_id),
            )
            prev_cat_map = {str(r["id"]): int(r["spent"]) - prev_offsets.for_category(r["id"]) for r in cur.fetchall()}

            top_driver_name = None
            top_driver_delta = 0.0
            top_driver_delta_available = False
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
                        top_driver_delta_available = prev_spent != 0
                else:
                    if -diff > max_impact and prev_spent > 0:
                        max_impact = -diff
                        top_driver_name = cname
                        top_driver_delta = calc_delta_pct(curr_spent, prev_spent)
                        top_driver_delta_available = prev_spent != 0

            if not top_driver_name and category_variance and total_outflow > 0:
                top_cat = max(category_variance, key=lambda x: int(x["spent"]))
                if int(top_cat["spent"]) > 0:
                    top_driver_name = top_cat["name"]
                    p_spent = prev_cat_map.get(str(top_cat["id"]), 0)
                    top_driver_delta = calc_delta_pct(int(top_cat["spent"]), p_spent)
                    top_driver_delta_available = p_spent != 0

            narrative_summary, trend_status = generate_financial_narrative(
                current_outflow=total_outflow,
                prev_outflow=prev_outflow,
                outflow_delta_pct=outflow_delta_pct,
                current_net=net_cashflow,
                top_driver_name=top_driver_name,
                top_driver_delta_pct=top_driver_delta if top_driver_delta_available else None,
            )

            kakeibo_data = get_kakeibo_breakdown(cur, user_id, start_utc, end_utc, total_inflow, analytics_offsets)

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
            # Legacy name for the observed trailing 30 days; it is not a projection.
            "projected_30d_outflow": spent_30d,
            "trailing_30d_outflow": spent_30d,
            "cycle_elapsed_days": cycle_elapsed_days,
            "cycle_total_days": total_cycle_days,
            "cycle_end_date": None,
        },
        "comparison": {
            "inflow_delta_pct": inflow_delta_pct,
            "outflow_delta_pct": outflow_delta_pct,
            "net_cashflow_delta_pct": net_cashflow_delta_pct,
            "savings_rate_delta_pts": savings_rate_delta_pts,
            "total_inflow": total_inflow,
            "total_outflow": total_outflow,
            **analytics_offsets.summary(gross_outflow),
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
            "top_driver_delta_available": top_driver_delta_available,
        },
    }


@router.get("/net-worth")
def get_dashboard_net_worth(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    emergency_multiplier = int(current_user.get("emergency_fund_multiplier") or DEFAULT_TARGET_MONTHS)

    # 1. Liquid Accounts (Assets)
    accounts = get_accounts_with_balances(user_id, include_archived=False)
    total_assets = sum(a["balance"] for a in accounts if a.get("parent_id") is None)
    liquid_balance = summarize_account_balances(accounts)["liquid_balance"]

    with db_conn() as conn:
        with conn.cursor() as cur:
            # 2. Obligations (Liabilities)
            cur.execute(
                """
                SELECT id, name, total_amount, remaining_amount, due_date, minimum_payment, notes
                FROM obligations
                WHERE user_id = %s AND is_archived = false AND remaining_amount > 0
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
            # The no-reserve fallback is liquid cash, as on the overview; investments are not a reserve.
            runway_info = calculate_ketahanan_dana(
                cur, user_id, liquid_balance, accounts, target_multiplier=emergency_multiplier
            )

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


@router.get("/period-comparison")
def get_period_comparison(
    timeframe: str = Query(default="cycle", pattern=PERIOD_PATTERN),
    cycle_offset: int = Query(default=0, ge=-24, le=12),
    current_user: dict = Depends(get_current_user),
):
    """The selected period against the previous one, both measured over the same elapsed time."""
    user_id = current_user["id"]
    payday_day = current_user.get("payday_day", 25)
    start_utc, end_utc, start_date, end_date, label = get_timeframe_window(
        payday_day, timeframe, cycle_offset=cycle_offset, user_id=user_id
    )
    prev_start, prev_end, _, _, prev_label = get_previous_timeframe_window(
        payday_day, timeframe, start_date, end_date, cycle_offset, user_id=user_id
    )
    previous = None if prev_end < prev_start else (prev_start, prev_end, prev_label)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            comparison = period_comparison(cur, user_id, current=(start_utc, end_utc, label), previous=previous)
    return {"ok": True, "timeframe": timeframe, "timezone": settings.tz, **comparison}


@router.get("/cash-flow-history")
def get_cash_flow_history(
    timeframe: str = Query(default="cycle", pattern=PERIOD_PATTERN),
    current_user: dict = Depends(get_current_user),
):
    """Observed inflow, outflow and net for the last completed periods and the one in progress."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            history = cash_flow_history(cur, current_user["id"], basis="month" if timeframe == "month" else "auto")
    return {"ok": True, "timezone": settings.tz, **history}


@router.get("/recurring-charges")
def get_recurring_charges(current_user: dict = Depends(get_current_user)):
    """Charges that recurred weekly or monthly with a steady amount over the last six months."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            charges = recurring_charges(cur, current_user["id"])
    return {"ok": True, "timezone": settings.tz, **charges}


@router.get("/question-rate")
def get_question_rate(
    weeks: int = Query(default=8, ge=1, le=MAX_WEEKS),
    current_user: dict = Depends(get_current_user),
):
    """Per local week: recorded payments, how many the owner was asked about and who recorded the rest."""
    today = datetime.now(ZoneInfo(settings.tz)).date()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            rate = question_rate(cur, current_user["id"], weeks=weeks, today=today, tz=settings.tz)
    return {"ok": True, "timezone": settings.tz, **rate}
