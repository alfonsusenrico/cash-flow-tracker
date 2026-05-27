"""Financial Health Dashboard endpoint — /dashboard and /v1/dashboard"""
from fastapi import APIRouter, Request
from app.db.pool import db_conn
from app.services.ledger.balances import get_account_balances
from app.services.ledger.period import now_utc, current_month_local, get_payday_day, prev_month_str, compute_dynamic_month_range
from app.services.projection import goal_projection
from app.services.metrics import (
    safe_to_spend,
    emergency_fund_coverage,
    savings_rate,
    investment_rate,
    cash_runway,
    monthly_drift,
    goal_feasibility,
)

router = APIRouter(tags=["dashboard"])


def _allocation_group(bucket_kind: str | None, mode: str) -> str:
    if bucket_kind == "investment":
        return "investment"
    if bucket_kind in ("emergency", "goal", "sinking"):
        return "savings_goal"
    if bucket_kind == "spending":
        return "dynamic_spending"
    if mode == "fixed":
        return "fixed_spending"
    return "dynamic_spending"


def _with_source(metric: dict, source: str, breakdown: dict | None = None) -> dict:
    metric["source"] = source
    if breakdown is not None:
        metric["breakdown"] = breakdown
    return metric


def _emergency_bucket_balance(cur, username: str, balances: dict[str, int]) -> int:
    cur.execute(
        """
        SELECT b.bucket_id::text AS bucket_id,
               COALESCE(
                 array_agg(a.account_id::text ORDER BY a.account_name)
                   FILTER (WHERE a.account_id IS NOT NULL),
                 ARRAY[]::text[]
               ) AS linked_account_ids
        FROM buckets b
        JOIN users u ON u.user_id = b.user_id
        LEFT JOIN LATERAL (
            SELECT account_id FROM bucket_accounts WHERE bucket_id = b.bucket_id
            UNION
            SELECT b.linked_account_id WHERE b.linked_account_id IS NOT NULL
        ) ba ON TRUE
        LEFT JOIN accounts a ON a.account_id = ba.account_id AND a.username = u.username
        WHERE u.username=%s AND b.kind='emergency' AND b.is_archived=FALSE
        GROUP BY b.bucket_id
        """,
        (username,),
    )
    total = 0
    for bucket in cur.fetchall():
        for account_id in bucket.get("linked_account_ids") or []:
            total += int(balances.get(account_id, 0))
    return total


@router.get("")
def get_dashboard(req: Request):
    username = req.state.username
    now = now_utc()
    month = current_month_local()

    with db_conn() as conn, conn.cursor() as cur:
        # ── Period range ──────────────────────────────────────────────────
        payday_day, _, _ = get_payday_day(cur, username, month)
        prev_day, _, _ = get_payday_day(cur, username, prev_month_str(month))
        from_date, to_date, from_dt, to_dt = compute_dynamic_month_range(cur, username, month, payday_day, prev_day)

        # ── Liquid balance ────────────────────────────────────────────────
        balances = get_account_balances(cur, username, now)
        liquid_total = sum(balances.values())

        cur.execute(
            """
            SELECT account_id::text AS account_id,
                   profile_type,
                   is_buffer,
                   is_no_limit
            FROM accounts
            WHERE username=%s
            """,
            (username,),
        )
        account_profiles = {row["account_id"]: row for row in cur.fetchall()}
        spendable_balance = sum(
            int(balance)
            for account_id, balance in balances.items()
            if not account_profiles.get(account_id, {}).get("is_buffer")
            and account_profiles.get(account_id, {}).get("profile_type") in ("fixed_spending", "dynamic_spending")
        )

        # ── Emergency bucket balance ──────────────────────────────────────
        emergency_balance = _emergency_bucket_balance(cur, username, balances)

        # ── This month income + expense ───────────────────────────────────
        cur.execute(
            """
            SELECT
              COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
              COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            JOIN users u ON u.user_id=a.user_id
            WHERE u.username=%s AND t.deleted_at IS NULL
              AND t.transfer_id IS NULL
              AND t.transaction_name <> 'Opening Balance'
              AND t.date >= %s AND t.date <= %s
            """,
            (username, from_dt, to_dt),
        )
        totals = cur.fetchone() or {}
        total_in = int(totals.get("total_in") or 0)
        total_out = int(totals.get("total_out") or 0)

        # ── Latest active allocation plan, draft fallback for new users ───
        cur.execute(
            """
            SELECT p.plan_id::text AS plan_id,
                   p.month,
                   p.status,
                   p.expected_income
            FROM allocation_plans p
            JOIN users u ON u.user_id = p.user_id
            WHERE u.username=%s
              AND p.status IN ('active','draft')
            ORDER BY CASE WHEN p.status='active' THEN 0 ELSE 1 END,
                     p.month DESC,
                     p.created_at DESC
            LIMIT 1
            """,
            (username,),
        )
        plan_row = cur.fetchone()
        expected_income = int(plan_row.get("expected_income") or 0) if plan_row else total_in

        allocation_items = []
        if plan_row:
            cur.execute(
                """
                SELECT i.planned_amount,
                       i.funded_amount,
                       i.status,
                       i.mode,
                       i.include_in_emergency_base,
                       b.kind AS bucket_kind
                FROM allocation_items i
                LEFT JOIN buckets b ON b.bucket_id=i.bucket_id
                WHERE i.plan_id=%s::uuid
                """,
                (plan_row["plan_id"],),
            )
            allocation_items = cur.fetchall()

        planned_spend = 0
        planned_savings = 0
        planned_investment = 0
        emergency_base = 0
        committed = 0
        for item in allocation_items:
            planned = int(item.get("planned_amount") or 0)
            group = _allocation_group(item.get("bucket_kind"), item.get("mode") or "fixed")
            if item.get("include_in_emergency_base") or group in ("fixed_spending", "dynamic_spending"):
                planned_spend += planned
            if item.get("include_in_emergency_base"):
                emergency_base += planned
            if group == "investment":
                planned_investment += planned
            if group == "savings_goal":
                planned_savings += planned
            if group == "cash_buffer":
                planned_savings += planned
            if item.get("status") in ("pending", "partial"):
                committed += max(0, planned - int(item.get("funded_amount") or 0))

        # ── Invested assets ───────────────────────────────────────────────
        cur.execute(
            """
            SELECT COALESCE(SUM(h.quantity * COALESCE(s.unit_price, 0)), 0) AS invested
            FROM asset_holdings h
            JOIN assets a ON a.asset_id = h.asset_id
            JOIN users u ON u.user_id = h.user_id
            LEFT JOIN LATERAL (
                SELECT unit_price FROM asset_snapshots WHERE asset_id=h.asset_id ORDER BY as_of_date DESC LIMIT 1
            ) s ON TRUE
            WHERE u.username=%s AND a.is_active=TRUE
            """,
            (username,),
        )
        inv_row = cur.fetchone()
        invested = int(inv_row["invested"] or 0) if inv_row else 0

        # ── Goals ─────────────────────────────────────────────────────────
        cur.execute(
            """
            SELECT g.goal_id::text,
                   g.name,
                   g.target_amount,
                   g.current_amount,
                   g.target_date,
                   g.inflation_rate,
                   g.expected_return,
                   g.linked_bucket_id::text AS linked_bucket_id,
                   COALESCE(
                     array_agg(a.account_id::text ORDER BY a.account_name)
                       FILTER (WHERE a.account_id IS NOT NULL),
                     ARRAY[]::text[]
                   ) AS linked_account_ids
            FROM financial_goals g
            JOIN users u ON u.user_id=g.user_id
            LEFT JOIN buckets b ON b.bucket_id=g.linked_bucket_id
            LEFT JOIN LATERAL (
                SELECT account_id FROM bucket_accounts WHERE bucket_id = b.bucket_id
                UNION
                SELECT b.linked_account_id WHERE b.linked_account_id IS NOT NULL
            ) ba ON TRUE
            LEFT JOIN accounts a ON a.account_id=ba.account_id AND a.username=u.username
            WHERE u.username=%s AND g.status='active'
            GROUP BY g.goal_id, g.name, g.target_amount, g.current_amount,
                     g.target_date, g.inflation_rate, g.expected_return, g.linked_bucket_id
            ORDER BY g.priority ASC
            """,
            (username,),
        )
        goals_rows = cur.fetchall()

    # ── Compute metrics ───────────────────────────────────────────────────
    net_worth = liquid_total + invested
    monthly_income = expected_income if expected_income > 0 else total_in
    available_monthly = max(0, monthly_income - total_out)
    remaining_spend_budget = max(0, planned_spend - total_out)
    safe_value = min(spendable_balance, remaining_spend_budget) if plan_row else max(0, liquid_total - committed)
    safe_metric = safe_to_spend(safe_value, 0, 0)
    if monthly_income > 0:
        safe_metric["pct"] = round(safe_value / monthly_income * 100, 1)

    metrics = {
        "safe_to_spend": _with_source(
            safe_metric,
            "allocation" if plan_row else "balance",
            {
                "spendable_balance": spendable_balance,
                "planned_spending": planned_spend,
                "actual_spending": total_out,
                "remaining_spend_budget": remaining_spend_budget,
                "committed_allocations": committed,
            },
        ),
        "emergency_fund": _with_source(
            emergency_fund_coverage(emergency_balance, emergency_base),
            "allocation" if plan_row else "missing_allocation",
            {"emergency_balance": emergency_balance, "monthly_spending_base": emergency_base, "target_months": 6},
        ),
        "savings_rate": _with_source(
            savings_rate(monthly_income, max(0, monthly_income - planned_savings)),
            "allocation" if plan_row else "transactions",
            {"planned_savings": planned_savings, "income": monthly_income},
        ),
        "investment_rate": _with_source(
            investment_rate(planned_investment, monthly_income),
            "allocation" if plan_row else "missing_allocation",
            {"planned_investment": planned_investment, "income": monthly_income},
        ),
        "cash_runway": _with_source(
            cash_runway(liquid_total, emergency_base),
            "allocation" if plan_row else "missing_allocation",
            {"liquid_balance": liquid_total, "monthly_spending_base": emergency_base},
        ),
        "monthly_drift": monthly_drift(planned_spend, total_out) if planned_spend > 0 else None,
    }

    # Goal feasibility
    goal_metrics = []
    for g in goals_rows:
        current_amount = int(g["current_amount"])
        if g.get("linked_bucket_id"):
            current_amount = sum(int(balances.get(account_id, 0)) for account_id in (g.get("linked_account_ids") or []))
        proj = goal_projection(
            target_amount=int(g["target_amount"]),
            current_amount=current_amount,
            target_date=g["target_date"],
            inflation_rate=float(g["inflation_rate"]),
            expected_return=float(g["expected_return"]),
        )
        goal_metrics.append({
            **goal_feasibility(g["name"], proj["required_monthly"], available_monthly),
            "progress_pct": proj["progress_pct"],
            "eta_months": proj["eta_months"],
        })

    # Overall health score (0–100)
    status_scores = {"ok": 100, "warn": 50, "critical": 0}
    scored = [m for m in metrics.values() if m is not None]
    health_score = int(sum(status_scores.get(m["status"], 50) for m in scored) / max(len(scored), 1))

    # Warnings
    warnings = []
    if not plan_row:
        warnings.append({"key": "allocation_plan", "label": "Create or activate an allocation plan to calculate health metrics accurately.", "severity": "warn"})
    elif planned_spend <= 0:
        warnings.append({"key": "spending_plan", "label": "Add spending allocation items to calculate safe-to-spend and runway.", "severity": "warn"})
    for key, m in metrics.items():
        if m and m["status"] == "critical":
            warnings.append({"key": key, "label": m["label"], "severity": "critical"})
        elif m and m["status"] == "warn":
            warnings.append({"key": key, "label": m["label"], "severity": "warn"})

    return {
        "month": month,
        "allocation_plan": {
            "plan_id": plan_row["plan_id"],
            "month": plan_row["month"],
            "status": plan_row["status"],
            "expected_income": expected_income,
        } if plan_row else None,
        "range": {"from": from_date, "to": to_date},
        "health_score": health_score,
        "net_worth": net_worth,
        "liquid_assets": liquid_total,
        "invested_assets": invested,
        "total_in": total_in,
        "total_out": total_out,
        "metrics": metrics,
        "goals": goal_metrics,
        "warnings": warnings,
    }
