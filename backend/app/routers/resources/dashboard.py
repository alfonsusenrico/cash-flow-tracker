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

        # ── Emergency bucket balance ──────────────────────────────────────
        cur.execute(
            """
            SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS balance
            FROM buckets b
            JOIN accounts a ON a.account_id = b.linked_account_id
            JOIN transactions t ON t.account_id = a.account_id AND t.deleted_at IS NULL
            JOIN users u ON u.user_id = b.user_id
            WHERE u.username=%s AND b.kind='emergency' AND b.is_archived=FALSE
            """,
            (username,),
        )
        emergency_row = cur.fetchone()
        emergency_balance = int(emergency_row["balance"] or 0) if emergency_row else 0

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
              AND t.date >= %s AND t.date <= %s
            """,
            (username, from_dt, to_dt),
        )
        totals = cur.fetchone() or {}
        total_in = int(totals.get("total_in") or 0)
        total_out = int(totals.get("total_out") or 0)

        # ── Avg monthly expense (last 3 months) ───────────────────────────
        cur.execute(
            """
            SELECT COALESCE(AVG(monthly_out), 0) AS avg_out
            FROM (
                SELECT DATE_TRUNC('month', t.date) AS m,
                       SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END) AS monthly_out
                FROM transactions t
                JOIN accounts a ON a.account_id=t.account_id
                JOIN users u ON u.user_id=a.user_id
                WHERE u.username=%s AND t.deleted_at IS NULL AND t.transfer_id IS NULL
                  AND t.date >= (now() - INTERVAL '3 months')
                GROUP BY m
            ) sub
            """,
            (username,),
        )
        avg_row = cur.fetchone()
        avg_monthly_expense = int(avg_row["avg_out"] or 0) if avg_row else 0

        # ── Planned spend (from active allocation plan) ───────────────────
        cur.execute(
            """
            SELECT COALESCE(SUM(i.planned_amount), 0) AS planned_spend
            FROM allocation_plans p
            JOIN allocation_items i ON i.plan_id = p.plan_id
            JOIN users u ON u.user_id = p.user_id
            WHERE u.username=%s AND p.month=%s AND p.status='active'
            """,
            (username, month),
        )
        plan_row = cur.fetchone()
        planned_spend = int(plan_row["planned_spend"] or 0) if plan_row else 0

        # ── Committed allocations (pending items in active plan) ──────────
        cur.execute(
            """
            SELECT COALESCE(SUM(i.planned_amount - i.funded_amount), 0) AS committed
            FROM allocation_plans p
            JOIN allocation_items i ON i.plan_id = p.plan_id
            JOIN users u ON u.user_id = p.user_id
            WHERE u.username=%s AND p.month=%s AND p.status='active'
              AND i.status IN ('pending','partial')
            """,
            (username, month),
        )
        committed_row = cur.fetchone()
        committed = int(committed_row["committed"] or 0) if committed_row else 0

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
            SELECT g.goal_id::text, g.name, g.target_amount, g.current_amount,
                   g.target_date, g.inflation_rate, g.expected_return
            FROM financial_goals g
            JOIN users u ON u.user_id=g.user_id
            WHERE u.username=%s AND g.status='active'
            ORDER BY g.priority ASC
            """,
            (username,),
        )
        goals_rows = cur.fetchall()

    # ── Compute metrics ───────────────────────────────────────────────────
    net_worth = liquid_total + invested
    available_monthly = max(0, total_in - total_out)

    metrics = {
        "safe_to_spend": safe_to_spend(liquid_total, committed, 0),
        "emergency_fund": emergency_fund_coverage(emergency_balance, avg_monthly_expense),
        "savings_rate": savings_rate(total_in, total_out),
        "investment_rate": investment_rate(invested, total_in),
        "cash_runway": cash_runway(liquid_total, avg_monthly_expense),
        "monthly_drift": monthly_drift(planned_spend, total_out) if planned_spend > 0 else None,
    }

    # Goal feasibility
    goal_metrics = []
    for g in goals_rows:
        proj = goal_projection(
            target_amount=int(g["target_amount"]),
            current_amount=int(g["current_amount"]),
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
    for key, m in metrics.items():
        if m and m["status"] == "critical":
            warnings.append({"key": key, "label": m["label"], "severity": "critical"})
        elif m and m["status"] == "warn":
            warnings.append({"key": key, "label": m["label"], "severity": "warn"})

    return {
        "month": month,
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
