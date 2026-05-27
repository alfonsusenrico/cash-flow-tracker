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
                   account_name,
                   profile_type,
                   is_buffer,
                   is_no_limit
            FROM accounts
            WHERE username=%s
            """,
            (username,),
        )
        account_rows = cur.fetchall()
        spendable_accounts = []
        for account in account_rows:
            account_id = account["account_id"]
            if account.get("is_buffer") or account.get("profile_type") not in ("fixed_spending", "dynamic_spending"):
                continue
            spendable_accounts.append(
                {
                    "account_id": account_id,
                    "account_name": account.get("account_name"),
                    "profile_type": account.get("profile_type"),
                    "is_no_limit": bool(account.get("is_no_limit")),
                    "balance": int(balances.get(account_id, 0)),
                }
            )
        spendable_accounts.sort(key=lambda row: (row.get("account_name") or "").lower())
        spendable_balance = sum(account["balance"] for account in spendable_accounts)

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
                SELECT i.item_id::text AS item_id,
                       i.label,
                       i.planned_amount,
                       i.funded_amount,
                       i.status,
                       i.mode,
                       i.include_in_emergency_base,
                       i.target_account_id::text AS target_account_id,
                       ta.account_name AS target_account_name,
                       b.kind AS bucket_kind,
                       b.name AS bucket_name
                FROM allocation_items i
                LEFT JOIN buckets b ON b.bucket_id=i.bucket_id
                LEFT JOIN accounts ta ON ta.account_id=i.target_account_id AND ta.username=%s
                WHERE i.plan_id=%s::uuid
                """,
                (username, plan_row["plan_id"]),
            )
            allocation_items = cur.fetchall()

        # ── External payables/receivables ────────────────────────────────
        cur.execute(
            """
            SELECT
              COALESCE(SUM(outstanding_amount) FILTER (WHERE kind='receivable' AND status IN ('open','partial')), 0) AS receivable_outstanding,
              COALESCE(SUM(outstanding_amount) FILTER (WHERE kind='payable' AND status IN ('open','partial')), 0) AS payable_outstanding,
              COALESCE(SUM(outstanding_amount) FILTER (WHERE kind='receivable' AND status IN ('open','partial') AND due_date < CURRENT_DATE), 0) AS receivable_overdue,
              COALESCE(SUM(outstanding_amount) FILTER (WHERE kind='payable' AND status IN ('open','partial') AND due_date < CURRENT_DATE), 0) AS payable_overdue,
              COALESCE(SUM(outstanding_amount) FILTER (WHERE kind='payable' AND status IN ('open','partial') AND due_date IS NOT NULL AND due_date <= %s::date), 0) AS payable_due_this_cycle,
              COUNT(*) FILTER (WHERE kind='payable' AND status IN ('open','partial') AND due_date IS NOT NULL AND due_date <= %s::date) AS payable_due_this_cycle_count,
              COALESCE(SUM(outstanding_amount) FILTER (WHERE status IN ('open','partial') AND due_date >= CURRENT_DATE AND due_date <= CURRENT_DATE + INTERVAL '30 days'), 0) AS due_soon,
              COUNT(*) FILTER (WHERE status IN ('open','partial')) AS open_count
            FROM obligations
            WHERE user_id=(SELECT user_id FROM users WHERE username=%s)
            """,
            (to_date, to_date, username),
        )
        obligation_row = cur.fetchone() or {}
        obligations = {
            "receivable_outstanding": int(obligation_row.get("receivable_outstanding") or 0),
            "payable_outstanding": int(obligation_row.get("payable_outstanding") or 0),
            "receivable_overdue": int(obligation_row.get("receivable_overdue") or 0),
            "payable_overdue": int(obligation_row.get("payable_overdue") or 0),
            "payable_due_this_cycle": int(obligation_row.get("payable_due_this_cycle") or 0),
            "payable_due_this_cycle_count": int(obligation_row.get("payable_due_this_cycle_count") or 0),
            "due_soon": int(obligation_row.get("due_soon") or 0),
            "open_count": int(obligation_row.get("open_count") or 0),
        }
        obligations["net_expected"] = obligations["receivable_outstanding"] - obligations["payable_outstanding"]

        cur.execute(
            """
            SELECT o.obligation_id::text AS obligation_id,
                   o.title,
                   o.due_date,
                   o.outstanding_amount,
                   c.name AS counterparty_name
            FROM obligations o
            LEFT JOIN counterparties c ON c.counterparty_id=o.counterparty_id
            WHERE o.user_id=(SELECT user_id FROM users WHERE username=%s)
              AND o.kind='payable'
              AND o.status IN ('open','partial')
              AND o.due_date IS NOT NULL
              AND o.due_date <= %s::date
            ORDER BY o.due_date ASC, o.outstanding_amount DESC, o.title ASC
            LIMIT 10
            """,
            (username, to_date),
        )
        payables_due = [
            {
                "obligation_id": row["obligation_id"],
                "title": row.get("title"),
                "due_date": row["due_date"].isoformat() if row.get("due_date") else None,
                "outstanding_amount": int(row.get("outstanding_amount") or 0),
                "counterparty_name": row.get("counterparty_name"),
            }
            for row in cur.fetchall()
        ]

        planned_spend = 0
        planned_savings = 0
        planned_investment = 0
        emergency_base = 0
        committed = 0
        spending_allocations = []
        for item in allocation_items:
            planned = int(item.get("planned_amount") or 0)
            funded = int(item.get("funded_amount") or 0)
            group = _allocation_group(item.get("bucket_kind"), item.get("mode") or "fixed")
            if item.get("include_in_emergency_base") or group in ("fixed_spending", "dynamic_spending"):
                planned_spend += planned
                spending_allocations.append(
                    {
                        "item_id": item.get("item_id"),
                        "label": item.get("label"),
                        "group": group,
                        "bucket_kind": item.get("bucket_kind"),
                        "bucket_name": item.get("bucket_name"),
                        "target_account_id": item.get("target_account_id"),
                        "target_account_name": item.get("target_account_name"),
                        "planned_amount": planned,
                        "funded_amount": funded,
                        "remaining_amount": max(0, planned - funded),
                        "include_in_emergency_base": bool(item.get("include_in_emergency_base")),
                        "status": item.get("status"),
                    }
                )
            if item.get("include_in_emergency_base"):
                emergency_base += planned
            if group == "investment":
                planned_investment += planned
            if group == "savings_goal":
                planned_savings += planned
            if group == "cash_buffer":
                planned_savings += planned
            if item.get("status") in ("pending", "partial"):
                committed += max(0, planned - funded)

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
    payable_due_this_cycle = obligations["payable_due_this_cycle"]
    capped_available = min(spendable_balance, remaining_spend_budget) if plan_row else max(0, liquid_total - committed)
    safe_value = max(0, capped_available - payable_due_this_cycle)
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
                "payables_due_this_cycle": payable_due_this_cycle,
                "payables_due_this_cycle_count": obligations["payable_due_this_cycle_count"],
                "capped_available": capped_available,
                "final_safe_to_spend": safe_value,
                "spendable_accounts": spendable_accounts,
                "spending_allocations": spending_allocations,
                "payables_due": payables_due,
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
        "obligations": obligations,
    }
