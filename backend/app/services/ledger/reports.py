"""Reporting helpers: daily/weekly series, budget status, budget-shift analysis,
financial safety report, summary computation, and audit writing.

BUG FIX: daily series now groups by local date (settings.tz) not UTC date.
"""
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException

from app.core.config import settings
from app.services.ledger.balances import get_account_balances
from app.services.ledger.period import APP_TZ, now_utc


def build_search_pattern(query: str | None) -> str | None:
    if not query:
        return None
    cleaned = query.strip().lower()[:64]
    return f"%{cleaned}%" if cleaned else None


def build_daily_series(from_date: str, to_date: str, rows: list[dict[str, Any]]) -> list[dict[str, int | str]]:
    from datetime import date as date_type
    start = datetime.fromisoformat(from_date).date()
    end = datetime.fromisoformat(to_date).date()
    by_day: dict[str, dict[str, int]] = {}
    for row in rows:
        day_val = row.get("day")
        day_key = day_val.isoformat() if hasattr(day_val, "isoformat") else str(day_val)
        by_day[day_key] = {
            "total_in": int(row.get("total_in") or 0),
            "total_out": int(row.get("total_out") or 0),
        }
    series: list[dict[str, int | str]] = []
    cursor = start
    while cursor <= end:
        key = cursor.isoformat()
        totals = by_day.get(key, {"total_in": 0, "total_out": 0})
        ti = int(totals.get("total_in") or 0)
        to_ = int(totals.get("total_out") or 0)
        series.append({"date": key, "total_in": ti, "total_out": to_, "net": ti - to_})
        cursor += timedelta(days=1)
    return series


def build_weekly_series(from_date: str, to_date: str, daily: list[dict[str, int | str]]) -> list[dict[str, int | str]]:
    start = datetime.fromisoformat(from_date).date()
    end = datetime.fromisoformat(to_date).date()
    by_day = {str(r.get("date")): r for r in daily if r.get("date")}
    series: list[dict[str, int | str]] = []
    cursor = start
    while cursor <= end:
        period_to = min(end, cursor + timedelta(days=6))
        ti = to_ = 0
        day = cursor
        while day <= period_to:
            r = by_day.get(day.isoformat(), {})
            ti += int(r.get("total_in") or 0)
            to_ += int(r.get("total_out") or 0)
            day += timedelta(days=1)
        series.append({"from": cursor.isoformat(), "to": period_to.isoformat(), "total_in": ti, "total_out": to_, "net": ti - to_})
        cursor = period_to + timedelta(days=1)
    return series


def compute_budget_status(budget_amount: int | None, used_amount: int) -> tuple[int | None, str | None, int | None]:
    if budget_amount is None:
        return None, None, None
    if budget_amount <= 0:
        return 100, "critical", int(budget_amount - used_amount)
    pct = int(round((used_amount / budget_amount) * 100))
    status = "critical" if pct >= 100 else "warn" if pct >= 80 else "ok"
    return pct, status, int(budget_amount - used_amount)


def compute_summary(cur, username: str, acc_by_id: dict[str, dict[str, Any]], to_dt: datetime) -> tuple[list[dict[str, Any]], int]:
    balances_all = get_account_balances(cur, username, to_dt)
    summary_accounts = [
        {"account_id": aid, "account_name": acc_by_id[aid]["account_name"], "balance": int(balances_all.get(aid, 0))}
        for aid in sorted(acc_by_id.keys(), key=lambda x: acc_by_id[x]["account_name"].lower())
    ]
    total_asset = sum(int(balances_all.get(aid, 0)) for aid in acc_by_id.keys())
    return summary_accounts, int(total_asset)


def write_transaction_audit(cur, *, username: str, performed_by: str, action: str, tx_row: dict[str, Any]) -> None:
    payload = json.dumps(
        {
            "transaction_id": tx_row.get("transaction_id"),
            "account_id": tx_row.get("account_id"),
            "transaction_type": tx_row.get("transaction_type"),
            "transaction_name": tx_row.get("transaction_name"),
            "amount": int(tx_row.get("amount") or 0),
            "date": tx_row["date"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if tx_row.get("date") else None,
            "is_transfer": bool(tx_row.get("is_transfer")),
            "is_cycle_topup": bool(tx_row.get("is_cycle_topup")),
            "transfer_id": tx_row.get("transfer_id"),
            "deleted_at": tx_row["deleted_at"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if tx_row.get("deleted_at") else None,
            "deleted_by": tx_row.get("deleted_by"),
            "delete_reason": tx_row.get("delete_reason"),
        },
        separators=(",", ":"),
    )
    cur.execute(
        """
        INSERT INTO transaction_audit (transaction_id, account_id, user_id, username, action, payload, performed_by)
        VALUES (%s::uuid, %s::uuid, (SELECT user_id FROM users WHERE username=%s), %s, %s, %s::jsonb, %s)
        """,
        (tx_row.get("transaction_id"), tx_row.get("account_id"), username, username, action, payload, performed_by),
    )


def compute_financial_safety_report(cur, username: str, lookback_hours: int = 24) -> dict[str, Any]:
    hours = max(1, min(168, int(lookback_hours or 24)))
    generated_at = now_utc()
    since = generated_at - timedelta(hours=hours)

    # Negative accounts — no longer uses opening_balance (dropped in V10)
    cur.execute(
        """
        SELECT a.account_id::text AS account_id,
               a.account_name,
               COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS balance
        FROM accounts a
        LEFT JOIN transactions t ON t.account_id=a.account_id AND t.deleted_at IS NULL
        WHERE a.username=%s
        GROUP BY a.account_id, a.account_name
        HAVING COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) < 0
        ORDER BY balance ASC
        LIMIT 50
        """,
        (username,),
    )
    negative_accounts = [
        {"account_id": r["account_id"], "account_name": r["account_name"], "balance": int(r.get("balance") or 0)}
        for r in cur.fetchall()
    ]

    cur.execute(
        """
        WITH grp AS (
            SELECT t.transfer_id::text AS transfer_id,
                   COUNT(*) AS row_count,
                   COUNT(*) FILTER (WHERE t.transaction_type='debit') AS debit_count,
                   COUNT(*) FILTER (WHERE t.transaction_type='credit') AS credit_count,
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS net_amount,
                   MIN(t.amount) AS min_amount, MAX(t.amount) AS max_amount
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE a.username=%s AND t.deleted_at IS NULL AND t.transfer_id IS NOT NULL
            GROUP BY t.transfer_id
        )
        SELECT * FROM grp
        WHERE row_count <> 2 OR debit_count <> 1 OR credit_count <> 1
           OR net_amount <> 0 OR min_amount <> max_amount
        ORDER BY transfer_id LIMIT 50
        """,
        (username,),
    )
    transfer_anomalies = [
        {k: int(v) if isinstance(v, (int, float)) else v for k, v in r.items()}
        for r in cur.fetchall()
    ]

    cur.execute(
        """
        SELECT COUNT(*) AS total_transactions,
               COALESCE(SUM(CASE WHEN t.transfer_id IS NOT NULL THEN 1 ELSE 0 END), 0) AS transfer_rows,
               COALESCE(SUM(CASE WHEN t.is_cycle_topup THEN 1 ELSE 0 END), 0) AS cycle_topup_rows
        FROM transactions t
        JOIN accounts a ON a.account_id=t.account_id
        WHERE a.username=%s AND t.deleted_at IS NULL AND t.date >= %s
        """,
        (username, since),
    )
    activity_row = cur.fetchone() or {}

    checks = {
        "negative_accounts": len(negative_accounts),
        "transfer_anomalies": len(transfer_anomalies),
    }
    risk_score = min(100, checks["transfer_anomalies"] * 20)

    return {
        "generated_at": generated_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "window_hours": hours,
        "window_since": since.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "risk_score": risk_score,
        "checks": checks,
        "activity": {
            "total_transactions": int(activity_row.get("total_transactions") or 0),
            "transfer_rows": int(activity_row.get("transfer_rows") or 0),
            "cycle_topup_rows": int(activity_row.get("cycle_topup_rows") or 0),
        },
        "findings": {
            "negative_accounts": negative_accounts,
            "transfer_anomalies": transfer_anomalies,
        },
    }


def compute_budget_shift_analysis(
    cur, username: str, month: str, from_dt: datetime, to_dt: datetime, strategy: str = "normal"
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT account_id::text AS account_id, account_name, profile_type,
               is_payroll_source, is_no_limit, is_buffer, fixed_limit_amount
        FROM accounts WHERE username=%s ORDER BY account_name
        """,
        (username,),
    )
    account_map = {r["account_id"]: r for r in cur.fetchall()}

    strategy_normalized = str(strategy or "normal").strip().lower()
    if strategy_normalized not in ("conservative", "normal", "aggressive"):
        strategy_normalized = "normal"

    cur.execute("SELECT account_id::text AS account_id, amount FROM budgets WHERE username=%s AND month=%s", (username, month))
    budgets = {r["account_id"]: int(r.get("amount") or 0) for r in cur.fetchall()}

    cur.execute(
        """
        SELECT t.account_id::text AS account_id,
               COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS real_spend,
               COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS real_income
        FROM transactions t JOIN accounts a ON a.account_id=t.account_id
        WHERE a.username=%s AND t.deleted_at IS NULL AND t.date >= %s AND t.date <= %s AND t.transfer_id IS NULL
        GROUP BY t.account_id
        """,
        (username, from_dt, to_dt),
    )
    real_by_acc = {r["account_id"]: r for r in cur.fetchall()}

    movement_by_acc: dict[str, Any] = {}
    edge_rows: list[dict[str, Any]] = []

    items: list[dict[str, Any]] = []
    total_budget = total_spend = total_movement_in = total_movement_out = 0

    for account_id, ar in account_map.items():
        fixed_limit = ar.get("fixed_limit_amount")
        budget = budgets.get(account_id)
        effective_budget = int(fixed_limit) if fixed_limit is not None else budget
        real_spend = int(real_by_acc.get(account_id, {}).get("real_spend") or 0)
        real_income = int(real_by_acc.get(account_id, {}).get("real_income") or 0)
        movement_in = int(movement_by_acc.get(account_id, {}).get("movement_in") or 0)
        movement_out = int(movement_by_acc.get(account_id, {}).get("movement_out") or 0)
        net_movement = movement_in - movement_out

        if bool(ar.get("is_no_limit")):
            status, reason, budget_gap, stress_ratio = "no_limit", "No-limit account", None, None
            suggested_budget = max(0, real_spend)
        elif effective_budget is None:
            status, reason, budget_gap, stress_ratio = "no_budget", "No budget set yet", None, None
            suggested_budget = max(0, real_spend)
        else:
            budget_gap = real_spend - effective_budget
            stress_ratio = (real_spend / effective_budget) if effective_budget > 0 else (1.0 if real_spend == 0 else 999.0)
            suggested_budget = max(real_spend, effective_budget)
            if str(ar.get("profile_type")) == "fixed_spending" and fixed_limit is not None:
                suggested_budget = int(fixed_limit)
            if budget_gap > 0:
                status, reason = "overspend", "Over budget"
            else:
                status, reason = "balanced", "Within planned budget"

        total_budget += int(effective_budget or 0)
        total_spend += real_spend
        total_movement_in += movement_in
        total_movement_out += movement_out

        items.append({
            "account_id": account_id,
            "account_name": ar.get("account_name"),
            "profile_type": str(ar.get("profile_type") or "dynamic_spending"),
            "is_payroll_source": bool(ar.get("is_payroll_source")),
            "is_no_limit": bool(ar.get("is_no_limit")),
            "is_buffer": bool(ar.get("is_buffer")),
            "fixed_limit_amount": int(fixed_limit) if fixed_limit is not None else None,
            "planned_budget": int(effective_budget) if effective_budget is not None else None,
            "actual_spend": real_spend,
            "actual_income": real_income,
            "movement_in": movement_in,
            "movement_out": movement_out,
            "net_movement": net_movement,
            "switch_in": movement_in,
            "switch_out": movement_out,
            "net_switch": net_movement,
            "budget_gap": budget_gap,
            "stress_ratio": round(stress_ratio, 4) if stress_ratio is not None else None,
            "suggested_budget": int(suggested_budget),
            "suggested_delta": int(suggested_budget - effective_budget) if effective_budget is not None else None,
            "status": status,
            "reason": reason,
        })

    items.sort(key=lambda r: (0 if r.get("status") == "overspend" else 1, -(r.get("budget_gap") or 0), r.get("account_name") or ""))

    return {
        "month": month,
        "strategy": strategy_normalized,
        "range": {"from": from_dt.date().isoformat(), "to": to_dt.date().isoformat()},
        "totals": {
            "planned_budget": total_budget,
            "actual_spend": total_spend,
            "budget_gap": total_spend - total_budget,
            "movement_in": total_movement_in,
            "movement_out": total_movement_out,
            "net_movement": total_movement_in - total_movement_out,
            "switch_in": total_movement_in,
            "switch_out": total_movement_out,
            "net_switch": total_movement_in - total_movement_out,
        },
        "movement_edges": [],
        "accounts": items,
        "switch_edges": [
            {
                "source_account_id": r.get("source_account_id"),
                "source_account_name": r.get("source_account_name"),
                "target_account_id": r.get("target_account_id"),
                "target_account_name": r.get("target_account_name"),
                "amount": int(r.get("amount") or 0),
            }
            for r in edge_rows
        ],
    }
