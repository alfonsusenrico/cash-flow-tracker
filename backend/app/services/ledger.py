import calendar
import csv
import io
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException
from fpdf import FPDF

from app.core.config import settings
from app.services.state import cache


def cache_get(key: str) -> Any | None:
    return cache.get(key)


def cache_set(key: str, value: Any, ttl: int) -> None:
    cache.set(key, value, ttl)


def invalidate_user_cache(username: str) -> None:
    cache.invalidate_prefix(f"{username}:")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_date_utc(date_str: str, end_of_day: bool = False) -> datetime:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format, expected YYYY-MM-DD")
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return dt + (timedelta(days=1) - timedelta(milliseconds=1) if end_of_day else timedelta(0))


def clamp_day(year: int, month: int, day: int) -> int:
    if month == 12:
        last_day = (datetime(year + 1, 1, 1) - timedelta(days=1)).day
    else:
        last_day = (datetime(year, month + 1, 1) - timedelta(days=1)).day
    return min(day, last_day)


def get_default_payday_day(cur, username: str) -> int:
    cur.execute("SELECT default_payday_day FROM users WHERE username=%s", (username,))
    row = cur.fetchone()
    try:
        return int(row["default_payday_day"]) if row else 25
    except Exception:
        return 25


def get_payday_day(cur, username: str, month: str) -> tuple[int, str, int | None]:
    cur.execute(
        "SELECT payday_day FROM payday_overrides WHERE username=%s AND month=%s",
        (username, month),
    )
    override = cur.fetchone()
    if override:
        return int(override["payday_day"]), "override", int(override["payday_day"])
    default_day = get_default_payday_day(cur, username)
    return int(default_day), "default", None


def parse_month(month: str) -> tuple[int, int]:
    try:
        dt = datetime.strptime(month, "%Y-%m")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid month format, expected YYYY-MM")
    return dt.year, dt.month


def prev_month_str(month: str) -> str:
    year, month_num = parse_month(month)
    prev_month = month_num - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1
    return f"{prev_year:04d}-{prev_month:02d}"


def compute_export_range(day: int) -> tuple[str, str, datetime, datetime]:
    if day < 1 or day > 31:
        raise HTTPException(status_code=400, detail="Day must be between 1 and 31")
    today = now_utc().date()
    payday_this = datetime(today.year, today.month, clamp_day(today.year, today.month, day)).date()
    if today <= payday_this:
        prev_month = today.month - 1
        prev_year = today.year
        if prev_month == 0:
            prev_month = 12
            prev_year -= 1
        last_payday = datetime(prev_year, prev_month, clamp_day(prev_year, prev_month, day)).date()
    else:
        last_payday = payday_this
    from_date = last_payday.isoformat()
    to_date = today.isoformat()
    from_dt = parse_date_utc(from_date, end_of_day=False)
    to_dt = parse_date_utc(to_date, end_of_day=True)
    return from_date, to_date, from_dt, to_dt


def compute_month_range(
    month: str,
    payday_day: int,
    prev_payday_day: int | None = None,
) -> tuple[str, str, datetime, datetime]:
    year, month_num = parse_month(month)
    payday = datetime(year, month_num, clamp_day(year, month_num, payday_day)).date()
    prev_month = month_num - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1
    prev_day = prev_payday_day if prev_payday_day is not None else payday_day
    prev_payday = datetime(prev_year, prev_month, clamp_day(prev_year, prev_month, prev_day)).date()
    start_date = prev_payday
    end_date = payday - timedelta(days=1)
    today = now_utc().date()
    if end_date > today:
        end_date = today
    from_date = start_date.isoformat()
    to_date = end_date.isoformat()
    from_dt = parse_date_utc(from_date, end_of_day=False)
    to_dt = parse_date_utc(to_date, end_of_day=True)
    return from_date, to_date, from_dt, to_dt


def compute_budget_status(budget_amount: int | None, used_amount: int) -> tuple[int | None, str | None, int | None]:
    if budget_amount is None:
        return None, None, None
    if budget_amount <= 0:
        return 100, "critical", int(budget_amount - used_amount)
    pct = int(round((used_amount / budget_amount) * 100))
    if pct >= 100:
        status = "critical"
    elif pct >= 80:
        status = "warn"
    else:
        status = "ok"
    return pct, status, int(budget_amount - used_amount)


def parse_currency(currency: str | None, fx_rate: str | None) -> tuple[str, float | None]:
    cur = (currency or "IDR").upper()
    if cur not in ("IDR", "USD"):
        cur = "IDR"
    fx = None
    if cur == "USD":
        try:
            fx = float(fx_rate or 0)
        except Exception:
            fx = None
        if not fx or fx <= 0:
            raise HTTPException(status_code=400, detail="fx_rate required for USD export")
    return cur, fx


def format_amount(amount: int, currency: str, fx_rate: float | None) -> str:
    if currency == "USD":
        value = float(amount) * float(fx_rate or 0)
        return f"${value:,.2f}"
    return f"Rp {amount:,.0f}".replace(",", ".")


def format_tx_date(iso_z: str) -> str:
    if not iso_z:
        return ""
    iso_val = iso_z.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso_val)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")


def safe_pdf_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\n", " ").replace("\r", " ")
    try:
        text.encode("latin-1")
        return text
    except UnicodeEncodeError:
        return text.encode("latin-1", "replace").decode("latin-1")


def build_search_pattern(query: str | None) -> str | None:
    if not query:
        return None
    cleaned = query.strip().lower()
    if not cleaned:
        return None
    if len(cleaned) > 64:
        cleaned = cleaned[:64]
    return f"%{cleaned}%"


def build_daily_series(from_date: str, to_date: str, rows: list[dict[str, Any]]) -> list[dict[str, int | str]]:
    start = datetime.fromisoformat(from_date).date()
    end = datetime.fromisoformat(to_date).date()
    by_day: dict[str, dict[str, int]] = {}
    for row in rows:
        day_val = row.get("day")
        day_key = day_val.isoformat() if hasattr(day_val, "isoformat") else str(day_val)
        total_in = int(row.get("total_in") or 0)
        total_out = int(row.get("total_out") or 0)
        by_day[day_key] = {"total_in": total_in, "total_out": total_out}

    series: list[dict[str, int | str]] = []
    cursor = start
    while cursor <= end:
        key = cursor.isoformat()
        totals = by_day.get(key, {"total_in": 0, "total_out": 0})
        total_in = int(totals.get("total_in") or 0)
        total_out = int(totals.get("total_out") or 0)
        series.append({"date": key, "total_in": total_in, "total_out": total_out, "net": int(total_in - total_out)})
        cursor += timedelta(days=1)
    return series


def build_weekly_series(from_date: str, to_date: str, daily: list[dict[str, int | str]]) -> list[dict[str, int | str]]:
    start = datetime.fromisoformat(from_date).date()
    end = datetime.fromisoformat(to_date).date()
    by_day: dict[str, dict[str, int | str]] = {}
    for row in daily:
        date_str = row.get("date")
        if not date_str:
            continue
        by_day[str(date_str)] = row

    series: list[dict[str, int | str]] = []
    cursor = start
    while cursor <= end:
        period_from = cursor
        period_to = min(end, cursor + timedelta(days=6))
        total_in = 0
        total_out = 0
        day = period_from
        while day <= period_to:
            row = by_day.get(day.isoformat(), {})
            total_in += int(row.get("total_in") or 0)
            total_out += int(row.get("total_out") or 0)
            day += timedelta(days=1)
        series.append(
            {
                "from": period_from.isoformat(),
                "to": period_to.isoformat(),
                "total_in": total_in,
                "total_out": total_out,
                "net": int(total_in - total_out),
            }
        )
        cursor = period_to + timedelta(days=1)
    return series


def parse_tx_datetime(date_str: str | None) -> datetime:
    if not date_str:
        return now_utc().replace(microsecond=0)
    try:
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.replace(microsecond=0)
    except Exception:
        return parse_date_utc(date_str, end_of_day=False).replace(microsecond=0)


def parse_uuid_value(value: Any, field_name: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail=f"{field_name} required")
    try:
        return str(uuid.UUID(raw))
    except (TypeError, ValueError, AttributeError):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


def lock_accounts_for_update(cur, username: str, account_ids: list[str]) -> None:
    unique_ids = sorted({parse_uuid_value(aid, "account_id") for aid in account_ids if aid})
    if not unique_ids:
        return
    cur.execute(
        """
        SELECT account_id::text AS account_id
        FROM accounts
        WHERE username=%s AND account_id = ANY(%s::uuid[])
        ORDER BY account_id
        FOR UPDATE
        """,
        (username, unique_ids),
    )
    rows = cur.fetchall()
    if len(rows) != len(unique_ids):
        raise HTTPException(status_code=404, detail="Account not found")


def get_balance_before(cur, account_id: str, before_dt: datetime, exclude_tx_ids: list[str] | None = None) -> int:
    sql = """
        SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS balance
        FROM transactions t
        WHERE t.account_id=%s::uuid AND t.date < %s AND t.deleted_at IS NULL
    """
    params: list[Any] = [account_id, before_dt]
    if exclude_tx_ids:
        sql += " AND t.transaction_id <> ALL(%s::uuid[])"
        params.append(exclude_tx_ids)
    cur.execute(sql, params)
    row = cur.fetchone() or {}
    return int(row.get("balance") or 0)


def ensure_account_non_negative(
    cur,
    account_id: str,
    effective_from: datetime,
    new_rows: list[dict[str, Any]] | None = None,
    exclude_tx_ids: list[str] | None = None,
) -> None:
    start_balance = get_balance_before(cur, account_id, effective_from, exclude_tx_ids)
    sql = """
        SELECT t.transaction_id::text AS transaction_id,
               t.date,
               t.transaction_type,
               t.amount
        FROM transactions t
        WHERE t.account_id=%s::uuid AND t.date >= %s AND t.deleted_at IS NULL
    """
    params: list[Any] = [account_id, effective_from]
    if exclude_tx_ids:
        sql += " AND t.transaction_id <> ALL(%s::uuid[])"
        params.append(exclude_tx_ids)
    sql += " ORDER BY t.date ASC, t.transaction_id ASC"
    cur.execute(sql, params)
    rows = cur.fetchall()
    if new_rows:
        rows.extend(new_rows)
    rows.sort(key=lambda r: (r["date"], str(r["transaction_id"])))
    balance = start_balance
    for row in rows:
        signed = int(row.get("amount") or 0)
        if row.get("transaction_type") == "credit":
            signed = -signed
        balance += signed
        if balance < 0:
            raise HTTPException(status_code=400, detail="Insufficient balance")


def get_account_balances(cur, username: str, up_to: datetime) -> dict[str, int]:
    cur.execute(
        """
        SELECT a.account_id::text AS account_id,
               COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS balance
        FROM accounts a
        LEFT JOIN transactions t
          ON t.account_id=a.account_id AND t.date <= %s AND t.deleted_at IS NULL
        WHERE a.username=%s
        GROUP BY a.account_id
        """,
        (up_to, username),
    )
    return {r["account_id"]: int(r["balance"] or 0) for r in cur.fetchall()}


def write_transaction_audit(
    cur,
    *,
    username: str,
    performed_by: str,
    action: str,
    tx_row: dict[str, Any],
) -> None:
    payload = json.dumps(
        {
            "transaction_id": tx_row.get("transaction_id"),
            "account_id": tx_row.get("account_id"),
            "transaction_type": tx_row.get("transaction_type"),
            "transaction_name": tx_row.get("transaction_name"),
            "amount": int(tx_row.get("amount") or 0),
            "date": tx_row.get("date").astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            if tx_row.get("date")
            else None,
            "is_transfer": bool(tx_row.get("is_transfer")),
            "is_cycle_topup": bool(tx_row.get("is_cycle_topup")),
            "transfer_id": tx_row.get("transfer_id"),
            "deleted_at": tx_row.get("deleted_at").astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            if tx_row.get("deleted_at")
            else None,
            "deleted_by": tx_row.get("deleted_by"),
            "delete_reason": tx_row.get("delete_reason"),
        },
        separators=(",", ":"),
    )
    cur.execute(
        """
        INSERT INTO transaction_audit (transaction_id, account_id, username, action, payload, performed_by)
        VALUES (%s::uuid, %s::uuid, %s, %s, %s::jsonb, %s)
        """,
        (
            tx_row.get("transaction_id"),
            tx_row.get("account_id"),
            username,
            action,
            payload,
            performed_by,
        ),
    )


def compute_budget_shift_analysis(
    cur,
    username: str,
    month: str,
    from_dt: datetime,
    to_dt: datetime,
    strategy: str = "normal",
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT account_id::text AS account_id,
               account_name,
               profile_type,
               is_payroll_source,
               is_no_limit,
               is_buffer,
               fixed_limit_amount
        FROM accounts
        WHERE username=%s
        ORDER BY account_name
        """,
        (username,),
    )
    accounts = cur.fetchall()
    account_map = {row["account_id"]: row for row in accounts}

    strategy_normalized = str(strategy or "normal").strip().lower()
    if strategy_normalized not in ("conservative", "normal", "aggressive"):
        strategy_normalized = "normal"

    strategy_cfg = {
        "conservative": {"receiver_weight": 0.5, "donor_weight": 0.3},
        "normal": {"receiver_weight": 0.8, "donor_weight": 0.5},
        "aggressive": {"receiver_weight": 1.0, "donor_weight": 0.8},
    }
    receiver_weight = float(strategy_cfg[strategy_normalized]["receiver_weight"])
    donor_weight = float(strategy_cfg[strategy_normalized]["donor_weight"])

    cur.execute(
        """
        SELECT account_id::text AS account_id,
               amount
        FROM budgets
        WHERE username=%s AND month=%s
        """,
        (username, month),
    )
    budget_rows = cur.fetchall()
    budgets = {row["account_id"]: int(row.get("amount") or 0) for row in budget_rows}

    cur.execute(
        """
        SELECT t.account_id::text AS account_id,
               COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS real_spend,
               COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS real_income
        FROM transactions t
        JOIN accounts a ON a.account_id=t.account_id
        WHERE a.username=%s
          AND t.deleted_at IS NULL
          AND t.date >= %s
          AND t.date <= %s
          AND t.transfer_id IS NULL
        GROUP BY t.account_id
        """,
        (username, from_dt, to_dt),
    )
    real_rows = cur.fetchall()

    cur.execute(
        """
        SELECT t.account_id::text AS account_id,
               COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS switch_in,
               COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS switch_out
        FROM transactions t
        JOIN accounts a ON a.account_id=t.account_id
        WHERE a.username=%s
          AND t.deleted_at IS NULL
          AND t.date >= %s
          AND t.date <= %s
          AND t.transfer_id IS NOT NULL
        GROUP BY t.account_id
        """,
        (username, from_dt, to_dt),
    )
    switch_rows = cur.fetchall()

    cur.execute(
        """
        SELECT src.account_id::text AS source_account_id,
               src.account_name AS source_account_name,
               dst.account_id::text AS target_account_id,
               dst.account_name AS target_account_name,
               COALESCE(SUM(t_out.amount), 0) AS amount
        FROM transactions t_out
        JOIN transactions t_in
          ON t_in.transfer_id=t_out.transfer_id
         AND t_in.deleted_at IS NULL
         AND t_in.transaction_type='debit'
        JOIN accounts src ON src.account_id=t_out.account_id
        JOIN accounts dst ON dst.account_id=t_in.account_id
        WHERE t_out.deleted_at IS NULL
          AND t_out.transaction_type='credit'
          AND src.username=%s
          AND dst.username=%s
          AND t_out.date >= %s
          AND t_out.date <= %s
        GROUP BY src.account_id, src.account_name, dst.account_id, dst.account_name
        ORDER BY amount DESC, src.account_name ASC, dst.account_name ASC
        """,
        (username, username, from_dt, to_dt),
    )
    edge_rows = cur.fetchall()

    real_by_acc = {
        row["account_id"]: {
            "real_spend": int(row.get("real_spend") or 0),
            "real_income": int(row.get("real_income") or 0),
        }
        for row in real_rows
    }
    switch_by_acc = {
        row["account_id"]: {
            "switch_in": int(row.get("switch_in") or 0),
            "switch_out": int(row.get("switch_out") or 0),
        }
        for row in switch_rows
    }

    items: list[dict[str, Any]] = []
    total_budget = 0
    total_spend = 0
    total_switch_in = 0
    total_switch_out = 0

    for account_id, account_row in account_map.items():
        account_name = account_row.get("account_name")
        profile_type = str(account_row.get("profile_type") or "dynamic_spending")
        is_payroll_source = bool(account_row.get("is_payroll_source"))
        is_no_limit = bool(account_row.get("is_no_limit"))
        is_buffer = bool(account_row.get("is_buffer"))
        fixed_limit_amount = account_row.get("fixed_limit_amount")

        budget = budgets.get(account_id)
        effective_budget = int(fixed_limit_amount) if fixed_limit_amount is not None else budget
        real_spend = int(real_by_acc.get(account_id, {}).get("real_spend") or 0)
        real_income = int(real_by_acc.get(account_id, {}).get("real_income") or 0)
        switch_in = int(switch_by_acc.get(account_id, {}).get("switch_in") or 0)
        switch_out = int(switch_by_acc.get(account_id, {}).get("switch_out") or 0)
        net_switch = int(switch_in - switch_out)

        if is_no_limit:
            budget_gap = None
            stress_ratio = None
            suggested_budget = max(0, real_spend)
            status = "no_limit"
            reason = "No-limit account"
        elif effective_budget is None:
            budget_gap = None
            stress_ratio = None
            suggested_budget = max(0, real_spend)
            status = "no_budget"
            reason = "No budget set yet"
        else:
            budget_gap = int(real_spend - effective_budget)
            stress_ratio = (real_spend / effective_budget) if effective_budget > 0 else (1.0 if real_spend == 0 else 999.0)
            suggested_budget = int(max(real_spend, effective_budget))
            if net_switch > 0:
                uplift = int(round(net_switch * receiver_weight))
                suggested_budget = max(suggested_budget, effective_budget + uplift)
            if switch_out > 0 and real_spend < effective_budget:
                reducible = min(switch_out, effective_budget - real_spend)
                cut = int(round(reducible * donor_weight))
                suggested_budget = max(real_spend, effective_budget - cut)

            if profile_type == "fixed_spending" and fixed_limit_amount is not None:
                suggested_budget = int(fixed_limit_amount)

            if budget_gap > 0 and net_switch > 0:
                status = "under_allocated"
                reason = "Over budget and receives switch-in"
            elif budget_gap > 0:
                status = "overspend"
                reason = "Over budget"
            elif net_switch < 0 and real_spend < effective_budget:
                status = "donor_capacity"
                reason = "Consistent switch-out while spend is below budget"
            else:
                status = "balanced"
                reason = "Within planned budget"

        total_budget += int(effective_budget or 0)
        total_spend += real_spend
        total_switch_in += switch_in
        total_switch_out += switch_out

        items.append(
            {
                "account_id": account_id,
                "account_name": account_name,
                "profile_type": profile_type,
                "is_payroll_source": is_payroll_source,
                "is_no_limit": is_no_limit,
                "is_buffer": is_buffer,
                "fixed_limit_amount": int(fixed_limit_amount) if fixed_limit_amount is not None else None,
                "planned_budget": int(effective_budget) if effective_budget is not None else None,
                "actual_spend": real_spend,
                "actual_income": real_income,
                "switch_in": switch_in,
                "switch_out": switch_out,
                "net_switch": net_switch,
                "budget_gap": budget_gap,
                "stress_ratio": round(stress_ratio, 4) if stress_ratio is not None else None,
                "suggested_budget": int(suggested_budget),
                "suggested_delta": int(suggested_budget - effective_budget) if effective_budget is not None else None,
                "status": status,
                "reason": reason,
            }
        )

    items.sort(
        key=lambda row: (
            0 if row.get("status") == "under_allocated" else 1,
            -(row.get("budget_gap") or 0),
            -abs(row.get("net_switch") or 0),
            row.get("account_name") or "",
        )
    )

    switch_edges = [
        {
            "source_account_id": row.get("source_account_id"),
            "source_account_name": row.get("source_account_name"),
            "target_account_id": row.get("target_account_id"),
            "target_account_name": row.get("target_account_name"),
            "amount": int(row.get("amount") or 0),
        }
        for row in edge_rows
    ]

    return {
        "month": month,
        "strategy": strategy_normalized,
        "range": {
            "from": from_dt.date().isoformat(),
            "to": to_dt.date().isoformat(),
        },
        "totals": {
            "planned_budget": int(total_budget),
            "actual_spend": int(total_spend),
            "budget_gap": int(total_spend - total_budget),
            "switch_in": int(total_switch_in),
            "switch_out": int(total_switch_out),
            "net_switch": int(total_switch_in - total_switch_out),
        },
        "accounts": items,
        "switch_edges": switch_edges,
    }


def recompute_balances_report(cur, username: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT account_id::text AS account_id,
               account_name
        FROM accounts
        WHERE username=%s
        ORDER BY account_name
        """,
        (username,),
    )
    accounts = cur.fetchall()
    result_accounts: list[dict[str, Any]] = []
    has_negative = False

    for account in accounts:
        account_id = account["account_id"]
        cur.execute(
            """
            SELECT transaction_id::text AS transaction_id,
                   date,
                   transaction_type,
                   amount
            FROM transactions
            WHERE account_id=%s::uuid AND deleted_at IS NULL
            ORDER BY date ASC, transaction_id ASC
            """,
            (account_id,),
        )
        rows = cur.fetchall()
        balance = 0
        min_balance = 0
        first_negative_at = None
        for row in rows:
            signed = int(row["amount"]) if row["transaction_type"] == "debit" else -int(row["amount"])
            balance += signed
            if balance < min_balance:
                min_balance = balance
            if balance < 0 and first_negative_at is None:
                first_negative_at = row["date"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                has_negative = True

        result_accounts.append(
            {
                "account_id": account_id,
                "account_name": account["account_name"],
                "transactions_count": len(rows),
                "current_balance": int(balance),
                "min_balance": int(min_balance),
                "first_negative_at": first_negative_at,
            }
        )

    total_asset = sum(int(row["current_balance"]) for row in result_accounts)
    return {"accounts": result_accounts, "has_negative": has_negative, "total_asset": int(total_asset)}


def compute_summary(cur, username: str, acc_by_id: dict[str, dict[str, Any]], to_dt: datetime) -> tuple[list[dict[str, Any]], int]:
    balances_all = get_account_balances(cur, username, to_dt)
    summary_accounts = [
        {"account_id": aid, "account_name": acc_by_id[aid]["account_name"], "balance": int(balances_all.get(aid, 0))}
        for aid in sorted(acc_by_id.keys(), key=lambda x: acc_by_id[x]["account_name"].lower())
    ]
    total_asset = sum(int(balances_all.get(aid, 0)) for aid in acc_by_id.keys())
    return summary_accounts, int(total_asset)


def build_ledger_data(
    cur,
    username: str,
    scope: str,
    account_id: str | None,
    from_dt: datetime,
    to_dt: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    cur.execute(
        """
        SELECT account_id::text, account_name
        FROM accounts
        WHERE username=%s
        """,
        (username,),
    )
    accounts = cur.fetchall()
    acc_by_id = {a["account_id"]: a for a in accounts}

    if scope not in ("all", "account"):
        raise HTTPException(status_code=400, detail="Invalid scope")
    if scope == "account" and not account_id:
        raise HTTPException(status_code=400, detail="account_id required for scope=account")
    if scope == "account" and account_id not in acc_by_id:
        raise HTTPException(status_code=404, detail="Account not found")

    acc_ids = list(acc_by_id.keys()) if scope == "all" else [account_id]

    cur.execute(
        """
        SELECT a.account_id::text AS account_id,
               COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS start_balance
        FROM accounts a
        LEFT JOIN transactions t
          ON t.account_id=a.account_id AND t.date < %s AND t.deleted_at IS NULL
        WHERE a.username=%s AND a.account_id = ANY(%s::uuid[])
        GROUP BY a.account_id
        """,
        (from_dt, username, acc_ids),
    )
    start_rows = cur.fetchall()
    balance = {r["account_id"]: int(r["start_balance"]) for r in start_rows}

    total_asset_running = None
    if scope == "all":
        total_asset_running = sum(int(balance.get(aid, 0)) for aid in acc_by_id.keys())

    cur.execute(
        """
        SELECT t.transaction_id::text AS transaction_id,
               t.account_id::text AS account_id,
               a.account_name,
               t.transaction_type,
               t.transaction_name,
               t.amount,
               t.date,
               t.is_transfer,
               t.is_cycle_topup,
               t.transfer_id::text AS transfer_id
        FROM transactions t
        JOIN accounts a ON a.account_id=t.account_id
        WHERE a.username=%s
          AND t.account_id = ANY(%s::uuid[])
          AND t.deleted_at IS NULL
          AND t.date >= %s AND t.date <= %s
        ORDER BY t.date ASC, t.transaction_id ASC
        """,
        (username, acc_ids, from_dt, to_dt),
    )
    txs = cur.fetchall()

    rows = []
    row_no = 0
    for t in txs:
        aid = t["account_id"]
        signed = int(t["amount"]) if t["transaction_type"] == "debit" else -int(t["amount"])
        balance[aid] = int(balance.get(aid, 0) + signed)
        row_no += 1
        row_balance = int(balance.get(aid, 0))
        if scope == "all" and total_asset_running is not None:
            total_asset_running += signed
            row_balance = int(total_asset_running)
        rows.append(
            {
                "no": row_no,
                "account_id": aid,
                "account_name": t["account_name"],
                "date": t["date"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "transaction_id": t["transaction_id"],
                "transaction_name": t["transaction_name"],
                "debit": int(t["amount"]) if t["transaction_type"] == "debit" else 0,
                "credit": int(t["amount"]) if t["transaction_type"] == "credit" else 0,
                "balance": row_balance,
                "is_transfer": bool(t.get("is_transfer")),
                "is_cycle_topup": bool(t.get("is_cycle_topup")),
                "transfer_id": t.get("transfer_id"),
            }
        )

    summary_accounts, total_asset = compute_summary(cur, username, acc_by_id, to_dt)

    return rows, summary_accounts, total_asset


def build_ledger_page(
    cur,
    username: str,
    scope: str,
    account_id: str | None,
    from_dt: datetime,
    to_dt: datetime,
    limit: int,
    offset: int,
    order: str,
    query: str | None,
    include_summary: bool = True,
    include_switch: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, dict[str, int | bool]]:
    cur.execute(
        """
        SELECT account_id::text, account_name
        FROM accounts
        WHERE username=%s
        """,
        (username,),
    )
    accounts = cur.fetchall()
    if not accounts:
        return [], [], 0, {"limit": limit, "offset": offset, "has_more": False, "next_offset": offset}

    acc_by_id = {a["account_id"]: a for a in accounts}

    if scope not in ("all", "account"):
        raise HTTPException(status_code=400, detail="Invalid scope")
    if scope == "account":
        if not account_id:
            raise HTTPException(status_code=400, detail="account_id required for scope=account")
        if account_id not in acc_by_id:
            raise HTTPException(status_code=404, detail="Account not found")

    # Internal switch visibility only applies to All scope.
    if scope != "all":
        include_switch = True

    if order not in ("asc", "desc"):
        order = "desc"
    order_dir = "ASC" if order == "asc" else "DESC"
    limit = max(1, min(int(limit or 25), 100))
    offset = max(0, int(offset or 0))

    summary_accounts: list[dict[str, Any]] = []
    total_asset = 0
    if include_summary:
        summary_key = f"{username}:ledger:{to_dt.isoformat()}"
        cached = cache_get(summary_key)
        if cached:
            summary_accounts, total_asset = cached
        else:
            summary_accounts, total_asset = compute_summary(cur, username, acc_by_id, to_dt)
            cache_set(summary_key, (summary_accounts, total_asset), settings.summary_cache_ttl)

    base_balance = 0
    if scope == "all":
        all_ids = list(acc_by_id.keys())
        cur.execute(
            """
            SELECT a.account_id::text AS account_id,
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS start_balance
            FROM accounts a
            LEFT JOIN transactions t
              ON t.account_id=a.account_id AND t.date < %s AND t.deleted_at IS NULL
            WHERE a.username=%s AND a.account_id = ANY(%s::uuid[])
            GROUP BY a.account_id
            """,
            (from_dt, username, all_ids),
        )
        start_rows = cur.fetchall()
        balance = {r["account_id"]: int(r["start_balance"] or 0) for r in start_rows}
        base_balance = sum(int(balance.get(aid, 0)) for aid in acc_by_id.keys())
    else:
        cur.execute(
            """
            SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS start_balance
            FROM transactions t
            WHERE t.account_id=%s::uuid AND t.date < %s AND t.deleted_at IS NULL
            """,
            (account_id, from_dt),
        )
        base_balance = int(cur.fetchone()["start_balance"] or 0)

    search_pattern = build_search_pattern(query)
    search_sql = ""
    search_params: list[Any] = []
    if search_pattern:
        search_sql = "WHERE transaction_name ILIKE %s"
        search_params.append(search_pattern)

    if scope == "all":
        if include_switch:
            sql = f"""
                WITH tx AS (
                    SELECT t.transaction_id::text AS transaction_id,
                           t.account_id::text AS account_id,
                           a.account_name,
                           t.transaction_type,
                           t.transaction_name,
                           t.amount,
                           t.date,
                           t.is_cycle_topup,
                           t.transfer_id::text AS transfer_id
                    FROM transactions t
                    JOIN accounts a ON a.account_id=t.account_id
                    WHERE a.username=%s
                      AND t.deleted_at IS NULL
                      AND t.date >= %s
                      AND t.date <= %s
                ),
                non_transfer AS (
                    SELECT transaction_id AS event_id,
                           account_id,
                           account_name,
                           transaction_name,
                           amount,
                           date,
                           false AS is_transfer,
                           false AS is_cycle_topup,
                           NULL::text AS transfer_id,
                           CASE WHEN transaction_type='debit' THEN amount ELSE -amount END AS signed_delta,
                           CASE WHEN transaction_type='debit' THEN amount ELSE 0 END AS debit,
                           CASE WHEN transaction_type='credit' THEN amount ELSE 0 END AS credit
                    FROM tx
                    WHERE transfer_id IS NULL
                ),
                transfer_group AS (
                    SELECT 'switch:' || transfer_id AS event_id,
                           NULL::text AS account_id,
                           'Internal'::text AS account_name,
                           CONCAT(
                               'Switch: ',
                               COALESCE(MAX(CASE WHEN transaction_type='credit' THEN account_name END), 'Unknown'),
                               ' → ',
                               COALESCE(MAX(CASE WHEN transaction_type='debit' THEN account_name END), 'Unknown')
                           ) AS transaction_name,
                           COALESCE(MAX(CASE WHEN transaction_type='debit' THEN amount ELSE 0 END), 0) AS amount,
                           MAX(date) AS date,
                           true AS is_transfer,
                           BOOL_OR(is_cycle_topup) AS is_cycle_topup,
                           transfer_id,
                           0::bigint AS signed_delta,
                           COALESCE(MAX(CASE WHEN transaction_type='debit' THEN amount ELSE 0 END), 0) AS debit,
                           COALESCE(MAX(CASE WHEN transaction_type='credit' THEN amount ELSE 0 END), 0) AS credit
                    FROM tx
                    WHERE transfer_id IS NOT NULL
                    GROUP BY transfer_id
                ),
                events AS (
                    SELECT * FROM non_transfer
                    UNION ALL
                    SELECT * FROM transfer_group
                ),
                events_running AS (
                    SELECT event_id,
                           account_id,
                           account_name,
                           transaction_name,
                           amount,
                           date,
                           is_transfer,
                           is_cycle_topup,
                           transfer_id,
                           debit,
                           credit,
                           SUM(signed_delta) OVER (ORDER BY date ASC, event_id ASC) AS running_delta
                    FROM events
                )
                SELECT event_id,
                       account_id,
                       account_name,
                       transaction_name,
                       amount,
                       date,
                       is_transfer,
                       is_cycle_topup,
                       transfer_id,
                       debit,
                       credit,
                       running_delta
                FROM events_running
                {search_sql}
                ORDER BY date {order_dir}, event_id {order_dir}
                LIMIT %s OFFSET %s
            """
        else:
            sql = f"""
                WITH events AS (
                    SELECT t.transaction_id::text AS event_id,
                           t.account_id::text AS account_id,
                           a.account_name,
                           t.transaction_name,
                           t.amount,
                           t.date,
                           false AS is_transfer,
                           t.is_cycle_topup,
                           NULL::text AS transfer_id,
                           CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END AS signed_delta,
                           CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END AS debit,
                           CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END AS credit
                    FROM transactions t
                    JOIN accounts a ON a.account_id=t.account_id
                    WHERE a.username=%s
                      AND t.deleted_at IS NULL
                      AND t.date >= %s
                      AND t.date <= %s
                      AND t.transfer_id IS NULL
                ),
                events_running AS (
                    SELECT event_id,
                           account_id,
                           account_name,
                           transaction_name,
                           amount,
                           date,
                           is_transfer,
                           is_cycle_topup,
                           transfer_id,
                           debit,
                           credit,
                           SUM(signed_delta) OVER (ORDER BY date ASC, event_id ASC) AS running_delta
                    FROM events
                )
                SELECT event_id,
                       account_id,
                       account_name,
                       transaction_name,
                       amount,
                       date,
                       is_transfer,
                       is_cycle_topup,
                       transfer_id,
                       debit,
                       credit,
                       running_delta
                FROM events_running
                {search_sql}
                ORDER BY date {order_dir}, event_id {order_dir}
                LIMIT %s OFFSET %s
            """
        params: list[Any] = [username, from_dt, to_dt]
        params.extend(search_params)
        params.extend([limit + 1, offset])
    else:
        base_filters = ["a.username=%s", "t.deleted_at IS NULL", "t.date >= %s", "t.date <= %s", "t.account_id=%s::uuid"]
        params = [username, from_dt, to_dt, account_id]

        sql = f"""
            WITH base AS (
                SELECT t.transaction_id::text AS transaction_id,
                       t.account_id::text AS account_id,
                       a.account_name,
                       t.transaction_type,
                       t.transaction_name,
                       t.amount,
                       t.date,
                       t.is_transfer,
                       t.is_cycle_topup,
                       t.transfer_id::text AS transfer_id,
                       SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END)
                         OVER (ORDER BY t.date ASC, t.transaction_id ASC) AS running_delta
                FROM transactions t
                JOIN accounts a ON a.account_id=t.account_id
                WHERE {' AND '.join(base_filters)}
            )
            SELECT transaction_id AS event_id,
                   account_id,
                   account_name,
                   transaction_name,
                   amount,
                   date,
                   is_transfer,
                   is_cycle_topup,
                   transfer_id,
                   CASE WHEN transaction_type='debit' THEN amount ELSE 0 END AS debit,
                   CASE WHEN transaction_type='credit' THEN amount ELSE 0 END AS credit,
                   running_delta
            FROM base
            {search_sql}
            ORDER BY date {order_dir}, event_id {order_dir}
            LIMIT %s OFFSET %s
        """
        params.extend(search_params)
        params.extend([limit + 1, offset])

    cur.execute(sql, params)
    raw_rows = cur.fetchall()
    has_more = len(raw_rows) > limit
    if has_more:
        raw_rows = raw_rows[:limit]

    rows = []
    for idx, r in enumerate(raw_rows, start=1):
        signed_balance = int(r.get("running_delta") or 0)
        balance = base_balance + signed_balance
        rows.append(
            {
                "no": offset + idx,
                "account_id": r.get("account_id"),
                "account_name": r.get("account_name") or "",
                "date": r["date"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "transaction_id": r["event_id"],
                "transaction_name": r["transaction_name"],
                "debit": int(r.get("debit") or 0),
                "credit": int(r.get("credit") or 0),
                "balance": int(balance),
                "is_transfer": bool(r.get("is_transfer")),
                "is_cycle_topup": bool(r.get("is_cycle_topup")),
                "transfer_id": r.get("transfer_id"),
            }
        )

    paging = {"limit": limit, "offset": offset, "has_more": has_more, "next_offset": offset + len(rows)}
    return rows, summary_accounts, total_asset, paging


def export_ledger_file(
    rows: list[dict[str, Any]],
    summary_accounts: list[dict[str, Any]],
    scope: str,
    account_id: str | None,
    username: str,
    from_date: str,
    to_date: str,
    export_format: str,
    currency: str,
    fx: float | None,
):
    account_name = "All"
    if scope == "account" and account_id:
        match = next((a for a in summary_accounts if a["account_id"] == account_id), None)
        if match:
            account_name = match["account_name"]

    include_account = scope == "all"
    headers = (
        ["No", "Account", "Date", "Transaction", "In", "Out", "Balance"]
        if include_account
        else ["No", "Date", "Transaction", "In", "Out", "Balance"]
    )

    def row_cells(r: dict[str, Any]) -> list[str]:
        debit = int(r.get("debit") or 0)
        credit = int(r.get("credit") or 0)
        base = [
            str(r.get("no") or ""),
            format_tx_date(r.get("date") or ""),
            str(r.get("transaction_name") or ""),
            format_amount(debit, currency, fx) if debit else "",
            format_amount(credit, currency, fx) if credit else "",
            format_amount(int(r.get("balance") or 0), currency, fx),
        ]
        if include_account:
            base.insert(1, str(r.get("account_name") or ""))
        return base

    if export_format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        for r in rows:
            writer.writerow(row_cells(r))
        filename = f"ledger_{from_date}_to_{to_date}.csv"
        return {
            "content": output.getvalue(),
            "media_type": "text/csv",
            "filename": filename,
        }

    pdf = FPDF(orientation="L" if include_account else "P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, "Ledger Export", ln=True)
    pdf.set_font("Helvetica", size=10)
    meta = f"User: {username} | Account: {account_name} | Range: {from_date} to {to_date}"
    pdf.multi_cell(0, 6, meta)
    pdf.ln(2)

    widths = [10, 32, 64, 28, 28, 28]
    if include_account:
        widths = [10, 36, 32, 58, 28, 28, 28]

    pdf.set_font("Helvetica", "B", 9)
    for idx, label in enumerate(headers):
        pdf.cell(widths[idx], 7, label, border=1)
    pdf.ln()

    pdf.set_font("Helvetica", size=9)
    for r in rows:
        cells = row_cells(r)
        for idx, val in enumerate(cells):
            cell = safe_pdf_text(val)
            if len(cell) > 40:
                cell = cell[:37] + "..."
            pdf.cell(widths[idx], 6, cell, border=1)
        pdf.ln()

    pdf_bytes = pdf.output(dest="S")
    if isinstance(pdf_bytes, bytearray):
        pdf_bytes = bytes(pdf_bytes)
    elif isinstance(pdf_bytes, str):
        pdf_bytes = pdf_bytes.encode("latin-1")
    filename = f"ledger_{from_date}_to_{to_date}.pdf"
    return {
        "content": pdf_bytes,
        "media_type": "application/pdf",
        "filename": filename,
    }
