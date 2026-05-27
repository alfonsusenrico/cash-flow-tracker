"""Balance computation, account locking, and transaction parsing utilities."""
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

from app.services.ledger.period import now_utc, parse_date_utc


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
    return int((cur.fetchone() or {}).get("balance") or 0)


def ensure_account_non_negative(
    cur,
    account_id: str,
    effective_from: datetime,
    new_rows: list[dict[str, Any]] | None = None,
    exclude_tx_ids: list[str] | None = None,
) -> None:
    start_balance = get_balance_before(cur, account_id, effective_from, exclude_tx_ids)
    sql = """
        SELECT t.transaction_id::text AS transaction_id, t.date, t.transaction_type, t.amount
        FROM transactions t
        WHERE t.account_id=%s::uuid AND t.date >= %s AND t.deleted_at IS NULL
    """
    params: list[Any] = [account_id, effective_from]
    if exclude_tx_ids:
        sql += " AND t.transaction_id <> ALL(%s::uuid[])"
        params.append(exclude_tx_ids)
    sql += " ORDER BY t.date ASC, t.transaction_id ASC"
    cur.execute(sql, params)
    rows = list(cur.fetchall())
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
        LEFT JOIN transactions t ON t.account_id=a.account_id AND t.date <= %s AND t.deleted_at IS NULL
        WHERE a.username=%s
        GROUP BY a.account_id
        """,
        (up_to, username),
    )
    return {r["account_id"]: int(r["balance"] or 0) for r in cur.fetchall()}


def get_balance_at_transaction(
    cur,
    account_id: str,
    tx_date: datetime,
    tx_id: str,
    exclude_transfer_ids: list[str] | None = None,
) -> int:
    sql = """
        SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS balance
        FROM transactions t
        WHERE t.account_id=%s::uuid AND t.deleted_at IS NULL
          AND (t.date < %s OR (t.date = %s AND t.transaction_id <= %s::uuid))
    """
    params: list[Any] = [account_id, tx_date, tx_date, tx_id]
    if exclude_transfer_ids:
        normalized = sorted({parse_uuid_value(v, "transfer_id") for v in exclude_transfer_ids if v})
        if normalized:
            sql += " AND (t.transfer_id IS NULL OR t.transfer_id <> ALL(%s::uuid[]))"
            params.append(normalized)
    cur.execute(sql, params)
    return int((cur.fetchone() or {}).get("balance") or 0)


def compute_shortfall_at_transaction(
    cur, account_id: str, tx_date: datetime, tx_id: str, exclude_transfer_ids: list[str] | None = None
) -> int:
    return max(0, -get_balance_at_transaction(cur, account_id, tx_date, tx_id, exclude_transfer_ids))


def recompute_balances_report(cur, username: str) -> dict[str, Any]:
    cur.execute(
        """
        WITH tx_running AS (
            SELECT a.account_id::text AS account_id,
                   a.account_name,
                   t.transaction_id,
                   t.date,
                   SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END)
                     OVER (
                        PARTITION BY a.account_id
                        ORDER BY t.date ASC, t.transaction_id ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                     ) AS running_balance
            FROM accounts a
            LEFT JOIN transactions t ON t.account_id=a.account_id AND t.deleted_at IS NULL
            WHERE a.username=%s
        ),
        account_stats AS (
            SELECT account_id,
                   account_name,
                   COUNT(transaction_id) AS transactions_count,
                   LEAST(0, COALESCE(MIN(running_balance), 0)) AS min_balance,
                   MIN(date) FILTER (WHERE running_balance < 0) AS first_negative_at
            FROM tx_running
            GROUP BY account_id, account_name
        ),
        current_rows AS (
            SELECT DISTINCT ON (account_id)
                   account_id,
                   running_balance AS current_balance
            FROM tx_running
            WHERE transaction_id IS NOT NULL
            ORDER BY account_id, date DESC, transaction_id DESC
        )
        SELECT s.account_id,
               s.account_name,
               s.transactions_count,
               COALESCE(c.current_balance, 0) AS current_balance,
               s.min_balance,
               s.first_negative_at
        FROM account_stats s
        LEFT JOIN current_rows c ON c.account_id=s.account_id
        ORDER BY s.account_name
        """,
        (username,),
    )
    result_accounts: list[dict[str, Any]] = []
    has_negative = False

    for account in cur.fetchall():
        first_negative_at = account.get("first_negative_at")
        first_negative_iso = None
        if first_negative_at is not None:
            first_negative_iso = first_negative_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            has_negative = True

        result_accounts.append({
            "account_id": account["account_id"],
            "account_name": account["account_name"],
            "transactions_count": int(account.get("transactions_count") or 0),
            "current_balance": int(account.get("current_balance") or 0),
            "min_balance": int(account.get("min_balance") or 0),
            "first_negative_at": first_negative_iso,
        })

    total_asset = sum(int(r["current_balance"]) for r in result_accounts)
    return {"accounts": result_accounts, "has_negative": has_negative, "total_asset": int(total_asset)}
