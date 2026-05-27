"""Ledger page builder and full-data builder (for export)."""
from datetime import timezone
from typing import Any

from fastapi import HTTPException

from app.core.config import settings
from app.services.ledger.balances import get_account_balances
from app.services.ledger.cache import cache_get, cache_set
from app.services.ledger.reports import build_search_pattern, compute_summary


def build_ledger_data(
    cur,
    username: str,
    scope: str,
    account_id: str | None,
    from_dt,
    to_dt,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    cur.execute("SELECT account_id::text, account_name FROM accounts WHERE username=%s", (username,))
    accounts = cur.fetchall()
    acc_by_id = {a["account_id"]: a for a in accounts}

    if scope not in ("all", "account"):
        raise HTTPException(status_code=400, detail="Invalid scope")
    if scope == "account":
        if not account_id:
            raise HTTPException(status_code=400, detail="account_id required for scope=account")
        if account_id not in acc_by_id:
            raise HTTPException(status_code=404, detail="Account not found")

    acc_ids = list(acc_by_id.keys()) if scope == "all" else [account_id]

    cur.execute(
        """
        SELECT a.account_id::text AS account_id,
               COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS start_balance
        FROM accounts a
        LEFT JOIN transactions t ON t.account_id=a.account_id AND t.date < %s AND t.deleted_at IS NULL
        WHERE a.username=%s AND a.account_id = ANY(%s::uuid[])
        GROUP BY a.account_id
        """,
        (from_dt, username, acc_ids),
    )
    balance = {r["account_id"]: int(r["start_balance"]) for r in cur.fetchall()}
    total_asset_running = sum(int(balance.get(aid, 0)) for aid in acc_by_id.keys()) if scope == "all" else None

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
               t.transfer_id::text AS transfer_id,
               t.category_id::text AS category_id,
               t.notes,
               t.tags,
               t.is_reviewed
        FROM transactions t
        JOIN accounts a ON a.account_id=t.account_id
        WHERE a.username=%s AND t.account_id = ANY(%s::uuid[])
          AND t.deleted_at IS NULL AND t.date >= %s AND t.date <= %s
        ORDER BY t.date ASC, t.transaction_id ASC
        """,
        (username, acc_ids, from_dt, to_dt),
    )
    rows = []
    for row_no, t in enumerate(cur.fetchall(), start=1):
        aid = t["account_id"]
        signed = int(t["amount"]) if t["transaction_type"] == "debit" else -int(t["amount"])
        balance[aid] = int(balance.get(aid, 0)) + signed
        row_balance = int(balance[aid])
        if scope == "all" and total_asset_running is not None:
            total_asset_running += signed
            row_balance = int(total_asset_running)
        rows.append({
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
            "category_id": t.get("category_id"),
            "notes": t.get("notes"),
            "tags": list(t.get("tags") or []),
            "is_reviewed": bool(t.get("is_reviewed")),
        })

    summary_accounts, total_asset = compute_summary(cur, username, acc_by_id, to_dt)
    return rows, summary_accounts, total_asset


def build_ledger_page(
    cur,
    username: str,
    scope: str,
    account_id: str | None,
    from_dt,
    to_dt,
    limit: int,
    offset: int,
    order: str,
    query: str | None,
    include_summary: bool = True,
    include_switch: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, dict[str, int | bool]]:
    cur.execute("SELECT account_id::text, account_name FROM accounts WHERE username=%s", (username,))
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
    if scope != "all":
        include_switch = True

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

    if scope == "all":
        cur.execute(
            """
            SELECT a.account_id::text AS account_id,
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS start_balance
            FROM accounts a
            LEFT JOIN transactions t ON t.account_id=a.account_id AND t.date < %s AND t.deleted_at IS NULL
            WHERE a.username=%s AND a.account_id = ANY(%s::uuid[])
            GROUP BY a.account_id
            """,
            (from_dt, username, list(acc_by_id.keys())),
        )
        start_rows = cur.fetchall()
        base_balance = sum(int(r["start_balance"] or 0) for r in start_rows)
    else:
        cur.execute(
            "SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS start_balance FROM transactions t WHERE t.account_id=%s::uuid AND t.date < %s AND t.deleted_at IS NULL",
            (account_id, from_dt),
        )
        base_balance = int(cur.fetchone()["start_balance"] or 0)

    search_pattern = build_search_pattern(query)
    search_sql = "WHERE transaction_name ILIKE %s" if search_pattern else ""
    search_params: list[Any] = [search_pattern] if search_pattern else []

    if scope == "all" and include_switch:
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
                       t.transfer_id::text AS transfer_id,
                       t.category_id::text AS category_id,
                       t.notes,
                       t.tags,
                       t.is_reviewed
                FROM transactions t
                JOIN accounts a ON a.account_id=t.account_id
                WHERE a.username=%s AND t.deleted_at IS NULL AND t.date >= %s AND t.date <= %s
            ),
            non_transfer AS (
                SELECT transaction_id AS event_id, account_id, account_name, transaction_name, amount, date,
                       false AS is_transfer, false AS is_cycle_topup, NULL::text AS transfer_id,
                       category_id, notes, tags, is_reviewed,
                       CASE WHEN transaction_type='debit' THEN amount ELSE -amount END AS signed_delta,
                       CASE WHEN transaction_type='debit' THEN amount ELSE 0 END AS debit,
                       CASE WHEN transaction_type='credit' THEN amount ELSE 0 END AS credit
                FROM tx WHERE transfer_id IS NULL
            ),
            transfer_group AS (
                SELECT 'switch:' || transfer_id AS event_id, NULL::text AS account_id, ''::text AS account_name,
                       CONCAT('Switch: ',
                           COALESCE(MAX(CASE WHEN transaction_type='credit' THEN account_name END), 'Unknown'),
                           ' → ',
                           COALESCE(MAX(CASE WHEN transaction_type='debit' THEN account_name END), 'Unknown')
                       ) AS transaction_name,
                       COALESCE(MAX(CASE WHEN transaction_type='debit' THEN amount ELSE 0 END), 0) AS amount,
                       MAX(date) AS date, true AS is_transfer, BOOL_OR(is_cycle_topup) AS is_cycle_topup, transfer_id,
                       MAX(category_id) AS category_id, NULL::text AS notes, '{{}}'::text[] AS tags, true AS is_reviewed,
                       0::bigint AS signed_delta,
                       COALESCE(MAX(CASE WHEN transaction_type='debit' THEN amount ELSE 0 END), 0) AS debit,
                       COALESCE(MAX(CASE WHEN transaction_type='credit' THEN amount ELSE 0 END), 0) AS credit
                FROM tx WHERE transfer_id IS NOT NULL GROUP BY transfer_id
            ),
            events AS (SELECT * FROM non_transfer UNION ALL SELECT * FROM transfer_group),
            events_running AS (
                SELECT *, SUM(signed_delta) OVER (ORDER BY date ASC, event_id ASC) AS running_delta FROM events
            )
            SELECT event_id, account_id, account_name, transaction_name, amount, date,
                   is_transfer, is_cycle_topup, transfer_id, category_id, notes, tags, is_reviewed,
                   debit, credit, running_delta
            FROM events_running
            {search_sql}
            ORDER BY date {order_dir}, event_id {order_dir}
            LIMIT %s OFFSET %s
        """
        params: list[Any] = [username, from_dt, to_dt] + search_params + [limit + 1, offset]
    elif scope == "all":
        sql = f"""
            WITH events AS (
                SELECT t.transaction_id::text AS event_id, t.account_id::text AS account_id, a.account_name,
                       t.transaction_name, t.amount, t.date, false AS is_transfer, t.is_cycle_topup,
                       NULL::text AS transfer_id,
                       t.category_id::text AS category_id,
                       t.notes,
                       t.tags,
                       t.is_reviewed,
                       CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END AS signed_delta,
                       CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END AS debit,
                       CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END AS credit
                FROM transactions t JOIN accounts a ON a.account_id=t.account_id
                WHERE a.username=%s AND t.deleted_at IS NULL AND t.date >= %s AND t.date <= %s AND t.transfer_id IS NULL
            ),
            events_running AS (SELECT *, SUM(signed_delta) OVER (ORDER BY date ASC, event_id ASC) AS running_delta FROM events)
            SELECT event_id, account_id, account_name, transaction_name, amount, date,
                   is_transfer, is_cycle_topup, transfer_id, category_id, notes, tags, is_reviewed,
                   debit, credit, running_delta
            FROM events_running
            {search_sql}
            ORDER BY date {order_dir}, event_id {order_dir}
            LIMIT %s OFFSET %s
        """
        params = [username, from_dt, to_dt] + search_params + [limit + 1, offset]
    else:
        sql = f"""
            WITH base AS (
                SELECT t.transaction_id::text AS transaction_id, t.account_id::text AS account_id, a.account_name,
                       t.transaction_type, t.transaction_name, t.amount, t.date, t.is_transfer, t.is_cycle_topup,
                       t.transfer_id::text AS transfer_id,
                       t.category_id::text AS category_id,
                       t.notes,
                       t.tags,
                       t.is_reviewed,
                       SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END)
                         OVER (ORDER BY t.date ASC, t.transaction_id ASC) AS running_delta
                FROM transactions t JOIN accounts a ON a.account_id=t.account_id
                WHERE a.username=%s AND t.deleted_at IS NULL AND t.date >= %s AND t.date <= %s AND t.account_id=%s::uuid
            )
            SELECT transaction_id AS event_id, account_id, account_name, transaction_name, amount, date,
                   is_transfer, is_cycle_topup, transfer_id,
                   category_id, notes, tags, is_reviewed,
                   CASE WHEN transaction_type='debit' THEN amount ELSE 0 END AS debit,
                   CASE WHEN transaction_type='credit' THEN amount ELSE 0 END AS credit,
                   running_delta
            FROM base
            {search_sql}
            ORDER BY date {order_dir}, event_id {order_dir}
            LIMIT %s OFFSET %s
        """
        params = [username, from_dt, to_dt, account_id] + search_params + [limit + 1, offset]

    cur.execute(sql, params)
    raw_rows = cur.fetchall()
    has_more = len(raw_rows) > limit
    if has_more:
        raw_rows = raw_rows[:limit]

    rows = [
        {
            "no": offset + idx,
            "account_id": r.get("account_id"),
            "account_name": r.get("account_name") or "",
            "date": r["date"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "transaction_id": r["event_id"],
            "transaction_name": r["transaction_name"],
            "debit": int(r.get("debit") or 0),
            "credit": int(r.get("credit") or 0),
            "balance": base_balance + int(r.get("running_delta") or 0),
            "is_transfer": bool(r.get("is_transfer")),
            "is_cycle_topup": bool(r.get("is_cycle_topup")),
            "transfer_id": r.get("transfer_id"),
            "category_id": r.get("category_id"),
            "notes": r.get("notes"),
            "tags": list(r.get("tags") or []),
            "is_reviewed": bool(r.get("is_reviewed")),
        }
        for idx, r in enumerate(raw_rows, start=1)
    ]

    paging = {"limit": limit, "offset": offset, "has_more": has_more, "next_offset": offset + len(rows)}
    return rows, summary_accounts, total_asset, paging
