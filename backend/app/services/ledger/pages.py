"""Ledger page builder and full-data builder (for export)."""
from datetime import timezone
from typing import Any

from fastapi import HTTPException

from app.core.config import settings
from app.services.ledger.balances import parse_uuid_value
from app.services.ledger.cache import cache_get, cache_set
from app.services.ledger.reports import build_search_pattern, compute_summary


_LEDGER_KINDS = {"all", "income", "expense", "transfer", "payroll"}


def _resolve_ledger_accounts(cur, username: str, scope: str, account_id: str | None) -> tuple[dict[str, Any], list[str]]:
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
        return acc_by_id, [account_id]
    return acc_by_id, list(acc_by_id.keys())


def _ledger_event_filter_sql(
    query: str | None,
    category_id: str | None,
    kind: str | None,
) -> tuple[str, list[Any]]:
    ledger_kind = str(kind or "all").strip().lower()
    if ledger_kind not in _LEDGER_KINDS:
        raise HTTPException(status_code=400, detail="Invalid ledger type filter")

    clauses: list[str] = []
    params: list[Any] = []
    search_pattern = build_search_pattern(query)
    if search_pattern:
        clauses.append("(transaction_name ILIKE %s OR account_name ILIKE %s OR COALESCE(category_name, '') ILIKE %s)")
        params.extend([search_pattern, search_pattern, search_pattern])

    if category_id:
        parsed_category_id = parse_uuid_value(category_id, "category_id")
        clauses.append("category_id=%s")
        params.append(parsed_category_id)

    if ledger_kind == "income":
        clauses.append("debit > 0 AND is_transfer = false")
    elif ledger_kind == "expense":
        clauses.append("credit > 0 AND is_transfer = false")
    elif ledger_kind == "transfer":
        clauses.append("is_transfer = true")
    elif ledger_kind == "payroll":
        clauses.append("is_cycle_topup = true")

    if not clauses:
        return "", []
    return "WHERE " + " AND ".join(f"({clause})" for clause in clauses), params


def build_ledger_export_summary(
    cur,
    username: str,
    scope: str,
    account_id: str | None,
    from_dt,
    to_dt,
) -> dict[str, int]:
    _, acc_ids = _resolve_ledger_accounts(cur, username, scope, account_id)
    if not acc_ids:
        return {"count": 0, "total_in": 0, "total_out": 0, "net": 0}

    cur.execute(
        """
        SELECT COUNT(*) AS row_count,
               COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
               COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
        FROM transactions t
        JOIN accounts a ON a.account_id=t.account_id
        WHERE a.username=%s AND t.account_id = ANY(%s::uuid[])
          AND t.deleted_at IS NULL AND t.transfer_id IS NULL AND t.date >= %s AND t.date <= %s
        """,
        (username, acc_ids, from_dt, to_dt),
    )
    row = cur.fetchone() or {}
    total_in = int(row.get("total_in") or 0)
    total_out = int(row.get("total_out") or 0)
    return {
        "count": int(row.get("row_count") or 0),
        "total_in": total_in,
        "total_out": total_out,
        "net": int(total_in - total_out),
    }


def build_ledger_data(
    cur,
    username: str,
    scope: str,
    account_id: str | None,
    from_dt,
    to_dt,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    acc_by_id, acc_ids = _resolve_ledger_accounts(cur, username, scope, account_id)
    if not acc_ids:
        return [], [], 0

    export_summary = build_ledger_export_summary(cur, username, scope, account_id, from_dt, to_dt)
    if export_summary["count"] > settings.ledger_export_max_rows:
        raise HTTPException(
            status_code=413,
            detail=f"Export too large. Narrow the date range or raise LEDGER_EXPORT_MAX_ROWS above {settings.ledger_export_max_rows}.",
        )

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
    category_id: str | None = None,
    kind: str = "all",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, dict[str, int | bool]]:
    acc_by_id, acc_ids = _resolve_ledger_accounts(cur, username, scope, account_id)
    if not acc_by_id:
        return [], [], 0, {"limit": limit, "offset": offset, "has_more": False, "next_offset": offset}
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
            (from_dt, username, acc_ids),
        )
        start_rows = cur.fetchall()
        base_balance = sum(int(r["start_balance"] or 0) for r in start_rows)
    else:
        cur.execute(
            "SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS start_balance FROM transactions t WHERE t.account_id=%s::uuid AND t.date < %s AND t.deleted_at IS NULL",
            (account_id, from_dt),
        )
        base_balance = int(cur.fetchone()["start_balance"] or 0)

    filter_sql, filter_params = _ledger_event_filter_sql(query, category_id, kind)

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
                       c.name AS category_name,
                       t.notes,
                       t.tags,
                       t.is_reviewed
                FROM transactions t
                JOIN accounts a ON a.account_id=t.account_id
                LEFT JOIN categories c ON c.category_id=t.category_id
                WHERE a.username=%s AND t.deleted_at IS NULL AND t.date >= %s AND t.date <= %s
            ),
            allocation_transfer AS (
                SELECT DISTINCT unnest(transfer_ids)::text AS transfer_id
                FROM allocation_funding_runs
            ),
            non_transfer AS (
                SELECT transaction_id AS event_id, account_id, account_name, transaction_name, amount, date,
                       false AS is_transfer, false AS is_cycle_topup, NULL::text AS transfer_id,
                       category_id, category_name, notes, tags, is_reviewed,
                       CASE WHEN transaction_type='debit' THEN amount ELSE -amount END AS signed_delta,
                       CASE WHEN transaction_type='debit' THEN amount ELSE 0 END AS debit,
                       CASE WHEN transaction_type='credit' THEN amount ELSE 0 END AS credit
                FROM tx WHERE transfer_id IS NULL
            ),
            transfer_group AS (
                SELECT 'movement:' || tx.transfer_id AS event_id, NULL::text AS account_id, ''::text AS account_name,
                       CONCAT(
                           CASE WHEN BOOL_OR(allocation_transfer.transfer_id IS NOT NULL) THEN 'Allocation Funding: ' ELSE 'Move: ' END,
                           COALESCE(MAX(CASE WHEN tx.transaction_type='credit' THEN tx.account_name END), 'Unknown'),
                           ' → ',
                           COALESCE(MAX(CASE WHEN tx.transaction_type='debit' THEN tx.account_name END), 'Unknown')
                       ) AS transaction_name,
                       COALESCE(MAX(CASE WHEN tx.transaction_type='debit' THEN tx.amount ELSE 0 END), 0) AS amount,
                       MAX(tx.date) AS date, true AS is_transfer, BOOL_OR(tx.is_cycle_topup) AS is_cycle_topup, tx.transfer_id,
                       MAX(tx.category_id) AS category_id, MAX(tx.category_name) AS category_name, NULL::text AS notes, '{{}}'::text[] AS tags, true AS is_reviewed,
                       0::bigint AS signed_delta,
                       COALESCE(MAX(CASE WHEN tx.transaction_type='debit' THEN tx.amount ELSE 0 END), 0) AS debit,
                       COALESCE(MAX(CASE WHEN tx.transaction_type='credit' THEN tx.amount ELSE 0 END), 0) AS credit
                FROM tx
                LEFT JOIN allocation_transfer ON allocation_transfer.transfer_id = tx.transfer_id
                WHERE tx.transfer_id IS NOT NULL
                GROUP BY tx.transfer_id
            ),
            events AS (SELECT * FROM non_transfer UNION ALL SELECT * FROM transfer_group),
            events_running AS (
                SELECT *, SUM(signed_delta) OVER (ORDER BY date ASC, event_id ASC) AS running_delta FROM events
            )
            SELECT event_id, account_id, account_name, transaction_name, amount, date,
                   is_transfer, is_cycle_topup, transfer_id, category_id, notes, tags, is_reviewed,
                   debit, credit, running_delta
            FROM events_running
            {filter_sql}
            ORDER BY date {order_dir}, event_id {order_dir}
            LIMIT %s OFFSET %s
        """
        params: list[Any] = [username, from_dt, to_dt] + filter_params + [limit + 1, offset]
    elif scope == "all":
        sql = f"""
            WITH events AS (
                SELECT t.transaction_id::text AS event_id, t.account_id::text AS account_id, a.account_name,
                       t.transaction_name, t.amount, t.date, false AS is_transfer, t.is_cycle_topup,
                       NULL::text AS transfer_id,
                       t.category_id::text AS category_id,
                       c.name AS category_name,
                       t.notes,
                       t.tags,
                       t.is_reviewed,
                       CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END AS signed_delta,
                       CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END AS debit,
                       CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END AS credit
                FROM transactions t
                JOIN accounts a ON a.account_id=t.account_id
                LEFT JOIN categories c ON c.category_id=t.category_id
                WHERE a.username=%s AND t.deleted_at IS NULL AND t.date >= %s AND t.date <= %s AND t.transfer_id IS NULL
            ),
            events_running AS (SELECT *, SUM(signed_delta) OVER (ORDER BY date ASC, event_id ASC) AS running_delta FROM events)
            SELECT event_id, account_id, account_name, transaction_name, amount, date,
                   is_transfer, is_cycle_topup, transfer_id, category_id, notes, tags, is_reviewed,
                   debit, credit, running_delta
            FROM events_running
            {filter_sql}
            ORDER BY date {order_dir}, event_id {order_dir}
            LIMIT %s OFFSET %s
        """
        params = [username, from_dt, to_dt] + filter_params + [limit + 1, offset]
    else:
        sql = f"""
            WITH base AS (
                SELECT t.transaction_id::text AS transaction_id, t.account_id::text AS account_id, a.account_name,
                       t.transaction_type, t.transaction_name, t.amount, t.date, t.is_transfer, t.is_cycle_topup,
                       t.transfer_id::text AS transfer_id,
                       t.category_id::text AS category_id,
                       c.name AS category_name,
                       t.notes,
                       t.tags,
                       t.is_reviewed,
                       SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END)
                         OVER (ORDER BY t.date ASC, t.transaction_id ASC) AS running_delta
                FROM transactions t
                JOIN accounts a ON a.account_id=t.account_id
                LEFT JOIN categories c ON c.category_id=t.category_id
                WHERE a.username=%s AND t.deleted_at IS NULL AND t.date >= %s AND t.date <= %s AND t.account_id=%s::uuid
            )
            SELECT transaction_id AS event_id, account_id, account_name, transaction_name, amount, date,
                   is_transfer, is_cycle_topup, transfer_id,
                   category_id, notes, tags, is_reviewed,
                   CASE WHEN transaction_type='debit' THEN amount ELSE 0 END AS debit,
                   CASE WHEN transaction_type='credit' THEN amount ELSE 0 END AS credit,
                   running_delta
            FROM base
            {filter_sql}
            ORDER BY date {order_dir}, event_id {order_dir}
            LIMIT %s OFFSET %s
        """
        params = [username, from_dt, to_dt, account_id] + filter_params + [limit + 1, offset]

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
