import base64
import json
import uuid
from datetime import timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from psycopg.errors import UniqueViolation

from app.core.config import settings
from app.db.pool import db_conn
from app.models.public import (
    AccountCreateRequest,
    ApiKeyInfoResponse,
    ApiKeyResetResponse,
    CursorLedgerResponse,
    EmptyBodyRequest,
    LedgerListRequest,
    PeriodQuery,
    PublicRegisterRequest,
    PublicRegisterResponse,
    TransactionUpsertRequest,
)
from app.services.auth import (
    create_api_key,
    enforce_public_rate_limit,
    enforce_register_rate_limit,
    get_api_user_by_token,
    get_active_api_key,
    parse_bearer_token,
    register_user,
)
from app.services.ledger import (
    build_daily_series,
    build_ledger_page,
    build_weekly_series,
    cache_get,
    cache_set,
    compute_budget_status,
    compute_month_range,
    ensure_account_non_negative,
    get_account_balances,
    get_default_payday_day,
    get_payday_day,
    invalidate_user_cache,
    lock_accounts_for_update,
    now_utc,
    parse_date_utc,
    parse_month,
    parse_tx_datetime,
    prev_month_str,
)

router = APIRouter(prefix="/v1")


def require_public_user(req: Request) -> str:
    token = parse_bearer_token(req)
    enforce_public_rate_limit(req, token)
    return get_api_user_by_token(token)


def encode_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def decode_cursor(token: str | None) -> dict[str, Any] | None:
    if not token:
        return None
    padded = token + "=" * (-len(token) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        payload = json.loads(decoded)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid cursor")
    return payload


def resolve_period_month(month: str | None, year: str | None) -> str:
    if month is None and year is None:
        return now_utc().strftime("%Y-%m")
    if month is None or year is None:
        raise HTTPException(status_code=400, detail="month and year must be provided together")

    month_clean = str(month).strip()
    year_clean = str(year).strip()
    if len(month_clean) != 2 or not month_clean.isdigit():
        raise HTTPException(status_code=400, detail="month must be MM format")
    if len(year_clean) != 4 or not year_clean.isdigit():
        raise HTTPException(status_code=400, detail="year must be YYYY format")

    month_num = int(month_clean)
    if month_num < 1 or month_num > 12:
        raise HTTPException(status_code=400, detail="month must be between 01 and 12")

    value = f"{year_clean}-{month_clean}"
    parse_month(value)
    return value


@router.post("/auth/register", response_model=PublicRegisterResponse)
async def public_register(req: Request, payload: PublicRegisterRequest):
    enforce_register_rate_limit(req)
    with db_conn() as conn, conn.cursor() as cur:
        try:
            username, full_name, api_key = register_user(cur, payload.model_dump())
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="User already exists")
    return PublicRegisterResponse(username=username, full_name=full_name, api_key=api_key)


@router.post("/api-key/info", response_model=ApiKeyInfoResponse)
def public_get_api_key(req: Request, _payload: EmptyBodyRequest):
    username = require_public_user(req)
    with db_conn() as conn, conn.cursor() as cur:
        key_meta = get_active_api_key(cur, username)
    if not key_meta:
        raise HTTPException(status_code=404, detail="API key not found")
    return {"api_key": key_meta}


@router.post("/api-key/reset", response_model=ApiKeyResetResponse)
def public_reset_api_key(req: Request, _payload: EmptyBodyRequest):
    username = require_public_user(req)
    with db_conn() as conn, conn.cursor() as cur:
        new_key = create_api_key(cur, username, "reset")
        key_meta = get_active_api_key(cur, username)
        conn.commit()
    if not key_meta:
        raise HTTPException(status_code=500, detail="Failed to generate API key")
    return {"ok": True, "api_key": new_key, "masked": key_meta["key_masked"]}


@router.post("/accounts/list")
def public_accounts(req: Request, _payload: EmptyBodyRequest):
    username = require_public_user(req)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.account_id::text,
                   a.account_name
            FROM accounts a
            WHERE a.username=%s
            ORDER BY a.account_name
            """,
            (username,),
        )
        return {"accounts": cur.fetchall()}


@router.post("/accounts")
def public_create_account(req: Request, payload: AccountCreateRequest):
    username = require_public_user(req)
    account_name = payload.account_name.strip()
    if not account_name:
        raise HTTPException(status_code=400, detail="account_name required")

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO accounts (username, account_name)
                VALUES (%s, %s)
                RETURNING account_id::text
                """,
                (username, account_name),
            )
            account_id = cur.fetchone()["account_id"]
            if payload.initial_balance > 0:
                cur.execute(
                    """
                    INSERT INTO transactions (account_id, transaction_type, transaction_name, amount, date, is_transfer)
                    VALUES (%s::uuid, 'debit', %s, %s, %s, false)
                    """,
                    (account_id, "Top Up Balance", payload.initial_balance, now_utc()),
                )
            if payload.monthly_limit is not None:
                budget_month = now_utc().strftime("%Y-%m")
                cur.execute(
                    """
                    INSERT INTO budgets (username, account_id, month, amount)
                    VALUES (%s, %s::uuid, %s, %s)
                    ON CONFLICT (username, account_id, month)
                    DO UPDATE SET amount=EXCLUDED.amount
                    """,
                    (username, account_id, budget_month, int(payload.monthly_limit)),
                )
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Account name already exists")

    invalidate_user_cache(username)
    return {"ok": True, "account_id": account_id}


@router.post("/transactions")
def public_upsert_transaction(req: Request, payload: TransactionUpsertRequest):
    username = require_public_user(req)

    # Update flow when transaction_id is present.
    if payload.transaction_id:
        transaction_id = payload.transaction_id
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.transaction_id::text AS transaction_id,
                       t.account_id::text AS account_id,
                       t.transaction_type,
                       t.transaction_name,
                       t.amount,
                       t.date,
                       t.is_transfer
                FROM transactions t
                JOIN accounts a ON a.account_id=t.account_id
                WHERE t.transaction_id=%s::uuid AND a.username=%s AND t.deleted_at IS NULL
                """,
                (transaction_id, username),
            )
            tx = cur.fetchone()
            if not tx:
                raise HTTPException(status_code=404, detail="Transaction not found")
            if tx.get("is_transfer"):
                raise HTTPException(status_code=400, detail="Use switch endpoints to edit transfers")

            new_account_id = payload.account_id or tx["account_id"]
            new_type = payload.transaction_type or tx["transaction_type"]
            if new_type not in ("debit", "credit"):
                raise HTTPException(status_code=400, detail="Invalid transaction_type")

            if payload.transaction_name is not None:
                new_name = payload.transaction_name.strip()
                if not new_name:
                    raise HTTPException(status_code=400, detail="transaction_name required")
            else:
                new_name = tx["transaction_name"]

            if payload.amount is not None:
                new_amount = int(payload.amount)
            else:
                new_amount = int(tx["amount"])
            if new_amount <= 0:
                raise HTTPException(status_code=400, detail="amount must be > 0")

            if payload.date:
                new_date = parse_tx_datetime(payload.date)
            else:
                new_date = tx["date"]

            old_account_id = tx["account_id"]
            old_date = tx["date"]

            lock_accounts_for_update(cur, username, [old_account_id, new_account_id])

            if new_account_id != old_account_id:
                ensure_account_non_negative(cur, old_account_id, old_date, [], exclude_tx_ids=[transaction_id])
                ensure_account_non_negative(
                    cur,
                    new_account_id,
                    new_date,
                    [
                        {
                            "transaction_id": transaction_id,
                            "date": new_date,
                            "transaction_type": new_type,
                            "amount": new_amount,
                        }
                    ],
                )
            else:
                effective_from = min(old_date, new_date)
                ensure_account_non_negative(
                    cur,
                    old_account_id,
                    effective_from,
                    [
                        {
                            "transaction_id": transaction_id,
                            "date": new_date,
                            "transaction_type": new_type,
                            "amount": new_amount,
                        }
                    ],
                    exclude_tx_ids=[transaction_id],
                )

            cur.execute(
                """
                UPDATE transactions
                SET account_id=%s::uuid,
                    transaction_type=%s,
                    transaction_name=%s,
                    amount=%s,
                    date=%s
                WHERE transaction_id=%s::uuid AND deleted_at IS NULL
                RETURNING transaction_id::text
                """,
                (new_account_id, new_type, new_name, new_amount, new_date, transaction_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Transaction not found")
            conn.commit()

        invalidate_user_cache(username)
        return {"ok": True, "transaction_id": transaction_id}

    # Create flow when transaction_id is absent.
    account_id = payload.account_id
    tx_type = payload.transaction_type
    name = (payload.transaction_name or "").strip()
    amount = payload.amount

    if not account_id:
        raise HTTPException(status_code=400, detail="account_id required")
    if tx_type not in ("debit", "credit"):
        raise HTTPException(status_code=400, detail="Invalid transaction_type")
    if not name:
        raise HTTPException(status_code=400, detail="transaction_name required")
    if amount is None or int(amount) <= 0:
        raise HTTPException(status_code=400, detail="amount must be > 0")

    dt = parse_tx_datetime(payload.date)
    with db_conn() as conn, conn.cursor() as cur:
        lock_accounts_for_update(cur, username, [account_id])
        temp_id = str(uuid.uuid4())
        ensure_account_non_negative(
            cur,
            account_id,
            dt,
            [
                {
                    "transaction_id": temp_id,
                    "date": dt,
                    "transaction_type": tx_type,
                    "amount": int(amount),
                }
            ],
        )

        cur.execute(
            """
            INSERT INTO transactions (account_id, transaction_type, transaction_name, amount, date, is_transfer)
            VALUES (%s::uuid, %s, %s, %s, %s, false)
            RETURNING transaction_id::text
            """,
            (account_id, tx_type, name, int(amount), dt),
        )
        tx_id = cur.fetchone()["transaction_id"]
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "transaction_id": tx_id}


@router.post("/ledger", response_model=CursorLedgerResponse)
def public_ledger(req: Request, payload: LedgerListRequest):
    username = require_public_user(req)
    query = payload
    scope = query.scope
    account_id = query.account_id
    from_date = query.from_date
    to_date = query.to_date
    limit = query.limit
    cursor = query.cursor
    order = query.order
    q = query.q

    if not to_date:
        to_dt = now_utc()
        to_date = to_dt.strftime("%Y-%m-%d")
    if not from_date:
        from_dt = parse_date_utc(to_date, end_of_day=False) - timedelta(days=30)
        from_date = from_dt.strftime("%Y-%m-%d")

    from_dt = parse_date_utc(from_date, end_of_day=False)
    to_dt = parse_date_utc(to_date, end_of_day=True)

    decoded = decode_cursor(cursor) or {}
    try:
        offset = int(decoded.get("offset", 0) or 0)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid cursor")
    if offset < 0:
        raise HTTPException(status_code=400, detail="Invalid cursor")
    limit = max(1, min(int(limit or 25), 100))

    # Cursor binds to query parameters to prevent accidental cross-query reuse.
    if decoded:
        if decoded.get("scope") != scope:
            raise HTTPException(status_code=400, detail="Cursor does not match scope")
        if (decoded.get("account_id") or None) != account_id:
            raise HTTPException(status_code=400, detail="Cursor does not match account_id")
        if decoded.get("from_date") != from_date or decoded.get("to_date") != to_date:
            raise HTTPException(status_code=400, detail="Cursor does not match date range")
        if decoded.get("order") != order:
            raise HTTPException(status_code=400, detail="Cursor does not match order")
        if (decoded.get("q") or None) != q:
            raise HTTPException(status_code=400, detail="Cursor does not match query")

    with db_conn() as conn, conn.cursor() as cur:
        rows, summary_accounts, total_asset, paging = build_ledger_page(
            cur,
            username,
            scope,
            account_id,
            from_dt,
            to_dt,
            limit,
            offset,
            order,
            q,
            True,
        )

    next_cursor = None
    if paging.get("has_more"):
        next_cursor = encode_cursor(
            {
                "offset": int(paging.get("next_offset") or 0),
                "scope": scope,
                "account_id": account_id,
                "from_date": from_date,
                "to_date": to_date,
                "order": order,
                "q": q,
            }
        )

    return CursorLedgerResponse(
        scope=scope,
        range={"from": from_date, "to": to_date},
        rows=rows,
        paging={
            "limit": int(paging.get("limit") or limit),
            "has_more": bool(paging.get("has_more")),
            "next_cursor": next_cursor,
        },
    )


@router.post("/summary")
def public_summary(req: Request, payload: PeriodQuery):
    username = require_public_user(req)
    month = resolve_period_month(payload.month, payload.year)

    cache_key = f"{username}:summary:{month}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    with db_conn() as conn, conn.cursor() as cur:
        payday_day, payday_source, override_day = get_payday_day(cur, username, month)
        default_day = get_default_payday_day(cur, username)
        prev_day, _, _ = get_payday_day(cur, username, prev_month_str(month))
        from_date, to_date, from_dt, to_dt = compute_month_range(month, payday_day, prev_day)
        start_cutoff = from_dt - timedelta(milliseconds=1)

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
            payload = {
                "range": {"from": from_date, "to": to_date},
                "month": month,
                "payday": {
                    "day": payday_day,
                    "source": payday_source,
                    "default_day": default_day,
                    "override_day": override_day,
                },
                "total_asset": 0,
                "accounts": [],
            }
            cache_set(cache_key, payload, settings.month_summary_ttl)
            return payload

        balances_start = get_account_balances(cur, username, start_cutoff)
        balances_end = get_account_balances(cur, username, to_dt)
        total_asset = sum(int(balances_end.get(acc["account_id"], 0)) for acc in accounts)

        cur.execute(
            """
            SELECT t.account_id::text AS account_id,
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE a.username=%s
              AND t.deleted_at IS NULL
              AND t.transfer_id IS NULL
              AND t.date >= %s
              AND t.date <= %s
            GROUP BY t.account_id
            """,
            (username, from_dt, to_dt),
        )
        totals = {r["account_id"]: r for r in cur.fetchall()}

        cur.execute(
            """
            SELECT budget_id::text AS budget_id,
                   account_id::text AS account_id,
                   amount
            FROM budgets
            WHERE username=%s AND month=%s
            """,
            (username, month),
        )
        budgets = {
            r["account_id"]: {"amount": int(r["amount"] or 0), "budget_id": r["budget_id"]}
            for r in cur.fetchall()
        }

    accounts_sorted = sorted(accounts, key=lambda a: a["account_name"].lower())
    payload_accounts = []
    for acc in accounts_sorted:
        acc_id = acc["account_id"]
        total_row = totals.get(acc_id, {})
        total_in = int(total_row.get("total_in") or 0)
        total_out = int(total_row.get("total_out") or 0)
        budget_info = budgets.get(acc_id)
        budget_amount = budget_info["amount"] if budget_info else None
        budget_id = budget_info["budget_id"] if budget_info else None
        budget_used = total_out if budget_amount is not None else None
        budget_pct, budget_status, budget_remaining = compute_budget_status(budget_amount, total_out)
        starting_balance = int(balances_start.get(acc_id, 0))
        current_balance = int(balances_end.get(acc_id, 0))
        payload_accounts.append(
            {
                "account_id": acc_id,
                "account_name": acc["account_name"],
                "starting_balance": starting_balance,
                "current_balance": current_balance,
                "total_in": total_in,
                "total_out": total_out,
                "budget_id": budget_id,
                "budget": int(budget_amount) if budget_amount is not None else None,
                "budget_used": int(budget_used) if budget_used is not None else None,
                "budget_remaining": int(budget_remaining) if budget_remaining is not None else None,
                "budget_pct": int(budget_pct) if budget_pct is not None else None,
                "budget_status": budget_status,
            }
        )

    payload = {
        "range": {"from": from_date, "to": to_date},
        "month": month,
        "payday": {
            "day": payday_day,
            "source": payday_source,
            "default_day": default_day,
            "override_day": override_day,
        },
        "total_asset": int(total_asset),
        "accounts": payload_accounts,
    }
    cache_set(cache_key, payload, settings.month_summary_ttl)
    return payload


@router.post("/analysis")
def public_analysis(req: Request, payload: PeriodQuery):
    username = require_public_user(req)
    month = resolve_period_month(payload.month, payload.year)

    cache_key = f"{username}:analysis:{month}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    with db_conn() as conn, conn.cursor() as cur:
        payday_day, payday_source, override_day = get_payday_day(cur, username, month)
        default_day = get_default_payday_day(cur, username)
        prev_day, _, _ = get_payday_day(cur, username, prev_month_str(month))
        from_date, to_date, from_dt, to_dt = compute_month_range(month, payday_day, prev_day)
        start_cutoff = from_dt - timedelta(milliseconds=1)

        base_filters = ["a.username=%s", "t.deleted_at IS NULL", "t.date >= %s", "t.date <= %s"]
        params: list[Any] = [username, from_dt, to_dt]

        cur.execute(
            f"""
            SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {' AND '.join(base_filters)}
              AND t.transfer_id IS NULL
            """,
            params,
        )
        totals_row = cur.fetchone() or {}
        total_in = int(totals_row.get("total_in") or 0)
        total_out = int(totals_row.get("total_out") or 0)

        cur.execute(
            f"""
            SELECT (t.date AT TIME ZONE 'UTC')::date AS day,
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {' AND '.join(base_filters)}
              AND t.transfer_id IS NULL
            GROUP BY day
            ORDER BY day
            """,
            params,
        )
        daily_rows = cur.fetchall()
        daily_series = build_daily_series(from_date, to_date, daily_rows)
        weekly_series = build_weekly_series(from_date, to_date, daily_series)

        cur.execute(
            f"""
            SELECT t.account_id::text AS account_id,
                   a.account_name,
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {' AND '.join(base_filters)}
              AND t.transfer_id IS NULL
            GROUP BY t.account_id, a.account_name
            ORDER BY total_out DESC, a.account_name ASC
            """,
            params,
        )
        categories_raw = cur.fetchall()

        cur.execute(
            """
            SELECT account_id::text AS account_id
            FROM accounts
            WHERE username=%s
            """,
            (username,),
        )
        acc_rows = cur.fetchall()
        balances_start = get_account_balances(cur, username, start_cutoff)
        balances = get_account_balances(cur, username, to_dt)
        total_asset = sum(int(balances.get(r["account_id"], 0)) for r in acc_rows)

    categories = []
    for row in categories_raw:
        account_id = row.get("account_id")
        total_in_cat = int(row.get("total_in") or 0)
        total_out_cat = int(row.get("total_out") or 0)
        starting_balance = int(balances_start.get(account_id, 0))
        usage_pct = int(round((total_out_cat / starting_balance) * 100)) if starting_balance > 0 else None
        categories.append(
            {
                "account_id": account_id,
                "account_name": row.get("account_name"),
                "total_in": total_in_cat,
                "total_out": total_out_cat,
                "net": int(total_in_cat - total_out_cat),
                "starting_balance": starting_balance,
                "usage_pct": usage_pct,
            }
        )

    payload = {
        "range": {"from": from_date, "to": to_date},
        "month": month,
        "payday": {
            "day": payday_day,
            "source": payday_source,
            "default_day": default_day,
            "override_day": override_day,
        },
        "total_asset": int(total_asset),
        "totals": {"total_in": total_in, "total_out": total_out, "net": int(total_in - total_out)},
        "daily": daily_series,
        "weekly": weekly_series,
        "categories": categories,
    }
    cache_set(cache_key, payload, settings.month_summary_ttl)
    return payload
