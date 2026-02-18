import base64
import json
import uuid
from datetime import timedelta, timezone
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response
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
    build_ledger_data,
    build_ledger_page,
    build_weekly_series,
    cache_get,
    cache_set,
    compute_budget_shift_analysis,
    compute_budget_status,
    compute_export_range,
    compute_month_range,
    ensure_account_non_negative,
    export_ledger_file,
    get_account_balances,
    get_default_payday_day,
    get_payday_day,
    invalidate_user_cache,
    lock_accounts_for_update,
    now_utc,
    parse_currency,
    parse_date_utc,
    parse_month,
    parse_tx_datetime,
    parse_uuid_value,
    prev_month_str,
    recompute_balances_report,
    write_transaction_audit,
)
from app.services.receipts import (
    build_receipt_relative_path,
    delete_receipt_row,
    get_receipt_row,
    infer_inline_filename,
    load_receipt_content,
    prepare_receipt_payload,
    remove_receipt_file,
    require_transaction_owner,
    serialize_receipt_row,
    store_receipt,
    upsert_receipt_row,
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


def parse_optional_bool(value: Any, field_name: str) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "on"):
            return True
        if lowered in ("false", "0", "no", "off", ""):
            return False
    raise HTTPException(status_code=400, detail=f"Invalid {field_name}, expected boolean")


def parse_int_field(value: Any, field_name: str, default: int | None = None) -> int:
    if value is None or (isinstance(value, str) and not value.strip()):
        if default is not None:
            return default
        raise HTTPException(status_code=400, detail=f"{field_name} required")
    if isinstance(value, bool):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")
    try:
        return int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


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
                    INSERT INTO transactions (
                        account_id,
                        transaction_type,
                        is_cycle_topup,
                        transaction_name,
                        amount,
                        date,
                        is_transfer
                    )
                    VALUES (%s::uuid, 'debit', true, %s, %s, %s, false)
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
        transaction_id = parse_uuid_value(payload.transaction_id, "transaction_id")
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.transaction_id::text AS transaction_id,
                       t.account_id::text AS account_id,
                       t.transaction_type,
                       t.is_cycle_topup,
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
            if payload.is_cycle_topup is None:
                is_cycle_topup = bool(tx.get("is_cycle_topup"))
            else:
                is_cycle_topup = bool(payload.is_cycle_topup)
            if is_cycle_topup and new_type != "debit":
                raise HTTPException(status_code=400, detail="Top-up/payroll can only be set on cash-in transactions")

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
                    is_cycle_topup=%s,
                    transaction_name=%s,
                    amount=%s,
                    date=%s
                WHERE transaction_id=%s::uuid AND deleted_at IS NULL
                RETURNING transaction_id::text
                """,
                (new_account_id, new_type, is_cycle_topup, new_name, new_amount, new_date, transaction_id),
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
    is_cycle_topup = bool(payload.is_cycle_topup) if payload.is_cycle_topup is not None else False
    if is_cycle_topup and tx_type != "debit":
        raise HTTPException(status_code=400, detail="Top-up/payroll can only be set on cash-in transactions")
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
            INSERT INTO transactions (
                account_id,
                transaction_type,
                is_cycle_topup,
                transaction_name,
                amount,
                date,
                is_transfer
            )
            VALUES (%s::uuid, %s, %s, %s, %s, %s, false)
            RETURNING transaction_id::text
            """,
            (account_id, tx_type, is_cycle_topup, name, int(amount), dt),
        )
        tx_id = cur.fetchone()["transaction_id"]
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "transaction_id": tx_id}


@router.post("/transactions/{transaction_id}/receipt")
async def public_upload_tx_receipt(
    transaction_id: str,
    req: Request,
    file: UploadFile = File(...),
    category: str | None = Form(None),
):
    username = require_public_user(req)
    transaction_id = parse_uuid_value(transaction_id, "transaction_id")
    raw = await file.read()
    prepared = prepare_receipt_payload(
        raw=raw,
        filename=file.filename,
        content_type=file.content_type,
        category=category,
    )

    relative_path = build_receipt_relative_path(
        username=username,
        transaction_id=transaction_id,
        category=prepared.category,
        ext=prepared.stored_ext,
    )
    old_relative_path: str | None = None
    stored_new_file = False

    try:
        store_receipt(relative_path, prepared.content)
        stored_new_file = True
        with db_conn() as conn, conn.cursor() as cur:
            require_transaction_owner(cur, username, transaction_id)
            row, old_relative_path = upsert_receipt_row(
                cur,
                username=username,
                transaction_id=transaction_id,
                prepared=prepared,
                relative_path=relative_path,
            )
            conn.commit()
    except HTTPException:
        if stored_new_file:
            remove_receipt_file(relative_path)
        raise
    except Exception:
        if stored_new_file:
            remove_receipt_file(relative_path)
        raise

    if old_relative_path and old_relative_path != relative_path:
        remove_receipt_file(old_relative_path)

    return {"ok": True, "receipt": serialize_receipt_row(row)}


@router.get("/transactions/{transaction_id}/receipt")
def public_get_tx_receipt(transaction_id: str, req: Request):
    username = require_public_user(req)
    transaction_id = parse_uuid_value(transaction_id, "transaction_id")
    with db_conn() as conn, conn.cursor() as cur:
        require_transaction_owner(cur, username, transaction_id)
        row = get_receipt_row(cur, username, transaction_id)
    if not row:
        raise HTTPException(status_code=404, detail="Receipt not found")
    return {"receipt": serialize_receipt_row(row)}


@router.get("/transactions/{transaction_id}/receipt/view")
def public_view_tx_receipt(transaction_id: str, req: Request):
    username = require_public_user(req)
    transaction_id = parse_uuid_value(transaction_id, "transaction_id")
    with db_conn() as conn, conn.cursor() as cur:
        require_transaction_owner(cur, username, transaction_id)
        row = get_receipt_row(cur, username, transaction_id)
    if not row:
        raise HTTPException(status_code=404, detail="Receipt not found")
    content = load_receipt_content(row["relative_path"], row["storage_encoding"])
    filename = infer_inline_filename(row["transaction_id"], row["category"], row["stored_mime"])
    return Response(
        content=content,
        media_type=row["stored_mime"],
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "private, max-age=60",
        },
    )


@router.delete("/transactions/{transaction_id}/receipt")
def public_delete_tx_receipt(transaction_id: str, req: Request):
    username = require_public_user(req)
    transaction_id = parse_uuid_value(transaction_id, "transaction_id")
    with db_conn() as conn, conn.cursor() as cur:
        require_transaction_owner(cur, username, transaction_id)
        deleted = delete_receipt_row(cur, username, transaction_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Receipt not found")
        conn.commit()
    remove_receipt_file(deleted.get("relative_path"))
    return {"ok": True}


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
            f"""
            SELECT t.account_id::text AS account_id,
                   COALESCE(SUM(t.amount), 0) AS topup_base
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {' AND '.join(base_filters)}
              AND t.is_cycle_topup = TRUE
              AND t.transaction_type = 'debit'
            GROUP BY t.account_id
            """,
            params,
        )
        topup_raw = cur.fetchall()

        cur.execute(
            """
            SELECT account_id::text AS account_id
            FROM accounts
            WHERE username=%s
            """,
            (username,),
        )
        acc_rows = cur.fetchall()
        balances = get_account_balances(cur, username, to_dt)
        total_asset = sum(int(balances.get(r["account_id"], 0)) for r in acc_rows)

    topup_by_account = {r.get("account_id"): int(r.get("topup_base") or 0) for r in topup_raw}

    categories = []
    for row in categories_raw:
        account_id = row.get("account_id")
        total_in_cat = int(row.get("total_in") or 0)
        total_out_cat = int(row.get("total_out") or 0)
        topup_base = int(topup_by_account.get(account_id, 0))
        usage_pct = int(round((total_out_cat / topup_base) * 100)) if topup_base > 0 else None
        categories.append(
            {
                "account_id": account_id,
                "account_name": row.get("account_name"),
                "total_in": total_in_cat,
                "total_out": total_out_cat,
                "net": int(total_in_cat - total_out_cat),
                "topup_base": topup_base,
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


@router.post("/analysis/budget-shift")
def public_budget_shift_analysis(req: Request, payload: PeriodQuery):
    username = require_public_user(req)
    month = resolve_period_month(payload.month, payload.year)

    cache_key = f"{username}:analysis:budget_shift:{month}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    with db_conn() as conn, conn.cursor() as cur:
        payday_day, _, _ = get_payday_day(cur, username, month)
        prev_day, _, _ = get_payday_day(cur, username, prev_month_str(month))
        _, _, from_dt, to_dt = compute_month_range(month, payday_day, prev_day)
        result = compute_budget_shift_analysis(cur, username, month, from_dt, to_dt)

    cache_set(cache_key, result, settings.month_summary_ttl)
    return result


@router.put("/accounts/{account_id}")
def public_update_account(account_id: str, req: Request, payload: AccountCreateRequest):
    username = require_public_user(req)
    account_id = parse_uuid_value(account_id, "account_id")
    account_name = payload.account_name.strip()
    if not account_name:
        raise HTTPException(status_code=400, detail="account_name required")

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM accounts WHERE username=%s AND account_id=%s::uuid",
            (username, account_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Account not found")
        try:
            cur.execute(
                "UPDATE accounts SET account_name=%s WHERE username=%s AND account_id=%s::uuid",
                (account_name, username, account_id),
            )
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Account name already exists")

    invalidate_user_cache(username)
    return {"ok": True}


@router.delete("/accounts/{account_id}")
def public_delete_account(account_id: str, req: Request):
    username = require_public_user(req)
    account_id = parse_uuid_value(account_id, "account_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM accounts WHERE username=%s AND account_id=%s::uuid RETURNING account_id",
            (username, account_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Account not found")
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.post("/budgets")
def public_upsert_budget(req: Request, payload: dict[str, Any]):
    username = require_public_user(req)
    account_id = parse_uuid_value(payload.get("account_id"), "account_id")
    month = str(payload.get("month") or "").strip()
    amount = parse_int_field(payload.get("amount"), "amount", default=0)
    if not month:
        raise HTTPException(status_code=400, detail="month required")
    parse_month(month)
    if amount < 0:
        raise HTTPException(status_code=400, detail="amount must be >= 0")

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM accounts WHERE username=%s AND account_id=%s::uuid",
            (username, account_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Account not found")
        cur.execute(
            """
            INSERT INTO budgets (username, account_id, month, amount)
            VALUES (%s, %s::uuid, %s, %s)
            ON CONFLICT (username, account_id, month)
            DO UPDATE SET amount=EXCLUDED.amount
            RETURNING budget_id::text
            """,
            (username, account_id, month, amount),
        )
        budget_id = cur.fetchone()["budget_id"]
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "budget_id": budget_id}


@router.get("/budgets")
def public_list_budgets(req: Request, month: str | None = None):
    username = require_public_user(req)
    if not month:
        month = now_utc().strftime("%Y-%m")
    parse_month(month)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.budget_id::text AS budget_id,
                   b.account_id::text AS account_id,
                   b.month,
                   b.amount
            FROM budgets b
            JOIN accounts a ON a.account_id=b.account_id
            WHERE b.username=%s AND b.month=%s
            ORDER BY a.account_name
            """,
            (username, month),
        )
        budgets = cur.fetchall()
    return {"month": month, "budgets": budgets}


@router.put("/budgets/{budget_id}")
def public_update_budget(budget_id: str, req: Request, payload: dict[str, Any]):
    username = require_public_user(req)
    budget_id = parse_uuid_value(budget_id, "budget_id")
    amount = parse_int_field(payload.get("amount"), "amount", default=0)
    if amount < 0:
        raise HTTPException(status_code=400, detail="amount must be >= 0")

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE budgets b
            SET amount=%s
            FROM accounts a
            WHERE b.account_id=a.account_id
              AND b.budget_id=%s::uuid
              AND b.username=%s
            RETURNING b.budget_id
            """,
            (amount, budget_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Budget not found")
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.delete("/budgets/{budget_id}")
def public_delete_budget(budget_id: str, req: Request):
    username = require_public_user(req)
    budget_id = parse_uuid_value(budget_id, "budget_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM budgets b
            USING accounts a
            WHERE b.account_id=a.account_id
              AND b.budget_id=%s::uuid
              AND b.username=%s
            RETURNING b.budget_id
            """,
            (budget_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Budget not found")
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.put("/transactions/{transaction_id}")
def public_update_transaction(transaction_id: str, req: Request, payload: TransactionUpsertRequest):
    payload.transaction_id = parse_uuid_value(transaction_id, "transaction_id")
    return public_upsert_transaction(req, payload)


@router.delete("/transactions/{transaction_id}")
def public_delete_transaction(transaction_id: str, req: Request):
    username = require_public_user(req)
    transaction_id = parse_uuid_value(transaction_id, "transaction_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS account_id,
                   t.date,
                   t.transaction_type,
                   t.transaction_name,
                   t.amount,
                   t.is_transfer,
                   t.is_cycle_topup,
                   t.transfer_id::text AS transfer_id
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
            raise HTTPException(status_code=400, detail="Use switch endpoints to delete transfers")

        lock_accounts_for_update(cur, username, [tx["account_id"]])
        deleted_at = now_utc()
        cur.execute(
            """
            UPDATE transactions
            SET deleted_at=%s,
                deleted_by=%s,
                delete_reason=%s
            WHERE transaction_id=%s::uuid
              AND deleted_at IS NULL
            RETURNING transaction_id::text AS transaction_id,
                      account_id::text AS account_id,
                      transaction_type,
                      transaction_name,
                      amount,
                      date,
                      is_transfer,
                      is_cycle_topup,
                      transfer_id::text AS transfer_id,
                      deleted_at,
                      deleted_by,
                      delete_reason
            """,
            (deleted_at, username, "user_request", transaction_id),
        )
        deleted_row = cur.fetchone()
        if not deleted_row:
            raise HTTPException(status_code=409, detail="Transaction changed, please retry")
        write_transaction_audit(cur, username=username, performed_by=username, action="soft_delete", tx_row=deleted_row)
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.post("/switch")
def public_create_switch(req: Request, payload: dict[str, Any]):
    username = require_public_user(req)
    source_account_id = parse_uuid_value(payload.get("source_account_id"), "source_account_id")
    target_account_id = parse_uuid_value(payload.get("target_account_id"), "target_account_id")
    amount = parse_int_field(payload.get("amount"), "amount", default=0)
    date_str = payload.get("date")
    is_cycle_topup = parse_optional_bool(payload.get("is_cycle_topup"), "is_cycle_topup")
    if is_cycle_topup is None:
        is_cycle_topup = False
    if source_account_id == target_account_id:
        raise HTTPException(status_code=400, detail="Source and target must differ")
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid switch payload")
    dt = parse_tx_datetime(date_str)

    with db_conn() as conn, conn.cursor() as cur:
        lock_accounts_for_update(cur, username, [source_account_id, target_account_id])
        cur.execute(
            """
            SELECT account_id::text AS account_id, account_name
            FROM accounts
            WHERE username=%s AND account_id IN (%s::uuid, %s::uuid)
            """,
            (username, source_account_id, target_account_id),
        )
        accounts = cur.fetchall()
        if len(accounts) != 2:
            raise HTTPException(status_code=404, detail="Account not found")
        acc_map = {a["account_id"]: a for a in accounts}
        source = acc_map.get(source_account_id)
        target = acc_map.get(target_account_id)

        temp_id = str(uuid.uuid4())
        ensure_account_non_negative(cur, source_account_id, dt, [{"transaction_id": temp_id, "date": dt, "transaction_type": "credit", "amount": amount}])

        transfer_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO transactions (
                account_id, transaction_type, is_cycle_topup, transaction_name, amount, date, is_transfer, transfer_id
            )
            VALUES
              (%s::uuid, 'credit', false, %s, %s, %s, true, %s::uuid),
              (%s::uuid, 'debit', %s, %s, %s, %s, true, %s::uuid)
            """,
            (
                source_account_id,
                f"Switching to {target['account_name']}",
                amount,
                dt,
                transfer_id,
                target_account_id,
                is_cycle_topup,
                f"Switching from {source['account_name']}",
                amount,
                dt,
                transfer_id,
            ),
        )
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "transfer_id": transfer_id}


@router.get("/switch/{transfer_id}")
def public_get_switch(transfer_id: str, req: Request):
    username = require_public_user(req)
    transfer_id = parse_uuid_value(transfer_id, "transfer_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS account_id,
                   t.transaction_type,
                   t.amount,
                   t.date,
                   t.is_cycle_topup,
                   a.account_name
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE t.transfer_id=%s::uuid AND a.username=%s AND t.deleted_at IS NULL
            """,
            (transfer_id, username),
        )
        rows = cur.fetchall()
        if len(rows) != 2:
            raise HTTPException(status_code=404, detail="Switch not found")
        source = next((r for r in rows if r["transaction_type"] == "credit"), None)
        target = next((r for r in rows if r["transaction_type"] == "debit"), None)
    return {
        "transfer_id": transfer_id,
        "source_account_id": source["account_id"],
        "target_account_id": target["account_id"],
        "amount": int(source["amount"]),
        "date": source["date"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "is_cycle_topup": bool(target.get("is_cycle_topup")),
    }


@router.put("/switch/{transfer_id}")
def public_update_switch(transfer_id: str, req: Request, payload: dict[str, Any]):
    username = require_public_user(req)
    transfer_id = parse_uuid_value(transfer_id, "transfer_id")
    source_account_id = payload.get("source_account_id")
    target_account_id = payload.get("target_account_id")
    amount = payload.get("amount")
    date = payload.get("date")
    is_cycle_topup = payload.get("is_cycle_topup")

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS account_id,
                   t.transaction_type,
                   t.amount,
                   t.date,
                   t.is_cycle_topup,
                   a.account_name
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE t.transfer_id=%s::uuid AND a.username=%s AND t.deleted_at IS NULL
            """,
            (transfer_id, username),
        )
        rows = cur.fetchall()
        if len(rows) != 2:
            raise HTTPException(status_code=404, detail="Switch not found")
        source = next((r for r in rows if r["transaction_type"] == "credit"), None)
        target = next((r for r in rows if r["transaction_type"] == "debit"), None)

        source_account_id = parse_uuid_value(source_account_id or source["account_id"], "source_account_id")
        target_account_id = parse_uuid_value(target_account_id or target["account_id"], "target_account_id")
        if source_account_id == target_account_id:
            raise HTTPException(status_code=400, detail="Source and target must differ")
        amount = parse_int_field(amount if amount is not None else source["amount"], "amount")
        if amount <= 0:
            raise HTTPException(status_code=400, detail="Amount must be > 0")
        new_date = parse_tx_datetime(date) if date else source["date"]
        parsed_topup = parse_optional_bool(is_cycle_topup, "is_cycle_topup")
        if parsed_topup is None:
            parsed_topup = bool(target.get("is_cycle_topup"))

        lock_accounts_for_update(cur, username, [source["account_id"], target["account_id"], source_account_id, target_account_id])
        cur.execute(
            "SELECT account_id::text AS account_id, account_name FROM accounts WHERE username=%s AND account_id IN (%s::uuid,%s::uuid)",
            (username, source_account_id, target_account_id),
        )
        accounts = cur.fetchall()
        if len(accounts) != 2:
            raise HTTPException(status_code=404, detail="Account not found")
        acc_map = {a["account_id"]: a for a in accounts}

        old_rows = [
            {"transaction_id": source["transaction_id"], "account_id": source["account_id"], "date": source["date"]},
            {"transaction_id": target["transaction_id"], "account_id": target["account_id"], "date": target["date"]},
        ]
        new_rows = [
            {"transaction_id": source["transaction_id"], "account_id": source_account_id, "date": new_date, "transaction_type": "credit", "amount": amount},
            {"transaction_id": target["transaction_id"], "account_id": target_account_id, "date": new_date, "transaction_type": "debit", "amount": amount},
        ]
        affected: dict[str, dict[str, Any]] = {}
        for row in old_rows:
            acc = row["account_id"]
            affected.setdefault(acc, {"exclude": [], "dates": [], "new": []})
            affected[acc]["exclude"].append(row["transaction_id"])
            affected[acc]["dates"].append(row["date"])
        for row in new_rows:
            acc = row["account_id"]
            affected.setdefault(acc, {"exclude": [], "dates": [], "new": []})
            affected[acc]["new"].append(row)
            affected[acc]["dates"].append(row["date"])
        for acc_id, p in affected.items():
            ensure_account_non_negative(cur, acc_id, min(p["dates"]), p["new"], exclude_tx_ids=p["exclude"])

        cur.execute(
            """
            UPDATE transactions
            SET account_id=%s::uuid, transaction_type='credit', is_cycle_topup=false, transaction_name=%s, amount=%s, date=%s, is_transfer=true
            WHERE transaction_id=%s::uuid AND transfer_id=%s::uuid AND deleted_at IS NULL
            """,
            (source_account_id, f"Switching to {acc_map[target_account_id]['account_name']}", amount, new_date, source["transaction_id"], transfer_id),
        )
        cur.execute(
            """
            UPDATE transactions
            SET account_id=%s::uuid, transaction_type='debit', is_cycle_topup=%s, transaction_name=%s, amount=%s, date=%s, is_transfer=true
            WHERE transaction_id=%s::uuid AND transfer_id=%s::uuid AND deleted_at IS NULL
            """,
            (target_account_id, parsed_topup, f"Switching from {acc_map[source_account_id]['account_name']}", amount, new_date, target["transaction_id"], transfer_id),
        )
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.delete("/switch/{transfer_id}")
def public_delete_switch(transfer_id: str, req: Request):
    username = require_public_user(req)
    transfer_id = parse_uuid_value(transfer_id, "transfer_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS account_id,
                   t.date,
                   t.transaction_type,
                   t.transaction_name,
                   t.amount,
                   t.is_transfer,
                   t.is_cycle_topup,
                   t.transfer_id::text AS transfer_id
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE t.transfer_id=%s::uuid AND a.username=%s AND t.deleted_at IS NULL
            """,
            (transfer_id, username),
        )
        rows = cur.fetchall()
        if len(rows) != 2:
            raise HTTPException(status_code=404, detail="Switch not found")
        lock_accounts_for_update(cur, username, [row["account_id"] for row in rows])
        deleted_at = now_utc()
        cur.execute(
            """
            UPDATE transactions
            SET deleted_at=%s, deleted_by=%s, delete_reason=%s
            WHERE transfer_id=%s::uuid AND deleted_at IS NULL
            RETURNING transaction_id::text AS transaction_id,
                      account_id::text AS account_id,
                      transaction_type,
                      transaction_name,
                      amount,
                      date,
                      is_transfer,
                      is_cycle_topup,
                      transfer_id::text AS transfer_id,
                      deleted_at,
                      deleted_by,
                      delete_reason
            """,
            (deleted_at, username, "user_request", transfer_id),
        )
        deleted_rows = cur.fetchall()
        if len(deleted_rows) != 2:
            raise HTTPException(status_code=409, detail="Switch changed, please retry")
        for row in deleted_rows:
            write_transaction_audit(cur, username=username, performed_by=username, action="soft_delete", tx_row=row)
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.get("/payday")
def public_get_payday(req: Request, month: str | None = None):
    username = require_public_user(req)
    if not month:
        month = now_utc().strftime("%Y-%m")
    parse_month(month)
    with db_conn() as conn, conn.cursor() as cur:
        payday_day, payday_source, override_day = get_payday_day(cur, username, month)
        default_day = get_default_payday_day(cur, username)
    return {"month": month, "day": payday_day, "source": payday_source, "default_day": default_day, "override_day": override_day}


@router.put("/payday")
def public_set_payday(req: Request, payload: dict[str, Any]):
    username = require_public_user(req)
    month = payload.get("month")
    day_val = payload.get("day")
    clear_override_value = parse_optional_bool(payload.get("clear_override"), "clear_override")
    clear_override = bool(clear_override_value) if clear_override_value is not None else False

    if month:
        parse_month(month)
        if clear_override:
            with db_conn() as conn, conn.cursor() as cur:
                cur.execute("DELETE FROM payday_overrides WHERE username=%s AND month=%s", (username, month))
                conn.commit()
        else:
            day = parse_int_field(day_val, "day")
            if day < 1 or day > 31:
                raise HTTPException(status_code=400, detail="Payday day must be between 1 and 31")
            with db_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO payday_overrides (username, month, payday_day)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (username, month)
                    DO UPDATE SET payday_day=EXCLUDED.payday_day
                    """,
                    (username, month, day),
                )
                conn.commit()
    else:
        day = parse_int_field(day_val, "day")
        if day < 1 or day > 31:
            raise HTTPException(status_code=400, detail="Payday day must be between 1 and 31")
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute("UPDATE users SET default_payday_day=%s WHERE username=%s", (day, username))
            conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.post("/balances/recompute")
def public_recompute_balances(req: Request, _payload: EmptyBodyRequest):
    username = require_public_user(req)
    with db_conn() as conn, conn.cursor() as cur:
        report = recompute_balances_report(cur, username)
    invalidate_user_cache(username)
    return {"ok": True, "checked_at": now_utc().isoformat().replace("+00:00", "Z"), **report}


@router.post("/transactions/audit")
def public_transaction_audit(req: Request, payload: dict[str, Any]):
    username = require_public_user(req)
    transaction_id = payload.get("transaction_id")
    limit = parse_int_field(payload.get("limit"), "limit", default=50)
    limit = max(1, min(limit, 200))
    if transaction_id:
        transaction_id = parse_uuid_value(transaction_id, "transaction_id")

    sql = """
        SELECT audit_id::text AS audit_id,
               transaction_id::text AS transaction_id,
               account_id::text AS account_id,
               username,
               action,
               payload,
               performed_by,
               performed_at
        FROM transaction_audit
        WHERE username=%s
    """
    params: list[Any] = [username]
    if transaction_id:
        sql += " AND transaction_id=%s::uuid"
        params.append(transaction_id)
    sql += " ORDER BY performed_at DESC LIMIT %s"
    params.append(limit)

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return {
        "audit": [
            {
                "audit_id": row["audit_id"],
                "transaction_id": row["transaction_id"],
                "account_id": row["account_id"],
                "username": row["username"],
                "action": row["action"],
                "payload": row["payload"],
                "performed_by": row["performed_by"],
                "performed_at": row["performed_at"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
            for row in rows
        ]
    }


@router.post("/export/preview")
def public_export_preview(req: Request, payload: dict[str, Any]):
    username = require_public_user(req)
    day = parse_int_field(payload.get("day"), "day")
    scope = str(payload.get("scope") or "all")
    account_id = payload.get("account_id")
    from_date, to_date, from_dt, to_dt = compute_export_range(day)

    with db_conn() as conn, conn.cursor() as cur:
        rows, _, _ = build_ledger_data(cur, username, scope, account_id, from_dt, to_dt)

    total_in = sum(int(r.get("debit") or 0) for r in rows)
    total_out = sum(int(r.get("credit") or 0) for r in rows)
    return {"range": {"from": from_date, "to": to_date}, "summary": {"count": len(rows), "total_in": int(total_in), "total_out": int(total_out), "net": int(total_in - total_out)}}


@router.post("/export")
def public_export(req: Request, payload: dict[str, Any]):
    username = require_public_user(req)
    day = parse_int_field(payload.get("day"), "day")
    export_format = str(payload.get("format") or "pdf").lower()
    scope = str(payload.get("scope") or "all")
    account_id = payload.get("account_id")
    currency = payload.get("currency")
    fx_rate = payload.get("fx_rate")

    if export_format not in ("pdf", "csv"):
        raise HTTPException(status_code=400, detail="Invalid export format")

    cur_currency, fx = parse_currency(currency, fx_rate)
    from_date, to_date, from_dt, to_dt = compute_export_range(day)

    with db_conn() as conn, conn.cursor() as cur:
        rows, summary_accounts, _ = build_ledger_data(cur, username, scope, account_id, from_dt, to_dt)

    export_payload = export_ledger_file(
        rows=rows,
        summary_accounts=summary_accounts,
        scope=scope,
        account_id=account_id,
        username=username,
        from_date=from_date,
        to_date=to_date,
        export_format=export_format,
        currency=cur_currency,
        fx=fx,
    )
    return Response(
        content=export_payload["content"],
        media_type=export_payload["media_type"],
        headers={"Content-Disposition": f'attachment; filename="{export_payload["filename"]}"'},
    )
