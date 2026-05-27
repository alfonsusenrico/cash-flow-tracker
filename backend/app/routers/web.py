import uuid
from datetime import timedelta, timezone
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response
from passlib.hash import bcrypt
from psycopg.errors import UniqueViolation

from app.core.config import settings
from app.db.pool import db_conn
from app.services.auth import (
    clear_login_rate_limit,
    create_api_key,
    enforce_login_rate_limit,
    enforce_register_rate_limit,
    get_active_api_key,
    register_user,
    require_session_user,
)
from app.services.categories import ensure_switching_category
from app.services.ledger import (
    build_daily_series,
    build_ledger_data,
    build_ledger_page,
    build_weekly_series,
    cache_get,
    cache_set,
    compute_shortfall_at_transaction,
    compute_financial_safety_report,
    compute_budget_shift_analysis,
    compute_budget_status,
    compute_export_range,
    compute_dynamic_month_range,
    current_month_local,
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

router = APIRouter()


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
        if isinstance(value, str):
            value = value.strip().replace(".", "").replace(",", "")
        return int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


def parse_uuid_field(value: Any, field_name: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail=f"{field_name} required")
    try:
        return str(uuid.UUID(raw))
    except (TypeError, ValueError, AttributeError):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


def parse_profile_type(value: Any) -> str:
    raw = str(value or "dynamic_spending").strip().lower()
    if raw not in ("tabungan", "fixed_spending", "dynamic_spending"):
        raise HTTPException(status_code=400, detail="Invalid profile_type")
    return raw


@router.get("/health")
def health():
    return {"ok": True}


@router.post("/auth/register")
async def register(req: Request):
    data = await req.json()
    enforce_register_rate_limit(req)

    with db_conn() as conn, conn.cursor() as cur:
        try:
            register_user(cur, data)
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="User already exists")

    return {"ok": True}


@router.post("/auth/login")
async def login(req: Request):
    data = await req.json()
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    if len(password.encode("utf-8")) > 72:
        raise HTTPException(status_code=400, detail="Password too long (max 72 bytes)")
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password required")
    rate_limited = False
    try:
        enforce_login_rate_limit(req, username)
    except HTTPException as exc:
        if exc.status_code != 429:
            raise
        rate_limited = True

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT username, password_hash, full_name FROM users WHERE username=%s", (username,))
        user = cur.fetchone()

    if not user or not bcrypt.verify(password, user["password_hash"]):
        if rate_limited:
            raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")
        raise HTTPException(status_code=401, detail="Invalid credentials")

    clear_login_rate_limit(req, username)
    req.session["username"] = user["username"]
    req.session["full_name"] = user["full_name"]
    return {"ok": True, "username": user["username"], "full_name": user["full_name"]}


@router.post("/auth/logout")
def logout(req: Request):
    req.session.clear()
    return {"ok": True}


@router.get("/me")
def me(req: Request):
    username = require_session_user(req)
    return {"username": username, "full_name": req.session.get("full_name", username), "tz": settings.tz}


@router.get("/api-key")
def get_api_key(req: Request):
    username = require_session_user(req)
    with db_conn() as conn, conn.cursor() as cur:
        key_meta = get_active_api_key(cur, username)
    if not key_meta:
        raise HTTPException(status_code=404, detail="API key not found")
    return {"api_key": key_meta}


@router.post("/api-key/reset")
def reset_api_key(req: Request):
    username = require_session_user(req)
    with db_conn() as conn, conn.cursor() as cur:
        new_key = create_api_key(cur, username, "reset")
        key_meta = get_active_api_key(cur, username)
        conn.commit()
    if not key_meta:
        raise HTTPException(status_code=500, detail="Failed to generate API key")
    return {"ok": True, "api_key": new_key, "masked": key_meta["key_masked"]}


@router.get("/accounts")
def list_accounts(req: Request):
    username = require_session_user(req)
    with db_conn() as conn, conn.cursor() as cur:
        balances = get_account_balances(cur, username, now_utc())
        cur.execute(
            """
            SELECT a.account_id::text,
                   a.account_name,
                   a.profile_type,
                   a.is_payroll_source,
                   a.is_no_limit,
                   a.is_buffer,
                   a.fixed_limit_amount,
                   a.institution,
                   a.account_number
            FROM accounts a
            WHERE a.username=%s
            ORDER BY a.account_name
            """,
            (username,),
        )
        accounts = cur.fetchall()
        for account in accounts:
            account["balance"] = int(balances.get(account["account_id"], 0))
        return {"accounts": accounts}


@router.post("/accounts")
async def create_account(req: Request):
    username = require_session_user(req)
    data = await req.json()
    account_name = (data.get("account_name") or "").strip()
    initial_balance_raw = data.get("initial_balance", 0)
    initial_balance = parse_int_field(initial_balance_raw, "initial_balance", default=0)
    if not account_name:
        raise HTTPException(status_code=400, detail="account_name required")
    if initial_balance < 0:
        raise HTTPException(status_code=400, detail="initial balance must be >= 0")

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO accounts (user_id, username, account_name)
                SELECT user_id, username, %s
                FROM users
                WHERE username=%s
                RETURNING account_id::text
                """,
                (account_name, username),
            )
            account_id = cur.fetchone()["account_id"]
            if initial_balance > 0:
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
                    VALUES (%s::uuid, 'debit', false, %s, %s, %s, false)
                    """,
                    (account_id, "Opening Balance", initial_balance, now_utc()),
                )
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Account name already exists")

    invalidate_user_cache(username)
    return {"ok": True, "account_id": account_id}


@router.get("/budgets")
def list_budgets(req: Request, month: str | None = None):
    username = require_session_user(req)
    if not month:
        month = current_month_local()
    parse_month(month)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.budget_id::text AS budget_id,
                   b.account_id::text AS account_id,
                   b.month,
                   b.amount,
                   b.source,
                   b.allocation_plan_id::text AS allocation_plan_id,
                   b.allocation_run_id::text AS allocation_run_id
            FROM budgets b
            JOIN accounts a ON a.account_id=b.account_id
            WHERE b.username=%s AND b.month=%s
            ORDER BY a.account_name
            """,
            (username, month),
        )
        return {"month": month, "budgets": cur.fetchall()}


@router.post("/budgets")
async def upsert_budget(req: Request):
    username = require_session_user(req)
    data = await req.json()
    account_id = parse_uuid_field(data.get("account_id"), "account_id")
    month = data.get("month")
    amount = parse_int_field(data.get("amount"), "amount", default=0)
    if not month:
        raise HTTPException(status_code=400, detail="account_id and month required")
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
            INSERT INTO budgets (user_id, username, account_id, month, amount)
            SELECT user_id, username, %s::uuid, %s, %s
            FROM users
            WHERE username=%s
            ON CONFLICT (username, account_id, month)
            DO UPDATE SET amount=EXCLUDED.amount,
                          source='manual',
                          allocation_plan_id=NULL,
                          allocation_run_id=NULL
            RETURNING budget_id::text
            """,
            (account_id, month, amount, username),
        )
        budget_id = cur.fetchone()["budget_id"]
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "budget_id": budget_id}


@router.put("/budgets/{budget_id}")
async def update_budget(budget_id: str, req: Request):
    username = require_session_user(req)
    budget_id = parse_uuid_field(budget_id, "budget_id")
    data = await req.json()
    amount = parse_int_field(data.get("amount"), "amount", default=0)
    if amount < 0:
        raise HTTPException(status_code=400, detail="amount must be >= 0")

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE budgets b
            SET amount=%s,
                source='manual',
                allocation_plan_id=NULL,
                allocation_run_id=NULL
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
def delete_budget(budget_id: str, req: Request):
    username = require_session_user(req)
    budget_id = parse_uuid_field(budget_id, "budget_id")
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


@router.put("/accounts/{account_id}")
async def update_account(account_id: str, req: Request):
    username = require_session_user(req)
    account_id = parse_uuid_field(account_id, "account_id")
    data = await req.json()
    account_name = (data.get("account_name") or "").strip()
    if not account_name:
        raise HTTPException(status_code=400, detail="account_name required")

    with db_conn() as conn, conn.cursor() as cur:
        # Get account info + user password hash
        cur.execute(
            """
            SELECT a.account_id::text AS account_id
            FROM accounts a
            WHERE a.username=%s AND a.account_id=%s::uuid
            """,
            (username, account_id),
        )
        acc = cur.fetchone()
        if not acc:
            raise HTTPException(status_code=404, detail="Account not found")

        updates = []
        params: list[Any] = []
        updates.append("account_name=%s")
        params.append(account_name)

        params.extend([username, account_id])
        try:
            cur.execute(
                f"""
                UPDATE accounts
                SET {", ".join(updates)}
                WHERE username=%s AND account_id=%s::uuid
                RETURNING account_id::text, account_name, profile_type, is_payroll_source, is_no_limit, is_buffer, fixed_limit_amount, institution, account_number
                """,
                params,
            )
            row = cur.fetchone()
            if row:
                row["balance"] = int(get_account_balances(cur, username, now_utc()).get(account_id, 0))
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Account name already exists")

    invalidate_user_cache(username)
    return {"ok": True, "account": row}


@router.put("/accounts/{account_id}/profile")
async def update_account_profile(account_id: str, req: Request):
    username = require_session_user(req)
    account_id = parse_uuid_field(account_id, "account_id")
    data = await req.json()

    account_name = None
    if "account_name" in data:
        account_name = (data.get("account_name") or "").strip()
        if not account_name:
            raise HTTPException(status_code=400, detail="account_name required")
    profile_type = parse_profile_type(data.get("profile_type"))
    is_payroll_source = bool(parse_optional_bool(data.get("is_payroll_source"), "is_payroll_source") or False)
    is_no_limit = bool(parse_optional_bool(data.get("is_no_limit"), "is_no_limit") or False)
    is_buffer = bool(parse_optional_bool(data.get("is_buffer"), "is_buffer") or False)

    fixed_limit_raw = data.get("fixed_limit_amount")
    fixed_limit_amount = None
    if fixed_limit_raw is not None and str(fixed_limit_raw).strip() != "":
        fixed_limit_amount = parse_int_field(fixed_limit_raw, "fixed_limit_amount", default=0)
        if fixed_limit_amount < 0:
            raise HTTPException(status_code=400, detail="fixed_limit_amount must be >= 0")

    institution = (data.get("institution") or "").strip() or None
    account_number = (data.get("account_number") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                UPDATE accounts
                SET account_name=COALESCE(%s, account_name),
                    profile_type=%s,
                    is_payroll_source=%s,
                    is_no_limit=%s,
                    is_buffer=%s,
                    fixed_limit_amount=%s,
                    institution=%s,
                    account_number=%s
                WHERE username=%s AND account_id=%s::uuid
                RETURNING account_id::text, account_name, profile_type, is_payroll_source, is_no_limit, is_buffer, fixed_limit_amount, institution, account_number
                """,
                (account_name, profile_type, is_payroll_source, is_no_limit, is_buffer, fixed_limit_amount, institution, account_number, username, account_id),
            )
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Account name already exists")
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Account not found")
        row["balance"] = int(get_account_balances(cur, username, now_utc()).get(account_id, 0))
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "account": row}


@router.delete("/accounts/{account_id}")
def delete_account(account_id: str, req: Request):
    username = require_session_user(req)
    account_id = parse_uuid_field(account_id, "account_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT account_id::text AS account_id FROM accounts WHERE username=%s AND account_id=%s::uuid",
            (username, account_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Account not found")
        cur.execute("DELETE FROM bucket_accounts WHERE account_id=%s::uuid", (account_id,))
        cur.execute(
            """
            UPDATE buckets
            SET linked_account_id=NULL
            WHERE linked_account_id=%s::uuid
              AND user_id=(SELECT user_id FROM users WHERE username=%s)
            """,
            (account_id, username),
        )
        cur.execute(
            "DELETE FROM accounts WHERE username=%s AND account_id=%s::uuid",
            (username, account_id),
        )
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.post("/transactions")
async def create_tx(req: Request):
    username = require_session_user(req)
    data = await req.json()

    account_id = data.get("account_id")
    tx_type = data.get("transaction_type")
    name = (data.get("transaction_name") or "").strip()
    amount = parse_int_field(data.get("amount"), "amount", default=0)
    date_str = data.get("date")  # ISO string (from input datetime-local) or YYYY-MM-DD
    is_cycle_topup = parse_optional_bool(data.get("is_cycle_topup"), "is_cycle_topup")
    if is_cycle_topup is None:
        is_cycle_topup = False
    category_id = (data.get("category_id") or "").strip() or None
    notes = (data.get("notes") or "").strip() or None
    currency = (data.get("currency") or "IDR").upper()
    if currency not in ("IDR", "USD"):
        currency = "IDR"
    original_amount_raw = data.get("original_amount")
    original_amount = int(original_amount_raw) if original_amount_raw is not None else None
    fx_rate_raw = data.get("fx_rate")
    fx_rate = float(fx_rate_raw) if fx_rate_raw is not None else None
    tags = [str(t).strip() for t in (data.get("tags") or []) if str(t).strip()]
    is_reviewed = bool(data.get("is_reviewed", False))

    if not account_id or tx_type not in ("debit", "credit") or not name or amount <= 0 or not date_str:
        raise HTTPException(status_code=400, detail="Invalid transaction payload")
    if is_cycle_topup and tx_type != "debit":
        raise HTTPException(status_code=400, detail="Top-up/payroll can only be set on cash-in transactions")

    dt = parse_tx_datetime(date_str)

    with db_conn() as conn, conn.cursor() as cur:
        lock_accounts_for_update(cur, username, [account_id])

        cur.execute(
            """
            INSERT INTO transactions (
                account_id,
                transaction_type,
                is_cycle_topup,
                transaction_name,
                amount,
                date,
                is_transfer,
                category_id,
                notes,
                currency,
                original_amount,
                fx_rate,
                tags,
                is_reviewed
            )
            VALUES (%s::uuid, %s, %s, %s, %s, %s, false, %s::uuid, %s, %s, %s, %s, %s, %s)
            RETURNING transaction_id::text
            """,
            (account_id, tx_type, is_cycle_topup, name, amount, dt,
             category_id, notes, currency, original_amount, fx_rate, tags, is_reviewed),
        )
        tx_id = cur.fetchone()["transaction_id"]
        shortfall = compute_shortfall_at_transaction(cur, account_id, dt, tx_id)
        conn.commit()

    invalidate_user_cache(username)
    return {
        "ok": True,
        "transaction_id": tx_id,
        "needs_loan": shortfall > 0,
        "shortfall": int(shortfall),
        "account_id": account_id,
    }


@router.post("/switch")
async def switch_balance(req: Request):
    username = require_session_user(req)
    data = await req.json()
    source_account_id = data.get("source_account_id")
    target_account_id = data.get("target_account_id")
    amount = parse_int_field(data.get("amount"), "amount", default=0)
    date_str = data.get("date")
    is_cycle_topup = parse_optional_bool(data.get("is_cycle_topup"), "is_cycle_topup")
    if is_cycle_topup is None:
        is_cycle_topup = False

    if not source_account_id or not target_account_id or amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid switch payload")
    if source_account_id == target_account_id:
        raise HTTPException(status_code=400, detail="Source and target must differ")

    dt = parse_tx_datetime(date_str)

    with db_conn() as conn, conn.cursor() as cur:
        lock_accounts_for_update(cur, username, [source_account_id, target_account_id])
        cur.execute(
            """
            SELECT account_id::text AS account_id,
                   account_name
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
        if not source or not target:
            raise HTTPException(status_code=404, detail="Account not found")

        temp_id = str(uuid.uuid4())
        ensure_account_non_negative(
            cur,
            source_account_id,
            dt,
            [
                {
                    "transaction_id": temp_id,
                    "date": dt,
                    "transaction_type": "credit",
                    "amount": amount,
                }
            ],
        )

        source_name = f"Switching to {target['account_name']}"
        target_name = f"Switching from {source['account_name']}"
        category_id = ensure_switching_category(cur, username)
        transfer_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO transactions (
                account_id,
                transaction_type,
                is_cycle_topup,
                transaction_name,
                amount,
                date,
                is_transfer,
                transfer_id,
                category_id
            )
            VALUES
              (%s::uuid, 'credit', false, %s, %s, %s, true, %s::uuid, %s::uuid),
              (%s::uuid, 'debit', %s, %s, %s, %s, true, %s::uuid, %s::uuid)
            RETURNING transaction_id::text
            """,
            (
                source_account_id,
                source_name,
                amount,
                dt,
                transfer_id,
                category_id,
                target_account_id,
                is_cycle_topup,
                target_name,
                amount,
                dt,
                transfer_id,
                category_id,
            ),
        )
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "transfer_id": transfer_id}


@router.get("/switch/{transfer_id}")
def get_switch(transfer_id: str, req: Request):
    username = require_session_user(req)
    transfer_id = parse_uuid_field(transfer_id, "transfer_id")
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
        if not source or not target:
            raise HTTPException(status_code=400, detail="Invalid switch data")
    return {
        "transfer_id": transfer_id,
        "source_account_id": source["account_id"],
        "target_account_id": target["account_id"],
        "amount": int(source["amount"]),
        "date": source["date"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "is_cycle_topup": bool(target.get("is_cycle_topup")),
    }


@router.put("/switch/{transfer_id}")
async def update_switch(transfer_id: str, req: Request):
    username = require_session_user(req)
    transfer_id = parse_uuid_field(transfer_id, "transfer_id")
    data = await req.json()
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
        if not source or not target:
            raise HTTPException(status_code=400, detail="Invalid switch data")

        source_account_id = data.get("source_account_id") or source["account_id"]
        target_account_id = data.get("target_account_id") or target["account_id"]
        if source_account_id == target_account_id:
            raise HTTPException(status_code=400, detail="Source and target must differ")

        if "amount" in data and data.get("amount") is not None:
            amount = parse_int_field(data.get("amount"), "amount")
        else:
            amount = int(source["amount"])
        if amount <= 0:
            raise HTTPException(status_code=400, detail="Amount must be > 0")

        if "date" in data and data.get("date"):
            new_date = parse_tx_datetime(data.get("date"))
        else:
            new_date = source["date"]
        is_cycle_topup = parse_optional_bool(data.get("is_cycle_topup"), "is_cycle_topup")
        if is_cycle_topup is None:
            is_cycle_topup = bool(target.get("is_cycle_topup"))

        lock_accounts_for_update(
            cur,
            username,
            [source["account_id"], target["account_id"], source_account_id, target_account_id],
        )

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
        source_label = acc_map[source_account_id]["account_name"]
        target_label = acc_map[target_account_id]["account_name"]
        source_name = f"Switching to {target_label}"
        target_name = f"Switching from {source_label}"
        category_id = ensure_switching_category(cur, username)

        old_rows = [
            {
                "transaction_id": source["transaction_id"],
                "account_id": source["account_id"],
                "date": source["date"],
            },
            {
                "transaction_id": target["transaction_id"],
                "account_id": target["account_id"],
                "date": target["date"],
            },
        ]
        new_rows = [
            {
                "transaction_id": source["transaction_id"],
                "account_id": source_account_id,
                "date": new_date,
                "transaction_type": "credit",
                "amount": amount,
            },
            {
                "transaction_id": target["transaction_id"],
                "account_id": target_account_id,
                "date": new_date,
                "transaction_type": "debit",
                "amount": amount,
            },
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

        for acc_id, payload in affected.items():
            effective_from = min(payload["dates"])
            ensure_account_non_negative(
                cur,
                acc_id,
                effective_from,
                payload["new"],
                exclude_tx_ids=payload["exclude"],
            )

        cur.execute(
            """
            UPDATE transactions
            SET account_id=%s::uuid,
                transaction_type='credit',
                is_cycle_topup=false,
                transaction_name=%s,
                amount=%s,
                date=%s,
                is_transfer=true,
                category_id=%s::uuid
            WHERE transaction_id=%s::uuid AND transfer_id=%s::uuid AND deleted_at IS NULL
            """,
            (
                source_account_id,
                source_name,
                amount,
                new_date,
                category_id,
                source["transaction_id"],
                transfer_id,
            ),
        )
        if cur.rowcount != 1:
            raise HTTPException(status_code=409, detail="Switch changed, please retry")
        cur.execute(
            """
            UPDATE transactions
            SET account_id=%s::uuid,
                transaction_type='debit',
                is_cycle_topup=%s,
                transaction_name=%s,
                amount=%s,
                date=%s,
                is_transfer=true,
                category_id=%s::uuid
            WHERE transaction_id=%s::uuid AND transfer_id=%s::uuid AND deleted_at IS NULL
            """,
            (
                target_account_id,
                is_cycle_topup,
                target_name,
                amount,
                new_date,
                category_id,
                target["transaction_id"],
                transfer_id,
            ),
        )
        if cur.rowcount != 1:
            raise HTTPException(status_code=409, detail="Switch changed, please retry")
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.delete("/switch/{transfer_id}")
def delete_switch(transfer_id: str, req: Request):
    username = require_session_user(req)
    transfer_id = parse_uuid_field(transfer_id, "transfer_id")
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
            SET deleted_at=%s,
                deleted_by=%s,
                delete_reason=%s
            WHERE transfer_id=%s::uuid
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
            (deleted_at, username, "user_request", transfer_id),
        )
        deleted_rows = cur.fetchall()
        if len(deleted_rows) != 2:
            raise HTTPException(status_code=409, detail="Switch changed, please retry")
        for row in deleted_rows:
            write_transaction_audit(
                cur,
                username=username,
                performed_by=username,
                action="soft_delete",
                tx_row=row,
            )
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.get("/loans")
def list_internal_loans(req: Request, status: str | None = None):
    username = require_session_user(req)
    statuses = ["open", "finalized"]
    raw_status = (status or "").strip().lower()
    if raw_status and raw_status != "all":
        statuses = [s.strip() for s in raw_status.split(",") if s.strip()]
        if not statuses:
            statuses = ["open", "finalized"]
        invalid = [s for s in statuses if s not in ("open", "finalized")]
        if invalid:
            raise HTTPException(status_code=400, detail="Invalid status filter")
        statuses = sorted(set(statuses))

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT l.loan_id::text AS loan_id,
                   l.username,
                   l.trigger_transaction_id::text AS trigger_transaction_id,
                   l.disbursement_transfer_id::text AS disbursement_transfer_id,
                   l.lender_account_id::text AS lender_account_id,
                   lender.account_name AS lender_account_name,
                   l.borrower_account_id::text AS borrower_account_id,
                   borrower.account_name AS borrower_account_name,
                   l.principal_amount,
                   l.status,
                   l.finalized_transfer_id::text AS finalized_transfer_id,
                   l.created_at,
                   l.finalized_at,
                   t.transaction_name AS trigger_transaction_name,
                   t.date AS trigger_transaction_date
            FROM internal_loans l
            JOIN accounts lender
              ON lender.account_id=l.lender_account_id
             AND lender.username=l.username
            JOIN accounts borrower
              ON borrower.account_id=l.borrower_account_id
             AND borrower.username=l.username
            JOIN transactions t ON t.transaction_id=l.trigger_transaction_id
            JOIN accounts trigger_owner
              ON trigger_owner.account_id=t.account_id
             AND trigger_owner.username=l.username
            WHERE l.username=%s
              AND l.status = ANY(%s::text[])
            ORDER BY l.created_at DESC, l.loan_id DESC
            """,
            (username, statuses),
        )
        rows = cur.fetchall()

    return {
        "loans": [
            {
                "loan_id": row["loan_id"],
                "username": row["username"],
                "trigger_transaction_id": row["trigger_transaction_id"],
                "trigger_transaction_name": row["trigger_transaction_name"],
                "trigger_transaction_date": row["trigger_transaction_date"]
                .astimezone(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "disbursement_transfer_id": row["disbursement_transfer_id"],
                "lender_account_id": row["lender_account_id"],
                "lender_account_name": row["lender_account_name"],
                "borrower_account_id": row["borrower_account_id"],
                "borrower_account_name": row["borrower_account_name"],
                "principal_amount": int(row.get("principal_amount") or 0),
                "status": row["status"],
                "finalized_transfer_id": row["finalized_transfer_id"],
                "created_at": row["created_at"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "finalized_at": row["finalized_at"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                if row.get("finalized_at")
                else None,
            }
            for row in rows
        ]
    }


@router.post("/loans/from-transaction")
async def create_internal_loan_from_transaction(req: Request):
    username = require_session_user(req)
    data = await req.json()
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Invalid payload")
    trigger_transaction_id = parse_uuid_field(data.get("transaction_id"), "transaction_id")
    lender_account_id = parse_uuid_field(data.get("lender_account_id"), "lender_account_id")

    amount_raw = data.get("amount")
    requested_amount: int | None = None
    if amount_raw is not None and not (isinstance(amount_raw, str) and not amount_raw.strip()):
        requested_amount = parse_int_field(amount_raw, "amount")
        if requested_amount <= 0:
            raise HTTPException(status_code=400, detail="amount must be > 0")

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS borrower_account_id,
                   t.transaction_name,
                   t.date,
                   t.is_transfer
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE t.transaction_id=%s::uuid
              AND a.username=%s
              AND t.deleted_at IS NULL
            """,
            (trigger_transaction_id, username),
        )
        trigger_tx = cur.fetchone()
        if not trigger_tx:
            raise HTTPException(status_code=404, detail="Trigger transaction not found")
        if trigger_tx.get("is_transfer"):
            raise HTTPException(status_code=400, detail="Trigger transaction must be non-transfer")

        borrower_account_id = trigger_tx["borrower_account_id"]
        if lender_account_id == borrower_account_id:
            raise HTTPException(status_code=400, detail="Lender account must differ from borrower account")

        lock_accounts_for_update(cur, username, [lender_account_id, borrower_account_id])

        cur.execute(
            """
            SELECT loan_id::text AS loan_id
            FROM internal_loans
            WHERE username=%s
              AND trigger_transaction_id=%s::uuid
              AND status='open'
            LIMIT 1
            """,
            (username, trigger_transaction_id),
        )
        existing_open = cur.fetchone()
        if existing_open:
            raise HTTPException(status_code=400, detail="Open loan already exists for this transaction")

        cur.execute(
            """
            SELECT disbursement_transfer_id::text AS transfer_id
            FROM internal_loans
            WHERE username=%s
              AND trigger_transaction_id=%s::uuid
            """,
            (username, trigger_transaction_id),
        )
        excluded_transfer_ids = [row["transfer_id"] for row in cur.fetchall() if row.get("transfer_id")]

        shortfall = compute_shortfall_at_transaction(
            cur,
            borrower_account_id,
            trigger_tx["date"],
            trigger_transaction_id,
            exclude_transfer_ids=excluded_transfer_ids,
        )
        if shortfall <= 0:
            raise HTTPException(status_code=400, detail="Trigger transaction has no shortfall")

        amount = requested_amount if requested_amount is not None else shortfall
        if amount <= 0:
            raise HTTPException(status_code=400, detail="amount must be > 0")
        if amount > shortfall:
            raise HTTPException(status_code=400, detail="amount exceeds transaction shortfall")

        temp_id = str(uuid.uuid4())
        ensure_account_non_negative(
            cur,
            lender_account_id,
            trigger_tx["date"],
            [
                {
                    "transaction_id": temp_id,
                    "date": trigger_tx["date"],
                    "transaction_type": "credit",
                    "amount": amount,
                }
            ],
        )

        cur.execute(
            """
            SELECT account_id::text AS account_id,
                   account_name
            FROM accounts
            WHERE username=%s
              AND account_id IN (%s::uuid, %s::uuid)
            """,
            (username, lender_account_id, borrower_account_id),
        )
        account_rows = cur.fetchall()
        if len(account_rows) != 2:
            raise HTTPException(status_code=404, detail="Account not found")
        account_by_id = {row["account_id"]: row for row in account_rows}
        lender_name = account_by_id[lender_account_id]["account_name"]
        borrower_name = account_by_id[borrower_account_id]["account_name"]

        transfer_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO transactions (
                account_id,
                transaction_type,
                is_cycle_topup,
                transaction_name,
                amount,
                date,
                is_transfer,
                transfer_id
            )
            VALUES
              (%s::uuid, 'credit', false, %s, %s, %s, true, %s::uuid),
              (%s::uuid, 'debit', false, %s, %s, %s, true, %s::uuid)
            """,
            (
                lender_account_id,
                f"Loan to {borrower_name}",
                amount,
                trigger_tx["date"],
                transfer_id,
                borrower_account_id,
                f"Loan from {lender_name}",
                amount,
                trigger_tx["date"],
                transfer_id,
            ),
        )

        cur.execute(
            """
            INSERT INTO internal_loans (
                user_id,
                username,
                trigger_transaction_id,
                disbursement_transfer_id,
                lender_account_id,
                borrower_account_id,
                principal_amount
            )
            SELECT user_id, username, %s::uuid, %s::uuid, %s::uuid, %s::uuid, %s
            FROM users
            WHERE username=%s
            RETURNING loan_id::text AS loan_id
            """,
            (
                trigger_transaction_id,
                transfer_id,
                lender_account_id,
                borrower_account_id,
                amount,
                username,
            ),
        )
        loan_id = cur.fetchone()["loan_id"]
        conn.commit()

    invalidate_user_cache(username)
    return {
        "ok": True,
        "loan_id": loan_id,
        "disbursement_transfer_id": transfer_id,
        "principal_amount": int(amount),
    }


@router.post("/loans/{loan_id}/finalize")
async def finalize_internal_loan(loan_id: str, req: Request):
    username = require_session_user(req)
    loan_id = parse_uuid_field(loan_id, "loan_id")
    try:
        data = await req.json()
    except Exception:
        data = {}
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Invalid payload")

    final_date = parse_tx_datetime(data.get("date"))

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT l.loan_id::text AS loan_id,
                   l.status,
                   l.principal_amount,
                   l.lender_account_id::text AS lender_account_id,
                   lender.account_name AS lender_account_name,
                   l.borrower_account_id::text AS borrower_account_id,
                   borrower.account_name AS borrower_account_name
            FROM internal_loans l
            JOIN accounts lender
              ON lender.account_id=l.lender_account_id
             AND lender.username=l.username
            JOIN accounts borrower
              ON borrower.account_id=l.borrower_account_id
             AND borrower.username=l.username
            WHERE l.loan_id=%s::uuid
              AND l.username=%s
            FOR UPDATE
            """,
            (loan_id, username),
        )
        loan_row = cur.fetchone()
        if not loan_row:
            raise HTTPException(status_code=404, detail="Loan not found")
        if loan_row.get("status") != "open":
            raise HTTPException(status_code=400, detail="Loan already finalized")

        lender_account_id = loan_row["lender_account_id"]
        borrower_account_id = loan_row["borrower_account_id"]
        principal = int(loan_row.get("principal_amount") or 0)

        lock_accounts_for_update(cur, username, [lender_account_id, borrower_account_id])

        temp_id = str(uuid.uuid4())
        ensure_account_non_negative(
            cur,
            borrower_account_id,
            final_date,
            [
                {
                    "transaction_id": temp_id,
                    "date": final_date,
                    "transaction_type": "credit",
                    "amount": principal,
                }
            ],
        )

        transfer_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO transactions (
                account_id,
                transaction_type,
                is_cycle_topup,
                transaction_name,
                amount,
                date,
                is_transfer,
                transfer_id
            )
            VALUES
              (%s::uuid, 'credit', false, %s, %s, %s, true, %s::uuid),
              (%s::uuid, 'debit', false, %s, %s, %s, true, %s::uuid)
            """,
            (
                borrower_account_id,
                f"Loan repayment to {loan_row['lender_account_name']}",
                principal,
                final_date,
                transfer_id,
                lender_account_id,
                f"Loan repayment from {loan_row['borrower_account_name']}",
                principal,
                final_date,
                transfer_id,
            ),
        )

        cur.execute(
            """
            UPDATE internal_loans
            SET status='finalized',
                finalized_transfer_id=%s::uuid,
                finalized_at=%s
            WHERE loan_id=%s::uuid
              AND username=%s
              AND status='open'
            RETURNING loan_id::text AS loan_id
            """,
            (transfer_id, final_date, loan_id, username),
        )
        updated = cur.fetchone()
        if not updated:
            raise HTTPException(status_code=409, detail="Loan changed, please retry")
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "loan_id": loan_id, "finalized_transfer_id": transfer_id}


@router.put("/transactions/{transaction_id}")
async def update_tx(transaction_id: str, req: Request):
    username = require_session_user(req)
    transaction_id = parse_uuid_field(transaction_id, "transaction_id")
    data = await req.json()

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS account_id,
                   t.transaction_type,
                   t.transaction_name,
                   t.amount,
                   t.date,
                   t.is_transfer,
                   t.is_cycle_topup,
                   t.category_id::text AS category_id,
                   t.notes,
                   t.currency,
                   t.original_amount,
                   t.fx_rate,
                   t.tags,
                   t.is_reviewed
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

        new_account_id = data.get("account_id") or tx["account_id"]
        new_type = data.get("transaction_type") or tx["transaction_type"]
        if new_type not in ("debit", "credit"):
            raise HTTPException(status_code=400, detail="Invalid type")

        if "transaction_name" in data:
            new_name = (data.get("transaction_name") or "").strip()
            if not new_name:
                raise HTTPException(status_code=400, detail="transaction_name required")
        else:
            new_name = tx["transaction_name"]

        if "amount" in data:
            new_amount = parse_int_field(data.get("amount"), "amount", default=0)
        else:
            new_amount = int(tx["amount"])
        if new_amount <= 0:
            raise HTTPException(status_code=400, detail="Amount must be > 0")

        if "date" in data and data.get("date"):
            new_date = parse_tx_datetime(data.get("date"))
        else:
            new_date = tx["date"]
        is_cycle_topup = parse_optional_bool(data.get("is_cycle_topup"), "is_cycle_topup")
        if is_cycle_topup is None:
            is_cycle_topup = bool(tx.get("is_cycle_topup"))
        if is_cycle_topup and new_type != "debit":
            raise HTTPException(status_code=400, detail="Top-up/payroll can only be set on cash-in transactions")

        old_account_id = tx["account_id"]
        new_category_id = (data.get("category_id") or None) if "category_id" in data else (tx.get("category_id") or None)
        new_notes = ((data.get("notes") or "").strip() or None) if "notes" in data else (tx.get("notes") or None)
        new_currency = (data.get("currency") or tx.get("currency") or "IDR").upper()
        if new_currency not in ("IDR", "USD"):
            new_currency = "IDR"
        new_original_amount = int(data["original_amount"]) if data.get("original_amount") is not None else (None if "original_amount" in data else tx.get("original_amount"))
        new_fx_rate = float(data["fx_rate"]) if data.get("fx_rate") is not None else (None if "fx_rate" in data else tx.get("fx_rate"))
        new_tags = (
            [str(t).strip() for t in (data.get("tags") or []) if str(t).strip()]
            if "tags" in data
            else list(tx.get("tags") or [])
        )
        new_is_reviewed = bool(data["is_reviewed"]) if "is_reviewed" in data else bool(tx.get("is_reviewed", False))

        lock_accounts_for_update(cur, username, [old_account_id, new_account_id])

        cur.execute(
            """
            UPDATE transactions
            SET account_id=%s::uuid,
                transaction_type=%s,
                is_cycle_topup=%s,
                transaction_name=%s,
                amount=%s,
                date=%s,
                category_id=%s::uuid,
                notes=%s,
                currency=%s,
                original_amount=%s,
                fx_rate=%s,
                tags=%s,
                is_reviewed=%s
            WHERE transaction_id=%s::uuid AND deleted_at IS NULL
            RETURNING transaction_id
            """,
            (new_account_id, new_type, is_cycle_topup, new_name, new_amount, new_date,
             new_category_id,
             new_notes,
             new_currency,
             new_original_amount,
             new_fx_rate,
             new_tags,
             new_is_reviewed,
             transaction_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Transaction not found")
        shortfall = compute_shortfall_at_transaction(cur, new_account_id, new_date, transaction_id)
        conn.commit()

    invalidate_user_cache(username)
    return {
        "ok": True,
        "transaction_id": transaction_id,
        "needs_loan": shortfall > 0,
        "shortfall": int(shortfall),
        "account_id": new_account_id,
    }


@router.delete("/transactions/{transaction_id}")
def delete_tx(transaction_id: str, req: Request):
    username = require_session_user(req)
    transaction_id = parse_uuid_field(transaction_id, "transaction_id")
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
            (transaction_id, username)
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
        write_transaction_audit(
            cur,
            username=username,
            performed_by=username,
            action="soft_delete",
            tx_row=deleted_row,
        )
        conn.commit()
    invalidate_user_cache(username)
    return {"ok": True}


@router.post("/transactions/{transaction_id}/receipt")
async def upload_tx_receipt(
    transaction_id: str,
    req: Request,
    file: UploadFile = File(...),
    category: str | None = Form(None),
):
    username = require_session_user(req)
    transaction_id = parse_uuid_field(transaction_id, "transaction_id")
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
def get_tx_receipt(transaction_id: str, req: Request):
    username = require_session_user(req)
    transaction_id = parse_uuid_field(transaction_id, "transaction_id")
    with db_conn() as conn, conn.cursor() as cur:
        require_transaction_owner(cur, username, transaction_id)
        row = get_receipt_row(cur, username, transaction_id)
    if not row:
        raise HTTPException(status_code=404, detail="Receipt not found")
    return {"receipt": serialize_receipt_row(row)}


@router.get("/transactions/{transaction_id}/receipt/view")
def view_tx_receipt(transaction_id: str, req: Request):
    username = require_session_user(req)
    transaction_id = parse_uuid_field(transaction_id, "transaction_id")
    with db_conn() as conn, conn.cursor() as cur:
        require_transaction_owner(cur, username, transaction_id)
        row = get_receipt_row(cur, username, transaction_id)
    if not row:
        raise HTTPException(status_code=404, detail="Receipt not found")

    payload = load_receipt_content(row["relative_path"], row["storage_encoding"])
    filename = infer_inline_filename(row["transaction_id"], row["category"], row["stored_mime"])
    return Response(
        content=payload,
        media_type=row["stored_mime"],
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "private, max-age=60",
        },
    )


@router.delete("/transactions/{transaction_id}/receipt")
def delete_tx_receipt(transaction_id: str, req: Request):
    username = require_session_user(req)
    transaction_id = parse_uuid_field(transaction_id, "transaction_id")
    with db_conn() as conn, conn.cursor() as cur:
        require_transaction_owner(cur, username, transaction_id)
        deleted = delete_receipt_row(cur, username, transaction_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Receipt not found")
        conn.commit()
    remove_receipt_file(deleted.get("relative_path"))
    return {"ok": True}


@router.get("/ledger")
def ledger(
    req: Request,
    scope: str = "all",
    account_id: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 25,
    offset: int = 0,
    order: str = "desc",
    q: str | None = None,
    include_summary: bool = True,
    include_switch: bool = False,
):
    """
    scope:
      - all: all accounts
      - account: only one account (account_id required)
    date filters:
      - from_date/to_date in YYYY-MM-DD (default last 30 days)
    supports pagination and sorting:
      - limit, offset, order (asc/desc), q (fuzzy)
    returns rows with:
      - debit, credit, balance (running balance)
    """
    username = require_session_user(req)

    # default range: last 30 days
    if not to_date:
        to_dt = now_utc()
        to_date = to_dt.strftime("%Y-%m-%d")
    if not from_date:
        from_dt = (parse_date_utc(to_date, end_of_day=False) - timedelta(days=30))
        from_date = from_dt.strftime("%Y-%m-%d")

    from_dt = parse_date_utc(from_date, end_of_day=False)
    to_dt = parse_date_utc(to_date, end_of_day=True)

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
            include_summary,
            include_switch,
        )

    return {
        "range": {"from": from_date, "to": to_date},
        "scope": scope,
        "rows": rows,
        "paging": paging,
        "summary": None
        if not include_summary
        else {"accounts": summary_accounts, "total_asset": int(total_asset)},
    }



@router.get("/summary")
def summary(req: Request, month: str | None = None):
    username = require_session_user(req)
    if not month:
        month = current_month_local()
    parse_month(month)
    cache_key = f"{username}:summary:{month}"
    cached = cache_get(cache_key)
    if cached:
        return cached
    with db_conn() as conn, conn.cursor() as cur:
        payday_day, payday_source, override_day = get_payday_day(cur, username, month)
        default_day = get_default_payday_day(cur, username)
        prev_day, _, _ = get_payday_day(cur, username, prev_month_str(month))
        from_date, to_date, from_dt, to_dt = compute_dynamic_month_range(cur, username, month, payday_day, prev_day)
        start_cutoff = from_dt - timedelta(milliseconds=1)

        cur.execute(
            """
            SELECT account_id::text, account_name, profile_type, is_no_limit
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
                   amount,
                   source,
                   allocation_plan_id::text AS allocation_plan_id
            FROM budgets
            WHERE username=%s AND month=%s
            """,
            (username, month),
        )
        budgets = {
            r["account_id"]: {
                "amount": int(r["amount"] or 0),
                "budget_id": r["budget_id"],
                "source": r.get("source") or "manual",
                "allocation_plan_id": r.get("allocation_plan_id"),
            }
            for r in cur.fetchall()
        }

    accounts_sorted = sorted(accounts, key=lambda a: a["account_name"].lower())
    payload_accounts = []
    for acc in accounts_sorted:
        acc_id = acc["account_id"]
        total_row = totals.get(acc_id, {})
        total_in = int(total_row.get("total_in") or 0)
        total_out = int(total_row.get("total_out") or 0)
        is_budgeted_spending = (
            not bool(acc.get("is_no_limit"))
            and str(acc.get("profile_type")) in ("dynamic_spending", "fixed_spending")
        )
        budget_info = budgets.get(acc_id) if is_budgeted_spending else None
        budget_amount = budget_info["amount"] if budget_info else None
        budget_id = budget_info["budget_id"] if budget_info else None
        budget_source = budget_info["source"] if budget_info else None
        budget_used = total_out if budget_amount is not None else None
        budget_pct, budget_status, budget_remaining = compute_budget_status(budget_amount, total_out)
        starting_balance = int(balances_start.get(acc_id, 0))
        current_balance = int(balances_end.get(acc_id, 0))
        payload_accounts.append(
            {
                "account_id": acc_id,
                "account_name": acc["account_name"],
                "profile_type": acc.get("profile_type"),
                "is_no_limit": bool(acc.get("is_no_limit")),
                "starting_balance": starting_balance,
                "current_balance": current_balance,
                "total_in": total_in,
                "total_out": total_out,
                "budget_id": budget_id,
                "budget_source": budget_source,
                "budget_allocation_plan_id": budget_info.get("allocation_plan_id") if budget_info else None,
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


@router.get("/analysis")
def analysis(req: Request, month: str | None = None):
    username = require_session_user(req)
    if not month:
        month = current_month_local()
    parse_month(month)
    cache_key = f"{username}:analysis:{month}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    with db_conn() as conn, conn.cursor() as cur:
        payday_day, payday_source, override_day = get_payday_day(cur, username, month)
        default_day = get_default_payday_day(cur, username)
        prev_day, _, _ = get_payday_day(cur, username, prev_month_str(month))
        from_date, to_date, from_dt, to_dt = compute_dynamic_month_range(cur, username, month, payday_day, prev_day)

        base_filters = ["a.username=%s", "t.deleted_at IS NULL", "t.date >= %s", "t.date <= %s"]
        params: list[Any] = [username, from_dt, to_dt]

        cur.execute(
            f"""
            SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {" AND ".join(base_filters)}
              AND t.transfer_id IS NULL
            """,
            params,
        )
        totals_row = cur.fetchone() or {}
        total_in = int(totals_row.get("total_in") or 0)
        total_out = int(totals_row.get("total_out") or 0)

        cur.execute(
            f"""
            SELECT (t.date AT TIME ZONE %s)::date AS day,
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {" AND ".join(base_filters)}
              AND t.transfer_id IS NULL
            GROUP BY day
            ORDER BY day
            """,
            [settings.tz] + params,
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
            WHERE {" AND ".join(base_filters)}
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
            WHERE {" AND ".join(base_filters)}
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


@router.get("/analysis/budget-shift")
def analysis_budget_shift(req: Request, month: str | None = None, mode: str = "normal"):
    username = require_session_user(req)
    if not month:
        month = current_month_local()
    parse_month(month)

    mode = str(mode or "normal").strip().lower()
    cache_key = f"{username}:analysis:budget_shift:{month}:{mode}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    with db_conn() as conn, conn.cursor() as cur:
        payday_day, _, _ = get_payday_day(cur, username, month)
        prev_day, _, _ = get_payday_day(cur, username, prev_month_str(month))
        _, _, from_dt, to_dt = compute_dynamic_month_range(cur, username, month, payday_day, prev_day)
        payload = compute_budget_shift_analysis(cur, username, month, from_dt, to_dt, strategy=mode)

    cache_set(cache_key, payload, settings.month_summary_ttl)
    return payload


@router.get("/safety-net/report")
def safety_net_report(req: Request, hours: int = 24):
    username = require_session_user(req)
    try:
        hours = max(1, min(int(hours or 24), 168))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid hours")

    with db_conn() as conn, conn.cursor() as cur:
        payload = compute_financial_safety_report(cur, username, lookback_hours=hours)
    return payload


@router.get("/payday")
def get_payday(req: Request, month: str | None = None):
    username = require_session_user(req)
    if not month:
        month = current_month_local()
    parse_month(month)
    with db_conn() as conn, conn.cursor() as cur:
        payday_day, payday_source, override_day = get_payday_day(cur, username, month)
        default_day = get_default_payday_day(cur, username)
    return {
        "month": month,
        "day": payday_day,
        "source": payday_source,
        "default_day": default_day,
        "override_day": override_day,
    }


@router.put("/payday")
async def set_payday(req: Request):
    username = require_session_user(req)
    data = await req.json()
    month = data.get("month")
    day_val = data.get("day")
    clear_override_value = parse_optional_bool(data.get("clear_override"), "clear_override")
    clear_override = bool(clear_override_value) if clear_override_value is not None else False

    if month:
        parse_month(month)
        if clear_override:
            with db_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM payday_overrides WHERE username=%s AND month=%s",
                    (username, month),
                )
                conn.commit()
        else:
            try:
                day = int(day_val)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="Invalid payday day")
            if day < 1 or day > 31:
                raise HTTPException(status_code=400, detail="Payday day must be between 1 and 31")
            with db_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO payday_overrides (user_id, username, month, payday_day)
                    SELECT user_id, username, %s, %s
                    FROM users
                    WHERE username=%s
                    ON CONFLICT (username, month)
                    DO UPDATE SET payday_day=EXCLUDED.payday_day
                    """,
                    (month, day, username),
                )
                conn.commit()
    else:
        try:
            day = int(day_val)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Invalid payday day")
        if day < 1 or day > 31:
            raise HTTPException(status_code=400, detail="Payday day must be between 1 and 31")
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET default_payday_day=%s WHERE username=%s",
                (day, username),
            )
            conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.post("/balances/recompute")
def recompute_balances(req: Request):
    username = require_session_user(req)
    with db_conn() as conn, conn.cursor() as cur:
        report = recompute_balances_report(cur, username)
    invalidate_user_cache(username)
    return {
        "ok": True,
        "checked_at": now_utc().isoformat().replace("+00:00", "Z"),
        **report,
    }


@router.get("/transactions/audit")
def list_transaction_audit(req: Request, transaction_id: str | None = None, limit: int = 50):
    username = require_session_user(req)
    try:
        limit = max(1, min(int(limit or 50), 200))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid limit")
    if transaction_id:
        transaction_id = parse_uuid_field(transaction_id, "transaction_id")

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


@router.get("/export/preview")
def export_preview(req: Request, day: int, scope: str = "all", account_id: str | None = None):
    username = require_session_user(req)
    from_date, to_date, from_dt, to_dt = compute_export_range(day)

    with db_conn() as conn, conn.cursor() as cur:
        rows, _, _ = build_ledger_data(cur, username, scope, account_id, from_dt, to_dt)

    total_in = sum(int(r.get("debit") or 0) for r in rows)
    total_out = sum(int(r.get("credit") or 0) for r in rows)
    return {
        "range": {"from": from_date, "to": to_date},
        "summary": {
            "count": len(rows),
            "total_in": int(total_in),
            "total_out": int(total_out),
            "net": int(total_in - total_out),
        },
    }


@router.get("/export")
def export_ledger(
    req: Request,
    day: int,
    format: str = "pdf",
    scope: str = "all",
    account_id: str | None = None,
    currency: str | None = None,
    fx_rate: str | None = None,
):
    username = require_session_user(req)
    export_format = (format or "pdf").lower()
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
