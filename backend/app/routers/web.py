import uuid
from datetime import timedelta, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from passlib.hash import bcrypt
from psycopg.errors import UniqueViolation

from app.core.config import settings
from app.db.pool import db_conn
from app.services.auth import (
    create_api_key,
    enforce_login_rate_limit,
    enforce_register_rate_limit,
    get_active_api_key,
    register_user,
    require_session_user,
)
from app.services.ledger import (
    build_daily_series,
    build_ledger_data,
    build_ledger_page,
    build_weekly_series,
    cache_get,
    cache_set,
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
    prev_month_str,
    recompute_balances_report,
    write_transaction_audit,
)

router = APIRouter()

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
    enforce_login_rate_limit(req, username)

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT username, password_hash, full_name FROM users WHERE username=%s", (username,))
        user = cur.fetchone()

    if not user or not bcrypt.verify(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

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
async def create_account(req: Request):
    username = require_session_user(req)
    data = await req.json()
    account_name = (data.get("account_name") or "").strip()
    initial_balance_raw = data.get("initial_balance", 0)
    try:
        initial_balance = int(initial_balance_raw or 0)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid initial balance")
    if not account_name:
        raise HTTPException(status_code=400, detail="account_name required")
    if initial_balance < 0:
        raise HTTPException(status_code=400, detail="initial balance must be >= 0")

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
            if initial_balance > 0:
                cur.execute(
                    """
                    INSERT INTO transactions (account_id, transaction_type, transaction_name, amount, date, is_transfer)
                    VALUES (%s::uuid, 'debit', %s, %s, %s, false)
                    """,
                    (account_id, "Top Up Balance", initial_balance, now_utc()),
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
        return {"month": month, "budgets": cur.fetchall()}


@router.post("/budgets")
async def upsert_budget(req: Request):
    username = require_session_user(req)
    data = await req.json()
    account_id = data.get("account_id")
    month = data.get("month")
    amount = int(data.get("amount") or 0)
    if not account_id or not month:
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


@router.put("/budgets/{budget_id}")
async def update_budget(budget_id: str, req: Request):
    username = require_session_user(req)
    data = await req.json()
    amount = int(data.get("amount") or 0)
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
def delete_budget(budget_id: str, req: Request):
    username = require_session_user(req)
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
        cur.execute(
            f"""
            UPDATE accounts
            SET {", ".join(updates)}
            WHERE username=%s AND account_id=%s::uuid
            """,
            params,
        )
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.delete("/accounts/{account_id}")
def delete_account(account_id: str, req: Request):
    username = require_session_user(req)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT account_id::text AS account_id FROM accounts WHERE username=%s AND account_id=%s::uuid",
            (username, account_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Account not found")
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
    amount = int(data.get("amount") or 0)
    date_str = data.get("date")  # ISO string (from input datetime-local) or YYYY-MM-DD

    if not account_id or tx_type not in ("debit", "credit") or not name or amount <= 0 or not date_str:
        raise HTTPException(status_code=400, detail="Invalid transaction payload")

    dt = parse_tx_datetime(date_str)

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
                    "amount": amount,
                }
            ],
        )

        cur.execute(
            """
            INSERT INTO transactions (account_id, transaction_type, transaction_name, amount, date, is_transfer)
            VALUES (%s::uuid, %s, %s, %s, %s, false)
            RETURNING transaction_id::text
            """,
            (account_id, tx_type, name, amount, dt),
        )
        tx_id = cur.fetchone()["transaction_id"]
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "transaction_id": tx_id}


@router.post("/switch")
async def switch_balance(req: Request):
    username = require_session_user(req)
    data = await req.json()
    source_account_id = data.get("source_account_id")
    target_account_id = data.get("target_account_id")
    amount = int(data.get("amount") or 0)
    date_str = data.get("date")

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
        transfer_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO transactions (account_id, transaction_type, transaction_name, amount, date, is_transfer, transfer_id)
            VALUES
              (%s::uuid, 'credit', %s, %s, %s, true, %s::uuid),
              (%s::uuid, 'debit', %s, %s, %s, true, %s::uuid)
            RETURNING transaction_id::text
            """,
            (
                source_account_id,
                source_name,
                amount,
                dt,
                transfer_id,
                target_account_id,
                target_name,
                amount,
                dt,
                transfer_id,
            ),
        )
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "transfer_id": transfer_id}


@router.get("/switch/{transfer_id}")
def get_switch(transfer_id: str, req: Request):
    username = require_session_user(req)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS account_id,
                   t.transaction_type,
                   t.amount,
                   t.date,
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
    }


@router.put("/switch/{transfer_id}")
async def update_switch(transfer_id: str, req: Request):
    username = require_session_user(req)
    data = await req.json()
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS account_id,
                   t.transaction_type,
                   t.amount,
                   t.date,
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

        amount = int(data.get("amount") or source["amount"])
        if amount <= 0:
            raise HTTPException(status_code=400, detail="Amount must be > 0")

        if "date" in data and data.get("date"):
            new_date = parse_tx_datetime(data.get("date"))
        else:
            new_date = source["date"]

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
                transaction_name=%s,
                amount=%s,
                date=%s,
                is_transfer=true
            WHERE transaction_id=%s::uuid AND transfer_id=%s::uuid AND deleted_at IS NULL
            """,
            (
                source_account_id,
                source_name,
                amount,
                new_date,
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
                transaction_name=%s,
                amount=%s,
                date=%s,
                is_transfer=true
            WHERE transaction_id=%s::uuid AND transfer_id=%s::uuid AND deleted_at IS NULL
            """,
            (
                target_account_id,
                target_name,
                amount,
                new_date,
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


@router.put("/transactions/{transaction_id}")
async def update_tx(transaction_id: str, req: Request):
    username = require_session_user(req)
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
            new_amount = int(data.get("amount") or 0)
        else:
            new_amount = int(tx["amount"])
        if new_amount <= 0:
            raise HTTPException(status_code=400, detail="Amount must be > 0")

        if "date" in data and data.get("date"):
            new_date = parse_tx_datetime(data.get("date"))
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
            RETURNING transaction_id
            """,
            (new_account_id, new_type, new_name, new_amount, new_date, transaction_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Transaction not found")
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.delete("/transactions/{transaction_id}")
def delete_tx(transaction_id: str, req: Request):
    username = require_session_user(req)
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
            cur, username, scope, account_id, from_dt, to_dt, limit, offset, order, q, include_summary
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
        month = now_utc().strftime("%Y-%m")
    parse_month(month)
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


@router.get("/analysis")
def analysis(req: Request, month: str | None = None):
    username = require_session_user(req)
    if not month:
        month = now_utc().strftime("%Y-%m")
    parse_month(month)
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
            SELECT (t.date AT TIME ZONE 'UTC')::date AS day,
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {" AND ".join(base_filters)}
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
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS switch_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS switch_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {" AND ".join(base_filters)}
              AND t.transfer_id IS NOT NULL
            GROUP BY t.account_id
            """,
            params,
        )
        switch_raw = cur.fetchall()

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

    switch_by_account = {
        r.get("account_id"): {
            "switch_in": int(r.get("switch_in") or 0),
            "switch_out": int(r.get("switch_out") or 0),
        }
        for r in switch_raw
    }

    categories = [
        {
            "account_id": r.get("account_id"),
            "account_name": r.get("account_name"),
            "total_in": int(r.get("total_in") or 0),
            "total_out": int(r.get("total_out") or 0),
            "net": int(r.get("total_in") or 0) - int(r.get("total_out") or 0),
            "switch_in": int((switch_by_account.get(r.get("account_id")) or {}).get("switch_in") or 0),
            "switch_out": int((switch_by_account.get(r.get("account_id")) or {}).get("switch_out") or 0),
        }
        for r in categories_raw
    ]

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


@router.get("/payday")
def get_payday(req: Request, month: str | None = None):
    username = require_session_user(req)
    if not month:
        month = now_utc().strftime("%Y-%m")
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
    clear_override = bool(data.get("clear_override"))

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
                    INSERT INTO payday_overrides (username, month, payday_day)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (username, month)
                    DO UPDATE SET payday_day=EXCLUDED.payday_day
                    """,
                    (username, month, day),
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
