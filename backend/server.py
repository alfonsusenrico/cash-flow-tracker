import os
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from passlib.hash import bcrypt
from psycopg import connect
from psycopg.rows import dict_row
from starlette.middleware.sessions import SessionMiddleware

DATABASE_URL = os.getenv("DATABASE_URL", "")
SESSION_SECRET = os.getenv("SESSION_SECRET", "change-me")
TZ = os.getenv("TZ", "Asia/Jakarta")  # for display in UI only; DB stores timestamptz
OPENING_TX_NAME = "Opening balance"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

app = FastAPI()
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="ledger_session",
    same_site="lax",
    https_only=False,  # set True if behind HTTPS
)


def db():
    return connect(DATABASE_URL, row_factory=dict_row)


def now_utc():
    return datetime.now(timezone.utc)


def require_user(req: Request) -> str:
    u = (req.session or {}).get("username")
    if not u:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return u


def get_main_account(cur, username: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT account_id::text AS account_id, account_name, opening_balance
        FROM accounts
        WHERE username=%s AND parent_account_id IS NULL
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (username,),
    )
    main = cur.fetchone()
    if not main:
        raise HTTPException(status_code=400, detail="Main account missing")
    return main


def parse_date_utc(date_str: str, end_of_day: bool = False) -> datetime:
    # Accepts YYYY-MM-DD, returns UTC datetime range boundary
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format, expected YYYY-MM-DD")
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return dt + (timedelta(days=1) - timedelta(milliseconds=1) if end_of_day else timedelta(0))


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/auth/register")
async def register(req: Request):
    # For initial setup / testing. You can remove later.
    data = await req.json()
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    if len(password.encode("utf-8")) > 72:
        raise HTTPException(status_code=400, detail="Password too long (max 72 bytes)")
    full_name = (data.get("full_name") or "").strip() or username
    opening_balance = int(data.get("opening_balance") or 0)

    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password required")
    if opening_balance < 0:
        raise HTTPException(status_code=400, detail="opening_balance must be >= 0")

    pw_hash = bcrypt.hash(password)

    delta_total = None
    with db() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                "INSERT INTO users (username, password_hash, full_name) VALUES (%s, %s, %s)",
                (username, pw_hash, full_name),
            )
            # auto-create main account
            cur.execute(
                """
                INSERT INTO accounts (username, account_name, parent_account_id, opening_balance)
                VALUES (%s, %s, NULL, %s)
                RETURNING account_id::text
                """,
                (username, "Main Account", opening_balance),
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=400, detail="User already exists or invalid data")

    return {"ok": True}


@app.post("/auth/login")
async def login(req: Request):
    data = await req.json()
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    if len(password.encode("utf-8")) > 72:
        raise HTTPException(status_code=400, detail="Password too long (max 72 bytes)")
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password required")

    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT username, password_hash, full_name FROM users WHERE username=%s", (username,))
        user = cur.fetchone()

    if not user or not bcrypt.verify(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    req.session["username"] = user["username"]
    req.session["full_name"] = user["full_name"]
    return {"ok": True, "username": user["username"], "full_name": user["full_name"]}


@app.post("/auth/logout")
def logout(req: Request):
    req.session.clear()
    return {"ok": True}


@app.get("/me")
def me(req: Request):
    username = require_user(req)
    return {"username": username, "full_name": req.session.get("full_name", username), "tz": TZ}


@app.get("/accounts")
def list_accounts(req: Request):
    username = require_user(req)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.account_id::text,
                   a.account_name,
                   a.parent_account_id::text AS parent_account_id,
                   a.opening_balance,
                   COALESCE(o.opening_adjust, 0) AS opening_adjust
            FROM accounts a
            LEFT JOIN (
                SELECT t.account_id,
                       SUM(CASE WHEN t.transaction_type = 'debit' THEN t.amount ELSE -t.amount END) AS opening_adjust
                FROM transactions t
                JOIN accounts a2 ON a2.account_id = t.account_id
                WHERE a2.username=%s AND t.transaction_name=%s
                GROUP BY t.account_id
            ) o ON a.account_id = o.account_id
            WHERE a.username=%s
            ORDER BY (a.parent_account_id IS NOT NULL), a.account_name
            """,
            (username, OPENING_TX_NAME, username),
        )
        return {"accounts": cur.fetchall()}


@app.post("/accounts")
async def create_account(req: Request):
    username = require_user(req)
    data = await req.json()
    account_name = (data.get("account_name") or "").strip()
    opening_balance = int(data.get("opening_balance") or 0)

    if not account_name:
        raise HTTPException(status_code=400, detail="account_name required")
    if opening_balance < 0:
        raise HTTPException(status_code=400, detail="opening_balance must be >= 0")

    with db() as conn, conn.cursor() as cur:
        main = get_main_account(cur, username)
        parent_account_id = main["account_id"]

        try:
            cur.execute(
                """
                INSERT INTO accounts (username, account_name, parent_account_id, opening_balance)
                VALUES (%s, %s, %s::uuid, %s)
                RETURNING account_id::text
                """,
                (username, account_name, parent_account_id, opening_balance),
            )
            account_id = cur.fetchone()["account_id"]
            conn.commit()
        except Exception:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Account name already exists")

    return {"ok": True, "account_id": account_id}


@app.put("/accounts/{account_id}")
async def update_account(account_id: str, req: Request):
    username = require_user(req)
    data = await req.json()
    account_name = (data.get("account_name") or "").strip()
    opening_balance = data.get("opening_balance", None)
    password = (data.get("password") or "").strip()

    if opening_balance is not None:
        opening_balance = int(opening_balance or 0)

    if not account_name and opening_balance is None:
        raise HTTPException(status_code=400, detail="No changes provided")

    with db() as conn, conn.cursor() as cur:
        # Get account info + user password hash
        cur.execute(
            """
            SELECT a.account_id::text AS account_id,
                   a.parent_account_id IS NULL AS is_main,
                   a.opening_balance,
                   u.password_hash
            FROM accounts a
            JOIN users u ON u.username = a.username
            WHERE a.username=%s AND a.account_id=%s::uuid
            """,
            (username, account_id),
        )
        acc = cur.fetchone()
        if not acc:
            raise HTTPException(status_code=404, detail="Account not found")

        updates = []
        params: list[Any] = []
        if account_name:
            updates.append("account_name=%s")
            params.append(account_name)

        if opening_balance is not None:
            if opening_balance < 0:
                raise HTTPException(status_code=400, detail="opening_balance must be >= 0")
            
            # Logic: 
            # Non-main accounts: can change anytime.
            # Main account: If already set (nonzero), requires password to change.
            current_bal = int(acc["opening_balance"] or 0)
            
            if acc["is_main"] and current_bal != 0 and opening_balance != current_bal:
                if not password:
                     raise HTTPException(status_code=403, detail="Password required to change Main Account opening balance")
                if not bcrypt.verify(password, acc["password_hash"]):
                     raise HTTPException(status_code=403, detail="Invalid password")

            updates.append("opening_balance=%s")
            params.append(opening_balance)

        if not updates:
            raise HTTPException(status_code=400, detail="No changes provided")

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

    return {"ok": True}


@app.delete("/accounts/{account_id}")
def delete_account(account_id: str, req: Request):
    username = require_user(req)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT account_id::text AS account_id, parent_account_id IS NULL AS is_main
            FROM accounts
            WHERE username=%s AND account_id=%s::uuid
            """,
            (username, account_id),
        )
        acc = cur.fetchone()
        if not acc:
            raise HTTPException(status_code=404, detail="Account not found")
        if acc["is_main"]:
            raise HTTPException(status_code=400, detail="Cannot delete main account")

        cur.execute(
            "DELETE FROM accounts WHERE username=%s AND account_id=%s::uuid",
            (username, account_id),
        )
        conn.commit()

    return {"ok": True}


@app.post("/transactions")
async def create_tx(req: Request):
    username = require_user(req)
    data = await req.json()

    account_id = data.get("account_id")
    tx_type = data.get("transaction_type")
    name = (data.get("transaction_name") or "").strip()
    amount = int(data.get("amount") or 0)
    date_str = data.get("date")  # ISO string (from input datetime-local) or YYYY-MM-DD

    if not account_id or tx_type not in ("debit", "credit") or not name or amount <= 0 or not date_str:
        raise HTTPException(status_code=400, detail="Invalid transaction payload")

    # Parse date string
    dt = None
    try:
        # Accept "YYYY-MM-DDTHH:MM" (no tz) -> treat as UTC
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    except Exception:
        # Accept YYYY-MM-DD as date-only
        dt = parse_date_utc(date_str, end_of_day=False)

    with db() as conn, conn.cursor() as cur:
        # ensure account belongs to user
        cur.execute(
            "SELECT 1 FROM accounts WHERE username=%s AND account_id=%s::uuid",
            (username, account_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=400, detail="Invalid account_id")

        cur.execute(
            """
            INSERT INTO transactions (account_id, transaction_type, transaction_name, amount, date)
            VALUES (%s::uuid, %s, %s, %s, %s)
            RETURNING transaction_id::text
            """,
            (account_id, tx_type, name, amount, dt),
        )
        tx_id = cur.fetchone()["transaction_id"]
        conn.commit()

    return {"ok": True, "transaction_id": tx_id}


@app.put("/transactions/{transaction_id}")
async def update_tx(transaction_id: str, req: Request):
    username = require_user(req)
    data = await req.json()
    
    # We allow updating mostly everything
    # But we need to handle if account_id changes (move tx)
    
    account_id = data.get("account_id")
    tx_type = data.get("transaction_type")
    name = (data.get("transaction_name") or "").strip()
    amount = data.get("amount")
    date_str = data.get("date")

    updates = []
    params: list[Any] = []

    if account_id:
        # verify new account belongs to user
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM accounts WHERE username=%s AND account_id=%s::uuid", (username, account_id))
            if not cur.fetchone():
                 raise HTTPException(status_code=400, detail="Invalid account_id")
        updates.append("account_id=%s::uuid")
        params.append(account_id)
    
    if tx_type:
        if tx_type not in ("debit", "credit"):
             raise HTTPException(status_code=400, detail="Invalid type")
        updates.append("transaction_type=%s")
        params.append(tx_type)
        
    if name:
        updates.append("transaction_name=%s")
        params.append(name)
        
    if amount is not None:
        amt = int(amount)
        if amt <= 0:
             raise HTTPException(status_code=400, detail="Amount must be > 0")
        updates.append("amount=%s")
        params.append(amt)
        
    if date_str:
        dt = None
        try:
            dt = datetime.fromisoformat(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        except:
             # try parse_date_utc if it was just YYYY-MM-DD, but frontend sends ISO usually
             pass
        if dt:
            updates.append("date=%s")
            params.append(dt)

    if not updates:
         raise HTTPException(status_code=400, detail="No changes")

    params.append(transaction_id)
    params.append(username)

    with db() as conn, conn.cursor() as cur:
        # Update ensuring tx belongs to user (via account join)
        cur.execute(
            f"""
            UPDATE transactions t
            SET {", ".join(updates)}
            FROM accounts a
            WHERE t.account_id = a.account_id
              AND t.transaction_id = %s::uuid
              AND a.username = %s
            RETURNING t.transaction_id
            """,
            params
        )
        if not cur.fetchone():
             raise HTTPException(status_code=404, detail="Transaction not found")
        conn.commit()

    return {"ok": True}


@app.delete("/transactions/{transaction_id}")
def delete_tx(transaction_id: str, req: Request):
    username = require_user(req)
    with db() as conn, conn.cursor() as cur:
        # Check ownership and delete
        cur.execute(
            """
            DELETE FROM transactions t
            USING accounts a
            WHERE t.account_id = a.account_id
              AND t.transaction_id = %s::uuid
              AND a.username = %s
            RETURNING t.transaction_id
            """,
            (transaction_id, username)
        )
        if not cur.fetchone():
             raise HTTPException(status_code=404, detail="Transaction not found")
        conn.commit()
    return {"ok": True}


@app.get("/ledger")
def ledger(req: Request, scope: str = "all", account_id: str | None = None, from_date: str | None = None, to_date: str | None = None):
    """
    scope:
      - all: all accounts
      - account: only one account (account_id required)
    date filters:
      - from_date/to_date in YYYY-MM-DD (default last 30 days)
    returns rows sorted by date asc then transaction_id asc, with:
      - debit, credit, balance (per-account running balance)
    """
    username = require_user(req)

    # default range: last 30 days
    if not to_date:
        to_dt = now_utc()
        to_date = to_dt.strftime("%Y-%m-%d")
    if not from_date:
        from_dt = (parse_date_utc(to_date, end_of_day=False) - timedelta(days=30))
        from_date = from_dt.strftime("%Y-%m-%d")

    from_dt = parse_date_utc(from_date, end_of_day=False)
    to_dt = parse_date_utc(to_date, end_of_day=True)

    if scope not in ("all", "account"):
        raise HTTPException(status_code=400, detail="Invalid scope")

    if scope == "account" and not account_id:
        raise HTTPException(status_code=400, detail="account_id required for scope=account")

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT account_id::text, account_name, parent_account_id::text AS parent_account_id, opening_balance
            FROM accounts
            WHERE username=%s
            ORDER BY (parent_account_id IS NOT NULL), account_name
            """,
            (username,),
        )
        all_accounts = cur.fetchall()

        if not all_accounts:
            return {"range": {"from": from_date, "to": to_date}, "rows": [], "summary": {"accounts": []}}

        main = next((a for a in all_accounts if not a["parent_account_id"]), None)
        if not main:
            raise HTTPException(status_code=400, detail="Main account missing")

        main_id = main["account_id"]
        acc_by_id: dict[str, dict[str, Any]] = {a["account_id"]: a for a in all_accounts}
        all_ids = list(acc_by_id.keys())

        cur.execute(
            """
            SELECT t.account_id::text AS account_id, COALESCE(SUM(t.amount), 0) AS opening_adjust
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE a.username=%s AND t.transaction_name=%s
            GROUP BY t.account_id
            """,
            (username, OPENING_TX_NAME),
        )
        opening_adjust = {r["account_id"]: int(r["opening_adjust"]) for r in cur.fetchall()}

        def fetch_txs(acc_ids: list[str]) -> list[dict[str, Any]]:
            cur.execute(
                """
                SELECT t.transaction_id::text AS transaction_id,
                       t.account_id::text AS account_id,
                       a.account_name,
                       t.transaction_type,
                       t.transaction_name,
                       t.amount,
                       t.date
                FROM transactions t
                JOIN accounts a ON a.account_id=t.account_id
                WHERE a.username=%s
                  AND t.account_id = ANY(%s::uuid[])
                  AND t.transaction_name <> %s
                  AND t.date >= %s AND t.date <= %s
                ORDER BY t.date ASC, t.transaction_id ASC
                """,
                (username, acc_ids, OPENING_TX_NAME, from_dt, to_dt),
            )
            return cur.fetchall()

        def signed_amount(t: dict[str, Any]) -> int:
            return int(t["amount"]) if t["transaction_type"] == "debit" else -int(t["amount"])

        if scope == "account":
            if account_id not in acc_by_id:
                raise HTTPException(status_code=404, detail="Account not found")

            if account_id == main_id:
                # Main account shows all transactions and a running total
                cur.execute(
                    """
                    SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS delta
                    FROM transactions t
                    JOIN accounts a ON a.account_id=t.account_id
                    WHERE a.username=%s AND t.transaction_name <> %s AND t.date < %s
                    """,
                    (username, OPENING_TX_NAME, from_dt),
                )
                delta = int(cur.fetchone()["delta"] or 0)
                running = int(main["opening_balance"]) + int(opening_adjust.get(main_id, 0)) + delta

                txs = fetch_txs(all_ids)
                rows = []
                for i, t in enumerate(txs, start=1):
                    signed = signed_amount(t)
                    running += signed
                    rows.append(
                        {
                            "no": i,
                            "account_id": t["account_id"],
                            "account_name": t["account_name"],
                            "date": t["date"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                            "transaction_id": t["transaction_id"],
                            "transaction_name": t["transaction_name"],
                            "debit": int(t["amount"]) if t["transaction_type"] == "debit" else 0,
                            "credit": int(t["amount"]) if t["transaction_type"] == "credit" else 0,
                            "balance": int(running),
                        }
                    )

                summary_accounts = [
                    {"account_id": main_id, "account_name": main["account_name"], "balance": int(running)}
                ]

                return {
                    "range": {"from": from_date, "to": to_date},
                    "scope": scope,
                    "rows": rows,
                    "summary": {"accounts": summary_accounts},
                }

            acc_ids = [account_id]
        else:
            acc_ids = all_ids

        # starting balance per account = opening + sum signed amounts before from_dt
        cur.execute(
            """
            SELECT a.account_id::text AS account_id,
                   a.opening_balance
                   + COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS start_balance
            FROM accounts a
            LEFT JOIN transactions t
              ON t.account_id=a.account_id AND t.transaction_name <> %s AND t.date < %s
            WHERE a.username=%s AND a.account_id = ANY(%s::uuid[])
            GROUP BY a.account_id, a.opening_balance
            """,
            (OPENING_TX_NAME, from_dt, username, acc_ids),
        )
        start_rows = cur.fetchall()
        balance = {
            r["account_id"]: int(r["start_balance"]) + int(opening_adjust.get(r["account_id"], 0))
            for r in start_rows
        }

        txs = fetch_txs(acc_ids)

        if scope == "all":
            cur.execute(
                """
                SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS delta
                FROM transactions t
                JOIN accounts a ON a.account_id=t.account_id
                WHERE a.username=%s AND t.transaction_name <> %s AND t.date < %s
                """,
                (username, OPENING_TX_NAME, from_dt),
            )
            delta_total = int(cur.fetchone()["delta"] or 0)

    rows = []
    total_signed = 0
    overall_running = None
    if scope == "all":
        overall_running = (
            int(main["opening_balance"]) + int(opening_adjust.get(main_id, 0)) + int(delta_total or 0)
        )
    for i, t in enumerate(txs, start=1):
        aid = t["account_id"]
        signed = signed_amount(t)
        balance[aid] = int(balance.get(aid, 0) + signed)
        total_signed += signed
        row_balance = balance[aid]
        if overall_running is not None:
            overall_running += signed
            row_balance = int(overall_running)
        rows.append(
            {
                "no": i,
                "account_id": aid,
                "account_name": t["account_name"],
                "date": t["date"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "transaction_id": t["transaction_id"],
                "transaction_name": t["transaction_name"],
                "debit": int(t["amount"]) if t["transaction_type"] == "debit" else 0,
                "credit": int(t["amount"]) if t["transaction_type"] == "credit" else 0,
                "balance": row_balance,
            }
        )

    summary_accounts = [
        {"account_id": aid, "account_name": acc_by_id[aid]["account_name"], "balance": int(balance.get(aid, 0))}
        for aid in sorted(acc_by_id.keys(), key=lambda x: acc_by_id[x]["account_name"].lower())
        if aid in balance
    ]

    if scope == "all":
        total_balance = (
            int(main["opening_balance"])
            + int(opening_adjust.get(main_id, 0))
            + int(delta_total or 0)
            + total_signed
        )
        for a in summary_accounts:
            if a["account_id"] == main_id:
                a["balance"] = int(total_balance)
                break

    return {
        "range": {"from": from_date, "to": to_date},
        "scope": scope,
        "rows": rows,
        "summary": {"accounts": summary_accounts},
    }


@app.exception_handler(HTTPException)
def http_exc_handler(_, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})
