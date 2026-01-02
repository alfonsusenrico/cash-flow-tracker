import csv
import io
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fpdf import FPDF
from passlib.hash import bcrypt
from psycopg import connect
from psycopg.rows import dict_row
from starlette.middleware.sessions import SessionMiddleware

DATABASE_URL = os.getenv("DATABASE_URL", "")
SESSION_SECRET = os.getenv("SESSION_SECRET")
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() == "true"
TZ = os.getenv("TZ", "Asia/Jakarta")  # for display in UI only; DB stores timestamptz

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")
if not SESSION_SECRET:
    raise RuntimeError("SESSION_SECRET is required")

app = FastAPI()
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="ledger_session",
    same_site="lax",
    https_only=COOKIE_SECURE,
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
        SELECT account_id::text AS account_id, account_name
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


def clamp_day(year: int, month: int, day: int) -> int:
    if month == 12:
        last_day = (datetime(year + 1, 1, 1) - timedelta(days=1)).day
    else:
        last_day = (datetime(year, month + 1, 1) - timedelta(days=1)).day
    return min(day, last_day)


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
    start = last_payday + timedelta(days=1)
    from_date = start.isoformat()
    to_date = today.isoformat()
    from_dt = parse_date_utc(from_date, end_of_day=False)
    to_dt = parse_date_utc(to_date, end_of_day=True)
    return from_date, to_date, from_dt, to_dt


def parse_month(month: str) -> tuple[int, int]:
    try:
        dt = datetime.strptime(month, "%Y-%m")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid month format, expected YYYY-MM")
    return dt.year, dt.month


def compute_month_range(month: str) -> tuple[str, str, datetime, datetime]:
    year, month_num = parse_month(month)
    payday = datetime(year, month_num, clamp_day(year, month_num, 31)).date()
    prev_month = month_num - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1
    prev_payday = datetime(prev_year, prev_month, clamp_day(prev_year, prev_month, 31)).date()
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


def is_switch_tx_name(name: str) -> bool:
    return name.startswith("Switching from ") or name.startswith("Switching to ")


def is_all_internal_name(name: str) -> bool:
    if name == "Top Up Balance":
        return True
    return is_switch_tx_name(name)


def build_search_pattern(query: str | None) -> str | None:
    if not query:
        return None
    cleaned = re.sub(r"[^a-z0-9]", "", query.lower())
    if not cleaned:
        return None
    if len(cleaned) > 64:
        cleaned = cleaned[:64]
    return "%" + "%".join(cleaned) + "%"


def parse_tx_datetime(date_str: str | None) -> datetime:
    if not date_str:
        return now_utc()
    try:
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return parse_date_utc(date_str, end_of_day=False)


def get_account_balances(cur, username: str, up_to: datetime) -> dict[str, int]:
    cur.execute(
        """
        SELECT a.account_id::text AS account_id,
               COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS balance
        FROM accounts a
        LEFT JOIN transactions t
          ON t.account_id=a.account_id AND t.date <= %s
        WHERE a.username=%s
        GROUP BY a.account_id
        """,
        (up_to, username),
    )
    return {r["account_id"]: int(r["balance"] or 0) for r in cur.fetchall()}


def compute_summary(
    cur,
    username: str,
    acc_by_id: dict[str, dict[str, Any]],
    main_id: str,
    to_dt: datetime,
) -> tuple[list[dict[str, Any]], int, int]:
    balances_all = get_account_balances(cur, username, to_dt)
    summary_accounts = [
        {
            "account_id": aid,
            "account_name": acc_by_id[aid]["account_name"],
            "balance": int(balances_all.get(aid, 0)),
        }
        for aid in sorted(acc_by_id.keys(), key=lambda x: acc_by_id[x]["account_name"].lower())
    ]

    cur.execute(
        """
        SELECT COALESCE(SUM(t.amount), 0) AS allocated_total
        FROM transactions t
        JOIN accounts a ON a.account_id=t.account_id
        WHERE a.username=%s
          AND a.parent_account_id IS NOT NULL
          AND t.transaction_name=%s
          AND t.transaction_type='debit'
          AND t.date <= %s
        """,
        (username, "Top Up Balance", to_dt),
    )
    allocated_total = int(cur.fetchone()["allocated_total"] or 0)
    main_balance = int(balances_all.get(main_id, 0))
    unallocated = main_balance - allocated_total
    total_asset = sum(
        int(balances_all.get(aid, 0))
        for aid, acc in acc_by_id.items()
        if acc["parent_account_id"] is not None
    ) + int(unallocated)

    return summary_accounts, int(total_asset), int(unallocated)


def build_ledger_data(
    cur,
    username: str,
    scope: str,
    account_id: str | None,
    from_dt: datetime,
    to_dt: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int]:
    cur.execute(
        """
        SELECT account_id::text, account_name, parent_account_id::text AS parent_account_id
        FROM accounts
        WHERE username=%s
        """,
        (username,),
    )
    accounts = cur.fetchall()
    acc_by_id = {a["account_id"]: a for a in accounts}
    main = next((a for a in accounts if a["parent_account_id"] is None), None)
    if not main:
        raise HTTPException(status_code=400, detail="Main account missing")
    main_id = main["account_id"]

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
          ON t.account_id=a.account_id AND t.date < %s
        WHERE a.username=%s AND a.account_id = ANY(%s::uuid[])
        GROUP BY a.account_id
        """,
        (from_dt, username, acc_ids),
    )
    start_rows = cur.fetchall()
    balance = {r["account_id"]: int(r["start_balance"]) for r in start_rows}

    total_asset_running = None
    if scope == "all":
        main_start = int(balance.get(main_id, 0))
        non_main_start = sum(
            int(balance.get(aid, 0))
            for aid, acc in acc_by_id.items()
            if acc["parent_account_id"] is not None
        )
        cur.execute(
            """
            SELECT COALESCE(SUM(t.amount), 0) AS allocated_total
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE a.username=%s
              AND a.parent_account_id IS NOT NULL
              AND t.transaction_name=%s
              AND t.transaction_type='debit'
              AND t.date < %s
            """,
            (username, "Top Up Balance", from_dt),
        )
        allocated_start = int(cur.fetchone()["allocated_total"] or 0)
        unallocated_start = main_start - allocated_start
        total_asset_running = non_main_start + unallocated_start

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
        name = (t.get("transaction_name") or "").strip()
        if scope == "all" and is_all_internal_name(name):
            continue
        if scope == "account" and account_id == main_id and is_switch_tx_name(name):
            continue
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
            }
        )

    summary_accounts, total_asset, unallocated = compute_summary(cur, username, acc_by_id, main_id, to_dt)

    return rows, summary_accounts, total_asset, unallocated


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
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int, dict[str, int | bool]]:
    cur.execute(
        """
        SELECT account_id::text, account_name, parent_account_id::text AS parent_account_id
        FROM accounts
        WHERE username=%s
        """,
        (username,),
    )
    accounts = cur.fetchall()
    if not accounts:
        return [], [], 0, 0, {"limit": limit, "offset": offset, "has_more": False, "next_offset": offset}

    acc_by_id = {a["account_id"]: a for a in accounts}
    main = next((a for a in accounts if a["parent_account_id"] is None), None)
    if not main:
        raise HTTPException(status_code=400, detail="Main account missing")
    main_id = main["account_id"]

    if scope not in ("all", "account"):
        raise HTTPException(status_code=400, detail="Invalid scope")
    if scope == "account":
        if not account_id:
            raise HTTPException(status_code=400, detail="account_id required for scope=account")
        if account_id not in acc_by_id:
            raise HTTPException(status_code=404, detail="Account not found")

    if order not in ("asc", "desc"):
        order = "desc"
    order_dir = "ASC" if order == "asc" else "DESC"
    limit = max(1, min(int(limit or 25), 100))
    offset = max(0, int(offset or 0))

    summary_accounts, total_asset, unallocated = compute_summary(cur, username, acc_by_id, main_id, to_dt)

    base_balance = 0
    if scope == "all":
        all_ids = list(acc_by_id.keys())
        cur.execute(
            """
            SELECT a.account_id::text AS account_id,
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS start_balance
            FROM accounts a
            LEFT JOIN transactions t
              ON t.account_id=a.account_id AND t.date < %s
            WHERE a.username=%s AND a.account_id = ANY(%s::uuid[])
            GROUP BY a.account_id
            """,
            (from_dt, username, all_ids),
        )
        start_rows = cur.fetchall()
        balance = {r["account_id"]: int(r["start_balance"]) for r in start_rows}
        main_start = int(balance.get(main_id, 0))
        non_main_start = sum(
            int(balance.get(aid, 0))
            for aid, acc in acc_by_id.items()
            if acc["parent_account_id"] is not None
        )
        cur.execute(
            """
            SELECT COALESCE(SUM(t.amount), 0) AS allocated_total
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE a.username=%s
              AND a.parent_account_id IS NOT NULL
              AND t.transaction_name=%s
              AND t.transaction_type='debit'
              AND t.date < %s
            """,
            (username, "Top Up Balance", from_dt),
        )
        allocated_start = int(cur.fetchone()["allocated_total"] or 0)
        unallocated_start = main_start - allocated_start
        base_balance = non_main_start + unallocated_start
    else:
        cur.execute(
            """
            SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS start_balance
            FROM transactions t
            WHERE t.account_id=%s::uuid AND t.date < %s
            """,
            (account_id, from_dt),
        )
        base_balance = int(cur.fetchone()["start_balance"] or 0)

    base_filters = ["a.username=%s", "t.date >= %s", "t.date <= %s"]
    params: list[Any] = [username, from_dt, to_dt]
    if scope == "account":
        base_filters.append("t.account_id=%s::uuid")
        params.append(account_id)
    if scope == "all":
        base_filters.append(
            "NOT (t.transaction_name = %s OR t.transaction_name ILIKE %s OR t.transaction_name ILIKE %s)"
        )
        params.extend(["Top Up Balance", "Switching from %", "Switching to %"])

    search_pattern = build_search_pattern(query)
    search_sql = ""
    search_params: list[Any] = []
    if search_pattern:
        search_sql = "WHERE transaction_name ILIKE %s OR account_name ILIKE %s"
        search_params.extend([search_pattern, search_pattern])

    sql = f"""
        WITH base AS (
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS account_id,
                   a.account_name,
                   t.transaction_type,
                   t.transaction_name,
                   t.amount,
                   t.date,
                   SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END)
                     OVER (ORDER BY t.date ASC, t.transaction_id ASC) AS running_delta
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {" AND ".join(base_filters)}
        )
        SELECT transaction_id, account_id, account_name, transaction_type, transaction_name, amount, date, running_delta
        FROM base
        {search_sql}
        ORDER BY date {order_dir}, transaction_id {order_dir}
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
                "account_id": r["account_id"],
                "account_name": r["account_name"],
                "date": r["date"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "transaction_id": r["transaction_id"],
                "transaction_name": r["transaction_name"],
                "debit": int(r["amount"]) if r["transaction_type"] == "debit" else 0,
                "credit": int(r["amount"]) if r["transaction_type"] == "credit" else 0,
                "balance": int(balance),
            }
        )

    paging = {"limit": limit, "offset": offset, "has_more": has_more, "next_offset": offset + len(rows)}
    return rows, summary_accounts, total_asset, unallocated, paging


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
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password required")

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
                INSERT INTO accounts (username, account_name, parent_account_id)
                VALUES (%s, %s, NULL)
                RETURNING account_id::text
                """,
                (username, "Main Account"),
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
                   a.parent_account_id::text AS parent_account_id
            FROM accounts a
            WHERE a.username=%s
            ORDER BY (a.parent_account_id IS NOT NULL), a.account_name
            """,
            (username,),
        )
        return {"accounts": cur.fetchall()}


@app.post("/accounts")
async def create_account(req: Request):
    username = require_user(req)
    data = await req.json()
    account_name = (data.get("account_name") or "").strip()
    if not account_name:
        raise HTTPException(status_code=400, detail="account_name required")

    with db() as conn, conn.cursor() as cur:
        main = get_main_account(cur, username)
        parent_account_id = main["account_id"]

        try:
            cur.execute(
                """
                INSERT INTO accounts (username, account_name, parent_account_id)
                VALUES (%s, %s, %s::uuid)
                RETURNING account_id::text
                """,
                (username, account_name, parent_account_id),
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
    if not account_name:
        raise HTTPException(status_code=400, detail="account_name required")

    with db() as conn, conn.cursor() as cur:
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

    dt = parse_tx_datetime(date_str)

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


@app.post("/allocate")
async def allocate_balance(req: Request):
    username = require_user(req)
    data = await req.json()
    target_account_id = data.get("target_account_id")
    amount = int(data.get("amount") or 0)
    date_str = data.get("date")

    if not target_account_id or amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid allocation payload")

    dt = parse_tx_datetime(date_str)

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT account_id::text AS account_id,
                   account_name,
                   parent_account_id IS NULL AS is_main
            FROM accounts
            WHERE username=%s AND account_id=%s::uuid
            """,
            (username, target_account_id),
        )
        target = cur.fetchone()
        if not target:
            raise HTTPException(status_code=404, detail="Account not found")
        if target["is_main"]:
            raise HTTPException(status_code=400, detail="Cannot allocate to main account")

        main = get_main_account(cur, username)
        cur.execute(
            "SELECT account_id::text AS account_id, parent_account_id FROM accounts WHERE username=%s",
            (username,),
        )
        all_accounts = cur.fetchall()
        balances_all = get_account_balances(cur, username, now_utc())
        now_ts = now_utc()
        cur.execute(
            """
            SELECT t.amount
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE a.username=%s
              AND a.parent_account_id IS NOT NULL
              AND t.transaction_name=%s
              AND t.transaction_type='debit'
              AND t.date <= %s
            """,
            (username, "Top Up Balance", now_ts),
        )
        allocated_total = sum(int(t["amount"]) for t in cur.fetchall())
        main_balance = int(balances_all.get(main["account_id"], 0))
        unallocated = main_balance - int(allocated_total)
        total_asset = sum(
            int(balances_all.get(a["account_id"], 0))
            for a in all_accounts
            if a["parent_account_id"] is not None
        ) + unallocated

        if amount > unallocated:
            raise HTTPException(status_code=400, detail="Insufficient unallocated balance")

        cur.execute(
            """
            INSERT INTO transactions (account_id, transaction_type, transaction_name, amount, date)
            VALUES (%s::uuid, 'debit', %s, %s, %s)
            RETURNING transaction_id::text
            """,
            (target_account_id, "Top Up Balance", amount, dt),
        )
        tx_id = cur.fetchone()["transaction_id"]
        conn.commit()

    return {"ok": True, "transaction_id": tx_id}


@app.post("/switch")
async def switch_balance(req: Request):
    username = require_user(req)
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

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT account_id::text AS account_id,
                   account_name,
                   parent_account_id IS NULL AS is_main
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
        if source["is_main"] or target["is_main"]:
            raise HTTPException(status_code=400, detail="Cannot switch with main account")

        balances_all = get_account_balances(cur, username, dt)
        source_balance = int(balances_all.get(source_account_id, 0))
        if amount > source_balance:
            raise HTTPException(status_code=400, detail="Insufficient balance in source account")

        source_name = f"Switching to {target['account_name']}"
        target_name = f"Switching from {source['account_name']}"
        cur.execute(
            """
            INSERT INTO transactions (account_id, transaction_type, transaction_name, amount, date)
            VALUES
              (%s::uuid, 'credit', %s, %s, %s),
              (%s::uuid, 'debit', %s, %s, %s)
            RETURNING transaction_id::text
            """,
            (
                source_account_id,
                source_name,
                amount,
                dt,
                target_account_id,
                target_name,
                amount,
                dt,
            ),
        )
        conn.commit()

    return {"ok": True}


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

    with db() as conn, conn.cursor() as cur:
        rows, summary_accounts, total_asset, unallocated, paging = build_ledger_page(
            cur, username, scope, account_id, from_dt, to_dt, limit, offset, order, q
        )

    return {
        "range": {"from": from_date, "to": to_date},
        "scope": scope,
        "rows": rows,
        "paging": paging,
        "summary": {"accounts": summary_accounts, "total_asset": int(total_asset), "unallocated": int(unallocated)},
    }


@app.get("/summary")
def summary(req: Request, month: str | None = None):
    username = require_user(req)
    if not month:
        month = now_utc().strftime("%Y-%m")
    from_date, to_date, from_dt, to_dt = compute_month_range(month)
    start_cutoff = from_dt - timedelta(milliseconds=1)

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT account_id::text, account_name, parent_account_id::text AS parent_account_id
            FROM accounts
            WHERE username=%s
            """,
            (username,),
        )
        accounts = cur.fetchall()
        if not accounts:
            return {"range": {"from": from_date, "to": to_date}, "accounts": []}

        balances_start = get_account_balances(cur, username, start_cutoff)
        balances_now = get_account_balances(cur, username, now_utc())
        main = next((a for a in accounts if a["parent_account_id"] is None), None)
        if not main:
            raise HTTPException(status_code=400, detail="Main account missing")
        main_id = main["account_id"]
        cur.execute(
            """
            SELECT COALESCE(SUM(t.amount), 0) AS allocated_total
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE a.username=%s
              AND a.parent_account_id IS NOT NULL
              AND t.transaction_name=%s
              AND t.transaction_type='debit'
              AND t.date <= %s
            """,
            (username, "Top Up Balance", start_cutoff),
        )
        allocated_start = int(cur.fetchone()["allocated_total"] or 0)
        main_balance_start = int(balances_start.get(main_id, 0))
        unallocated_start = main_balance_start - allocated_start
        total_asset_start = sum(
            int(balances_start.get(acc["account_id"], 0))
            for acc in accounts
            if acc["parent_account_id"] is not None
        ) + int(unallocated_start)

        cur.execute(
            """
            SELECT COALESCE(SUM(t.amount), 0) AS allocated_total
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE a.username=%s
              AND a.parent_account_id IS NOT NULL
              AND t.transaction_name=%s
              AND t.transaction_type='debit'
              AND t.date <= %s
            """,
            (username, "Top Up Balance", now_utc()),
        )
        allocated_now = int(cur.fetchone()["allocated_total"] or 0)
        main_balance_now = int(balances_now.get(main_id, 0))
        unallocated_now = main_balance_now - allocated_now
        total_asset_now = sum(
            int(balances_now.get(acc["account_id"], 0))
            for acc in accounts
            if acc["parent_account_id"] is not None
        ) + int(unallocated_now)

        cur.execute(
            """
            SELECT t.account_id::text AS account_id,
                   COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE a.username=%s
              AND t.date >= %s
              AND t.date <= %s
            GROUP BY t.account_id
            """,
            (username, from_dt, to_dt),
        )
        totals = {r["account_id"]: r for r in cur.fetchall()}
        cur.execute(
            """
            SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE a.username=%s
              AND t.date >= %s
              AND t.date <= %s
              AND NOT (
                t.transaction_name = %s
                OR t.transaction_name ILIKE %s
                OR t.transaction_name ILIKE %s
              )
            """,
            (username, from_dt, to_dt, "Top Up Balance", "Switching from %", "Switching to %"),
        )
        filtered_totals = cur.fetchone() or {}
        total_in_all = int(filtered_totals.get("total_in") or 0)
        total_out_all = int(filtered_totals.get("total_out") or 0)

    accounts_sorted = sorted(
        accounts,
        key=lambda a: (a["parent_account_id"] is not None, a["account_name"].lower()),
    )
    payload = []
    for acc in accounts_sorted:
        acc_id = acc["account_id"]
        total_row = totals.get(acc_id, {})
        total_in = int(total_row.get("total_in") or 0)
        total_out = int(total_row.get("total_out") or 0)
        payload.append(
            {
                "account_id": acc_id,
                "account_name": acc["account_name"],
                "is_main": acc["parent_account_id"] is None,
                "starting_balance": int(total_asset_start)
                if acc["parent_account_id"] is None
                else int(balances_start.get(acc_id, 0)),
                "current_balance": int(total_in_all - total_out_all)
                if acc["parent_account_id"] is None
                else int(total_in - total_out),
                "total_in": int(total_in_all) if acc["parent_account_id"] is None else total_in,
                "total_out": int(total_out_all) if acc["parent_account_id"] is None else total_out,
            }
        )

    return {"range": {"from": from_date, "to": to_date}, "accounts": payload}


@app.get("/export/preview")
def export_preview(req: Request, day: int, scope: str = "all", account_id: str | None = None):
    username = require_user(req)
    from_date, to_date, from_dt, to_dt = compute_export_range(day)

    with db() as conn, conn.cursor() as cur:
        rows, _, _, _ = build_ledger_data(cur, username, scope, account_id, from_dt, to_dt)

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


@app.get("/export")
def export_ledger(
    req: Request,
    day: int,
    format: str = "pdf",
    scope: str = "all",
    account_id: str | None = None,
    currency: str | None = None,
    fx_rate: str | None = None,
):
    username = require_user(req)
    export_format = (format or "pdf").lower()
    if export_format not in ("pdf", "csv"):
        raise HTTPException(status_code=400, detail="Invalid export format")

    cur_currency, fx = parse_currency(currency, fx_rate)
    from_date, to_date, from_dt, to_dt = compute_export_range(day)

    with db() as conn, conn.cursor() as cur:
        rows, summary_accounts, _, _ = build_ledger_data(cur, username, scope, account_id, from_dt, to_dt)

    account_name = "Total Ledger"
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
            format_amount(debit, cur_currency, fx) if debit else "",
            format_amount(credit, cur_currency, fx) if credit else "",
            format_amount(int(r.get("balance") or 0), cur_currency, fx),
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
        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

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
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.exception_handler(HTTPException)
def http_exc_handler(_, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})
