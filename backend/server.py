import calendar
import csv
import io
import os
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fpdf import FPDF
from passlib.hash import bcrypt
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from starlette.middleware.sessions import SessionMiddleware

DATABASE_URL = os.getenv("DATABASE_URL", "")
SESSION_SECRET = os.getenv("SESSION_SECRET")
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() == "true"
TZ = os.getenv("TZ", "Asia/Jakarta")  # for display in UI only; DB stores timestamptz
SUMMARY_CACHE_TTL = int(os.getenv("SUMMARY_CACHE_TTL", "30"))
MONTH_SUMMARY_TTL = int(os.getenv("MONTH_SUMMARY_TTL", "60"))
LOGIN_RATE_LIMIT = int(os.getenv("LOGIN_RATE_LIMIT", "10"))
LOGIN_RATE_WINDOW = int(os.getenv("LOGIN_RATE_WINDOW", "300"))
LOGIN_USER_RATE_LIMIT = int(os.getenv("LOGIN_USER_RATE_LIMIT", "5"))
REGISTER_RATE_LIMIT = int(os.getenv("REGISTER_RATE_LIMIT", "5"))
REGISTER_RATE_WINDOW = int(os.getenv("REGISTER_RATE_WINDOW", "900"))
PASSWORD_MIN_LEN = int(os.getenv("PASSWORD_MIN_LEN", "8"))
USERNAME_RE = re.compile(r"^[a-zA-Z0-9._-]{3,32}$")
DB_POOL_MIN = max(1, int(os.getenv("DB_POOL_MIN", "1")))
DB_POOL_MAX = max(DB_POOL_MIN, int(os.getenv("DB_POOL_MAX", "10")))
INVITE_CODE = (os.getenv("INVITE_CODE") or "").strip()

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")
if not SESSION_SECRET:
    raise RuntimeError("SESSION_SECRET is required")

DB_POOL = ConnectionPool(
    DATABASE_URL,
    min_size=DB_POOL_MIN,
    max_size=DB_POOL_MAX,
    open=False,
    kwargs={"row_factory": dict_row},
)

app = FastAPI()
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="ledger_session",
    same_site="strict",
    https_only=COOKIE_SECURE,
)

_CACHE: dict[str, tuple[float, Any]] = {}
_RATE_LIMIT: dict[str, list[float]] = {}


@app.on_event("startup")
def open_db_pool() -> None:
    DB_POOL.open()


@app.on_event("shutdown")
def close_db_pool() -> None:
    DB_POOL.close()


def cache_get(key: str) -> Any | None:
    payload = _CACHE.get(key)
    if not payload:
        return None
    expires_at, value = payload
    if time.time() > expires_at:
        _CACHE.pop(key, None)
        return None
    return value


def cache_set(key: str, value: Any, ttl: int) -> None:
    _CACHE[key] = (time.time() + max(1, ttl), value)


def invalidate_user_cache(username: str) -> None:
    prefix = f"{username}:"
    for key in list(_CACHE.keys()):
        if key.startswith(prefix):
            _CACHE.pop(key, None)


def get_client_ip(req: Request) -> str:
    forwarded = req.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = req.headers.get("x-real-ip", "")
    if real_ip:
        return real_ip.strip()
    if req.client:
        return req.client.host
    return "unknown"


def rate_limit_exceeded(key: str, limit: int, window_seconds: int) -> bool:
    now = time.time()
    cutoff = now - window_seconds
    events = _RATE_LIMIT.get(key, [])
    events = [ts for ts in events if ts >= cutoff]
    if len(events) >= limit:
        _RATE_LIMIT[key] = events
        return True
    events.append(now)
    if events:
        _RATE_LIMIT[key] = events
    else:
        _RATE_LIMIT.pop(key, None)
    return False


def db():
    return DB_POOL.connection()


def now_utc():
    return datetime.now(timezone.utc)


def require_user(req: Request) -> str:
    u = (req.session or {}).get("username")
    if not u:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return u


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


def build_daily_series(
    from_date: str, to_date: str, rows: list[dict[str, Any]]
) -> list[dict[str, int | str]]:
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
        series.append(
            {
                "date": key,
                "total_in": total_in,
                "total_out": total_out,
                "net": int(total_in - total_out),
            }
        )
        cursor += timedelta(days=1)
    return series


def build_weekly_series(
    from_date: str, to_date: str, daily: list[dict[str, int | str]]
) -> list[dict[str, int | str]]:
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
        return now_utc()
    try:
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return parse_date_utc(date_str, end_of_day=False)


def get_balance_before(
    cur,
    account_id: str,
    before_dt: datetime,
    exclude_tx_ids: list[str] | None = None,
) -> int:
    sql = """
        SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END), 0) AS balance
        FROM transactions t
        WHERE t.account_id=%s::uuid AND t.date < %s
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
        WHERE t.account_id=%s::uuid AND t.date >= %s
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
    to_dt: datetime,
) -> tuple[list[dict[str, Any]], int]:
    balances_all = get_account_balances(cur, username, to_dt)
    summary_accounts = [
        {
            "account_id": aid,
            "account_name": acc_by_id[aid]["account_name"],
            "balance": int(balances_all.get(aid, 0)),
        }
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
               t.transfer_id::text AS transfer_id
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
            cache_set(summary_key, (summary_accounts, total_asset), SUMMARY_CACHE_TTL)

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
        base_balance = sum(int(balance.get(aid, 0)) for aid in acc_by_id.keys())
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

    search_pattern = build_search_pattern(query)
    search_sql = ""
    search_params: list[Any] = []
    if search_pattern:
        search_sql = "WHERE transaction_name ILIKE %s"
        search_params.append(search_pattern)

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
                   t.transfer_id::text AS transfer_id,
                   SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE -t.amount END)
                     OVER (ORDER BY t.date ASC, t.transaction_id ASC) AS running_delta
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {" AND ".join(base_filters)}
        )
        SELECT transaction_id, account_id, account_name, transaction_type, transaction_name, amount, date, is_transfer, transfer_id, running_delta
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
                "is_transfer": bool(r.get("is_transfer")),
                "transfer_id": r.get("transfer_id"),
            }
        )

    paging = {"limit": limit, "offset": offset, "has_more": has_more, "next_offset": offset + len(rows)}
    return rows, summary_accounts, total_asset, paging


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/auth/register")
async def register(req: Request):
    # Invite-only registration.
    data = await req.json()
    client_ip = get_client_ip(req)
    if rate_limit_exceeded(f"register:ip:{client_ip}", REGISTER_RATE_LIMIT, REGISTER_RATE_WINDOW):
        raise HTTPException(status_code=429, detail="Too many registration attempts. Try again later.")
    if not INVITE_CODE:
        raise HTTPException(status_code=403, detail="Registration disabled")
    invite_code = (data.get("invite_code") or "").strip()
    if invite_code != INVITE_CODE:
        raise HTTPException(status_code=403, detail="Invalid invite code")
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password required")
    if not USERNAME_RE.fullmatch(username):
        raise HTTPException(
            status_code=400,
            detail="Invalid username. Use 3-32 chars: letters, numbers, dot, underscore, or hyphen.",
        )
    if len(password) < PASSWORD_MIN_LEN:
        raise HTTPException(status_code=400, detail=f"Password too short (min {PASSWORD_MIN_LEN})")
    if len(password.encode("utf-8")) > 72:
        raise HTTPException(status_code=400, detail="Password too long (max 72 bytes)")
    full_name = (data.get("full_name") or "").strip() or username

    pw_hash = bcrypt.hash(password)

    with db() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                "INSERT INTO users (username, password_hash, full_name) VALUES (%s, %s, %s)",
                (username, pw_hash, full_name),
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
    client_ip = get_client_ip(req)
    if rate_limit_exceeded(f"login:ip:{client_ip}", LOGIN_RATE_LIMIT, LOGIN_RATE_WINDOW):
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")
    if rate_limit_exceeded(f"login:user:{username}", LOGIN_USER_RATE_LIMIT, LOGIN_RATE_WINDOW):
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")

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
                   a.account_name
            FROM accounts a
            WHERE a.username=%s
            ORDER BY a.account_name
            """,
            (username,),
        )
        return {"accounts": cur.fetchall()}


@app.post("/accounts")
async def create_account(req: Request):
    username = require_user(req)
    data = await req.json()
    account_name = (data.get("account_name") or "").strip()
    initial_balance_raw = data.get("initial_balance", 0)
    try:
        initial_balance = int(initial_balance_raw or 0)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid initial balance")
    if not account_name:
        raise HTTPException(status_code=400, detail="account_name required")
    if initial_balance < 0:
        raise HTTPException(status_code=400, detail="initial balance must be >= 0")

    with db() as conn, conn.cursor() as cur:
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
        except Exception:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Account name already exists")

    invalidate_user_cache(username)
    return {"ok": True, "account_id": account_id}


@app.get("/budgets")
def list_budgets(req: Request, month: str | None = None):
    username = require_user(req)
    if not month:
        month = now_utc().strftime("%Y-%m")
    parse_month(month)
    with db() as conn, conn.cursor() as cur:
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


@app.post("/budgets")
async def upsert_budget(req: Request):
    username = require_user(req)
    data = await req.json()
    account_id = data.get("account_id")
    month = data.get("month")
    amount = int(data.get("amount") or 0)
    if not account_id or not month:
        raise HTTPException(status_code=400, detail="account_id and month required")
    parse_month(month)
    if amount < 0:
        raise HTTPException(status_code=400, detail="amount must be >= 0")

    with db() as conn, conn.cursor() as cur:
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


@app.put("/budgets/{budget_id}")
async def update_budget(budget_id: str, req: Request):
    username = require_user(req)
    data = await req.json()
    amount = int(data.get("amount") or 0)
    if amount < 0:
        raise HTTPException(status_code=400, detail="amount must be >= 0")

    with db() as conn, conn.cursor() as cur:
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


@app.delete("/budgets/{budget_id}")
def delete_budget(budget_id: str, req: Request):
    username = require_user(req)
    with db() as conn, conn.cursor() as cur:
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

    invalidate_user_cache(username)
    return {"ok": True}


@app.delete("/accounts/{account_id}")
def delete_account(account_id: str, req: Request):
    username = require_user(req)
    with db() as conn, conn.cursor() as cur:
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


@app.get("/switch/{transfer_id}")
def get_switch(transfer_id: str, req: Request):
    username = require_user(req)
    with db() as conn, conn.cursor() as cur:
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
            WHERE t.transfer_id=%s::uuid AND a.username=%s
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


@app.put("/switch/{transfer_id}")
async def update_switch(transfer_id: str, req: Request):
    username = require_user(req)
    data = await req.json()
    with db() as conn, conn.cursor() as cur:
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
            WHERE t.transfer_id=%s::uuid AND a.username=%s
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
            WHERE transaction_id=%s::uuid AND transfer_id=%s::uuid
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
        cur.execute(
            """
            UPDATE transactions
            SET account_id=%s::uuid,
                transaction_type='debit',
                transaction_name=%s,
                amount=%s,
                date=%s,
                is_transfer=true
            WHERE transaction_id=%s::uuid AND transfer_id=%s::uuid
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
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@app.delete("/switch/{transfer_id}")
def delete_switch(transfer_id: str, req: Request):
    username = require_user(req)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS account_id,
                   t.date
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE t.transfer_id=%s::uuid AND a.username=%s
            """,
            (transfer_id, username),
        )
        rows = cur.fetchall()
        if len(rows) != 2:
            raise HTTPException(status_code=404, detail="Switch not found")

        affected: dict[str, dict[str, Any]] = {}
        for row in rows:
            acc = row["account_id"]
            affected.setdefault(acc, {"exclude": [], "dates": [], "new": []})
            affected[acc]["exclude"].append(row["transaction_id"])
            affected[acc]["dates"].append(row["date"])

        for acc_id, payload in affected.items():
            effective_from = min(payload["dates"])
            ensure_account_non_negative(
                cur,
                acc_id,
                effective_from,
                [],
                exclude_tx_ids=payload["exclude"],
            )

        cur.execute(
            "DELETE FROM transactions WHERE transfer_id=%s::uuid",
            (transfer_id,),
        )
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@app.put("/transactions/{transaction_id}")
async def update_tx(transaction_id: str, req: Request):
    username = require_user(req)
    data = await req.json()

    with db() as conn, conn.cursor() as cur:
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
            WHERE t.transaction_id=%s::uuid AND a.username=%s
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

        if new_account_id != old_account_id:
            cur.execute(
                "SELECT 1 FROM accounts WHERE username=%s AND account_id=%s::uuid",
                (username, new_account_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=400, detail="Invalid account_id")
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
            WHERE transaction_id=%s::uuid
            RETURNING transaction_id
            """,
            (new_account_id, new_type, new_name, new_amount, new_date, transaction_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Transaction not found")
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@app.delete("/transactions/{transaction_id}")
def delete_tx(transaction_id: str, req: Request):
    username = require_user(req)
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.transaction_id::text AS transaction_id,
                   t.account_id::text AS account_id,
                   t.date,
                   t.is_transfer
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE t.transaction_id=%s::uuid AND a.username=%s
            """,
            (transaction_id, username)
        )
        tx = cur.fetchone()
        if not tx:
            raise HTTPException(status_code=404, detail="Transaction not found")
        if tx.get("is_transfer"):
            raise HTTPException(status_code=400, detail="Use switch endpoints to delete transfers")

        ensure_account_non_negative(cur, tx["account_id"], tx["date"], [], exclude_tx_ids=[transaction_id])

        cur.execute(
            """
            DELETE FROM transactions
            WHERE transaction_id=%s::uuid
            RETURNING transaction_id
            """,
            (transaction_id,),
        )
        conn.commit()
    invalidate_user_cache(username)
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



@app.get("/summary")
def summary(req: Request, month: str | None = None):
    username = require_user(req)
    if not month:
        month = now_utc().strftime("%Y-%m")
    parse_month(month)
    cache_key = f"{username}:summary:{month}"
    cached = cache_get(cache_key)
    if cached:
        return cached
    with db() as conn, conn.cursor() as cur:
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
                "accounts": [],
            }
            cache_set(cache_key, payload, MONTH_SUMMARY_TTL)
            return payload

        balances_start = get_account_balances(cur, username, start_cutoff)
        balances_end = get_account_balances(cur, username, to_dt)

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
        "accounts": payload_accounts,
    }
    cache_set(cache_key, payload, MONTH_SUMMARY_TTL)
    return payload


@app.get("/analysis")
def analysis(req: Request, month: str | None = None):
    username = require_user(req)
    if not month:
        month = now_utc().strftime("%Y-%m")
    parse_month(month)
    cache_key = f"{username}:analysis:{month}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    with db() as conn, conn.cursor() as cur:
        payday_day, payday_source, override_day = get_payday_day(cur, username, month)
        default_day = get_default_payday_day(cur, username)
        prev_day, _, _ = get_payday_day(cur, username, prev_month_str(month))
        from_date, to_date, from_dt, to_dt = compute_month_range(month, payday_day, prev_day)

        base_filters = ["a.username=%s", "t.date >= %s", "t.date <= %s"]
        params: list[Any] = [username, from_dt, to_dt]

        cur.execute(
            f"""
            SELECT COALESCE(SUM(CASE WHEN t.transaction_type='debit' THEN t.amount ELSE 0 END), 0) AS total_in,
                   COALESCE(SUM(CASE WHEN t.transaction_type='credit' THEN t.amount ELSE 0 END), 0) AS total_out
            FROM transactions t
            JOIN accounts a ON a.account_id=t.account_id
            WHERE {" AND ".join(base_filters)}
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
            GROUP BY t.account_id, a.account_name
            ORDER BY total_out DESC, a.account_name ASC
            """,
            params,
        )
        categories_raw = cur.fetchall()

    categories = [
        {
            "account_id": r.get("account_id"),
            "account_name": r.get("account_name"),
            "total_in": int(r.get("total_in") or 0),
            "total_out": int(r.get("total_out") or 0),
            "net": int(r.get("total_in") or 0) - int(r.get("total_out") or 0),
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
        "totals": {"total_in": total_in, "total_out": total_out, "net": int(total_in - total_out)},
        "daily": daily_series,
        "weekly": weekly_series,
        "categories": categories,
    }
    cache_set(cache_key, payload, MONTH_SUMMARY_TTL)
    return payload


@app.get("/payday")
def get_payday(req: Request, month: str | None = None):
    username = require_user(req)
    if not month:
        month = now_utc().strftime("%Y-%m")
    parse_month(month)
    with db() as conn, conn.cursor() as cur:
        payday_day, payday_source, override_day = get_payday_day(cur, username, month)
        default_day = get_default_payday_day(cur, username)
    return {
        "month": month,
        "day": payday_day,
        "source": payday_source,
        "default_day": default_day,
        "override_day": override_day,
    }


@app.put("/payday")
async def set_payday(req: Request):
    username = require_user(req)
    data = await req.json()
    month = data.get("month")
    day_val = data.get("day")
    clear_override = bool(data.get("clear_override"))

    if month:
        parse_month(month)
        if clear_override:
            with db() as conn, conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM payday_overrides WHERE username=%s AND month=%s",
                    (username, month),
                )
                conn.commit()
        else:
            try:
                day = int(day_val)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid payday day")
            if day < 1 or day > 31:
                raise HTTPException(status_code=400, detail="Payday day must be between 1 and 31")
            with db() as conn, conn.cursor() as cur:
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
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid payday day")
        if day < 1 or day > 31:
            raise HTTPException(status_code=400, detail="Payday day must be between 1 and 31")
        with db() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET default_payday_day=%s WHERE username=%s",
                (day, username),
            )
            conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@app.get("/export/preview")
def export_preview(req: Request, day: int, scope: str = "all", account_id: str | None = None):
    username = require_user(req)
    from_date, to_date, from_dt, to_dt = compute_export_range(day)

    with db() as conn, conn.cursor() as cur:
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
        rows, summary_accounts, _ = build_ledger_data(cur, username, scope, account_id, from_dt, to_dt)

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
