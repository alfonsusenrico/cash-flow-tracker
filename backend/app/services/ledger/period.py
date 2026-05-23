"""Period and date utilities — payday range computation, timezone helpers."""
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from app.core.config import settings

try:
    APP_TZ = ZoneInfo(settings.tz)
except Exception:
    APP_TZ = timezone.utc


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_local() -> datetime:
    return now_utc().astimezone(APP_TZ)


def current_month_local() -> str:
    return now_local().strftime("%Y-%m")


def local_day_start_utc(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=APP_TZ).astimezone(timezone.utc)


def local_day_end_utc(day: date) -> datetime:
    return (local_day_start_utc(day + timedelta(days=1)) - timedelta(milliseconds=1)).replace(microsecond=0)


def local_date_iso(dt: datetime) -> str:
    return dt.astimezone(APP_TZ).date().isoformat()


def parse_date_utc(date_str: str, end_of_day: bool = False) -> datetime:
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


def parse_month(month: str) -> tuple[int, int]:
    try:
        dt = datetime.strptime(month, "%Y-%m")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid month format, expected YYYY-MM")
    return dt.year, dt.month


def prev_month_str(month: str) -> str:
    year, month_num = parse_month(month)
    prev = month_num - 1
    prev_year = year
    if prev == 0:
        prev = 12
        prev_year -= 1
    return f"{prev_year:04d}-{prev:02d}"


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
    return from_date, to_date, parse_date_utc(from_date), parse_date_utc(to_date, end_of_day=True)


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
    end_date = min(payday - timedelta(days=1), now_utc().date())
    from_date = prev_payday.isoformat()
    to_date = end_date.isoformat()
    return from_date, to_date, parse_date_utc(from_date), parse_date_utc(to_date, end_of_day=True)


def compute_dynamic_month_range(
    cur: Any,
    username: str,
    month: str,
    payday_day: int,
    prev_payday_day: int | None = None,
) -> tuple[str, str, datetime, datetime]:
    year, month_num = parse_month(month)
    prev_month = month_num - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1
    prev_day = prev_payday_day if prev_payday_day is not None else payday_day
    default_start = datetime(prev_year, prev_month, clamp_day(prev_year, prev_month, prev_day)).date()
    month_start = datetime(year, month_num, 1).date()
    next_month_start = datetime(year + 1, 1, 1).date() if month_num == 12 else datetime(year, month_num + 1, 1).date()

    def pick_anchor(window_from: datetime, window_to: datetime, *, order: str) -> dict[str, Any] | None:
        # Prefer payroll-source accounts first, fall back to any cycle topup.
        for extra in ("AND a.is_payroll_source = TRUE", ""):
            cur.execute(
                f"""
                SELECT t.date FROM transactions t
                JOIN accounts a ON a.account_id=t.account_id
                WHERE a.username=%s AND t.deleted_at IS NULL
                  AND t.is_cycle_topup = TRUE AND t.transaction_type = 'debit'
                  AND t.is_transfer = FALSE
                  AND t.date >= %s AND t.date < %s
                  {extra}
                ORDER BY t.date {order} LIMIT 1
                """,
                (username, window_from, window_to),
            )
            row = cur.fetchone()
            if row:
                return row
        return None

    row_start = pick_anchor(
        local_day_start_utc(month_start - timedelta(days=7)),
        local_day_start_utc(month_start + timedelta(days=8)),
        order="DESC",
    )
    from_dt = row_start["date"] if row_start else local_day_start_utc(default_start)

    row_end = pick_anchor(
        local_day_start_utc(next_month_start - timedelta(days=7)),
        local_day_start_utc(next_month_start + timedelta(days=8)),
        order="ASC",
    )
    to_dt = (row_end["date"] - timedelta(microseconds=1)) if row_end else now_utc().replace(microsecond=0)
    if to_dt < from_dt:
        to_dt = from_dt

    return local_date_iso(from_dt), local_date_iso(to_dt), from_dt, to_dt
