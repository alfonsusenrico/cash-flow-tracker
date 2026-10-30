"""Report windows: the confirmed salary cycle, rolling 30/90 days and the calendar month.

The dashboard and the ledger use the same windows so a figure and the ledger rows behind it always
cover the same days. A window is a whole number of local days, returned as UTC bounds.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.services.salary_cycles import load_user_cycle

TIMEFRAMES: tuple[str, ...] = ("cycle", "30d", "90d", "month")
TIMEFRAME_PATTERN = "^(" + "|".join(TIMEFRAMES) + ")$"
PERIOD_PATTERN = "^(cycle|month)$"

MONTH_NAMES_ID = (
    "Januari", "Februari", "Maret", "April", "Mei", "Juni",
    "Juli", "Agustus", "September", "Oktober", "November", "Desember",
)

Window = tuple[datetime, datetime, date, date, str]


def utc_bounds(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    tz = ZoneInfo(settings.tz)
    return (
        datetime.combine(start_date, time.min, tzinfo=tz).astimezone(timezone.utc),
        datetime.combine(end_date, time.max, tzinfo=tz).astimezone(timezone.utc),
    )


def short_local_date(day: date) -> str:
    return f"{day.day} {MONTH_NAMES_ID[day.month - 1][:3]}"


def shift_month(year: int, month: int, offset: int) -> tuple[int, int]:
    index = year * 12 + (month - 1) + offset
    return index // 12, index % 12 + 1


def month_window(target_date: date, month_offset: int) -> tuple[date, date, str]:
    year, month = shift_month(target_date.year, target_date.month, month_offset)
    start_date = date(year, month, 1)
    next_year, next_month = shift_month(year, month, 1)
    end_date = date(next_year, next_month, 1) - timedelta(days=1)
    return start_date, end_date, f"{MONTH_NAMES_ID[month - 1]} {year}"


def get_timeframe_window(
    payday_day: int,
    timeframe: str = "cycle",
    ref_date: date | None = None,
    cycle_offset: int = 0,
    *,
    user_id: str | None = None,
) -> Window:
    """Returns (start_utc, end_utc, start_date, end_date, label).

    `cycle_offset` steps whole payday cycles for `cycle` and whole calendar months for `month`; the
    rolling windows ignore it.
    """
    tz = ZoneInfo(settings.tz)
    target_date = ref_date or datetime.now(tz).date()

    if timeframe == "30d":
        start_date = target_date - timedelta(days=29)
        end_date = target_date
        label = "30 Hari Terakhir"
    elif timeframe == "90d":
        start_date = target_date - timedelta(days=89)
        end_date = target_date
        label = "90 Hari Terakhir"
    elif timeframe == "month":
        start_date, end_date, label = month_window(target_date, cycle_offset)
    elif user_id is not None:  # Observed salary cycle; no future payday is assumed.
        now = datetime.now(timezone.utc)
        cycle = load_user_cycle(user_id, offset=cycle_offset, as_of=now)
        if cycle.start is not None:
            start_utc = cycle.start.astimezone(timezone.utc)
            end_utc = min(now, cycle.end_exclusive - timedelta(microseconds=1)) if cycle.end_exclusive else now
            start_date = start_utc.astimezone(tz).date()
            end_date = end_utc.astimezone(tz).date()
            if cycle.end_exclusive:
                label = f"Siklus {short_local_date(start_date)} – {short_local_date(end_date)}"
            else:
                label = f"Siklus sejak {short_local_date(start_date)} · berlanjut"
            return start_utc, end_utc, start_date, end_date, label
        start_date, end_date, month_label = month_window(target_date, cycle_offset)
        start_utc, end_utc = utc_bounds(start_date, end_date)
        end_utc = min(end_utc, now)
        reason = 'perlu ditinjau' if cycle.status == 'needs_review' else 'belum tersedia'
        return start_utc, end_utc, start_date, end_date, f"{month_label} · siklus gaji {reason}"
    else:  # Legacy in-process callers without a user still use the historical fixed-day helper.
        from app.routers.pulse import get_cycle_window

        curr = target_date
        step = -1 if cycle_offset < 0 else 1
        for _ in range(abs(cycle_offset)):
            c_start, c_end, _, _ = get_cycle_window(payday_day, curr)
            if step < 0:
                curr = c_start.astimezone(tz).date() - timedelta(days=1)
            else:
                curr = c_end.astimezone(tz).date() + timedelta(days=1)

        c_start, c_end, _, _ = get_cycle_window(payday_day, curr)
        start_date = c_start.astimezone(tz).date()
        end_date = c_end.astimezone(tz).date()
        label = f"Periode {start_date.strftime('%d %b')} - {end_date.strftime('%d %b')}"

    start_utc, end_utc = utc_bounds(start_date, end_date)
    now = datetime.now(timezone.utc)
    today = now.astimezone(tz).date()
    if start_date <= today <= end_date:
        end_utc = min(end_utc, now)
    return start_utc, end_utc, start_date, end_date, label


def get_previous_timeframe_window(
    payday_day: int,
    timeframe: str,
    start_date: date,
    end_date: date,
    cycle_offset: int,
    *,
    user_id: str | None = None,
) -> Window:
    if timeframe == "30d":
        prev_start_date = start_date - timedelta(days=30)
        prev_end_date = start_date - timedelta(days=1)
        prev_start_utc, prev_end_utc = utc_bounds(prev_start_date, prev_end_date)
        return prev_start_utc, prev_end_utc, prev_start_date, prev_end_date, "30 Hari Sebelumnya"
    if timeframe == "90d":
        prev_start_date = start_date - timedelta(days=90)
        prev_end_date = start_date - timedelta(days=1)
        prev_start_utc, prev_end_utc = utc_bounds(prev_start_date, prev_end_date)
        return prev_start_utc, prev_end_utc, prev_start_date, prev_end_date, "90 Hari Sebelumnya"
    if timeframe == "month":
        return get_timeframe_window(payday_day, "month", cycle_offset=cycle_offset - 1)
    if user_id is not None:
        now = datetime.now(timezone.utc)
        current = load_user_cycle(user_id, offset=cycle_offset, as_of=now)
        previous = load_user_cycle(user_id, offset=cycle_offset - 1, as_of=now)
        if current.start is not None and previous.start is None:
            today = now.astimezone(ZoneInfo(settings.tz)).date()
            return now, now - timedelta(microseconds=1), today, today, "Siklus sebelumnya belum tersedia"
    return get_timeframe_window(payday_day, "cycle", cycle_offset=cycle_offset - 1, user_id=user_id)
