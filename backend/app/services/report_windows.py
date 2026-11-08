"""Report windows: the confirmed salary cycle, rolling 30/90 days and the calendar month.

The dashboard and the ledger use the same windows so a figure and the ledger rows behind it always
cover the same days. A window is a whole number of local days, returned as UTC bounds.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.i18n import t
from app.services.salary_cycles import SalaryCycle, load_user_cycle

TIMEFRAMES: tuple[str, ...] = ("cycle", "30d", "90d", "month")
TIMEFRAME_PATTERN = "^(" + "|".join(TIMEFRAMES) + ")$"
PERIOD_PATTERN = "^(cycle|month)$"

Window = tuple[datetime, datetime, date, date, str]


def utc_bounds(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    tz = ZoneInfo(settings.tz)
    return (
        datetime.combine(start_date, time.min, tzinfo=tz).astimezone(timezone.utc),
        datetime.combine(end_date, time.max, tzinfo=tz).astimezone(timezone.utc),
    )


def short_local_date(language: str, day: date) -> str:
    """ "25 Okt" / "25 Oct". Month names come from the catalogue, never from the host locale (D2)."""
    return t(language, "periods.short_date", day=day.day, month=t(language, f"periods.month_short.{day.month}"))


def month_label(language: str, year: int, month: int) -> str:
    """ "Oktober 2026" / "October 2026"."""
    return t(language, "periods.month_year", month=t(language, f"periods.month.{month}"), year=year)


def cycle_range_label(language: str, start: date, end: date) -> str:
    return t(language, "periods.cycle_range", start=short_local_date(language, start), end=short_local_date(language, end))


def shift_month(year: int, month: int, offset: int) -> tuple[int, int]:
    index = year * 12 + (month - 1) + offset
    return index // 12, index % 12 + 1


def month_window(target_date: date, month_offset: int, *, language: str) -> tuple[date, date, str]:
    year, month = shift_month(target_date.year, target_date.month, month_offset)
    start_date = date(year, month, 1)
    next_year, next_month = shift_month(year, month, 1)
    end_date = date(next_year, next_month, 1) - timedelta(days=1)
    return start_date, end_date, month_label(language, year, month)


def get_timeframe_window(
    payday_day: int,
    timeframe: str = "cycle",
    ref_date: date | None = None,
    cycle_offset: int = 0,
    *,
    user_id: str | None = None,
    cycle: SalaryCycle | None = None,
    language: str,
) -> Window:
    """Returns (start_utc, end_utc, start_date, end_date, label), the label in `language` (the account's).

    `cycle_offset` steps whole payday cycles for `cycle` and whole calendar months for `month`; the
    rolling windows ignore it. A caller that already resolved the owner's cycle passes it as `cycle`,
    so its figures and its window come from the same receipts.
    """
    tz = ZoneInfo(settings.tz)
    target_date = ref_date or datetime.now(tz).date()

    if timeframe == "30d":
        start_date = target_date - timedelta(days=29)
        end_date = target_date
        label = t(language, "periods.last_30")
    elif timeframe == "90d":
        start_date = target_date - timedelta(days=89)
        end_date = target_date
        label = t(language, "periods.last_90")
    elif timeframe == "month":
        start_date, end_date, label = month_window(target_date, cycle_offset, language=language)
    elif user_id is not None:  # Observed salary cycle; no future payday is assumed.
        now = datetime.now(timezone.utc)
        if cycle is None:
            cycle = load_user_cycle(user_id, offset=cycle_offset, as_of=now)
        if cycle.start is not None:
            start_utc = cycle.start.astimezone(timezone.utc)
            end_utc = min(now, cycle.end_exclusive - timedelta(microseconds=1)) if cycle.end_exclusive else now
            start_date = start_utc.astimezone(tz).date()
            end_date = end_utc.astimezone(tz).date()
            if cycle.end_exclusive:
                label = cycle_range_label(language, start_date, end_date)
            else:
                label = t(language, "periods.cycle_ongoing", start=short_local_date(language, start_date))
            return start_utc, end_utc, start_date, end_date, label
        start_date, end_date, fallback_month = month_window(target_date, cycle_offset, language=language)
        start_utc, end_utc = utc_bounds(start_date, end_date)
        end_utc = min(end_utc, now)
        key = "periods.month_cycle_review" if cycle.status == "needs_review" else "periods.month_cycle_unavailable"
        return start_utc, end_utc, start_date, end_date, t(language, key, month=fallback_month)
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
        label = t(language, "periods.legacy_range", start=start_date.strftime('%d %b'), end=end_date.strftime('%d %b'))

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
    language: str,
) -> Window:
    if timeframe == "30d":
        prev_start_date = start_date - timedelta(days=30)
        prev_end_date = start_date - timedelta(days=1)
        prev_start_utc, prev_end_utc = utc_bounds(prev_start_date, prev_end_date)
        return prev_start_utc, prev_end_utc, prev_start_date, prev_end_date, t(language, "periods.previous_30")
    if timeframe == "90d":
        prev_start_date = start_date - timedelta(days=90)
        prev_end_date = start_date - timedelta(days=1)
        prev_start_utc, prev_end_utc = utc_bounds(prev_start_date, prev_end_date)
        return prev_start_utc, prev_end_utc, prev_start_date, prev_end_date, t(language, "periods.previous_90")
    if timeframe == "month":
        return get_timeframe_window(payday_day, "month", cycle_offset=cycle_offset - 1, language=language)
    if user_id is not None:
        now = datetime.now(timezone.utc)
        current = load_user_cycle(user_id, offset=cycle_offset, as_of=now)
        previous = load_user_cycle(user_id, offset=cycle_offset - 1, as_of=now)
        if current.start is not None and previous.start is None:
            today = now.astimezone(ZoneInfo(settings.tz)).date()
            return now, now - timedelta(microseconds=1), today, today, t(language, "periods.previous_cycle_missing")
    return get_timeframe_window(payday_day, "cycle", cycle_offset=cycle_offset - 1, user_id=user_id, language=language)
