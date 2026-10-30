"""Observed reporting periods: confirmed salary cycles, else calendar months, in the application timezone.

A period is complete once it has ended. It is used only when it began on or after the owner's first
recorded transaction, so months before tracking started never count as zero spending.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.services.report_windows import MONTH_NAMES_ID, shift_month, short_local_date
from app.services.salary_receipts import receipts as confirmed_receipts

COMPLETED_PERIOD_LIMIT = 6


@dataclass(frozen=True)
class ObservedPeriod:
    kind: str  # "cycle" or "month"
    start: datetime  # UTC, inclusive
    end: datetime  # UTC, exclusive; for the current period, the instant it was observed
    label: str
    complete: bool

    @property
    def last_instant(self) -> datetime:
        """Inclusive upper bound for queries and ledger links."""
        return self.end - timedelta(microseconds=1)

    def describe(self) -> dict[str, Any]:
        zone = ZoneInfo(settings.tz)
        first_day = self.start.astimezone(zone).date()
        last_day = self.last_instant.astimezone(zone).date()
        return {
            "kind": self.kind,
            "label": self.label,
            "start": self.start.isoformat(),
            "end": self.last_instant.isoformat(),
            "start_date": first_day.isoformat(),
            "end_date": last_day.isoformat(),
            "days": (last_day - first_day).days + 1,
            "complete": self.complete,
        }


@dataclass(frozen=True)
class PeriodHistory:
    completed: list[ObservedPeriod]  # newest first
    current: ObservedPeriod | None
    cycle_status: str  # "confirmed", "not_configured" or "needs_review"


def _local_midnight(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=ZoneInfo(settings.tz)).astimezone(timezone.utc)


def _month_period(year: int, month: int, *, now: datetime) -> ObservedPeriod:
    start = _local_midnight(date(year, month, 1))
    next_year, next_month = shift_month(year, month, 1)
    end = _local_midnight(date(next_year, next_month, 1))
    complete = end <= now
    return ObservedPeriod("month", start, end if complete else now, f"{MONTH_NAMES_ID[month - 1]} {year}", complete)


def _cycle_label(start: datetime, last: datetime, *, open_cycle: bool) -> str:
    zone = ZoneInfo(settings.tz)
    first_day = short_local_date(start.astimezone(zone).date())
    if open_cycle:
        return f"Siklus sejak {first_day}"
    return f"Siklus {first_day} – {short_local_date(last.astimezone(zone).date())}"


def _first_recorded_day(cur, user_id: str) -> date | None:
    cur.execute("SELECT MIN(date) AS first FROM transactions WHERE user_id = %s", (user_id,))
    row = cur.fetchone()
    first = row["first"] if row else None
    return first.astimezone(ZoneInfo(settings.tz)).date() if first else None


def _salary_boundaries(cur, user_id: str, now: datetime) -> tuple[list[datetime], str]:
    instants = sorted(row["date"] for row in confirmed_receipts(cur.connection, user_id) if row["date"] <= now)
    if not instants:
        return [], "not_configured"
    zone = ZoneInfo(settings.tz)
    days = [instant.astimezone(zone).date() for instant in instants]
    if len(days) != len(set(days)):
        # Same-day receipts need owner review before they may bound a cycle.
        return [], "needs_review"
    return instants, "confirmed"


def _months_before(limit_end: datetime, *, now: datetime, first_day: date, count: int) -> list[ObservedPeriod]:
    """Complete calendar months ending on or before `limit_end`, newest first, after tracking began."""
    zone = ZoneInfo(settings.tz)
    anchor = limit_end.astimezone(zone).date()
    year, month = anchor.year, anchor.month
    months: list[ObservedPeriod] = []
    # Walk back from the month containing `limit_end`; it qualifies only if it ended by then.
    for _ in range(count + 1):
        period = _month_period(year, month, now=now)
        if period.complete and period.end <= limit_end:
            if period.start.astimezone(zone).date() < first_day:
                break
            months.append(period)
            if len(months) == count:
                break
        year, month = shift_month(year, month, -1)
    return months


def load_period_history(
    cur, user_id: str, *, now: datetime | None = None, limit: int = COMPLETED_PERIOD_LIMIT, basis: str = "auto",
) -> PeriodHistory:
    """Completed periods (newest first) and the current one.

    `basis="auto"` uses closed confirmed salary cycles, then complete calendar months before the first
    confirmed salary; `basis="month"` uses calendar months only.
    """
    now = now or datetime.now(timezone.utc)
    zone = ZoneInfo(settings.tz)
    first_day = _first_recorded_day(cur, user_id)
    today = now.astimezone(zone).date()
    boundaries, cycle_status = ([], "not_configured") if basis == "month" else _salary_boundaries(cur, user_id, now)
    if first_day is None:
        return PeriodHistory([], None, cycle_status)
    if boundaries:
        current = ObservedPeriod("cycle", boundaries[-1], now, _cycle_label(boundaries[-1], now, open_cycle=True), False)
        completed = [
            ObservedPeriod("cycle", start, end, _cycle_label(start, end - timedelta(microseconds=1), open_cycle=False), True)
            for start, end in zip(boundaries[:-1], boundaries[1:])
        ]
        completed.reverse()
        completed = completed[:limit]
        if len(completed) < limit:
            completed += _months_before(boundaries[0], now=now, first_day=first_day, count=limit - len(completed))
        return PeriodHistory(completed, current, cycle_status)
    current = _month_period(today.year, today.month, now=now)
    completed = _months_before(current.start, now=now, first_day=first_day, count=limit)
    return PeriodHistory(completed, current, cycle_status)
