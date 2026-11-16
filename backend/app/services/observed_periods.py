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
from app.i18n import account_language
from app.services.report_windows import cycle_range_label, month_label, shift_month
from app.services.salary_receipts import load_running_cycle_label_end, receipts as confirmed_receipts

COMPLETED_PERIOD_LIMIT = 6


@dataclass(frozen=True)
class ObservedPeriod:
    kind: str  # "cycle" or "month"
    start: datetime  # UTC, inclusive
    end: datetime  # UTC, exclusive; for the current period, the instant it was observed
    label: str
    complete: bool
    # The last day a cycle's label names: its last day once complete, else the running cycle's estimate.
    # A label only, never a bound; None for a month.
    label_end_date: date | None = None

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
            "label_end_date": self.label_end_date.isoformat() if self.label_end_date else None,
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


def month_period(year: int, month: int, *, now: datetime, language: str) -> ObservedPeriod:
    start = _local_midnight(date(year, month, 1))
    next_year, next_month = shift_month(year, month, 1)
    end = _local_midnight(date(next_year, next_month, 1))
    complete = end <= now
    return ObservedPeriod("month", start, end if complete else now, month_label(language, year, month), complete)


def closed_cycle(start: datetime, end: datetime, *, language: str) -> ObservedPeriod:
    """The cycle from one confirmed receipt to the next, `end` exclusive."""
    zone = ZoneInfo(settings.tz)
    first_day = start.astimezone(zone).date()
    last_day = (end - timedelta(microseconds=1)).astimezone(zone).date()
    return ObservedPeriod("cycle", start, end, cycle_range_label(language, first_day, last_day), True, last_day)


def running_cycle(cur, user_id: str, start: datetime, *, now: datetime, language: str) -> ObservedPeriod:
    """The cycle since the latest confirmed receipt: it runs to now, its label to the estimated last day."""
    first_day = start.astimezone(ZoneInfo(settings.tz)).date()
    label_end = load_running_cycle_label_end(cur.connection, user_id, first_day, now)
    return ObservedPeriod("cycle", start, now, cycle_range_label(language, first_day, label_end), False, label_end)


def first_recorded_day(cur, user_id: str) -> date | None:
    cur.execute("SELECT MIN(date) AS first FROM transactions WHERE user_id = %s", (user_id,))
    row = cur.fetchone()
    first = row["first"] if row else None
    return first.astimezone(ZoneInfo(settings.tz)).date() if first else None


def salary_boundaries(cur, user_id: str, now: datetime) -> tuple[list[datetime], str]:
    instants = sorted(row["date"] for row in confirmed_receipts(cur.connection, user_id) if row["date"] <= now)
    if not instants:
        return [], "not_configured"
    zone = ZoneInfo(settings.tz)
    days = [instant.astimezone(zone).date() for instant in instants]
    if len(days) != len(set(days)):
        # Same-day receipts need owner review before they may bound a cycle.
        return [], "needs_review"
    return instants, "confirmed"


def _months_before(
    limit_end: datetime, *, now: datetime, first_day: date, count: int, language: str,
) -> list[ObservedPeriod]:
    """Complete calendar months ending on or before `limit_end`, newest first, after tracking began."""
    zone = ZoneInfo(settings.tz)
    anchor = limit_end.astimezone(zone).date()
    year, month = anchor.year, anchor.month
    months: list[ObservedPeriod] = []
    # Walk back from the month containing `limit_end`; it qualifies only if it ended by then.
    for _ in range(count + 1):
        period = month_period(year, month, now=now, language=language)
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
    first_day = first_recorded_day(cur, user_id)
    language, _region = account_language(cur, user_id)
    today = now.astimezone(zone).date()
    boundaries, cycle_status = ([], "not_configured") if basis == "month" else salary_boundaries(cur, user_id, now)
    if first_day is None:
        return PeriodHistory([], None, cycle_status)
    if boundaries:
        current = running_cycle(cur, user_id, boundaries[-1], now=now, language=language)
        completed = [
            closed_cycle(start, end, language=language) for start, end in zip(boundaries[:-1], boundaries[1:])
        ]
        completed.reverse()
        completed = completed[:limit]
        if len(completed) < limit:
            completed += _months_before(boundaries[0], now=now, first_day=first_day, count=limit - len(completed),
                                        language=language)
        return PeriodHistory(completed, current, cycle_status)
    current = month_period(today.year, today.month, now=now, language=language)
    completed = _months_before(current.start, now=now, first_day=first_day, count=limit, language=language)
    return PeriodHistory(completed, current, cycle_status)
