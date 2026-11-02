"""Observed, non-overlapping salary-cycle boundaries and the expected next salary date."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

# The expected-date rule (salary-cycles "Expected Next Salary Date").
EXPECTED_RECEIPT_COUNT = 3
MIN_RECEIPT_GAP_DAYS = 26
MAX_RECEIPT_GAP_DAYS = 35
MAX_DAY_OF_MONTH_SPREAD = 3


@dataclass(frozen=True)
class SalaryCycle:
    status: str
    start: datetime | None = None
    end_exclusive: datetime | None = None


def resolve_cycle(
    receipts: list[datetime], *, offset: int = 0, tz_name: str,
    as_of: datetime | None = None,
) -> SalaryCycle:
    """Use receipt instants; the active cycle has no known future end."""
    now = as_of or datetime.now(timezone.utc)
    if now.tzinfo is None or any(receipt.tzinfo is None for receipt in receipts):
        raise ValueError('Salary cycle timestamps must be timezone-aware')
    zone = ZoneInfo(tz_name)
    boundaries = sorted(receipt for receipt in receipts if receipt <= now)
    if not boundaries:
        return SalaryCycle('not_configured')
    days = [value.astimezone(zone).date() for value in boundaries]
    if len(days) != len(set(days)):
        return SalaryCycle('needs_review')
    index = len(boundaries) - 1 + offset
    if index < 0 or index >= len(boundaries):
        return SalaryCycle('unavailable')
    end = boundaries[index + 1] if index + 1 < len(boundaries) else None
    return SalaryCycle('closed' if end is not None else 'open', boundaries[index], end)


def load_user_cycle(user_id: str, *, offset: int = 0, as_of: datetime | None = None) -> SalaryCycle:
    """Read the owner's eligible receipts before resolving their observed cycle."""
    from app.core.config import settings
    from app.db.pool import db_conn
    from app.services.salary_receipts import receipts

    with db_conn() as conn:
        rows = receipts(conn, user_id)
    return resolve_cycle([row['date'] for row in rows], offset=offset, tz_name=settings.tz, as_of=as_of)


@dataclass(frozen=True)
class ExpectedSalary:
    """A planning estimate only: it never closes, shortens or extends a cycle and is never stored."""

    status: str
    date: date | None = None
    receipt_dates: tuple[date, ...] = ()
    reason: str | None = None


def _month_end_offset(day: date) -> int:
    """Days from the start of the next month for late days, so 31 Aug, 30 Sep and 1 Nov sit 1 day apart.

    Early days keep their distance from the 1st; a payer who pays on the last working day of each
    month is therefore as regular as one who pays on the 25th.
    """
    month_length = calendar.monthrange(day.year, day.month)[1]
    return day.day - 1 if day.day <= 15 else day.day - month_length - 1


def _day_spread(days: list[date]) -> int:
    plain = [day.day for day in days]
    wrapped = [_month_end_offset(day) for day in days]
    return min(max(plain) - min(plain), max(wrapped) - min(wrapped))


def _same_day_next_month(day: date) -> date:
    year, month = (day.year + 1, 1) if day.month == 12 else (day.year, day.month + 1)
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def expected_next_salary(receipts: list[datetime], *, tz_name: str, as_of: datetime) -> ExpectedSalary:
    """Estimate the next salary date from the last three confirmed receipts of the confirmed source.

    The receipts must be 26 to 35 local days apart and their local days of the month within 3 of each
    other; the estimate is the latest receipt's day in the following month, clamped to its length.
    Once that date has passed it is reported as overdue, never moved forward.
    """
    if as_of.tzinfo is None or any(receipt.tzinfo is None for receipt in receipts):
        raise ValueError('Salary receipt timestamps must be timezone-aware')
    zone = ZoneInfo(tz_name)
    days = sorted(receipt.astimezone(zone).date() for receipt in receipts if receipt <= as_of)
    latest = tuple(days[-EXPECTED_RECEIPT_COUNT:])
    if len(latest) < EXPECTED_RECEIPT_COUNT:
        return ExpectedSalary('unknown', receipt_dates=latest, reason='too_few_receipts')
    gaps = [(later - earlier).days for earlier, later in zip(latest, latest[1:])]
    if any(not MIN_RECEIPT_GAP_DAYS <= gap <= MAX_RECEIPT_GAP_DAYS for gap in gaps) \
            or _day_spread(list(latest)) > MAX_DAY_OF_MONTH_SPREAD:
        return ExpectedSalary('unknown', receipt_dates=latest, reason='irregular_receipts')
    expected = _same_day_next_month(latest[-1])
    today = as_of.astimezone(zone).date()
    status = 'overdue' if expected < today else 'estimated'
    return ExpectedSalary(status, expected, latest, None)
