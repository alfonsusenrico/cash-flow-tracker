"""Observed, non-overlapping salary-cycle boundaries."""

from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


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
