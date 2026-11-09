"""The expected next salary date: an estimate from regular confirmed receipts; all dates are synthetic."""
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from app.services.salary_cycles import expected_next_salary

JAKARTA = ZoneInfo("Asia/Jakarta")


def local(year, month, day, hour=9, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=JAKARTA)


def estimate(receipts, today):
    return expected_next_salary(receipts, tz_name="Asia/Jakarta", as_of=datetime.combine(today, datetime.min.time(), JAKARTA) + timedelta(hours=12))


def test_regular_receipts_give_the_latest_day_in_the_next_month():
    result = estimate([local(2026, 8, 25), local(2026, 9, 25), local(2026, 10, 24)], date(2026, 11, 2))
    assert (result.status, result.date, result.reason) == ("estimated", date(2026, 11, 24), None)
    assert result.receipt_dates == (date(2026, 8, 25), date(2026, 9, 25), date(2026, 10, 24))


def test_only_the_last_three_receipts_count():
    # An early irregular receipt no longer matters once three regular ones follow it.
    receipts = [local(2026, 6, 3), local(2026, 8, 25), local(2026, 9, 25), local(2026, 10, 25)]
    result = estimate(receipts, date(2026, 11, 2))
    assert (result.status, result.date) == ("estimated", date(2026, 11, 25))


def test_too_few_receipts_are_unknown():
    result = estimate([local(2026, 9, 25), local(2026, 10, 25)], date(2026, 11, 2))
    assert (result.status, result.date, result.reason) == ("unknown", None, "too_few_receipts")
    assert estimate([], date(2026, 11, 2)).reason == "too_few_receipts"


@pytest.mark.parametrize("receipts", [
    # A 16-day gap.
    [local(2026, 8, 25), local(2026, 9, 10), local(2026, 10, 24)],
    # Gaps inside 26–35 days, but the days of the month spread over 4 days.
    [local(2026, 8, 10), local(2026, 9, 14), local(2026, 10, 10)],
    # A 36-day gap.
    [local(2026, 8, 20), local(2026, 9, 25), local(2026, 10, 25)],
])
def test_irregular_receipts_are_unknown(receipts):
    result = estimate(receipts, date(2026, 11, 2))
    assert (result.status, result.date, result.reason) == ("unknown", None, "irregular_receipts")


def test_a_passed_date_is_overdue_and_never_moves_forward():
    receipts = [local(2026, 8, 24), local(2026, 9, 24), local(2026, 10, 24)]
    assert estimate(receipts, date(2026, 11, 24)).status == "estimated"
    late = estimate(receipts, date(2026, 11, 26))
    assert (late.status, late.date) == ("overdue", date(2026, 11, 24))
    assert estimate(receipts, date(2027, 1, 10)).date == date(2026, 11, 24)


def test_day_31_is_clamped_to_a_30_day_month():
    result = estimate([local(2026, 8, 31), local(2026, 9, 30), local(2026, 10, 31)], date(2026, 11, 2))
    assert (result.status, result.date) == ("estimated", date(2026, 11, 30))


def test_day_31_is_clamped_to_february():
    result = estimate([local(2026, 11, 30), local(2026, 12, 31), local(2027, 1, 31)], date(2027, 2, 2))
    assert (result.status, result.date) == ("estimated", date(2027, 2, 28))


def test_month_end_payers_who_cross_into_the_next_month_stay_regular():
    # 31 Aug, 30 Sep and 1 Nov are one day apart around the month end, not 30 days.
    result = estimate([local(2026, 8, 31), local(2026, 9, 30), local(2026, 11, 1)], date(2026, 11, 3))
    assert (result.status, result.date) == ("estimated", date(2026, 12, 1))


def test_days_are_local_jakarta_days_at_the_midnight_boundary():
    # 17:30 UTC on the 24th is 00:30 on the 25th in Jakarta; a UTC reading would expect the 24th.
    receipts = [datetime(2026, month, 24, 17, 30, tzinfo=timezone.utc) for month in (8, 9, 10)]
    result = estimate(receipts, date(2026, 11, 2))
    assert result.receipt_dates == (date(2026, 8, 25), date(2026, 9, 25), date(2026, 10, 25))
    assert result.date == date(2026, 11, 25)
    # One minute before local midnight stays on the 24th.
    receipts = [datetime(2026, month, 24, 16, 59, tzinfo=timezone.utc) for month in (8, 9, 10)]
    assert estimate(receipts, date(2026, 11, 2)).date == date(2026, 11, 24)


def test_receipts_after_the_moment_of_asking_are_ignored():
    receipts = [local(2026, 8, 25), local(2026, 9, 25), local(2026, 10, 25), local(2026, 11, 25)]
    assert estimate(receipts, date(2026, 11, 2)).date == date(2026, 11, 25)


def test_naive_timestamps_are_rejected():
    with pytest.raises(ValueError):
        expected_next_salary([datetime(2026, 10, 25)], tz_name="Asia/Jakarta", as_of=local(2026, 11, 2))


def test_report_windows_ignore_the_estimate(monkeypatch):
    """An open cycle runs to now even after the expected date has passed; no window ends at the estimate."""
    from app.services import report_windows
    from app.services.salary_cycles import SalaryCycle

    start = datetime.now(timezone.utc) - timedelta(days=40)
    monkeypatch.setattr(report_windows, "load_user_cycle", lambda *_args, **_kwargs: SalaryCycle("open", start, None))
    before = datetime.now(timezone.utc)
    begin, finish, _, _, label = report_windows.get_timeframe_window(25, "cycle", user_id="synthetic", language="id")
    assert begin == start
    assert finish >= before
    assert label.endswith("berlanjut")
