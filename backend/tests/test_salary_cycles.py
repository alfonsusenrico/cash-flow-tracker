"""Observed salary cycles; all dates and amounts are synthetic."""
from datetime import datetime, timezone

import pytest
from app.services.salary_cycles import resolve_cycle


def dt(year, month, day, hour=0):
    return datetime(year, month, day, hour, tzinfo=timezone.utc)


def test_actual_receipts_close_prior_cycle_and_leave_current_end_unknown():
    receipts = [dt(2026, 8, 31), dt(2026, 10, 3), dt(2026, 11, 2)]
    now = dt(2026, 10, 30)
    current = resolve_cycle(receipts, tz_name='Asia/Jakarta', as_of=now)
    previous = resolve_cycle(receipts, offset=-1, tz_name='Asia/Jakarta', as_of=now)
    assert (current.status, current.start, current.end_exclusive) == ('open', receipts[1], None)
    assert (previous.status, previous.start, previous.end_exclusive) == ('closed', receipts[0], receipts[1])
    assert resolve_cycle(receipts, offset=1, tz_name='Asia/Jakarta', as_of=now).status == 'unavailable'
    assert resolve_cycle(receipts, offset=-2, tz_name='Asia/Jakarta', as_of=now).status == 'unavailable'


def test_absent_and_ambiguous_receipts_are_not_invented():
    now = dt(2026, 10, 30)
    assert resolve_cycle([], tz_name='Asia/Jakarta', as_of=now).status == 'not_configured'
    assert resolve_cycle([dt(2026, 10, 1, 1), dt(2026, 10, 1, 2)], tz_name='Asia/Jakarta', as_of=now).status == 'needs_review'
    # Different UTC dates can still be the same local calendar day.
    assert resolve_cycle([dt(2026, 9, 30, 18), dt(2026, 10, 1, 1)], tz_name='Asia/Jakarta', as_of=now).status == 'needs_review'


def test_naive_timestamps_are_rejected():
    with pytest.raises(ValueError):
        resolve_cycle([datetime(2026, 10, 1)], tz_name='Asia/Jakarta', as_of=dt(2026, 10, 30))


def test_report_window_uses_actual_receipt_timestamps(monkeypatch):
    from datetime import timedelta
    from app.services import report_windows
    from app.services.salary_cycles import SalaryCycle

    start = dt(2026, 9, 27, 4)
    end = dt(2026, 10, 3, 5)
    monkeypatch.setattr(report_windows, 'load_user_cycle', lambda *_args, **_kwargs: SalaryCycle('closed', start, end))
    begin, finish, _, _, label = report_windows.get_timeframe_window(25, 'cycle', user_id='synthetic')
    assert begin == start
    assert finish == end - timedelta(microseconds=1)
    assert 'Siklus' in label


def test_report_window_discloses_missing_cycle_and_offers_calendar(monkeypatch):
    from app.services import report_windows
    from app.services.salary_cycles import SalaryCycle

    monkeypatch.setattr(report_windows, 'load_user_cycle', lambda *_args, **_kwargs: SalaryCycle('not_configured'))
    start, end, _, _, label = report_windows.get_timeframe_window(25, 'cycle', user_id='synthetic')
    assert 'siklus gaji belum tersedia' in label
    assert start <= end
