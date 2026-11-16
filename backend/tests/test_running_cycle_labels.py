"""Salary-cycle labels as a date range, the running cycle ending on its estimated last day
(improve-ledger-readability decision 5, report-periods-and-drilldown "Salary Cycle Labels" and "Cycle Label Is
Not a Window"). Every date, name and amount is synthetic.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from app.services.report_windows import cycle_range_label
from app.services.salary_cycles import (
    ExpectedSalary,
    expected_next_salary,
    next_payday_from_setting,
    running_cycle_label_end,
)
from test_cycle_summary import at_local, make_owner, owner, shift_months, today, unlimited_rate_limits  # noqa: F401

JAKARTA = ZoneInfo("Asia/Jakarta")
UNKNOWN = ExpectedSalary("unknown", reason="too_few_receipts")


def regular(today_: date) -> ExpectedSalary:
    receipts = [datetime(2026, 8, 25, 9, tzinfo=JAKARTA), datetime(2026, 9, 25, 9, tzinfo=JAKARTA),
                datetime(2026, 10, 24, 9, tzinfo=JAKARTA)]
    return expected_next_salary(receipts, tz_name="Asia/Jakarta", as_of=datetime.combine(today_, datetime.min.time(), JAKARTA))


# --- The estimate ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("start", "payday", "today_", "label_end"), [
    # The payday nearest 30 Okt is 29 Okt; the next is 29 Nov, where the label ends (payday to payday).
    (date(2026, 10, 30), 29, date(2026, 11, 16), date(2026, 11, 29)),
    # The salary is late: the label never ends before today.
    (date(2026, 10, 30), 29, date(2026, 11, 30), date(2026, 11, 30)),
    # The salary came early on 27 Nov: its payday is 29 Nov, the next 29 Des.
    (date(2026, 11, 27), 29, date(2026, 11, 27), date(2026, 12, 29)),
    # Payday 31 in a 30-day month: the nearest payday of 31 Okt is 31 Okt, the next is clamped to 30 Nov.
    (date(2026, 10, 31), 31, date(2026, 11, 5), date(2026, 11, 30)),
    # A clamped payday (30 Sep) moves on to the payday's own day in the next month (31 Okt).
    (date(2026, 9, 30), 31, date(2026, 10, 2), date(2026, 10, 31)),
    # February clamps to its length.
    (date(2026, 1, 31), 31, date(2026, 2, 2), date(2026, 2, 28)),
])
def test_label_end_from_the_payday_setting(start, payday, today_, label_end):
    assert running_cycle_label_end(start, today_, expected=UNKNOWN, payday_day=payday) == label_end


def test_next_payday_across_a_year_end():
    assert next_payday_from_setting(date(2026, 12, 30), 29) == date(2027, 1, 29)
    assert next_payday_from_setting(date(2027, 1, 2), 30) == date(2027, 1, 30)


def test_regular_receipts_decide_over_the_payday_setting():
    estimate = regular(date(2026, 11, 2))
    assert (estimate.status, estimate.date) == ("estimated", date(2026, 11, 24))
    assert running_cycle_label_end(date(2026, 10, 24), date(2026, 11, 2), expected=estimate, payday_day=29) == date(
        2026, 11, 24)


def test_overdue_estimate_ends_the_label_today():
    estimate = regular(date(2026, 11, 30))
    assert estimate.status == "overdue"
    assert running_cycle_label_end(date(2026, 10, 24), date(2026, 11, 30), expected=estimate, payday_day=29) == date(
        2026, 11, 30)


def test_range_in_both_languages():
    assert cycle_range_label("id", date(2026, 10, 30), date(2026, 11, 29)) == "30 Okt – 29 Nov"
    assert cycle_range_label("en", date(2026, 10, 30), date(2026, 11, 29)) == "30 Oct – 29 Nov"


# --- The payloads ---------------------------------------------------------------------------------------------


@pytest.fixture
def cycling(owner):
    """An owner with two confirmed receipts (too few for an estimate) and the payday setting on the latest's day."""
    start = today() - timedelta(days=17)
    previous = shift_months(start, -1)
    owner.salary(previous)
    owner.salary(start)
    owner.db("UPDATE users SET payday_day = %s WHERE id = %s", (start.day, owner.user_id))
    owner.start, owner.previous = start, previous
    # The payday nearest the start is the start itself, so the next salary is expected a month later.
    owner.label_end = max(today(), shift_months(start, 1))
    return owner


def ledger(owner, query):
    response = owner.session.get(f"/api/transactions?limit=1&{query}")
    assert response.status_code == 200, response.text
    return response.json()["summary"]


def test_ledger_labels_the_running_and_closed_cycles(cycling):
    start, label_end = cycling.start, cycling.label_end
    summary = ledger(cycling, "period=cycle")
    expected_label = cycle_range_label("id", start, label_end)
    for block in (summary["cycle"], summary["period"]):
        assert block["label"] == expected_label
        assert block["label_end_date"] == label_end.isoformat()

    closed = ledger(cycling, "period=cycle&period_offset=-1")["period"]
    closed_end = datetime.fromisoformat(closed["end"]).astimezone(JAKARTA).date()
    assert closed["label_end_date"] == closed_end.isoformat()
    assert closed["label"] == cycle_range_label("id", cycling.previous, closed_end)

    assert ledger(cycling, "period=month")["period"]["label_end_date"] is None
    assert ledger(cycling, "")["period"] is None


def test_running_cycle_window_still_ends_at_now(cycling):
    before = datetime.now(timezone.utc)
    period = ledger(cycling, "period=cycle")["period"]
    after = datetime.now(timezone.utc)
    end = datetime.fromisoformat(period["end"])
    assert datetime.fromisoformat(period["start"]) == at_local(cycling.start).astimezone(timezone.utc)
    # The label names the estimate; the window, its figures and its rows stop at now.
    assert before - timedelta(seconds=5) <= end <= after
    assert date.fromisoformat(period["label_end_date"]) > end.astimezone(JAKARTA).date()


def test_english_account_reads_the_range_in_english(cycling):
    cycling.db("UPDATE users SET language = 'en' WHERE id = %s", (cycling.user_id,))
    summary = ledger(cycling, "period=cycle")
    assert summary["cycle"]["label"] == cycle_range_label("en", cycling.start, cycling.label_end)
    english = f"{cycling.start.day} {cycling.start:%b} – {cycling.label_end.day} {cycling.label_end:%b}"
    assert summary["cycle"]["label"] == english


def test_beranda_period_carries_the_estimate(cycling, make_owner):
    period = cycling.summary()["period"]
    assert (period["basis"], period["label_end_date"]) == ("cycle", cycling.label_end.isoformat())
    assert period["label"] == cycle_range_label("id", cycling.start, cycling.label_end)
    assert cycling.session.get("/api/pulse").json()["cycle_label"] == period["label"]

    month_owner = make_owner()
    month_owner.expense(10_000, "makan")
    month = month_owner.summary()["period"]
    assert (month["basis"], month["label_end_date"]) == ("month", None)


def test_observed_and_report_periods_carry_the_label_end(cycling):
    from app.db.pool import db_conn
    from app.services.monthly_report import describe_period, list_periods
    from app.services.observed_periods import load_period_history

    now = datetime.now(timezone.utc)
    with db_conn() as conn:
        with conn.cursor() as cur:
            history = load_period_history(cur, cycling.user_id, now=now)
            months = load_period_history(cur, cycling.user_id, now=now, basis="month")
            listing = list_periods(cur, cycling.user_id, "cycle", now=now)

    current = history.current.describe()
    assert (current["kind"], current["complete"]) == ("cycle", False)
    assert current["label_end_date"] == cycling.label_end.isoformat()
    assert current["label"] == cycle_range_label("id", cycling.start, cycling.label_end)
    assert current["end_date"] == today().isoformat()
    closed = [period.describe() for period in history.completed if period.kind == "cycle"]
    assert closed and all(period["label_end_date"] == period["end_date"] for period in closed)
    assert closed[0]["label"] == cycle_range_label(
        "id", date.fromisoformat(closed[0]["start_date"]), date.fromisoformat(closed[0]["end_date"]))
    assert months.current.describe()["label_end_date"] is None

    reported = [describe_period(period, "cycle", None) for period in listing["periods"]]
    assert reported[0]["label_end_date"] == cycling.label_end.isoformat()
    assert reported[0]["label"] == current["label"]
    assert all(period["label_end_date"] == period["end_date"] for period in reported[1:])
