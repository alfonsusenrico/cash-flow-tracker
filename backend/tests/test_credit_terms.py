"""Credit due dates and instalments (add-credit-spending-flow D2, D7): the Credit Due Dates scenarios, without a database.

Dates are synthetic and absolute: the rules are calendar arithmetic, independent of today.
"""
from datetime import date

import pytest

from app.services.credit_terms import (
    OWNER, SYSTEM, Terms, derived_first_due, installment_amounts, installment_due_dates, opening_due,
    statement_close, statement_date,
)


@pytest.mark.parametrize("purchase,due", [(date(2026, 11, 5), date(2026, 12, 11)), (date(2026, 12, 1), date(2027, 1, 11))])
def test_spaylater_billed_on_the_first_and_due_on_the_eleventh(purchase, due):
    assert derived_first_due(purchase, 1, 11) == due


@pytest.mark.parametrize("purchase,due", [(date(2026, 11, 20), date(2026, 12, 5)), (date(2026, 11, 25), date(2027, 1, 5))])
def test_bill_on_the_25th_due_on_the_5th(purchase, due):
    assert statement_close(date(2026, 11, 20), 25) == date(2026, 11, 25)
    assert statement_close(date(2026, 11, 25), 25) == date(2026, 12, 25)
    assert derived_first_due(purchase, 25, 5) == due


def test_an_empty_bill_day_means_the_first():
    assert statement_close(date(2026, 11, 5), None) == date(2026, 12, 1)
    assert derived_first_due(date(2026, 11, 5), None, 11) == date(2026, 12, 11)


def test_instalment_amounts_put_the_remainder_on_the_first():
    assert installment_amounts(1_000_000, 3) == [333_334, 333_333, 333_333]
    assert installment_amounts(1_200_000, 6) == [200_000] * 6
    assert installment_amounts(120_000, 1) == [120_000]
    for amount in (1, 7, 99_999, 1_000_001):
        for count in (1, 2, 3, 12, 36):
            parts = installment_amounts(amount, count)
            assert sum(parts) == amount and len(parts) == count and parts[0] >= max(parts[1:] or [0])


def test_three_instalments_from_5_november_on_spaylater():
    dates = installment_due_dates(Terms(3), date(2026, 11, 5), 1, 11)
    assert dates == [date(2026, 12, 11), date(2027, 1, 11), date(2027, 2, 11)]


def test_an_owner_first_due_date_shifts_the_whole_series():
    terms = Terms(3, date(2026, 12, 1), OWNER)
    assert installment_due_dates(terms, date(2026, 11, 5), 1, 11) == [date(2026, 12, 1), date(2027, 1, 1), date(2027, 2, 1)]


def test_a_kept_first_due_date_anchors_its_series_after_the_payment_day_changes():
    kept = Terms(2, date(2026, 11, 11), SYSTEM)
    assert installment_due_dates(kept, date(2026, 10, 5), 1, 5) == [date(2026, 11, 11), date(2026, 12, 11)]


def test_days_are_clamped_to_short_months():
    # A purchase on 31 January with bill day 31 closes on 28 February, and payment day 31 falls on it.
    assert statement_close(date(2027, 1, 31), 31) == date(2027, 2, 28)
    assert derived_first_due(date(2027, 1, 31), 31, 31) == date(2027, 2, 28)
    assert installment_due_dates(Terms(3), date(2027, 1, 31), 31, 31) == [
        date(2027, 2, 28), date(2027, 3, 31), date(2027, 4, 30),
    ]
    assert statement_close(date(2028, 1, 31), 31) == date(2028, 2, 29)
    assert statement_date(date(2027, 2, 28), 31) == date(2027, 2, 28)
    assert statement_date(date(2027, 3, 5), 31) == date(2027, 2, 28)


def test_no_payment_day_means_no_due_date():
    assert derived_first_due(date(2026, 11, 5), 1, None) is None
    assert installment_due_dates(Terms(3), date(2026, 11, 5), 1, None) is None
    assert opening_due(date(2026, 11, 3), None) is None


def test_opening_debt_is_due_on_the_first_payment_day_from_creation():
    assert opening_due(date(2026, 11, 3), 11) == date(2026, 11, 11)
    assert opening_due(date(2026, 11, 11), 11) == date(2026, 11, 11)
    assert opening_due(date(2026, 11, 12), 11) == date(2026, 12, 11)


def test_statement_date_is_the_latest_bill_day_on_or_before_the_due_date():
    assert statement_date(date(2026, 12, 11), 1) == date(2026, 12, 1)
    assert statement_date(date(2026, 12, 5), 25) == date(2026, 11, 25)
    assert statement_date(date(2026, 12, 25), 25) == date(2026, 12, 25)
