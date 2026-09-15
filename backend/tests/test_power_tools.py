import datetime
from app.routers.dashboard import get_timeframe_window
from app.routers.accounts import AccountReconcile
from app.routers.transactions import TransactionUpdate
from app.routers.categories import CategoryUpdate


def test_timeframe_cycle_offset():
    ref_date = datetime.date(2026, 9, 10)
    # Current cycle: Aug 25 - Sep 24
    s_utc_0, e_utc_0, s_d_0, e_d_0, label_0 = get_timeframe_window(25, "cycle", ref_date, cycle_offset=0)
    assert s_d_0 == datetime.date(2026, 8, 25)
    assert e_d_0 == datetime.date(2026, 9, 24)

    # Previous cycle: Jul 25 - Aug 24
    s_utc_prev, e_utc_prev, s_d_prev, e_d_prev, label_prev = get_timeframe_window(25, "cycle", ref_date, cycle_offset=-1)
    assert s_d_prev == datetime.date(2026, 7, 25)
    assert e_d_prev == datetime.date(2026, 8, 24)


def test_account_reconcile_model():
    rec = AccountReconcile(actual_balance=5000000, notes="Verified against BCA app")
    assert rec.actual_balance == 5000000
    assert rec.notes == "Verified against BCA app"


def test_transaction_update_model():
    up = TransactionUpdate(amount=75000, notes="Dinner with friends")
    assert up.amount == 75000
    assert up.notes == "Dinner with friends"
    assert "amount" in up.model_fields_set
    assert "category_id" not in up.model_fields_set


def test_category_update_model():
    up = CategoryUpdate(monthly_budget=3000000)
    assert up.monthly_budget == 3000000
    assert "monthly_budget" in up.model_fields_set
