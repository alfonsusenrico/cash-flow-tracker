import calendar
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.routers.recurring import calculate_next_due_date, format_recurring_rule_row
from app.services.auth import get_current_user


def test_calculate_next_due_date_monthly_day():
    ref = date(2026, 9, 10)
    # Day 20 of current month has not passed yet
    due = calculate_next_due_date(schedule_type="monthly_day", schedule_day=20, from_date=ref)
    assert due == date(2026, 9, 20)

    # Day 5 has already passed, should roll to next month
    due_past = calculate_next_due_date(schedule_type="monthly_day", schedule_day=5, from_date=ref)
    assert due_past == date(2026, 10, 5)

    # Advance=True should force next month even if day 20 hasn't passed
    due_advance = calculate_next_due_date(schedule_type="monthly_day", schedule_day=20, from_date=ref, advance=True)
    assert due_advance == date(2026, 10, 20)


def test_calculate_next_due_date_month_clamping():
    # Test day 31 in February
    ref = date(2026, 1, 31)
    due = calculate_next_due_date(schedule_type="monthly_day", schedule_day=31, from_date=ref, advance=True)
    assert due.month == 2
    assert due.day == 28  # 2026 is not a leap year


def test_calculate_next_due_date_payday():
    ref = date(2026, 9, 15)
    # User payday is 25th
    due = calculate_next_due_date(schedule_type="payday", schedule_day=None, user_payday_day=25, from_date=ref)
    assert due == date(2026, 9, 25)

    # Day 26 (after payday)
    ref_post = date(2026, 9, 26)
    due_post = calculate_next_due_date(schedule_type="payday", schedule_day=None, user_payday_day=25, from_date=ref_post)
    assert due_post == date(2026, 10, 25)


def test_calculate_next_due_date_weekly():
    # 2026-09-14 is a Monday (isoweekday = 1)
    ref = date(2026, 9, 14)
    # Target Wednesday (weekday = 3)
    due_wed = calculate_next_due_date(schedule_type="weekly", schedule_day=3, from_date=ref)
    assert due_wed == date(2026, 9, 16)

    # Target Monday with advance=True
    due_next_mon = calculate_next_due_date(schedule_type="weekly", schedule_day=1, from_date=ref, advance=True)
    assert due_next_mon == date(2026, 9, 21)


def test_format_recurring_rule_row():
    row = {
        "id": str(uuid4()),
        "name": "Cicilan Rumah",
        "type": "expense",
        "amount": 3500000,
        "source_account_id": str(uuid4()),
        "source_account_name": "BCA Payroll",
        "target_account_id": None,
        "target_account_name": None,
        "category_id": str(uuid4()),
        "category_name": "Cicilan",
        "category_icon": "home",
        "category_color": "#EF4444",
        "obligation_id": str(uuid4()),
        "obligation_name": "KPR BCA",
        "schedule_type": "monthly_day",
        "schedule_day": 25,
        "notes": "Cicilan autodebit",
        "is_payroll_allocation": False,
        "auto_post": True,
        "last_executed_at": datetime.now(timezone.utc),
        "next_due_date": date(2026, 9, 25),
        "is_active": True,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }

    formatted = format_recurring_rule_row(row)
    assert formatted["name"] == "Cicilan Rumah"
    assert formatted["amount"] == 3500000
    assert formatted["auto_post"] is True
    assert formatted["obligation_name"] == "KPR BCA"
    assert formatted["next_due_date"] == "2026-09-25"


def test_api_create_and_list_recurring_rules():
    user_id = str(uuid4())
    src_acc_id = str(uuid4())
    target_acc_id = str(uuid4())
    rule_id = str(uuid4())

    mock_user = {"id": user_id, "username": "enrico", "payday_day": 25}
    app.dependency_overrides[get_current_user] = lambda: mock_user

    try:
        client = TestClient(app)

        with patch("app.routers.recurring.db_conn") as mock_conn:
            mock_cur = MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cur

            # Mock source and target account checks
            mock_cur.fetchone.side_effect = [
                {"id": src_acc_id},  # source account exists
                {"id": target_acc_id},  # target account exists
                {"id": rule_id},  # insert returning id
            ]

            # 1. Create transfer rule
            create_resp = client.post(
                "/api/recurring",
                json={
                    "name": "Alokasi Belanja",
                    "type": "transfer",
                    "amount": 2000000,
                    "source_account_id": src_acc_id,
                    "target_account_id": target_acc_id,
                    "schedule_type": "payday",
                    "is_payroll_allocation": True,
                    "auto_post": False,
                },
            )
            assert create_resp.status_code == 200
            data = create_resp.json()
            assert data["ok"] is True
            assert data["id"] == rule_id

            # 2. List rules
            mock_cur.fetchall.return_value = [
                {
                    "id": rule_id,
                    "name": "Alokasi Belanja",
                    "type": "transfer",
                    "amount": 2000000,
                    "source_account_id": src_acc_id,
                    "source_account_name": "BCA",
                    "target_account_id": target_acc_id,
                    "target_account_name": "Jago - Makan",
                    "category_id": None,
                    "category_name": None,
                    "category_icon": None,
                    "category_color": None,
                    "obligation_id": None,
                    "obligation_name": None,
                    "schedule_type": "payday",
                    "schedule_day": None,
                    "notes": None,
                    "is_payroll_allocation": True,
                    "auto_post": False,
                    "last_executed_at": None,
                    "next_due_date": date(2026, 9, 25),
                    "is_active": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }
            ]

            list_resp = client.get("/api/recurring?is_payroll=true")
            assert list_resp.status_code == 200
            list_data = list_resp.json()
            assert list_data["ok"] is True
            assert len(list_data["rules"]) == 1
            assert list_data["rules"][0]["name"] == "Alokasi Belanja"
    finally:
        app.dependency_overrides.clear()


def test_api_payroll_batch_execute():
    user_id = str(uuid4())
    src_id = str(uuid4())
    dst1_id = str(uuid4())
    dst2_id = str(uuid4())
    rule1_id = str(uuid4())

    mock_user = {"id": user_id, "username": "enrico", "payday_day": 25}
    app.dependency_overrides[get_current_user] = lambda: mock_user

    try:
        client = TestClient(app)

        with patch("app.routers.recurring.db_conn") as mock_conn:
            mock_cur = MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cur

            # Internal movement categories lookup (expense, income)
            # Account checks in execution order:
            # Item 1: source check, target check, then rule fetch
            # Item 2: source check, target check (no rule_id)
            mock_cur.fetchone.side_effect = [
                {"id": str(uuid4())},  # internal movement expense category
                {"id": str(uuid4())},  # internal movement income category
                {"id": src_id},
                {"id": dst1_id},
                {
                    "id": rule1_id,
                    "schedule_type": "payday",
                    "schedule_day": None,
                    "next_due_date": date(2026, 9, 25),
                },
                {"id": src_id},
                {"id": dst2_id},
            ]

            resp = client.post(
                "/api/recurring/payroll/execute",
                json={
                    "items": [
                        {
                            "rule_id": rule1_id,
                            "source_account_id": src_id,
                            "target_account_id": dst1_id,
                            "amount": 2500000,
                            "notes": "Alokasi Makan",
                        },
                        {
                            "source_account_id": src_id,
                            "target_account_id": dst2_id,
                            "amount": 500000,
                            "notes": "Alokasi Transport",
                        },
                    ]
                },
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["ok"] is True
            assert data["allocated_items_count"] == 2
    finally:
        app.dependency_overrides.clear()
