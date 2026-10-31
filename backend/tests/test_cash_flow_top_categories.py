"""Top spending categories in the phone's cash-flow summary.

The list is a view of the summary's own `outflow`: same window, same inclusion rules, reimbursements
netted per category. All names and amounts are synthetic.
"""
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from app.services.report_figures import top_categories
from test_reimbursements import owner, record, shared_dinner_cycle  # noqa: F401 - fixture

SUMMARY = "/api/dashboard/cash-flow-summary"


def summary(client, period="today"):
    response = client.get(SUMMARY, params={"period": period})
    assert response.status_code == 200, response.text
    report = response.json()
    listed = report["top_categories"]
    assert isinstance(listed, list) and len(listed) <= 3
    assert all(item["outflow"] > 0 for item in listed)
    assert sum(item["outflow"] for item in listed) <= report["outflow"]
    assert listed == sorted(listed, key=lambda item: (-item["outflow"], item["name"]))
    return report


def pairs(report):
    return [(item["name"], item["outflow"]) for item in report["top_categories"]]


def listed_total(report):
    return sum(item["outflow"] for item in report["top_categories"])


UNCATEGORISED = {"category_id": None, "name": "Tanpa kategori", "kakeibo_type": None}


def add_category(owner, name, *, pillar="need", user_id=None):
    from app.db.pool import db_conn

    with db_conn() as conn:
        category_id = conn.execute(
            "INSERT INTO categories (user_id, name, kind, kakeibo_type) VALUES (%s, %s, 'expense', %s) RETURNING id",
            (user_id or owner["user_id"], name, pillar),
        ).fetchone()["id"]
        conn.commit()
    return str(category_id)


def add_expense(owner, amount, category_id, *, when=None, user_id=None, account_id=None):
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, source, review_status)
               VALUES (%s, %s, %s, 'expense', %s, %s, 'manual', 'reviewed')""",
            (user_id or owner["user_id"], account_id or owner["cash"], category_id, amount,
             when or datetime.now(timezone.utc)),
        )
        conn.commit()


def test_listed_categories_add_up_to_outflow_net_of_reimbursements(client, owner):
    shared_dinner_cycle(client, owner)
    for period in ("today", "month"):
        report = summary(client, period)
        assert report["outflow"] == 300_000 and report["reimbursement_offset"] == 200_000
        assert report["top_categories"] == [
            {"category_id": owner["groceries"], "name": "Belanja Dapur", "kakeibo_type": "need", "outflow": 200_000},
            {"category_id": owner["dinner"], "name": "Makan Bareng", "kakeibo_type": "want", "outflow": 100_000},
        ]
        assert sum(item["outflow"] for item in report["top_categories"]) == report["outflow"]


def test_a_fully_reimbursed_category_is_left_out(client, owner):
    record(client, owner, "dinner", 100_000)
    record(client, owner, "groceries", 50_000)
    record(client, owner, "reimbursement", 150_000, tx_type="income", offset="dinner")
    report = summary(client)
    assert (report["outflow"], report["reimbursement_excess"]) == (50_000, 50_000)
    assert pairs(report) == [("Belanja Dapur", 50_000)]


def test_movements_transfers_and_investments_are_left_out_and_a_fee_counts_once(client, owner):
    from app.db.pool import db_conn
    from app.services.movement_pairing import FEE_CATEGORY_NAME, link_pair

    record(client, owner, "groceries", 20_000)
    record(client, owner, "salary", 5_000_000, tx_type="income")
    record(client, owner, "investment", 400_000)
    record(client, owner, "movement", 900_000)
    with db_conn() as conn:
        bank = str(conn.execute(
            "INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Bank Contoh', 'bank', 0) RETURNING id",
            (owner["user_id"],),
        ).fetchone()["id"])
        conn.commit()
    moved = client.post("/api/movements", json={
        "source_account_id": owner["cash"], "target_account_id": bank, "amount": 250_000,
    })
    assert moved.status_code == 200, moved.text
    with db_conn() as conn:
        legs = {}
        for tx_type, account, amount in [("expense", owner["cash"], 102_500), ("income", bank, 100_000)]:
            legs[tx_type] = str(conn.execute(
                """INSERT INTO transactions (user_id, account_id, type, amount, source, review_status)
                   VALUES (%s, %s, %s, %s, 'manual', 'reviewed') RETURNING id""",
                (owner["user_id"], account, tx_type, amount),
            ).fetchone()["id"])
        with conn.cursor() as cur:
            link_pair(cur, owner["user_id"], legs["expense"], legs["income"], fee=2_500, notes=None)
        conn.commit()
    position = client.post("/api/accounts", json={
        "name": "Saham Contoh", "type": "investment", "instrument_type": "stock", "instrument_symbol": "TEST.JK",
        "units": 10, "avg_buy_price": 1_000, "initial_balance": 0,
    })
    assert position.status_code == 200, position.text
    bought = client.post("/api/transactions", json={
        "type": "expense", "account_id": owner["cash"], "target_account_id": position.json()["account"]["id"],
        "amount": 10_000, "investment_action": "buy", "units": 5, "price_per_unit": 2_000,
    })
    assert bought.status_code == 200, bought.text

    report = summary(client)
    assert report["outflow"] == 22_500
    assert pairs(report) == [("Belanja Dapur", 20_000), (FEE_CATEGORY_NAME, 2_500)]
    assert sum(item["outflow"] for item in report["top_categories"]) == report["outflow"]


def test_today_and_month_use_the_summary_window(client, owner, monkeypatch):
    from app.core.config import settings
    from app.routers import dashboard

    zone = ZoneInfo(settings.tz)
    now = datetime(2026, 3, 15, 12, 0, tzinfo=zone)

    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz else now.replace(tzinfo=None)

    monkeypatch.setattr(dashboard, "datetime", FixedClock)
    add_expense(owner, 30_000, owner["dinner"], when=datetime(2026, 3, 15, 8, 0, tzinfo=zone))
    add_expense(owner, 80_000, owner["groceries"], when=datetime(2026, 3, 3, 10, 0, tzinfo=zone))
    add_expense(owner, 500_000, owner["misc"], when=datetime(2026, 2, 28, 23, 0, tzinfo=zone))
    add_expense(owner, 700_000, owner["misc"], when=datetime(2026, 3, 15, 18, 0, tzinfo=zone))

    today = summary(client, "today")
    assert (today["outflow"], pairs(today)) == (30_000, [("Makan Bareng", 30_000)])
    month = summary(client, "month")
    assert (month["outflow"], pairs(month)) == (110_000, [("Belanja Dapur", 80_000), ("Makan Bareng", 30_000)])


def test_no_spending_gives_an_empty_list(client, owner):
    assert summary(client)["top_categories"] == []
    record(client, owner, "salary", 5_000_000, tx_type="income")
    record(client, owner, "reimbursement", 80_000, tx_type="income")
    report = summary(client, "month")
    assert (report["outflow"], report["top_categories"]) == (0, [])


def add_stranger_account(owner):
    from app.db.pool import db_conn

    with db_conn() as conn:
        stranger = str(conn.execute(
            "SELECT user_id FROM categories WHERE id = %s", (owner["stranger_dinner"],),
        ).fetchone()["user_id"])
        stranger_cash = str(conn.execute(
            "INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Kas Lain', 'bank', 0) RETURNING id",
            (stranger,),
        ).fetchone()["id"])
        conn.commit()
    return stranger, stranger_cash


def test_another_owners_spending_is_not_listed(client, owner):
    stranger, stranger_cash = add_stranger_account(owner)
    add_expense(owner, 900_000, owner["stranger_dinner"], user_id=stranger, account_id=stranger_cash)
    record(client, owner, "groceries", 40_000)
    report = summary(client)
    assert (report["outflow"], pairs(report)) == (40_000, [("Belanja Dapur", 40_000)])
    assert owner["stranger_dinner"] not in {item["category_id"] for item in report["top_categories"]}


def test_bearer_key_reads_the_same_list(client, owner):
    from fastapi.testclient import TestClient

    from app.db.pool import db_conn
    from app.main import app
    from app.services.auth import get_current_user, hash_token

    shared_dinner_cycle(client, owner)
    session_view = summary(client)
    plain = f"synthetic-key-{uuid4().hex}"
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES (%s, %s, 'synthet')",
            (owner["user_id"], hash_token(plain)),
        )
        conn.commit()
    app.dependency_overrides.pop(get_current_user)
    phone = TestClient(app, base_url="https://testserver", headers={"Authorization": f"Bearer {plain}"})
    bearer_view = summary(phone)
    assert bearer_view["top_categories"] == session_view["top_categories"]
    assert bearer_view["outflow"] == session_view["outflow"]
    anonymous = TestClient(app, base_url="https://testserver", headers={"Authorization": "Bearer synthetic-unknown"})
    assert anonymous.get(SUMMARY).status_code == 401


def test_at_most_three_are_listed_and_ties_go_by_name(client, owner):
    snacks = add_category(owner, "Camilan", pillar="want")
    hobby = add_category(owner, "Hobi", pillar="culture")
    for category_id in (owner["misc"], snacks, owner["dinner"], hobby):
        add_expense(owner, 50_000, category_id)
    add_expense(owner, 10_000, owner["groceries"])

    report = summary(client)
    assert report["outflow"] == 210_000
    assert report["top_categories"] == [
        {"category_id": snacks, "name": "Camilan", "kakeibo_type": "want", "outflow": 50_000},
        {"category_id": hobby, "name": "Hobi", "kakeibo_type": "culture", "outflow": 50_000},
        # A category without a classification reports as "need", as the category breakdown does.
        {"category_id": owner["misc"], "name": "Lain-lain", "kakeibo_type": "need", "outflow": 50_000},
    ]


def breakdown_row(name, spent, *, category_id="", uncategorized=False, pillar="need"):
    return {"id": str(uuid4()) if category_id == "" else category_id, "name": name, "kakeibo_type": pillar,
            "spent": spent, "is_uncategorized": uncategorized}


def test_top_categories_ranks_uncategorised_spending_and_skips_empty_rows():
    rows = [
        breakdown_row("Makan", 40_000, category_id="b"),
        breakdown_row("Jajan", 0),
        breakdown_row("Tanpa kategori", 55_000, category_id=None, uncategorized=True),
        breakdown_row("Transport", 40_000, category_id="a"),
        breakdown_row("Belanja", 70_000, category_id="c"),
        breakdown_row("Hobi", 5_000),
    ]
    assert top_categories(rows) == [
        {"category_id": "c", "name": "Belanja", "kakeibo_type": "need", "outflow": 70_000},
        {**UNCATEGORISED, "outflow": 55_000},
        {"category_id": "b", "name": "Makan", "kakeibo_type": "need", "outflow": 40_000},
    ]
    assert top_categories([breakdown_row("Tanpa kategori", 10_000, category_id=None, uncategorized=True)]) == [
        {**UNCATEGORISED, "outflow": 10_000},
    ]
    assert top_categories([breakdown_row("Tanpa kategori", 0, category_id=None, uncategorized=True)]) == []
    assert top_categories([]) == []


def test_a_category_named_like_the_uncategorised_bucket_keeps_a_total_order():
    rows = [
        breakdown_row("Tanpa kategori", 30_000, category_id="d"),
        breakdown_row("Tanpa kategori", 30_000, category_id=None, uncategorized=True),
    ]
    assert [item["category_id"] for item in top_categories(rows)] == [None, "d"]


@pytest.mark.parametrize("limit", [1, 2])
def test_top_categories_respects_a_smaller_limit(limit):
    rows = [breakdown_row(name, amount) for name, amount in [("A", 3), ("B", 2), ("C", 1)]]
    assert len(top_categories(rows, limit)) == limit


def test_uncategorised_spending_ranks_with_the_categories(client, owner):
    add_expense(owner, 90_000, owner["groceries"])
    add_expense(owner, 60_000, None)
    add_expense(owner, 15_000, None)
    add_expense(owner, 40_000, owner["dinner"])
    add_expense(owner, 10_000, owner["misc"])

    report = summary(client)
    assert report["outflow"] == 215_000
    assert report["top_categories"] == [
        {"category_id": owner["groceries"], "name": "Belanja Dapur", "kakeibo_type": "need", "outflow": 90_000},
        {**UNCATEGORISED, "outflow": 75_000},
        {"category_id": owner["dinner"], "name": "Makan Bareng", "kakeibo_type": "want", "outflow": 40_000},
    ]
    # "Lain-lain" is cut by the limit, so the list falls short of the outflow by exactly its amount.
    assert listed_total(report) == report["outflow"] - 10_000


@pytest.mark.parametrize("groups", [1, 2, 3])
def test_listed_amounts_equal_the_outflow_when_no_group_is_cut(client, owner, groups):
    spending = [(None, 70_000), (owner["groceries"], 50_000), (owner["dinner"], 30_000)][:groups]
    for category_id, amount in spending:
        add_expense(owner, amount, category_id)

    for period in ("today", "month"):
        report = summary(client, period)
        assert len(report["top_categories"]) == groups
        assert listed_total(report) == report["outflow"] == sum(amount for _, amount in spending)
        assert report["top_categories"][0] == {**UNCATEGORISED, "outflow": 70_000}


def test_listed_amounts_never_exceed_the_outflow(client, owner):
    snacks = add_category(owner, "Camilan", pillar="want")
    for category_id, amount in [
        (owner["groceries"], 80_000), (owner["dinner"], 60_000), (snacks, 40_000), (owner["misc"], 20_000), (None, 5_000),
    ]:
        add_expense(owner, amount, category_id)

    report = summary(client)
    assert report["outflow"] == 205_000
    assert pairs(report) == [("Belanja Dapur", 80_000), ("Makan Bareng", 60_000), ("Camilan", 40_000)]
    assert listed_total(report) == 180_000 < report["outflow"]

    record(client, owner, "reimbursement", 70_000, tx_type="income", offset="groceries")
    netted = summary(client)
    assert netted["outflow"] == 135_000
    assert pairs(netted) == [("Makan Bareng", 60_000), ("Camilan", 40_000), ("Lain-lain", 20_000)]
    assert listed_total(netted) <= netted["outflow"]


def test_reimbursements_net_their_category_and_leave_uncategorised_spending_whole(client, owner):
    add_expense(owner, 100_000, None)
    record(client, owner, "dinner", 300_000)
    record(client, owner, "groceries", 50_000)
    record(client, owner, "reimbursement", 250_000, tx_type="income", offset="dinner")
    # Without an offset category a reimbursement is unallocated and lowers no spending, uncategorised included.
    record(client, owner, "reimbursement", 40_000, tx_type="income")

    for period in ("today", "month"):
        report = summary(client, period)
        assert (report["outflow"], report["reimbursement_offset"], report["reimbursement_unallocated"]) == (
            200_000, 250_000, 40_000,
        )
        assert report["top_categories"] == [
            {**UNCATEGORISED, "outflow": 100_000},
            {"category_id": owner["groceries"], "name": "Belanja Dapur", "kakeibo_type": "need", "outflow": 50_000},
            {"category_id": owner["dinner"], "name": "Makan Bareng", "kakeibo_type": "want", "outflow": 50_000},
        ]
        assert listed_total(report) == report["outflow"]


def test_another_owners_uncategorised_spending_is_not_listed(client, owner):
    from app.main import app
    from app.services.auth import get_current_user

    stranger, stranger_cash = add_stranger_account(owner)
    add_expense(owner, 900_000, None, user_id=stranger, account_id=stranger_cash)
    add_expense(owner, 20_000, None)
    record(client, owner, "groceries", 10_000)

    report = summary(client)
    assert report["outflow"] == 30_000
    assert report["top_categories"] == [
        {**UNCATEGORISED, "outflow": 20_000},
        {"category_id": owner["groceries"], "name": "Belanja Dapur", "kakeibo_type": "need", "outflow": 10_000},
    ]

    app.dependency_overrides[get_current_user] = lambda: {
        "id": stranger, "username": "stranger-owner", "name": "Rina Contoh", "currency": "IDR",
        "payday_day": 25, "monthly_spending_budget": 3_000_000,
    }
    theirs = summary(client)
    assert (theirs["outflow"], theirs["top_categories"]) == (900_000, [{**UNCATEGORISED, "outflow": 900_000}])
