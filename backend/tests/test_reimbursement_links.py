"""Reimbursements linked to the exact expenses they repay (link-reimbursements-to-expenses). Synthetic data only."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from category_keys import seeded_key

JAKARTA = timezone(timedelta(hours=7))
LAST_CYCLE = datetime(2026, 10, 28, 18, 0, tzinfo=JAKARTA)
THIS_CYCLE = datetime(2026, 10, 31, 9, 0, tzinfo=JAKARTA)


@pytest.fixture
def owner(client):
    from app.main import app
    from app.db.pool import db_conn
    from app.services.auth import get_current_user

    user_id, stranger_id = str(uuid4()), str(uuid4())
    ids = {name: str(uuid4()) for name in ("cash", "food", "shopping", "fun", "other_income", "reimbursement", "internal_in", "stranger_food", "stranger_cash")}
    with db_conn() as conn:
        with conn.cursor() as cur:
            for uid in (user_id, stranger_id):
                cur.execute(
                    "INSERT INTO users (id, username, name, password_hash, invite_code, language) VALUES (%s, %s, 'Dina Contoh', 'synthetic-hash', 'TEST', 'id')",
                    (uid, f"links_{uuid4().hex}"),
                )
            for key, uid in (("cash", user_id), ("stranger_cash", stranger_id)):
                cur.execute(
                    "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'Kas Contoh', 'bank', 10000000)",
                    (ids[key], uid),
                )
            for key, uid, name, kind, pillar, offsets in [
                ("food", user_id, "Makan Contoh", "expense", "need", False),
                ("shopping", user_id, "Belanja Contoh", "expense", "need", False),
                ("fun", user_id, "Hiburan Contoh", "expense", "want", False),
                ("other_income", user_id, "Pendapatan Lain", "income", None, False),
                ("reimbursement", user_id, "Patungan & Ganti Uang", "income", None, True),
                ("internal_in", user_id, "Internal Movement", "income", None, False),
                ("stranger_food", stranger_id, "Makan Contoh", "expense", "need", False),
            ]:
                cur.execute(
                    """INSERT INTO categories (id, user_id, name, kind, kakeibo_type, offsets_spending, system_key)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (ids[key], uid, name, kind, pillar, offsets, seeded_key(name)),
                )
        conn.commit()
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id, "username": "links-owner", "name": "Dina Contoh", "currency": "IDR", "payday_day": 25,
    }
    yield {"user_id": user_id, "stranger_id": stranger_id, **ids}
    app.dependency_overrides.clear()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = ANY(%s)", ([user_id, stranger_id],))
        conn.commit()


def row(owner, category, amount, when, *, tx_type="expense", pillar=None, notes="Contoh", user="user_id", account="cash"):
    from app.db.pool import db_conn

    with db_conn() as conn:
        tx_id = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, kakeibo_type, source)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'manual') RETURNING id""",
            (owner[user], owner[account], owner[category], tx_type, amount, notes, when, pillar),
        ).fetchone()["id"]
        conn.commit()
    return str(tx_id)


def reimbursement(owner, amount, when, notes="Ganti uang contoh"):
    return row(owner, "reimbursement", amount, when, tx_type="income", notes=notes)


def link(client, reimbursement_id, expenses, rest_category=None, expect=200):
    body = {"expense_ids": expenses}
    if rest_category:
        body["rest_category_id"] = rest_category
    response = client.put(f"/api/transactions/{reimbursement_id}/reimburses", json=body)
    assert response.status_code == expect, response.text
    return response.json()


def held(expense_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        rows = conn.execute(
            "SELECT reimbursement_id, amount FROM reimbursement_links WHERE expense_id = %s ORDER BY reimbursement_id", (expense_id,),
        ).fetchall()
    return {str(item["reimbursement_id"]): int(item["amount"]) for item in rows}


def offsets(owner, start, end):
    from app.db.pool import db_conn
    from app.services.reimbursements import load_spending_offsets

    with db_conn() as conn:
        with conn.cursor() as cur:
            return load_spending_offsets(cur, owner["user_id"], start, end)


LAST_WINDOW = (datetime(2026, 9, 30, tzinfo=JAKARTA), datetime(2026, 10, 29, 23, 59, tzinfo=JAKARTA))
THIS_WINDOW = (datetime(2026, 10, 30, tzinfo=JAKARTA), datetime(2026, 11, 29, 23, 59, tzinfo=JAKARTA))


def test_repaid_in_the_next_cycle_lowers_the_cycle_of_the_expenses(client, owner):
    pelangi = row(owner, "food", 60_600, LAST_CYCLE, pillar="culture")
    superindo = row(owner, "food", 193_555, LAST_CYCLE + timedelta(minutes=40), pillar="culture")
    daging = row(owner, "food", 138_225, LAST_CYCLE + timedelta(days=1), pillar="culture")
    friends = [reimbursement(owner, 75_000, THIS_CYCLE + timedelta(hours=hour)) for hour in range(4)]
    for friend in friends:
        link(client, friend, [pelangi, superindo, daging])
    totals = {expense: sum(held(expense).values()) for expense in (pelangi, superindo, daging)}
    assert totals == {pelangi: 60_600, superindo: 193_555, daging: 45_845}
    last = offsets(owner, *LAST_WINDOW)
    assert (last.applied, last.for_category(owner["food"]), last.for_pillar("culture")) == (300_000, 300_000, 300_000)
    this = offsets(owner, *THIS_WINDOW)
    assert (this.applied, this.unallocated, this.excess) == (0, 0, 0)


def test_only_the_expense_it_repaid_and_the_rest_unallocated(client, owner):
    coffee = row(owner, "shopping", 25_500, LAST_CYCLE, pillar="unexpected", notes="beli kopi titipan mama")
    row(owner, "shopping", 54_000, LAST_CYCLE - timedelta(days=20))
    row(owner, "shopping", 65_000, LAST_CYCLE - timedelta(days=3))
    mother = reimbursement(owner, 100_000, LAST_CYCLE + timedelta(hours=12))
    body = link(client, mother, [coffee])
    assert body["reimbursement"]["repays"][0]["amount"] == 25_500 and body["reimbursement"]["unallocated"] == 74_500
    last = offsets(owner, *LAST_WINDOW)
    assert last.for_category_pillar(owner["shopping"], "unexpected") == 25_500
    assert last.for_category_pillar(owner["shopping"], "need") == 0
    assert (last.applied, last.unallocated) == (25_500, 74_500)


def test_two_reimbursements_on_one_expense(client, owner):
    dinner = row(owner, "food", 205_350, THIS_CYCLE)
    first = reimbursement(owner, 68_450, THIS_CYCLE + timedelta(minutes=40))
    second = reimbursement(owner, 68_450, THIS_CYCLE + timedelta(minutes=42))
    link(client, second, [dinner])
    link(client, first, [dinner])
    assert held(dinner) == {first: 68_450, second: 68_450}
    assert offsets(owner, *THIS_WINDOW).for_category(owner["food"]) == 136_900


def test_never_below_zero_and_earlier_reimbursement_first(client, owner):
    game = row(owner, "fun", 80_000, THIS_CYCLE)
    early = reimbursement(owner, 60_000, THIS_CYCLE + timedelta(hours=1))
    late = reimbursement(owner, 60_000, THIS_CYCLE + timedelta(hours=2))
    link(client, late, [game])
    link(client, early, [game])
    assert held(game) == {early: 60_000, late: 20_000}
    this = offsets(owner, *THIS_WINDOW)
    assert (this.for_category(owner["fun"]), this.unallocated) == (80_000, 40_000)


def test_links_are_validated_and_change_nothing_on_refusal(client, owner):
    expense = row(owner, "food", 50_000, THIS_CYCLE)
    money_back = reimbursement(owner, 20_000, THIS_CYCLE + timedelta(hours=1))
    stranger = row(owner, "stranger_food", 50_000, THIS_CYCLE, user="stranger_id", account="stranger_cash")
    salary = row(owner, "other_income", 1_000_000, THIS_CYCLE, tx_type="income")
    cases = [
        (money_back, [stranger], "reimbursement_link_invalid"),
        (money_back, [salary], "reimbursement_link_invalid"),
        (money_back, [str(uuid4())], "reimbursement_link_invalid"),
        (money_back, [], "reimbursement_link_count"),
        (money_back, [expense] * 1 + [str(uuid4()) for _ in range(20)], "reimbursement_link_count"),
        (salary, [expense], "offset_not_allowed"),
    ]
    for target, expenses, code in cases:
        body = link(client, target, expenses, expect=422)
        assert body["detail"]["code"] == code, code
        assert body["detail"]["message"]
    assert held(expense) == {}
    for wrong in (owner["food"], owner["internal_in"]):
        refused = link(client, money_back, [expense], rest_category=wrong, expect=422)
        assert refused["detail"]["code"] == "reimbursement_rest_category"
    assert held(expense) == {}


def test_the_rest_as_income_keeps_the_balance(client, owner):
    from app.db.pool import db_conn

    coffee = row(owner, "shopping", 25_500, LAST_CYCLE, pillar="unexpected")
    mother = reimbursement(owner, 100_000, LAST_CYCLE + timedelta(hours=12), notes="Dari ibu")
    before = client.get("/api/accounts").json()["accounts"]
    body = link(client, mother, [coffee], rest_category=owner["other_income"])
    rest_id = body["rest_transaction_id"]
    assert body["reimbursement"]["unallocated"] == 0
    with db_conn() as conn:
        amounts = {str(item["id"]): (int(item["amount"]), str(item["category_id"]), item["date"], item["notes"])
                   for item in conn.execute("SELECT id, amount, category_id, date, notes FROM transactions WHERE id = ANY(%s::uuid[])",
                                            ([mother, rest_id],)).fetchall()}
    assert amounts[mother][0] == 25_500
    assert amounts[rest_id][:2] == (74_500, owner["other_income"])
    assert amounts[rest_id][2:] == amounts[mother][2:]
    after = client.get("/api/accounts").json()["accounts"]
    balance = lambda accounts: next(item["balance"] for item in accounts if item["id"] == owner["cash"])
    assert balance(after) == balance(before)
    assert offsets(owner, *LAST_WINDOW).unallocated == 0


def test_links_follow_edits_and_deletes(client, owner):
    game = row(owner, "fun", 80_000, THIS_CYCLE)
    first = reimbursement(owner, 60_000, THIS_CYCLE + timedelta(hours=1))
    second = reimbursement(owner, 40_000, THIS_CYCLE + timedelta(hours=2))
    link(client, first, [game])
    link(client, second, [game])
    assert held(game) == {first: 60_000, second: 20_000}
    assert client.patch(f"/api/transactions/{game}", json={"amount": 30_000}).status_code == 200
    assert held(game) == {first: 30_000, second: 0}
    assert client.patch(f"/api/transactions/{game}", json={"amount": 80_000}).status_code == 200
    assert client.delete(f"/api/transactions/{first}").status_code == 200
    assert held(game) == {second: 40_000}
    # A reimbursement whose category stops offsetting loses its links.
    assert client.patch(f"/api/transactions/{second}", json={"category_id": owner["other_income"]}).status_code == 200
    assert held(game) == {}


def test_detail_shows_both_sides_and_unlinking_restores(client, owner):
    game = row(owner, "fun", 80_000, THIS_CYCLE, notes="main BL")
    money_back = reimbursement(owner, 40_000, THIS_CYCLE + timedelta(hours=1), notes="Ganti uang main BL")
    link(client, money_back, [game])
    expense = client.get(f"/api/transactions/{game}").json()["transaction"]["reimbursement"]
    assert expense["repaid_total"] == 40_000
    assert [(item["transaction_id"], item["amount"], item["notes"]) for item in expense["repaid_by"]] == [(money_back, 40_000, "Ganti uang main BL")]
    repays = client.get(f"/api/transactions/{money_back}").json()["transaction"]["reimbursement"]
    assert [(item["transaction_id"], item["amount"]) for item in repays["repays"]] == [(game, 40_000)]
    assert client.delete(f"/api/transactions/{money_back}/reimburses").status_code == 200
    assert held(game) == {}
    assert offsets(owner, *THIS_WINDOW).unallocated == 40_000


def test_category_fallback_is_capped_by_what_links_leave(client, owner):
    from app.db.pool import db_conn

    meal = row(owner, "food", 60_000, THIS_CYCLE)
    row(owner, "food", 40_000, THIS_CYCLE + timedelta(hours=1))
    linked = reimbursement(owner, 60_000, THIS_CYCLE + timedelta(hours=2))
    link(client, linked, [meal])
    by_category = reimbursement(owner, 80_000, THIS_CYCLE + timedelta(hours=3))
    with db_conn() as conn:
        conn.execute("UPDATE transactions SET offset_category_id = %s WHERE id = %s", (owner["food"], by_category))
        conn.commit()
    this = offsets(owner, *THIS_WINDOW)
    assert (this.for_category(owner["food"]), this.excess) == (100_000, 40_000)
    # Linking clears the category link the reimbursement had.
    link(client, by_category, [meal])
    with db_conn() as conn:
        assert conn.execute("SELECT offset_category_id FROM transactions WHERE id = %s", (by_category,)).fetchone()["offset_category_id"] is None
