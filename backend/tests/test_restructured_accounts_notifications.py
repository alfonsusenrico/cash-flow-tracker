"""Notifications after the owner's restructuring (organize-accounts-by-use D8, task 4.1). No rule changes.

Synthetic owner, names and texts. The structure after the move: a "BCA" rekening with pockets "ATM"
(default) and "RDN BCA" (role dana_investasi), and a "Bank Jago" rekening with pockets "Kantong Utama"
(default), "Dana Darurat" (goal-linked) and "GoPay Tabungan" (role tabungan), the account GoPay-app
notifications map to. A link says Jago's reports of GoPay Tabungan repeat the GoPay app's.
"""
from dataclasses import replace
from uuid import uuid4

import pytest

from test_duplicate_arrivals import (
    GOPAY, GOPAY_CREDIT, JAGO, JAGO_CREDIT, MYBCA, MYBCA_DEBIT, add_link, assert_skipped, balances, event_row, legs,
    send,
)
from test_notification_processing_database import (
    RDN_TOP_UP, at, deposit_events, ingest, ingest_with_source_confirmation, notification, transactions_of,
)
from category_keys import seeded_key

START_BALANCE = 1_000_000


@pytest.fixture
def restructured_owner(client, monkeypatch):
    from app.db.pool import db_conn
    from app.main import app
    from app.services import notification_processing as processing
    from app.services.auth import get_current_user

    user_id = str(uuid4())
    keys = ("bca", "atm", "rdn", "jago", "main", "emergency", "gopay", "stockbit",
            "expense", "income", "internal_out", "internal_in", "belanja")
    ids = {key: str(uuid4()) for key in keys}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO users (id, username, name, password_hash, invite_code, language)
                   VALUES (%s, %s, 'Raka Purnama', 'synthetic-hash', 'TEST', 'id')""",
                (user_id, f"restructured_{uuid4().hex}"),
            )
            for key, name, kind, parent, balance, role in [
                ("bca", "BCA", "bank", None, 0, "harian"),
                ("atm", "ATM", "bank", "bca", START_BALANCE, "harian"),
                ("rdn", "RDN BCA", "bank", "bca", START_BALANCE, "dana_investasi"),
                ("jago", "Bank Jago", "bank", None, 0, "harian"),
                ("main", "Kantong Utama", "bank", "jago", START_BALANCE, "harian"),
                ("emergency", "Dana Darurat", "bank", "jago", START_BALANCE, "tabungan"),
                ("gopay", "GoPay Tabungan", "bank", "jago", START_BALANCE, "tabungan"),
                ("stockbit", "Stockbit", "investment", None, 0, "investasi"),
            ]:
                cur.execute(
                    """INSERT INTO accounts (id, user_id, name, type, parent_id, initial_balance, role)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (ids[key], user_id, name, kind, ids[parent] if parent else None, balance, role),
                )
            cur.execute("UPDATE accounts SET default_pocket_id = %s WHERE id = %s", (ids["atm"], ids["bca"]))
            cur.execute("UPDATE accounts SET default_pocket_id = %s WHERE id = %s", (ids["main"], ids["jago"]))
            cur.execute("UPDATE accounts SET default_funding_account_id = %s WHERE id = %s", (ids["rdn"], ids["stockbit"]))
            for key, name, kind in [
                ("expense", "Tagihan & Utilitas", "expense"), ("income", "Transfer Masuk", "income"),
                ("internal_out", "Internal Movement", "expense"), ("internal_in", "Internal Movement", "income"),
                ("belanja", "Belanja", "expense"),
            ]:
                cur.execute("INSERT INTO categories (id, user_id, name, kind, system_key) VALUES (%s, %s, %s, %s, %s)",
                            (ids[key], user_id, name, kind, seeded_key(name)))
            cur.execute("INSERT INTO goals (user_id, name, target_amount) VALUES (%s, 'Dana Darurat Contoh', 500000) RETURNING id", (user_id,))
            cur.execute("INSERT INTO goal_accounts (goal_id, account_id) VALUES (%s, %s)", (cur.fetchone()["id"], ids["emergency"]))
        conn.commit()
    monkeypatch.setattr(processing, "settings", replace(processing.settings, notification_ai_enabled=False))
    app.dependency_overrides[get_current_user] = lambda: {"id": user_id, "name": "Raka Purnama", "currency": "IDR"}
    yield {"user_id": user_id, **ids}
    app.dependency_overrides.clear()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()


def movement_pillar(owner):
    rows = [row for row in legs(owner) if row["movement_role"] == "outbound"]
    assert len(rows) == 1
    return rows[0]["kakeibo_type"]


# --- BCA -> GoPay top-up: myBCA, GoPay and Jago report one arrival -----------------------------------


@pytest.mark.parametrize("order", ["gopay_jago_mybca", "jago_gopay_mybca", "gopay_mybca_jago"])
def test_bca_to_gopay_top_up_books_one_movement_and_the_link_skips_jago(client, restructured_owner, order):
    owner = restructured_owner
    link = add_link(owner)
    reports = {"gopay": (GOPAY_CREDIT, GOPAY), "jago": (JAGO_CREDIT, JAGO), "mybca": (MYBCA_DEBIT, MYBCA)}
    sent = {}
    for index, name in enumerate(order.split("_")):
        text, package = reports[name]
        sent[name] = send(client, text, package, index * 0.7)

    rows = legs(owner)
    assert [(str(row["account_id"]), row["type"]) for row in rows] == [(owner["atm"], "expense"), (owner["gopay"], "income")]
    assert rows[0]["movement_id"] is not None and rows[0]["movement_id"] == rows[1]["movement_id"]
    assert_skipped(sent["jago"]["event_id"], link)
    money = balances(owner)
    assert (money[owner["atm"]], money[owner["gopay"]], money[owner["main"]]) == (500_000, 1_500_000, START_BALANCE)
    # A top-up into a Tabungan pocket is saving.
    assert movement_pillar(owner) == "saving"


# --- Jago pocket moves with GoPay Tabungan as a Jago pocket -------------------------------------------


def test_moving_out_of_gopay_tabungan_records_a_complete_pocket_move(client, restructured_owner):
    owner = restructured_owner
    result = ingest(client, notification(
        "You've moved Rp300.000 out of your GoPay Tabungan Pocket. Need help? Contact Tanya Jago at 1500 746.",
        "com.jago.digitalBanking",
    ))["results"][0]
    assert (result["status"], result["type"], result["amount"]) == ("recorded", "internal_movement", 300_000), result
    assert (result["source"], result["target"]) == ("Bank Jago · GoPay Tabungan", "Bank Jago · Kantong Utama")
    money = balances(owner)
    assert (money[owner["gopay"]], money[owner["main"]]) == (700_000, 1_300_000)
    assert movement_pillar(owner) == "saving"


def test_moving_from_main_pocket_to_gopay_tabungan(client, restructured_owner):
    owner = restructured_owner
    result = ingest(client, notification(
        "Rp250.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket.", "com.jago.digitalBanking",
    ))["results"][0]
    assert (result["status"], result["type"]) == ("recorded", "internal_movement"), result
    assert (result["source"], result["target"]) == ("Bank Jago · Kantong Utama", "Bank Jago · GoPay Tabungan")
    money = balances(owner)
    assert (money[owner["main"]], money[owner["gopay"]]) == (750_000, 1_250_000)
    assert movement_pillar(owner) == "saving"


def test_gopay_payment_is_paid_from_the_gopay_tabungan_pocket(client, restructured_owner):
    result = ingest(client, notification("QRIS Rp32.000 ke Kedai Awan berhasil", "com.gopay.wallet"))["results"][0]
    assert (result["status"], result["type"], result["source"]) == ("recorded", "expense", "Bank Jago · GoPay Tabungan"), result


# --- Jago transfer to the owner's own BCA ------------------------------------------------------------


@pytest.mark.parametrize("jago_last", [True, False])
def test_jago_transfer_to_own_bca_pairs_into_the_atm_pocket(client, restructured_owner, jago_last):
    owner = restructured_owner
    bca = at(notification("You received IDR 625,000.00 from RA*A ***NAMA at Account Transfer category.", MYBCA), 42)
    jago = at(notification("You've transferred Rp625.000 to RAKA PURNAMA. Need help? Contact Tanya Jago at 1500 746.", JAGO), 46)
    first, second = (bca, jago) if jago_last else (jago, bca)
    ingest_with_source_confirmation(client, first, owner)
    paired = ingest_with_source_confirmation(client, second, owner)
    assert paired["type"] == "internal_movement", paired
    assert (paired["source"], paired["target"]) == ("Bank Jago · Kantong Utama", "BCA · ATM")
    rows = transactions_of(owner)
    assert len(rows) == 2 and len({row["movement_id"] for row in rows}) == 1
    money = balances(owner)
    assert (money[owner["main"]], money[owner["atm"]], money[owner["rdn"]]) == (375_000, 1_625_000, START_BALANCE)


# --- An RDN deposit with the RDN as a BCA pocket -----------------------------------------------------


@pytest.mark.parametrize("rdn_first", [True, False])
def test_rdn_deposit_lands_on_the_rdn_pocket(client, restructured_owner, rdn_first):
    owner = restructured_owner
    rdn = at(notification(RDN_TOP_UP, MYBCA), 60)
    jago = at(notification("You've sent Rp225.180 to RAKA PURNAMA.", JAGO), 30)
    first, second = (rdn, jago) if rdn_first else (jago, rdn)
    alone = ingest_with_source_confirmation(client, first, owner)
    if rdn_first:
        assert (alone["type"], alone["target"], alone["description"]) == ("income", "BCA · RDN BCA", "Pindah saldo masuk")
    paired = ingest_with_source_confirmation(client, second, owner)
    assert paired["type"] == "internal_movement", paired
    assert (paired["source"], paired["target"]) == ("Bank Jago · Kantong Utama", "BCA · RDN BCA")
    money = balances(owner)
    assert (money[owner["rdn"]], money[owner["atm"]]) == (START_BALANCE + 225_180, START_BALANCE)
    # Cash into an RDN is not saving.
    assert movement_pillar(owner) is None


def test_bank_and_broker_reports_of_one_rdn_deposit_record_once_on_the_pocket(client, restructured_owner):
    owner = restructured_owner
    bank, broker = deposit_events(0, 3 * 3600 + 120)
    recorded = ingest(client, bank)["results"][0]
    confirmed = ingest(client, broker)["results"][0]
    assert (recorded["type"], recorded["target"]) == ("income", "BCA · RDN BCA"), recorded
    assert confirmed["status"] == "recorded" and confirmed["record_key"] == recorded["record_key"]
    [row] = transactions_of(owner)
    assert str(row["account_id"]) == owner["rdn"]
    assert event_row(confirmed["event_id"])["provenance_kind"] == "deposit_confirmation"


def test_ordinary_bca_spending_lands_on_the_atm_pocket_not_the_rdn(client, restructured_owner):
    result = ingest(client, notification("You spent IDR 45,000.00 at Shopping.", MYBCA))["results"][0]
    assert (result["status"], result["source"]) == ("recorded", "BCA · ATM"), result
