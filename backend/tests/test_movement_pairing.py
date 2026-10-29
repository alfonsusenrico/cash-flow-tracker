"""Fee-tolerant, deferred and owner-completed movement pairing (phase 5).

All texts, names and amounts are synthetic. The owner of `notification_owner` is "Raka Purnama".
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from test_notification_processing_database import (  # noqa: F401 - fixture
    add_account, at, ingest, notification, notification_owner,
)

ROOT = Path(__file__).resolve().parents[2]
GOPAY = "com.gojek.gopay"
BCA_INCOMING = "com.bca"
MYBCA = "com.bca.mybca.omni.android"
SHOPEEPAY = "com.shopeepay.id"


def idr(amount: int) -> str:
    return f"{amount:,}".replace(",", ".")


def gopay_to_own_bca(amount: int, seconds: int = 0) -> dict:
    return at(notification(f"Rp{idr(amount)} udah dikirim ke BCA Raka Purnama.", GOPAY), seconds)


def bca_from_owner(amount: int, seconds: int = 0) -> dict:
    return at(notification(
        f"Pemasukan sebesar IDR {amount:,}.00 dari RA** PUR**MA di kategori Transfer Rekening.", BCA_INCOMING,
    ), seconds)


def rows_of(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(
            """SELECT t.*, c.name AS category, c.kakeibo_type AS category_pillar,
                      c.is_excluded_from_budget AS category_excluded
               FROM transactions t LEFT JOIN categories c ON c.id = t.category_id
               WHERE t.user_id = %s ORDER BY t.type, t.amount""",
            (owner["user_id"],),
        ).fetchall()


def event_of(event_id):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute("SELECT * FROM notification_events WHERE id = %s", (event_id,)).fetchone()


def tx_of_event(event_id):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(
            "SELECT t.* FROM transactions t JOIN notification_events n ON n.transaction_id = t.id WHERE n.id = %s",
            (event_id,),
        ).fetchone()


def result_of(client, event_id):
    return client.get(f"/api/ingest/notifications/{event_id}/result").json()["result"]


def repair():
    from app.services.movement_pairing import repair_unpaired_movements
    return repair_unpaired_movements()


def spent_on(owner, account_key):
    return sum(int(row["amount"]) for row in rows_of(owner)
               if row["type"] == "expense" and str(row["account_id"]) == owner[account_key])


# --- Fee table and evidence -------------------------------------------------------------------


@pytest.mark.parametrize("expense,income,fee", [
    (102500, 100000, 2500), (106500, 100000, 6500), (101000, 100000, 1000), (102900, 100000, 2900),
    (100000, 100000, 0), (112500, 100000, None), (101500, 100000, None), (100000, 102500, None),
])
def test_transfer_fee_table(expense, income, fee):
    from app.services.movement_pairing import transfer_fee
    assert transfer_fee(expense, income) == fee


def test_fee_above_the_maximum_difference_is_never_tolerated(monkeypatch):
    from app.services import movement_pairing
    monkeypatch.setattr(movement_pairing, "settings", replace(
        movement_pairing.settings, movement_transfer_fees=(2500, 15000), movement_fee_max_difference=10000,
    ))
    assert movement_pairing.transfer_fee(115000, 100000) is None
    assert movement_pairing.transfer_fee(102500, 100000) == 2500


@pytest.mark.parametrize("env,expected_fees,error", [
    ({"MOVEMENT_TRANSFER_FEES": "1000, 2500"}, (1000, 2500), None),
    ({"MOVEMENT_TRANSFER_FEES": "2500,15000"}, (), "movement_pairing_configuration_invalid"),
    ({"MOVEMENT_FEE_MAX_DIFFERENCE": "-1"}, (), "movement_pairing_configuration_invalid"),
    ({"MOVEMENT_REPAIR_INTERVAL_SECONDS": "5"}, (), "movement_pairing_configuration_invalid"),
    ({"MOVEMENT_REPAIR_MAX_GAP_SECONDS": "abc"}, (), "movement_pairing_configuration_invalid"),
])
def test_pairing_settings_validation(monkeypatch, env, expected_fees, error):
    from app.core.config import load_settings
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    loaded = load_settings()
    assert loaded.movement_transfer_fees == expected_fees
    assert loaded.movement_pairing_configuration_error == error


def test_default_pairing_settings():
    from app.core.config import load_settings
    loaded = load_settings()
    assert loaded.movement_transfer_fees == (1000, 2500, 2900, 6500)
    assert loaded.movement_fee_max_difference == 10000
    assert loaded.movement_repair_interval_seconds == 600
    assert loaded.movement_repair_lookback_hours == 48
    assert loaded.movement_pairing_configuration_error is None


def test_evidence_strength():
    from app.services.notification_resolution import pairing_evidence
    base = {"currency": "IDR", "transfer": False, "self_identity": False, "external_counterparty": False}
    debit = {**base, "account_id": "a", "root_account_id": "bank", "direction": "expense"}
    top_up = {**base, "account_id": "w", "root_account_id": "wallet", "direction": "income", "transfer": True}
    assert pairing_evidence(debit, top_up) == "weak"
    assert pairing_evidence({**debit, "self_identity": True}, top_up) == "strong"
    assert pairing_evidence(debit, {**top_up, "remote_root_account_id": "bank"}) == "strong"
    assert pairing_evidence(debit, {**top_up, "external_counterparty": True}) is None


# --- Fee tolerance ----------------------------------------------------------------------------


@pytest.mark.parametrize("fee", [2500, 6500, 1000, 2900])
def test_known_fee_pairs_with_equal_legs_and_a_fee_row(client, notification_owner, fee):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(100000 + fee))["results"][0]
    incoming = ingest(client, bca_from_owner(100000, 60))["results"][0]
    assert incoming["type"] == "internal_movement" and incoming["amount"] == 100000, incoming
    rows = rows_of(owner)
    legs = [row for row in rows if row["movement_id"]]
    assert len(legs) == 2 and {int(row["amount"]) for row in legs} == {100000}
    [fee_row] = [row for row in rows if not row["movement_id"]]
    assert int(fee_row["amount"]) == fee and str(fee_row["account_id"]) == owner["gopay"]
    assert fee_row["category"] == "Biaya Admin & Transfer" and fee_row["category_pillar"] == "need"
    assert fee_row["category_excluded"] is False and fee_row["kakeibo_type"] == "need"
    assert (fee_row["source"], fee_row["review_status"], fee_row["classified_by"]) == ("movement", "auto", "system")
    assert fee_row["fee_for_movement_id"] == legs[0]["movement_id"]
    assert {row["review_status"] for row in legs} == {"auto"}
    # The source account still pays the full debit.
    assert spent_on(owner, "gopay") == 100000 + fee
    assert result_of(client, outgoing["event_id"])["type"] == "internal_movement"
    listed = client.get("/api/transactions?limit=10").json()["transactions"]
    assert [entry["amount"] for entry in listed if entry["fee_for_movement_id"] == str(legs[0]["movement_id"])] == [fee]


def test_unknown_or_too_large_difference_is_not_paired(client, notification_owner):
    ingest(client, gopay_to_own_bca(112500))
    incoming = ingest(client, bca_from_owner(100000, 60))["results"][0]
    assert incoming["type"] == "income"
    assert all(row["movement_id"] is None for row in rows_of(notification_owner))


def test_weak_evidence_never_tolerates_a_fee(client, notification_owner):
    add_account(notification_owner, "shopeepay", "ShopeePay")
    ingest(client, at(notification("You spent IDR 51,000.00 at Shopping.", MYBCA), 0))
    top_up = ingest(client, at(notification(
        "Pengisian saldo sebesar Rp50.000 telah ditambahkan ke ShopeePay-mu.", SHOPEEPAY), 30))["results"][0]
    assert top_up["type"] == "income"
    assert all(row["movement_id"] is None for row in rows_of(notification_owner))


def test_split_returns_the_fee_and_is_never_re_paired(client, notification_owner):
    owner = notification_owner
    ingest(client, gopay_to_own_bca(102500))
    ingest(client, bca_from_owner(100000, 60))
    movement_id = next(row["movement_id"] for row in rows_of(owner) if row["movement_id"])
    response = client.post(f"/api/movements/{movement_id}/split")
    assert response.status_code == 200, response.text
    rows = rows_of(owner)
    assert len(rows) == 2 and all(row["movement_id"] is None for row in rows)
    assert {int(row["amount"]) for row in rows} == {102500, 100000}
    assert all(row["owner_split_at"] is not None for row in rows)
    assert spent_on(owner, "gopay") == 102500
    assert repair()["paired"] == 0
    assert all(row["movement_id"] is None for row in rows_of(owner))


def test_deleting_a_movement_deletes_its_fee(client, notification_owner):
    ingest(client, gopay_to_own_bca(106500))
    ingest(client, bca_from_owner(100000, 60))
    movement_id = next(row["movement_id"] for row in rows_of(notification_owner) if row["movement_id"])
    assert client.delete(f"/api/movements/{movement_id}").status_code == 200
    assert rows_of(notification_owner) == []


def test_fee_row_cannot_be_merged_into_a_movement(client, notification_owner):
    from app.db.pool import db_conn
    owner = notification_owner
    ingest(client, gopay_to_own_bca(102500))
    ingest(client, bca_from_owner(100000, 60))
    fee_row = next(row for row in rows_of(owner) if row["fee_for_movement_id"])
    with db_conn() as conn:
        income_id = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date)
               VALUES (%s, %s, %s, 'income', 2500, 'Catatan', NOW()) RETURNING id""",
            (owner["user_id"], owner["bca"], owner["income"]),
        ).fetchone()["id"]
        conn.commit()
    response = client.post("/api/movements/merge", json={
        "expense_transaction_id": str(fee_row["id"]), "income_transaction_id": str(income_id),
    })
    assert response.status_code == 409


# --- Deferred re-pairing ----------------------------------------------------------------------


def test_late_leg_is_paired_by_the_re_pairing_pass(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(102500))["results"][0]
    incoming = ingest(client, bca_from_owner(100000, 40 * 60))["results"][0]
    assert incoming["type"] == "income"
    assert tx_of_event(outgoing["event_id"])["review_status"] == "needs_review"
    counts = repair()
    assert counts["paired"] == 1 and counts["with_fee"] == 1
    legs = [row for row in rows_of(owner) if row["movement_id"]]
    assert len(legs) == 2 and {int(row["amount"]) for row in legs} == {100000}
    assert {row["review_status"] for row in legs} == {"auto"}
    first, second = result_of(client, outgoing["event_id"]), result_of(client, incoming["event_id"])
    assert first["type"] == second["type"] == "internal_movement"
    assert first["record_key"] == second["record_key"] and first["amount"] == 100000
    assert first["description"] == "Pindah saldo ke BCA Harian"
    assert event_of(incoming["event_id"])["confirmed_role"] == "inbound"
    assert repair()["paired"] == 0


def test_late_pair_closes_the_purpose_question(client, notification_owner):
    add_account(notification_owner, "shopeepay", "ShopeePay")
    debit = ingest(client, at(notification("You spent IDR 300,000.00 at Account Transfer.", MYBCA), 0))["results"][0]
    assert event_of(debit["event_id"])["active_question"]["type"] == "purpose"
    ingest(client, at(notification(
        "RAKA PURNAMA mengirimkan dana sebesar Rp300.000 ke ShopeePay-mu melalui BI-Fast.", SHOPEEPAY), 2400))
    assert repair()["paired"] == 1
    assert event_of(debit["event_id"])["active_question"] is None
    assert tx_of_event(debit["event_id"])["review_status"] == "auto"


def test_weak_pair_is_not_linked_outside_the_immediate_window(client, notification_owner):
    add_account(notification_owner, "shopeepay", "ShopeePay")
    ingest(client, at(notification("You spent IDR 50,000.00 at Shopping.", MYBCA), 0))
    ingest(client, at(notification("Pengisian saldo sebesar Rp50.000 telah ditambahkan ke ShopeePay-mu.", SHOPEEPAY), 2400))
    assert repair()["paired"] == 0
    assert all(row["movement_id"] is None for row in rows_of(notification_owner))


def test_manual_internal_movement_row_is_a_candidate(client, notification_owner):
    from app.db.pool import db_conn
    owner = notification_owner
    add_account(owner, "shopeepay", "ShopeePay")
    with db_conn() as conn:
        for account, category, amount in [("bca", "internal_out", 1502500), ("gopay", "expense", 1500000)]:
            conn.execute(
                """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date,
                       source, review_status, classified_by)
                   VALUES (%s, %s, %s, 'expense', %s, 'Catatan sendiri', '2026-09-29T06:00:00Z',
                           'manual', 'reviewed', 'owner')""",
                (owner["user_id"], owner[account], owner[category], amount),
            )
        conn.commit()
    incoming = ingest(client, at(notification(
        "RAKA PURNAMA mengirimkan dana sebesar Rp1.500.000 ke ShopeePay-mu melalui BI-Fast.", SHOPEEPAY), 2400))["results"][0]
    assert incoming["type"] == "income"
    assert repair()["paired"] == 1
    rows = rows_of(owner)
    legs = [row for row in rows if row["movement_id"]]
    assert {str(row["account_id"]) for row in legs} == {owner["bca"], owner["shopeepay"]}
    assert {int(row["amount"]) for row in legs} == {1500000}
    # The manual row stays the owner's reviewed classification.
    assert next(row for row in legs if row["type"] == "expense")["review_status"] == "reviewed"
    assert any(row["fee_for_movement_id"] and int(row["amount"]) == 2500 for row in rows)
    other = next(row for row in rows if str(row["account_id"]) == owner["gopay"])
    assert other["movement_id"] is None and other["notes"] == "Catatan sendiri"


def test_owner_reclassified_notification_row_is_not_a_candidate(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(100000))["results"][0]
    row = tx_of_event(outgoing["event_id"])
    response = client.patch(f"/api/transactions/{row['id']}", json={"category_id": owner["expense"]})
    assert response.status_code == 200, response.text
    ingest(client, bca_from_owner(100000, 40 * 60))
    assert repair()["paired"] == 0


def test_concurrent_passes_pair_once(client, notification_owner):
    from app.db.pool import db_conn
    ingest(client, gopay_to_own_bca(102500))
    ingest(client, bca_from_owner(100000, 40 * 60))
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: repair(), range(2)))
    assert sum(result["paired"] for result in results) == 1
    with db_conn() as conn:
        movements = conn.execute(
            "SELECT COUNT(DISTINCT movement_id) AS n, COUNT(movement_id) AS legs FROM transactions WHERE user_id = %s",
            (notification_owner["user_id"],)).fetchone()
        fees = conn.execute(
            "SELECT COUNT(*) AS n FROM transactions WHERE user_id = %s AND fee_for_movement_id IS NOT NULL",
            (notification_owner["user_id"],)).fetchone()
    assert (movements["n"], movements["legs"], fees["n"]) == (1, 2, 1)


def test_pass_ignores_legs_recorded_before_the_lookback(client, notification_owner):
    from app.db.pool import db_conn
    ingest(client, gopay_to_own_bca(100000))
    ingest(client, bca_from_owner(100000, 40 * 60))
    with db_conn() as conn:
        conn.execute("UPDATE transactions SET created_at = NOW() - INTERVAL '49 hours' WHERE user_id = %s",
                     (notification_owner["user_id"],))
        conn.commit()
    assert repair()["paired"] == 0


# --- Awaiting counterpart and completion ------------------------------------------------------


def overview(client):
    response = client.get("/api/dashboard/overview?timeframe=90d")
    assert response.status_code == 200, response.text
    return response.json()


def test_one_sided_self_transfer_awaits_its_counterpart_and_is_not_spending(client, notification_owner):
    before = overview(client)["kpis"]
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    assert outgoing["type"] == "expense"
    row = tx_of_event(outgoing["event_id"])
    assert row["review_status"] == "needs_review" and row["movement_id"] is None
    event = event_of(outgoing["event_id"])
    assert "awaiting_counterpart" in event["interpretation"]["review_reasons"]
    assert event["active_question"] is None
    after = overview(client)["kpis"]
    assert (after["total_outflow"], after["total_inflow"]) == (before["total_outflow"], before["total_inflow"])


def test_complete_outbound_leg_creates_the_inbound_leg_once(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    row = tx_of_event(outgoing["event_id"])
    body = {"transaction_id": str(row["id"]), "target_account_id": owner["bca"]}
    response = client.post("/api/movements/complete", json=body)
    assert response.status_code == 200, response.text
    completed = response.json()
    assert completed["idempotent"] is False and completed["expense_transaction_id"] == str(row["id"])
    legs = rows_of(owner)
    assert len(legs) == 2 and {int(leg["amount"]) for leg in legs} == {500000}
    assert len({leg["movement_id"] for leg in legs}) == 1 and legs[0]["movement_id"]
    inbound = next(leg for leg in legs if leg["type"] == "income")
    assert str(inbound["account_id"]) == owner["bca"] and inbound["source"] == "movement"
    assert {leg["review_status"] for leg in legs} == {"reviewed"}
    assert result_of(client, outgoing["event_id"])["type"] == "internal_movement"
    again = client.post("/api/movements/complete", json=body)
    assert again.status_code == 200 and again.json()["idempotent"] is True
    assert again.json()["movement_id"] == completed["movement_id"]
    assert len(rows_of(owner)) == 2
    other = client.post("/api/movements/complete", json={**body, "target_account_id": owner["funding"]})
    assert other.status_code == 409 and other.json()["detail"]["code"] == "already_linked"


def test_complete_inbound_leg_creates_the_outbound_leg(client, notification_owner):
    owner = notification_owner
    incoming = ingest(client, notification("Raka Purnama has sent Rp125.000 to you."))["results"][0]
    row = tx_of_event(incoming["event_id"])
    assert row["review_status"] == "needs_review"
    wrong_side = client.post("/api/movements/complete", json={"transaction_id": str(row["id"]), "target_account_id": owner["bca"]})
    assert wrong_side.status_code == 422 and wrong_side.json()["detail"]["code"] == "counterpart_side_mismatch"
    response = client.post("/api/movements/complete", json={"transaction_id": str(row["id"]), "source_account_id": owner["bca"]})
    assert response.status_code == 200, response.text
    outbound = next(leg for leg in rows_of(owner) if leg["type"] == "expense")
    assert str(outbound["account_id"]) == owner["bca"] and int(outbound["amount"]) == 125000
    assert outbound["movement_id"] == tx_of_event(incoming["event_id"])["movement_id"]


def test_complete_validation(client, notification_owner):
    from app.db.pool import db_conn
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    tx_id = str(tx_of_event(outgoing["event_id"])["id"])
    stranger = str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Lain', 'x', 'TEST')",
                     (stranger, f"other_{uuid4().hex}"))
        foreign_account = conn.execute("INSERT INTO accounts (user_id, name, type) VALUES (%s, 'Lain', 'bank') RETURNING id",
                                       (stranger,)).fetchone()["id"]
        foreign_tx = conn.execute(
            "INSERT INTO transactions (user_id, account_id, type, amount, date) VALUES (%s, %s, 'expense', 500000, NOW()) RETURNING id",
            (stranger, foreign_account)).fetchone()["id"]
        plain = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date)
               VALUES (%s, %s, %s, 'expense', 500000, NOW()) RETURNING id""",
            (owner["user_id"], owner["bca"], owner["expense"])).fetchone()["id"]
        conn.commit()
    try:
        cases = [
            ({"transaction_id": tx_id}, 422, None),
            ({"transaction_id": tx_id, "target_account_id": owner["bca"], "source_account_id": owner["bca"]}, 422, None),
            ({"transaction_id": str(uuid4()), "target_account_id": owner["bca"]}, 404, None),
            ({"transaction_id": str(foreign_tx), "target_account_id": owner["bca"]}, 404, None),
            ({"transaction_id": tx_id, "target_account_id": str(foreign_account)}, 404, None),
            ({"transaction_id": tx_id, "target_account_id": owner["gopay"]}, 422, "same_account"),
            ({"transaction_id": tx_id, "target_account_id": owner["stockbit"]}, 422, "investment_trade_required"),
            ({"transaction_id": tx_id, "target_account_id": owner["jago"]}, 422, "account_has_pockets"),
            ({"transaction_id": str(plain), "target_account_id": owner["gopay"]}, 422, "not_awaiting_counterpart"),
        ]
        for body, status, code in cases:
            response = client.post("/api/movements/complete", json=body)
            assert response.status_code == status, (body, response.text)
            if code:
                assert response.json()["detail"]["code"] == code
        assert all(row["movement_id"] is None for row in rows_of(owner))
    finally:
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (stranger,))
            conn.commit()


def test_awaiting_leg_is_paired_when_the_counterpart_arrives_late(client, notification_owner):
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    ingest(client, bca_from_owner(500000, 3 * 3600))
    assert repair()["paired"] == 1
    assert tx_of_event(outgoing["event_id"])["review_status"] == "auto"


# --- Cross-institution guard ------------------------------------------------------------------


def test_weak_pair_with_a_repeated_amount_is_flagged_not_linked(client, notification_owner):
    owner = notification_owner
    add_account(owner, "shopeepay", "ShopeePay")
    third_party = ingest(client, at(notification("Rp50.000 udah dikirim ke BCA Mira Langit.", GOPAY), 0))["results"][0]
    top_up = ingest(client, at(notification(
        "Pengisian saldo sebesar Rp50.000 telah ditambahkan ke ShopeePay-mu.", SHOPEEPAY), 30))["results"][0]
    debit = ingest(client, at(notification("You spent IDR 50,000.00 at Shopping.", MYBCA), 60))["results"][0]
    assert debit["type"] == "expense"
    assert all(row["movement_id"] is None for row in rows_of(owner))
    for event_id in (top_up["event_id"], debit["event_id"]):
        assert tx_of_event(event_id)["review_status"] == "needs_review"
        assert "ambiguous_movement" in event_of(event_id)["interpretation"]["review_reasons"]
    assert "ambiguous_movement" not in (event_of(third_party["event_id"])["interpretation"].get("review_reasons") or [])
    assert repair()["paired"] == 0


def test_strong_pair_is_linked_despite_a_repeated_amount(client, notification_owner):
    ingest(client, at(notification("You've paid Rp100.000 to Kedai Awan"), 0))
    ingest(client, gopay_to_own_bca(100000, 10))
    incoming = ingest(client, bca_from_owner(100000, 60))["results"][0]
    assert incoming["type"] == "internal_movement"


# --- Reports and schema -----------------------------------------------------------------------


def test_reports_are_unchanged_for_existing_movements(client, notification_owner):
    from app.db.pool import db_conn
    owner = notification_owner
    ingest(client, gopay_to_own_bca(100000))
    ingest(client, bca_from_owner(100000, 60))
    created = client.post("/api/movements", json={
        "source_account_id": owner["bca"], "target_account_id": owner["gopay"], "amount": 200000,
        "date": "2026-09-29T07:00:00+00:00", "notes": "Isi saldo",
    })
    assert created.status_code == 200, created.text
    with db_conn() as conn:
        before_rows = conn.execute("SELECT * FROM transactions WHERE user_id = %s ORDER BY id", (owner["user_id"],)).fetchall()
    before = (overview(client)["kpis"], client.get("/api/pulse").json())
    counts = repair()
    assert counts["paired"] == 0 and counts["ambiguous"] == 0
    with db_conn() as conn:
        after_rows = conn.execute("SELECT * FROM transactions WHERE user_id = %s ORDER BY id", (owner["user_id"],)).fetchall()
    assert after_rows == before_rows
    assert (overview(client)["kpis"], client.get("/api/pulse").json()) == before


def test_v30_is_idempotent(client):
    from app.db.pool import db_conn
    versioned = ROOT / "db/migrations/V30__movement_pairing_fees_and_splits.sql"
    with db_conn() as conn:
        conn.execute(versioned.read_text())
        conn.execute(versioned.read_text())
        columns = {row["column_name"] for row in conn.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_name = 'transactions' AND column_name IN ('owner_split_at', 'fee_for_movement_id')""").fetchall()}
        conn.rollback()
    assert columns == {"owner_split_at", "fee_for_movement_id"}


# --- Completing when the other leg already exists ---------------------------------------------


def insert_leg(owner, account_key, tx_type, amount, *, category_key, days_from_row=0, row_date=None, source="notification"):
    from app.db.pool import db_conn
    with db_conn() as conn:
        tx_id = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, source, review_status, classified_by)
               VALUES (%s, %s, %s, %s, %s, 'Pindah saldo contoh', %s + %s * INTERVAL '1 day', %s, 'auto', 'rule') RETURNING id""",
            (owner["user_id"], owner[account_key], owner[category_key], tx_type, amount, row_date, days_from_row, source),
        ).fetchone()["id"]
        conn.commit()
    return str(tx_id)


def test_complete_links_an_existing_internal_movement_leg_instead_of_creating_one(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    row = tx_of_event(outgoing["event_id"])
    existing = insert_leg(owner, "bca", "income", 500000, category_key="internal_in", days_from_row=1, row_date=row["date"])
    body = {"transaction_id": str(row["id"]), "target_account_id": owner["bca"]}

    response = client.post("/api/movements/complete", json=body)

    assert response.status_code == 200, response.text
    completed = response.json()
    assert completed["created_transaction_id"] is None and completed["linked_transaction_id"] == existing
    legs = rows_of(owner)
    assert len(legs) == 2, "no third row: the existing leg is linked"
    assert len({leg["movement_id"] for leg in legs}) == 1 and legs[0]["movement_id"]
    assert {leg["review_status"] for leg in legs} == {"reviewed"}
    again = client.post("/api/movements/complete", json=body)
    assert again.status_code == 200 and again.json()["idempotent"] is True
    assert again.json()["linked_transaction_id"] == existing and len(rows_of(owner)) == 2


def test_complete_asks_before_creating_when_another_match_exists(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    row = tx_of_event(outgoing["event_id"])
    income = insert_leg(owner, "bca", "income", 500000, category_key="income", row_date=row["date"])
    body = {"transaction_id": str(row["id"]), "target_account_id": owner["bca"]}

    refused = client.post("/api/movements/complete", json=body)

    assert refused.status_code == 409
    detail = refused.json()["detail"]
    assert detail["code"] == "counterpart_candidates"
    assert [candidate["id"] for candidate in detail["candidates"]] == [income]
    assert len(rows_of(owner)) == 2, "nothing written while the owner decides"
    confirmed = client.post("/api/movements/complete", json={**body, "create_new": True})
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["created_transaction_id"] not in (None, income)
    assert len(rows_of(owner)) == 3


def test_two_internal_movement_matches_are_never_linked_automatically(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    row = tx_of_event(outgoing["event_id"])
    first = insert_leg(owner, "bca", "income", 500000, category_key="internal_in", row_date=row["date"])
    second = insert_leg(owner, "bca", "income", 500000, category_key="internal_in", days_from_row=2, row_date=row["date"])
    refused = client.post("/api/movements/complete", json={"transaction_id": str(row["id"]), "target_account_id": owner["bca"]})
    assert refused.status_code == 409
    assert [candidate["id"] for candidate in refused.json()["detail"]["candidates"]] == [first, second]


@pytest.mark.parametrize(("amount", "days"), [(500000, 5), (499000, 0)])
def test_rows_outside_the_window_or_amount_do_not_block_completion(client, notification_owner, amount, days):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    row = tx_of_event(outgoing["event_id"])
    insert_leg(owner, "bca", "income", amount, category_key="internal_in", days_from_row=-days, row_date=row["date"])
    response = client.post("/api/movements/complete", json={"transaction_id": str(row["id"]), "target_account_id": owner["bca"]})
    assert response.status_code == 200, response.text
    assert response.json()["created_transaction_id"]


def test_review_queue_waits_only_for_flagged_legs(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    flagged = str(tx_of_event(outgoing["event_id"])["id"])
    historical = insert_leg(owner, "gopay", "expense", 75000, category_key="internal_out",
                            row_date=tx_of_event(outgoing["event_id"])["date"], days_from_row=-30)

    queue = client.get("/api/review/queue").json()

    assert [item["id"] for item in queue["awaiting"]] == [flagged]
    grouped = {item["id"] for group in queue["groups"] for item in group["transactions"]}
    assert historical in grouped, "an older one-sided row is an ordinary row to accept, not a pending transfer"
