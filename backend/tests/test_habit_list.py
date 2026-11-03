"""GET /api/habits: the owner's learned habits and top-up patterns, read only (redesign-money-and-settings D7).

Every name, amount and text is synthetic. Labels are written as the owner's corrections and reviewed purpose
answers, the evidence the habit decision trusts; top-ups go through the real conversion endpoint.
"""
import logging
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from test_cycle_summary import counted_statements, make_owner, owner, unlimited_rate_limits  # noqa: F401 - fixtures

HABITS = "/api/habits"
START = datetime(2026, 9, 1, 12, tzinfo=timezone.utc)


def table_checksums():
    from app.db.pool import db_conn

    with db_conn() as conn:
        tables = [row["relname"] for row in conn.execute("SELECT relname FROM pg_stat_user_tables ORDER BY relname")]
        return {table: conn.execute(f'SELECT md5(COALESCE(string_agg(t::text, \'|\' ORDER BY t::text), \'\')) AS sum FROM "{table}" t').fetchone()["sum"]
                for table in tables}


def label(owner, amount, *, category="makan", kakeibo="need", merchant="Kedai Awan", account="wallet", kind="correction",
          signature="gopay|expense|merchant-less", institution="gopay", minutes=0):
    """One owner label of a habit key, `minutes` after START; returns its feedback id."""
    row = owner.db(
        """INSERT INTO classification_feedback (user_id, kind, transaction_id, question_id, reply_id, after_merchant,
               after_notes, after_category_id, after_kakeibo_type, after_type, amount, account_id, institution,
               text_signature, after_review_status, local_weekday, local_hour, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, 'makan siang', %s, %s, 'expense', %s, %s, %s, %s, %s, 1, 12, %s) RETURNING id""",
        (owner.user_id, kind, str(uuid4()), str(uuid4()) if kind == "purpose_answer" else None,
         str(uuid4()) if kind == "purpose_answer" else None, merchant, owner[category], kakeibo, amount, owner[account],
         institution, signature, "reviewed" if kind == "purpose_answer" else None, START + timedelta(minutes=minutes)),
    )
    return str(row["id"])


def habits(owner):
    response = owner.session.get(HABITS)
    assert response.status_code == 200, response.text
    return response.json()


def test_a_ready_habit_reads_like_the_spec(owner):
    for minutes, amount in enumerate((30_000, 31_000, 28_000)):
        label(owner, amount, minutes=minutes, kind="purpose_answer")
    data = habits(owner)
    assert data["auto_record"] == {"habits": True, "topups": True}
    assert data["thresholds"] == {"min_confirmations": 3, "min_agreement": 0.9}
    [entry] = data["habits"]
    assert (entry["label"], entry["category"]["name"], entry["kakeibo"]) == ("Kedai Awan", "Makan Contoh", "need")
    assert (entry["confirmations"], entry["agreeing"], entry["agreement"], entry["state"]) == (3, 3, 1.0, "ready")
    assert entry["account"] == {"id": owner["wallet"], "name": "Dompet Contoh"}
    assert entry["amount_anchor"] == 28_000 and entry["key"].startswith("h_")
    assert data["truncated"] is False and data["topups"] == []


def test_learning_and_conflicting_states_and_corrections_weigh_two(owner):
    label(owner, 50_000, signature="gopay|expense|a", minutes=0)
    label(owner, 52_000, signature="gopay|expense|a", minutes=1)
    for minutes, category in enumerate(("makan", "makan", "jajan"), start=10):
        label(owner, 40_000, signature="gopay|expense|b", category=category, kind="purpose_answer", minutes=minutes, merchant="Toko B")
    states = {entry["amount_anchor"]: entry for entry in habits(owner)["habits"]}
    assert states[52_000]["state"] == "learning" and states[52_000]["confirmations"] == 2
    assert states[40_000]["state"] == "conflicting" and states[40_000]["agreeing"] == 2
    # One correction outweighs one answer: two answers and a correction of the other category.
    label(owner, 70_000, signature="gopay|expense|c", kind="purpose_answer", minutes=20, merchant="Toko C")
    label(owner, 70_000, signature="gopay|expense|c", kind="purpose_answer", minutes=21, merchant="Toko C")
    label(owner, 70_000, signature="gopay|expense|c", kind="correction", category="jajan", minutes=22, merchant="Toko C")
    weighted = next(entry for entry in habits(owner)["habits"] if entry["amount_anchor"] == 70_000)
    assert weighted["category"]["name"] == "Jajan Contoh" and weighted["agreement"] == 0.5


def test_undone_labels_do_not_count(owner):
    ids = [label(owner, 30_000 + minutes, minutes=minutes, kind="purpose_answer") for minutes in range(3)]
    from app.db.pool import db_conn
    from app.services.review_undo import revert_feedback

    with db_conn() as conn:
        with conn.cursor() as cur:
            revert_feedback(cur, owner.user_id, [ids[0]])
        conn.commit()
    [entry] = habits(owner)["habits"]
    assert (entry["confirmations"], entry["state"]) == (2, "learning")


def test_two_amount_bands_of_one_merchant(owner):
    for minutes, amount in enumerate((30_000, 29_000, 31_000)):
        label(owner, amount, minutes=minutes, kind="purpose_answer")
    for minutes, amount in enumerate((250_000, 240_000), start=10):
        label(owner, amount, minutes=minutes, kind="purpose_answer")
    entries = sorted(habits(owner)["habits"], key=lambda entry: entry["amount_anchor"])
    assert [(entry["amount_anchor"], entry["state"]) for entry in entries] == [(31_000, "ready"), (240_000, "learning")]
    assert len({entry["key"] for entry in entries}) == 2


def test_merchant_less_label_names_the_institution_and_never_the_signature(owner):
    for minutes in range(3):
        label(owner, 20_000, merchant=None, institution="bca", signature="bca|expense|synthetic-signature", minutes=minutes,
              kind="purpose_answer")
    response = owner.session.get(HABITS)
    [entry] = response.json()["habits"]
    assert entry["label"] == "Tanpa nama toko · BCA"
    assert "synthetic-signature" not in response.text and "bca|expense" not in response.text


def test_ai_and_movement_labels_are_not_habits(owner):
    label(owner, 20_000, kind="capture", minutes=0)  # a capture the owner did not classify
    label(owner, 20_000, category="movement", kakeibo=None, minutes=1)
    assert habits(owner)["habits"] == []


def test_list_is_cut_at_one_hundred_entries(owner):
    from app.services import habit_list

    for index in range(3):
        label(owner, 10_000, signature=f"gopay|expense|{index}", minutes=index)
    original = habit_list.ENTRY_LIMIT
    habit_list.ENTRY_LIMIT = 2
    try:
        data = habits(owner)
    finally:
        habit_list.ENTRY_LIMIT = original
    assert len(data["habits"]) == 2 and data["truncated"] is True
    assert data["habits"][0]["last_confirmed_at"] >= data["habits"][1]["last_confirmed_at"]


def test_another_owner_sees_nothing_and_anonymous_gets_401(make_owner):
    from app.main import app

    first, second = make_owner(), make_owner()
    for minutes in range(3):
        label(first, 30_000, minutes=minutes, kind="purpose_answer")
    assert habits(second)["habits"] == []
    assert TestClient(app, base_url="https://testserver").get(HABITS).status_code == 401


def test_api_key_reads_the_same_list(owner):
    from app.main import app
    from app.services.auth import hash_token

    for minutes in range(3):
        label(owner, 30_000, minutes=minutes, kind="purpose_answer")
    plain = f"synthetic-key-{uuid4().hex}"
    owner.db("INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES (%s, %s, 'synthet')", (owner.user_id, hash_token(plain)))
    phone = TestClient(app, base_url="https://testserver", headers={"Authorization": f"Bearer {plain}"})
    assert phone.get(HABITS).json()["habits"] == habits(owner)["habits"]


def test_read_only_without_model_or_logged_values(owner, monkeypatch, caplog):
    from app.main import app

    for minutes in range(3):
        label(owner, 30_000, minutes=minutes, kind="purpose_answer")

    class NoModel:
        def __getattr__(self, name):
            raise AssertionError(f"model provider used: {name}")

    monkeypatch.setattr(app.state, "notification_provider", NoModel(), raising=False)
    before = table_checksums()
    with caplog.at_level(logging.DEBUG):
        habits(owner)
    assert table_checksums() == before
    logged = " ".join(record.getMessage() for record in caplog.records)
    assert "Kedai Awan" not in logged and "30000" not in logged and "30.000" not in logged


def test_switch_off_is_reported(owner, monkeypatch):
    from dataclasses import replace
    from app.services import habit_list

    monkeypatch.setattr(habit_list, "settings", replace(habit_list.settings, habit_auto_record_enabled=False))
    for minutes in range(3):
        label(owner, 30_000, minutes=minutes, kind="purpose_answer")
    data = habits(owner)
    assert data["auto_record"]["habits"] is False
    # The state describes the evidence; the section words "ready but switched off" from both facts.
    assert data["habits"][0]["state"] == "ready"


def test_statement_count_is_constant(owner, monkeypatch):
    def count() -> int:
        with monkeypatch.context() as patch:
            with counted_statements(patch) as statements:
                habits(owner)
                return len(statements)

    for minutes in range(10):
        label(owner, 30_000, minutes=minutes, signature=f"gopay|expense|{minutes % 3}", kind="purpose_answer")
    small = count()
    for minutes in range(10, 400):
        label(owner, 30_000 + minutes, minutes=minutes, signature=f"gopay|expense|{minutes % 60}", kind="purpose_answer")
    assert small > 0 and count() == small


# --- Learned top-ups ------------------------------------------------------------------------------


@pytest.fixture
def topup_owner(owner):
    owner.account("rdn", "RDN Contoh")
    owner.account("platform", "Bibit Contoh", kind="investment")
    for key, name in (("fund_a", "Reksa Dana A"), ("fund_b", "Reksa Dana B")):
        # Created through the API, as the owner would, so the product is amount-tracked from the start.
        response = owner.session.post("/api/accounts", json={"name": name, "type": "investment", "initial_balance": 0,
                                                             "instrument_type": "mutual_fund", "parent_id": owner["platform"]})
        assert response.status_code == 200, response.text
        owner.ids[key] = response.json()["account"]["id"]
    owner.db("UPDATE accounts SET initial_balance = 5000000 WHERE id = %s", (owner["rdn"],))
    return owner


def converted(owner, amount, target, *, days_ago):
    tx_id = owner.tx("expense", amount, account="rdn", when=datetime.now(timezone.utc) - timedelta(days=days_ago),
                     review_status="needs_review", notes="Pindah saldo keluar")
    response = owner.session.post("/api/investment-topups/from-transaction", json={"transaction_id": tx_id, "target_account_id": owner[target]})
    assert response.status_code == 200, response.text
    return tx_id


def test_a_learned_topup_pattern(topup_owner):
    converted(topup_owner, 112_590, "fund_a", days_ago=40)
    converted(topup_owner, 112_590, "fund_a", days_ago=10)
    [entry] = habits(topup_owner)["topups"]
    assert entry["source_account"] == {"id": topup_owner["rdn"], "name": "RDN Contoh"}
    assert entry["amount"] == 112_590
    assert entry["product"] == {"id": topup_owner["fund_a"], "name": "Reksa Dana A", "parent_name": "Bibit Contoh"}
    assert (entry["confirmations"], entry["state"]) == (2, "learning")


def test_topup_ready_after_three_and_conflicting_after_a_rejection(topup_owner):
    for days in (90, 60, 30):
        converted(topup_owner, 200_000, "fund_b", days_ago=days)
    [entry] = habits(topup_owner)["topups"]
    assert entry["state"] == "ready"
    rejected = topup_owner.tx("expense", 200_000, account="rdn", review_status="needs_review", notes="Pindah saldo keluar")
    response = topup_owner.session.post("/api/review/topup-suggestions/reject",
                                        json={"transaction_id": rejected, "suggested_target_account_id": topup_owner["fund_b"]})
    assert response.status_code == 200, response.text
    [entry] = habits(topup_owner)["topups"]
    assert entry["state"] == "conflicting"
