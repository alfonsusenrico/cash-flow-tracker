"""Which pocket pays, learned from the owner's own choices (learn-payment-pockets), on synthetic data."""

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from app.services.pocket_learning import decide, first_word, payment_key
from test_notification_model_reading import add_pockets
from test_notification_processing_database import count_transactions, ingest, notification_owner  # noqa: F401

JAGO = "com.jago.digitalBanking"
START = datetime(2026, 11, 1, 3, 0, tzinfo=timezone.utc)
STATIONS = ["SPBU 54.602.45 HR M", "SPBU No ID 54 602 62 MBL", "SPBU 54​.60262,RY​.MENGN-", "SPBU 31.102.02 KT"]


# Rule and key ------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("name", STATIONS)
def test_one_station_chain_shares_a_key(name):
    assert first_word(name) == "spbu"


def test_first_word_ignores_case_digits_and_punctuation():
    assert first_word("KREDIVO") == "kredivo" and first_word("  12 fal.ai") == "fal" and first_word("123") is None


@pytest.mark.parametrize("choices,auto,top", [
    ([], False, None),
    ([("t", 1)], False, "t"),
    ([("t", 1), ("t", 1)], True, "t"),
    ([("t", 2)], True, "t"),
    ([("t", 1), ("t", 1), ("t", 1), ("m", 1)], False, "t"),
    ([("t", 1)] * 5 + [("m", 1)], True, "t"),
    ([("m", 1), ("m", 1), ("t", 1)], False, "m"),
])
def test_rule_of_succession(choices, auto, top):
    decision = decide(choices, auto_enabled=True)
    assert (decision.auto, decision.account_id) == (auto, top)


def test_two_corrections_away_from_a_habit_ask_again():
    # Newest first: two corrections to Main after five Transportasi answers.
    decision = decide([("m", 2), ("m", 2)] + [("t", 1)] * 5, auto_enabled=True)
    assert not decision.auto


def test_only_the_latest_ten_choices_count():
    decision = decide([("m", 1)] * 3 + [("t", 1)] * 10, auto_enabled=True)
    assert (decision.agreeing, decision.support) == (7, 10)


def test_kill_switch_never_records():
    assert not decide([("t", 1)] * 5, auto_enabled=False).auto


def test_debit_card_payments_share_the_notification_kind():
    from app.services.notification_evidence import collect_facts

    keys = {payment_key(collect_facts({"package_name": JAGO, "body_text": f"You've paid Rp{amount} using your debit card.",
                                       "post_time": START}), []) for amount in ("45.000", "189.667")}
    assert len(keys) == 1 and next(iter(keys)).startswith("jago|signature:")


def test_replay_of_the_owners_ten_payments():
    """The owner's 10 Jago payments without a pocket, 28 Sep to 13 Nov 2026, as keys and the pocket chosen."""
    payments = [("debit", "Subscription"), ("goffee", "Main"), ("kredivo", "Cicilan"), ("spbu", "Transportasi"),
                ("tjoekoer", "Transportasi"), ("jeanny", "Gopay"), ("kredivo", "Cicilan"), ("spbu", "Transportasi"),
                ("debit", "Subscription"), ("spbu", "Transportasi")]
    history: dict[str, list[tuple[str, int]]] = {}
    automatic = wrong = asked = preselected = 0
    for key, pocket in payments:
        decision = decide(history.get(key, []), auto_enabled=True)
        if decision.auto:
            automatic += 1
            wrong += decision.account_id != pocket
        else:
            asked += 1
            preselected += decision.account_id == pocket
        history.setdefault(key, []).insert(0, (pocket, 1))
    assert (automatic, wrong, asked, preselected) == (1, 0, 9, 3)


# Database scenarios --------------------------------------------------------------------------------------------

def paid(merchant, amount="150.000", *, minutes=0, version="2.4.1"):
    payload = {"device_id": "synthetic-device", "package_name": JAGO, "title": "Jago",
               "body_text": f"You have paid Rp {amount} to {merchant}.", "payload_hash": uuid4().hex,
               "post_time": (START + timedelta(minutes=minutes)).isoformat()}
    if version:
        payload["source_version"] = version
    return payload


def complete(client, event_id):
    from app.services import notification_processing as processing

    claimed = processing.claim_event(include_ai=False)
    if claimed:
        asyncio.run(processing.process_claim(claimed, None))
    return client.get(f"/api/ingest/notifications/{event_id}/result").json()["result"]


def answer(client, result, account_id):
    response = client.post(f"/api/ingest/notifications/{result['event_id']}/confirm-source-pocket", json={
        "question_id": result["confirmation"]["question_id"], "reply_id": str(uuid4()), "account_id": account_id})
    assert response.status_code == 200, response.text
    return complete(client, result["event_id"])


def pay_and_answer(client, merchant, account_id, *, minutes):
    asked = ingest(client, paid(merchant, minutes=minutes))["results"][0]
    assert asked["status"] == "needs_confirmation", asked
    recorded = answer(client, asked, account_id)
    assert recorded["status"] == "recorded"
    return recorded


def account_of(event_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return str(conn.execute(
            """SELECT t.account_id FROM notification_events n JOIN transactions t ON t.id = n.transaction_id
               WHERE n.id = %s""", (event_id,)).fetchone()["account_id"])


@pytest.fixture
def pockets(notification_owner):
    return add_pockets(notification_owner, "Transportasi", "Cicilan")


def test_first_payment_to_a_merchant_asks_with_the_default_preselected(client, notification_owner, pockets):
    result = ingest(client, paid(STATIONS[0]))["results"][0]
    assert result["status"] == "needs_confirmation"
    assert result["confirmation"]["type"] == "source_pocket"
    assert result["confirmation"]["suggested_account_id"] == notification_owner["main"]
    assert count_transactions(notification_owner) == 0


def test_two_agreeing_answers_record_the_next_payment_and_say_why(client, notification_owner, pockets):
    for minutes, station in enumerate(STATIONS[:2]):
        pay_and_answer(client, station, pockets["Transportasi"], minutes=minutes)
    learned = ingest(client, paid(STATIONS[2], minutes=5))["results"][0]
    assert learned["status"] == "recorded", learned
    assert account_of(learned["event_id"]) == pockets["Transportasi"]
    assert learned["pocket_habit"] == {"label": "Kantong dari kebiasaan: Transportasi",
                                       "basis": "2 dari 2 pilihan sebelumnya"}


def test_one_answer_only_preselects(client, notification_owner, pockets):
    pay_and_answer(client, STATIONS[0], pockets["Transportasi"], minutes=0)
    asked = ingest(client, paid(STATIONS[1], minutes=1))["results"][0]
    assert asked["status"] == "needs_confirmation"
    assert asked["confirmation"]["suggested_account_id"] == pockets["Transportasi"]


def test_a_web_correction_teaches_and_replaces_the_answer(client, notification_owner, pockets):
    first = pay_and_answer(client, STATIONS[0], notification_owner["main"], minutes=0)
    response = client.patch(f"/api/transactions/{first['transaction_id']}", json={"account_id": pockets["Transportasi"]})
    assert response.status_code == 200, response.text
    learned = ingest(client, paid(STATIONS[1], minutes=1))["results"][0]
    assert learned["status"] == "recorded" and account_of(learned["event_id"]) == pockets["Transportasi"]


def test_thin_evidence_with_an_exception_keeps_asking(client, notification_owner, pockets, monkeypatch):
    from app.core import config

    # Learned recording off while the history is built, so every payment is answered by hand.
    monkeypatch.setattr(config, "settings", replace(config.settings, pocket_learning_auto_record_enabled=False))
    history = [pockets["Transportasi"]] * 3 + [notification_owner["main"]]
    for minutes, account in enumerate(history):
        pay_and_answer(client, STATIONS[minutes], account, minutes=minutes)
    monkeypatch.setattr(config, "settings", replace(config.settings, pocket_learning_auto_record_enabled=True))
    asked = ingest(client, paid(STATIONS[0], minutes=9))["results"][0]
    assert asked["status"] == "needs_confirmation"
    assert asked["confirmation"]["suggested_account_id"] == pockets["Transportasi"]


def test_other_merchants_are_learned_separately(client, notification_owner, pockets):
    for minutes in range(2):
        pay_and_answer(client, STATIONS[minutes], pockets["Transportasi"], minutes=minutes)
    asked = ingest(client, paid("KREDIVO", "550.050", minutes=5))["results"][0]
    assert asked["status"] == "needs_confirmation"


def test_an_archived_learned_pocket_is_asked(client, notification_owner, pockets):
    from app.db.pool import db_conn

    for minutes in range(2):
        pay_and_answer(client, STATIONS[minutes], pockets["Transportasi"], minutes=minutes)
    with db_conn() as conn:
        conn.execute("UPDATE accounts SET is_archived = TRUE WHERE id = %s", (pockets["Transportasi"],))
        conn.commit()
    asked = ingest(client, paid(STATIONS[2], minutes=5))["results"][0]
    assert asked["status"] == "needs_confirmation"
    assert asked["confirmation"]["suggested_account_id"] == notification_owner["main"]


def test_kill_switch_asks_with_the_learned_pocket_preselected(client, notification_owner, pockets, monkeypatch):
    from app.core import config

    for minutes in range(2):
        pay_and_answer(client, STATIONS[minutes], pockets["Transportasi"], minutes=minutes)
    monkeypatch.setattr(config, "settings", replace(config.settings, pocket_learning_auto_record_enabled=False))
    asked = ingest(client, paid(STATIONS[2], minutes=5))["results"][0]
    assert asked["status"] == "needs_confirmation"
    assert asked["confirmation"]["suggested_account_id"] == pockets["Transportasi"]


def test_incoming_money_keeps_the_default_pocket(client, notification_owner, pockets):
    payload = {**paid("x"), "body_text": "Andra Contoh has sent Rp50.000 to you."}
    result = ingest(client, payload)["results"][0]
    assert result["status"] == "recorded" and account_of(result["event_id"]) == notification_owner["main"]


def test_an_older_companion_reviews_without_assuming(client, notification_owner, pockets):
    result = ingest(client, paid(STATIONS[0], version=None))["results"][0]
    assert result["status"] == "needs_review" and count_transactions(notification_owner) == 0


def test_a_payment_naming_its_pocket_is_not_asked(client, notification_owner, pockets):
    payload = {**paid("x"), "body_text": "Payment of Rp150.000 to SPBU Contoh using Transportasi Pocket was successful."}
    result = ingest(client, payload)["results"][0]
    assert result["status"] == "recorded" and account_of(result["event_id"]) == pockets["Transportasi"]


# Money added into a pocket with no named source learns per target pocket --------------------------------------

def test_money_into_a_pocket_learns_its_usual_source_and_mixed_sources_keep_asking(client, notification_owner, monkeypatch):
    from app.core import config
    from test_notification_model_reading import POSTED, add_pockets as add, added, answer_source, movements, process
    from test_notification_model_reading import provider_for, reading
    from test_notification_processing_database import enable_ai

    enable_ai(monkeypatch)
    pocket = add(notification_owner, "Bayar Kredit", "Tabungan", "Makan-Jalan")
    provider = provider_for(reading(target=pocket["Bayar Kredit"]))

    def top_up(minutes):
        return ingest(client, added("47.619", posted=POSTED + timedelta(minutes=minutes)))["results"][0]

    monkeypatch.setattr(config, "settings", replace(config.settings, pocket_learning_auto_record_enabled=False))
    for minutes, source in enumerate(["Makan-Jalan", "Makan-Jalan", "Tabungan"]):
        asked = process(client, top_up(minutes), provider)
        assert answer_source(client, asked, pocket[source]).status_code == 200
        assert process(client, asked, provider)["status"] == "recorded"
    monkeypatch.setattr(config, "settings", replace(config.settings, pocket_learning_auto_record_enabled=True))
    mixed = process(client, top_up(10), provider)
    assert mixed["status"] == "needs_confirmation"
    assert mixed["confirmation"]["suggested_account_id"] == pocket["Makan-Jalan"]
    # One Tabungan exception: the habit needs 5 Makan-Jalan of 6 choices, (5 + 1) / (6 + 2) = 0.75.
    for minutes, asked in ((10, mixed), (11, None), (12, None)):
        asked = asked or process(client, top_up(minutes), provider)
        assert asked["status"] == "needs_confirmation"
        assert answer_source(client, asked, pocket["Makan-Jalan"]).status_code == 200
        process(client, asked, provider)
    learned = process(client, top_up(13), provider)
    assert learned["status"] == "recorded" and learned["source"].endswith("Makan-Jalan")
    assert learned["pocket_habit"]["basis"] == "5 dari 6 pilihan sebelumnya"
    assert len(movements(notification_owner)) == 7
