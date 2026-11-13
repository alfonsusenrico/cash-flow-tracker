from datetime import datetime, timezone

import pytest

from app.services.notification_evidence import collect_facts
from app.services.notification_parser import parse_notification


JAGO = "com.jago.digitalBanking"
ADDED = "You've added Rp47.619 into Bayar Kredit Locked Pocket. Need help? Contact Tanya Jago at 1500 746."


def facts_for(text, package=JAGO, **extra):
    return collect_facts({"package_name": package, "title": "Jago", "body_text": text,
                          "post_time": datetime(2026, 11, 13, 2, 42, 9, tzinfo=timezone.utc), **extra})


def test_unrecognised_settled_wording_waits_for_a_reading():
    facts = facts_for(ADDED)
    assert facts.status == "candidate" and facts.error_code is None
    assert facts.needs_reading and facts.amount == 47619


def test_unrecognised_wording_without_a_direction_cue_is_read_not_dropped():
    facts = facts_for("Rp150.000 sudah masuk ke Kantong Liburan kamu.")
    assert facts.status == "candidate" and facts.needs_reading


@pytest.mark.parametrize("text", [
    "Top up sekarang dan dapatkan bonus Rp10.000!",
    "Pembayaran Rp25.000 ke Toko Contoh gagal",
    "Laba Emiten Contoh naik menjadi Rp2,1 triliun",
])
def test_noise_stays_ignored_without_a_reading(text):
    facts = facts_for(text)
    assert facts.status == "ignored" and not facts.needs_reading


def test_recognised_wording_needs_no_reading():
    facts = facts_for("You've moved Rp10.333 from Subscriptions Pocket to your Tabungan Pocket.")
    assert facts.status == "candidate" and not facts.needs_reading
    assert (facts.parsed.source_pocket, facts.parsed.target_pocket) == ("Subscriptions", "Tabungan")


def test_into_pocket_rule_needs_a_move_verb():
    parsed = parse_notification(JAGO, "Jago", ADDED, None)
    assert parsed.source_pocket != "Kantong Utama"
    moved = parse_notification(JAGO, "Jago", "You've moved Rp500.000 into your Tabungan Pocket.", None)
    assert (moved.direction, moved.source_pocket, moved.target_pocket) == ("internal", "Kantong Utama", "Tabungan")


# Database scenarios: the model reads unrecognised wording, and the backend keeps every money decision.
import asyncio
import json
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

from app.services.notification_interpretation import Interpretation
from test_notification_processing_database import count_transactions, enable_ai, ingest, notification_owner  # noqa: F401

POSTED = datetime(2026, 11, 13, 2, 42, 9, tzinfo=timezone.utc)


def added(amount="47.619", pocket="Bayar Kredit", *, posted=POSTED):
    return {
        "device_id": "synthetic-device", "package_name": JAGO, "title": "Jago", "source_version": "2.4.1",
        "body_text": f"You've added Rp{amount} into {pocket} Locked Pocket. Need help? Contact Tanya Jago at 1500 746.",
        "payload_hash": uuid4().hex, "post_time": posted.isoformat(),
        "expected_amount": float(amount.replace(".", "")), "expected_direction": "internal",
    }


def add_pockets(owner, *names):
    from app.db.pool import db_conn

    ids = {}
    with db_conn() as conn:
        for name in names:
            ids[name] = str(uuid4())
            conn.execute("INSERT INTO accounts (id, user_id, parent_id, name, type, initial_balance) VALUES (%s,%s,%s,%s,'bank',1000000)",
                         (ids[name], owner["user_id"], owner["jago"], name))
        conn.commit()
    return ids


def reading(*, amount="47.619", target, target_quote="Bayar Kredit Locked", source=None, source_quote="",
            confidence=0.92, direction_quote="You've added", outcome="record"):
    record = outcome == "record"
    return Interpretation.model_validate_json(json.dumps({
        "outcome": outcome, "direction": "internal_movement" if record else None,
        "description": "Pindah ke Bayar Kredit" if record else "", "merchant": None,
        "account_id": None, "source_account_id": source if record else None,
        "target_account_id": target if record else None, "category_id": None, "kakeibo": None,
        "amount_evidence": f"Rp{amount}" if record else "", "direction_evidence": direction_quote if record else "",
        "source_evidence": source_quote, "target_evidence": target_quote if record else "",
        "candidate_transaction_id": None, "confidence": confidence,
        "review_reason": "none" if record else "uncertain_facts",
        "mappings": [{"role": "target", "account_id": target, "confidence": 0.95, "alternatives": []}] if record else [],
    }))


def provider_for(answer):
    provider = AsyncMock()
    provider.interpret.return_value = answer
    return provider


def process(client, result, provider):
    from app.services import notification_processing as processing

    claimed = processing.claim_event()
    assert str(claimed["id"]) == result["event_id"]
    asyncio.run(processing.process_claim(claimed, provider))
    return client.get(f"/api/ingest/notifications/{result['event_id']}/result").json()["result"]


def answer_source(client, result, account_id):
    return client.post(f"/api/ingest/notifications/{result['event_id']}/confirm-source-pocket", json={
        "question_id": result["confirmation"]["question_id"], "reply_id": str(uuid4()), "account_id": account_id,
    })


def movements(owner):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(
            """SELECT o.account_id AS source, i.account_id AS target, o.amount, o.date
               FROM transactions o JOIN transactions i ON i.movement_id = o.movement_id AND i.type = 'income'
               WHERE o.user_id = %s AND o.type = 'expense' ORDER BY o.date""", (owner["user_id"],)).fetchall()


def hand_entered(client, source, target, amount, when):
    response = client.post("/api/movements", json={
        "source_account_id": source, "target_account_id": target, "amount": amount,
        "notes": "beli makan", "date": when.isoformat(),
    })
    assert response.status_code == 200, response.text


@pytest.fixture
def pockets(notification_owner, monkeypatch):
    enable_ai(monkeypatch)
    return add_pockets(notification_owner, "Bayar Kredit", "Tabungan", "Makan-Jalan")


def test_added_into_locked_pocket_asks_for_the_source_with_a_suggestion(client, notification_owner, pockets):
    owner = notification_owner
    result = ingest(client, added())["results"][0]
    assert result["status"] == "queued"
    provider = provider_for(reading(target=pockets["Bayar Kredit"], source=pockets["Tabungan"]))
    asked = process(client, result, provider)
    assert asked["status"] == "needs_confirmation"
    question = asked["confirmation"]
    assert question["type"] == "source_pocket" and question["amount"] == 47619
    assert question["suggested_account_id"] == pockets["Tabungan"]
    assert question["destination_label"] == "Bayar Kredit"
    options = client.get(f"/api/ingest/notifications/{result['event_id']}/source-pockets").json()["result"]
    assert pockets["Bayar Kredit"] not in {row["id"] for row in options["options"]}
    assert options["suggested_account_id"] == pockets["Tabungan"]
    assert count_transactions(owner) == 0
    context = provider.interpret.await_args.args[0]
    assert context["facts"]["needs_reading"] is True and "recent_movements" in context


def test_owner_answer_records_one_move_without_another_model_call(client, notification_owner, pockets):
    owner = notification_owner
    result = ingest(client, added())["results"][0]
    provider = provider_for(reading(target=pockets["Bayar Kredit"], source=pockets["Tabungan"]))
    asked = process(client, result, provider)
    assert answer_source(client, asked, pockets["Makan-Jalan"]).status_code == 200
    recorded = process(client, asked, provider)
    assert recorded["status"] == "recorded" and recorded["type"] == "internal_movement"
    assert provider.interpret.await_count == 1
    rows = movements(owner)
    assert [(str(row["source"]), str(row["target"]), row["amount"]) for row in rows] == [
        (pockets["Makan-Jalan"], pockets["Bayar Kredit"], 47619)]
    assert rows[0]["date"] == POSTED


def test_answer_naming_the_target_or_nothing_never_debits_the_main_pocket(client, notification_owner, pockets):
    owner = notification_owner
    result = ingest(client, added())["results"][0]
    asked = process(client, result, provider_for(reading(target=pockets["Bayar Kredit"])))
    assert "suggested_account_id" not in asked["confirmation"]
    assert answer_source(client, asked, pockets["Bayar Kredit"]).status_code == 409
    from app.services import notification_processing as processing

    assert processing.claim_event() is None
    assert client.get(f"/api/ingest/notifications/{result['event_id']}/result").json()["result"]["status"] == "needs_confirmation"
    assert count_transactions(owner) == 0


@pytest.mark.parametrize("answer", [
    {"confidence": 0.6},
    {"direction_quote": "You've moved"},
    {"target_quote": "Bayar Kredit Unlocked"},
])
def test_an_unsure_or_unquoted_reading_goes_to_review(client, notification_owner, pockets, answer):
    result = ingest(client, added())["results"][0]
    reviewed = process(client, result, provider_for(reading(target=pockets["Bayar Kredit"], **answer)))
    assert reviewed["status"] == "needs_review" and reviewed["error_code"] == "wording_unconfirmed"
    assert count_transactions(notification_owner) == 0


def test_the_model_can_judge_unrecognised_wording_as_noise(client, notification_owner, pockets):
    payload = {**added(), "body_text": "Tagihan Rp150.000 jatuh tempo besok.", "expected_amount": None,
               "expected_direction": None}
    result = ingest(client, payload)["results"][0]
    assert result["status"] == "queued"
    ignored = process(client, result, provider_for(reading(target=pockets["Bayar Kredit"], outcome="ignored")))
    assert ignored["status"] == "ignored"
    assert count_transactions(notification_owner) == 0


def test_without_the_model_unrecognised_wording_stays_unproven(client, notification_owner):
    result = ingest(client, added())["results"][0]
    assert result["status"] == "needs_review" and result["error_code"] == "settlement_unproven"
    assert count_transactions(notification_owner) == 0


def test_retry_discards_the_reading_and_reads_again(client, notification_owner, pockets):
    from app.db.pool import db_conn

    result = ingest(client, added())["results"][0]
    provider = provider_for(reading(target=pockets["Bayar Kredit"], source=pockets["Tabungan"]))
    process(client, result, provider)
    assert client.post(f"/api/ingest/notifications/{result['event_id']}/retry").status_code == 200
    with db_conn() as conn:
        stored = conn.execute("SELECT model_reading FROM notification_events WHERE id = %s", (result["event_id"],)).fetchone()
    assert stored["model_reading"] is None
    again = process(client, result, provider)
    assert provider.interpret.await_count == 2 and again["status"] == "needs_confirmation"


def test_a_hand_entered_move_is_confirmed_not_duplicated(client, notification_owner, pockets):
    owner = notification_owner
    hand_entered(client, pockets["Makan-Jalan"], pockets["Bayar Kredit"], 130900, POSTED - timedelta(minutes=57))
    result = ingest(client, added("130.900"))["results"][0]
    confirmed = process(client, result, provider_for(reading(amount="130.900", target=pockets["Bayar Kredit"],
                                                             source=pockets["Tabungan"])))
    assert confirmed["status"] == "recorded" and "confirmation" not in confirmed
    assert len(movements(owner)) == 1 and count_transactions(owner) == 2


def test_hand_entered_moves_outside_the_window_or_already_reported_are_not_confirmed(client, notification_owner, pockets):
    owner = notification_owner
    hand_entered(client, pockets["Makan-Jalan"], pockets["Bayar Kredit"], 130900, POSTED - timedelta(hours=49))
    result = ingest(client, added("130.900"))["results"][0]
    provider = provider_for(reading(amount="130.900", target=pockets["Bayar Kredit"]))
    assert process(client, result, provider)["status"] == "needs_confirmation"
    hand_entered(client, pockets["Tabungan"], pockets["Bayar Kredit"], 130900, POSTED - timedelta(hours=1))
    first = ingest(client, added("130.900", posted=POSTED + timedelta(seconds=5)))["results"][0]
    assert process(client, first, provider)["status"] == "recorded"
    second = ingest(client, added("130.900", posted=POSTED + timedelta(seconds=9)))["results"][0]
    assert process(client, second, provider)["status"] == "needs_confirmation"
    assert len(movements(owner)) == 2


def test_several_hand_entered_moves_ask_then_follow_the_answer(client, notification_owner, pockets):
    owner = notification_owner
    hand_entered(client, pockets["Makan-Jalan"], pockets["Bayar Kredit"], 75260, POSTED - timedelta(hours=3))
    hand_entered(client, pockets["Tabungan"], pockets["Bayar Kredit"], 75260, POSTED - timedelta(hours=2))
    result = ingest(client, added("75.260"))["results"][0]
    provider = provider_for(reading(amount="75.260", target=pockets["Bayar Kredit"]))
    asked = process(client, result, provider)
    assert asked["status"] == "needs_confirmation"
    assert answer_source(client, asked, pockets["Tabungan"]).status_code == 200
    confirmed = process(client, asked, provider)
    assert confirmed["status"] == "recorded" and confirmed["source"].endswith("Tabungan")
    assert len(movements(owner)) == 2


def test_the_production_sequence_resolves_once_each(client, notification_owner, pockets):
    """Five Jago top-ups of a locked pocket, four entered by hand first, one under the pocket's old name."""
    owner = notification_owner
    extra = add_pockets(owner, "Dana Darurat Lama")
    wib = timezone(timedelta(hours=7))
    entries = [
        ("75.260", "Spaylater", extra["Dana Darurat Lama"], datetime(2026, 11, 5, 22, 0, tzinfo=wib), datetime(2026, 11, 5, 22, 43, 18, tzinfo=wib)),
        ("108.001", "Bayar Kredit", pockets["Makan-Jalan"], datetime(2026, 11, 7, 18, 0, tzinfo=wib), datetime(2026, 11, 8, 7, 37, 20, tzinfo=wib)),
        ("214.225", "Bayar Kredit", pockets["Makan-Jalan"], datetime(2026, 11, 8, 20, 0, tzinfo=wib), datetime(2026, 11, 9, 9, 53, 53, tzinfo=wib)),
        ("130.900", "Bayar Kredit", pockets["Makan-Jalan"], datetime(2026, 11, 10, 19, 30, tzinfo=wib), datetime(2026, 11, 10, 20, 27, 6, tzinfo=wib)),
        ("47.619", "Bayar Kredit", None, None, datetime(2026, 11, 13, 9, 42, 9, tzinfo=wib)),
    ]
    for amount, _pocket, source, entered, _posted in entries:
        if source:
            hand_entered(client, source, pockets["Bayar Kredit"], int(amount.replace(".", "")), entered)
    outcomes = []
    for amount, pocket, _source, _entered, posted in entries:
        result = ingest(client, added(amount, pocket, posted=posted))["results"][0]
        mapping_confidence = 0.6 if pocket == "Spaylater" else 0.95
        answer = reading(amount=amount, target=pockets["Bayar Kredit"], target_quote=f"{pocket} Locked",
                         source=pockets["Tabungan"])
        answer.mappings[0].confidence = mapping_confidence
        provider = provider_for(answer)
        outcome = process(client, result, provider)
        if outcome.get("mapping_proposal"):
            outcomes.append("mapping")
            response = client.post(f"/api/ingest/notifications/{result['event_id']}/confirm-mapping",
                                   json={"account_id": pockets["Bayar Kredit"]})
            assert response.status_code == 200, response.text
            outcome = process(client, result, provider)
        outcomes.append("question" if outcome["status"] == "needs_confirmation" else outcome["status"])
    assert outcomes == ["mapping", "recorded", "recorded", "recorded", "recorded", "question"]
    assert len(movements(owner)) == 4
