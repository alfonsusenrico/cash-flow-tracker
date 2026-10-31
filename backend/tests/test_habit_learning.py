"""Habit learning for merchant-less payments (learn-habits-to-ask-less).

All texts, names and amounts are synthetic.
"""
import importlib.util
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.core import config
from app.services.habits import Label, decide, latest_labels, within_band
from test_ingestion_memory_and_purpose import (  # noqa: F401 - fixtures
    BCA, MASKED_INCOMING, answer, bca, enable_ai, event_row, feedback_rows, ingest, normalization, notification,
    notification_owner, owner, result_of, run_claim, tx_for,
)

ROOT = Path(__file__).resolve().parents[2]
START = datetime(2026, 9, 1, 12, tzinfo=timezone.utc)


def label(category="makan", kakeibo="need", *, kind="purpose_answer", minutes=0, transaction=None, hour=12,
          weekday=2, notes=None):
    return Label(
        transaction_id=transaction or str(uuid4()), kind=kind, category_id=category, kakeibo=kakeibo, notes=notes,
        merchant=None, amount=50_000, weekday=weekday, hour=hour, created_at=START + timedelta(minutes=minutes),
    )


def labels(*categories, kind="purpose_answer"):
    return [label(category, kind=kind, minutes=index) for index, category in enumerate(categories)]


def decision(evidence, *, enabled=True, weekday=2, hour=12):
    return decide(evidence, weekday=weekday, hour=hour, auto_enabled=enabled)


# --- Decision rule (no database) ---------------------------------------------------------------


def test_three_agreeing_answers_record_automatically():
    result = decision(labels("makan", "makan", "makan"))
    assert (result.auto, result.category_id, result.kakeibo, result.agreeing, result.support) == (
        True, "makan", "need", 3, 3)
    assert result.basis() == {"category_id": "makan", "kakeibo": "need", "support": 3, "agreeing": 3,
                              "agreement": 1.0}


def test_two_answers_only_suggest():
    result = decision(labels("makan", "makan"))
    assert result.auto is False and result.category_id == "makan" and result.support == 2


def test_three_of_four_agreeing_only_suggest():
    result = decision(labels("makan", "makan", "jajan", "makan"))
    assert result.auto is False and result.category_id == "makan" and result.agreement == 0.75


def test_nine_of_ten_answers_pass_exactly_at_the_threshold():
    result = decision(labels(*["makan"] * 9, "jajan"))
    assert result.auto is True and result.agreement == 0.9


def test_a_correction_weighs_twice_an_answer():
    # One disagreeing answer among nine leaves 90%; one disagreeing correction drops it to 9/11.
    evidence = labels(*["makan"] * 9) + [label("jajan", "want", kind="correction", minutes=60)]
    result = decision(evidence)
    assert result.auto is False and result.agreement == pytest.approx(9 / 11)
    assert decision(labels("makan", "makan", "makan") + [label("jajan", "want", kind="correction", minutes=60)]
                    ).agreement == pytest.approx(3 / 5)


def test_the_pillar_is_part_of_the_agreement():
    evidence = labels("makan", "makan") + [label("makan", "want", minutes=5)]
    assert decision(evidence).auto is False


def test_a_later_correction_replaces_the_answer_of_its_transaction():
    transaction = str(uuid4())
    evidence = labels("makan", "makan") + [
        label("makan", transaction=transaction, minutes=10),
        label("jajan", "want", kind="correction", transaction=transaction, minutes=20),
    ]
    assert [item.kind for item in latest_labels(evidence)].count("purpose_answer") == 2
    result = decision(evidence)
    assert result.support == 3 and result.auto is False and result.agreement == pytest.approx(2 / 4)


def test_only_the_latest_ten_labelled_payments_count():
    evidence = labels(*["jajan"] * 5) + [label("makan", minutes=100 + index) for index in range(10)]
    result = decision(evidence)
    assert (result.support, result.category_id, result.auto) == (10, "makan", True)


def test_time_of_day_breaks_ties_and_picks_the_wording():
    evidence = [
        label("makan", notes="makan siang", hour=12, minutes=1),
        label("jajan", "want", notes="ngopi sore", hour=16, minutes=2),
    ]
    assert decision(evidence, hour=12).category_id == "makan"
    assert decision(evidence, hour=16).category_id == "jajan"
    same = [label("makan", notes="makan siang", hour=12, minutes=1), label("makan", notes="makan malam", hour=19,
                                                                          minutes=2)]
    assert decision(same, hour=12).notes == "makan siang" and decision(same, hour=20).notes == "makan malam"


def test_kill_switch_never_records():
    result = decision(labels("makan", "makan", "makan"), enabled=False)
    assert result.auto is False and result.category_id == "makan"


def test_no_label_no_decision():
    assert decision([]) is None


@pytest.mark.parametrize("amount,labelled,expected", [
    (50_000, 40_000, True), (40_000, 50_000, True), (50_000, 39_999, False), (62_501, 50_000, False),
    (12_000, 12_000, True), (0, 10_000, False), (10_000, 0, False),
])
def test_amount_band_is_a_symmetric_quarter(amount, labelled, expected):
    assert within_band(amount, labelled) is expected


def load_simulation():
    spec = importlib.util.spec_from_file_location("habit_simulation", ROOT / "scripts" / "verify" / "habit_simulation.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_offline_simulation_asks_less_each_week_with_few_wrong_records():
    simulation = load_simulation()
    chosen = simulation.CONFIGS[0]
    totals = simulation.averaged(chosen, "steady")
    weekly = [asked / payments for asked, payments in totals["weeks"]]
    habitual = [asked / payments for asked, payments in totals["habitual"]]
    assert weekly[-1] < weekly[0] - 0.2 and habitual[-1] < habitual[0] - 0.25
    assert totals["wrong"] / totals["payments"] <= 0.05
    drift = simulation.averaged(chosen, "drift")
    drift_habitual = [asked / payments for asked, payments in drift["habitual"]]
    # After the habit changes in week 5 it is asked again, then learned again by week 7.
    assert drift_habitual[4] > 0.6 and drift_habitual[6] < 0.2


# --- Recording through the notification pipeline ------------------------------------------------


def groceries(amount, day, *, hours=0, **kwargs):
    return bca(f"You spent IDR {amount:,}.00 at Groceries.", days=day, hours=hours, version="1.8.0", **kwargs)


def pay(client, amount, day, **kwargs):
    return ingest(client, groceries(amount, day, **kwargs))["results"][0]


def answer_by_text(client, monkeypatch, result, *, category, kakeibo, notes):
    """The owner types the purpose; the mocked normaliser classifies it (no real model call)."""
    from app.main import app
    from app.services import notification_processing as processing
    saved = processing.settings
    provider = AsyncMock()
    provider.normalize_purpose.return_value = normalization(
        merchant=None, description=notes, category_id=category, kakeibo=kakeibo, offset_category_id=None,
        confidence=0.9)
    enable_ai(monkeypatch)
    monkeypatch.setattr(app.state, "notification_provider", provider)
    response = answer(client, result["event_id"], {"question_id": result["question"]["question_id"],
                                                   "reply_id": str(uuid4()), "text": notes})
    monkeypatch.setattr(processing, "settings", saved)
    assert response.status_code == 200, response.text


def answer_by_guess(client, result):
    guess = result["question"]["suggestions"][0]
    assert guess["kind"] == "habit"
    response = answer(client, result["event_id"], {"question_id": result["question"]["question_id"],
                                                   "reply_id": str(uuid4()), "suggestion_id": guess["id"]})
    assert response.status_code == 200, response.text


def teach(client, owner, monkeypatch, amounts=(12_000, 11_500, 12_500)):
    """Three owner answers for small Groceries payments: one typed, then two taps on the guess."""
    first = pay(client, amounts[0], 0)
    answer_by_text(client, monkeypatch, first, category=owner["makan"], kakeibo="need", notes="makan siang")
    for day, amount in enumerate(amounts[1:], start=1):
        answer_by_guess(client, pay(client, amount, day))


def test_three_agreeing_answers_record_the_next_payment_without_a_question(client, owner, monkeypatch):
    teach(client, owner, monkeypatch)
    result = pay(client, 12_000, 3, hours=2)
    assert "question" not in result and result["review_status"] == "reviewed"
    assert result["habit"] == {"label": "Dicatat dari kebiasaan: Makanan & Minuman",
                               "basis": "3 dari 3 jawaban sebelumnya"}
    assert result["description"] == "makan siang"
    row = tx_for(result["event_id"])
    posted = datetime.fromisoformat(groceries(12_000, 3, hours=2)["post_time"])
    assert (row["amount"], str(row["account_id"]), row["date"]) == (12_000, owner["bca"], posted)
    assert (str(row["category_id"]), row["kakeibo_type"], row["notes"]) == (owner["makan"], "need", "makan siang")
    assert (row["classified_by"], row["review_status"], row["classification_confidence"]) == ("habit", "reviewed", None)
    assert event_row(result["event_id"])["active_question"] is None
    stored = event_row(result["event_id"])["interpretation"]
    assert stored["source"] == "habit" and stored["review_reasons"] == []
    assert stored["habit"] == {"category_id": owner["makan"], "kakeibo": "need", "support": 3, "agreeing": 3,
                               "agreement": 1.0}
    assert len(feedback_rows(owner)) == 3
    detail = client.get(f"/api/transactions/{row['id']}").json()["transaction"]
    assert detail["classified_by"] == "habit" and detail["habit_basis"] == {"agreeing": 3, "support": 3}
    listed = client.get("/api/transactions").json()["transactions"]
    assert next(item for item in listed if item["id"] == str(row["id"]))["habit_basis"] == {"agreeing": 3,
                                                                                           "support": 3}
    queue = client.get("/api/review/queue").json()
    queued = {item["id"] for group in queue["groups"] for item in group["transactions"]}
    assert str(row["id"]) not in queued and not queue["questions"]
    assert client.get("/api/review/summary").json()["attention"] == 0
    # A habit-recorded payment is not evidence: the next one still rests on the three answers.
    later = pay(client, 12_500, 4)
    assert later["habit"]["basis"] == "3 dari 3 jawaban sebelumnya" and len(feedback_rows(owner)) == 3


def test_two_answers_only_put_the_guess_first(client, owner, monkeypatch):
    first = pay(client, 50_000, 0)
    answer_by_text(client, monkeypatch, first, category=owner["makan"], kakeibo="need", notes="makan siang")
    answer_by_guess(client, pay(client, 48_000, 1))
    third = pay(client, 52_000, 2)
    [guess, *_] = third["question"]["suggestions"]
    assert (guess["kind"], guess["label"], guess["category_id"], guess["notes"]) == (
        "habit", "Sepertinya Makanan & Minuman?", owner["makan"], "makan siang")
    assert guess["basis"] == {"agreeing": 2, "support": 2}
    assert tx_for(third["event_id"])["review_status"] == "needs_review"
    queue = client.get("/api/review/queue")
    assert queue.status_code == 200, queue.text
    [listed] = [item for item in queue.json()["questions"] if item["event_id"] == third["event_id"]]
    assert listed["question"]["suggestions"][0] == guess


def test_three_of_four_agreeing_answers_still_ask(client, owner, monkeypatch):
    teach(client, owner, monkeypatch, amounts=(50_000, 48_000, 52_000))
    pay(client, 50_000, 3, hours=1)  # recorded from the habit, 3 of 3
    fourth = pay(client, 51_000, 4, hours=1)
    assert "question" not in fourth
    # The owner files a fifth payment differently by answering it on a day the habit was switched off.
    monkeypatch.setattr(config, "settings", replace(config.settings, habit_auto_record_enabled=False))
    other = pay(client, 49_000, 5)
    answer_by_text(client, monkeypatch, other, category=owner["jajan"], kakeibo="want", notes="jajan")
    monkeypatch.setattr(config, "settings", replace(config.settings, habit_auto_record_enabled=True))
    asked = pay(client, 50_000, 6)
    assert asked["question"]["suggestions"][0]["label"] == "Sepertinya Makanan & Minuman?"
    assert asked["question"]["suggestions"][0]["basis"] == {"agreeing": 3, "support": 4}


def test_a_correction_of_a_habit_record_asks_the_next_payment(client, owner, monkeypatch):
    teach(client, owner, monkeypatch)
    recorded = pay(client, 12_000, 3)
    row = tx_for(recorded["event_id"])
    response = client.patch(f"/api/transactions/{row['id']}", json={"category_id": owner["jajan"],
                                                                     "kakeibo_type": "want", "notes": "jajan"})
    assert response.status_code == 200, response.text
    correction = feedback_rows(owner)[-1]
    assert (correction["kind"], correction["before_classified_by"], correction["after_classified_by"]) == (
        "correction", "habit", "owner")
    edited = tx_for(recorded["event_id"])
    assert (edited["amount"], str(edited["account_id"]), edited["classified_by"]) == (12_000, owner["bca"], "owner")
    nxt = pay(client, 12_000, 4)
    assert nxt["question"]["suggestions"][0]["basis"] == {"agreeing": 3, "support": 4}
    assert tx_for(nxt["event_id"])["classified_by"] == "rule"


def test_amount_outside_the_band_is_asked(client, owner, monkeypatch):
    teach(client, owner, monkeypatch)
    result = pay(client, 16_000, 3)
    assert "question" in result
    assert all(item["kind"] != "habit" for item in result["question"]["suggestions"])


def test_kill_switch_only_suggests(client, owner, monkeypatch):
    teach(client, owner, monkeypatch)
    monkeypatch.setattr(config, "settings", replace(config.settings, habit_auto_record_enabled=False))
    result = pay(client, 12_000, 3)
    assert result["question"]["suggestions"][0]["label"] == "Sepertinya Makanan & Minuman?"
    assert tx_for(result["event_id"])["classified_by"] == "rule"


def test_reprocessing_a_habit_recorded_event_changes_nothing(client, owner, monkeypatch):
    from app.db.pool import db_conn
    teach(client, owner, monkeypatch)
    payload = groceries(12_000, 3)
    first = ingest(client, payload)["results"][0]
    again = ingest(client, payload)["results"][0]
    assert again["event_id"] == first["event_id"] and again["habit"] == first["habit"]
    with db_conn() as conn:
        count = conn.execute("SELECT COUNT(*) AS n FROM transactions WHERE idempotency_key = %s",
                             (f"notification:{first['event_id']}",)).fetchone()["n"]
    assert count == 1 and len(feedback_rows(owner)) == 3


def insert_feedback_rows(user_id, account_id, *, times=3, kind="correction", category, kakeibo="need",
                         signature="bca|expense|groceries", after_type="expense", amount=12_000, **extra):
    from app.db.pool import db_conn
    columns = {"after_review_status": None, "after_classified_by": None, "event_id": None, "reply_id": None,
               "question_id": None, **extra}
    with db_conn() as conn:
        for _ in range(times):
            conn.execute(
                """INSERT INTO classification_feedback (user_id, kind, transaction_id, event_id, question_id, reply_id,
                       after_notes, after_category_id, after_kakeibo_type, after_type, amount, account_id,
                       institution, text_signature, after_review_status, after_classified_by, local_weekday, local_hour)
                   VALUES (%s, %s, %s, %s, %s, %s, 'makan siang', %s, %s, %s, %s, %s, 'bca', %s, %s, %s, 1, 12)""",
                (user_id, kind, str(uuid4()), columns["event_id"],
                 columns["question_id"] or (str(uuid4()) if kind == "purpose_answer" else None),
                 columns["reply_id"] or (str(uuid4()) if kind == "purpose_answer" else None),
                 category, kakeibo, after_type, amount, account_id, signature, columns["after_review_status"],
                 columns["after_classified_by"]),
            )
        conn.commit()


def test_ai_classified_and_unconfirmed_labels_are_never_evidence(client, owner):
    user, account = owner["user_id"], owner["bca"]
    insert_feedback_rows(user, account, kind="purpose_answer", category=owner["makan"], after_review_status="needs_review")
    insert_feedback_rows(user, account, kind="capture", category=owner["makan"], after_classified_by="ai")
    insert_feedback_rows(user, account, kind="accepted_suggestion", category=owner["makan"])
    # Answers written before V39 count only when their event recorded the reply as reviewed.
    insert_feedback_rows(user, account, kind="purpose_answer", category=owner["makan"])
    result = pay(client, 12_000, 0)
    assert "question" in result
    assert all(item["kind"] != "habit" for item in result["question"]["suggestions"])


def test_owner_capture_answers_and_reviewed_legacy_answers_are_evidence(client, owner):
    from app.db.pool import db_conn
    from psycopg.types.json import Jsonb
    user, account = owner["user_id"], owner["bca"]
    insert_feedback_rows(user, account, times=2, kind="capture", category=owner["makan"], after_classified_by="owner")
    event_id, reply_id = str(uuid4()), str(uuid4())
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO notification_events (id, user_id, device_id, package_name, body_text, payload_hash,
                   post_time, processing_state, interpretation)
               VALUES (%s, %s, 'synthetic-device', %s, 'synthetic', %s, NOW(), 'needs_review', %s)""",
            (event_id, user, BCA, uuid4().hex, Jsonb({"purpose_answer": {"reply_id": reply_id,
                                                                          "review_status": "reviewed"}})),
        )
        conn.commit()
    insert_feedback_rows(user, account, times=1, kind="purpose_answer", category=owner["makan"], event_id=event_id,
                         reply_id=reply_id)
    result = pay(client, 12_000, 0)
    assert result["habit"]["basis"] == "3 dari 3 jawaban sebelumnya"


def test_labels_in_movement_categories_never_become_a_habit(client, owner):
    insert_feedback_rows(owner["user_id"], owner["bca"], category=owner["internal_out"], kakeibo=None)
    result = pay(client, 12_000, 0)
    assert all(item["kind"] != "habit" for item in result["question"]["suggestions"])


def test_income_and_transfer_shaped_payments_are_never_recorded_from_a_habit(client, owner):
    insert_feedback_rows(owner["user_id"], owner["bca"], category=owner["patungan"], kakeibo=None,
                         signature="bca|income|account transfer|from:***ab ****cd *e", after_type="income",
                         amount=57_000)
    incoming = ingest(client, bca(MASKED_INCOMING, version="1.8.0"))["results"][0]
    received = tx_for(incoming["event_id"])
    assert "habit" not in incoming and (received is None or received["classified_by"] != "habit")
    insert_feedback_rows(owner["user_id"], owner["bca"], category=owner["makan"], signature="bca|expense|transfer",
                         amount=50_000)
    transfer = ingest(client, bca("You spent IDR 50,000.00 at Transfer.", version="1.8.0"))["results"][0]
    row = tx_for(transfer["event_id"])
    assert row["classified_by"] != "habit" and "habit" not in transfer


def test_another_users_labels_are_never_used(client, owner):
    from app.db.pool import db_conn
    from app.services.habits import load_labels
    other, other_category = str(uuid4()), str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Sari', 'x', 'T')",
                     (other, f"other_{uuid4().hex}"))
        conn.execute("INSERT INTO categories (id, user_id, name, kind, kakeibo_type) VALUES (%s, %s, 'Makan', 'expense', 'need')",
                     (other_category, other))
        conn.commit()
    # Same account id, signature and amounts, but another owner's rows and category.
    insert_feedback_rows(other, owner["bca"], category=other_category)
    with db_conn() as conn:
        with conn.cursor() as cur:
            assert load_labels(cur, owner["user_id"], institution="bca", signature="bca|expense|groceries",
                               account_id=owner["bca"], amount=12_000) == []
            assert len(load_labels(cur, other, institution="bca", signature="bca|expense|groceries",
                                   account_id=owner["bca"], amount=12_000)) == 3
    result = pay(client, 12_000, 0)
    assert "question" in result and all(item["kind"] != "habit" for item in result["question"]["suggestions"])


def test_ai_mode_records_from_the_habit_without_calling_the_model(client, owner, monkeypatch):
    insert_feedback_rows(owner["user_id"], owner["bca"], category=owner["makan"])
    enable_ai(monkeypatch)
    queued = ingest(client, groceries(12_000, 0))["results"][0]
    assert queued["status"] == "queued"
    provider = AsyncMock()
    run_claim(provider)
    provider.interpret.assert_not_awaited()
    row = tx_for(queued["event_id"])
    assert (row["classified_by"], row["review_status"], str(row["category_id"])) == ("habit", "reviewed", owner["makan"])


def test_pairing_a_habit_row_as_a_movement_drops_the_habit_classifier(client, owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services.movement_pairing import link_pair
    insert_feedback_rows(owner["user_id"], owner["bca"], category=owner["makan"])
    recorded = pay(client, 12_000, 0)
    expense = tx_for(recorded["event_id"])
    assert expense["classified_by"] == "habit"
    with db_conn() as conn:
        income_id = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, source,
                   review_status, classified_by)
               VALUES (%s, %s, %s, 'income', 12000, 'synthetic', %s, 'manual', 'reviewed', 'owner') RETURNING id""",
            (owner["user_id"], owner["gopay"], owner["income"], expense["date"]),
        ).fetchone()["id"]
        with conn.cursor() as cur:
            link_pair(cur, owner["user_id"], str(expense["id"]), str(income_id), fee=0, notes="Pindah saldo")
        conn.commit()
    assert tx_for(recorded["event_id"])["classified_by"] == "system"


# --- Weekly question rate -----------------------------------------------------------------------


def test_question_rate_counts_each_recording_outcome(client, owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services.question_rate import question_rate
    teach(client, owner, monkeypatch)                    # three asked and answered
    pay(client, 12_000, 3)                                # habit
    open_one = pay(client, 80_000, 3, hours=1)            # asked, not answered yet
    recorded = pay(client, 12_000, 4)                     # habit, then corrected
    row = tx_for(recorded["event_id"])
    assert client.patch(f"/api/transactions/{row['id']}", json={"category_id": owner["jajan"]}).status_code == 200
    ingest(client, notification("You've paid Rp125.000 to Kedai Awan", seconds=40))  # deterministic rule
    assert "question" in open_one
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, source,
                       review_status, classified_by)
                   VALUES (%s, %s, %s, 'expense', 10000, 'parkir', %s, 'capture', 'reviewed', 'rule') RETURNING id""",
                (owner["user_id"], owner["gopay"], owner["makan"], datetime(2026, 9, 29, 1, tzinfo=timezone.utc)),
            )
            capture_tx = cur.fetchone()["id"]
            cur.execute(
                """INSERT INTO text_captures (user_id, client_capture_id, text, device_time, status, draft, answers,
                       transaction_id, recorded_at)
                   VALUES (%s, %s, 'parkir 10rb', NOW(), 'recorded', %s, '[]'::jsonb, %s, NOW())""",
                (owner["user_id"], str(uuid4()), '{"recorded": {"classified_by": "rule"}}', capture_tx),
            )
            rate = question_rate(cur, owner["user_id"], weeks=2, today=date(2026, 10, 5), tz="Asia/Jakarta")
    [previous, current] = rate["weeks"]
    assert (current["week_start"], current["week_end"], current["in_progress"]) == ("2026-10-05", "2026-10-11", True)
    assert previous["week_start"] == "2026-09-28" and previous["in_progress"] is False
    assert {key: previous[key] for key in ("recorded", "asked", "answered", "by_memory", "by_habit", "by_ai",
                                           "by_other")} == {
        "recorded": 8, "asked": 4, "answered": 3, "by_memory": 1, "by_habit": 2, "by_ai": 0, "by_other": 1}
    assert current["recorded"] == 0 and rate["has_data"] is True
    # The correction is counted in the week it was made, here the real current week outside this window.
    assert previous["corrections"] + current["corrections"] == 0


def test_question_rate_endpoint_has_an_honest_empty_state(client, owner):
    response = client.get("/api/dashboard/question-rate?weeks=8")
    assert response.status_code == 200
    body = response.json()
    assert len(body["weeks"]) == 8 and body["has_data"] is False and body["weeks"][-1]["in_progress"] is True
    assert all(week["recorded"] == 0 for week in body["weeks"])
    assert client.get("/api/dashboard/question-rate?weeks=0").status_code == 422
    assert client.get("/api/dashboard/question-rate?weeks=27").status_code == 422


def test_question_rate_counts_corrections_in_their_week(client, owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services.question_rate import question_rate
    insert_feedback_rows(owner["user_id"], owner["bca"], category=owner["makan"])
    recorded = pay(client, 12_000, 0)
    row = tx_for(recorded["event_id"])
    assert client.patch(f"/api/transactions/{row['id']}", json={"category_id": owner["jajan"]}).status_code == 200
    today = datetime.now(timezone.utc).date()
    with db_conn() as conn:
        with conn.cursor() as cur:
            rate = question_rate(cur, owner["user_id"], weeks=1, today=today, tz="UTC")
    assert (rate["weeks"][0]["corrections"], rate["weeks"][0]["habit_corrections"]) == (1, 1)
