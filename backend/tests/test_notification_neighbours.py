"""Time-aware interpretation: neighbours in context and the same-money proposal (D5, OD116, OD120).

All texts, names and amounts are synthetic.
"""
import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

from app.services.notification_interpretation import Interpretation
from app.services.openai_notification_provider import ProviderError
from test_duplicate_arrivals import (
    GOPAY, GOPAY_CREDIT, JAGO, JAGO_CREDIT, MYBCA, MYBCA_DEBIT, START, event, event_row, incomes, send,
)
from test_notification_processing_database import enable_ai, ingest, notification_owner  # noqa: F401


def claim_and_process(provider):
    from app.services import notification_processing as processing
    claimed = processing.claim_event()
    assert claimed is not None
    asyncio.run(processing.process_claim(claimed, provider))
    return claimed


def credit(owner, *, account, text_evidence, same_money_as=None):
    return Interpretation.model_validate_json(json.dumps({
        "outcome": "record", "direction": "income", "description": "Transfer masuk", "merchant": None,
        "account_id": owner[account], "category_id": owner["income"], "kakeibo": None,
        "source_account_id": None, "target_account_id": None,
        "amount_evidence": "Rp500.000", "direction_evidence": text_evidence,
        "source_evidence": "", "target_evidence": "", "candidate_transaction_id": None,
        "confidence": 0.9, "review_reason": "none", "mappings": [], "same_money_as": same_money_as,
    }))


def provider_for(proposal_from_context):
    provider = AsyncMock()

    async def interpret(context):
        return proposal_from_context(context)

    provider.interpret.side_effect = interpret
    return provider


def neighbour_id(context, institution):
    return next(item["id"] for item in context["neighbours"] if item["institution"] == institution)


def record_gopay(client, owner):
    send(client, GOPAY_CREDIT, GOPAY, 0)
    claim_and_process(provider_for(lambda context: credit(owner, account="gopay", text_evidence="telah menerima")))


# --- 4.1 Neighbour context ----------------------------------------------------------------------------


def test_neighbours_are_bounded_ordered_and_carry_no_text(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing

    enable_ai(monkeypatch)
    for index in range(22):
        ingest(client, event(f"Transfer QRIS Rp {10 + index}.000 berhasil ke Toko Rahasia {index}", GOPAY, index * 5))
    target = send(client, JAGO_CREDIT, JAGO, 50)
    stored = event_row(target["event_id"])
    facts, context = processing.read_context(stored)
    neighbours = context["neighbours"]
    assert len(neighbours) == 20 and context["neighbours_incomplete"] is True
    seconds = [item["seconds_from_this"] for item in neighbours]
    assert seconds == sorted(seconds)
    # The nearest twenty: 50 seconds either side holds 21 events, the farthest is left out.
    assert max(abs(value) for value in seconds) <= 50
    assert set(neighbours[0]) == {"id", "institution", "direction", "amount", "seconds_from_this", "state",
                                  "account_id", "in_movement"}
    assert all(item["state"] == "queued" and item["direction"] == "expense" for item in neighbours)
    serialised = json.dumps(context["neighbours"])
    assert "Toko Rahasia" not in serialised and "QRIS" not in serialised


def test_neighbours_report_recorded_legs_and_skip_other_owners(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services import notification_processing as processing

    owner = notification_owner
    send(client, GOPAY_CREDIT, GOPAY, 0)
    other = str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Lain', 'x', 'T')",
                     (other, f"other_{uuid4().hex}"))
        payload = event(MYBCA_DEBIT, MYBCA, 1)
        conn.execute(
            """INSERT INTO notification_events (user_id, device_id, package_name, body_text, payload_hash, post_time,
                   processing_state, processing_mode)
               VALUES (%s, 'synthetic-device', %s, %s, %s, %s, 'queued', 'ai')""",
            (other, MYBCA, MYBCA_DEBIT, uuid4().hex, payload["post_time"]),
        )
        conn.commit()
    try:
        enable_ai(monkeypatch)
        target = send(client, JAGO_CREDIT, JAGO, 0.13)
        facts, context = processing.read_context(event_row(target["event_id"]))
        assert [(item["institution"], item["state"], item["account_id"], item["in_movement"])
                for item in context["neighbours"]] == [("gopay", "recorded", owner["gopay"], False)]
        assert context["neighbours"][0]["seconds_from_this"] == -0.1
        assert context["neighbours_incomplete"] is False
    finally:
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (other,))
            conn.commit()


def test_failed_neighbour_query_is_incomplete_and_processing_continues(client, notification_owner, monkeypatch):
    from app.services import notification_context

    monkeypatch.setattr(notification_context, "NEIGHBOUR_SQL", "SELECT * FROM no_such_neighbour_table")
    result = send(client, GOPAY_CREDIT, GOPAY, 0)
    assert result["status"] == "recorded"
    from app.db.pool import db_conn
    with db_conn() as conn:
        facts = notification_context.collect_facts(dict(event_row(result["event_id"])))
        neighbours, ids, incomplete = notification_context.neighbour_context(
            conn.cursor(), notification_owner["user_id"], facts, result["event_id"])
    assert (neighbours, ids, incomplete) == ([], {}, True)


def test_budget_drops_the_farthest_neighbours_and_marks_them_incomplete():
    from app.services.notification_context import CONTEXT_BYTE_LIMIT, context_size, fit_context_budget

    context = {
        "notification": "x" * (CONTEXT_BYTE_LIMIT - 250), "similar_history": [], "memory": [],
        "neighbours": [{"id": f"n{index}", "seconds_from_this": seconds, "institution": "gopay"}
                       for index, seconds in enumerate((-200, -5, 1, 30, 250), start=1)],
        "neighbours_incomplete": False,
    }
    fit_context_budget(context)
    assert context_size(context) <= CONTEXT_BYTE_LIMIT
    assert context["neighbours_incomplete"] is True
    kept = [item["seconds_from_this"] for item in context["neighbours"]]
    assert kept and set(kept) <= {-5, 1, 30} and 250 not in kept and -200 not in kept


def test_invalid_neighbour_window_disables_ai(monkeypatch):
    from app.core.config import load_settings
    from app.services.openai_notification_provider import configuration_error

    monkeypatch.setenv("NOTIFICATION_AI_ENABLED", "true")
    monkeypatch.setenv("NOTIFICATION_NEIGHBOUR_WINDOW_SECONDS", "10")
    loaded = load_settings()
    assert configuration_error(loaded) == "processor_configuration_invalid"
    assert loaded.notification_neighbour_window_seconds == 300
    monkeypatch.setenv("NOTIFICATION_NEIGHBOUR_WINDOW_SECONDS", "600")
    assert load_settings().notification_neighbour_window_seconds == 600


# --- 4.2 The same-money proposal ----------------------------------------------------------------------


def test_confirmed_proposal_becomes_a_duplicate(client, notification_owner, monkeypatch):
    owner = notification_owner
    enable_ai(monkeypatch)
    record_gopay(client, owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    claim_and_process(provider_for(lambda context: credit(
        owner, account="main", text_evidence="You've received", same_money_as=neighbour_id(context, "gopay"))))
    stored = event_row(jago["event_id"])
    assert stored["provenance_kind"] == "duplicate_observation"
    assert stored["prompt_version"] == "notification-interpretation-11"
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["gopay"]]


def test_unconfirmed_proposal_is_held_and_books_nothing(client, notification_owner, monkeypatch):
    owner = notification_owner
    send(client, MYBCA_DEBIT, MYBCA, -3)
    enable_ai(monkeypatch)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    # A recorded neighbour, but the opposite direction: the rule can't confirm it.
    claim_and_process(provider_for(lambda context: credit(
        owner, account="main", text_evidence="You've received", same_money_as=neighbour_id(context, "bca"))))
    stored = event_row(jago["event_id"])
    assert (stored["processing_state"], stored["error_code"]) == ("needs_review", "possible_duplicate_observation")
    assert stored["transaction_id"] is None and incomes(owner) == []


def test_queued_neighbour_cannot_be_the_first_observation(client, notification_owner, monkeypatch):
    owner = notification_owner
    enable_ai(monkeypatch)
    gopay = send(client, GOPAY_CREDIT, GOPAY, 0)
    send(client, JAGO_CREDIT, JAGO, 0.13)
    # GoPay is read first and names the still queued Jago event: rejected, the guard decides, GoPay books.
    claim_and_process(provider_for(lambda context: credit(
        owner, account="gopay", text_evidence="telah menerima", same_money_as=neighbour_id(context, "jago"))))
    assert event_row(gopay["event_id"])["provenance_kind"] == "observed_leg"
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["gopay"]]


def test_unknown_neighbour_id_is_rejected_and_the_rule_still_guards(client, notification_owner, monkeypatch):
    owner = notification_owner
    enable_ai(monkeypatch)
    record_gopay(client, owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    claim_and_process(provider_for(lambda context: credit(
        owner, account="main", text_evidence="You've received", same_money_as="n9")))
    assert event_row(jago["event_id"])["provenance_kind"] == "duplicate_observation"
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["gopay"]]


def test_null_proposal_with_a_rule_match_is_a_duplicate(client, notification_owner, monkeypatch):
    owner = notification_owner
    enable_ai(monkeypatch)
    record_gopay(client, owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    claim_and_process(provider_for(lambda context: credit(owner, account="main", text_evidence="You've received")))
    assert event_row(jago["event_id"])["provenance_kind"] == "duplicate_observation"


def test_failing_model_still_guarded(client, notification_owner, monkeypatch):
    owner = notification_owner
    enable_ai(monkeypatch)
    record_gopay(client, owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    provider = AsyncMock()
    provider.interpret.side_effect = ProviderError("provider_invalid_output")
    claim_and_process(provider)
    assert event_row(jago["event_id"])["provenance_kind"] == "duplicate_observation"
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["gopay"]]
