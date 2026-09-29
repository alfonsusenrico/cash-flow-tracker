import asyncio
import json
from copy import deepcopy
from dataclasses import replace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers.ingest import NotificationEventIn
from app.services.auth import get_current_user
from app.services.notification_dry_run import run_dry_run
from app.services.notification_evidence import collect_facts
from app.services.notification_interpretation import Interpretation
from app.services.notification_context import sanitize_text
from app.services.openai_notification_provider import ProviderError, dry_run_configuration_error
from evaluation.notification_cases import synthetic_cases


def interpreted_case(name, **changes):
    case = next(case for case in synthetic_cases() if case.name == name)
    facts = collect_facts(case.event)
    context = deepcopy(case.context)
    context["notification"] = facts.text
    expected = case.expected
    values = {
        "outcome": "record", "direction": facts.direction,
        "description": "Transaksi contoh", "account_id": expected.get("account_id"),
        "category_id": expected.get("category_id"), "kakeibo": expected.get("kakeibo"),
        "source_account_id": expected.get("source_account_id"),
        "target_account_id": expected.get("target_account_id"),
        "amount_evidence": facts.amount_quotes[0], "direction_evidence": facts.text[:400],
        "source_evidence": "", "target_evidence": "", "candidate_transaction_id": None,
        "confidence": 0.9, "review_reason": "none",
    }
    values.update(changes)
    return case, facts, context, Interpretation.model_validate_json(json.dumps(values))


@pytest.mark.parametrize("outcome,reason", [("needs_review", "uncertain_mapping"), ("ignored", "none")])
def test_model_abstention_is_never_reported_as_a_record(outcome, reason):
    case, facts, context, proposal = interpreted_case(
        "jago-coffee", outcome=outcome, review_reason=reason, direction=None,
        description="", account_id=None, category_id=None, amount_evidence="",
        direction_evidence="", kakeibo=None,
    )
    provider = AsyncMock()
    provider.interpret.return_value = proposal
    with patch("app.services.notification_dry_run.context_for", return_value=(facts, context)):
        result = asyncio.run(run_dry_run(provider, str(uuid4()), case.event))
    assert result["status"] == outcome
    assert "type" not in result and "account" not in result
    if outcome == "needs_review":
        assert result["error_code"] == reason


def test_dry_run_rejects_owned_but_wrong_institution_account():
    from evaluation.notification_cases import fixture_id
    case, facts, context, proposal = interpreted_case("jago-coffee", account_id=fixture_id(1))
    provider = AsyncMock()
    provider.interpret.return_value = proposal
    with patch("app.services.notification_dry_run.context_for", return_value=(facts, context)):
        result = asyncio.run(run_dry_run(provider, str(uuid4()), case.event))
    assert result == {"status": "needs_review", "error_code": "conflicting_observed_account"}


def test_dry_run_returns_only_the_observed_expense_endpoint():
    from evaluation.notification_cases import fixture_id
    case, facts, context, proposal = interpreted_case("gopay-bank-out", target_account_id=fixture_id(1))
    provider = AsyncMock()
    provider.interpret.return_value = proposal
    with patch("app.services.notification_dry_run.context_for", return_value=(facts, context)):
        result = asyncio.run(run_dry_run(provider, str(uuid4()), case.event))
    assert result["status"] == "would_record"
    assert result["source"] == "GoPay" and result["target"] is None


def test_dry_run_uses_canonical_movement_pillar_and_qualified_pockets():
    case, facts, context, proposal = interpreted_case("jago-dual", kakeibo="want")
    provider = AsyncMock()
    provider.interpret.return_value = proposal
    with patch("app.services.notification_dry_run.context_for", return_value=(facts, context)):
        result = asyncio.run(run_dry_run(provider, str(uuid4()), case.event))
    assert result["status"] == "would_record"
    assert result["source"] == "Bank Jago · Kantong Utama"
    assert result["target"] == "Bank Jago · Dana Darurat"
    assert result["kakeibo"] == "saving"


def test_redacted_income_quote_is_valid_but_rewritten_quote_is_rejected():
    case, facts, context, proposal = interpreted_case("jago-english-income")
    context["notification"] = sanitize_text(facts.text, "Raka Purnama")
    provider = AsyncMock()
    provider.interpret.return_value = proposal.model_copy(update={"direction_evidence": "[SELF] has sent"})
    with patch("app.services.notification_dry_run.context_for", return_value=(facts, context)):
        result = asyncio.run(run_dry_run(provider, str(uuid4()), case.event))
        assert result["status"] == "would_record"
        provider.interpret.return_value = proposal
        result = asyncio.run(run_dry_run(provider, str(uuid4()), case.event))
    assert result == {"status": "needs_review", "error_code": "fabricated_direction_evidence"}


def test_invalid_provider_output_requires_review_instead_of_success():
    case, facts, context, _ = interpreted_case("jago-coffee")
    provider = AsyncMock()
    provider.interpret.side_effect = ProviderError("provider_invalid_output")
    with patch("app.services.notification_dry_run.context_for", return_value=(facts, context)):
        result = asyncio.run(run_dry_run(provider, str(uuid4()), case.event))
    assert result == {"status": "needs_review", "error_code": "provider_invalid_output"}


def payload():
    return {
        "device_id": "synthetic-device", "package_name": "com.jago.digitalbanking",
        "body_text": "You've paid Rp125.000 to Kedai Awan",
        "post_time": "2026-09-28T10:00:27Z", "payload_hash": "synthetic-payload",
    }


def test_dry_run_configuration_requires_development_flag_and_credential():
    from app.core.config import settings
    assert dry_run_configuration_error(replace(settings, app_env="production", notification_ai_dry_run_enabled=True)) == "dry_run_unavailable"
    assert dry_run_configuration_error(replace(settings, app_env="development", notification_ai_dry_run_enabled=False)) == "dry_run_unavailable"
    assert dry_run_configuration_error(replace(settings, app_env="development", notification_ai_dry_run_enabled=True)) == "provider_credential_missing"


def test_direct_dry_run_is_authenticated_and_never_calls_ingestion():
    user_id = str(uuid4())
    app.dependency_overrides[get_current_user] = lambda: {"id": user_id}
    result = {"status": "would_record", "description": "Bayar Kedai Awan", "amount": 125000, "currency": "IDR"}
    with patch("app.routers.ingest.dry_run_guard", return_value=object()), \
         patch("app.services.notification_dry_run.run_dry_run", new=AsyncMock(return_value=result)) as dry_run, \
         patch("app.services.notification_processing.accept_event") as accept_event:
        response = TestClient(app).post("/api/ingest/notifications/dry-run", json={"event": payload()})
    assert response.status_code == 200 and response.json()["result"] == result
    assert dry_run.await_args.args[1] == user_id
    assert accept_event.call_count == 0
    app.dependency_overrides.clear()


def test_disabled_dry_run_is_not_found_before_event_loading():
    app.dependency_overrides[get_current_user] = lambda: {"id": str(uuid4())}
    with patch("app.routers.ingest.dry_run_guard", side_effect=__import__("fastapi").HTTPException(status_code=404)), \
         patch("app.services.notification_dry_run.stored_event") as stored:
        response = TestClient(app).post(f"/api/ingest/notifications/{uuid4()}/dry-run")
    assert response.status_code == 404
    assert stored.call_count == 0
    app.dependency_overrides.clear()


def test_ignored_and_stockbit_dry_runs_never_call_provider():
    cases = {case.name: case for case in synthetic_cases()}
    provider = AsyncMock()
    for name, expected in (("promotion", "ignored"), ("stockbit-buy", "not_ai_eligible")):
        event = NotificationEventIn.model_validate({
            "device_id": "synthetic", "package_name": cases[name].event["package_name"],
            "body_text": cases[name].event["body_text"], "post_time": cases[name].event["post_time"],
            "payload_hash": name,
        }).model_dump()
        assert asyncio.run(run_dry_run(provider, str(uuid4()), event))["status"] == expected
    assert provider.interpret.await_count == 0
