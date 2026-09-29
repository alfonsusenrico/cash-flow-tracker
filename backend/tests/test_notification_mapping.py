"""Unresolved-name reporting, learned aliases, and AI mapping decisions."""

import json
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import pytest

from evaluation.notification_cases import BASE_CONTEXT, fixture_id
from app.services.notification_evidence import EvidenceError, collect_facts
from app.services.notification_interpretation import Interpretation
from app.services.notification_mapping import mapping_decision, supports_confirmation
from app.services.notification_resolution import alias_key, endpoint_context

POST_TIME = datetime.fromisoformat("2026-09-29T17:00:00+07:00")
GOPAY_TABUNGAN_MOVE = "Rp1.250.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."


def facts_for(text, package="com.jago.digitalBanking"):
    return collect_facts({"package_name": package, "body_text": text, "post_time": POST_TIME})


def test_unresolved_pocket_is_reported_with_eligible_liquid_accounts():
    context = endpoint_context(facts_for(GOPAY_TABUNGAN_MOVE), BASE_CONTEXT["accounts"])
    assert context["source_account_id"] == fixture_id(3)
    [name] = context["unresolved_names"]
    assert (name["role"], name["name"], name["institution"], name["name_normalized"]) == (
        "target", "GoPay Tabungan", "jago", "gopay tabungan")
    assert fixture_id(5) in name["eligible_account_ids"]
    assert fixture_id(3) not in name["eligible_account_ids"]


def test_learned_alias_resolves_before_name_matching():
    accounts = deepcopy(BASE_CONTEXT["accounts"])
    gopay = next(row for row in accounts if row["id"] == fixture_id(5))
    gopay["notification_names"] = [alias_key("jago", "gopay tabungan")]
    assert endpoint_context(facts_for(GOPAY_TABUNGAN_MOVE), accounts) == {
        "source_account_id": fixture_id(3), "target_account_id": fixture_id(5),
    }


def test_ambiguous_institution_is_mappable_among_its_candidates_only():
    accounts = deepcopy(BASE_CONTEXT["accounts"])
    accounts.append({"id": fixture_id(11), "name": "GoPay Later", "type": "ewallet", "parent_id": None})
    context = endpoint_context(facts_for("QRIS Rp32.000 ke Kedai Awan berhasil", "com.gopay.wallet"), accounts)
    [name] = context["unresolved_names"]
    assert name["role"] == "observed" and name["name_normalized"] == ""
    assert set(name["eligible_account_ids"]) == {fixture_id(5), fixture_id(11)}


def test_institution_without_any_related_account_stays_a_plain_mapping_error():
    accounts = [row for row in deepcopy(BASE_CONTEXT["accounts"]) if row["name"] != "GoPay"]
    context = endpoint_context(facts_for("QRIS Rp32.000 ke Kedai Awan berhasil", "com.gopay.wallet"), accounts)
    assert context == {"mapping_error": "uncertain_institution_mapping"}


def movement_proposal(target, mappings):
    return Interpretation.model_validate_json(json.dumps({
        "outcome": "record", "direction": "internal_movement", "description": "Pindah saldo",
        "account_id": None, "source_account_id": fixture_id(3), "target_account_id": target,
        "category_id": None, "kakeibo": None, "amount_evidence": "Rp1.250.000",
        "direction_evidence": "has been moved", "source_evidence": "", "target_evidence": "",
        "candidate_transaction_id": None, "confidence": 0.9, "review_reason": "none", "mappings": mappings,
    }))


def mapping_context():
    return {"facts": endpoint_context(facts_for(GOPAY_TABUNGAN_MOVE), BASE_CONTEXT["accounts"])}


def test_decision_accepts_eligible_choice_and_keeps_lowest_confidence():
    decision = mapping_decision(movement_proposal(fixture_id(5), [
        {"role": "target", "account_id": fixture_id(5), "confidence": 0.93,
         "alternatives": [fixture_id(4), fixture_id(5), fixture_id(8)]},
    ]), mapping_context())
    [entry] = decision["entries"]
    assert decision["confidence"] == 0.93
    assert entry["account_id"] == fixture_id(5) and entry["alternatives"] == [fixture_id(4), fixture_id(8)]


@pytest.mark.parametrize("target,mappings,code", [
    (fixture_id(5), [], "uncertain_pocket_mapping"),
    (fixture_id(99), [{"role": "target", "account_id": fixture_id(99), "confidence": 0.95, "alternatives": []}], "invalid_mapping_proposal"),
    (fixture_id(4), [{"role": "target", "account_id": fixture_id(5), "confidence": 0.95, "alternatives": []}], "invalid_mapping_proposal"),
])
def test_decision_rejects_missing_ineligible_or_inconsistent_mappings(target, mappings, code):
    with pytest.raises(EvidenceError, match=code):
        mapping_decision(movement_proposal(target, mappings), mapping_context())


def test_decision_is_none_without_unresolved_names():
    assert mapping_decision(SimpleNamespace(outcome="record", mappings=[]), {"facts": {}}) is None


@pytest.mark.parametrize("version,expected", [
    ("1.3.0", True), ("1.10.2", True), ("2.0.0-beta", True), ("1.2.0", False), ("1.0.0", False), (None, False), ("abc", False),
])
def test_confirmation_requires_companion_1_3(version, expected):
    assert supports_confirmation({"source_version": version}) is expected


def test_openai_schema_marks_every_field_required_at_every_level():
    from app.services.openai_notification_provider import response_schema

    schema = response_schema()["schema"]
    mapping = schema["$defs"]["MappingProposal"]
    assert "mappings" in schema["required"] and set(mapping["required"]) == set(mapping["properties"])
    assert "default" not in json.dumps(schema)


@pytest.mark.parametrize("value,threshold,error", [
    ("0.85", 0.85, None), ("0.5", 0.5, None), ("1.0", 1.0, None),
    ("0.49", 0.85, "processor_configuration_invalid"), ("nan", 0.85, "processor_configuration_invalid"),
])
def test_mapping_threshold_configuration_bounds(monkeypatch, value, threshold, error):
    from app.core.config import load_settings

    monkeypatch.setenv("NOTIFICATION_MAPPING_AUTO_THRESHOLD", value)
    loaded = load_settings()
    assert loaded.notification_mapping_auto_threshold == threshold
    assert loaded.notification_ai_configuration_error == error
