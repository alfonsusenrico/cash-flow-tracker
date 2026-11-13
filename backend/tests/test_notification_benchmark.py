import asyncio
import json
from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from evaluation.notification_benchmark import (
    BenchmarkBudgetExceeded,
    benchmark_input,
    enforce_cost_cap,
    estimated_run_cost,
    evaluate_model,
    evaluation_route,
    score_case,
    selected_reasoning_efforts,
    summarize,
)
from evaluation.notification_cases import fixture_id, synthetic_cases
from app.services.notification_evidence import requires_source_pocket
from app.services.notification_interpretation import Interpretation
from app.services.openai_notification_provider import ProviderError


def correct_proposal(case):
    facts, context = benchmark_input(case)
    expected = case.expected
    record = expected["outcome"] == "record"
    return Interpretation.model_validate_json(json.dumps({
        "outcome": expected["outcome"], "direction": expected["direction"] if record else None,
        "description": "Bayar Kedai Awan" if record else "",
        "account_id": expected.get("account_id") if record else None,
        "source_account_id": expected.get("source_account_id") if record else None,
        "target_account_id": expected.get("target_account_id") if record else None,
        "category_id": expected.get("category_id") if record else None,
        "kakeibo": expected.get("kakeibo") if record else None,
        "amount_evidence": facts.amount_quotes[0] if record else "",
        "direction_evidence": context["notification"][:400] if record else "",
        "source_evidence": expected.get("source_evidence", "") if record else "",
        "target_evidence": expected.get("target_evidence", "") if record else "",
        "candidate_transaction_id": expected.get("candidate_transaction_id") if record else None,
        "confidence": 0.9, "review_reason": "none" if record else "uncertain_facts",
    }))


MODEL_CASES = [case for case in synthetic_cases() if evaluation_route(case) == "model"]


@pytest.mark.parametrize("case", MODEL_CASES, ids=lambda case: case.name)
def test_synthetic_expected_interpretations_and_trusted_scoring(case):
    row = score_case(case, correct_proposal(case), latency=0.1)
    assert row["schema"] and row["facts"] and row["references"]
    assert row["classification"] and row["movement"] and row["abstention"]
    assert not row["unsafe_validated_write"]
    facts, _ = benchmark_input(case)
    held = case.expected["outcome"] == "record" and requires_source_pocket(facts)
    expected = "needs_confirmation" if held else case.expected.get("validated_outcome", case.expected["outcome"])
    assert row["validated_outcome"] == expected


def test_provider_failures_are_counted_and_never_omitted():
    case = synthetic_cases()[0]
    provider = AsyncMock()
    provider.interpret.side_effect = ProviderError("provider_access_denied")
    report = asyncio.run(evaluate_model(provider, [case], repeats=3, interval=0))
    assert report["offered"] == report["failed_requests"] == 3
    assert report["completed"] == 0 and report["composite_score"] == 0
    assert report["eligible_by_evaluation"] is False
    assert report["requests_attempted"] == 1 and provider.interpret.await_count == 1
    assert report["unavailable_reason"] == "provider_access_denied"


def test_incomplete_provider_response_usage_is_counted_without_stale_usage():
    class IncompleteProvider:
        last_usage = None

        async def interpret(self, context, *, prompt):
            self.last_usage = {"input_tokens": 1000, "output_tokens": 500}
            raise ProviderError("provider_timeout")

    report = asyncio.run(evaluate_model(IncompleteProvider(), synthetic_cases()[:1], repeats=3, interval=0))
    assert report["requests_attempted"] == 1
    assert report["token_usage"]["input_tokens"] == 1000
    assert report["token_usage"]["output_tokens"] == 500
    assert report["observed_usage_usd"] == pytest.approx(0.0008)
    assert report["cases"][0]["usage"] is not None
    assert all(row["usage"] is None for row in report["cases"][1:])


def test_deterministic_routes_are_scored_without_provider_calls():
    provider = AsyncMock()
    provider.last_usage = None
    provider.interpret.side_effect = [
        correct_proposal(case)
        for case in MODEL_CASES
        for _ in range(3)
    ]
    report = asyncio.run(evaluate_model(provider, synthetic_cases(), repeats=3, interval=0))
    assert provider.interpret.await_count == len(MODEL_CASES) * 3
    assert report["requests_attempted"] == len(MODEL_CASES) * 3
    assert report["deterministic_routes"]["gate_pass"] is True
    assert report["deterministic_routes"]["by_route"]["entrance_filter"]["offered"] > 0
    assert report["deterministic_routes"]["by_route"]["evidence_review"]["offered"] > 0
    assert report["deterministic_routes"]["by_route"]["deterministic_stockbit"]["offered"] == 2


@pytest.mark.parametrize("error_code", ["provider_timeout", "provider_unavailable"])
def test_transient_provider_outage_circuits_after_one_synthetic_probe(error_code):
    provider = AsyncMock()
    provider.interpret.side_effect = ProviderError(error_code, transient=True)
    report = asyncio.run(evaluate_model(provider, synthetic_cases()[:2], repeats=3, interval=0))
    assert report["offered"] == report["failed_requests"] == 6
    assert report["requests_attempted"] == provider.interpret.await_count == 1
    assert report["unavailable_reason"] == error_code


def test_raw_foreign_reference_error_is_visible_even_if_guard_rejects_it():
    case = synthetic_cases()[0]
    proposal = correct_proposal(case).model_copy(update={"account_id": UUID(fixture_id(999))})
    row = score_case(case, proposal, latency=0.2)
    assert row["references"] is False and row["validation_rejected"]
    assert row["validated_outcome"] == "needs_review"
    assert row["validation_rejection_code"] == "invalid_account_reference"
    assert row["raw_proposal"]["account_id"] == str(UUID(fixture_id(999)))
    assert row["trusted_decision"] == {"outcome": "needs_review"}
    assert row["unsafe_validated_write"] is False


def test_safety_gate_overrides_high_composite_score():
    case = synthetic_cases()[0]
    row = score_case(case, correct_proposal(case), latency=0.1)
    row.update(unsafe_validated_write=True, unambiguous_classification=True)
    report = summarize([row])
    assert report["composite_score"] == 100
    assert report["validated_safety_gate_pass"] is False
    assert report["eligible_by_evaluation"] is False


def test_private_fixture_input_and_unsupported_reasoning_refused():
    case = synthetic_cases()[0]
    with pytest.raises(ValueError, match="synthetic"):
        benchmark_input(replace(case, context={"synthetic": False}))
    with pytest.raises(ValueError):
        selected_reasoning_efforts(["medium"])
    assert selected_reasoning_efforts(["none", "low"]) == ["low", "none"]


def test_conservative_cost_cap_refuses_before_live_requests():
    estimate = estimated_run_cost(MODEL_CASES, repeats=3, configurations=1)
    assert estimate > 0
    with pytest.raises(BenchmarkBudgetExceeded):
        enforce_cost_cap(estimate, estimate / 2)


def test_benchmark_modules_do_not_import_runtime_database_loaders():
    import subprocess
    import sys
    result = subprocess.run([
        sys.executable, "-c",
        "import sys; import evaluation.notification_benchmark; assert 'app.db.pool' not in sys.modules; assert 'app.core.config' not in sys.modules",
    ], check=True, capture_output=True)
    assert result.returncode == 0


SEQUENCE = {case.name: case for case in synthetic_cases() if case.expected.get("sequence")}


def test_sequence_cases_score_as_ordinary_observed_legs():
    rows = [score_case(case, correct_proposal(case), latency=0.1) for case in SEQUENCE.values()]
    report = summarize(rows)
    assert report["validated_safety_gate_pass"] is True and "same_money" not in report
    assert all(row["sequence"] and row["references"] and not row["unsafe_validated_write"] for row in rows)


ENGLISH_MODEL_CASES = [
    case for case in __import__("evaluation.notification_cases", fromlist=["english_output_cases"]).english_output_cases()
    if evaluation_route(case) == "model"
]


def english_proposal(case, description):
    return correct_proposal(case).model_copy(update={"description": description})


def test_english_cases_score_an_english_description_and_reject_placeholders():
    case = next(case for case in ENGLISH_MODEL_CASES if case.name == "jago-coffee")
    assert score_case(case, english_proposal(case, "Coffee at Kedai Awan"), latency=0.1)["description_rubric_pass"]
    for rejected in ("Payment", "Pembayaran", "Transfer in", "Bayar Kedai Awan"):
        assert not score_case(case, english_proposal(case, rejected), latency=0.1)["description_rubric_pass"], rejected
    indonesian = next(case for case in MODEL_CASES if case.name == "jago-coffee")
    assert score_case(indonesian, correct_proposal(indonesian), latency=0.1)["description_rubric_pass"]


def test_english_quality_gate_requires_english_descriptions():
    rows = [score_case(case, english_proposal(case, "Coffee at Kedai Awan"), latency=0.1) for case in ENGLISH_MODEL_CASES]
    for row in rows:
        row["unambiguous_classification"] = True
    assert summarize(rows)["quality_gate_pass"] is True
    rows = [score_case(case, correct_proposal(case), latency=0.1) for case in ENGLISH_MODEL_CASES]
    for row in rows:
        row["unambiguous_classification"] = True
    report = summarize(rows)
    assert report["description_rubric_percent"] == 0 and report["quality_gate_pass"] is False


def test_english_evaluation_sends_the_en_rendering_offline():
    from app.services.prompt_language import load_prompt

    provider = AsyncMock()
    provider.interpret.side_effect = [english_proposal(case, "Coffee at Kedai Awan") for case in ENGLISH_MODEL_CASES]
    report = asyncio.run(evaluate_model(provider, ENGLISH_MODEL_CASES, repeats=1, interval=0))
    assert report["requests_attempted"] == len(ENGLISH_MODEL_CASES)
    prompts = {call.kwargs["prompt"].version for call in provider.interpret.await_args_list}
    assert prompts == {load_prompt("notification_interpretation", "en").version} == {"notification-interpretation-13-en"}


@pytest.mark.parametrize("partition", ["development", "held_out"])
def test_english_benchmark_costs_about_what_the_indonesian_one_does(partition):
    from evaluation.notification_benchmark import benchmark_cases

    indonesian = estimated_run_cost(benchmark_cases("id", partition), repeats=3, configurations=1)
    english = estimated_run_cost(benchmark_cases("en", partition), repeats=3, configurations=1)
    assert 0 < english <= indonesian * 1.05


def named_case(name):
    return next(case for case in synthetic_cases() if case.name == name)


def test_schema_valid_record_without_category_is_scored_as_incomplete_not_as_schema_failure():
    case = named_case("integral-decimal")
    proposal = correct_proposal(case).model_copy(update={"category_id": None, "kakeibo": None})
    row = score_case(case, proposal, latency=0.1)
    assert row["schema"] and row["error_code"] is None
    assert row["incomplete_record"] == "category_required"
    assert row["validation_rejected"] and row["validation_rejection_code"] == "model_uncertain"
    assert row["validated_outcome"] == "needs_review"
    assert row["classification"] is False and row["unsafe_validated_write"] is False
    report = summarize([row])
    assert report["incomplete_records"] == 1 and report["failed_requests"] == 0
    assert report["raw_metrics_percent"]["schema"] == 100


def test_invalid_reply_shape_and_synthetic_text_are_recorded():
    from app.services.openai_notification_provider import OutputDiagnosis

    reply = json.dumps({"outcome": "record", "direction": "expense", "description": "x" * 161,
                        "account_id": fixture_id(1), "category_id": None, "mappings": [{}]})

    class LongReplyProvider:
        last_usage = None
        last_invalid_output = None

        async def interpret(self, context, *, prompt):
            self.last_usage = {"input_tokens": 3200, "output_tokens": 287}
            self.last_invalid_output = reply
            raise ProviderError("provider_invalid_output", diagnosis=OutputDiagnosis(
                "schema_mismatch", fields=("description:string_too_long",), output_tokens=287,
            ))

    report = asyncio.run(evaluate_model(LongReplyProvider(), [named_case("bca-qris")], repeats=1, interval=0))
    [row] = report["cases"]
    assert row["error_code"] == "provider_invalid_output" and row["schema"] is False
    failure = row["invalid_output"]
    assert failure["failure"] == "schema_mismatch" and failure["fields"] == ["description:string_too_long"]
    assert failure["synthetic_reply"] == reply
    assert failure["shape"]["string_lengths"]["description"] == 161
    assert failure["shape"]["null_references"] == [
        "source_account_id", "target_account_id", "category_id", "candidate_transaction_id",
    ]
    assert failure["shape"]["mappings"] == 1 and failure["shape"]["outcome"] == "record"
    assert report["failed_requests"] == 1 and report["invalid_output_failures"] == {"schema_mismatch": 1}


def test_undecodable_captured_reply_still_has_a_shape():
    from evaluation.notification_benchmark import reply_shape

    assert reply_shape('{"outcome":') == {"decodable": False, "length": 11}
    assert reply_shape(None) is None
