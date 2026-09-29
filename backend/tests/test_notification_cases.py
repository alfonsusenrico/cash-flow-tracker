import pytest

from evaluation.notification_cases import synthetic_cases
from app.services.notification_evidence import collect_facts


def test_fixture_scope_and_partition():
    cases = synthetic_cases()
    assert len(cases) >= 40
    assert len({case.name for case in cases}) == len(cases)
    assert {case.partition for case in cases} == {"development", "held_out"}
    assert all(case.context["synthetic"] for case in cases)
    assert {"movement-29", "movement-30", "movement-ambiguity", "movement-history-omitted"} <= {case.name for case in cases}


@pytest.mark.parametrize("case", synthetic_cases(), ids=lambda case: case.name)
def test_synthetic_fact_expectations(case):
    facts = collect_facts(case.event)
    assert facts.status == case.expected["facts_status"]
    assert facts.amount == case.expected["amount"]
    assert facts.direction == case.expected["direction"]
    assert facts.timestamp.second == 27


def test_unpaired_cross_account_income_uses_an_income_category_not_movement_category():
    case = next(case for case in synthetic_cases() if case.name == "gopay-jago-in")
    assert case.expected["outcome"] == "record"
    assert case.expected["category_id"] == "00000000-0000-4000-8000-000000000068"
    assert case.expected["candidate_transaction_id"] is None


def test_model_context_identifies_the_default_pocket_instead_of_its_parent():
    from copy import deepcopy
    from evaluation.notification_benchmark import benchmark_input
    from evaluation.notification_cases import SyntheticCase, fixture_id

    case = next(case for case in synthetic_cases() if case.name == "mybca-en-income")
    context = deepcopy(case.context)
    context["accounts"][0]["default_pocket_id"] = fixture_id(10)
    context["accounts"].append({"id": fixture_id(10), "name": "ATM", "type": "bank", "parent_id": fixture_id(1)})
    _, model_context = benchmark_input(SyntheticCase(case.name, case.partition, case.event, context, case.expected))
    assert model_context["facts"]["effective_account_id"] == fixture_id(10)


def test_unknown_pocket_is_an_explicit_mapping_error_in_model_context():
    from evaluation.notification_benchmark import benchmark_input
    case = next(case for case in synthetic_cases() if case.name == "jago-unknown-pocket")
    _, model_context = benchmark_input(case)
    assert model_context["facts"]["mapping_error"] == "uncertain_pocket_mapping"
