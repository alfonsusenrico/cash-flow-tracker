"""Offline checks of the synthetic item-group benchmark: cases build and the scoring is strict."""

from app.services.capture_normalization import CaptureItemGroup, validated_item_group
from evaluation.capture_item_benchmark import CASES, EXISTING, case_context, classify


def case(name):
    return next(item for item in CASES if item.name == name)


def test_cases_are_distinct_and_carry_the_owner_groups():
    assert len({item.name for item in CASES}) == len(CASES)
    for item in CASES:
        context = case_context(item)
        assert context["text"] == item.text and context["item_groups"]


def test_scoring_separates_correct_wrong_new_and_fallback():
    parking = case("parking-new-word")
    assert classify(parking, "parkir") == "correct"
    assert classify(parking, "kopi") == "wrong_existing"
    assert classify(parking, "parkiran") == "other_new_group"
    assert classify(parking, None) == "fallback_first_word"
    assert "parkir" in EXISTING


def test_the_production_check_runs_on_benchmark_contexts():
    context = case_context(case("haircut-synonym"))
    assert validated_item_group(CaptureItemGroup(group="potong rambut", span="cukur rambut"), context) == "potong rambut"
    assert validated_item_group(CaptureItemGroup(group="potong rambut", span="tidak ada"), context) is None
