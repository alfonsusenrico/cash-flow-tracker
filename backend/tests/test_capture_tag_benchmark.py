"""Offline checks of the synthetic capture tag benchmark: cases build and the scoring is strict."""

import json
from uuid import UUID

from app.services.capture_normalization import CaptureNormalization, validate_capture_normalization
from evaluation.capture_tag_benchmark import CASES, TAGS, case_context, classify


def checked(case, tags, description="Makan"):
    context = case_context(case)
    result = CaptureNormalization.model_validate_json(json.dumps({
        "merchant": None, "description": description, "type": "expense", "type_confidence": 0.9,
        "category_id": None, "kakeibo": None, "category_confidence": 0.5, "account_id": None,
        "account_confidence": 0, "time": {"span": None, "local_datetime": None, "confidence": 0}, "tags": tags,
    }))
    return validate_capture_normalization(result, context)


def case(name):
    return next(item for item in CASES if item.name == name)


def test_cases_are_synthetic_and_reference_known_tags():
    known = {tag["id"] for tag in TAGS}
    assert len({item.name for item in CASES}) == len(CASES)
    for item in CASES:
        assert item.existing <= known
        assert all(UUID(tag_id) for tag_id in item.existing)
        context = case_context(item)
        assert context["text"] == item.text
    assert case_context(case("none-plain"))["tags"] == []
    assert case_context(case("none-item-before-colon"))["tags"]


def test_scoring_separates_correct_unwanted_duplicate_and_missed():
    shortening = case("prefix-shortening")
    coach = shortening.existing and next(iter(shortening.existing))
    assert classify(shortening, checked(shortening, [{"tag_id": coach, "new_name": None, "span": "fellow coach"}],
                                        "Bayar makan di Kafe Contoh"))["outcome"] == "correct"
    assert classify(shortening, checked(shortening, [{"tag_id": None, "new_name": "fellow coach",
                                                      "span": "fellow coach"}]))["outcome"] == "duplicate_tag"
    assert classify(shortening, checked(shortening, []))["outcome"] == "missed_tag"
    item = case("none-item-before-colon")
    assert classify(item, checked(item, [{"tag_id": None, "new_name": "makan siang",
                                          "span": "makan siang"}]))["outcome"] == "unwanted_tag"
    assert classify(shortening, checked(shortening, [{"tag_id": coach, "new_name": None, "span": "fellow coach"}],
                                        "fellow coach makan"))["name_repeats_tag"] is True
