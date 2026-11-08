"""Strict schema and validation for classifying a typed capture and reading its time (design D2).

The model's time is a fallback for a phrase the parser flagged but could not place; it is accepted only
when deterministic checks pass (understand-capture-time D5), and it can never touch the amount.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.services.kakeibo import Pillar
from app.services.prompt_language import CAPTURE_NORMALIZATION, INDONESIAN_VERSIONS
from app.services.prompt_language import PLACEHOLDER_DESCRIPTIONS as PLACEHOLDERS_BY_LANGUAGE


CAPTURE_PROMPT_VERSION = INDONESIAN_VERSIONS[CAPTURE_NORMALIZATION]
# Upper bound of the serialised capture input; history, then memory, are dropped to fit.
CAPTURE_CONTEXT_BYTE_LIMIT = 30000
# Either language's placeholder is dropped whatever language the model wrote in: "Payment" like "Pembayaran".
PLACEHOLDER_DESCRIPTIONS = PLACEHOLDERS_BY_LANGUAGE["id"] | PLACEHOLDERS_BY_LANGUAGE["en"]
# Without an explicit date, a model time older than this is not believed.
MODEL_TIME_MAX_AGE = timedelta(days=31)


class CaptureTimeInterpretation(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    span: str | None = Field(max_length=80)
    local_datetime: str | None = Field(max_length=40)
    confidence: float = Field(ge=0, le=1)


class CaptureNormalization(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    merchant: str | None = Field(max_length=120)
    description: str = Field(min_length=1, max_length=160)
    type: Literal["expense", "income", "movement"]
    type_confidence: float = Field(ge=0, le=1)
    category_id: UUID | None
    kakeibo: Pillar | None
    category_confidence: float = Field(ge=0, le=1)
    account_id: UUID | None
    account_confidence: float = Field(ge=0, le=1)
    time: CaptureTimeInterpretation


class InvalidCaptureNormalization(ValueError):
    pass


def validate_capture_normalization(result: CaptureNormalization, context: dict) -> dict:
    """Checked proposal; raises InvalidCaptureNormalization for any reference outside the context.

    A category of the other direction is kept: the gate only uses it when the kind matches.
    """
    categories = {row["id"]: row for row in context["categories"]}
    category_id = str(result.category_id) if result.category_id else None
    if category_id and category_id not in categories:
        raise InvalidCaptureNormalization("invalid_category")
    account_id = str(result.account_id) if result.account_id else None
    if account_id and account_id not in {row["id"] for row in context["accounts"]}:
        raise InvalidCaptureNormalization("invalid_account")
    category = categories.get(category_id) if category_id else None
    kakeibo = None
    if category and category["kind"] == "expense":
        kakeibo = result.kakeibo or category.get("default_kakeibo")
    description = " ".join(result.description.split())
    if description.casefold() in PLACEHOLDER_DESCRIPTIONS:
        description = ""
    return {
        "merchant": " ".join((result.merchant or "").split()) or None,
        "description": description or None,
        "type": result.type, "type_confidence": result.type_confidence,
        "category_id": category_id, "category_kind": category["kind"] if category else None,
        "kakeibo": kakeibo, "category_confidence": result.category_confidence,
        "account_id": account_id, "account_confidence": result.account_confidence,
    }



def validate_capture_time(
    result: CaptureTimeInterpretation, *, text: str, time_facts: dict, device_time: datetime, tz: str,
    threshold: float,
) -> tuple[dict | None, str]:
    """(checked time, outcome code) for the model's reading of a flagged time phrase.

    Usable only when the parser left the time unresolved, the span is the owner's own words around a
    flagged phrase, the moment is not after the device time, it falls on the parsed day or explicit date
    when there is one and otherwise within 31 days. Below the capture threshold the checked time is only a
    suggestion for the question (`suggestion_only`).
    """
    if time_facts.get("status") != "unresolved":
        return None, "not_needed"
    if not result.span or not result.local_datetime:
        return None, "no_time"
    collapsed = " ".join(text.split())
    source = collapsed.casefold()
    span = " ".join(result.span.split()).casefold()
    hints = [" ".join(hint.split()).casefold() for hint in time_facts.get("hints") or [] if hint]
    if not span or span not in source or not any(hint in span for hint in hints):
        return None, "span_not_in_text"
    try:
        moment = datetime.fromisoformat(result.local_datetime)
    except ValueError:
        return None, "invalid_datetime"
    if moment.tzinfo is None:
        return None, "invalid_datetime"
    if moment > device_time:
        return None, "future"
    local = moment.astimezone(ZoneInfo(tz))
    named = time_facts.get("explicit_date") or time_facts.get("day")
    # The day is the parser's when it read one; the model may only place the time within it.
    if named and local.date() != date.fromisoformat(named):
        return None, "day_mismatch"
    if not time_facts.get("explicit_date") and device_time - moment > MODEL_TIME_MAX_AGE:
        return None, "too_old"
    start = source.index(span)
    evidence = collapsed[start:start + len(span)] if len(source) == len(collapsed) else span
    checked = {"date": local.isoformat(), "evidence": evidence, "confidence": result.confidence}
    if result.confidence < threshold:
        return {**checked, "suggestion_only": True}, "low_confidence"
    return checked, "accepted"
