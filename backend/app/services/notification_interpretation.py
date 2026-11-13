from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.services.kakeibo import Pillar
from app.services.notification_evidence import EvidenceError, NotificationFacts, parse_idr_amount
from app.services.prompt_language import INDONESIAN_VERSIONS, NOTIFICATION_INTERPRETATION


PROMPT_VERSION = INDONESIAN_VERSIONS[NOTIFICATION_INTERPRETATION]
Kakeibo = Pillar


class MappingProposal(BaseModel):
    """The model's choice of owned account for a name the backend could not resolve."""

    model_config = ConfigDict(extra="forbid", strict=True)

    role: Literal["observed", "source", "target"]
    account_id: UUID
    confidence: float = Field(ge=0, le=1)
    alternatives: list[UUID] = Field(max_length=3)


class Interpretation(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    outcome: Literal["record", "ignored", "needs_review"]
    direction: Literal["expense", "income", "internal_movement"] | None
    description: str = Field(max_length=160)
    merchant: str | None = Field(default=None, max_length=120)
    account_id: UUID | None
    source_account_id: UUID | None
    target_account_id: UUID | None
    category_id: UUID | None
    kakeibo: Kakeibo | None
    amount_evidence: str = Field(max_length=100)
    direction_evidence: str = Field(max_length=400)
    source_evidence: str = Field(max_length=200)
    target_evidence: str = Field(max_length=200)
    candidate_transaction_id: UUID | None
    confidence: float = Field(ge=0, le=1)
    review_reason: Literal["uncertain_facts", "uncertain_mapping", "ambiguous_movement", "none"]
    mappings: list[MappingProposal] = Field(default_factory=list, max_length=3)


# Sources whose interpretation is built by backend code; only these may record a row without a category.
DETERMINISTIC_SOURCES = frozenset({"deterministic", "deterministic_fallback"})


def record_shape_error(interpretation: Interpretation, *, allow_uncategorized: bool = False) -> str | None:
    """What a record still lacks, as a diagnostic reason, or None.

    These rules are not part of the structured-output schema sent to the provider (strict mode cannot express
    conditional requirements), so a reply that breaks them is a complete answer that the backend rejects, never
    an invalid provider output (align-notification-output-contract D1). The reason is diagnostic only: it is
    reported by the benchmark and is not an error code.
    """
    if interpretation.outcome != "record":
        return None
    if not interpretation.description.strip() or not interpretation.direction or not interpretation.amount_evidence:
        return "incomplete_record"
    if interpretation.direction == "internal_movement":
        if not interpretation.source_account_id or not interpretation.target_account_id:
            return "movement_endpoints_required"
        if interpretation.source_account_id == interpretation.target_account_id:
            return "movement_endpoints_identical"
        return None
    if not interpretation.account_id:
        return "observed_account_required"
    if not interpretation.category_id and not allow_uncategorized:
        return "category_required"
    return None


def validate_interpretation(
    interpretation: Interpretation, facts: NotificationFacts, context: dict, *, allow_uncategorized: bool = False
) -> None:
    if interpretation.outcome != "record":
        return
    if record_shape_error(interpretation, allow_uncategorized=allow_uncategorized):
        # A model record without a required reference is model uncertainty (D2): the deterministic fallback
        # decides what can be recorded, and the model is not asked again.
        raise EvidenceError("model_uncertain")
    if facts.confirmed_source_account_id:
        proposed_source = (interpretation.source_account_id if interpretation.direction == "internal_movement"
                           else interpretation.account_id)
        if str(proposed_source) != facts.confirmed_source_account_id:
            raise EvidenceError("conflicting_source_pocket")
    if facts.needs_reading:
        # Unrecognised wording records only from the event's accepted, stored reading.
        raise EvidenceError("settlement_unproven")
    if facts.status != "candidate" or facts.amount is None or not facts.direction:
        raise EvidenceError(facts.error_code or "direction_unproven")
    if interpretation.direction != facts.direction:
        raise EvidenceError("conflicting_direction")
    if interpretation.direction == "income" and interpretation.kakeibo is not None:
        raise EvidenceError("income_kakeibo_not_permitted")
    if interpretation.amount_evidence not in facts.amount_quotes:
        raise EvidenceError("fabricated_amount_evidence")
    if parse_idr_amount(interpretation.amount_evidence) != facts.amount:
        raise EvidenceError("conflicting_amount")
    evidence_text = context.get("notification", facts.text)
    if not interpretation.direction_evidence or interpretation.direction_evidence not in evidence_text:
        raise EvidenceError("fabricated_direction_evidence")
    for quote in (interpretation.source_evidence, interpretation.target_evidence):
        if quote and quote not in evidence_text:
            raise EvidenceError("fabricated_endpoint_evidence")
    if context.get("incomplete"):
        raise EvidenceError("incomplete_context")
    accounts = {str(account["id"]): account for account in context["accounts"]}
    for reference in (
        interpretation.account_id, interpretation.source_account_id, interpretation.target_account_id
    ):
        if reference is None:
            continue
        account = accounts.get(str(reference))
        if not account or account.get("type") not in {"bank", "cash", "wallet", "ewallet"} or account.get("instrument_type"):
            raise EvidenceError("invalid_account_reference")
    if interpretation.category_id is not None:
        categories = {str(category["id"]): category for category in context["categories"]}
        category = categories.get(str(interpretation.category_id))
        if not category or category["kind"] != interpretation.direction:
            raise EvidenceError("invalid_category_reference")
    roles = [mapping.role for mapping in interpretation.mappings]
    if len(roles) != len(set(roles)):
        raise EvidenceError("invalid_mapping_proposal")
    for mapping in interpretation.mappings:
        for reference in (mapping.account_id, *mapping.alternatives):
            account = accounts.get(str(reference))
            if not account or account.get("type") not in {"bank", "cash", "wallet", "ewallet"} or account.get("instrument_type"):
                raise EvidenceError("invalid_mapping_proposal")
    if interpretation.candidate_transaction_id is not None:
        candidates = {str(candidate["id"]) for candidate in context.get("candidates", [])}
        if str(interpretation.candidate_transaction_id) not in candidates:
            raise EvidenceError("invalid_candidate_reference")
