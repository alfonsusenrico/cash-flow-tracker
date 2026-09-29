from __future__ import annotations

from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.services.notification_evidence import EvidenceError, NotificationFacts, parse_idr_amount


PROMPT_VERSION = "notification-interpretation-5"
PROMPT_PATH = Path(__file__).with_name("prompts") / "notification_interpretation.md"
Kakeibo = Literal["need", "want", "culture", "unexpected", "saving"]


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

    @model_validator(mode="after")
    def check_record_shape(self):
        if self.outcome != "record":
            return self
        if not self.description.strip() or not self.direction or not self.amount_evidence:
            raise ValueError("Incomplete financial interpretation")
        if self.direction == "internal_movement":
            if not self.source_account_id or not self.target_account_id:
                raise ValueError("Movement endpoints required")
            if self.source_account_id == self.target_account_id:
                raise ValueError("Movement endpoints must differ")
        elif not self.account_id or not self.category_id:
            raise ValueError("Observed account and category required")
        return self


def validate_interpretation(
    interpretation: Interpretation, facts: NotificationFacts, context: dict
) -> None:
    if interpretation.outcome != "record":
        return
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


def system_prompt() -> str:
    return PROMPT_PATH.read_text(encoding="utf-8")
