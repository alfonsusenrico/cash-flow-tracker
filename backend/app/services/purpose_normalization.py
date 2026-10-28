"""Strict schema and validation for normalising the owner's free-text purpose answer."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.services.kakeibo import Pillar


PURPOSE_PROMPT_VERSION = "purpose-normalization-1"
PURPOSE_PROMPT_PATH = Path(__file__).with_name("prompts") / "purpose_normalization.md"


class PurposeNormalization(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    merchant: str | None = Field(max_length=120)
    description: str = Field(min_length=1, max_length=160)
    category_id: UUID | None
    kakeibo: Pillar | None
    offset_category_id: UUID | None
    confidence: float = Field(ge=0, le=1)


class InvalidNormalization(ValueError):
    pass


def purpose_prompt() -> str:
    return PURPOSE_PROMPT_PATH.read_text(encoding="utf-8")


def validate_normalization(result: PurposeNormalization, context: dict, direction: str) -> dict:
    """Checked values to apply; raises InvalidNormalization for any reference outside the context."""
    description = " ".join(result.description.split())
    if not description:
        raise InvalidNormalization("empty_description")
    merchant = " ".join((result.merchant or "").split()) or None
    categories = {str(row["id"]): row for row in context["categories"]}
    category_id = str(result.category_id) if result.category_id else None
    if category_id and category_id not in categories:
        raise InvalidNormalization("invalid_category")
    if direction == "income" and result.kakeibo is not None:
        raise InvalidNormalization("income_pillar")
    offset_id = str(result.offset_category_id) if result.offset_category_id else None
    if offset_id:
        eligible = {str(row["id"]) for row in context.get("offset_categories", [])}
        if not category_id or not categories[category_id].get("offsets_spending") or offset_id not in eligible:
            raise InvalidNormalization("invalid_offset")
    return {
        "merchant": merchant, "notes": description, "category_id": category_id,
        "kakeibo_type": result.kakeibo if direction == "expense" and category_id else None,
        "offset_category_id": offset_id,
    }
