"""Strict schema and validation for classifying a typed capture (design D2)."""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.services.kakeibo import Pillar


CAPTURE_PROMPT_VERSION = "capture-normalization-1"
CAPTURE_PROMPT_PATH = Path(__file__).with_name("prompts") / "capture_normalization.md"
# Upper bound of the serialised capture input; history, then memory, are dropped to fit.
CAPTURE_CONTEXT_BYTE_LIMIT = 30000
PLACEHOLDER_DESCRIPTIONS = frozenset({
    "pembayaran", "belanja", "transaksi", "pengeluaran", "pemasukan", "transfer", "lainnya",
})


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


class InvalidCaptureNormalization(ValueError):
    pass


def capture_prompt() -> str:
    return CAPTURE_PROMPT_PATH.read_text(encoding="utf-8")


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
        kakeibo = result.kakeibo or category.get("default_kakeibo") or "need"
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
