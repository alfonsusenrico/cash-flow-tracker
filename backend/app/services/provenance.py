"""Where a transaction came from, whether the owner still needs to review it, and who classified it."""

from __future__ import annotations

import re
from dataclasses import dataclass

SOURCES: tuple[str, ...] = (
    "manual", "notification", "recurring", "payroll", "movement", "reconcile", "investment", "import", "capture",
)
REVIEW_STATUSES: tuple[str, ...] = ("auto", "needs_review", "reviewed")
CLASSIFIERS: tuple[str, ...] = ("rule", "ai", "owner", "system")

SOURCE_PATTERN = "^(" + "|".join(SOURCES) + ")$"
REVIEW_STATUS_PATTERN = "^(" + "|".join(REVIEW_STATUSES) + ")$"
# "auto" describes an unconfirmed automatic classification, so only automation may write it.
OWNER_REVIEW_STATUS_PATTERN = "^(needs_review|reviewed)$"

MERCHANT_MAX_LENGTH = 120

# Fields whose change by the owner makes the owner the classifier of the row.
CLASSIFICATION_FIELDS: tuple[str, ...] = (
    "category_id", "kakeibo_type", "notes", "merchant", "type", "offset_category_id",
)


@dataclass(frozen=True)
class Provenance:
    source: str
    review_status: str
    classified_by: str | None
    confidence: float | None = None

    def __post_init__(self) -> None:
        if self.source not in SOURCES:
            raise ValueError(f"unknown transaction source: {self.source}")
        if self.review_status not in REVIEW_STATUSES:
            raise ValueError(f"unknown review status: {self.review_status}")
        if self.classified_by is not None and self.classified_by not in CLASSIFIERS:
            raise ValueError(f"unknown classifier: {self.classified_by}")
        if self.confidence is not None and not 0 <= self.confidence <= 1:
            raise ValueError("classification confidence must be between 0 and 1")

    def columns(self) -> tuple[str, str, str | None, float | None]:
        """Values for (source, review_status, classified_by, classification_confidence)."""
        return (self.source, self.review_status, self.classified_by, self.confidence)


def system_provenance(source: str) -> Provenance:
    """Rows derived from something the owner configured: a movement, rule, reconcile or top-up."""
    return Provenance(source, "reviewed", "system")


def owner_provenance(review_status: str = "reviewed") -> Provenance:
    return Provenance("manual", review_status, "owner")


def notification_provenance(interpretation_source: str | None, confidence: float | None) -> Provenance:
    """Provenance of a notification-recorded row from the interpretation that produced it.

    The deterministic interpretation carries a fixed confidence of 1.0 that was never measured, so
    only a model's confidence is stored.
    """
    if interpretation_source == "manual":
        return Provenance("notification", "reviewed", "owner")
    if interpretation_source == "ai":
        return Provenance("notification", "auto", "ai", confidence)
    return Provenance("notification", "auto", "rule")


def capture_provenance(classified_by: str, confidence: float | None = None) -> Provenance:
    """Rows recorded from the owner's typed text count as reviewed (owner decision 2026-10-29)."""
    return Provenance("capture", "reviewed", classified_by, confidence if classified_by == "ai" else None)


# Applied when the owner changes a classification field; the old confidence described another classifier.
OWNER_EDIT_ASSIGNMENTS = "owner_edited_at = NOW(), classified_by = 'owner', classification_confidence = NULL"

_WHITESPACE = re.compile(r"\s+")


def normalize_merchant(value: str | None) -> str | None:
    """Trim and collapse whitespace; keep case because it is a display name. Blank means no merchant."""
    if value is None:
        return None
    normalized = _WHITESPACE.sub(" ", value).strip()
    if not normalized:
        return None
    if len(normalized) > MERCHANT_MAX_LENGTH:
        raise ValueError(f"merchant must be at most {MERCHANT_MAX_LENGTH} characters")
    return normalized
