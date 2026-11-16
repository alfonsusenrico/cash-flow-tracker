"""A transaction's name and the owner's note, and the `notes` alias older clients still speak.

The name is the row's title; automatic writers set and learn it. The note is a remark only the owner writes:
no automatic writer reads or writes it and no model input includes it (separate-transaction-name-and-note D3).
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import AfterValidator, BaseModel, Field, model_validator

NOTE_MAX_LENGTH = 500


def clean_note(value: str | None) -> str | None:
    """A trimmed note; whitespace alone is no note."""
    if value is None:
        return None
    return value.strip() or None


OwnerNote = Annotated[str | None, Field(max_length=NOTE_MAX_LENGTH), AfterValidator(clean_note)]


def fold_notes_alias(data: Any) -> Any:
    """Read a request's `notes` as its `name`; when both are sent, `name` wins (D2 compatibility window)."""
    if not isinstance(data, dict) or "notes" not in data:
        return data
    folded = dict(data)
    alias = folded.pop("notes")
    folded.setdefault("name", alias)
    return folded


class TransactionTextInput(BaseModel):
    """Request fields that write a transaction's text. Each model declares `name` with its own length limit."""

    note: OwnerNote = None

    @model_validator(mode="before")
    @classmethod
    def accept_notes_alias(cls, data: Any) -> Any:
        return fold_notes_alias(data)


def text_fields(row: dict[str, Any]) -> dict[str, Any]:
    """A transaction's name and note in a response, with `notes` kept as a copy of the name for older clients."""
    return {"name": row["name"], "note": row.get("note") or None, "notes": row["name"]}
