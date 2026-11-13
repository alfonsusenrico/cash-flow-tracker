"""The model's reading of notification wording the fixed rules do not recognise (read-unrecognised-notifications D2).

The reading supplies only what the rules could not prove: whether money moved, its direction, and for a
Jago move the pocket names as written. It is accepted only with verbatim quotes and enough confidence,
and it is stored so later steps of the event use the same facts without asking the model again.
"""

from __future__ import annotations

from app.services.notification_evidence import EvidenceError, NotificationFacts, parse_idr_amount, text_fingerprint
from app.services.notification_interpretation import Interpretation


READING_DIRECTIONS = frozenset({"expense", "income", "internal_movement"})


def model_reading(
    proposal: Interpretation, facts: NotificationFacts, context: dict, prompt_version: str | None, *, threshold: float,
) -> dict:
    """The reading to store, or EvidenceError("wording_unconfirmed") when the answer does not prove it.

    `threshold` is the review confidence threshold, passed in so the benchmark stays free of runtime settings.
    """
    text = context.get("notification", facts.text)
    try:
        amount_matches = (proposal.amount_evidence in facts.amount_quotes
                          and parse_idr_amount(proposal.amount_evidence) == facts.amount)
    except EvidenceError:
        amount_matches = False
    proven = (
        proposal.outcome == "record" and proposal.direction in READING_DIRECTIONS and amount_matches
        and bool(proposal.direction_evidence.strip()) and proposal.direction_evidence in text
        and all(not quote or quote in text for quote in (proposal.source_evidence, proposal.target_evidence))
        # A direction cue the rules did find still has to agree with the model.
        and (facts.direction is None or facts.direction == proposal.direction)
        and proposal.confidence >= threshold
    )
    if proposal.direction == "internal_movement":
        proven = proven and facts.institution == "jago" and bool(proposal.target_evidence.strip())
    if not proven:
        raise EvidenceError("wording_unconfirmed")
    source_named = proposal.direction == "internal_movement" and bool(proposal.source_evidence.strip())
    suggestion = None
    if proposal.direction == "internal_movement" and not source_named and proposal.source_account_id:
        suggestion = str(proposal.source_account_id)
    return {
        "direction": proposal.direction,
        "source_name": proposal.source_evidence.strip() or None if source_named else None,
        "target_name": proposal.target_evidence.strip() or None if proposal.direction == "internal_movement" else None,
        "source_named": source_named,
        "suggested_source_account_id": suggestion,
        "confidence": round(float(proposal.confidence), 4),
        "prompt_version": prompt_version,
        "text_hash": text_fingerprint(facts.text),
    }
