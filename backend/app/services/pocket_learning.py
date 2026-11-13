"""Which pocket pays, learned from the owner's own choices (learn-payment-pockets).

A notification that names no pocket of an account with pockets is never put on an assumed pocket. The owner's
earlier choices for the same key decide it when they agree strongly enough; otherwise the owner is asked with
the likely pocket preselected. Deterministic statistics, no model.

Evidence is read from what the ledger already holds, so there is nothing to backfill:
- an answer to the source-pocket question of an event, or the account the owner chose when resolving it by
  hand (weight 1);
- a payment the owner later moved to another pocket than the one it was recorded on (a correction, weight 2).
A payment never moved is not evidence: an unnoticed wrong pocket must not teach.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.services.merchant_memory import SUPPORTED_PACKAGES, classification_key, text_signature

# Only the latest choices of a key count, so a changed habit is learned (design D5).
EVIDENCE_WINDOW = 10
ANSWER_WEIGHT = 1
# The owner went out of their way to fix a recorded pocket, so a correction counts double.
CORRECTION_WEIGHT = 2
# Record automatically when the rule of succession, (agreeing + 1) / (total + 2), reaches this (design D5).
AUTO_PROBABILITY = 0.75
# Events read to find one key's choices; keys are computed in Python, so the scan is bounded.
SCAN_LIMIT = 300
FIRST_WORD = re.compile(r"[a-z]+")


@dataclass(frozen=True)
class PocketDecision:
    """The learned pocket of a key: `auto` records without a question; otherwise it is only preselected."""

    account_id: str | None
    auto: bool
    # Choices, not weights: shown to the owner as "2 dari 3 pilihan sebelumnya".
    agreeing: int
    support: int


def first_word(name: str | None) -> str | None:
    """The merchant's first word, lower-cased letters only: "SPBU 54.602.45 HR M" and "SPBU No ID …" are both
    "spbu". Station and branch numbers would otherwise split one merchant into many (design D4)."""
    match = FIRST_WORD.search((name or "").lower())
    return match.group(0) if match else None


def payment_key(facts, identity: list[str]) -> str | None:
    """The learning key of a payment: the merchant or recipient's first word, else the kind of notification
    (for example a debit card payment)."""
    if facts.status != "candidate" or facts.direction != "expense":
        return None
    key = classification_key(facts, identity)
    word = first_word(key.get("merchant")) if key else None
    if word:
        return f"{facts.institution}|merchant:{word}"
    return f"{facts.institution}|signature:{text_signature(facts)}"


def move_key(target_id: str) -> str:
    """Money added into a pocket with no named source learns per target pocket."""
    return f"target:{target_id}"


def decide(choices: list[tuple[str, int]], *, auto_enabled: bool) -> PocketDecision:
    """The decision from (account_id, weight) choices, newest first."""
    window = choices[:EVIDENCE_WINDOW]
    weights: dict[str, int] = {}
    for account_id, weight in window:
        weights[account_id] = weights.get(account_id, 0) + weight
    if not weights:
        return PocketDecision(None, False, 0, 0)
    total = sum(weights.values())
    # dict order is first-seen, newest first: a tie goes to the owner's latest choice.
    top = max(weights, key=weights.__getitem__)
    probability = (weights[top] + 1) / (total + 2)
    agreeing = sum(1 for account_id, _weight in window if account_id == top)
    return PocketDecision(top, auto_enabled and probability >= AUTO_PROBABILITY, agreeing, len(window))


def learned_pocket(cur, event: dict, facts, *, eligible: set[str], target_id: str | None,
                   auto_enabled: bool) -> PocketDecision:
    """The owner's learned pocket for this event's key, among the eligible pockets."""
    from app.services.merchant_memory import owner_identity

    user_id = str(event["user_id"])
    identity = owner_identity(cur, user_id)
    key = move_key(target_id) if target_id else payment_key(facts, identity)
    if key is None:
        return PocketDecision(None, False, 0, 0)
    choices = load_choices(cur, user_id, institution=facts.institution, key=key, exclude_event_id=str(event["id"]),
                           identity=identity)
    return decide([choice for choice in choices if choice[0] in eligible], auto_enabled=auto_enabled)


def load_choices(cur, user_id: str, *, institution: str, key: str, exclude_event_id: str,
                 identity: list[str]) -> list[tuple[str, int]]:
    """The owner's choices for `key`, newest first, one per event: its correction if any, else its answer."""
    from app.services.notification_evidence import collect_facts
    from app.services.notification_processing import source_event
    from app.services.notification_source_pocket import jago_accounts

    packages = sorted(package for package, name in SUPPORTED_PACKAGES.items() if name == institution)
    cur.execute(
        """SELECT n.*, a.account_id AS answered_account_id, t.account_id AS current_account_id,
                  t.type AS current_type, t.movement_id AS current_movement_id
           FROM notification_events n
           LEFT JOIN notification_source_pocket_answers a ON a.user_id = n.user_id AND a.event_id = n.id
           LEFT JOIN transactions t ON t.user_id = n.user_id AND t.id = n.transaction_id
           WHERE n.user_id = %s AND lower(n.package_name) = ANY(%s) AND n.id <> %s
             AND (a.event_id IS NOT NULL
                  OR (t.type = 'expense' AND t.movement_id IS NULL
                      AND (t.account_id::text <> n.interpretation ->> 'account_id'
                           OR n.interpretation ->> 'source' = 'manual')))
           ORDER BY GREATEST(t.updated_at, a.created_at) DESC NULLS LAST, n.id
           LIMIT %s""",
        (user_id, packages, exclude_event_id, SCAN_LIMIT),
    )
    rows = cur.fetchall()
    accounts = jago_accounts(cur, user_id) if key.startswith("target:") else None
    choices: list[tuple[str, int]] = []
    for row in rows:
        facts = collect_facts(source_event(row))
        if accounts is not None:
            from app.services.notification_source_pocket import resolve_move_target

            target = resolve_move_target(accounts, facts)
            if target is None or move_key(target) != key:
                continue
        elif payment_key(facts, identity) != key:
            continue
        recorded = (row.get("interpretation") or {}).get("account_id")
        current = str(row["current_account_id"]) if row.get("current_account_id") else None
        plain_expense = row.get("current_type") == "expense" and not row.get("current_movement_id")
        if current and recorded and current != recorded and plain_expense:
            choices.append((current, CORRECTION_WEIGHT))
        elif row.get("answered_account_id"):
            choices.append((str(row["answered_account_id"]), ANSWER_WEIGHT))
        elif current and plain_expense and (row.get("interpretation") or {}).get("source") == "manual":
            # Resolved by hand in Tinjau: the owner chose the account as an answer would.
            choices.append((current, ANSWER_WEIGHT))
        if len(choices) >= EVIDENCE_WINDOW:
            break
    return choices
