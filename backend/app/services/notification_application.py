from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from app.core.config import settings
from app.services.ledger_mutations import (
    LIQUID_ACCOUNT_TYPES, create_bilateral_movement, get_locked_ledger_balance, lock_owned_accounts,
    movement_kakeibo,
)
from app.services.merchant_memory import (
    PLACEHOLDER_COUNTERPARTIES, count_memory_hit, learn_from_model, normalize_key_text, stored_key_fields,
)
from app.services.movement_linkage import SELF_TRANSFER_NOTES, movement_note
from app.services.movement_pairing import find_counterpart, flag_ambiguous, leg_evidence, link_pair, load_accounts
from app.services.notification_context import CANDIDATE_LIMIT, discover_candidates, sanitize_text
from app.services.notification_evidence import EvidenceError, NotificationFacts, parse_idr_amount
from app.services.notification_interpretation import Interpretation, validate_interpretation
from app.services.notification_resolution import (
    institution_parent, main_endpoint, movement_categories, observed_endpoint,
    pocket_endpoint, root_account_id, transfer_signature,
)
from app.services.notification_sender import sender_gate
from app.services.provenance import MERCHANT_MAX_LENGTH, Provenance, normalize_merchant, notification_provenance


# A second notification may confirm a movement role only when it arrives this close to it.
ROLE_CONFIRMATION_WINDOW_SECONDS = 30

# myBCA "Financial Diary" uses a fixed English category taxonomy; map it to the
# Indonesian category names this app seeds so deterministic processing can resolve them.
BANK_CATEGORY_HINT_ALIASES = {
    "shopping": ("Belanja",),
    "salary": ("Gaji",),
    "food & beverage": ("Makanan & Minuman", "Makanan"),
    "food & beverages": ("Makanan & Minuman", "Makanan"),
    "transportation": ("Transportasi",),
    "entertainment": ("Hiburan",),
    "health": ("Kesehatan",),
    "education": ("Pendidikan",),
    "bills": ("Tagihan & Utilitas", "Tagihan"),
    "payment": ("Tagihan & Utilitas", "Pembayaran"),
    "account transfer": ("Transfer Keluar", "Transfer Masuk", "Transfer"),
    "transfer rekening": ("Transfer Keluar", "Transfer Masuk", "Transfer"),
    "miscellaneous": ("Lainnya", "Lain-lain"),
}


def hinted_categories(categories: list[dict], hint: str | None) -> list[dict]:
    if not hint:
        return []
    exact = [row for row in categories if row["name"].casefold() == hint.casefold()]
    if exact:
        return exact
    for alias in BANK_CATEGORY_HINT_ALIASES.get(hint.strip().casefold(), ()):
        matches = [row for row in categories if row["name"].casefold() == alias.casefold()]
        if matches:
            return matches
    return []


def generic_description(facts: NotificationFacts) -> str:
    """Indonesian description for a notification that names no merchant, never a parser placeholder."""
    hint = " ".join((facts.parsed.category_hint or "").split())
    folded = hint.casefold()
    if folded in {"transfer masuk", "transfer keluar"}:
        return hint[:1].upper() + hint[1:].lower()
    base = "Pengeluaran" if facts.direction == "expense" else "Pemasukan"
    if not hint or folded in {"pengeluaran", "pemasukan"} or folded in PLACEHOLDER_COUNTERPARTIES:
        return base
    if folded.startswith("pembayaran"):
        return hint
    return f"{base} {hint}"


def notification_description(facts: NotificationFacts, context: dict) -> str:
    key = context.get("_classification_key") or {}
    if key.get("merchant"):
        return sanitize_text(key["merchant"])[:160]
    counterparty = " ".join((facts.parsed.counterparty or "").split())
    if counterparty and normalize_key_text(counterparty) not in PLACEHOLDER_COUNTERPARTIES:
        return sanitize_text(counterparty)[:160]
    return generic_description(facts)


def memory_interpretation(facts: NotificationFacts, context: dict) -> Interpretation | None:
    """Classification from the owner's merchant memory, or None when no owner entry applies (design D5)."""
    hit = context.get("_owner_memory_hit")
    endpoint_facts = context.get("facts") or {}
    if (not hit or facts.status != "candidate" or facts.direction not in {"expense", "income"} or not facts.amount
            or endpoint_facts.get("unresolved_names") or endpoint_facts.get("mapping_error")):
        return None
    try:
        observed = observed_endpoint(facts, context["accounts"])
    except EvidenceError:
        return None
    payload = {
        "outcome": "record", "direction": facts.direction,
        "description": sanitize_text(hit["notes"] or hit["merchant"] or notification_description(facts, context))[:160],
        "merchant": hit["merchant"], "account_id": str(observed["id"]),
        "source_account_id": None, "target_account_id": None,
        "category_id": hit["category_id"], "kakeibo": hit["kakeibo"],
        "amount_evidence": facts.amount_quotes[0], "direction_evidence": context.get("notification", facts.text)[:400],
        "source_evidence": "", "target_evidence": "", "candidate_transaction_id": None,
        "confidence": 1.0, "review_reason": "none",
    }
    return Interpretation.model_validate_json(json.dumps(payload))


def classify_without_model(facts: NotificationFacts, context: dict, *, fallback: bool = False) -> tuple[Interpretation, str]:
    """Owner memory first, then the deterministic path; returns the interpretation and its source."""
    proposal = memory_interpretation(facts, context)
    if proposal is not None:
        return proposal, "memory"
    return deterministic_interpretation(facts, context), "deterministic_fallback" if fallback else "deterministic"


def deterministic_interpretation(facts: NotificationFacts, context: dict) -> Interpretation:
    if facts.status != "candidate" or not facts.direction or not facts.amount:
        raise EvidenceError(facts.error_code or "direction_unproven")
    accounts = context["accounts"]
    source = target = observed = None
    category = None
    merchant = None
    if facts.direction == "internal_movement":
        if facts.institution != "jago":
            raise EvidenceError("incomplete_movement")
        parent = institution_parent(accounts, "jago")
        source = pocket_endpoint(facts.parsed.source_pocket, parent, accounts, institution="jago")
        target = pocket_endpoint(facts.parsed.target_pocket, parent, accounts, institution="jago")
        if str(source["id"]) == str(target["id"]):
            raise EvidenceError("same_movement_endpoint")
        description = f"Pindah saldo ke {target['name']}"
    else:
        observed = observed_endpoint(facts, accounts)
        categories = [row for row in context["categories"] if row["kind"] == facts.direction]
        matches = []
        signature = transfer_signature(facts, context, str(observed["id"]))
        self_transfer = signature["transfer"] and signature["self_identity"]
        if self_transfer:
            matches = [row for row in categories if row["name"] == "Internal Movement"]
        if not matches:
            matches = hinted_categories(categories, facts.parsed.category_hint)
        # No single category: the row is still recorded, uncategorised and flagged (design D1).
        category = matches[0] if len(matches) == 1 else None
        if self_transfer or (context.get("facts") or {}).get("counterparty_is_owner"):
            description = SELF_TRANSFER_NOTES[facts.direction]
        else:
            description = notification_description(facts, context)
            merchant = (context.get("_classification_key") or {}).get("merchant")
    payload = {
        "outcome": "record", "direction": facts.direction, "description": description, "merchant": merchant,
        "account_id": str(observed["id"]) if observed else None,
        "source_account_id": str(source["id"]) if source else None,
        "target_account_id": str(target["id"]) if target else None,
        "category_id": str(category["id"]) if category else None, "kakeibo": None,
        "amount_evidence": facts.amount_quotes[0], "direction_evidence": context.get("notification", facts.text)[:400],
        "source_evidence": "", "target_evidence": "", "candidate_transaction_id": None,
        "confidence": 1.0, "review_reason": "none",
    }
    return Interpretation.model_validate_json(json.dumps(payload), context={"allow_uncategorized": True})




def reconciliation_flag(cur, user_id: str, event_id: str, account_id: str, amount: int) -> None:
    if get_locked_ledger_balance(cur, user_id, account_id) < amount:
        cur.execute(
            """UPDATE accounts SET reconciliation_required = TRUE,
               reconciliation_reason = 'settled_notification_negative_balance',
               reconciliation_event_id = %s, updated_at = NOW() WHERE user_id = %s AND id = %s""",
            (event_id, user_id, account_id),
        )


def account_label(account: dict, accounts: list[dict]) -> str:
    parent = next((row for row in accounts if str(row["id"]) == str(account.get("parent_id"))), None)
    label = f"{parent['name']} · {account['name']}" if parent else account["name"]
    return sanitize_text(label)[:220]


def committed_snapshot(*, key: str, kind: str, description: str, amount: int, source: str | None, target: str | None) -> dict:
    return {"record_key": key, "type": kind, "description": sanitize_text(description)[:160],
            "source": source, "target": target, "amount": amount, "currency": "IDR"}


def save_recorded(cur, event: dict, tx_id: str, snapshot: dict, signature: dict, *, movement_id: str | None = None, provenance: str = "observed_leg", role: str | None = None):
    cur.execute(
        """UPDATE notification_events SET processing_state = 'recorded', transaction_id = %s,
           result_key = %s, result_snapshot = %s, interpretation = %s, movement_id = %s,
           provenance_kind = %s, confirmed_role = %s, error_code = NULL,
           lease_token = NULL, lease_expires_at = NULL, active_question = NULL, updated_at = NOW()
           WHERE user_id = %s AND id = %s""",
        (tx_id, snapshot["record_key"], json.dumps(snapshot), json.dumps(signature), movement_id,
         provenance, role, str(event["user_id"]), str(event["id"])),
    )


def lock_application_categories(cur, user_id: str, expected: dict[str, str]):
    cur.execute(
        """SELECT id, kind FROM categories WHERE user_id = %s AND id = ANY(%s)
           AND is_archived = FALSE ORDER BY id FOR SHARE""",
        (user_id, sorted(expected)),
    )
    current = {str(row["id"]): row["kind"] for row in cur.fetchall()}
    if current != expected:
        raise EvidenceError("invalid_current_category")


def resolve_record(facts: NotificationFacts, proposal: Interpretation, context: dict) -> dict:
    validate_interpretation(proposal, facts, context)
    accounts = context["accounts"]
    if proposal.direction == "internal_movement":
        if facts.institution != "jago":
            raise EvidenceError("incomplete_movement")
        parent = institution_parent(accounts, "jago")
        source = pocket_endpoint(facts.parsed.source_pocket, parent, accounts, institution="jago")
        target = pocket_endpoint(facts.parsed.target_pocket, parent, accounts, institution="jago")
        if str(source["id"]) != str(proposal.source_account_id) or str(target["id"]) != str(proposal.target_account_id):
            raise EvidenceError("conflicting_movement_endpoints")
        if str(source["id"]) == str(target["id"]):
            raise EvidenceError("same_movement_endpoint")
        expense_category, income_category = movement_categories(context)
        return {
            "source": source, "target": target, "observed": None,
            "expense_category": expense_category, "income_category": income_category,
            "category_id": expense_category, "kakeibo": movement_kakeibo(source, target),
        }
    observed = observed_endpoint(facts, accounts, proposal)
    account_id = str(observed["id"])
    if account_id != str(proposal.account_id):
        raise EvidenceError("conflicting_observed_account")
    signature = transfer_signature(facts, context, account_id)
    category_id = str(proposal.category_id) if proposal.category_id else None
    kakeibo = proposal.kakeibo
    description = proposal.description
    if signature["transfer"] and signature["self_identity"]:
        expense_category, income_category = movement_categories(context)
        category_id = expense_category if facts.direction == "expense" else income_category
        kakeibo = None
        description = SELF_TRANSFER_NOTES[facts.direction]
    elif (context.get("facts") or {}).get("counterparty_is_owner"):
        description = SELF_TRANSFER_NOTES[facts.direction]
    return {
        "description": description,
        "observed": observed, "signature": signature, "category_id": category_id,
        "source": observed if facts.direction == "expense" else None,
        "target": observed if facts.direction == "income" else None, "kakeibo": kakeibo,
    }


def apply_interpretation(
    cur, event: dict, facts: NotificationFacts, proposal: Interpretation, context: dict, *, source: str = "ai"
) -> int:
    from app.services.notification_source_pocket import resolved_facts, source_pocket_gate

    if source_pocket_gate(cur, event, facts):
        return 0
    facts = resolved_facts(cur, event, facts)
    validate_interpretation(proposal, facts, context)
    if proposal.outcome != "record":
        state = "ignored" if proposal.outcome == "ignored" else "needs_review"
        cur.execute("UPDATE notification_events SET processing_state = %s, error_code = %s, lease_token = NULL, lease_expires_at = NULL WHERE id = %s", (state, "model_uncertain" if state == "needs_review" else None, event["id"]))
        return 0
    source_name = source
    provenance = notification_provenance(source_name, proposal.confidence)
    review_reasons = []
    if source_name == "ai":
        if proposal.confidence < settings.notification_review_confidence_threshold:
            review_reasons.append("low_confidence")
        if proposal.review_reason != "none":
            review_reasons.append("model_flagged")
    elif source_name == "deterministic_fallback":
        review_reasons.append("model_fallback")
    memory_hit = context.get("_owner_memory_hit") if source_name == "memory" else None
    user_id, event_id = str(event["user_id"]), str(event["id"])
    accounts = context["accounts"]
    key = f"notification:{event_id}"
    resolved = resolve_record(facts, proposal, context)
    if proposal.direction == "internal_movement":
        source, target = resolved["source"], resolved["target"]
        exp_category, inc_category = resolved["expense_category"], resolved["income_category"]
        locked = lock_owned_accounts(cur, user_id, [str(source["id"]), str(target["id"])])
        lock_application_categories(cur, user_id, {exp_category: "expense", inc_category: "income"})
        reconciliation_flag(cur, user_id, event_id, str(source["id"]), facts.amount)
        result = create_bilateral_movement(
            cur, user_id=user_id, source_id=str(source["id"]), target_id=str(target["id"]),
            amount=facts.amount, notes=proposal.description, tx_date=facts.timestamp,
            expense_category_id=exp_category, income_category_id=inc_category,
            source_account=locked[str(source["id"])], target_account=locked[str(target["id"])],
            idempotency_key=key, allow_negative=True, provenance=provenance,
        )
        snapshot = committed_snapshot(key=key, kind="internal_movement", description=proposal.description,
                                      amount=facts.amount, source=account_label(source, accounts), target=account_label(target, accounts))
        signature = {"source_account_id": str(source["id"]), "target_account_id": str(target["id"]),
                     "currency": "IDR", "transfer": True, "source": source_name}
        save_recorded(cur, event, result["expense_transaction_id"], snapshot, signature, movement_id=result["movement_id"], provenance="complete_movement")
        return 2
    return record_observed_leg(
        cur, event, facts, context, observed=resolved["observed"], category_id=resolved["category_id"],
        kakeibo=resolved["kakeibo"], description=resolved["description"],
        signature={**resolved["signature"], "source": source_name}, provenance=provenance,
        merchant=proposal.merchant, review_reasons=review_reasons,
        offset_category_id=memory_hit["offset_category_id"] if memory_hit else None,
        memory_id=memory_hit["memory_id"] if memory_hit else None,
    )


def record_observed_leg(
    cur, event: dict, facts: NotificationFacts, context: dict, *, observed: dict,
    category_id: str | None, kakeibo: str | None, description: str, signature: dict, provenance: Provenance,
    merchant: str | None = None, review_reasons: list[str] | None = None,
    offset_category_id: str | None = None, memory_id: str | None = None,
) -> int:
    """Record one observed leg, linking it to a unique counterpart leg when evidence allows.

    Without a counterpart the leg is recorded alone and flagged needs_review for every doubt about its
    classification; a merchant-less leg also gets a purpose question (design D1, D9).
    """
    from app.services.notification_evidence import requires_source_pocket
    from app.services.notification_source_pocket import resolved_facts, source_pocket_gate

    if source_pocket_gate(cur, event, facts):
        return 0
    facts = resolved_facts(cur, event, facts)
    if requires_source_pocket(facts) and str(observed["id"]) != facts.confirmed_source_account_id:
        raise EvidenceError("conflicting_observed_account")
    user_id, event_id = str(event["user_id"]), str(event["id"])
    accounts = context["accounts"]
    record_key = f"notification:{event_id}"
    account_id = str(observed["id"])
    signature = {**signature, "category_id": category_id, "kakeibo": kakeibo}
    if facts.parsed.rdn_deposit:
        signature["rdn_deposit"] = True
        signature["deposit_source"] = facts.institution
        if confirm_existing_deposit(cur, event, facts, account_id, signature):
            return 0
    candidates = discover_candidates(cur, user_id, facts)
    overflow = len(candidates) > CANDIDATE_LIMIT
    possible_complete = [
        row for row in candidates
        if row["movement_id"] and str(row["account_id"]) == account_id and row["type"] == facts.direction
        and abs((row["post_time"] - facts.timestamp).total_seconds()) < ROLE_CONFIRMATION_WINDOW_SECONDS
    ]
    if possible_complete:
        confirmations = {}
        role = "outbound" if facts.direction == "expense" else "inbound"
        for candidate in possible_complete:
            evidence = candidate.get("evidence") or {}
            remote = evidence.get("target_account_id") if role == "outbound" else evidence.get("source_account_id")
            if (candidate["provenance_kind"] == "complete_movement" and signature["transfer"]
                    and signature["remote_account_id"] == remote and signature["self_identity"]):
                confirmations[str(candidate["movement_id"])] = candidate
        if len(confirmations) != 1 or overflow:
            raise EvidenceError("ambiguous_existing_movement")
        candidate = next(iter(confirmations.values()))
        cur.execute("SELECT 1 FROM notification_events WHERE user_id = %s AND movement_id = %s AND confirmed_role = %s", (user_id, candidate["movement_id"], role))
        if cur.fetchone():
            raise EvidenceError("movement_role_consumed")
        save_recorded(cur, event, str(candidate["id"]), candidate["result_snapshot"], signature,
                      movement_id=str(candidate["movement_id"]), provenance="role_confirmation", role=role)
        return 0
    in_movement_category = category_id in movement_category_ids(context)
    pairing = find_counterpart(cur, user_id, {
        "id": None, "type": facts.direction, "account_id": account_id, "amount": facts.amount,
        "date": facts.timestamp, "evidence": leg_evidence({
            "event_id": event_id, "evidence": signature, "movement_category": in_movement_category,
            "account_id": account_id, "type": facts.direction,
        }, load_accounts(cur, user_id)),
    }, gap_seconds=settings.notification_pairing_window_seconds)
    counterpart = pairing.counterpart
    held, sender_description = sender_gate(
        cur, event, facts, context, observed, signature, proven_movement=counterpart is not None,
    )
    if held:
        return 0
    if sender_description is not None:
        description = sender_description
    if counterpart:
        movement_categories(context)
    key = context.get("_classification_key")
    reasons = [] if counterpart else list(review_reasons or [])
    if not counterpart and category_id is None:
        reasons.append("uncertain_category")
    if not counterpart and pairing.ambiguous:
        reasons.append("ambiguous_movement")
    if not counterpart and in_movement_category:
        reasons.append("awaiting_counterpart")
    self_transfer = bool((signature.get("transfer") and signature.get("self_identity"))
                         or (context.get("facts") or {}).get("counterparty_is_owner"))
    ask_purpose = bool(
        not counterpart and key and key["merchant_less"] and not self_transfer
        and provenance.classified_by != "owner" and not in_movement_category
    )
    if ask_purpose:
        reasons.append("purpose_question")
    if reasons and provenance.review_status == "auto":
        provenance = replace(provenance, review_status="needs_review")
    signature = {**signature, **stored_key_fields(key), "review_reasons": reasons}
    stored_merchant = None if self_transfer else normalize_merchant((merchant or "")[:MERCHANT_MAX_LENGTH])
    locked_ids = [account_id]
    if counterpart:
        cur.execute("SELECT id FROM transactions WHERE user_id = %s AND id = %s FOR UPDATE", (user_id, counterpart["id"]))
        if not cur.fetchone():
            raise EvidenceError("counterpart_changed")
        locked_ids.append(str(counterpart["account_id"]))
    locked = lock_owned_accounts(cur, user_id, locked_ids)
    if any(row["type"] not in LIQUID_ACCOUNT_TYPES or row.get("instrument_type") for row in locked.values()):
        raise EvidenceError("invalid_current_account")
    expected_categories = {category_id: facts.direction} if category_id else {}
    if counterpart:
        exp_category, inc_category = movement_categories(context)
        expected_categories.update({exp_category: "expense", inc_category: "income"})
    lock_application_categories(cur, user_id, expected_categories)
    if facts.direction == "expense":
        reconciliation_flag(cur, user_id, event_id, account_id, facts.amount)
    cur.execute(
        """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, kakeibo_type, idempotency_key,
               merchant, offset_category_id, source, review_status, classified_by, classification_confidence)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
        (user_id, account_id, category_id, facts.direction, facts.amount,
         sanitize_text(description), facts.timestamp, kakeibo if category_id else None, record_key,
         stored_merchant, offset_category_id if facts.direction == "income" else None, *provenance.columns()),
    )
    tx_id = str(cur.fetchone()["id"])
    snapshot = committed_snapshot(key=record_key, kind=facts.direction, description=description,
                                  amount=facts.amount, source=account_label(observed, accounts) if facts.direction == "expense" else None,
                                  target=account_label(observed, accounts) if facts.direction == "income" else None)
    if counterpart:
        expense_id = tx_id if facts.direction == "expense" else str(counterpart["id"])
        income_id = tx_id if facts.direction == "income" else str(counterpart["id"])
        other = next(row for row in accounts if str(row["id"]) == str(counterpart["account_id"]))
        source = observed if facts.direction == "expense" else other
        target = other if facts.direction == "expense" else observed
        note = movement_note(cur, user_id, str(target["id"]))
        # Links equal legs; a known fee on the outbound leg becomes its own expense (link_pair).
        result = link_pair(cur, user_id, expense_id, income_id, fee=pairing.fee, notes=note)
        snapshot = committed_snapshot(key=counterpart["result_key"] or record_key, kind="internal_movement",
                                      description=note, amount=facts.amount - pairing.fee if facts.direction == "expense" else facts.amount,
                                      source=account_label(source, accounts), target=account_label(target, accounts))
        if counterpart.get("event_id"):
            # The counterpart's classification doubts and purpose question ended with the pairing (design D1).
            cur.execute(
                """UPDATE notification_events SET movement_id = %s, confirmed_role = %s, result_snapshot = %s,
                       active_question = CASE WHEN active_question ->> 'type' = 'purpose' THEN NULL
                                              ELSE active_question END
                   WHERE user_id = %s AND id = %s""",
                (result["movement_id"], "inbound" if facts.direction == "expense" else "outbound", json.dumps(snapshot), user_id, counterpart["event_id"]),
            )
        save_recorded(cur, event, tx_id, snapshot, signature, movement_id=result["movement_id"],
                      role="outbound" if facts.direction == "expense" else "inbound")
    else:
        save_recorded(cur, event, tx_id, snapshot, signature)
        if pairing.ambiguous:
            cur.execute("UPDATE notification_events SET error_code = 'ambiguous_movement' WHERE id = %s", (event_id,))
            flag_ambiguous(cur, user_id, list(pairing.ambiguous_ids))
        if ask_purpose:
            from app.services.purpose_questions import attach_purpose_question

            attach_purpose_question(
                cur, event, facts, context, observed=observed, transaction_id=tx_id, key=key,
                current={"merchant": stored_merchant, "notes": sanitize_text(description)[:160],
                         "category_id": category_id, "kakeibo": kakeibo if facts.direction == "expense" else None,
                         "offset_category_id": None},
            )
    if memory_id:
        count_memory_hit(cur, user_id, memory_id)
    if provenance.classified_by == "ai" and provenance.review_status == "auto" and not counterpart:
        learn_from_model(cur, user_id, key, {
            "merchant": stored_merchant, "notes": sanitize_text(description)[:160], "category_id": category_id,
            "kakeibo_type": kakeibo if facts.direction == "expense" else None, "offset_category_id": None,
        })
    return 1


def movement_category_ids(context: dict) -> set[str]:
    return {str(row["id"]) for row in context["categories"] if row["name"] == "Internal Movement"}


# The broker confirms an RDN deposit hours after the bank reports it (observed: 3 h).
DEPOSIT_CONFIRMATION_WINDOW = timedelta(hours=48)


def confirm_existing_deposit(cur, event: dict, facts: NotificationFacts, account_id: str, signature: dict) -> bool:
    """Attach a second report of the same RDN deposit (bank vs broker) to the first one.

    Returns True when the event was recorded as a confirmation and no transaction is needed.
    """
    user_id = str(event["user_id"])
    cur.execute(
        """SELECT n.id AS event_id, n.transaction_id, n.result_snapshot
           FROM notification_events n JOIN transactions t ON t.id = n.transaction_id AND t.user_id = n.user_id
           WHERE n.user_id = %s AND n.processing_state = 'recorded' AND n.id <> %s
             AND n.interpretation ->> 'rdn_deposit' = 'true'
             AND n.interpretation ->> 'deposit_source' <> %s
             AND n.interpretation ->> 'deposit_confirmed_by' IS NULL
             AND t.account_id = %s AND t.type = 'income' AND t.amount = %s
             AND n.post_time BETWEEN %s AND %s
           ORDER BY n.post_time FOR UPDATE OF n""",
        (user_id, str(event["id"]), facts.institution, account_id, facts.amount,
         facts.timestamp - DEPOSIT_CONFIRMATION_WINDOW, facts.timestamp + DEPOSIT_CONFIRMATION_WINDOW),
    )
    matches = cur.fetchall()
    if len(matches) != 1:
        return False
    original = matches[0]
    cur.execute(
        """UPDATE notification_events SET interpretation = interpretation || %s::jsonb
           WHERE user_id = %s AND id = %s""",
        (json.dumps({"deposit_confirmed_by": str(event["id"])}), user_id, original["event_id"]),
    )
    save_recorded(cur, event, str(original["transaction_id"]), original["result_snapshot"],
                  {**signature, "confirms_event": str(original["event_id"])}, provenance="deposit_confirmation")
    return True


def apply_manual_resolution(
    cur, event: dict, facts: NotificationFacts, context: dict, *, direction: str, amount: int,
    account_id: str, category_id: str, description: str, kakeibo: str | None,
) -> int:
    """Record an owner-chosen interpretation of an unresolved event, then run the pairing pass."""
    accounts = context["accounts"]
    observed = next((row for row in accounts if str(row["id"]) == account_id), None)
    if not observed or observed.get("type") not in LIQUID_ACCOUNT_TYPES or observed.get("instrument_type"):
        raise EvidenceError("invalid_account_reference")
    category = next((row for row in context["categories"] if str(row["id"]) == category_id), None)
    if not category or category["kind"] != direction:
        raise EvidenceError("invalid_category_reference")
    resolved_facts = replace(facts, status="candidate", error_code=None, amount=amount, direction=direction)
    try:
        signature = transfer_signature(resolved_facts, context, account_id)
    except EvidenceError:
        signature = {
            "account_id": account_id, "direction": direction, "currency": resolved_facts.currency,
            "transfer": False, "self_identity": False, "remote_account_id": None, "reference_hash": None,
            "root_account_id": root_account_id(accounts, account_id),
            "external_counterparty": bool((context.get("facts") or {}).get("external_counterparty")),
        }
    return record_observed_leg(
        cur, event, resolved_facts, context, observed=observed, category_id=category_id,
        kakeibo=kakeibo if direction == "expense" else None, description=description,
        signature={**signature, "source": "manual"}, provenance=notification_provenance("manual", None),
    )


def apply_broker_trade(cur, event: dict, facts: NotificationFacts, context: dict) -> int:
    parsed = facts.parsed
    if facts.institution != "stockbit" or not parsed.investment_action or not parsed.lots or not parsed.price_per_unit:
        raise EvidenceError("unsupported_broker_event")
    if facts.amount != parsed.price_per_unit:
        raise EvidenceError("conflicting_trade_price")
    quantities = re.findall(r"\b([\d.,]+)\s+lot\b", facts.text, re.I)
    if not quantities or any(parse_idr_amount(value) != parsed.lots for value in quantities):
        raise EvidenceError("conflicting_trade_quantity")
    amount = parsed.lots * 100 * parsed.price_per_unit
    if amount > 9_223_372_036_854_775_807:
        raise EvidenceError("invalid_trade_amount")
    user_id, event_id = str(event["user_id"]), str(event["id"])
    broker = institution_parent(context["accounts"], "stockbit")
    funding_id = broker.get("default_funding_account_id")
    funding = next((row for row in context["accounts"] if str(row["id"]) == str(funding_id)), None)
    if not funding or funding["type"] not in {"bank", "cash", "wallet", "ewallet"} or funding.get("instrument_type"):
        raise EvidenceError("broker_funding_missing")
    cur.execute("SELECT * FROM accounts WHERE user_id = %s AND parent_id = %s AND instrument_symbol = %s AND is_archived = FALSE ORDER BY id FOR UPDATE", (user_id, broker["id"], parsed.instrument_symbol))
    positions = cur.fetchall()
    if len(positions) > 1:
        raise EvidenceError("ambiguous_broker_position")
    units = Decimal(parsed.lots * 100)
    price = parsed.price_per_unit
    if not positions:
        if parsed.investment_action != "buy":
            raise EvidenceError("broker_position_missing")
        cur.execute(
            """INSERT INTO accounts (user_id, parent_id, name, type, initial_balance, instrument_type,
               instrument_symbol, units, avg_buy_price, last_price, last_price_at)
               VALUES (%s, %s, %s, 'investment', 0, 'stock', %s, 0, 0, %s, NOW()) RETURNING *""",
            (user_id, broker["id"], parsed.symbol, parsed.instrument_symbol, price),
        )
        position = cur.fetchone()
    else:
        position = positions[0]
    old_units = Decimal(position.get("units") or 0)
    old_price = Decimal(position.get("avg_buy_price") or 0)
    if parsed.investment_action == "buy":
        updated_units = old_units + units
        average = int(((old_units * old_price + units * price) / updated_units).to_integral_value())
    else:
        updated_units = max(Decimal(0), old_units - units)
        average = int(old_price)
    exp_category, inc_category = movement_categories(context)
    source, target = (funding, position) if parsed.investment_action == "buy" else (position, funding)
    locked = lock_owned_accounts(cur, user_id, [str(source["id"]), str(target["id"])])
    lock_application_categories(cur, user_id, {exp_category: "expense", inc_category: "income"})
    if parsed.investment_action == "buy":
        reconciliation_flag(cur, user_id, event_id, str(funding_id), amount)
    cur.execute("UPDATE accounts SET units = %s, avg_buy_price = %s, last_price = %s, last_price_at = NOW(), updated_at = NOW() WHERE user_id = %s AND id = %s", (updated_units, average, price, user_id, position["id"]))
    key = f"notification:{event_id}"
    description = f"{'Beli' if parsed.investment_action == 'buy' else 'Jual'} {parsed.lots} lot {parsed.symbol}"
    result = create_bilateral_movement(
        cur, user_id=user_id, source_id=str(source["id"]), target_id=str(target["id"]), amount=amount,
        notes=description, tx_date=facts.timestamp, expense_category_id=exp_category, income_category_id=inc_category,
        source_account=locked[str(source["id"])], target_account=locked[str(target["id"])],
        idempotency_key=key, is_trade=True, allow_negative=True,
        provenance=notification_provenance(None, None),
    )
    snapshot = committed_snapshot(key=key, kind="internal_movement", description=description, amount=amount,
                                  source=account_label(source, context["accounts"]), target=account_label(target, context["accounts"]))
    save_recorded(cur, event, result["expense_transaction_id"], snapshot, {"trade": True}, movement_id=result["movement_id"], provenance="broker_trade")
    return 2
