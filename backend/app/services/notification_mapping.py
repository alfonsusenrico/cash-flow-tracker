"""AI-proposed account mapping for bank-notification names the backend cannot resolve."""

from __future__ import annotations

import re

from psycopg.types.json import Jsonb

from app.services.notification_evidence import EvidenceError


ROLE_FIELDS = {"observed": "account_id", "source": "source_account_id", "target": "target_account_id"}
# Companion builds from this version understand the needs_confirmation state and its proposal.
CONFIRMATION_MIN_VERSION = (1, 3, 0)


def supports_confirmation(event: dict) -> bool:
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", str(event.get("source_version") or ""))
    return bool(match) and tuple(int(part) for part in match.groups()) >= CONFIRMATION_MIN_VERSION


def mapping_decision(proposal, context: dict) -> dict | None:
    """Validate the model's mappings for the context's unresolved names (design D2).

    Returns None when nothing is unresolved. Raises EvidenceError when a required mapping is
    missing or violates eligibility, so the event falls back to review.
    """
    facts = context.get("facts") or {}
    unresolved = facts.get("unresolved_names") or []
    if not unresolved:
        return None
    reason = facts.get("mapping_error") or "uncertain_mapping"
    if proposal is None or proposal.outcome != "record":
        raise EvidenceError(reason)
    by_role = {mapping.role: mapping for mapping in proposal.mappings}
    entries = []
    for name in unresolved:
        mapping = by_role.get(name["role"])
        if mapping is None:
            raise EvidenceError(reason)
        chosen = str(mapping.account_id)
        eligible = set(name["eligible_account_ids"])
        if chosen not in eligible or str(getattr(proposal, ROLE_FIELDS[name["role"]])) != chosen:
            raise EvidenceError("invalid_mapping_proposal")
        alternatives = []
        for account_id in map(str, mapping.alternatives):
            if account_id in eligible and account_id != chosen and account_id not in alternatives:
                alternatives.append(account_id)
        entries.append({
            "role": name["role"], "name": name["name"], "institution": name["institution"],
            "name_normalized": name["name_normalized"], "account_id": chosen,
            "confidence": round(min(1.0, max(0.0, float(mapping.confidence))), 4),
            "alternatives": alternatives[:3],
        })
    return {"entries": entries, "confidence": min(entry["confidence"] for entry in entries)}


def store_aliases(cur, user_id: str, entries: list[dict], source: str) -> None:
    """Remember name -> account pairs; an owner choice is never replaced by an AI one."""
    for entry in entries:
        cur.execute(
            """INSERT INTO notification_account_aliases
                   (user_id, institution, name_normalized, name_display, account_id, source)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (user_id, institution, name_normalized) DO UPDATE
               SET account_id = EXCLUDED.account_id, source = EXCLUDED.source,
                   name_display = EXCLUDED.name_display, updated_at = NOW()
               WHERE notification_account_aliases.source = 'ai' OR EXCLUDED.source = 'owner'""",
            (user_id, entry["institution"], entry["name_normalized"], entry["name"][:150], entry["account_id"], source),
        )


def account_labels(cur, user_id: str, account_ids: list[str]) -> dict[str, str]:
    if not account_ids:
        return {}
    cur.execute(
        """SELECT a.id, a.name, p.name AS parent_name FROM accounts a
           LEFT JOIN accounts p ON p.id = a.parent_id AND p.user_id = a.user_id
           WHERE a.user_id = %s AND a.id = ANY(%s)""",
        (user_id, account_ids),
    )
    return {
        str(row["id"]): f"{row['parent_name']} · {row['name']}" if row.get("parent_name") else row["name"]
        for row in cur.fetchall()
    }


def mark_automatic_mapping(cur, event: dict, decision: dict) -> None:
    labels = account_labels(cur, str(event["user_id"]), [entry["account_id"] for entry in decision["entries"]])
    mapped = [{"name": entry["name"], "account": labels.get(entry["account_id"]), "mode": "auto",
               "confidence": entry["confidence"]} for entry in decision["entries"]]
    cur.execute(
        """UPDATE notification_events
           SET interpretation = COALESCE(interpretation, '{}'::jsonb) || %s,
               result_snapshot = COALESCE(result_snapshot, '{}'::jsonb) || %s
           WHERE user_id = %s AND id = %s""",
        (Jsonb({"mapping": {"mode": "auto", "entries": decision["entries"]}}), Jsonb({"mapped": mapped}),
         str(event["user_id"]), str(event["id"])),
    )


def request_confirmation(cur, event: dict, decision: dict) -> None:
    """Park the event with its least certain mapping for the owner to confirm or correct."""
    entry = min(decision["entries"], key=lambda item: item["confidence"])
    cur.execute(
        """UPDATE notification_events SET processing_state = 'needs_confirmation',
               error_code = 'mapping_confirmation_required', interpretation = %s,
               lease_token = NULL, lease_expires_at = NULL, updated_at = NOW()
           WHERE user_id = %s AND id = %s""",
        (Jsonb({"mapping_proposal": entry}), str(event["user_id"]), str(event["id"])),
    )


def proposal_view(cur, event: dict) -> dict | None:
    proposal = (event.get("interpretation") or {}).get("mapping_proposal")
    if not proposal:
        return None
    ids = [proposal["account_id"], *proposal.get("alternatives", [])]
    labels = account_labels(cur, str(event["user_id"]), ids)
    return {
        "name": proposal["name"], "role": proposal["role"], "confidence": proposal["confidence"],
        "proposed": {"id": proposal["account_id"], "label": labels.get(proposal["account_id"])},
        "alternatives": [{"id": account_id, "label": labels[account_id]}
                         for account_id in proposal.get("alternatives", []) if account_id in labels],
    }
