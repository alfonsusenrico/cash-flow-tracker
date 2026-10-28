from __future__ import annotations

import json
import re
from datetime import timedelta
from typing import Any

from app.services.merchant_memory import (
    classification_key, institution_packages, load_memory, merchant_match_key, owner_memory_hit,
)
from app.services.notification_evidence import MONEY_PATTERN, NotificationFacts
from app.services.notification_resolution import (
    alias_key, endpoint_context, is_external_counterparty, names_owner, owner_aliases, redact_owner_names,
)


ACCOUNT_LIMIT = 200
CATEGORY_LIMIT = 200
CANDIDATE_LIMIT = 50
# Upper bound of the serialised model input, shared with the provider.
CONTEXT_BYTE_LIMIT = 60000


def sanitize_text(value: str | None, identity: str | list[str] | None = None) -> str:
    text = (value or "").replace("[SELF]", "[UNTRUSTED_MARKER]")
    money_tokens: list[str] = []

    def preserve_money(match):
        money_tokens.append(match.group(0))
        return f"[MONEY_{len(money_tokens) - 1}]"

    text = MONEY_PATTERN.sub(preserve_money, text)
    aliases = [identity] if isinstance(identity, str) else list(identity or [])
    aliases = [alias.strip() for alias in aliases if alias and len(alias.strip()) >= 3]
    if aliases:
        text = redact_owner_names(text, aliases)
    text = re.sub(r"(?i)\bBearer\s+\S+", "[REDACTED]", text)
    text = re.sub(r"(?i)\b(?:api[_ -]?key|token|password|secret|otp|kode akses)\s*[:=]?\s*\S+", "[REDACTED]", text)
    text = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "[REDACTED]", text)
    text = re.sub(r"(?<!\w)(?:\+?\d[\d -]{5,}\d)(?!\w)", "[REDACTED]", text)
    text = re.sub(r"(?i)\b(?:sk|oc)_[A-Za-z0-9_-]{12,}\b", "[REDACTED]", text)
    for index, token in enumerate(money_tokens):
        text = text.replace(f"[MONEY_{index}]", token)
    return text


def load_context(cur, user_id: str, facts: NotificationFacts, *, history_limit: int = 20) -> dict:
    history_limit = max(0, min(50, history_limit))
    cur.execute("SELECT name, name_aliases FROM users WHERE id = %s", (user_id,))
    profile = cur.fetchone() or {}
    identity = owner_aliases(profile)
    cur.execute(
        """
        SELECT a.id, a.name, a.type, a.parent_id, a.default_pocket_id,
               a.default_funding_account_id, a.instrument_type,
               EXISTS (SELECT 1 FROM goal_accounts ga WHERE ga.account_id = a.id) AS is_savings
        FROM accounts a LEFT JOIN accounts parent ON parent.id = a.parent_id AND parent.user_id = a.user_id
        WHERE a.user_id = %s AND a.is_archived = FALSE
        ORDER BY (a.name ILIKE %s OR parent.name ILIKE %s) DESC, a.id
        LIMIT %s
        """,
        (user_id, f"%{facts.institution or ''}%", f"%{facts.institution or ''}%", ACCOUNT_LIMIT + 1),
    )
    accounts = cur.fetchall()
    cur.execute(
        """SELECT id, name, kind, kakeibo_type FROM categories
           WHERE user_id = %s AND is_archived = FALSE ORDER BY id LIMIT %s""",
        (user_id, CATEGORY_LIMIT + 1),
    )
    categories = cur.fetchall()
    cur.execute(
        """SELECT r.merchant_pattern, r.category_id FROM merchant_category_rules r
           JOIN categories c ON c.id = r.category_id AND c.user_id = r.user_id
           WHERE r.user_id = %s AND c.is_archived = FALSE ORDER BY r.id LIMIT 101""",
        (user_id,),
    )
    rules = cur.fetchall()
    key = classification_key(facts, identity)
    memory = load_memory(cur, user_id, key["match_key"] if key else None)
    history = similar_history(cur, user_id, facts, key, history_limit)
    candidates = discover_candidates(cur, user_id, facts)
    incomplete = (
        len(accounts) > ACCOUNT_LIMIT or len(categories) > CATEGORY_LIMIT
        or len(rules) > 100 or len(candidates) > CANDIDATE_LIMIT
    )
    cur.execute(
        """SELECT al.institution, al.name_normalized, al.name_display, al.source, al.account_id
           FROM notification_account_aliases al
           JOIN accounts a ON a.id = al.account_id AND a.user_id = al.user_id
           WHERE al.user_id = %s AND a.is_archived = FALSE""",
        (user_id,),
    )
    # Earlier name -> account decisions; shown to the model as evidence for similar names.
    learned_names: dict[str, list[dict]] = {}
    for alias in cur.fetchall():
        learned_names.setdefault(str(alias["account_id"]), []).append({
            "key": alias_key(alias["institution"], alias["name_normalized"]),
            "name": sanitize_text(alias["name_display"], identity)[:150],
            "institution": alias["institution"],
            "confirmed_by": alias["source"],
        })
    account_fields = ("id", "name", "type", "parent_id", "default_pocket_id", "default_funding_account_id", "instrument_type", "is_savings")
    sanitized_accounts = [{key: row.get(key) for key in account_fields} for row in accounts[:ACCOUNT_LIMIT]]
    for row in sanitized_accounts:
        row["learned_notification_names"] = learned_names.get(str(row["id"]), [])
    for row in sanitized_accounts:
        row["name"] = sanitize_text(row["name"], identity)[:150]
    sanitized_categories = [
        {"id": row["id"], "name": sanitize_text(row["name"], identity)[:100], "kind": row["kind"],
         "default_kakeibo": row.get("kakeibo_type")}
        for row in categories[:CATEGORY_LIMIT]
    ]
    sanitized_history = [{
        "match": row["match"], "merchant": sanitize_text(row.get("merchant"), identity)[:120] or None,
        "description": sanitize_text(row.get("description"), identity)[:160],
        "category_id": row["category_id"], "kakeibo": row["kakeibo"], "amount": row["amount"],
        "date": row["date"], "owner_confirmed": row["owner_confirmed"],
    } for row in history]
    sanitized_memory = [{
        "match_key": sanitize_text(row["match_key"], identity)[:220],
        "merchant": sanitize_text(row.get("display_merchant"), identity)[:120] or None,
        "description": sanitize_text(row.get("notes"), identity)[:160] or None,
        "category_id": row["category_id"], "kakeibo": row.get("kakeibo_type"), "source": row["source"],
    } for row in memory]
    sanitized_rules = [{"merchant_pattern": sanitize_text(row["merchant_pattern"], identity)[:200], "category_id": row["category_id"]} for row in rules[:100]]
    sanitized_candidates = []
    for candidate in candidates[:CANDIDATE_LIMIT]:
        sanitized_candidates.append({
            key: candidate.get(key) for key in
            ("id", "type", "account_id", "amount", "date", "movement_id", "movement_role", "evidence")
        })
    context = {
        "notification": sanitize_text(facts.text, identity),
        "self_identity_detected": names_owner(facts.text, identity),
        "facts": {"institution": facts.institution, "direction": facts.direction,
                  "amount_quotes": facts.amount_quotes, "timestamp": facts.timestamp.isoformat(),
                  "external_counterparty": is_external_counterparty(
                      facts.parsed.counterparty, facts.parsed.category_hint, identity),
                  "counterparty_is_owner": names_owner(facts.parsed.counterparty, identity),
                  **endpoint_context(facts, sanitized_accounts)},
        "accounts": sanitized_accounts, "categories": sanitized_categories,
        "rules": sanitized_rules, "memory": sanitized_memory, "similar_history": sanitized_history,
        "classification": {
            "match_key": sanitize_text(key["match_key"], identity) if key else None,
            "merchant_less": key["merchant_less"] if key else None,
        },
        "candidates": sanitized_candidates, "incomplete": incomplete,
    }
    fit_context_budget(context)
    # Internal fields stay out of the model input (the provider drops underscore-prefixed keys).
    context["_classification_key"] = key
    context["_owner_memory_hit"] = owner_memory_hit(memory, key, facts.direction)
    return context


def similar_history(cur, user_id: str, facts: NotificationFacts, key: dict | None, limit: int) -> list[dict]:
    """Past rows most like this notification, owner-confirmed first (design D7)."""
    if limit <= 0 or facts.direction not in {"expense", "income"} or not facts.amount:
        return []
    match_key = key["match_key"] if key else None
    merchant_key = merchant_match_key(key.get("merchant")) if key else None
    cur.execute(
        """
        SELECT * FROM (
            SELECT t.id, t.merchant, LEFT(t.notes, 160) AS description, t.category_id, t.amount, t.date,
                   CASE WHEN t.type = 'expense' THEN COALESCE(t.kakeibo_type, c.kakeibo_type) END AS kakeibo,
                   (t.classified_by = 'owner' OR t.review_status = 'reviewed') AS owner_confirmed,
                   (n.interpretation ->> 'match_key' = %(match_key)s
                    OR ('merchant:' || lower(regexp_replace(trim(t.merchant), '[[:space:]]+', ' ', 'g'))) = %(merchant_key)s
                   ) AS same_key,
                   (n.package_name = ANY(%(packages)s) AND t.amount BETWEEN %(low)s AND %(high)s) AS same_band
            FROM transactions t
            JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id AND a.is_archived = FALSE
            LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
            LEFT JOIN LATERAL (
                SELECT interpretation, package_name FROM notification_events
                WHERE user_id = t.user_id AND transaction_id = t.id ORDER BY post_time, id LIMIT 1
            ) n ON TRUE
            WHERE t.user_id = %(user_id)s AND t.type = %(direction)s AND t.movement_id IS NULL
        ) history_rows
        WHERE same_key OR same_band
        ORDER BY same_key DESC NULLS LAST, owner_confirmed DESC NULLS LAST, date DESC, id
        LIMIT %(limit)s
        """,
        {"user_id": user_id, "direction": facts.direction, "match_key": match_key or "",
         "merchant_key": merchant_key or "", "packages": institution_packages(facts.institution),
         "low": facts.amount // 2, "high": facts.amount * 2, "limit": limit},
    )
    return [{**row, "match": "same_key" if row["same_key"] else "same_institution_amount",
             "owner_confirmed": bool(row["owner_confirmed"])} for row in cur.fetchall()]


def context_size(context: dict) -> int:
    public = {key: value for key, value in context.items() if not key.startswith("_")}
    return len(json.dumps(public, default=str, ensure_ascii=False).encode("utf-8"))


def fit_context_budget(context: dict) -> None:
    """Drop the least similar history, then the last memory entries, until the input fits."""
    for field in ("similar_history", "memory"):
        while context[field] and context_size(context) > CONTEXT_BYTE_LIMIT:
            context[field].pop()


def discover_candidates(
    cur, user_id: str, facts: NotificationFacts, *, window_seconds: int | None = None
) -> list[dict[str, Any]]:
    if not facts.amount or facts.direction not in {"expense", "income", "internal_movement"}:
        return []
    if window_seconds is None:
        from app.core.config import settings

        window_seconds = settings.notification_pairing_window_seconds
    window = timedelta(seconds=window_seconds)
    cur.execute(
        """
        SELECT t.id, t.type, t.account_id, t.amount, t.date, t.movement_id, t.movement_role,
               n.id AS event_id, n.post_time, n.interpretation AS evidence,
               n.result_key, n.result_snapshot, n.provenance_kind, n.confirmed_role
        FROM notification_events n
        JOIN transactions t ON t.user_id = n.user_id
          AND (t.id = n.transaction_id OR t.movement_id = COALESCE(n.movement_id,
               (SELECT movement_id FROM transactions WHERE user_id = n.user_id AND id = n.transaction_id)))
        JOIN accounts a ON a.id = t.account_id AND a.user_id = n.user_id
        WHERE n.user_id = %s
          AND ((n.processing_state = 'recorded' AND n.provenance_kind IN ('observed_leg', 'complete_movement', 'role_confirmation'))
               OR (n.provenance_kind IS NULL AND t.movement_id IS NOT NULL))
          AND n.post_time > %s AND n.post_time < %s AND t.amount = %s
          AND a.is_archived = FALSE AND a.type IN ('cash', 'bank', 'wallet', 'ewallet')
          AND a.instrument_type IS NULL AND t.goal_id IS NULL AND t.obligation_id IS NULL
          AND t.recurring_rule_id IS NULL
          AND NOT EXISTS (SELECT 1 FROM transaction_obligation_allocations alloc WHERE alloc.transaction_id = t.id)
        ORDER BY n.post_time, n.id, t.id LIMIT %s
        """,
        (user_id, facts.timestamp - window, facts.timestamp + window, facts.amount, CANDIDATE_LIMIT + 1),
    )
    return cur.fetchall()
