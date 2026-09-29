from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from app.services.notification_evidence import MONEY_PATTERN, NotificationFacts
from app.services.notification_resolution import endpoint_context


ACCOUNT_LIMIT = 200
CATEGORY_LIMIT = 200
CANDIDATE_LIMIT = 50


def sanitize_text(value: str | None, identity: str | None = None) -> str:
    text = (value or "").replace("[SELF]", "[UNTRUSTED_MARKER]")
    money_tokens: list[str] = []

    def preserve_money(match):
        money_tokens.append(match.group(0))
        return f"[MONEY_{len(money_tokens) - 1}]"

    text = MONEY_PATTERN.sub(preserve_money, text)
    if identity and len(identity.strip()) >= 3:
        text = re.sub(re.escape(identity.strip()), "[SELF]", text, flags=re.I)
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
    cur.execute("SELECT name FROM users WHERE id = %s", (user_id,))
    profile = cur.fetchone() or {}
    identity = profile.get("name")
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
        """SELECT id, name, kind FROM categories
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
    cur.execute(
        """SELECT t.id, t.type, t.account_id, t.category_id, t.amount, t.date, t.kakeibo_type,
                  LEFT(t.notes, 160) AS description
           FROM transactions t JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
           WHERE t.user_id = %s AND a.is_archived = FALSE
           ORDER BY t.date DESC, t.id LIMIT %s""",
        (user_id, history_limit),
    )
    history = cur.fetchall()
    candidates = discover_candidates(cur, user_id, facts)
    incomplete = (
        len(accounts) > ACCOUNT_LIMIT or len(categories) > CATEGORY_LIMIT
        or len(rules) > 100 or len(candidates) > CANDIDATE_LIMIT
    )
    account_fields = ("id", "name", "type", "parent_id", "default_pocket_id", "default_funding_account_id", "instrument_type", "is_savings")
    sanitized_accounts = [{key: row.get(key) for key in account_fields} for row in accounts[:ACCOUNT_LIMIT]]
    for row in sanitized_accounts:
        row["name"] = sanitize_text(row["name"], identity)[:150]
    sanitized_categories = [{"id": row["id"], "name": sanitize_text(row["name"], identity)[:100], "kind": row["kind"]} for row in categories[:CATEGORY_LIMIT]]
    for row in history:
        row["description"] = sanitize_text(row.get("description"), identity)
    sanitized_rules = [{"merchant_pattern": sanitize_text(row["merchant_pattern"], identity)[:200], "category_id": row["category_id"]} for row in rules[:100]]
    sanitized_candidates = []
    for candidate in candidates[:CANDIDATE_LIMIT]:
        sanitized_candidates.append({
            key: candidate.get(key) for key in
            ("id", "type", "account_id", "amount", "date", "movement_id", "movement_role", "evidence")
        })
    return {
        "notification": sanitize_text(facts.text, identity),
        "self_identity_detected": bool(identity and len(identity.strip()) >= 3 and re.search(re.escape(identity.strip()), facts.text, re.I)),
        "facts": {"institution": facts.institution, "direction": facts.direction,
                  "amount_quotes": facts.amount_quotes, "timestamp": facts.timestamp.isoformat(),
                  **endpoint_context(facts, sanitized_accounts)},
        "accounts": sanitized_accounts, "categories": sanitized_categories,
        "rules": sanitized_rules, "history": history, "candidates": sanitized_candidates,
        "incomplete": incomplete,
    }


def discover_candidates(cur, user_id: str, facts: NotificationFacts) -> list[dict[str, Any]]:
    if not facts.amount or facts.direction not in {"expense", "income", "internal_movement"}:
        return []
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
        (user_id, facts.timestamp - timedelta(seconds=30), facts.timestamp + timedelta(seconds=30), facts.amount, CANDIDATE_LIMIT + 1),
    )
    return cur.fetchall()
