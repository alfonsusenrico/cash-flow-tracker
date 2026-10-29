"""What a typed capture still lacks: facts, memory, model context, account and category resolution, and
the one question to ask next (design D1–D4).
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import HTTPException

from app.core.config import settings
from app.services.capture_normalization import CAPTURE_CONTEXT_BYTE_LIMIT
from app.services.capture_parsing import CaptureDateError, parse_capture
from app.services.ledger_mutations import LIQUID_ACCOUNT_TYPES
from app.services.merchant_memory import load_memory, merchant_match_key, owner_identity, owner_memory_hit
from app.services.notification_context import sanitize_text
from app.services.reimbursements import NON_REPORTABLE_CATEGORY_NAMES


ACCOUNT_OPTION_LIMIT = 6
CATEGORY_OPTION_LIMIT = 5
HISTORY_WINDOW = 5
HISTORY_AGREEMENT = 3
MODEL_ACCOUNT_THRESHOLD = 0.85
SIMILAR_HISTORY_LIMIT = 10
SIMILAR_WORD_LIMIT = 5
SIMILAR_WORD_MIN_LENGTH = 3
CATEGORY_USAGE_DAYS = 90
KINDS = ("expense", "income", "movement")
KIND_OPTIONS = (
    {"id": "expense", "label": "Pengeluaran"},
    {"id": "income", "label": "Pemasukan"},
    {"id": "movement", "label": "Pindah saldo"},
)
# Accounts each kind needs: an expense leaves a source, income arrives in a target, a movement needs both.
KIND_ROLES = {"expense": ("source",), "income": ("target",), "movement": ("source", "target")}
AMOUNT_PROMPTS = {
    "missing": "Berapa nominalnya?",
    "invalid": "Nominalnya kurang jelas. Berapa nominalnya?",
    "several": "Ada beberapa nominal. Kirim satu transaksi per pesan, atau tulis nominal untuk transaksi ini.",
}
ACCOUNT_PROMPTS = {
    ("expense", "source"): "Dari rekening mana?",
    ("income", "target"): "Masuk ke rekening mana?",
    ("movement", "source"): "Dari rekening mana?",
    ("movement", "target"): "Ke rekening mana?",
}


def processing_settings():
    # The processor's settings object, so the AI switch and thresholds have one source in tests too.
    from app.services.notification_processing import settings as current

    return current


# --- Owner data ---------------------------------------------------------------------------------


def owner_accounts(cur, user_id: str) -> list[dict]:
    cur.execute(
        """SELECT id, name, type, parent_id, default_pocket_id, instrument_type
           FROM accounts WHERE user_id = %s AND is_archived = FALSE ORDER BY name, id""",
        (user_id,),
    )
    return [{**row, "id": str(row["id"]), "parent_id": str(row["parent_id"]) if row["parent_id"] else None,
             "default_pocket_id": str(row["default_pocket_id"]) if row["default_pocket_id"] else None}
            for row in cur.fetchall()]


def owner_account_aliases(cur, user_id: str) -> list[dict]:
    cur.execute(
        """SELECT al.name_display AS name, al.account_id FROM notification_account_aliases al
           JOIN accounts a ON a.id = al.account_id AND a.user_id = al.user_id AND a.is_archived = FALSE
           WHERE al.user_id = %s ORDER BY al.name_display, al.id""",
        (user_id,),
    )
    return [{"name": row["name"], "account_id": str(row["account_id"])} for row in cur.fetchall()]


def liquid_leaves(accounts: list[dict]) -> dict[str, dict]:
    """Accounts that can hold a capture: liquid, not an instrument, and without active pockets."""
    parents = {row["parent_id"] for row in accounts if row["parent_id"]}
    return {row["id"]: row for row in accounts
            if row["type"] in LIQUID_ACCOUNT_TYPES and not row["instrument_type"] and row["id"] not in parents}


def account_label(account: dict, accounts: list[dict]) -> str:
    parent = next((row for row in accounts if row["id"] == account.get("parent_id")), None)
    return f"{parent['name']} · {account['name']}" if parent else account["name"]


def reportable_categories(cur, user_id: str, kind: str | None = None) -> dict[str, dict]:
    cur.execute(
        """SELECT id, name, kind, kakeibo_type, offsets_spending FROM categories
           WHERE user_id = %s AND is_archived = FALSE AND name <> ALL(%s) AND (%s::text IS NULL OR kind = %s)
           ORDER BY kind, name, id""",
        (user_id, list(NON_REPORTABLE_CATEGORY_NAMES), kind, kind),
    )
    return {str(row["id"]): row for row in cur.fetchall()}


def default_pillar(kind: str, category: dict | None) -> str | None:
    if kind != "expense":
        return None
    return (category or {}).get("kakeibo_type") or "need"


# --- Facts, memory and model context --------------------------------------------------------------


def capture_facts(cur, user_id: str, request: dict) -> tuple[dict, list[dict], list[dict]]:
    accounts = owner_accounts(cur, user_id)
    aliases = owner_account_aliases(cur, user_id)
    try:
        facts = parse_capture(request["text"], request["device_time"], accounts=accounts, aliases=aliases,
                              tz=settings.tz)
    except CaptureDateError:
        raise HTTPException(status_code=422, detail={"code": "invalid_capture_date"}) from None
    return facts.as_dict(), accounts, aliases


def capture_key(facts: dict) -> str | None:
    return merchant_match_key(facts["phrase"]) if facts["phrase"] else None


def phrase_words(facts: dict) -> list[str]:
    words = [word for word in facts["phrase"].split() if len(word) >= SIMILAR_WORD_MIN_LENGTH and not word.isdigit()]
    return list(dict.fromkeys(words))[:SIMILAR_WORD_LIMIT]


def memory_state(cur, user_id: str, facts: dict) -> dict:
    """Owner memory for the capture key: the hit that skips the model, the remembered account, close entries."""
    key = capture_key(facts)
    kind = facts["kind"] or facts["kind_guess"]
    entries = load_memory(cur, user_id, key) if key else []
    exact = entries[0] if entries and entries[0]["match_key"] == key else None
    hit = None
    if key and kind in {"expense", "income"}:
        hit = owner_memory_hit(entries, {"match_key": key, "key_kind": "merchant"}, kind)
        if hit:
            hit["kind"] = kind
    words = set(phrase_words(facts))
    close = [{"match_key": row["match_key"], "merchant": row["display_merchant"], "description": row["notes"],
              "category_id": str(row["category_id"]), "kind": row["category_kind"], "kakeibo": row["kakeibo_type"],
              "source": row["source"]}
             for row in entries
             if row["match_key"] == key or words & set(row["match_key"].removeprefix("merchant:").split())]
    return {
        "hit": hit, "close": close[:10],
        "account_id": str(exact["account_id"]) if exact and exact["account_id"] else None,
    }


def similar_rows(cur, user_id: str, facts: dict, kind: str | None = None) -> list[dict]:
    """Past expense and income rows sharing a word with the capture, owner-confirmed first."""
    words = phrase_words(facts)
    if not words:
        return []
    patterns = [f"%{word}%" for word in words]
    cur.execute(
        """SELECT t.merchant, LEFT(t.notes, 160) AS description, t.category_id, t.type, t.amount, t.account_id,
                  CASE WHEN t.type = 'expense' THEN COALESCE(t.kakeibo_type, c.kakeibo_type) END AS kakeibo,
                  (t.classified_by = 'owner' OR t.review_status = 'reviewed') AS owner_confirmed
           FROM transactions t
           JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id AND c.is_archived = FALSE
           WHERE t.user_id = %s AND t.movement_id IS NULL AND t.type IN ('expense', 'income')
             AND (%s::text IS NULL OR t.type = %s) AND c.name <> ALL(%s)
             AND (t.notes ILIKE ANY(%s) OR t.merchant ILIKE ANY(%s))
           ORDER BY owner_confirmed DESC, t.date DESC, t.id
           LIMIT %s""",
        (user_id, kind, kind, list(NON_REPORTABLE_CATEGORY_NAMES), patterns, patterns, SIMILAR_HISTORY_LIMIT),
    )
    return cur.fetchall()


def context_bytes(context: dict) -> int:
    return len(json.dumps(context, default=str, ensure_ascii=False).encode("utf-8"))


def model_context(cur, user_id: str, request: dict, facts: dict, memory: dict, accounts: list[dict],
                  aliases: list[dict]) -> dict | None:
    """Sanitised model input within the byte budget; None when even the minimum does not fit."""
    identity = owner_identity(cur, user_id)
    leaves = liquid_leaves(accounts)
    names = {}
    for alias in aliases:
        names.setdefault(alias["account_id"], []).append(sanitize_text(alias["name"], identity)[:100])
    categories = reportable_categories(cur, user_id).values()
    mentioned = [account_label(leaves[item], accounts) for mention in facts["mentions"]
                 for item in mention["candidates"] if item in leaves]
    context = {
        "text": sanitize_text(request["text"], identity),
        "facts": {"kind": facts["kind"], "kind_guess": facts["kind_guess"], "amount": facts["amount"],
                  "local_date": facts["date"][:10],
                  "accounts": [sanitize_text(label, identity)[:150] for label in mentioned]},
        "categories": [{"id": str(row["id"]), "name": sanitize_text(row["name"], identity)[:100], "kind": row["kind"],
                        "default_kakeibo": row["kakeibo_type"]} for row in categories],
        "accounts": [{"id": account_id, "label": sanitize_text(account_label(row, accounts), identity)[:150],
                      "type": row["type"], "aliases": names.get(account_id, [])[:5]}
                     for account_id, row in leaves.items()],
        "memory": [{"merchant": sanitize_text(item["merchant"], identity)[:120] or None,
                    "description": sanitize_text(item["description"], identity)[:160] or None,
                    "category_id": item["category_id"], "kakeibo": item["kakeibo"], "source": item["source"]}
                   for item in memory["close"]],
        "similar_history": [{"merchant": sanitize_text(row["merchant"], identity)[:120] or None,
                             "description": sanitize_text(row["description"], identity)[:160],
                             "category_id": str(row["category_id"]), "type": row["type"], "kakeibo": row["kakeibo"],
                             "account_id": str(row["account_id"]), "owner_confirmed": bool(row["owner_confirmed"])}
                            for row in similar_rows(cur, user_id, facts)],
    }
    for field in ("similar_history", "memory"):
        while context[field] and context_bytes(context) > CAPTURE_CONTEXT_BYTE_LIMIT:
            context[field].pop()
    return context if context_bytes(context) <= CAPTURE_CONTEXT_BYTE_LIMIT else None


def model_unavailable(provider) -> str | None:
    from app.services.openai_notification_provider import configuration_error

    current = processing_settings()
    if provider is None:
        return "ai_disabled"
    return configuration_error(current) or current.capture_configuration_error


def needs_model(facts: dict, memory: dict) -> bool:
    kind = facts["kind"] or facts["kind_guess"]
    return kind in {"expense", "income"} and bool(facts["phrase"]) and memory["hit"] is None


# --- Resolution and questions ----------------------------------------------------------------------


def resolved_amount(draft: dict) -> int | None:
    return draft["owner"].get("amount") or draft["facts"]["amount"]


def resolved_kind(draft: dict) -> str | None:
    """The kind is certain from the word table or the owner; the model can only flag it (D2, D4)."""
    if draft["owner"].get("kind"):
        return draft["owner"]["kind"]
    kind = draft["facts"]["kind"]
    model = draft.get("model")
    if kind in {"expense", "income"} and model and model["type"] != kind:
        return None
    return kind


def account_history(cur, user_id: str, draft: dict, kind: str, role: str, other_id: str | None) -> list[str]:
    """Accounts of the latest rows with the same key, or of movements sharing the known endpoint (D3)."""
    if kind == "movement":
        if not other_id:
            return []
        own_role, other_role = ("outbound", "inbound") if role == "source" else ("inbound", "outbound")
        cur.execute(
            """SELECT mine.account_id FROM transactions mine
               JOIN transactions other ON other.user_id = mine.user_id AND other.movement_id = mine.movement_id
                    AND other.movement_role = %s
               WHERE mine.user_id = %s AND mine.movement_role = %s AND other.account_id = %s
               ORDER BY mine.date DESC, mine.id DESC LIMIT %s""",
            (other_role, user_id, own_role, other_id, HISTORY_WINDOW),
        )
        return [str(row["account_id"]) for row in cur.fetchall()]
    key = capture_key(draft["facts"])
    if not key:
        return []
    cur.execute(
        """SELECT t.account_id FROM transactions t
           WHERE t.user_id = %(user_id)s AND t.type = %(kind)s AND t.movement_id IS NULL
             AND (t.id IN (SELECT transaction_id FROM text_captures
                           WHERE user_id = %(user_id)s AND match_key = %(key)s AND status = 'recorded'
                             AND transaction_id IS NOT NULL)
                  OR ('merchant:' || lower(regexp_replace(btrim(t.merchant), '[[:space:]]+', ' ', 'g'))) = %(key)s)
           ORDER BY t.date DESC, t.id DESC LIMIT %(limit)s""",
        {"user_id": user_id, "kind": kind, "key": key, "limit": HISTORY_WINDOW},
    )
    return [str(row["account_id"]) for row in cur.fetchall()]


def account_for(cur, user_id: str, draft: dict, kind: str, role: str, leaves: dict, other_id: str | None,
                use_history: bool) -> tuple[str | None, str | None, list[str]]:
    """(account, how it was resolved, accounts to offer first) for one role, in the D3 order."""
    answered = draft["owner"].get("accounts", {}).get(role)
    if answered:
        return answered, "owner", []
    mentions = [item for item in draft["facts"]["mentions"] if kind != "movement" or item["role"] == role]
    candidates = sorted({item for mention in mentions for item in mention["candidates"]
                         if item in leaves and item != other_id})
    if mentions:
        # An explicit mention is never overridden by memory or history; more than one match is asked.
        if len(candidates) == 1 and all(set(mention["candidates"]) & set(candidates) for mention in mentions):
            return candidates[0], "mention", []
        return None, None, candidates
    if not use_history:
        return None, None, []
    if kind != "movement":
        remembered = draft["memory"].get("account_id")
        if remembered in leaves:
            return remembered, "memory", []
    history = [item for item in account_history(cur, user_id, draft, kind, role, other_id)
               if item in leaves and item != other_id]
    if history:
        account_id, count = Counter(history).most_common(1)[0]
        if count >= HISTORY_AGREEMENT:
            return account_id, "history", []
    model = draft.get("model")
    if (kind != "movement" and model and model["account_id"] in history
            and model["account_confidence"] >= MODEL_ACCOUNT_THRESHOLD):
        return model["account_id"], "model", []
    return None, None, []


def resolve_accounts(cur, user_id: str, draft: dict, kind: str, leaves: dict) -> tuple[dict, str | None, list]:
    """Resolved accounts by role, the first unresolved role and its preferred options."""
    roles = KIND_ROLES[kind]
    resolved: dict[str, str] = {}
    preferred: dict[str, list[str]] = {}
    for role in roles:
        account_id, _, first = account_for(cur, user_id, draft, kind, role, leaves, None, use_history=False)
        if account_id:
            resolved[role] = account_id
        preferred[role] = first
    for role in roles:
        if role in resolved:
            continue
        other = next((resolved[item] for item in roles if item != role and item in resolved), None)
        account_id, _, _ = account_for(cur, user_id, draft, kind, role, leaves, other, use_history=True)
        if account_id:
            resolved[role] = account_id
    if kind == "movement" and resolved.get("source") and resolved.get("source") == resolved.get("target"):
        resolved.pop("target")
    missing = next((role for role in roles if role not in resolved), None)
    return resolved, missing, preferred.get(missing, []) if missing else []


def classification_for(cur, user_id: str, draft: dict, kind: str) -> dict | None:
    """Category and pillar from the owner's answer, owner memory, or a confident model (D4)."""
    categories = reportable_categories(cur, user_id, kind)
    answered = draft["owner"].get("category")
    if answered and answered["category_id"] in categories:
        return {**answered, "by": "owner"}
    hit = draft["memory"].get("hit")
    if hit and hit["kind"] == kind and hit["category_id"] in categories:
        return {"category_id": hit["category_id"], "kakeibo": hit["kakeibo"],
                "offset_category_id": hit["offset_category_id"], "merchant": hit["merchant"],
                "description": hit["notes"], "by": "memory"}
    model = draft.get("model")
    threshold = processing_settings().capture_confidence_threshold
    if (model and model["category_id"] in categories and model["category_kind"] == kind
            and model["category_confidence"] >= threshold):
        return {"category_id": model["category_id"], "kakeibo": model["kakeibo"], "offset_category_id": None,
                "merchant": model["merchant"], "description": model["description"], "by": "model",
                "confidence": model["category_confidence"]}
    return None


def account_options(cur, user_id: str, leaves: dict, accounts: list[dict], preferred: list[str],
                    exclude: str | None) -> list[dict]:
    """Up to six liquid accounts, the mentioned ones first, then the most recently used."""
    cur.execute(
        """SELECT account_id, MAX(date) AS last_used FROM transactions
           WHERE user_id = %s AND account_id = ANY(%s::uuid[]) GROUP BY account_id""",
        (user_id, list(leaves)),
    )
    last_used = {str(row["account_id"]): row["last_used"] for row in cur.fetchall()}
    floor = datetime.min.replace(tzinfo=timezone.utc)
    ranked = sorted((item for item in leaves if item not in preferred),
                    key=lambda item: (-(last_used.get(item) or floor).timestamp(), leaves[item]["name"].casefold()))
    ordered = [item for item in [*preferred, *ranked] if item != exclude][:ACCOUNT_OPTION_LIMIT]
    return [{"id": item, "label": account_label(leaves[item], accounts), "type": leaves[item]["type"]}
            for item in ordered]


def category_options(cur, user_id: str, draft: dict, kind: str) -> list[dict]:
    """Up to five ranked categories with their pillar: model, memory, similar rows, recent use, then by name."""
    categories = reportable_categories(cur, user_id, kind)
    ranked: list[tuple[str, str | None]] = []
    model = draft.get("model")
    if model and model["category_kind"] == kind and model["category_id"]:
        ranked.append((model["category_id"], model["kakeibo"]))
    ranked.extend((item["category_id"], item["kakeibo"]) for item in draft["memory"]["close"] if item["kind"] == kind)
    ranked.extend((str(row["category_id"]), row["kakeibo"]) for row in similar_rows(cur, user_id, draft["facts"], kind))
    cur.execute(
        """SELECT category_id, COUNT(*) AS uses FROM transactions
           WHERE user_id = %s AND type = %s AND movement_id IS NULL AND category_id IS NOT NULL
             AND date > NOW() - make_interval(days => %s)
           GROUP BY category_id ORDER BY uses DESC, category_id LIMIT 20""",
        (user_id, kind, CATEGORY_USAGE_DAYS),
    )
    ranked.extend((str(row["category_id"]), None) for row in cur.fetchall())
    ranked.extend((category_id, None) for category_id in sorted(categories, key=lambda item: categories[item]["name"]))
    options, seen = [], set()
    for category_id, pillar in ranked:
        if category_id not in categories or category_id in seen:
            continue
        seen.add(category_id)
        category = categories[category_id]
        options.append({
            "id": f"c{len(options) + 1}", "category_id": category_id, "label": category["name"],
            "kakeibo": (pillar or default_pillar(kind, category)) if kind == "expense" else None,
        })
        if len(options) == CATEGORY_OPTION_LIMIT:
            break
    return options


def question(kind: str, prompt: str, *, role: str | None = None, reason: str | None = None,
             options: list[dict] | None = None, allows_text: bool = False, allows_other: bool = False) -> dict:
    return {"question_id": str(uuid4()), "type": kind, "role": role, "reason": reason, "prompt": prompt,
            "options": options or [], "allows_text": allows_text, "allows_other": allows_other}


def evaluate(cur, user_id: str, draft: dict) -> tuple[dict | None, dict | None]:
    """(question, plan): the first missing field asked in the order amount, kind, account, category (D4)."""
    facts = draft["facts"]
    amount = resolved_amount(draft)
    if amount is None:
        reason = facts["amount_issue"] or "missing"
        return question("amount", AMOUNT_PROMPTS[reason], reason=reason, allows_text=True), None
    kind = resolved_kind(draft)
    if kind is None:
        return question("kind", "Ini pengeluaran, pemasukan, atau pindah saldo?", options=list(KIND_OPTIONS)), None
    accounts = owner_accounts(cur, user_id)
    leaves = liquid_leaves(accounts)
    resolved, missing, preferred = resolve_accounts(cur, user_id, draft, kind, leaves)
    if missing:
        exclude = next((resolved[role] for role in KIND_ROLES[kind] if role != missing and role in resolved), None)
        options = account_options(cur, user_id, leaves, accounts, preferred, exclude)
        return question("account", ACCOUNT_PROMPTS[(kind, missing)], role=missing, options=options,
                        allows_other=True), None
    classification = None
    if kind != "movement":
        classification = classification_for(cur, user_id, draft, kind)
        if classification is None:
            return question("category", "Kategorinya apa?", options=category_options(cur, user_id, draft, kind),
                            allows_other=True), None
    return None, {"kind": kind, "amount": amount, "accounts": resolved, "classification": classification}
