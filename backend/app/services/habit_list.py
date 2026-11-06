"""The owner's learned habits and top-up patterns as a read-only list (redesign-money-and-settings D7).

Derived from the same live owner labels, filters, window and weights as the habit decision and the top-up
learner; nothing is stored. A notification text signature never leaves this module.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import defaultdict
from typing import Any

from app.core.config import settings
from app.services.habits import EVIDENCE_KINDS, MIN_AGREEMENT, MIN_SUPPORT, Label, decide, trusted_label_sql, within_band
from app.services.merchant_memory import live_feedback_sql
from app.services.notification_resolution import INSTITUTION_LABELS
from app.services.system_categories import reportable_key_sql
from app.services.topup_learning import label_rows, labels_from_rows, product_stats

logger = logging.getLogger(__name__)

# Rows read per list; the newest labels decide, so older ones beyond this never change a state.
ROW_LIMIT = 2000
ENTRY_LIMIT = 100


def _key(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


def _state(support: int, ready: bool) -> str:
    if support < MIN_SUPPORT:
        return "learning"
    return "ready" if ready else "conflicting"


def _habit_rows(cur, user_id: str) -> list[dict]:
    """The latest live owner label of every keyed transaction, newest first (one statement)."""
    cur.execute(
        f"""
        WITH latest AS (
            SELECT DISTINCT ON (f.transaction_id) f.*
            FROM classification_feedback f
            WHERE f.user_id = %(user_id)s AND f.transaction_id IS NOT NULL AND {live_feedback_sql("f")}
            ORDER BY f.transaction_id, f.created_at DESC, f.id DESC
        )
        SELECT f.transaction_id, f.kind, f.account_id, f.institution, f.text_signature, f.after_category_id,
               c.name AS category_name, f.after_kakeibo_type, f.after_notes, f.after_merchant, f.amount,
               f.local_weekday, f.local_hour, f.created_at, a.name AS account_name
        FROM latest f
        JOIN categories c ON c.id = f.after_category_id AND c.user_id = f.user_id
             AND c.kind = 'expense' AND c.is_archived = FALSE AND {reportable_key_sql("c")}
        LEFT JOIN accounts a ON a.id = f.account_id AND a.user_id = f.user_id
        LEFT JOIN notification_events answered
             ON f.kind = 'purpose_answer' AND answered.user_id = f.user_id AND answered.id = f.event_id
        WHERE f.kind = ANY(%(kinds)s) AND f.after_type = 'expense' AND f.account_id IS NOT NULL
          AND f.institution IS NOT NULL AND f.text_signature IS NOT NULL AND {trusted_label_sql()}
        ORDER BY f.created_at DESC, f.id DESC
        LIMIT %(limit)s
        """,
        {"user_id": user_id, "kinds": list(EVIDENCE_KINDS), "limit": ROW_LIMIT},
    )
    return cur.fetchall()


def _habits(user_id: str, rows: list[dict]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for row in rows:
        by_key[(str(row["account_id"]), row["institution"], row["text_signature"])].append(row)
    entries = []
    for (account_id, institution, signature), key_rows in by_key.items():
        remaining = list(key_rows)  # newest first
        while remaining:
            anchor = remaining[0]
            band = [row for row in remaining if within_band(int(row["amount"]), int(anchor["amount"]))]
            remaining = [row for row in remaining if row not in band]
            labels = [Label(
                transaction_id=str(row["transaction_id"]), kind=row["kind"], category_id=str(row["after_category_id"]),
                kakeibo=row["after_kakeibo_type"], notes=row["after_notes"], merchant=row["after_merchant"],
                amount=int(row["amount"]), weekday=row["local_weekday"], hour=row["local_hour"], created_at=row["created_at"],
            ) for row in band]
            decision = decide(labels, weekday=None, hour=None, auto_enabled=True)
            if decision is None:
                continue
            category_name = next(row["category_name"] for row in band if str(row["after_category_id"]) == decision.category_id)
            institution_name = INSTITUTION_LABELS.get(institution, institution)
            entries.append({
                "key": "h_" + _key(user_id, account_id, institution, signature, str(anchor["transaction_id"])),
                "label": decision.merchant or f"Tanpa nama toko · {institution_name}",
                "account": {"id": account_id, "name": anchor["account_name"]},
                "category": {"id": decision.category_id, "name": category_name},
                "kakeibo": decision.kakeibo,
                "amount_anchor": int(anchor["amount"]),
                "confirmations": decision.support,
                "agreeing": decision.agreeing,
                "agreement": round(decision.agreement, 4),
                "state": _state(decision.support, decision.auto),
                "last_confirmed_at": max(row["created_at"] for row in band).isoformat(),
            })
    return entries


def _topups(cur, user_id: str) -> list[dict[str, Any]]:
    rows = label_rows(cur, user_id, account_id=None, amount=None, limit=ROW_LIMIT)
    by_pattern: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for row in rows:
        by_pattern[(str(row["source_account_id"]), int(row["amount"]))].append(row)
    patterns = []
    for (source_id, amount), pattern_rows in by_pattern.items():
        stats = product_stats(labels_from_rows(pattern_rows))
        confirmed = [entry for entry in stats.values() if entry.support > 0]
        if not confirmed:
            continue
        leading = max(confirmed, key=lambda entry: (entry.agree - entry.against, entry.support, entry.product_id))
        patterns.append((source_id, amount, leading))
    ids = sorted({source for source, _, _ in patterns} | {leading.product_id for _, _, leading in patterns})
    names: dict[str, dict] = {}
    if ids:
        cur.execute(
            """SELECT a.id, a.name, parent.name AS parent_name FROM accounts a
               LEFT JOIN accounts parent ON parent.id = a.parent_id AND parent.user_id = a.user_id
               WHERE a.user_id = %s AND a.id = ANY(%s::uuid[])""",
            (user_id, ids),
        )
        names = {str(row["id"]): row for row in cur.fetchall()}
    entries = []
    for source_id, amount, leading in patterns:
        source, product = names.get(source_id), names.get(leading.product_id)
        if source is None or product is None:
            continue
        entries.append({
            "key": "t_" + _key(user_id, source_id, str(amount)),
            "source_account": {"id": source_id, "name": source["name"]},
            "amount": amount,
            "product": {"id": leading.product_id, "name": product["name"], "parent_name": product["parent_name"]},
            "confirmations": leading.support,
            "state": _state(leading.support, leading.learned and leading.qualifies),
            "last_confirmed_at": leading.last_confirmed.isoformat() if leading.last_confirmed else None,
        })
    return entries


def _newest(entries: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
    entries.sort(key=lambda entry: (entry["last_confirmed_at"] or "", entry["key"]), reverse=True)
    return entries[:ENTRY_LIMIT], len(entries) > ENTRY_LIMIT


def habit_list(cur, user_id: str) -> dict[str, Any]:
    started = time.perf_counter()
    habits, habits_cut = _newest(_habits(user_id, _habit_rows(cur, user_id)))
    topups, topups_cut = _newest(_topups(cur, user_id))
    # Counts and duration only: labels, merchants and amounts never reach the logs.
    logger.info("habit_list habits=%d topups=%d ms=%.1f", len(habits), len(topups), (time.perf_counter() - started) * 1000)
    return {
        "auto_record": {"habits": settings.habit_auto_record_enabled, "topups": settings.topup_auto_record_enabled},
        "thresholds": {"min_confirmations": MIN_SUPPORT, "min_agreement": MIN_AGREEMENT},
        "habits": habits,
        "topups": topups,
        "truncated": habits_cut or topups_cut,
    }
