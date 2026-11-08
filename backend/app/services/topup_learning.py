"""Investment top-ups learned from the owner's own conversions (clarify-review-and-learn-topups D4–D8).

Deterministic statistics, no model. A debit pattern is the owner's source account and the exact amount;
its evidence is the owner's live conversions of such debits and the owner's answers to suggestions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from app.errors import ErrorCode, api_error
from app.i18n import format_money, t
from app.services.habits import ANSWER_WEIGHT, CORRECTION_WEIGHT, EVIDENCE_WINDOW, MIN_AGREEMENT, MIN_SUPPORT, basis_text
from app.services.ledger_mutations import LIQUID_ACCOUNT_TYPES
from app.services.system_categories import INTERNAL_MOVEMENT, is_system_sql

logger = logging.getLogger(__name__)

# Debits of one pattern this close together are one round of top-ups (design D7, open decision 1).
PAIR_WINDOW = timedelta(days=3)
# Bounds the evidence rows read for one pattern; only the newest EVIDENCE_WINDOW labels are used.
EVIDENCE_ROW_LIMIT = 200

FIRST_EVENT_INTERPRETATION = """
    LEFT JOIN LATERAL (
        SELECT e.interpretation FROM notification_events e
        WHERE e.user_id = t.user_id AND e.transaction_id = t.id
        ORDER BY e.post_time, e.id LIMIT 1
    ) ev ON TRUE
"""


@dataclass(frozen=True)
class TopupLabel:
    """One transaction's latest state for a pattern: a confirmed product, or None for "Bukan top up"."""

    transaction_id: str
    target_id: str | None
    suggested_id: str | None
    weight: int
    labelled_at: datetime
    date: datetime


@dataclass
class ProductStats:
    product_id: str
    support: int = 0
    agree: int = 0
    against: int = 0
    labels: int = 0
    last_confirmed: datetime | None = None

    @property
    def learned(self) -> bool:
        return self.agree > self.against

    @property
    def qualifies(self) -> bool:
        # Integer comparison: exactly 90% passes, as for category habits.
        total = self.agree + self.against
        return self.support >= MIN_SUPPORT and 10 * self.agree >= round(10 * MIN_AGREEMENT) * total

    def basis(self) -> dict[str, Any]:
        total = self.agree + self.against
        return {"agreeing": self.support, "support": self.labels,
                "agreement": round(self.agree / total, 4) if total else 0.0}


@dataclass(frozen=True)
class Assignment:
    """The product a debit is assigned to, or None with the products the owner should choose from."""

    target_id: str | None
    choices: tuple[str, ...]
    rule: str  # "single", "pair" or "ask"


# --- Pure decision rules ---------------------------------------------------------------------


def latest_labels(labels: list[TopupLabel], *, window: int = EVIDENCE_WINDOW) -> list[TopupLabel]:
    return sorted(labels, key=lambda label: (label.labelled_at, label.transaction_id), reverse=True)[:window]


def product_stats(labels: list[TopupLabel]) -> dict[str, ProductStats]:
    """Per-product support, agreement weight and weight against it (design D5).

    A rejection counts against every product of the pattern; a correction counts against the product the
    owner moved away from. Confirming one product is never evidence against another.
    """
    stats: dict[str, ProductStats] = {}
    for label in labels:
        if label.target_id is None:
            continue
        entry = stats.setdefault(label.target_id, ProductStats(label.target_id))
        entry.support += 1
        entry.labels += 1
        entry.agree += label.weight
        if entry.last_confirmed is None or label.date > entry.last_confirmed:
            entry.last_confirmed = label.date
    for label in labels:
        if label.target_id is None:
            for entry in stats.values():
                entry.against += label.weight
                entry.labels += 1
        elif label.suggested_id and label.suggested_id != label.target_id and label.suggested_id in stats:
            stats[label.suggested_id].against += label.weight
            stats[label.suggested_id].labels += 1
    return stats


def ranked_learned(stats: dict[str, ProductStats], names: dict[str, str]) -> list[str]:
    learned = [entry for entry in stats.values() if entry.learned and entry.product_id in names]
    learned.sort(key=lambda entry: (-entry.agree, -entry.support, names[entry.product_id], entry.product_id))
    return [entry.product_id for entry in learned]


def assign(debit_id: str, pending: list[str], learned: list[str], filled: set[str]) -> Assignment | None:
    """One-to-one assignment of the pending debits of a window to the learned products (design D7)."""
    if not learned:
        return None
    if len(learned) == 1:
        return Assignment(learned[0], (learned[0],), "single")
    open_products = [product for product in learned if product not in filled]
    if open_products and debit_id in pending and len(pending) == len(open_products):
        return Assignment(open_products[pending.index(debit_id)], tuple(open_products), "pair")
    return Assignment(None, tuple(open_products or learned), "ask")


# --- Evidence and state ----------------------------------------------------------------------


def label_rows(cur, user_id: str, *, account_id: str | None, amount: int | None, limit: int) -> list[dict]:
    """The owner's live conversions and latest suggestion answers, one row per transaction, newest first.

    With `account_id` and `amount` the rows of one pattern; without them every pattern of the owner
    (the habit list), each row naming its `source_account_id` and `amount`.
    """
    cur.execute(
        f"""
        WITH conversions AS (
            SELECT t.id AS transaction_id, it.target_account_id, it.created_at AS converted_at, t.date,
                   it.source_account_id, it.amount,
                   ev.interpretation ->> 'match_key' AS match_key
            FROM investment_topups it
            JOIN transactions t ON t.user_id = it.user_id AND t.movement_id = it.id
                 AND t.id = (it.converted_from ->> 'transaction_id')::uuid
            {FIRST_EVENT_INTERPRETATION}
            WHERE it.user_id = %(user_id)s
              AND (%(account_id)s::uuid IS NULL OR it.source_account_id = %(account_id)s::uuid)
              AND (%(amount)s::bigint IS NULL OR it.amount = %(amount)s::bigint)
              AND it.is_deleted = FALSE AND it.converted_from IS NOT NULL
              AND t.classified_by IS DISTINCT FROM 'habit'
        ), answers AS (
            SELECT DISTINCT ON (f.transaction_id) f.transaction_id, f.outcome, f.source_account_id, f.amount,
                   f.suggested_target_account_id, f.chosen_target_account_id, f.created_at AS answered_at,
                   t.date, ev.interpretation ->> 'match_key' AS match_key
            FROM investment_topup_feedback f
            JOIN transactions t ON t.user_id = f.user_id AND t.id = f.transaction_id
            {FIRST_EVENT_INTERPRETATION}
            WHERE f.user_id = %(user_id)s
              AND (%(account_id)s::uuid IS NULL OR f.source_account_id = %(account_id)s::uuid)
              AND (%(amount)s::bigint IS NULL OR f.amount = %(amount)s::bigint)
            ORDER BY f.transaction_id, f.created_at DESC, f.id DESC
        )
        SELECT COALESCE(c.transaction_id, a.transaction_id) AS transaction_id, c.target_account_id,
               c.converted_at, a.outcome, a.suggested_target_account_id, a.chosen_target_account_id,
               a.answered_at, COALESCE(c.date, a.date) AS date, COALESCE(c.match_key, a.match_key) AS match_key,
               COALESCE(c.source_account_id, a.source_account_id) AS source_account_id,
               COALESCE(c.amount, a.amount) AS amount
        FROM conversions c FULL OUTER JOIN answers a ON a.transaction_id = c.transaction_id
        ORDER BY GREATEST(c.converted_at, a.answered_at) DESC
        LIMIT %(limit)s
        """,
        {"user_id": user_id, "account_id": account_id, "amount": amount, "limit": limit},
    )
    return cur.fetchall()


def labels_from_rows(rows: list[dict], match_key: str | None = None) -> list[TopupLabel]:
    """Labels of one pattern from its rows, newest EVIDENCE_WINDOW first (design D4)."""
    labels = []
    for row in rows:
        if match_key and row["match_key"] and row["match_key"] != match_key:
            continue
        if row["target_account_id"]:
            target = str(row["target_account_id"])
            corrected = row["outcome"] == "corrected" and str(row["chosen_target_account_id"]) == target
            labels.append(TopupLabel(
                transaction_id=str(row["transaction_id"]), target_id=target,
                suggested_id=str(row["suggested_target_account_id"]) if corrected else None,
                weight=CORRECTION_WEIGHT if corrected else ANSWER_WEIGHT,
                labelled_at=max(filter(None, [row["converted_at"], row["answered_at"]])), date=row["date"],
            ))
        elif row["outcome"] == "rejected":
            suggested = row["suggested_target_account_id"]
            labels.append(TopupLabel(
                transaction_id=str(row["transaction_id"]), target_id=None,
                suggested_id=str(suggested) if suggested else None, weight=CORRECTION_WEIGHT,
                labelled_at=row["answered_at"], date=row["date"],
            ))
    return latest_labels(labels)


def load_labels(cur, user_id: str, *, account_id: str, amount: int, match_key: str | None) -> list[TopupLabel]:
    """The owner's labels of a pattern, newest EVIDENCE_WINDOW first (design D4).

    Confirmations are read from live converted top-ups, so conversions made before the learner existed
    count without being copied. Top-ups the learner recorded itself are not evidence.
    """
    rows = label_rows(cur, user_id, account_id=account_id, amount=amount, limit=EVIDENCE_ROW_LIMIT)
    return labels_from_rows(rows, match_key)


def eligible_products(cur, user_id: str, product_ids: list[str]) -> dict[str, dict]:
    """Active amount-tracked mutual-fund products of the owner, with their "Induk · Produk" label."""
    if not product_ids:
        return {}
    cur.execute(
        """SELECT a.id, a.name, parent.name AS parent_name
           FROM accounts a
           LEFT JOIN accounts parent ON parent.id = a.parent_id AND parent.user_id = a.user_id
           WHERE a.user_id = %s AND a.id = ANY(%s::uuid[]) AND a.is_archived = FALSE
             AND a.instrument_type = 'mutual_fund' AND a.units IS NULL
             AND NOT EXISTS (SELECT 1 FROM accounts child WHERE child.parent_id = a.id)""",
        (user_id, product_ids),
    )
    return {
        str(row["id"]): {
            "id": str(row["id"]), "name": row["name"],
            "label": f"{row['parent_name']} · {row['name']}" if row["parent_name"] else row["name"],
        }
        for row in cur.fetchall()
    }


def pending_debits(cur, user_id: str, *, account_id: str, amount: int, around: datetime,
                   match_key: str | None) -> list[dict]:
    """The owner's unlinked debits of the pattern in the window that still await their counterpart."""
    cur.execute(
        f"""
        SELECT t.id, t.date, ev.interpretation ->> 'match_key' AS match_key
        FROM transactions t
        JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id AND {is_system_sql('c', INTERNAL_MOVEMENT)}
        {FIRST_EVENT_INTERPRETATION}
        LEFT JOIN LATERAL (
            SELECT f.outcome FROM investment_topup_feedback f
            WHERE f.user_id = t.user_id AND f.transaction_id = t.id
            ORDER BY f.created_at DESC, f.id DESC LIMIT 1
        ) answer ON TRUE
        WHERE t.user_id = %s AND t.account_id = %s AND t.amount = %s AND t.type = 'expense'
          AND t.movement_id IS NULL AND t.review_status = 'needs_review' AND t.owner_split_at IS NULL
          AND t.date BETWEEN %s AND %s
          AND COALESCE(ev.interpretation -> 'review_reasons', '[]'::jsonb) ? 'awaiting_counterpart'
          AND answer.outcome IS DISTINCT FROM 'rejected'
        ORDER BY t.date, t.id
        """,
        (user_id, account_id, amount, around - PAIR_WINDOW, around + PAIR_WINDOW),
    )
    return [row for row in cur.fetchall() if not (match_key and row["match_key"] and row["match_key"] != match_key)]


def filled_products(cur, user_id: str, *, account_id: str, amount: int, around: datetime) -> set[str]:
    cur.execute(
        """SELECT DISTINCT target_account_id FROM investment_topups
           WHERE user_id = %s AND source_account_id = %s AND amount = %s AND is_deleted = FALSE
             AND date BETWEEN %s AND %s""",
        (user_id, account_id, amount, around - PAIR_WINDOW, around + PAIR_WINDOW),
    )
    return {str(row["target_account_id"]) for row in cur.fetchall()}


@dataclass(frozen=True)
class Pattern:
    """What the learner knows about one source account and amount, for one notification match key."""

    stats: dict[str, ProductStats]
    products: dict[str, dict]
    learned: list[str]


def load_pattern(cur, user_id: str, *, account_id: str, amount: int, match_key: str | None) -> Pattern:
    labels = load_labels(cur, user_id, account_id=account_id, amount=amount, match_key=match_key)
    stats = product_stats(labels)
    products = eligible_products(cur, user_id, list(stats))
    names = {product_id: product["name"] for product_id, product in products.items()}
    return Pattern(stats=stats, products=products, learned=ranked_learned(stats, names))


def debit_assignment(cur, user_id: str, debit: dict, pattern: Pattern) -> tuple[Assignment | None, list[dict]]:
    """The assignment of one awaiting debit and the pending debits of its window."""
    if not pattern.learned:
        return None, []
    account_id, amount = str(debit["account_id"]), int(debit["amount"])
    pending = pending_debits(cur, user_id, account_id=account_id, amount=amount, around=debit["date"],
                             match_key=debit.get("match_key"))
    # A debit the owner rejected, kept separate or already settled gets no suggestion.
    if str(debit["id"]) not in {str(row["id"]) for row in pending}:
        return None, pending
    filled = filled_products(cur, user_id, account_id=account_id, amount=amount, around=debit["date"])
    return assign(str(debit["id"]), [str(row["id"]) for row in pending], pattern.learned, filled), pending


# --- Suggestion ------------------------------------------------------------------------------


def previous_month(earlier: datetime, later: datetime) -> bool:
    from app.core.config import settings

    zone = ZoneInfo(settings.tz)
    first, second = earlier.astimezone(zone), later.astimezone(zone)
    return (second.year * 12 + second.month) - (first.year * 12 + first.month) == 1


def suggestion_view(
    debit: dict, assignment: Assignment, pattern: Pattern, pending: list[dict], *, language: str, region: str,
) -> dict[str, Any]:
    """The `topup_suggestion` of an awaiting debit in the review queue, in the account's language (D5)."""
    choices = [pattern.products[product_id] for product_id in assignment.choices]
    amount = int(debit["amount"])
    amount_text = format_money(amount, region, "inline")
    account_name = debit.get("account_name") or t(language, "labels.fallback.this_account")
    if assignment.target_id is None:
        names = [product["name"] for product in choices]
        prompt_params = {"amount": amount, "account": account_name, "products": names}
        return {
            "target_account_id": None, "target_label": None, "choices": choices, "basis": None,
            "prompt": t(language, "questions.topup.choose", amount=amount_text, account=account_name,
                        products=t(language, "labels.fallback.or").join(names)),
            "prompt_code": "questions.topup.choose", "prompt_params": prompt_params,
            "explanation": t(language, "reasons.topup.choose"),
            "explanation_code": "reasons.topup.choose", "explanation_params": {},
        }
    stats = pattern.stats[assignment.target_id]
    product = pattern.products[assignment.target_id]
    last_month = bool(stats.last_confirmed and previous_month(stats.last_confirmed, debit["date"]))
    prompt_code = "questions.topup.last_month" if last_month else "questions.topup.before"
    basis = stats.basis()
    explanation_code = "reasons.topup.single"
    explanation_params: dict[str, Any] = {"agreeing": basis["agreeing"], "support": basis["support"],
                                          "amount": amount, "account": account_name}
    explanation = t(language, explanation_code, basis=basis_text(language, basis["agreeing"], basis["support"]),
                    amount=amount_text, account=account_name)
    if assignment.rule == "pair":
        explanation_code = "reasons.topup.pair"
        explanation_params = {"debits": len(pending), "products": len(assignment.choices),
                              "amount": amount, "account": account_name}
        explanation = t(language, explanation_code, **{**explanation_params, "amount": amount_text})
    return {
        "target_account_id": assignment.target_id, "target_label": product["label"], "choices": choices,
        "basis": basis, "prompt": t(language, prompt_code, product=product["name"]),
        "prompt_code": prompt_code, "prompt_params": {"product": product["name"]},
        "explanation": explanation, "explanation_code": explanation_code, "explanation_params": explanation_params,
    }


def topup_suggestion(
    cur, user_id: str, debit: dict, account_type: str | None, cache: dict, *, language: str, region: str,
) -> dict[str, Any] | None:
    """The suggestion for an awaiting outgoing debit from a liquid account, or None."""
    if debit["type"] != "expense" or account_type not in LIQUID_ACCOUNT_TYPES:
        return None
    key = (str(debit["account_id"]), int(debit["amount"]), debit.get("match_key"))
    if key not in cache:
        cache[key] = load_pattern(cur, user_id, account_id=key[0], amount=key[1], match_key=key[2])
    pattern = cache[key]
    assignment, pending = debit_assignment(cur, user_id, debit, pattern)
    if assignment is None:
        return None
    return suggestion_view(debit, assignment, pattern, pending, language=language, region=region)


# --- Owner answers ---------------------------------------------------------------------------


def record_answer(cur, user_id: str, transaction: dict, *, outcome: str, suggested_id: str | None,
                  chosen_id: str | None) -> str:
    """Store the owner's answer about one debit and return its id."""
    cur.execute(
        """INSERT INTO investment_topup_feedback (
               user_id, transaction_id, source_account_id, amount, outcome,
               suggested_target_account_id, chosen_target_account_id
           ) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
        (user_id, str(transaction["id"]), str(transaction["account_id"]), int(transaction["amount"]),
         outcome, suggested_id, chosen_id),
    )
    return str(cur.fetchone()["id"])


def reject_suggestion(cur, user_id: str, transaction_id: str, suggested_id: str | None) -> bool:
    """Store "Bukan top up" for an unlinked expense; False when the latest answer already says so."""
    cur.execute(
        "SELECT id, account_id, amount, type, movement_id FROM transactions WHERE user_id = %s AND id = %s FOR UPDATE",
        (user_id, transaction_id),
    )
    transaction = cur.fetchone()
    if not transaction:
        raise api_error(404, ErrorCode.TRANSACTION_NOT_FOUND)
    if transaction["type"] != "expense" or transaction["movement_id"]:
        raise api_error(409, ErrorCode.TRANSACTION_NOT_SUGGESTIBLE)
    if suggested_id:
        cur.execute("SELECT 1 FROM accounts WHERE user_id = %s AND id = %s", (user_id, suggested_id))
        if not cur.fetchone():
            raise api_error(404, ErrorCode.ACCOUNT_NOT_FOUND, message_key="product_not_found")
    cur.execute(
        """SELECT outcome FROM investment_topup_feedback WHERE user_id = %s AND transaction_id = %s
           ORDER BY created_at DESC, id DESC LIMIT 1""",
        (user_id, transaction_id),
    )
    latest = cur.fetchone()
    if latest and latest["outcome"] == "rejected":
        return False
    record_answer(cur, user_id, transaction, outcome="rejected", suggested_id=suggested_id, chosen_id=None)
    return True


# --- Automatic recording ---------------------------------------------------------------------


def auto_record(cur, user_id: str, transaction_id: str) -> list[str]:
    """Convert the pending debits of a new awaiting debit's window whose learned product qualifies (D8).

    Returns the ids of the converted debits. Each conversion runs in a savepoint: a refusal leaves that
    debit awaiting, as it was, and never fails the notification that triggered the evaluation.
    """
    from app.core.config import settings
    from app.services.investment_topups import convert_transaction_to_topup

    if not settings.topup_auto_record_enabled:
        return []
    cur.execute(
        f"""SELECT t.id, t.account_id, t.amount, t.date, t.type, a.type AS account_type,
                   ev.interpretation ->> 'match_key' AS match_key
            FROM transactions t JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
            {FIRST_EVENT_INTERPRETATION}
            WHERE t.user_id = %s AND t.id = %s""",
        (user_id, transaction_id),
    )
    debit = cur.fetchone()
    if not debit or debit["type"] != "expense" or debit["account_type"] not in LIQUID_ACCOUNT_TYPES:
        return []
    pattern = load_pattern(cur, user_id, account_id=str(debit["account_id"]), amount=int(debit["amount"]),
                           match_key=debit["match_key"])
    if not any(pattern.stats[product_id].qualifies for product_id in pattern.learned):
        return []
    _, pending = debit_assignment(cur, user_id, debit, pattern)
    planned = []
    for row in pending:
        assignment, _ = debit_assignment(cur, user_id, {**row, "account_id": debit["account_id"],
                                                        "amount": debit["amount"]}, pattern)
        if assignment and assignment.target_id and pattern.stats[assignment.target_id].qualifies:
            planned.append((str(row["id"]), assignment.target_id))
    converted = []
    for debit_id, target_id in planned:
        basis = {"kind": "investment_topup", "target_account_id": target_id, **pattern.stats[target_id].basis()}
        cur.execute("SAVEPOINT topup_learning")
        try:
            convert_transaction_to_topup(cur, user_id=user_id, transaction_id=debit_id, target_id=target_id,
                                         learned_basis=basis)
        except HTTPException as refusal:
            cur.execute("ROLLBACK TO SAVEPOINT topup_learning")
            code = refusal.detail.get("code") if isinstance(refusal.detail, dict) else refusal.status_code
            logger.info("learned top-up not recorded: %s", code)
            continue
        cur.execute("RELEASE SAVEPOINT topup_learning")
        converted.append(debit_id)
    return converted
