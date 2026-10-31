"""Habits learned from the owner's own answers to merchant-less payments (learn-habits-to-ask-less).

Deterministic statistics, no model: the owner's labelled examples for one habit key decide whether a
payment is recorded without a question, and otherwise which suggestion comes first.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

# Approved thresholds (owner 2026-10-31): record automatically from 3 labelled payments when at least
# 90% of their weight agrees on one category and pillar.
MIN_SUPPORT = 3
MIN_AGREEMENT = 0.9
# A payment belongs to a habit when its amount is within ±25% of a labelled one, measured as a ratio
# so that Rp 40.000 against Rp 50.000 and Rp 400.000 against Rp 500.000 are equally close (design D3).
AMOUNT_RATIO_NUMERATOR, AMOUNT_RATIO_DENOMINATOR = 5, 4
# Only the latest labelled payments of a key count, so a changed habit is learned (design D5).
EVIDENCE_WINDOW = 10
ANSWER_WEIGHT = 1
# An owner correction overrides a recorded value, so it counts double (design D4).
CORRECTION_WEIGHT = 2
HOUR_TOLERANCE = 2
EVIDENCE_KINDS = ("purpose_answer", "correction", "capture")


@dataclass(frozen=True)
class Label:
    """One owner-labelled payment of a habit key, the latest label of its transaction."""

    transaction_id: str
    kind: str
    category_id: str
    kakeibo: str | None
    notes: str | None
    merchant: str | None
    amount: int
    weekday: int | None
    hour: int | None
    created_at: datetime

    @property
    def weight(self) -> int:
        return CORRECTION_WEIGHT if self.kind == "correction" else ANSWER_WEIGHT

    @property
    def classification(self) -> tuple[str, str | None]:
        return (self.category_id, self.kakeibo)


@dataclass(frozen=True)
class HabitDecision:
    category_id: str
    kakeibo: str | None
    notes: str | None
    merchant: str | None
    support: int
    agreeing: int
    agreement: float
    auto: bool

    def basis(self) -> dict:
        """What is stored with a habit-recorded payment and shown as its explanation."""
        return {"category_id": self.category_id, "kakeibo": self.kakeibo, "support": self.support,
                "agreeing": self.agreeing, "agreement": round(self.agreement, 4)}


def within_band(amount: int, labelled: int) -> bool:
    if amount <= 0 or labelled <= 0:
        return False
    return (AMOUNT_RATIO_DENOMINATOR * labelled <= AMOUNT_RATIO_NUMERATOR * amount
            and AMOUNT_RATIO_DENOMINATOR * amount <= AMOUNT_RATIO_NUMERATOR * labelled)


def hour_distance(left: int, right: int) -> int:
    difference = abs(left - right) % 24
    return min(difference, 24 - difference)


def time_fit(label: Label, weekday: int | None, hour: int | None) -> int:
    score = 0
    if hour is not None and label.hour is not None and hour_distance(hour, label.hour) <= HOUR_TOLERANCE:
        score += 1
    if weekday is not None and label.weekday == weekday:
        score += 1
    return score


def latest_labels(labels: list[Label], *, window: int = EVIDENCE_WINDOW) -> list[Label]:
    """The newest label per transaction, then the newest `window` transactions.

    A later correction of a transaction replaces its earlier answer: one payment is one example.
    """
    newest: dict[str, Label] = {}
    for label in sorted(labels, key=lambda item: item.created_at, reverse=True):
        newest.setdefault(label.transaction_id, label)
    ordered = sorted(newest.values(), key=lambda item: item.created_at, reverse=True)
    return ordered[:window]


def decide(
    labels: list[Label], *, weekday: int | None, hour: int | None, auto_enabled: bool,
    window: int = EVIDENCE_WINDOW,
) -> HabitDecision | None:
    """The habit of a key from its owner labels, or None without any label.

    Agreement is the weight of the leading category and pillar over the total weight. Time of day and
    weekday never change the agreement; they only break ties and pick the wording of the leading label.
    """
    evidence = latest_labels(labels, window=window)
    if not evidence:
        return None
    groups: dict[tuple[str, str | None], list[Label]] = {}
    for label in evidence:
        groups.setdefault(label.classification, []).append(label)

    def rank(members: list[Label]) -> tuple:
        return (sum(item.weight for item in members), sum(time_fit(item, weekday, hour) for item in members),
                max(item.created_at for item in members))

    leading = max(groups.values(), key=rank)
    total_weight = sum(item.weight for item in evidence)
    leading_weight = sum(item.weight for item in leading)
    # Integer comparison: 9 of 10 must pass exactly at the threshold.
    agrees = 10 * leading_weight >= round(10 * MIN_AGREEMENT) * total_weight
    wording = max(leading, key=lambda item: (time_fit(item, weekday, hour), item.created_at))
    support = len(evidence)
    return HabitDecision(
        category_id=wording.category_id, kakeibo=wording.kakeibo, notes=wording.notes, merchant=wording.merchant,
        support=support, agreeing=len(leading), agreement=leading_weight / total_weight,
        auto=auto_enabled and support >= MIN_SUPPORT and agrees,
    )


# --- Evidence and evaluation ------------------------------------------------------------------


def trusted_label_sql(alias: str = "f") -> str:
    """Whether a feedback row is the owner's own label (design D1).

    Corrections are always owner edits. A purpose answer counts only when it classified the row (a text
    answer the normaliser could not classify keeps the earlier automatic category). A capture counts
    only when the owner classified it. Rows written before V39 carry no review state: a purpose answer
    then counts when its event recorded the same reply as reviewed, and an older capture never counts.
    """
    return f"""CASE {alias}.kind
        WHEN 'correction' THEN TRUE
        WHEN 'purpose_answer' THEN COALESCE(
            {alias}.after_review_status = 'reviewed',
            (answered.interpretation -> 'purpose_answer' ->> 'reply_id') = {alias}.reply_id::text
            AND answered.interpretation -> 'purpose_answer' ->> 'review_status' = 'reviewed',
            FALSE)
        WHEN 'capture' THEN COALESCE({alias}.after_classified_by = 'owner', FALSE)
        ELSE FALSE END"""


def load_labels(cur, user_id: str, *, institution: str, signature: str, account_id: str, amount: int) -> list[Label]:
    """The owner's latest labels of a habit key, at most the evidence window, newest first.

    Every feedback row of a transaction is read so that its latest one decides; a later row that is not an
    owner label (an accepted history suggestion, an unclassified answer) leaves the transaction out.
    """
    from app.services.reimbursements import NON_REPORTABLE_CATEGORY_NAMES

    cur.execute(
        f"""
        WITH keyed AS (
            SELECT DISTINCT transaction_id FROM classification_feedback
            WHERE user_id = %(user_id)s AND transaction_id IS NOT NULL AND account_id = %(account_id)s
              AND text_signature = %(signature)s AND institution = %(institution)s
        ), latest AS (
            SELECT DISTINCT ON (f.transaction_id) f.*
            FROM classification_feedback f JOIN keyed ON keyed.transaction_id = f.transaction_id
            WHERE f.user_id = %(user_id)s
            ORDER BY f.transaction_id, f.created_at DESC, f.id DESC
        )
        SELECT f.transaction_id, f.kind, f.after_category_id, f.after_kakeibo_type, f.after_notes,
               f.after_merchant, f.amount, f.local_weekday, f.local_hour, f.created_at
        FROM latest f
        JOIN categories c ON c.id = f.after_category_id AND c.user_id = f.user_id
             AND c.kind = 'expense' AND c.is_archived = FALSE AND c.name <> ALL(%(excluded)s)
        LEFT JOIN notification_events answered
             ON f.kind = 'purpose_answer' AND answered.user_id = f.user_id AND answered.id = f.event_id
        WHERE f.kind = ANY(%(kinds)s) AND f.after_type = 'expense' AND f.account_id = %(account_id)s
          AND f.text_signature = %(signature)s AND f.institution = %(institution)s
          AND %(denominator)s::bigint * f.amount <= %(numerator)s::bigint * %(amount)s::bigint
          AND %(denominator)s::bigint * %(amount)s::bigint <= %(numerator)s::bigint * f.amount
          AND {trusted_label_sql()}
        ORDER BY f.created_at DESC, f.id DESC
        LIMIT %(window)s
        """,
        {"user_id": user_id, "account_id": account_id, "signature": signature, "institution": institution,
         "excluded": list(NON_REPORTABLE_CATEGORY_NAMES), "kinds": list(EVIDENCE_KINDS), "amount": amount,
         "numerator": AMOUNT_RATIO_NUMERATOR, "denominator": AMOUNT_RATIO_DENOMINATOR, "window": EVIDENCE_WINDOW},
    )
    return [Label(
        transaction_id=str(row["transaction_id"]), kind=row["kind"], category_id=str(row["after_category_id"]),
        kakeibo=row["after_kakeibo_type"], notes=row["after_notes"], merchant=row["after_merchant"],
        amount=int(row["amount"]), weekday=row["local_weekday"], hour=row["local_hour"], created_at=row["created_at"],
    ) for row in cur.fetchall()]


def habit_account(facts, context: dict) -> dict | None:
    """The observed account of a merchant-less expense that a habit may classify, else None (design D6).

    Income, movements, own-account and transfer-shaped payments, payments naming the owner and payments
    whose account still needs the model or the owner are never habit candidates.
    """
    from app.services.notification_evidence import EvidenceError
    from app.services.notification_resolution import observed_endpoint, transfer_signature

    key = context.get("_classification_key")
    endpoint_facts = context.get("facts") or {}
    if (not key or not key.get("merchant_less") or facts.status != "candidate" or facts.direction != "expense"
            or not facts.amount or facts.parsed.own_account_transfer or facts.parsed.rdn_deposit
            or context.get("self_identity_detected") or endpoint_facts.get("counterparty_is_owner")
            or endpoint_facts.get("unresolved_names") or endpoint_facts.get("mapping_error")
            or not key.get("institution") or not key.get("text_signature")):
        return None
    try:
        account = observed_endpoint(facts, context["accounts"])
    except EvidenceError:
        return None
    if transfer_signature(facts, context, str(account["id"]))["transfer"]:
        return None
    return account


def evaluate_habit(cur, user_id: str, facts, context: dict) -> dict | None:
    """The habit decision for a notification, stored in the context as `_habit`."""
    from app.core.config import settings
    from app.services.merchant_memory import local_time_features

    account = habit_account(facts, context)
    if account is None:
        return None
    key = context["_classification_key"]
    labels = load_labels(cur, user_id, institution=key["institution"], signature=key["text_signature"],
                         account_id=str(account["id"]), amount=facts.amount)
    weekday, hour = local_time_features(facts.timestamp)
    decision = decide(labels, weekday=weekday, hour=hour, auto_enabled=settings.habit_auto_record_enabled)
    if decision is None:
        return None
    return {"decision": decision, "account_id": str(account["id"])}


def habit_applies(context: dict) -> bool:
    """A habit records this payment without the model, the way an owner memory hit does."""
    habit = context.get("_habit")
    return bool(habit and habit["decision"].auto)


def basis_text(agreeing: int, support: int) -> str:
    return f"{agreeing} dari {support} jawaban sebelumnya"


def habit_label(category_name: str | None) -> str:
    return f"Dicatat dari kebiasaan: {category_name}" if category_name else "Dicatat dari kebiasaan"


def guess_label(category_name: str) -> str:
    return f"Sepertinya {category_name}?"
