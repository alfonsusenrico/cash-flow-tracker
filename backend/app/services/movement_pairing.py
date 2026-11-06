"""Automatic pairing of recorded legs into movements.

One rule set serves the immediate pairing of a new notification leg, the deferred re-pairing pass and
the owner's completion of a one-sided transfer: fee tolerance, evidence strength, ambiguity and the
transfer-fee row. A movement is always exactly two linked legs of equal amount; a fee is a separate
expense traced to its movement.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException

from app.core.config import settings
from app.db.pool import db_conn
from app.services.ledger_mutations import (
    MOVEMENT_ACCOUNT_TYPES, ensure_generic_movement_accounts, lock_owned_accounts,
)
from app.services.movement_linkage import (
    ensure_internal_movement_categories, link_transactions_as_movement, movement_note,
)
from app.services.notification_resolution import pairing_evidence
from app.services.provenance import system_provenance
from app.services.system_categories import INTERNAL_MOVEMENT, is_movement, is_system_sql

logger = logging.getLogger(__name__)

CANDIDATE_LIMIT = 50
REPAIR_LEG_LIMIT = 500
FEE_CATEGORY_NAME = "Biaya Admin & Transfer"
FEE_NOTES = "Biaya transfer"
COMPLETION_KEY_PREFIX = "movement-complete:"
# The other leg of an own transfer may already be on the chosen account as its own row; completing must
# link it rather than record the money twice.
COMPLETION_MATCH_WINDOW = timedelta(days=3)

# Eligible unlinked legs: a notification leg the owner has not reclassified, or any row the owner or
# the pipeline filed in the movement category. Rows the owner split apart never pair automatically again.
CANDIDATE_SQL = f"""
    SELECT t.id, t.type, t.account_id, t.amount, t.date, t.source, t.review_status, t.classified_by,
           n.id AS event_id, n.interpretation AS evidence, n.result_key,
           {is_system_sql('c', INTERNAL_MOVEMENT)} AS movement_category
    FROM transactions t
    JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
    LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
    LEFT JOIN LATERAL (
        SELECT e.id, e.interpretation, e.result_key FROM notification_events e
        WHERE e.user_id = t.user_id AND e.transaction_id = t.id
          AND e.processing_state = 'recorded' AND e.provenance_kind = 'observed_leg'
        ORDER BY e.post_time, e.id LIMIT 1
    ) n ON TRUE
    WHERE t.user_id = %(user_id)s
      AND t.movement_id IS NULL AND t.owner_split_at IS NULL AND t.fee_for_movement_id IS NULL
      AND t.type IN ('expense', 'income') AND (%(direction)s::text IS NULL OR t.type = %(direction)s::text)
      AND t.source IN ('notification', 'manual')
      AND ((n.id IS NOT NULL AND t.owner_edited_at IS NULL) OR {is_system_sql('c', INTERNAL_MOVEMENT)})
      AND a.is_archived = FALSE AND a.type IN ('cash', 'bank', 'wallet', 'ewallet') AND a.instrument_type IS NULL
      AND t.goal_id IS NULL AND t.obligation_id IS NULL AND t.recurring_rule_id IS NULL
      AND NOT EXISTS (SELECT 1 FROM transaction_obligation_allocations alloc WHERE alloc.transaction_id = t.id)
      AND t.id <> ALL(%(exclude)s::uuid[])
      {{filters}}
    ORDER BY t.date, t.id
    LIMIT %(limit)s
"""


@dataclass(frozen=True)
class PairingDecision:
    counterpart: dict | None = None
    fee: int = 0
    ambiguous: bool = False
    # Other legs that compete for the same transfer; flagged for review with the new leg.
    ambiguous_ids: tuple[str, ...] = ()


def transfer_fee(expense_amount: int, income_amount: int) -> int | None:
    """The known transfer fee by which an outbound leg exceeds its inbound leg, 0 when equal, else None."""
    difference = int(expense_amount) - int(income_amount)
    if difference == 0:
        return 0
    if 0 < difference <= settings.movement_fee_max_difference and difference in settings.movement_transfer_fees:
        return difference
    return None


def max_fee_difference() -> int:
    return settings.movement_fee_max_difference if settings.movement_transfer_fees else 0


def load_accounts(cur, user_id: str) -> dict[str, dict]:
    cur.execute(
        "SELECT id, name, type, parent_id, instrument_type, is_archived FROM accounts WHERE user_id = %s",
        (user_id,),
    )
    return {str(row["id"]): row for row in cur.fetchall()}


def root_of(accounts: dict[str, dict], account_id: str | None) -> str | None:
    account = accounts.get(str(account_id)) if account_id else None
    if not account:
        return None
    return str(account.get("parent_id") or account["id"])


def leg_evidence(row: dict, accounts: dict[str, dict]) -> dict:
    """Pairing evidence of a stored leg, using its current account and the owner's own filing.

    A row filed as Internal Movement is the owner's (or the pipeline's) statement that it is one half
    of an own transfer, so it carries owner identity and no third party.
    """
    evidence = dict(row.get("evidence") or {}) if row.get("event_id") else {}
    if row.get("movement_category"):
        evidence.update(transfer=True, self_identity=True, external_counterparty=False)
    account_id = str(row["account_id"])
    remote = evidence.get("remote_account_id")
    evidence.update(
        account_id=account_id, direction=row["type"], currency="IDR",
        root_account_id=root_of(accounts, account_id),
        remote_root_account_id=root_of(accounts, remote) if remote else None,
    )
    return evidence


def load_candidates(
    cur, user_id: str, *, direction: str | None, around: list[datetime], gap_seconds: int,
    min_amount: int, max_amount: int, exclude: list[str], extra_filters: str = "", extra: dict | None = None,
    limit: int = CANDIDATE_LIMIT + 1,
) -> list[dict]:
    gap = timedelta(seconds=gap_seconds)
    cur.execute(
        # The window is open at both ends, as notification pairing always was.
        CANDIDATE_SQL.format(filters="AND t.date > %(earliest)s AND t.date < %(latest)s "
                                     "AND t.amount BETWEEN %(min_amount)s AND %(max_amount)s " + extra_filters),
        {"user_id": user_id, "direction": direction, "exclude": [str(value) for value in exclude if value],
         "earliest": min(around) - gap, "latest": max(around) + gap,
         "min_amount": min_amount, "max_amount": max_amount, "limit": limit, **(extra or {})},
    )
    return cur.fetchall()


def opposite_candidates(cur, user_id: str, leg: dict, *, gap_seconds: int, exclude: list[str]) -> list[dict]:
    amount = int(leg["amount"])
    if leg["type"] == "expense":
        low, high, direction = max(1, amount - max_fee_difference()), amount, "income"
    else:
        low, high, direction = amount, amount + max_fee_difference(), "expense"
    return load_candidates(cur, user_id, direction=direction, around=[leg["date"]], gap_seconds=gap_seconds,
                           min_amount=low, max_amount=high, exclude=exclude)


def match(leg: dict, other: dict) -> tuple[str, int] | None:
    """(strength, fee) when two opposite legs may be one transfer under the amount, gap and evidence rules."""
    expense, income = (leg, other) if leg["type"] == "expense" else (other, leg)
    fee = transfer_fee(expense["amount"], income["amount"])
    if fee is None:
        return None
    strength = pairing_evidence(leg["evidence"], other["evidence"])
    if strength is None:
        return None
    if strength == "weak":
        gap = abs((leg["date"] - other["date"]).total_seconds())
        # Weak evidence only links an exact amount inside the immediate window.
        if fee or gap >= settings.notification_pairing_window_seconds:
            return None
    return strength, fee


def as_leg(row: dict, accounts: dict[str, dict]) -> dict:
    return {**row, "id": str(row["id"]), "evidence": leg_evidence(row, accounts)}


def find_counterpart(cur, user_id: str, leg: dict, *, gap_seconds: int, accounts: dict[str, dict] | None = None) -> PairingDecision:
    """The unique counterpart of `leg`, or why there is none.

    `leg` has `id` (None while not yet inserted), `type`, `account_id`, `amount`, `date` and `evidence`.
    A pair is accepted only when the counterpart is the leg's single match and the leg is the
    counterpart's single match; a weak pair also needs the amount to be unique among candidates.
    """
    accounts = accounts if accounts is not None else load_accounts(cur, user_id)
    rows = opposite_candidates(cur, user_id, leg, gap_seconds=gap_seconds, exclude=[leg.get("id")])
    if len(rows) > CANDIDATE_LIMIT:
        return PairingDecision(ambiguous=True)
    matches = []
    for row in rows:
        other = as_leg(row, accounts)
        result = match(leg, other)
        if result:
            matches.append((other, result))
    if not matches:
        return PairingDecision()
    if len(matches) > 1:
        return PairingDecision(ambiguous=True, ambiguous_ids=tuple(other["id"] for other, _ in matches))
    counterpart, (strength, fee) = matches[0]
    competitors = opposite_candidates(cur, user_id, counterpart, gap_seconds=gap_seconds,
                                      exclude=[counterpart["id"], leg.get("id")])
    competing = [row for row in competitors if match(counterpart, as_leg(row, accounts))]
    if competing or len(competitors) > CANDIDATE_LIMIT:
        return PairingDecision(ambiguous=True, ambiguous_ids=(counterpart["id"], *(str(row["id"]) for row in competing)))
    if strength == "weak":
        same_amount = load_candidates(
            cur, user_id, direction=None, around=[leg["date"], counterpart["date"]],
            gap_seconds=settings.notification_pairing_window_seconds,
            min_amount=int(leg["amount"]), max_amount=int(leg["amount"]),
            exclude=[counterpart["id"], leg.get("id")],
        )
        if same_amount:
            # Only the two legs that would have been linked are in doubt; the other rows are not
            # evidence of a transfer, only of a repeated amount.
            return PairingDecision(ambiguous=True, ambiguous_ids=(counterpart["id"],))
    return PairingDecision(counterpart=counterpart, fee=fee)


# Two apps report one arrival within seconds (observed: 0.13 s); a minute's slack covers a slow companion.
SAME_ARRIVAL_WINDOW_SECONDS = 120

# Reports of one linked account (D3, OD129). Mirrors arrive within 0.6 s when both apps notify at once, but
# not every app notifies in real time and post times are each app's own; five minutes allows a late app,
# while the link, one direction, absorb-once and the possible-transfer check keep coincidences out.
LINKED_REPORT_WINDOW_SECONDS = 300

# The legs one second report may repeat (D2). Unlike the guard's candidates, a kept leg may already be in a
# movement: a late Jago report still repeats a GoPay credit that paired with its BCA debit meanwhile.
LINKED_REPORT_SQL = """
    SELECT t.id, t.type, t.account_id, t.amount, t.date, t.review_status, t.classified_by, t.owner_edited_at,
           t.movement_id, n.id AS event_id, n.package_name, n.post_time, n.interpretation, n.result_key,
           n.result_snapshot,
           ARRAY(SELECT d.package_name FROM notification_events d
                 WHERE d.user_id = n.user_id AND d.provenance_kind = 'duplicate_observation'
                   AND d.duplicate_of_transaction_id = t.id AND d.id <> %(event_id)s) AS absorbed_packages
    FROM notification_events n
    JOIN transactions t ON t.id = n.transaction_id AND t.user_id = n.user_id
    WHERE n.user_id = %(user_id)s AND n.processing_state = 'recorded' AND n.provenance_kind = 'observed_leg'
      AND n.id <> %(event_id)s AND t.id <> %(transaction_id)s
      AND t.owner_split_at IS NULL AND t.fee_for_movement_id IS NULL
      AND t.goal_id IS NULL AND t.obligation_id IS NULL AND t.recurring_rule_id IS NULL
      AND t.type = %(type)s AND t.amount = %(amount)s
      AND n.post_time BETWEEN %(earliest)s AND %(latest)s
    ORDER BY n.post_time, n.id
"""

# A same-amount leg in the other direction on the linked account this close to the new report may be the
# other half of a real transfer, so the link does not skip and the normal pairing decides (D2).
POSSIBLE_TRANSFER_SQL = """
    SELECT t.id FROM transactions t
    WHERE t.user_id = %(user_id)s AND t.account_id = %(account_id)s AND t.type <> %(type)s
      AND t.amount = %(amount)s AND t.movement_id IS NULL AND t.fee_for_movement_id IS NULL
      AND t.date BETWEEN %(earliest)s AND %(latest)s
    LIMIT 1
"""

# A recorded leg may already be in a movement: when the bank's report pairs it before the second app's report
# arrives, that report still repeats it (catch-second-report-after-pairing D1). Its absorbed reports keep it to
# one report per app (absorb-one-report-without-link D1).
SAME_ARRIVAL_SQL = """
    SELECT t.id, t.type, t.account_id, t.amount, t.date, t.review_status, t.classified_by, t.owner_edited_at,
           t.movement_id, n.id AS event_id, n.package_name, n.post_time, n.interpretation, n.result_key,
           n.result_snapshot,
           ARRAY(SELECT d.package_name FROM notification_events d
                 WHERE d.user_id = n.user_id AND d.provenance_kind = 'duplicate_observation'
                   AND d.duplicate_of_transaction_id = t.id AND d.id <> %(event_id)s) AS absorbed_packages
    FROM notification_events n
    JOIN transactions t ON t.id = n.transaction_id AND t.user_id = n.user_id
    WHERE n.user_id = %(user_id)s AND n.processing_state = 'recorded' AND n.provenance_kind = 'observed_leg'
      AND n.id <> %(event_id)s AND t.id <> %(transaction_id)s
      AND t.owner_split_at IS NULL AND t.fee_for_movement_id IS NULL
      AND t.goal_id IS NULL AND t.obligation_id IS NULL AND t.recurring_rule_id IS NULL
      AND t.type = %(type)s AND t.amount = %(amount)s AND t.account_id <> %(account_id)s
      AND n.post_time BETWEEN %(earliest)s AND %(latest)s
    ORDER BY n.post_time, n.id
"""


@dataclass(frozen=True)
class ArrivalDecision:
    """What the one-arrival guard found for a leg: the single other observation of the same money, or
    near misses (mirrored legs beyond the guard window, or several originals) that only flag doubt."""

    original: dict | None = None
    doubtful_ids: tuple[str, ...] = ()
    # Set when an account link decided (reconcile-linked-account-reports D2): the second report is the new
    # leg, or the recorded `original` when `replace_original` (the second report arrived first).
    reporting_link_id: str | None = None
    replace_original: bool = False


def institution_of(package_name: str | None) -> str | None:
    from app.services.notification_evidence import SUPPORTED_PACKAGES

    return SUPPORTED_PACKAGES.get(str(package_name or "").strip().lower())


def names_each_other(accounts: dict[str, dict], leg: dict, other: dict) -> bool:
    """Each leg's remote account is the other leg's account or its root (design D1)."""
    leg_remote = root_of(accounts, leg.get("remote_account_id"))
    other_remote = root_of(accounts, (other.get("interpretation") or {}).get("remote_account_id"))
    return bool(
        leg_remote and other_remote
        and leg_remote == root_of(accounts, str(other["account_id"]))
        and other_remote == root_of(accounts, str(leg["account_id"]))
    )


def same_arrival(cur, user_id: str, leg: dict, accounts: dict[str, dict]) -> ArrivalDecision:
    """The recorded observation of the same money as `leg`, reported by another institution's app.

    `leg` has `event_id`, `transaction_id` (None while not yet inserted), `type`, `amount`, `account_id`,
    `post_time`, `institution` and `remote_account_id`. An owner's account link decides first
    (linked_arrival). Otherwise a match needs the same direction and amount, a different account, another
    app, post times at most SAME_ARRIVAL_WINDOW_SECONDS apart, and both legs naming each other's account
    (or its root). Mirrored legs further apart, inside the pairing window, and a second possible original
    are doubts, never duplicates.
    """
    if not leg.get("institution"):
        return ArrivalDecision()
    linked = linked_arrival(cur, user_id, leg, accounts)
    if linked is not None:
        return linked
    window = timedelta(seconds=max(SAME_ARRIVAL_WINDOW_SECONDS, settings.notification_pairing_window_seconds))
    cur.execute(SAME_ARRIVAL_SQL, {
        "user_id": user_id, "event_id": str(leg.get("event_id") or "00000000-0000-0000-0000-000000000000"),
        "transaction_id": str(leg.get("transaction_id") or "00000000-0000-0000-0000-000000000000"),
        "type": leg["type"], "amount": int(leg["amount"]), "account_id": str(leg["account_id"]),
        "earliest": leg["post_time"] - window, "latest": leg["post_time"] + window,
    })
    rows = cur.fetchall()
    if not leg.get("remote_account_id"):
        return ArrivalDecision()
    mirrored = [
        row for row in rows
        if institution_of(row["package_name"]) not in {None, leg["institution"]} and names_each_other(accounts, leg, row)
        and not absorbed_from(row, leg["institution"])
    ]
    close = [row for row in mirrored if within_same_arrival(row, leg)]
    if len(close) == 1 and len(mirrored) == 1:
        if possible_transfer(cur, user_id, str(close[0]["account_id"]), leg):
            # Possibly the other half of a real transfer: neither held nor flagged, pairing decides (D3).
            return ArrivalDecision()
        return ArrivalDecision(original=close[0])
    return ArrivalDecision(doubtful_ids=tuple(str(row["id"]) for row in mirrored))


def possible_transfer(cur, user_id: str, account_id: str, leg: dict) -> bool:
    """Whether an unpaired leg of the same amount in the other direction is on `account_id` near `leg`."""
    near = timedelta(seconds=SAME_ARRIVAL_WINDOW_SECONDS)
    cur.execute(POSSIBLE_TRANSFER_SQL, {
        "user_id": user_id, "account_id": account_id, "type": leg["type"], "amount": int(leg["amount"]),
        "earliest": leg["post_time"] - near, "latest": leg["post_time"] + near,
    })
    return cur.fetchone() is not None


def absorbed_from(row: dict, institution: str) -> bool:
    """Whether the recorded leg already absorbed a report from `institution`: one arrival, one report per app."""
    return institution in {institution_of(package) for package in row.get("absorbed_packages") or []}


def within_same_arrival(row: dict, leg: dict) -> bool:
    return abs((row["post_time"] - leg["post_time"]).total_seconds()) <= SAME_ARRIVAL_WINDOW_SECONDS


def touches(accounts: dict[str, dict], account_id: str, leg_account_id: str | None, remote_id: str | None) -> bool:
    """Whether a leg reports the linked account: it is on it, or names it (or, for a root account, any
    account under it) as its remote."""
    if leg_account_id and str(leg_account_id) == account_id:
        return True
    if not remote_id:
        return False
    if str(remote_id) == account_id:
        return True
    linked = accounts.get(account_id)
    return bool(linked and not linked.get("parent_id") and root_of(accounts, remote_id) == account_id)


def linked_arrival(cur, user_id: str, leg: dict, accounts: dict[str, dict]) -> ArrivalDecision | None:
    """What the owner's account links decide for `leg` (D2, D4), or None when no link applies.

    A link (account X, institution I) pairs `leg` with exactly one recorded report of the same direction and
    amount from another institution, at most LINKED_REPORT_WINDOW_SECONDS away, when one of the two is on X
    and the other, from I, reports X. The report that is not on X is the second report. A kept leg absorbs
    at most one report per institution (OD133); matches further away, inside the pairing window, and several
    matches are doubts; an opposite-direction leg on X right beside the new report hands it to pairing.
    """
    from app.services.account_links import active_links

    links: dict[str, dict[str, str]] = {}
    for link in active_links(cur, user_id):
        links.setdefault(str(link["account_id"]), {})[link["institution"]] = str(link["id"])
    if not links:
        return None
    window = timedelta(seconds=max(LINKED_REPORT_WINDOW_SECONDS, settings.notification_pairing_window_seconds))
    cur.execute(LINKED_REPORT_SQL, {
        "user_id": user_id, "event_id": str(leg.get("event_id") or "00000000-0000-0000-0000-000000000000"),
        "transaction_id": str(leg.get("transaction_id") or "00000000-0000-0000-0000-000000000000"),
        "type": leg["type"], "amount": int(leg["amount"]),
        "earliest": leg["post_time"] - window, "latest": leg["post_time"] + window,
    })
    leg_account = str(leg["account_id"])
    candidates = []
    for row in cur.fetchall():
        institution = institution_of(row["package_name"])
        if institution in {None, leg["institution"]}:
            continue
        row_account = str(row["account_id"])
        row_remote = (row.get("interpretation") or {}).get("remote_account_id")
        for account_id, institutions in links.items():
            if (row_account == account_id and leg["institution"] in institutions
                    and touches(accounts, account_id, leg_account, leg.get("remote_account_id"))):
                if absorbed_from(row, leg["institution"]):
                    continue
                candidates.append((row, account_id, institutions[leg["institution"]], False))
                break
            if (leg_account == account_id and leg_account != row_account and institution in institutions
                    and touches(accounts, account_id, row_account, row_remote)):
                candidates.append((row, account_id, institutions[institution], True))
                break
    if not candidates:
        return None
    seconds = lambda row: abs((row["post_time"] - leg["post_time"]).total_seconds())  # noqa: E731
    close = [item for item in candidates if seconds(item[0]) <= LINKED_REPORT_WINDOW_SECONDS]
    if len(close) != 1:
        return ArrivalDecision(doubtful_ids=tuple(str(item[0]["id"]) for item in candidates))
    row, account_id, link_id, second_recorded_first = close[0]
    if possible_transfer(cur, user_id, account_id, leg):
        # Possibly the other half of a real transfer: neither skipped nor held, pairing decides.
        return ArrivalDecision()
    if not second_recorded_first:
        return ArrivalDecision(original=row, reporting_link_id=link_id)
    if replaceable_leg(row):
        return ArrivalDecision(original=row, reporting_link_id=link_id, replace_original=True)
    # The owner touched the earlier report: it keeps the money and the new one waits in Tinjau (OD119).
    return ArrivalDecision(original=row)


def replaceable_leg(row: dict) -> bool:
    """An observation that only the pipeline has touched, so another observation may take its place (D2).

    A leg in a movement never is: removing it would break the movement (catch-second-report-after-pairing D2).
    """
    interpretation = row.get("interpretation") or {}
    return (row.get("movement_id") is None and row["review_status"] in {"auto", "needs_review"}
            and row.get("owner_edited_at") is None and row.get("classified_by") != "owner"
            and not interpretation.get("purpose_answer"))


def keeps_new_observation(new_basis: str | None, original: dict) -> bool:
    """Whether the new observation replaces the recorded one: only a named account over a default-pocket
    fallback, and only while the recorded leg is untouched (D2, OD119)."""
    original_basis = (original.get("interpretation") or {}).get("account_basis")
    return new_basis == "named" and original_basis == "default_pocket" and replaceable_leg(original)


def mark_duplicate_observation(cur, user_id: str, event_id: str, kept_transaction_id: str,
                               kept_key: str | None, kept_snapshot: dict | None, interpretation: dict | None,
                               reporting_link_id: str | None = None) -> None:
    """Record an event as a second observation of the kept leg: no transaction, the kept leg's result.

    With `reporting_link_id` an owner's account link skipped it, so it is already reviewed and never waits
    in "Terlihat dua kali" (reconcile-linked-account-reports D2).
    """
    cur.execute(
        """UPDATE notification_events SET processing_state = 'recorded', provenance_kind = 'duplicate_observation',
               transaction_id = NULL, duplicate_of_transaction_id = %s, movement_id = NULL, confirmed_role = NULL, result_key = %s, result_snapshot = %s,
               interpretation = COALESCE(%s::jsonb, interpretation), active_question = NULL, error_code = NULL,
               reporting_link_id = %s, reviewed_at = CASE WHEN %s::uuid IS NULL THEN NULL ELSE NOW() END,
               lease_token = NULL, lease_expires_at = NULL, updated_at = NOW()
           WHERE user_id = %s AND id = %s""",
        (kept_transaction_id, kept_key, json.dumps(kept_snapshot) if kept_snapshot is not None else None,
         json.dumps(interpretation) if interpretation is not None else None, reporting_link_id, reporting_link_id,
         user_id, event_id),
    )


def refresh_duplicate_results(cur, user_id: str, transaction_ids: list[str], key: str, snapshot: dict) -> None:
    """Second observations show what their kept leg became, e.g. one leg of a movement."""
    cur.execute(
        """UPDATE notification_events SET result_key = %s, result_snapshot = %s, updated_at = NOW()
           WHERE user_id = %s AND provenance_kind = 'duplicate_observation'
             AND duplicate_of_transaction_id = ANY(%s::uuid[])""",
        (key, json.dumps(snapshot), user_id, [str(value) for value in transaction_ids]),
    )


def drop_observation(cur, user_id: str, original: dict) -> None:
    """Remove a replaced observation's transaction with its balance effect, in the caller's transaction."""
    from app.services.ledger_mutations import delete_transaction_rows

    delete_transaction_rows(cur, user_id, str(original["id"]))


def repair_duplicate_arrivals(cur, user_id: str, accounts: dict[str, dict], legs: list[dict]) -> int:
    """Apply the one-arrival rule to legs recorded before it could (the deferred pass); idempotent."""
    merged = 0
    gone: set[str] = set()
    for row in legs:
        if not row.get("event_id") or str(row["id"]) in gone:
            continue
        cur.execute(
            """SELECT n.id AS event_id, n.package_name, n.post_time, n.interpretation, n.result_key, n.result_snapshot,
                      t.id, t.type, t.account_id, t.amount, t.review_status, t.classified_by, t.owner_edited_at,
                      t.movement_id
               FROM notification_events n JOIN transactions t ON t.id = n.transaction_id AND t.user_id = n.user_id
               WHERE n.user_id = %s AND n.id = %s AND n.provenance_kind = 'observed_leg' AND t.movement_id IS NULL""",
            (user_id, row["event_id"]),
        )
        current = cur.fetchone()
        if not current:
            continue
        leg = {
            "event_id": str(current["event_id"]), "transaction_id": str(current["id"]), "type": current["type"],
            "amount": current["amount"], "account_id": str(current["account_id"]), "post_time": current["post_time"],
            "institution": institution_of(current["package_name"]),
            "remote_account_id": (current["interpretation"] or {}).get("remote_account_id"),
        }
        decision = same_arrival(cur, user_id, leg, accounts)
        original = decision.original
        if not original:
            continue
        if original["post_time"] > current["post_time"] and original["movement_id"] is None:
            # Each pair is decided once, from its later observation. A paired original is never visited,
            # so its pair is decided from the unpaired leg (catch-second-report-after-pairing D4).
            continue
        if decision.reporting_link_id and decision.replace_original:
            # The owner's link keeps this later report and skips the second report recorded earlier.
            dropped, kept = original, current
        elif decision.reporting_link_id and replaceable_leg(current):
            dropped, kept = current, original
        elif decision.reporting_link_id:
            continue
        elif keeps_new_observation((current["interpretation"] or {}).get("account_basis"), original):
            dropped, kept = original, current
        elif replaceable_leg(current):
            dropped, kept = current, original
        else:
            continue
        drop_observation(cur, user_id, dropped)
        mark_duplicate_observation(cur, user_id, str(dropped["event_id"]), str(kept["id"]),
                                   kept["result_key"], kept["result_snapshot"], None,
                                   reporting_link_id=decision.reporting_link_id)
        gone.add(str(dropped["id"]))
        merged += 1
    return merged


def flag_ambiguous(cur, user_id: str, transaction_ids: list[str]) -> int:
    """Flag automatically classified legs for review with the reason ambiguous_movement; returns rows flagged.

    Rows the owner already reviewed keep their status, so a repeated pass never reopens them.
    """
    ids = sorted({str(value) for value in transaction_ids if value})
    if not ids:
        return 0
    cur.execute(
        """UPDATE transactions SET review_status = 'needs_review'
           WHERE user_id = %s AND id = ANY(%s::uuid[]) AND movement_id IS NULL AND review_status = 'auto'
           RETURNING id""",
        (user_id, ids),
    )
    flagged = len(cur.fetchall())
    cur.execute(
        """UPDATE notification_events
           SET interpretation = jsonb_set(interpretation, '{review_reasons}',
                   COALESCE(interpretation -> 'review_reasons', '[]'::jsonb) || '["ambiguous_movement"]'::jsonb)
           WHERE user_id = %s AND transaction_id = ANY(%s::uuid[]) AND provenance_kind = 'observed_leg'
             AND jsonb_typeof(interpretation) = 'object'
             AND NOT (COALESCE(interpretation -> 'review_reasons', '[]'::jsonb) @> '["ambiguous_movement"]'::jsonb)""",
        (user_id, ids),
    )
    return flagged


def ensure_fee_category(cur, user_id: str) -> dict:
    cur.execute(
        """INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget)
           VALUES (%s, %s, 'receipt', '#64748b', 'expense', 'need', FALSE)
           ON CONFLICT (user_id, name, kind) DO UPDATE SET name = EXCLUDED.name
           RETURNING id, kakeibo_type""",
        (user_id, FEE_CATEGORY_NAME),
    )
    return cur.fetchone()


def link_pair(cur, user_id: str, expense_id: str, income_id: str, *, fee: int, notes: str | None) -> dict:
    """Link two legs as one movement; with a fee, the outbound leg sheds the fee into its own expense row.

    Both legs are locked in id order before any change, as `link_transactions_as_movement` does, so
    concurrent pairing of the same rows serialises instead of deadlocking.
    """
    cur.execute(
        """SELECT id, type, account_id, amount, date, movement_id FROM transactions
           WHERE user_id = %s AND id = ANY(%s::uuid[]) ORDER BY id FOR UPDATE""",
        (user_id, [expense_id, income_id]),
    )
    rows = {str(row["id"]): row for row in cur.fetchall()}
    if len(rows) != 2 or any(row["movement_id"] for row in rows.values()):
        raise HTTPException(status_code=409, detail="A leg is already linked or was removed")
    expense, income = rows[expense_id], rows[income_id]
    if transfer_fee(expense["amount"], income["amount"]) != fee:
        raise HTTPException(status_code=409, detail="The legs changed since they were matched")
    if fee:
        cur.execute(
            "UPDATE transactions SET amount = amount - %s, updated_at = NOW() WHERE user_id = %s AND id = %s",
            (fee, user_id, expense_id),
        )
    result = link_transactions_as_movement(cur, user_id, expense_id, income_id, notes=notes)
    fee_transaction_id = None
    if fee:
        category = ensure_fee_category(cur, user_id)
        # The fee takes the category's pillar; a category without one leaves it unclassified, never `need`.
        cur.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, kakeibo_type,
                   idempotency_key, fee_for_movement_id, source, review_status, classified_by, classification_confidence)
               VALUES (%s, %s, %s, 'expense', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (user_id, expense["account_id"], category["id"], fee, FEE_NOTES, expense["date"],
             category["kakeibo_type"], f"movement-fee:{result['movement_id']}", result["movement_id"],
             "movement", "auto", "system", None),
        )
        fee_transaction_id = str(cur.fetchone()["id"])
    # The movement settles the legs' classification doubts (record-and-flag D1 of phase 3).
    cur.execute(
        """UPDATE transactions SET review_status = 'auto'
           WHERE user_id = %s AND movement_id = %s AND review_status = 'needs_review'
             AND classified_by IS DISTINCT FROM 'owner'""",
        (user_id, result["movement_id"]),
    )
    # A movement leg is classified by the pairing, no longer by a spending habit.
    cur.execute(
        """UPDATE transactions SET classified_by = 'system'
           WHERE user_id = %s AND movement_id = %s AND classified_by = 'habit'""",
        (user_id, result["movement_id"]),
    )
    return {**result, "fee": fee, "fee_transaction_id": fee_transaction_id}


def undo_movement_fee(cur, user_id: str, movement_id: str, outbound_id: str) -> int:
    """Return a movement's fee to its outbound leg and remove the fee row; returns the fee restored.

    A credit bill's fee sits on the credit account, not the outbound leg: it stays as the owner's own expense
    and only loses its tie to the movement (add-credit-accounts D6).
    """
    cur.execute(
        """UPDATE transactions SET fee_for_movement_id = NULL, updated_at = NOW()
           WHERE user_id = %s AND fee_for_movement_id = %s AND movement_id IS NULL AND type = 'expense'
             AND account_id <> (SELECT account_id FROM transactions WHERE user_id = %s AND id = %s)""",
        (user_id, movement_id, user_id, outbound_id),
    )
    cur.execute(
        """DELETE FROM transactions
           WHERE user_id = %s AND fee_for_movement_id = %s AND movement_id IS NULL AND type = 'expense'
           RETURNING amount""",
        (user_id, movement_id),
    )
    row = cur.fetchone()
    if not row:
        return 0
    cur.execute(
        "UPDATE transactions SET amount = amount + %s, updated_at = NOW() WHERE user_id = %s AND id = %s",
        (row["amount"], user_id, outbound_id),
    )
    return int(row["amount"])


def attach_events_to_movement(cur, user_id: str, movement_id: str, accounts: dict[str, dict]) -> None:
    """Show a movement linked after recording as one movement in each leg's notification result."""
    from app.services.notification_application import account_label, committed_snapshot

    cur.execute(
        """SELECT id, type, account_id, amount, notes FROM transactions
           WHERE user_id = %s AND movement_id = %s""",
        (user_id, movement_id),
    )
    legs = {row["type"]: row for row in cur.fetchall()}
    cur.execute(
        """SELECT id, transaction_id, result_key FROM notification_events
           WHERE user_id = %s AND transaction_id = ANY(%s::uuid[]) AND provenance_kind = 'observed_leg'
             AND processing_state = 'recorded'
           ORDER BY post_time, id FOR UPDATE""",
        (user_id, [str(leg["id"]) for leg in legs.values()]),
    )
    events = cur.fetchall()
    if not events:
        return
    listed = list(accounts.values())
    source, target = accounts[str(legs["expense"]["account_id"])], accounts[str(legs["income"]["account_id"])]
    key = events[0]["result_key"] or f"notification:{events[0]['id']}"
    snapshot = committed_snapshot(
        key=key, kind="internal_movement", description=legs["expense"]["notes"] or "Pindah saldo",
        amount=int(legs["expense"]["amount"]), source=account_label(source, listed), target=account_label(target, listed),
    )
    refresh_duplicate_results(cur, user_id, [str(leg["id"]) for leg in legs.values()], key, snapshot)
    for event in events:
        role = "outbound" if str(event["transaction_id"]) == str(legs["expense"]["id"]) else "inbound"
        cur.execute(
            """UPDATE notification_events SET movement_id = %s, confirmed_role = %s, result_key = %s,
                   result_snapshot = %s, updated_at = NOW(),
                   active_question = CASE WHEN active_question ->> 'type' = 'purpose' THEN NULL
                                          ELSE active_question END
               WHERE user_id = %s AND id = %s""",
            (movement_id, role, key, json.dumps(snapshot), user_id, event["id"]),
        )


def pair_legs(cur, user_id: str, leg: dict, counterpart: dict, fee: int, accounts: dict[str, dict]) -> dict:
    expense, income = (leg, counterpart) if leg["type"] == "expense" else (counterpart, leg)
    # As in an owner merge, notification wording describes one leg; owner-written notes are kept.
    from_notification = bool(leg.get("event_id") or counterpart.get("event_id"))
    notes = movement_note(cur, user_id, str(income["account_id"])) if from_notification else None
    result = link_pair(cur, user_id, expense["id"], income["id"], fee=fee, notes=notes)
    attach_events_to_movement(cur, user_id, result["movement_id"], accounts)
    return result


def pair_answered_transfer(cur, user_id: str, transaction_id: str, recorder=None) -> bool:
    """Link a row the owner just filed as Internal Movement with its unique counterpart, if any (D4).

    `recorder` is the answer's undo record; it captures the counterpart before the link, so undoing the
    answer also undoes the movement.
    """
    accounts = load_accounts(cur, user_id)
    cur.execute(
        CANDIDATE_SQL.format(filters="AND t.id = %(transaction_id)s"),
        {"user_id": user_id, "direction": None, "exclude": [], "transaction_id": transaction_id, "limit": 1},
    )
    row = cur.fetchone()
    if not row:
        return False
    leg = as_leg(row, accounts)
    decision = find_counterpart(cur, user_id, leg, gap_seconds=settings.notification_pairing_window_seconds,
                                accounts=accounts)
    if not decision.counterpart:
        return False
    if recorder is not None:
        recorder.capture([decision.counterpart["id"]])
    pair_legs(cur, user_id, leg, decision.counterpart, decision.fee, accounts)
    return True


def await_counterpart(cur, user_id: str, transaction_id: str) -> None:
    """Keep a one-sided transfer in Tinjau until its other side is linked."""
    cur.execute(
        "UPDATE transactions SET review_status = 'needs_review', updated_at = NOW() WHERE user_id = %s AND id = %s",
        (user_id, transaction_id),
    )
    cur.execute(
        """UPDATE notification_events
           SET interpretation = jsonb_set(interpretation, '{review_reasons}',
                   COALESCE(interpretation -> 'review_reasons', '[]'::jsonb) || '["awaiting_counterpart"]'::jsonb)
           WHERE user_id = %s AND transaction_id = %s AND provenance_kind = 'observed_leg'
             AND jsonb_typeof(interpretation) = 'object'
             AND NOT (COALESCE(interpretation -> 'review_reasons', '[]'::jsonb) @> '["awaiting_counterpart"]'::jsonb)""",
        (user_id, transaction_id),
    )


def recent_unpaired_legs(cur, user_id: str, since: datetime) -> list[dict]:
    cur.execute(
        CANDIDATE_SQL.format(filters="AND t.created_at >= %(since)s"),
        {"user_id": user_id, "direction": None, "exclude": [], "since": since, "limit": REPAIR_LEG_LIMIT},
    )
    return cur.fetchall()


def repair_owner(user_id: str, since: datetime) -> dict[str, int]:
    counts = {"paired": 0, "with_fee": 0, "ambiguous": 0, "skipped": 0, "duplicates": 0}
    with db_conn() as conn:
        with conn.cursor() as cur:
            # The same per-owner lock as notification processing: one pairing decision at a time.
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
            accounts = load_accounts(cur, user_id)
            linked: set[str] = set()
            legs = recent_unpaired_legs(cur, user_id, since)
            # A second app's report of the same money goes before pairing, so it can't make a pair ambiguous.
            counts["duplicates"] = repair_duplicate_arrivals(cur, user_id, accounts, legs)
            if counts["duplicates"]:
                legs = recent_unpaired_legs(cur, user_id, since)
            for row in legs:
                if str(row["id"]) in linked:
                    continue
                leg = as_leg(row, accounts)
                decision = find_counterpart(cur, user_id, leg, gap_seconds=settings.movement_repair_max_gap_seconds,
                                            accounts=accounts)
                if decision.counterpart:
                    try:
                        with conn.transaction():
                            pair_legs(cur, user_id, leg, decision.counterpart, decision.fee, accounts)
                    except HTTPException:
                        counts["skipped"] += 1
                        continue
                    linked.update({leg["id"], decision.counterpart["id"]})
                    counts["paired"] += 1
                    counts["with_fee"] += 1 if decision.fee else 0
                elif decision.ambiguous:
                    counts["ambiguous"] += flag_ambiguous(cur, user_id, [leg["id"], *decision.ambiguous_ids])
        conn.commit()
    return counts


def repair_unpaired_movements(*, now: datetime | None = None) -> dict[str, int]:
    """Re-pair unpaired legs recorded within the lookback; idempotent and safe to run concurrently."""
    totals = {"paired": 0, "with_fee": 0, "ambiguous": 0, "skipped": 0, "duplicates": 0}
    if settings.movement_pairing_configuration_error:
        return totals
    since = (now or datetime.now(timezone.utc)) - timedelta(hours=settings.movement_repair_lookback_hours)
    with db_conn() as conn:
        owners = conn.execute(
            """SELECT DISTINCT user_id FROM transactions
               WHERE movement_id IS NULL AND owner_split_at IS NULL AND created_at >= %s
                 AND type IN ('expense', 'income') AND source IN ('notification', 'manual')""",
            (since,),
        ).fetchall()
    for owner in owners:
        for name, value in repair_owner(str(owner["user_id"]), since).items():
            totals[name] += value
    return totals


def complete_movement(
    cur, user_id: str, transaction_id: str, *, counterpart_account_id: str, role: str, create_new: bool = False,
    recorder=None,
) -> dict:
    """Complete a one-sided own transfer on the account the owner names, and link both legs as one movement.

    An unlinked opposite row of the same amount near that date on the named account is the missing leg:
    one clean Internal Movement match is linked; any other match is returned for the owner to choose
    (409 counterpart_candidates) unless `create_new` confirms that a new leg is wanted.

    `role` is the side the owner names: "target" for an outbound leg, "source" for an inbound one.
    Idempotent: repeating the same completion returns the existing movement.
    """
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
    cur.execute(
        """SELECT t.*, c.name AS category_name, c.system_key AS category_system_key FROM transactions t
           LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
           WHERE t.user_id = %s AND t.id = %s FOR UPDATE OF t""",
        (user_id, transaction_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")
    if recorder is not None:
        # The Tinjau undo record (redesign-review-triage D10): the leg's state before anything is written.
        recorder.capture([transaction_id])
    expected_role = {"expense": "target", "income": "source"}.get(row["type"])
    if expected_role != role:
        raise HTTPException(status_code=422, detail={
            "code": "counterpart_side_mismatch",
            "message": "An outgoing leg needs target_account_id; an incoming leg needs source_account_id.",
        })
    completion_key = f"{COMPLETION_KEY_PREFIX}{transaction_id}"
    if row["movement_id"]:
        cur.execute(
            """SELECT id, account_id, idempotency_key FROM transactions
               WHERE user_id = %s AND movement_id = %s AND id <> %s""",
            (user_id, row["movement_id"], transaction_id),
        )
        partner = cur.fetchone()
        # Either a leg created by an earlier completion or an existing row linked by it.
        if partner and str(partner["account_id"]) == counterpart_account_id:
            response = completion_response(row, partner, idempotent=True)
            if partner["idempotency_key"] != completion_key:
                response = {**response, "created_transaction_id": None, "linked_transaction_id": str(partner["id"])}
            return response
        raise HTTPException(status_code=409, detail={"code": "already_linked", "message": "The transaction is already part of a movement."})
    if not is_movement(row["category_system_key"]):
        raise HTTPException(status_code=422, detail={
            "code": "not_awaiting_counterpart",
            "message": "Only an unlinked Internal Movement transaction can be completed.",
        })
    if row["goal_id"] or row["obligation_id"] or row["recurring_rule_id"] or row["fee_for_movement_id"]:
        raise HTTPException(status_code=409, detail={"code": "transaction_not_completable", "message": "A linked transaction cannot be completed."})
    own_account_id = str(row["account_id"])
    if counterpart_account_id == own_account_id:
        raise HTTPException(status_code=422, detail={"code": "same_account", "message": "Accounts must be different"})
    locked = lock_owned_accounts(cur, user_id, [own_account_id, counterpart_account_id])
    other = locked[counterpart_account_id]
    ensure_generic_movement_accounts(locked[own_account_id], other)
    if any(account["type"] not in MOVEMENT_ACCOUNT_TYPES for account in locked.values()):
        raise HTTPException(status_code=422, detail={"code": "account_not_liquid", "message": "Both accounts must be liquid"})
    if other["has_children"]:
        raise HTTPException(status_code=422, detail={"code": "account_has_pockets", "message": "Choose a pocket, not its parent account"})

    expense_category_id, income_category_id = ensure_internal_movement_categories(cur, user_id)
    new_type = "income" if row["type"] == "expense" else "expense"
    target_id = counterpart_account_id if row["type"] == "expense" else own_account_id
    notes = movement_note(cur, user_id, target_id)
    candidates = existing_counterparts(cur, user_id, row, counterpart_account_id, new_type)
    if not create_new and candidates:
        only = candidates[0]
        if len(candidates) == 1 and is_movement(only["category_system_key"]) and only["owner_split_at"] is None:
            if recorder is not None:
                recorder.capture([str(only["id"])])
            return link_existing_counterpart(cur, user_id, row, only, notes)
        raise HTTPException(status_code=409, detail={
            "code": "counterpart_candidates",
            "message": "The chosen account already has a matching transaction; link it or confirm a new one.",
            "candidates": [candidate_summary(candidate) for candidate in candidates],
        })
    cur.execute(
        """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, idempotency_key,
               source, review_status, classified_by, classification_confidence)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
        (user_id, counterpart_account_id, income_category_id if new_type == "income" else expense_category_id,
         new_type, row["amount"], notes, row["date"], completion_key, *system_provenance("movement").columns()),
    )
    partner = cur.fetchone()
    expense_id, income_id = (transaction_id, str(partner["id"])) if row["type"] == "expense" else (str(partner["id"]), transaction_id)
    result = link_transactions_as_movement(cur, user_id, expense_id, income_id, notes=notes)
    # Completing is the owner's review of the transfer; the classification itself stays the system's.
    cur.execute(
        "UPDATE transactions SET review_status = 'reviewed', updated_at = NOW() WHERE user_id = %s AND movement_id = %s",
        (user_id, result["movement_id"]),
    )
    attach_events_to_movement(cur, user_id, result["movement_id"], load_accounts(cur, user_id))
    row = {**row, "movement_id": result["movement_id"]}
    return completion_response(row, partner, idempotent=False)


def existing_counterparts(cur, user_id: str, row: dict, account_id: str, counterpart_type: str) -> list[dict]:
    cur.execute(
        """SELECT t.*, c.name AS category_name, c.system_key AS category_system_key FROM transactions t
           LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
           WHERE t.user_id = %s AND t.account_id = %s AND t.type = %s AND t.amount = %s
             AND t.movement_id IS NULL AND t.fee_for_movement_id IS NULL
             AND t.goal_id IS NULL AND t.obligation_id IS NULL AND t.recurring_rule_id IS NULL
             AND t.date BETWEEN %s AND %s AND t.id <> %s
           ORDER BY abs(extract(epoch FROM t.date - %s)), t.id
           LIMIT 5
           FOR UPDATE OF t""",
        (user_id, account_id, counterpart_type, row["amount"], row["date"] - COMPLETION_MATCH_WINDOW,
         row["date"] + COMPLETION_MATCH_WINDOW, row["id"], row["date"]),
    )
    return cur.fetchall()


def candidate_summary(candidate: dict) -> dict[str, Any]:
    return {
        "id": str(candidate["id"]), "type": candidate["type"], "amount": int(candidate["amount"]),
        "date": candidate["date"].isoformat(), "account_id": str(candidate["account_id"]),
        "category_name": candidate["category_name"], "notes": candidate["notes"],
    }


def link_existing_counterpart(cur, user_id: str, row: dict, partner: dict, notes: str | None) -> dict:
    expense_id, income_id = (str(row["id"]), str(partner["id"])) if row["type"] == "expense" else (str(partner["id"]), str(row["id"]))
    result = link_transactions_as_movement(cur, user_id, expense_id, income_id, notes=notes)
    cur.execute(
        "UPDATE transactions SET review_status = 'reviewed', updated_at = NOW() WHERE user_id = %s AND movement_id = %s",
        (user_id, result["movement_id"]),
    )
    attach_events_to_movement(cur, user_id, result["movement_id"], load_accounts(cur, user_id))
    response = completion_response({**row, "movement_id": result["movement_id"]}, partner, idempotent=False)
    return {**response, "created_transaction_id": None, "linked_transaction_id": str(partner["id"])}


def completion_response(row: dict, partner: dict, *, idempotent: bool) -> dict[str, Any]:
    expense_id, income_id = (row["id"], partner["id"]) if row["type"] == "expense" else (partner["id"], row["id"])
    return {
        "ok": True, "movement_id": str(row["movement_id"]),
        "expense_transaction_id": str(expense_id), "income_transaction_id": str(income_id),
        "created_transaction_id": str(partner["id"]), "idempotent": idempotent,
    }
