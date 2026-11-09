"""Manual balance check, "Cocokkan saldo" (manual-balance-check D1–D6).

The owner types the balance the bank app shows. The check compares it with the recorded balance and
lists likely causes found in the ledger and in notification reports since the last known-good point.
It is deterministic and read-only: nothing here writes, and nothing calls a model. Each cause carries
the signed `effect` fixing it would have on the recorded balance, so the causes that explain a
difference `d = actual - recorded` are those whose effects sum to `d`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any


from app.errors import ErrorCode, api_error
from app.services.ledger_mutations import LIQUID_ACCOUNT_TYPES
from app.services.notification_evidence import SUPPORTED_PACKAGES, collect_facts
from app.services.notification_resolution import INSTITUTION_LABELS
from app.services.system_categories import is_movement

WINDOW_WITHOUT_ANCHOR = timedelta(days=30)  # OD140
DUPLICATE_GAP_SECONDS = 300  # OD141: wider than the 120 s guard because the check only suggests
CAPTURE_GAP = timedelta(hours=12)  # OD142
CANDIDATE_LIMIT = 20
EXPLANATION_MAX_SIZE = 3
WAITING_STATES = ("needs_review", "needs_confirmation", "failed")
PENDING_STATES = ("queued", "processing")


@dataclass(frozen=True)
class CheckScope:
    account: dict[str, Any]
    account_ids: list[str]
    is_parent: bool


def load_scope(cur, user_id: str, account_id: str) -> CheckScope:
    """The account and, for a parent, its pockets (D2). Products and amount-tracked rows are refused."""
    cur.execute(
        """SELECT id, name, type, instrument_type, investment_tracking_mode, parent_id
           FROM accounts WHERE user_id = %s AND (id = %s OR parent_id = %s) AND is_archived = FALSE""",
        (user_id, account_id, account_id),
    )
    rows = cur.fetchall()
    account = next((row for row in rows if str(row["id"]) == account_id), None)
    if account is None:
        raise api_error(404, ErrorCode.ACCOUNT_NOT_FOUND)
    for row in rows:
        liquid = row["type"] in LIQUID_ACCOUNT_TYPES and not row.get("instrument_type")
        if not liquid or row.get("investment_tracking_mode") == "amount":
            raise api_error(409, ErrorCode.BALANCE_CHECK_NOT_LIQUID)
    pockets = [str(row["id"]) for row in rows if str(row["id"]) != account_id]
    return CheckScope(account=account, account_ids=[account_id, *pockets], is_parent=bool(pockets))


def recorded_balance(cur, user_id: str, account_ids: list[str]) -> int:
    """Initial balances plus income minus expense over the scope, each row counted once."""
    cur.execute(
        """SELECT COALESCE(SUM(a.initial_balance), 0)
                  + COALESCE((SELECT SUM(CASE WHEN t.type = 'income' THEN t.amount ELSE -t.amount END)
                              FROM transactions t
                              WHERE t.user_id = %s AND t.account_id = ANY(%s::uuid[])
                                AND t.type IN ('income', 'expense')), 0) AS balance
           FROM accounts a WHERE a.user_id = %s AND a.id = ANY(%s::uuid[])""",
        (user_id, account_ids, user_id, account_ids),
    )
    return int(cur.fetchone()["balance"] or 0)


def window_start(cur, user_id: str, scope: CheckScope, now: datetime) -> dict[str, Any]:
    """The newest of the last matched point and the last adjustment, else the last 30 days (D3)."""
    cur.execute(
        """SELECT MAX(matched_at) AS at FROM account_balance_matches WHERE user_id = %s AND account_id = %s""",
        (user_id, str(scope.account["id"])),
    )
    matched = cur.fetchone()["at"]
    cur.execute(
        """SELECT MAX(date) AS at FROM transactions
           WHERE user_id = %s AND account_id = ANY(%s::uuid[]) AND source = 'reconcile'""",
        (user_id, scope.account_ids),
    )
    adjusted = cur.fetchone()["at"]
    anchors = [(at, basis) for at, basis in ((matched, "matched"), (adjusted, "adjusted")) if at is not None]
    if not anchors:
        return {"at": now - WINDOW_WITHOUT_ANCHOR, "basis": "window"}
    at, basis = max(anchors, key=lambda pair: pair[0])
    return {"at": at, "basis": basis}


def _account_label(name: str | None, parent: str | None) -> str | None:
    if not name:
        return None
    return f"{parent} · {name}" if parent else name


def _app_label(package: str | None) -> str | None:
    institution = SUPPORTED_PACKAGES.get(str(package or "").strip().lower())
    return INSTITUTION_LABELS.get(institution) if institution else None


def _removal_effect(row_type: str, amount: int) -> int:
    """Removing a recorded row moves the balance against its type."""
    return -amount if row_type == "income" else amount


def _recording_effect(direction: str | None, amount: int | None) -> int | None:
    """Recording a report that booked nothing moves the balance with its direction."""
    if amount is None or direction not in ("income", "expense"):
        return None
    return amount if direction == "income" else -amount


ROW_CANDIDATES_SQL = """
    SELECT t.id, t.account_id, t.type, t.amount, t.date, t.merchant, t.notes, t.source,
           t.movement_id, t.fee_for_movement_id, c.name AS category, c.system_key AS category_system_key,
           a.name AS account_name, p.name AS parent_name, e.package_name,
           twin.id AS twin_id, twin.account_id AS twin_account_id, twin.date AS twin_date,
           twin.movement_id AS twin_movement_id, twin.account_name AS twin_account_name,
           twin.parent_name AS twin_parent_name, twin.package_name AS twin_package
    FROM transactions t
    JOIN accounts a ON a.id = t.account_id
    LEFT JOIN accounts p ON p.id = a.parent_id
    LEFT JOIN categories c ON c.id = t.category_id
    LEFT JOIN LATERAL (
        SELECT n.package_name FROM notification_events n
        WHERE n.user_id = t.user_id AND n.transaction_id = t.id ORDER BY n.post_time LIMIT 1
    ) e ON TRUE
    LEFT JOIN LATERAL (
        SELECT s.id, s.account_id, s.date, s.movement_id, sa.name AS account_name, sp.name AS parent_name,
               (SELECT n.package_name FROM notification_events n
                WHERE n.user_id = s.user_id AND n.transaction_id = s.id ORDER BY n.post_time LIMIT 1) AS package_name
        FROM transactions s
        JOIN accounts sa ON sa.id = s.account_id
        LEFT JOIN accounts sp ON sp.id = sa.parent_id
        WHERE s.user_id = t.user_id AND s.id <> t.id AND s.type = t.type AND s.amount = t.amount
          AND s.source <> 'reconcile'
          AND s.date BETWEEN t.date - make_interval(secs => %(gap)s) AND t.date + make_interval(secs => %(gap)s)
        ORDER BY ABS(EXTRACT(EPOCH FROM s.date - t.date)), s.id
        LIMIT 1
    ) twin ON TRUE
    WHERE t.user_id = %(user_id)s AND t.account_id = ANY(%(account_ids)s::uuid[])
      AND t.date >= %(since)s AND t.type IN ('income', 'expense')
      AND t.source <> 'reconcile' AND t.movement_id IS NULL AND t.fee_for_movement_id IS NULL
    ORDER BY t.date DESC, t.id
"""


def row_candidates(cur, user_id: str, scope: CheckScope, since: datetime) -> list[dict[str, Any]]:
    """Possible double bookings and unpaired movement legs on the scope (D4)."""
    cur.execute(ROW_CANDIDATES_SQL, {
        "user_id": user_id, "account_ids": scope.account_ids, "since": since, "gap": DUPLICATE_GAP_SECONDS,
    })
    in_scope = set(scope.account_ids)
    candidates = []
    for row in cur.fetchall():
        twin_id = row["twin_id"]
        if twin_id is not None:
            twin_in_scope = str(row["twin_account_id"]) in in_scope and row["twin_movement_id"] is None
            # A pair inside the scope is one cause: report only its later row, so the effect counts once.
            if twin_in_scope and (row["twin_date"], str(twin_id)) > (row["date"], str(row["id"])):
                twin_id = None
        if twin_id is None and not is_movement(row["category_system_key"]):
            continue
        kind = "duplicate" if twin_id is not None else "unpaired"
        candidate = {
            "key": f"tx:{row['id']}",
            "kind": kind,
            "effect": _removal_effect(row["type"], int(row["amount"])),
            "amount": int(row["amount"]),
            "direction": row["type"],
            "at": row["date"].isoformat(),
            "label": row["merchant"] or row["notes"] or row["category"] or None,
            "account": _account_label(row["account_name"], row["parent_name"]),
            "app": _app_label(row["package_name"]),
            "link": {"kind": "transaction", "id": str(row["id"])},
            "twin": None,
        }
        if kind == "duplicate":
            candidate["twin"] = {
                "transaction_id": str(twin_id),
                "account": _account_label(row["twin_account_name"], row["twin_parent_name"]),
                "app": _app_label(row["twin_package"]),
                "at": row["twin_date"].isoformat(),
            }
        candidates.append(candidate)
    return candidates


REPORT_CANDIDATES_SQL = """
    SELECT n.* FROM notification_events n
    WHERE n.user_id = %(user_id)s AND n.post_time >= %(since)s AND n.transaction_id IS NULL
      AND n.is_financial IS DISTINCT FROM FALSE
      AND (
          (n.provenance_kind = 'duplicate_observation'
           AND (n.reviewed_at IS NULL OR n.reporting_link_id IS NOT NULL OR n.skip_pattern_id IS NOT NULL))
          OR (n.provenance_kind IS DISTINCT FROM 'duplicate_observation'
              AND n.processing_state = ANY(%(states)s))
      )
    ORDER BY n.post_time DESC, n.id
    LIMIT 200
"""


def _report_kind(event: dict[str, Any]) -> str:
    if event.get("provenance_kind") == "duplicate_observation":
        # A linked account skips a second report; older skips came from retired skip patterns.
        skipped = event.get("reporting_link_id") or event.get("skip_pattern_id")
        return "skipped" if skipped else "held"
    return "pending" if event.get("processing_state") in PENDING_STATES else "unrecorded"


def _report_link(kind: str) -> dict[str, Any]:
    folder = {"held": "dua", "skipped": "lewat"}.get(kind)
    return {"kind": "review", "folder": folder}


def report_candidates(cur, user_id: str, scope: CheckScope, since: datetime) -> tuple[list[dict], list[dict]]:
    """Reports that booked nothing: held, skipped, waiting and still processing (D4).

    A report whose account is known to be in the scope is a candidate; one whose account is unknown is
    listed apart and never used in an explanation, since it may belong to any account.
    """
    cur.execute(REPORT_CANDIDATES_SQL, {
        "user_id": user_id, "since": since, "states": [*WAITING_STATES, *PENDING_STATES],
    })
    in_scope = set(scope.account_ids)
    attributed, unattributed = [], []
    for event in cur.fetchall():
        interpretation = event.get("interpretation") or {}
        account_id = interpretation.get("account_id")
        if account_id is not None and str(account_id) not in in_scope:
            continue
        facts = collect_facts(event)
        direction = interpretation.get("direction") or facts.direction
        kind = _report_kind(event)
        item = {
            "key": f"ev:{event['id']}",
            "kind": kind,
            "effect": _recording_effect(direction, facts.amount),
            "amount": facts.amount,
            "direction": direction if direction in ("income", "expense") else None,
            "at": event["post_time"].isoformat(),
            "label": None,
            "account": None,
            "app": _app_label(event.get("package_name")),
            "link": _report_link(kind),
            "twin": None,
        }
        (attributed if account_id is not None else unattributed).append(item)
    return attributed, unattributed


def explain(candidates: list[dict[str, Any]], difference: int) -> list[str] | None:
    """The smallest set of up to three causes whose effects sum exactly to the difference (D5).

    Candidates arrive newest first, so among sets of one size the first found holds the newest rows.
    """
    if difference == 0:
        return None
    usable = [candidate for candidate in candidates if candidate["effect"] is not None]
    for size in range(1, EXPLANATION_MAX_SIZE + 1):
        for chosen in combinations(usable, size):
            if sum(candidate["effect"] for candidate in chosen) == difference:
                return [candidate["key"] for candidate in chosen]
    return None


def check_balance(cur, user_id: str, account_id: str, actual_balance: int, now: datetime | None = None) -> dict:
    """The whole read-only check for one account (D1)."""
    now = now or datetime.now(timezone.utc)
    scope = load_scope(cur, user_id, account_id)
    recorded = recorded_balance(cur, user_id, scope.account_ids)
    since = window_start(cur, user_id, scope, now)
    rows = row_candidates(cur, user_id, scope, since["at"])
    reports, unattributed = report_candidates(cur, user_id, scope, since["at"])
    candidates = sorted([*rows, *reports], key=lambda item: item["at"], reverse=True)[:CANDIDATE_LIMIT]
    difference = actual_balance - recorded
    cur.execute("SELECT MAX(post_time) AS at FROM notification_events WHERE user_id = %s", (user_id,))
    last_report = cur.fetchone()["at"]
    return {
        "account": {"id": account_id, "name": scope.account["name"], "is_parent": scope.is_parent},
        "actual_balance": actual_balance,
        "recorded_balance": recorded,
        "difference": difference,
        "since": {"at": since["at"].isoformat(), "basis": since["basis"]},
        "candidates": candidates,
        "unattributed": unattributed[:CANDIDATE_LIMIT],
        "explanation": explain(candidates, difference),
        "last_report_at": last_report.isoformat() if last_report else None,
        "capture_gap": bool(last_report and now - last_report > CAPTURE_GAP),
        "can_adjust": not scope.is_parent,
    }


def record_match(cur, user_id: str, account_id: str, actual_balance: int) -> dict:
    """Store a matched point when the typed balance equals the recorded one (D6, OD143)."""
    scope = load_scope(cur, user_id, account_id)
    cur.execute(
        "SELECT id FROM accounts WHERE user_id = %s AND id = ANY(%s::uuid[]) ORDER BY id FOR UPDATE",
        (user_id, scope.account_ids),
    )
    recorded = recorded_balance(cur, user_id, scope.account_ids)
    if recorded != actual_balance:
        raise api_error(409, ErrorCode.BALANCE_MISMATCH, extra={"difference": actual_balance - recorded})
    cur.execute(
        """INSERT INTO account_balance_matches (user_id, account_id, balance)
           VALUES (%s, %s, %s) RETURNING matched_at""",
        (user_id, account_id, actual_balance),
    )
    return {"matched_at": cur.fetchone()["matched_at"].isoformat(), "balance": actual_balance}
