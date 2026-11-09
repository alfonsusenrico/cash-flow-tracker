"""Server-side undo of Tinjau review actions (redesign-review-triage D9–D17).

Each covered action records, in its own database transaction, the before and after images of what it
changed, and returns a single-use token. Undo restores the before images within ten minutes, but only
while everything the action touched still equals its after image; anything else is a conflict and
nothing is written. Learning stays honest: undo appends `undo` feedback rows instead of changing labels,
restores memory only while unchanged, and removes the top-up answers the action stored.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from app.db.pool import db_conn
from app.errors import api_error
from app.services.merchant_memory import jsonable, memory_row, merchant_match_key, restore_memory

logger = logging.getLogger(__name__)

UNDO_WINDOW = timedelta(minutes=10)
RETENTION = timedelta(hours=24)
SWEEP_LIMIT = 500
TOKEN_BYTES = 32
TOKEN_PATTERN = r"^[A-Za-z0-9_-]{43}$"

KINDS = frozenset({
    "review_accept", "review_recategorise", "review_rename", "review_keep_separate",
    "movement_complete", "movement_merge", "topup_conversion", "topup_rejection",
    "purpose_answer", "suggestion_accept", "suggestion_reject", "seen_twice_separate",
})

# Every column whose change after the action makes an undo a conflict (D10, D12).
TRANSACTION_IMAGE_COLUMNS = (
    "type", "amount", "account_id", "date", "category_id", "kakeibo_type", "notes", "merchant",
    "offset_category_id", "movement_id", "movement_role", "fee_for_movement_id", "goal_id", "obligation_id",
    "recurring_rule_id", "idempotency_key", "source", "review_status", "classified_by",
    "classification_confidence", "owner_edited_at", "owner_split_at", "updated_at",
)
# What undo writes back. Amount, account, date, type, source, idempotency key and goal, debt and plan
# links are never written; the conflict check proves they are unchanged (D11).
RESTORED_COLUMNS = {
    "category_id": "uuid", "kakeibo_type": "text", "notes": "text", "merchant": "text",
    "offset_category_id": "uuid", "movement_id": "uuid", "movement_role": "text", "review_status": "text",
    "classified_by": "text", "classification_confidence": "numeric", "owner_edited_at": "timestamptz",
    "owner_split_at": "timestamptz",
}
PRODUCT_COLUMNS = ("investment_tracking_mode", "investment_cost_basis", "last_price", "investment_value_estimated",
                   "updated_at")
RESTORED_ROW_FIELDS = ("id", "type", "amount", "date", "account_id", "category_id", "kakeibo_type",
                       "review_status", "movement_id")


def now() -> datetime:
    """The undo clock; tests replace it to reach expiry and retention."""
    return datetime.now(timezone.utc)


def token_hash(token: str) -> bytes:
    return hashlib.sha256(token.encode("ascii")).digest()


def null_token() -> dict[str, None]:
    return {"undo_token": None, "expires_at": None}


def undo_error(status: int, code: str, **extra: Any) -> HTTPException:
    return api_error(status, code, extra=extra)


def plain(value: Any) -> Any:
    """A JSON-safe value; instants in UTC so images compare equal whatever the session time zone."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


# --- Images ----------------------------------------------------------------------------------------


def transaction_images(cur, user_id: str, transaction_ids: list[str], *, lock: bool = True) -> dict[str, dict]:
    if not transaction_ids:
        return {}
    cur.execute(
        f"""SELECT id, {", ".join(TRANSACTION_IMAGE_COLUMNS)} FROM transactions
            WHERE user_id = %s AND id = ANY(%s::uuid[]) ORDER BY id {"FOR UPDATE" if lock else ""}""",
        (user_id, sorted(set(transaction_ids))),
    )
    return {str(row["id"]): plain(row) for row in cur.fetchall()}


def event_images(cur, user_id: str, transaction_ids: list[str], event_ids: list[str], *,
                 lock: bool = True) -> dict[str, dict]:
    if not transaction_ids and not event_ids:
        return {}
    cur.execute(
        f"""SELECT id, transaction_id, processing_state, active_question, movement_id, confirmed_role, result_key,
                   result_snapshot, interpretation -> 'purpose_answer' AS purpose_answer
            FROM notification_events
            WHERE user_id = %s AND (transaction_id = ANY(%s::uuid[]) OR id = ANY(%s::uuid[]))
            ORDER BY id {"FOR UPDATE" if lock else ""}""",
        (user_id, sorted(set(transaction_ids)), sorted(set(event_ids))),
    )
    return {str(row["id"]): plain(row) for row in cur.fetchall()}


def product_image(cur, user_id: str, account_id: str) -> dict | None:
    cur.execute(
        f"SELECT {', '.join(PRODUCT_COLUMNS)} FROM accounts WHERE user_id = %s AND id = %s",
        (user_id, account_id),
    )
    row = cur.fetchone()
    return plain(row) if row else None


def topup_image(cur, user_id: str, topup_id: str, *, lock: bool = False) -> dict | None:
    cur.execute(
        f"""SELECT id, target_account_id, amount, date, is_deleted, updated_at FROM investment_topups
            WHERE user_id = %s AND id = %s {"FOR UPDATE" if lock else ""}""",
        (user_id, topup_id),
    )
    row = cur.fetchone()
    return plain(row) if row else None


def suggestion_statuses(cur, user_id: str, suggestion_ids: list[str]) -> dict[str, str]:
    if not suggestion_ids:
        return {}
    cur.execute(
        "SELECT id, status FROM transaction_suggestions WHERE user_id = %s AND id = ANY(%s::uuid[])",
        (user_id, suggestion_ids),
    )
    return {str(row["id"]): row["status"] for row in cur.fetchall()}


def learned_memory_keys(cur, user_id: str, transaction_ids: list[str], merchants: list[str | None]) -> set[str]:
    """Memory keys an owner edit of these rows can write: each row's event key and merchant keys (D13)."""
    from app.services.merchant_memory import transaction_event_key

    keys = {key for merchant in merchants if (key := merchant_match_key(merchant))}
    if not transaction_ids:
        return keys
    cur.execute(
        """SELECT DISTINCT ON (transaction_id) transaction_id, interpretation ->> 'match_key' AS match_key
           FROM notification_events WHERE user_id = %s AND transaction_id = ANY(%s::uuid[])
           ORDER BY transaction_id, post_time, id""",
        (user_id, transaction_ids),
    )
    for row in cur.fetchall():
        if row["match_key"]:
            keys.add(row["match_key"])
        elif key := transaction_event_key(cur, user_id, str(row["transaction_id"])).get("match_key"):
            keys.add(key)
    return keys


# --- Recording an action ---------------------------------------------------------------------------


class ActionRecorder:
    """Collects one review action's before and after images inside the action's database transaction.

    Call `capture` after the action holds its locks and before it writes, then `finish` after its last
    write. Rows inserted, feedback appended, memory upserted and top-up answers stored in the same
    transaction are found by the transaction timestamp, which PostgreSQL's NOW() keeps constant.
    """

    def __init__(self, cur, user_id: str, kind: str):
        if kind not in KINDS:
            raise ValueError(f"unknown review action kind {kind}")
        self.cur, self.user_id, self.kind = cur, str(user_id), kind
        self.order: list[str] = []
        self.before_transactions: dict[str, dict] = {}
        self.event_ids: set[str] = set()
        self.before_events: dict[str, dict] = {}
        self.memory_before: dict[str, dict | None] = {}
        self.product: dict | None = None
        self.suggestions_before: dict[str, str] = {}

    def capture(self, transaction_ids: list[str], *, event_ids: list[str] = (), learns: bool = False,
                merchants: list[str | None] = ()) -> None:
        new = [str(value) for value in transaction_ids if str(value) not in self.before_transactions]
        for value in new:
            self.order.append(value)
        images = transaction_images(self.cur, self.user_id, new)
        self.before_transactions.update(images)
        self.event_ids.update(str(value) for value in event_ids)
        events = event_images(self.cur, self.user_id, list(self.before_transactions), sorted(self.event_ids))
        for event_id, image in events.items():
            self.before_events.setdefault(event_id, image)
        if learns:
            before_merchants = [image.get("merchant") for image in images.values()]
            self.capture_memory(learned_memory_keys(self.cur, self.user_id, new, [*before_merchants, *merchants]))

    def capture_memory(self, keys: set[str]) -> None:
        for key in sorted(keys):
            if key not in self.memory_before:
                self.memory_before[key] = memory_row(self.cur, self.user_id, key)

    def capture_product(self, account_id: str) -> None:
        self.product = {"id": str(account_id), "before": product_image(self.cur, self.user_id, str(account_id))}

    def capture_suggestions(self, suggestion_ids: list[str]) -> None:
        self.suggestions_before.update(suggestion_statuses(self.cur, self.user_id, suggestion_ids))

    def _written_memory(self) -> dict[str, dict]:
        cur = self.cur
        cur.execute(
            """SELECT match_key, updated_at, created_at = NOW() AS created FROM merchant_memory
               WHERE user_id = %s AND updated_at = NOW()""",
            (self.user_id,),
        )
        written = {}
        for row in cur.fetchall():
            key = row["match_key"]
            if key in self.memory_before:
                before = self.memory_before[key]
            elif row["created"]:
                before = None
            else:
                # Not captured and not new: its earlier value is unknown, so undo leaves it as it is.
                logger.warning("Review undo memory not captured kind=%s", self.kind)
                continue
            written[key] = {"match_key": key, "before": before, "after_updated_at": jsonable(row["updated_at"])}
        return written

    def finish(self, *, created_transaction_ids: list[str] = (), topup_id: str | None = None,
               extra: dict | None = None) -> dict:
        """Store the record and return `undo_token` and `expires_at`, or nulls when nothing changed."""
        cur, user_id = self.cur, self.user_id
        created = [str(value) for value in created_transaction_ids if value]
        after_transactions = transaction_images(cur, user_id, [*self.before_transactions, *created], lock=False)
        involved = sorted({*self.before_transactions, *created})
        after_events = event_images(cur, user_id, involved, sorted(self.event_ids), lock=False)
        transactions = {
            tx_id: {"before": before, "after": after_transactions.get(tx_id)}
            for tx_id, before in self.before_transactions.items()
            if after_transactions.get(tx_id) != before
        }
        created_images = {tx_id: after_transactions[tx_id] for tx_id in created if tx_id in after_transactions}
        events = {
            event_id: {"before": self.before_events.get(event_id), "after": after}
            for event_id, after in after_events.items()
            if self.before_events.get(event_id) != after
        }
        cur.execute(
            """SELECT id FROM classification_feedback
               WHERE user_id = %s AND created_at = NOW() AND kind <> 'undo' AND transaction_id = ANY(%s::uuid[])
               ORDER BY id""",
            (user_id, involved),
        )
        feedback_ids = [str(row["id"]) for row in cur.fetchall()]
        cur.execute(
            """SELECT id FROM investment_topup_feedback
               WHERE user_id = %s AND created_at = NOW() AND transaction_id = ANY(%s::uuid[]) ORDER BY id""",
            (user_id, involved),
        )
        topup_feedback_ids = [str(row["id"]) for row in cur.fetchall()]
        memory = self._written_memory()
        suggestions_after = suggestion_statuses(cur, user_id, list(self.suggestions_before))
        suggestions = {
            sid: {"before": status, "after": suggestions_after.get(sid)}
            for sid, status in self.suggestions_before.items() if suggestions_after.get(sid) != status
        }
        topup = None
        if topup_id:
            topup = {"id": str(topup_id), "after": topup_image(cur, user_id, str(topup_id))}
            if self.product:
                topup["product"] = {**self.product, "after": product_image(cur, user_id, self.product["id"])}
        changed = (transactions or created_images or events or feedback_ids or topup_feedback_ids or memory
                   or suggestions or topup)
        if not changed:
            return null_token()

        moment = now()
        clear_owner_snapshots(cur, user_id, moment)
        token = secrets.token_urlsafe(TOKEN_BYTES)
        expires_at = moment + UNDO_WINDOW
        ordered_ids = [*self.order, *(tx_id for tx_id in created if tx_id not in self.order)]
        snapshot = {
            "transactions": transactions, "created_transactions": created_images, "events": events,
            "memory": memory, "feedback_ids": feedback_ids, "topup_feedback_ids": topup_feedback_ids,
            "topup": topup, "suggestions": suggestions, **(extra or {}),
        }
        cur.execute(
            """INSERT INTO review_actions (user_id, token_hash, kind, transaction_ids, snapshot, created_at, expires_at)
               VALUES (%s, %s, %s, %s::uuid[], %s, %s, %s)""",
            (user_id, token_hash(token), self.kind, ordered_ids, Jsonb(snapshot), moment, expires_at),
        )
        logger.info("Review action recorded kind=%s", self.kind)
        return {"undo_token": token, "expires_at": expires_at.isoformat()}


# --- Retention (D17) -------------------------------------------------------------------------------


def clear_owner_snapshots(cur, user_id: str, moment: datetime) -> None:
    """The lazy clear: expired snapshots of one owner lose their content, day-old records go."""
    cur.execute(
        """UPDATE review_actions SET snapshot = NULL
           WHERE user_id = %s AND snapshot IS NOT NULL AND expires_at <= %s""",
        (user_id, moment),
    )
    cur.execute("DELETE FROM review_actions WHERE user_id = %s AND created_at <= %s", (user_id, moment - RETENTION))


def sweep_expired(limit: int = SWEEP_LIMIT) -> dict[str, int]:
    """One bounded pass for every owner, run by the background loop (D17)."""
    moment = now()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE review_actions SET snapshot = NULL
                   WHERE id IN (SELECT id FROM review_actions WHERE snapshot IS NOT NULL AND expires_at <= %s
                                ORDER BY expires_at LIMIT %s FOR UPDATE SKIP LOCKED)""",
                (moment, limit),
            )
            cleared = cur.rowcount
            cur.execute(
                """DELETE FROM review_actions
                   WHERE id IN (SELECT id FROM review_actions WHERE created_at <= %s
                                ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED)""",
                (moment - RETENTION, limit),
            )
            deleted = cur.rowcount
        conn.commit()
    return {"cleared": cleared, "deleted": deleted}


# --- Undo ------------------------------------------------------------------------------------------


def compared(image: dict | None) -> dict | None:
    """An image without its write time. A row that an earlier undo put back to exactly this state is not a
    later change, so the tab's actions can be undone newest first."""
    if image is None:
        return None
    return {key: value for key, value in image.items() if key != "updated_at"}


def conflicting_ids(cur, user_id: str, snapshot: dict) -> list[str]:
    """Transactions whose row, event or top-up changed since the action; locks what undo will write."""
    expected = {tx_id: images["after"] for tx_id, images in snapshot["transactions"].items()}
    expected.update(snapshot["created_transactions"])
    current = transaction_images(cur, user_id, list(expected))
    changed = {tx_id for tx_id, image in expected.items() if compared(current.get(tx_id)) != compared(image)}
    events = snapshot["events"]
    if events:
        current_events = event_images(cur, user_id, [], list(events))
        for event_id, images in events.items():
            if current_events.get(event_id) != images["after"]:
                after = images["after"] or images["before"] or {}
                changed.add(str(after.get("transaction_id")) if after.get("transaction_id") else event_id)
    topup = snapshot.get("topup")
    if topup and topup_image(cur, user_id, topup["id"], lock=True) != topup["after"]:
        changed.update(snapshot["created_transactions"] or snapshot["transactions"])
    return sorted(changed)


def restore_transactions(cur, user_id: str, snapshot: dict) -> None:
    rows = []
    for tx_id, images in snapshot["transactions"].items():
        before = images["before"]
        if before is None:
            continue
        rows.append({"id": tx_id, **{column: before.get(column) for column in RESTORED_COLUMNS}})
    if not rows:
        return
    record = ", ".join(f"{column} {kind}" for column, kind in {"id": "uuid", **RESTORED_COLUMNS}.items())
    assignments = ", ".join(f"{column} = r.{column}" for column in RESTORED_COLUMNS)
    cur.execute(
        f"""UPDATE transactions t SET {assignments}, updated_at = NOW()
            FROM jsonb_to_recordset(%s) AS r({record})
            WHERE t.user_id = %s AND t.id = r.id""",
        (Jsonb(rows), user_id),
    )


def restore_events(cur, user_id: str, snapshot: dict) -> None:
    rows = [{"id": event_id, **images["before"]} for event_id, images in snapshot["events"].items() if images["before"]]
    if not rows:
        return
    # A JSON null inside the recordset is the jsonb value 'null', not SQL NULL; NULLIF maps it back.
    cur.execute(
        """UPDATE notification_events e
           SET active_question = NULLIF(r.active_question, 'null'::jsonb), movement_id = r.movement_id,
               confirmed_role = r.confirmed_role, result_key = r.result_key,
               result_snapshot = NULLIF(r.result_snapshot, 'null'::jsonb),
               interpretation = CASE
                   WHEN NULLIF(r.purpose_answer, 'null'::jsonb) IS NULL THEN e.interpretation - 'purpose_answer'
                   ELSE COALESCE(e.interpretation, '{}'::jsonb) || jsonb_build_object('purpose_answer', r.purpose_answer)
               END,
               updated_at = NOW()
           FROM jsonb_to_recordset(%s) AS r(id uuid, active_question jsonb, movement_id uuid, confirmed_role text,
                                             result_key text, result_snapshot jsonb, purpose_answer jsonb)
           WHERE e.user_id = %s AND e.id = r.id""",
        (Jsonb(rows), user_id),
    )


def revert_feedback(cur, user_id: str, feedback_ids: list[str]) -> None:
    """Append one `undo` row per label: identity columns copied, the label's values as its before (D13)."""
    if not feedback_ids:
        return
    cur.execute(
        """INSERT INTO classification_feedback (
               user_id, kind, reverts_feedback_id, transaction_id, event_id, account_id, amount, institution,
               text_signature, match_key, local_weekday, local_hour, transaction_source,
               before_merchant, before_notes, before_category_id, before_category_name, before_kakeibo_type,
               before_type, before_offset_category_id, before_classified_by)
           SELECT user_id, 'undo', id, transaction_id, event_id, account_id, amount, institution,
                  text_signature, match_key, local_weekday, local_hour, transaction_source,
                  after_merchant, after_notes, after_category_id, after_category_name, after_kakeibo_type,
                  after_type, after_offset_category_id, after_classified_by
           FROM classification_feedback
           WHERE user_id = %s AND id = ANY(%s::uuid[]) AND kind <> 'undo'
           ORDER BY id
           ON CONFLICT (reverts_feedback_id) WHERE reverts_feedback_id IS NOT NULL DO NOTHING""",
        (user_id, feedback_ids),
    )


def restore_suggestions(cur, user_id: str, suggestions: dict[str, dict]) -> None:
    for status in sorted({images["after"] for images in suggestions.values() if images["after"]}):
        ids = [sid for sid, images in suggestions.items() if images["after"] == status and images["before"] == "pending"]
        if not ids:
            continue
        # A suggestion decided again since keeps its status; one pending suggestion per row stays the rule.
        cur.execute(
            """UPDATE transaction_suggestions s SET status = 'pending', decided_at = NULL, updated_at = NOW()
               WHERE s.user_id = %s AND s.id = ANY(%s::uuid[]) AND s.status = %s
                 AND NOT EXISTS (SELECT 1 FROM transaction_suggestions other
                                 WHERE other.transaction_id = s.transaction_id AND other.status = 'pending'
                                   AND other.id <> s.id)""",
            (user_id, ids, status),
        )


def undo_topup(cur, user_id: str, snapshot: dict) -> None:
    """Remove the top-up through the existing deletion, then restore the product if unchanged (D11)."""
    from app.services.investment_topups import mutate_topup

    topup = snapshot["topup"]
    product = topup.get("product")
    unchanged = bool(product) and compared(product_image(cur, user_id, product["id"])) == compared(product["after"])
    try:
        mutate_topup(cur, user_id, topup["id"], delete=True)
    except HTTPException as error:
        if error.status_code == 409:
            raise undo_error(409, "undo_conflict", transaction_ids=sorted(snapshot["transactions"])) from None
        raise
    if unchanged and product["before"]:
        before = product["before"]
        cur.execute(
            """UPDATE accounts SET investment_tracking_mode = %s, investment_cost_basis = %s, last_price = %s,
                   investment_value_estimated = %s, updated_at = NOW()
               WHERE user_id = %s AND id = %s""",
            (before["investment_tracking_mode"], before["investment_cost_basis"], before["last_price"],
             before["investment_value_estimated"], user_id, product["id"]),
        )


def restored_rows(cur, user_id: str, transaction_ids: list[str]) -> list[dict]:
    if not transaction_ids:
        return []
    cur.execute(
        f"""SELECT {", ".join(RESTORED_ROW_FIELDS)} FROM transactions
            WHERE user_id = %s AND id = ANY(%s::uuid[]) ORDER BY date, id""",
        (user_id, transaction_ids),
    )
    rows = []
    for row in cur.fetchall():
        item = plain(row)
        item["amount"] = int(row["amount"])
        item["date"] = row["date"].isoformat() if row["date"] else None
        rows.append(item)
    return rows


def undo_response(record: dict, *, already: bool, restored: list[dict], removed: list[str]) -> dict:
    return {
        "ok": True, "kind": record["kind"], "already_undone": already,
        "transaction_ids": [str(value) for value in record["transaction_ids"]],
        "removed_transaction_ids": removed, "restored": restored,
    }


def undo_action(user_id: str, token: str) -> dict:
    """Restore the before-state of the action that issued `token` (D9, D11, D12)."""
    user_id = str(user_id)
    moment = now()
    with db_conn() as conn:
        with conn.cursor() as cur:
            # The lock order of learned auto-record: the owner's notification lock, then the conversion lock.
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
            cur.execute(
                "SELECT id, kind, transaction_ids FROM review_actions WHERE user_id = %s AND token_hash = %s",
                (user_id, token_hash(token)),
            )
            found = cur.fetchone()
            if found and found["kind"] == "topup_conversion" and found["transaction_ids"]:
                debit = str(found["transaction_ids"][0])
                cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                            (f"investment-topup-from:{user_id}:{debit}",))
            record = None
            if found:
                cur.execute(
                    """SELECT id, kind, transaction_ids, snapshot, created_at, expires_at, undone_at
                       FROM review_actions WHERE user_id = %s AND id = %s FOR UPDATE""",
                    (user_id, found["id"]),
                )
                record = cur.fetchone()
            clear_owner_snapshots(cur, user_id, moment)
            if record is None or record["created_at"] <= moment - RETENTION:
                conn.commit()
                logger.info("Review undo outcome=undo_not_found")
                raise undo_error(404, "undo_not_found")
            if record["undone_at"] is not None:
                conn.commit()
                logger.info("Review undo kind=%s outcome=already_undone", record["kind"])
                return undo_response(record, already=True, restored=[], removed=[])
            if moment >= record["expires_at"] or record["snapshot"] is None:
                cur.execute("UPDATE review_actions SET snapshot = NULL WHERE id = %s", (record["id"],))
                conn.commit()
                logger.info("Review undo kind=%s outcome=undo_expired", record["kind"])
                raise undo_error(410, "undo_expired")
            snapshot = record["snapshot"]
            conflicts = conflicting_ids(cur, user_id, snapshot)
            if conflicts:
                conn.rollback()
                logger.info("Review undo kind=%s outcome=undo_conflict", record["kind"])
                raise undo_error(409, "undo_conflict", transaction_ids=conflicts)

            created = list(snapshot["created_transactions"])
            if snapshot.get("topup"):
                undo_topup(cur, user_id, snapshot)
            if created:
                # The created leg is unchanged (checked above); the original leg is restored below.
                cur.execute("DELETE FROM transactions WHERE user_id = %s AND id = ANY(%s::uuid[])", (user_id, created))
            restore_transactions(cur, user_id, snapshot)
            restore_events(cur, user_id, snapshot)
            if snapshot.get("seen_twice"):
                from app.services.seen_twice import restore_seen_twice

                restore_seen_twice(cur, user_id, snapshot["seen_twice"])
            revert_feedback(cur, user_id, snapshot["feedback_ids"])
            for learned in snapshot["memory"].values():
                restore_memory(cur, user_id, learned)
            if snapshot["topup_feedback_ids"]:
                cur.execute("DELETE FROM investment_topup_feedback WHERE user_id = %s AND id = ANY(%s::uuid[])",
                            (user_id, snapshot["topup_feedback_ids"]))
            restore_suggestions(cur, user_id, snapshot["suggestions"])
            cur.execute("UPDATE review_actions SET undone_at = %s, snapshot = NULL WHERE id = %s",
                        (moment, record["id"]))
            restored = restored_rows(cur, user_id, [tx_id for tx_id in snapshot["transactions"]])
        conn.commit()
    logger.info("Review undo kind=%s outcome=restored", record["kind"])
    return undo_response(record, already=False, restored=restored, removed=sorted(created))
