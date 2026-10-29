"""The owner's review queue: open purpose questions, flagged and automatic rows, and unfinished movements."""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator, model_validator

from app.core.config import settings
from app.db.pool import db_conn
from app.routers.transactions import classification_change
from app.services.auth import get_current_user
from app.services.kakeibo import PILLAR_PATTERN, effective_pillar
from app.services.merchant_memory import (
    MATCH_KEY_MAX_LENGTH,
    merchant_match_key,
    normalize_key_text,
    record_owner_corrections,
    snapshot_rows,
)
from app.services.movement_pairing import CANDIDATE_LIMIT, load_candidates
from app.services.provenance import OWNER_EDIT_ASSIGNMENTS, normalize_merchant
from app.services.purpose_questions import list_open_questions

router = APIRouter(prefix="/review", tags=["Review"])

QUEUE_ROW_LIMIT = 1000
QUESTION_LIMIT = 200
BULK_LIMIT = 200
AMBIGUOUS_CANDIDATE_LIMIT = 5
MOVEMENT_GROUP_KEY = "movement"

QUEUE_SQL = """
    SELECT t.id, t.type, t.amount, t.date, t.account_id, a.name AS account_name,
           t.category_id, c.name AS category_name,
           CASE WHEN t.type = 'expense' THEN COALESCE(t.kakeibo_type, c.kakeibo_type, 'need') END AS kakeibo_type,
           t.merchant, t.notes, t.review_status, t.source, t.classified_by,
           t.movement_id, t.movement_role, t.fee_for_movement_id,
           COALESCE(c.name = 'Internal Movement', FALSE) AS movement_category,
           ev.interpretation -> 'review_reasons' AS review_reasons,
           ev.interpretation ->> 'match_key' AS match_key,
           question.id IS NOT NULL AS has_open_question
    FROM transactions t
    JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
    LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
    LEFT JOIN LATERAL (
        SELECT e.interpretation FROM notification_events e
        WHERE e.user_id = t.user_id AND e.transaction_id = t.id
        ORDER BY e.post_time, e.id LIMIT 1
    ) ev ON TRUE
    LEFT JOIN LATERAL (
        SELECT e.id FROM notification_events e
        WHERE e.user_id = t.user_id AND e.processing_state = 'recorded'
          AND e.active_question ->> 'type' = 'purpose'
          AND (e.active_question ->> 'transaction_id')::uuid = t.id
        LIMIT 1
    ) question ON TRUE
    WHERE t.user_id = %s AND t.review_status IN ('auto', 'needs_review') AND t.type IN ('expense', 'income')
    ORDER BY t.date DESC, t.id
    LIMIT %s
"""


# Reasons that a later event settles: pairing settles the movement doubts, an answer closes the question.
MOVEMENT_REASONS = frozenset({"awaiting_counterpart", "ambiguous_movement", "purpose_question"})


def review_reasons(row: dict) -> list[str]:
    """The row's open review reasons; the stored list keeps reasons that later events resolved."""
    reasons = row.get("review_reasons")
    if not isinstance(reasons, list):
        return []
    current = []
    for reason in dict.fromkeys(reasons):
        if not isinstance(reason, str):
            continue
        if row.get("movement_id") and reason in MOVEMENT_REASONS:
            continue
        if reason == "purpose_question" and not row.get("has_open_question"):
            continue
        current.append(reason)
    return current


def queue_item(row: dict) -> dict[str, Any]:
    return {
        "id": str(row["id"]), "type": row["type"], "amount": int(row["amount"]),
        "date": row["date"].isoformat() if row["date"] else None,
        "account_id": str(row["account_id"]), "account_name": row["account_name"],
        "category_id": str(row["category_id"]) if row["category_id"] else None,
        "category_name": row["category_name"], "kakeibo_type": row["kakeibo_type"],
        "merchant": row["merchant"], "notes": row["notes"], "review_status": row["review_status"],
        "source": row["source"], "classified_by": row["classified_by"],
        "movement_id": str(row["movement_id"]) if row["movement_id"] else None,
        "movement_role": row["movement_role"],
        "fee_for_movement_id": str(row["fee_for_movement_id"]) if row["fee_for_movement_id"] else None,
        "reasons": review_reasons(row),
    }


def group_identity(row: dict) -> tuple[str, str, str]:
    """(kind, key, label) of the group a queue row belongs to.

    Rows group by merchant; without one, by the notification's text signature (phase 3 match key),
    then by notes, then by category. The key includes the type, so every group can be recategorised
    into one category kind.
    """
    if row["movement_id"]:
        return "movement", MOVEMENT_GROUP_KEY, "Pindah saldo antar rekening"
    merchant_key = merchant_match_key(row["merchant"])
    if merchant_key:
        return "merchant", f"{row['type']}|{merchant_key}", row["merchant"]
    notes = (row["notes"] or "").strip()
    match_key = row.get("match_key") or ""
    if match_key.startswith("signature:"):
        return "signature", f"{row['type']}|{match_key}", notes or "Tanpa keterangan"
    if notes:
        return "notes", f"{row['type']}|" + f"notes:{normalize_key_text(notes)}"[:MATCH_KEY_MAX_LENGTH], notes
    category = str(row["category_id"]) if row["category_id"] else "none"
    return "category", f"{row['type']}|category:{category}", row["category_name"] or "Tanpa kategori"


def build_groups(rows: list[dict]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        kind, key, label = group_identity(row)
        group = groups.get(key)
        if group is None:
            group = groups[key] = {
                "key": key, "kind": kind, "type": MOVEMENT_GROUP_KEY if kind == "movement" else row["type"],
                "label": label, "count": 0, "total": 0, "needs_review_count": 0, "auto_count": 0,
                "first_date": None, "last_date": None, "reasons": [], "category_ids": set(),
                "editable": kind != "movement", "transactions": [],
            }
        item = queue_item(row)
        group["transactions"].append(item)
        group["count"] += 1
        # A movement moves money once; count its outgoing legs only.
        if kind != "movement" or row["type"] == "expense":
            group["total"] += item["amount"]
        group["needs_review_count" if row["review_status"] == "needs_review" else "auto_count"] += 1
        if item["date"]:
            group["first_date"] = min(filter(None, [group["first_date"], item["date"]]))
            group["last_date"] = max(filter(None, [group["last_date"], item["date"]]))
        for reason in item["reasons"]:
            if reason not in group["reasons"]:
                group["reasons"].append(reason)
        group["category_ids"].add(item["category_id"])
    result = []
    for group in groups.values():
        category_ids = group.pop("category_ids")
        shared = next(iter(category_ids)) if len(category_ids) == 1 else None
        first = group["transactions"][0]
        group["category"] = {"id": shared, "name": first["category_name"]} if shared else None
        result.append(group)
    result.sort(key=lambda item: (-item["needs_review_count"], -item["count"], -item["total"], item["label"]))
    return result


def ambiguous_candidates(cur, user_id: str, row: dict, account_names: dict[str, str]) -> list[dict[str, Any]]:
    """Opposite unlinked legs of the same amount the owner may link this leg with.

    The merge endpoint links only equal amounts, so fee-different legs are not offered.
    """
    direction = "income" if row["type"] == "expense" else "expense"
    candidates = load_candidates(
        cur, user_id, direction=direction, around=[row["date"]],
        gap_seconds=settings.movement_repair_max_gap_seconds,
        min_amount=int(row["amount"]), max_amount=int(row["amount"]),
        exclude=[str(row["id"])], limit=CANDIDATE_LIMIT + 1,
    )
    usable = [candidate for candidate in candidates if str(candidate["account_id"]) != str(row["account_id"])]
    usable.sort(key=lambda candidate: abs((candidate["date"] - row["date"]).total_seconds()))
    return [
        {
            "id": str(candidate["id"]), "type": candidate["type"], "amount": int(candidate["amount"]),
            "date": candidate["date"].isoformat(), "account_id": str(candidate["account_id"]),
            "account_name": account_names.get(str(candidate["account_id"])),
        }
        for candidate in usable[:AMBIGUOUS_CANDIDATE_LIMIT]
    ]


@router.get("/summary")
def review_summary(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    with db_conn() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE t.review_status = 'needs_review' OR question.id IS NOT NULL) AS attention,
                COUNT(*) FILTER (WHERE t.review_status = 'auto' AND question.id IS NULL) AS unreviewed
            FROM transactions t
            LEFT JOIN LATERAL (
                SELECT e.id FROM notification_events e
                WHERE e.user_id = t.user_id AND e.processing_state = 'recorded'
                  AND e.active_question ->> 'type' = 'purpose'
                  AND (e.active_question ->> 'transaction_id')::uuid = t.id
                LIMIT 1
            ) question ON TRUE
            WHERE t.user_id = %s AND t.type IN ('expense', 'income')
              AND (t.review_status IN ('auto', 'needs_review') OR question.id IS NOT NULL)
            """,
            (user_id,),
        ).fetchone()
    return {"ok": True, "attention": int(row["attention"]), "unreviewed": int(row["unreviewed"])}


@router.get("/queue")
def review_queue(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    questions = list_open_questions(user_id, QUESTION_LIMIT)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(QUEUE_SQL, (user_id, QUEUE_ROW_LIMIT + 1))
            rows = cur.fetchall()
            truncated = len(rows) > QUEUE_ROW_LIMIT
            rows = rows[:QUEUE_ROW_LIMIT]
            cur.execute("SELECT id, name FROM accounts WHERE user_id = %s", (user_id,))
            account_names = {str(account["id"]): account["name"] for account in cur.fetchall()}

            grouped, awaiting, ambiguous = [], [], []
            for row in rows:
                if row["has_open_question"] and not row["movement_id"]:
                    continue
                unlinked = row["movement_id"] is None
                is_ambiguous = (unlinked and row["review_status"] == "needs_review"
                                and "ambiguous_movement" in review_reasons(row))
                candidates = ambiguous_candidates(cur, user_id, row, account_names) if is_ambiguous else []
                # An ambiguous leg needs the owner's choice among its candidates; once none is left, an own
                # transfer only waits for its counterpart account.
                if is_ambiguous and (candidates or not row["movement_category"]):
                    ambiguous.append({**queue_item(row), "candidates": candidates})
                # Only legs the pairing flagged wait for a counterpart; older one-sided rows the owner left
                # as they are stay ordinary rows to accept.
                elif unlinked and row["movement_category"] and "awaiting_counterpart" in review_reasons(row):
                    awaiting.append(queue_item(row))
                else:
                    grouped.append(row)
    groups = build_groups(grouped)
    return {
        "ok": True,
        "truncated": truncated,
        "counts": {
            "questions": len(questions),
            "needs_review": sum(group["needs_review_count"] for group in groups),
            "auto": sum(group["auto_count"] for group in groups),
            "groups": len(groups),
            "awaiting": len(awaiting),
            "ambiguous": len(ambiguous),
        },
        "questions": questions,
        "groups": groups,
        "awaiting": awaiting,
        "ambiguous": ambiguous,
    }


class BulkReview(BaseModel):
    action: Literal["accept", "recategorise", "rename", "keep_separate"]
    transaction_ids: list[UUID] = Field(min_length=1, max_length=BULK_LIMIT)
    category_id: UUID | None = None
    kakeibo_type: str | None = Field(default=None, pattern=PILLAR_PATTERN)
    merchant: str | None = Field(default=None, max_length=500)
    notes: str | None = Field(default=None, max_length=500)

    @field_validator("merchant")
    @classmethod
    def normalize_merchant_field(cls, value: str | None) -> str | None:
        return normalize_merchant(value)

    @field_validator("notes")
    @classmethod
    def normalize_notes(cls, value: str | None) -> str | None:
        return (value or "").strip() or None

    @model_validator(mode="after")
    def fields_match_action(self) -> "BulkReview":
        if len(set(self.transaction_ids)) != len(self.transaction_ids):
            raise ValueError("transaction_ids must not repeat")
        given = self.model_fields_set - {"action", "transaction_ids"}
        allowed = {
            "accept": set(), "keep_separate": set(),
            "recategorise": {"category_id", "kakeibo_type"}, "rename": {"merchant", "notes"},
        }[self.action]
        if given - allowed:
            raise ValueError(f"{self.action} does not take {', '.join(sorted(given - allowed))}")
        if self.action == "recategorise" and self.category_id is None:
            raise ValueError("recategorise needs category_id")
        if self.action == "rename" and not given:
            raise ValueError("rename needs merchant or notes")
        return self


def bulk_error(status: int, code: str, message: str, **extra: Any) -> HTTPException:
    return HTTPException(status_code=status, detail={"code": code, "message": message, **extra})


def locked_rows(cur, user_id: str, ids: list[str]) -> dict[str, dict]:
    cur.execute(
        """SELECT t.id, t.type, t.movement_id, t.category_id, t.kakeibo_type, t.notes, t.merchant,
                  t.offset_category_id, t.review_status, c.kakeibo_type AS category_kakeibo_type
           FROM transactions t LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
           WHERE t.user_id = %s AND t.id = ANY(%s::uuid[])
           ORDER BY t.id FOR UPDATE OF t""",
        (user_id, ids),
    )
    rows = {str(row["id"]): row for row in cur.fetchall()}
    missing = [value for value in ids if value not in rows]
    if missing:
        raise bulk_error(404, "transactions_not_found", "Sebagian transaksi tidak ditemukan.", transaction_ids=missing)
    return rows


def reject_movement_legs(rows: dict[str, dict], code: str, message: str) -> None:
    legs = sorted(row_id for row_id, row in rows.items() if row["movement_id"])
    if legs:
        raise bulk_error(409, code, message, transaction_ids=legs)


def mark_reviewed(cur, user_id: str, ids: list[str]) -> None:
    if ids:
        cur.execute(
            """UPDATE transactions SET review_status = 'reviewed', updated_at = NOW()
               WHERE user_id = %s AND id = ANY(%s::uuid[]) AND review_status <> 'reviewed'""",
            (user_id, ids),
        )


def recategorise(cur, user_id: str, rows: dict[str, dict], payload: BulkReview) -> int:
    reject_movement_legs(rows, "movement_leg_not_editable", "Pindah saldo diubah lewat detail pindah saldo.")
    cur.execute(
        """SELECT id, kind, kakeibo_type, offsets_spending FROM categories
           WHERE user_id = %s AND id = %s AND is_archived = FALSE""",
        (user_id, str(payload.category_id)),
    )
    category = cur.fetchone()
    if not category:
        raise bulk_error(404, "category_not_found", "Kategori tidak ditemukan atau sudah diarsipkan.")
    if any(row["type"] != category["kind"] for row in rows.values()):
        raise bulk_error(422, "category_type_mismatch", "Jenis kategori harus sama dengan jenis semua transaksi.")
    if payload.kakeibo_type and category["kind"] != "expense":
        raise bulk_error(422, "pillar_not_allowed", "Pilar Kakeibo hanya untuk pengeluaran.")

    category_id = str(category["id"])
    keeps_offset = category["kind"] == "income" and bool(category["offsets_spending"])
    changed = [
        row_id for row_id, row in rows.items()
        if classification_change(row, {
            "category_id": category_id,
            "kakeibo_type": effective_pillar(row["type"], payload.kakeibo_type, category["kakeibo_type"]),
            "notes": row["notes"], "merchant": row["merchant"], "type": row["type"],
            "offset_category_id": (str(row["offset_category_id"]) if row["offset_category_id"] else None) if keeps_offset else None,
        })
    ]
    before = snapshot_rows(cur, user_id, changed)
    if changed:
        # A NULL row pillar follows the category, so recategorising without a pillar adopts the new one.
        cur.execute(
            f"""UPDATE transactions
                SET category_id = %s, kakeibo_type = %s,
                    offset_category_id = CASE WHEN %s THEN offset_category_id END,
                    review_status = 'reviewed', updated_at = NOW(), {OWNER_EDIT_ASSIGNMENTS}
                WHERE user_id = %s AND id = ANY(%s::uuid[])""",
            (category_id, payload.kakeibo_type if category["kind"] == "expense" else None, keeps_offset, user_id, changed),
        )
    mark_reviewed(cur, user_id, [row_id for row_id in rows if row_id not in before])
    return record_owner_corrections(cur, user_id, before)


def rename(cur, user_id: str, rows: dict[str, dict], payload: BulkReview) -> int:
    reject_movement_legs(rows, "movement_leg_not_editable", "Pindah saldo diubah lewat detail pindah saldo.")
    set_merchant = "merchant" in payload.model_fields_set
    set_notes = "notes" in payload.model_fields_set
    changed = [
        row_id for row_id, row in rows.items()
        if classification_change(row, {
            "category_id": str(row["category_id"]) if row["category_id"] else None,
            "kakeibo_type": effective_pillar(row["type"], row["kakeibo_type"], row["category_kakeibo_type"]),
            "notes": payload.notes if set_notes else row["notes"],
            "merchant": payload.merchant if set_merchant else row["merchant"],
            "type": row["type"],
            "offset_category_id": str(row["offset_category_id"]) if row["offset_category_id"] else None,
        })
    ]
    before = snapshot_rows(cur, user_id, changed)
    if changed:
        cur.execute(
            f"""UPDATE transactions
                SET merchant = CASE WHEN %s THEN %s ELSE merchant END,
                    notes = CASE WHEN %s THEN %s ELSE notes END,
                    review_status = 'reviewed', updated_at = NOW(), {OWNER_EDIT_ASSIGNMENTS}
                WHERE user_id = %s AND id = ANY(%s::uuid[])""",
            (set_merchant, payload.merchant, set_notes, payload.notes, user_id, changed),
        )
    mark_reviewed(cur, user_id, [row_id for row_id in rows if row_id not in before])
    return record_owner_corrections(cur, user_id, before)


def keep_separate(cur, user_id: str, rows: dict[str, dict]) -> None:
    reject_movement_legs(rows, "already_linked", "Transaksi ini sudah tertaut sebagai pindah saldo.")
    # The owner's decision that no automatic pairing should link these legs (movement pairing D7).
    cur.execute(
        """UPDATE transactions
           SET owner_split_at = COALESCE(owner_split_at, NOW()), review_status = 'reviewed', updated_at = NOW()
           WHERE user_id = %s AND id = ANY(%s::uuid[])""",
        (user_id, list(rows)),
    )


@router.post("/bulk")
def bulk_review(payload: BulkReview, current_user: dict = Depends(get_current_user)):
    """Apply one review action to up to 200 of the owner's rows, all or nothing."""
    user_id = current_user["id"]
    ids = [str(value) for value in payload.transaction_ids]
    corrections = 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Serialises with notification processing, which writes the same rows, questions and memory.
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
            rows = locked_rows(cur, user_id, ids)
            if payload.action == "accept":
                mark_reviewed(cur, user_id, ids)
            elif payload.action == "recategorise":
                corrections = recategorise(cur, user_id, rows, payload)
            elif payload.action == "rename":
                corrections = rename(cur, user_id, rows, payload)
            else:
                keep_separate(cur, user_id, rows)
        conn.commit()
    return {"ok": True, "action": payload.action, "transaction_count": len(ids), "corrections": corrections}
