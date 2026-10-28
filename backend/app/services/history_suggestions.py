"""Owner-triggered suggestions that re-enrich past automatic rows; nothing changes until the owner accepts."""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import psycopg
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.core.config import settings
from app.db.pool import db_conn
from app.services.kakeibo import Pillar, effective_pillar
from app.services.merchant_memory import merchant_match_key, normalize_key_text, owner_identity
from app.services.provenance import MERCHANT_MAX_LENGTH, normalize_merchant
from app.services.reimbursements import NON_REPORTABLE_CATEGORY_NAMES


logger = logging.getLogger(__name__)

HISTORY_PROMPT_VERSION = "history-suggestions-1"
HISTORY_PROMPT_PATH = Path(__file__).with_name("prompts") / "history_suggestions.md"
MAX_WINDOW_DAYS = 366
NOTES_MAX_LENGTH = 500
REASON_MAX_LENGTH = 160
# Model proposals below this confidence are not worth the owner's attention.
MIN_AI_CONFIDENCE = 0.5
MEMORY_CONTEXT_LIMIT = 30
OWNER_HISTORY_LIMIT = 40
RUN_LEASE_SECONDS = 300
JAJAN_CATEGORY_NAME = "jajan & nongkrong"

# Words in an incoming transfer that say a friend is paying back a shared expense.
REPAYMENT_PATTERN = re.compile(
    r"\b(patungan|urunan|ganti(?:in)?(?: uang)?|bayar(?:in)?|balikin|split|tf makan|transfer makan|uang makan)\b",
    re.IGNORECASE,
)
MEAL_PATTERN = re.compile(r"\b(makan|minum|nasi|bakso|mie|warung|resto|restoran|lunch|dinner)\b", re.IGNORECASE)
CAFE_PATTERN = re.compile(
    r"\b(kopi|coffee|cafe|café|kafe|caffe|ngopi|nongkrong|jajan|jajanan|snack|boba|dessert|martabak|"
    r"es teh|starbucks|janji jiwa|kopi kenangan|fore|tomoro|chatime|mixue|point coffee)\b",
    re.IGNORECASE,
)

ELIGIBLE_SQL = f"""
    SELECT t.id, t.type, t.amount, t.date, t.account_id, a.name AS account_name, t.merchant, t.notes,
           t.category_id, c.name AS category_name, t.kakeibo_type, c.kakeibo_type AS category_kakeibo_type,
           t.offset_category_id,
           ev.interpretation ->> 'match_key' AS match_key,
           ev.interpretation ->> 'text_signature' AS text_signature,
           concat_ws(' ', ev.title, ev.body_text, ev.big_text) AS event_text
    FROM transactions t
    JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
    LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
    LEFT JOIN LATERAL (
        SELECT e.interpretation, e.title, e.body_text, e.big_text FROM notification_events e
        WHERE e.user_id = t.user_id AND e.transaction_id = t.id
        ORDER BY e.post_time, e.id LIMIT 1
    ) ev ON TRUE
    WHERE t.user_id = %(user_id)s AND t.type IN ('expense', 'income')
      AND t.classified_by IN ('rule', 'ai') AND t.owner_edited_at IS NULL
      AND t.movement_id IS NULL AND t.fee_for_movement_id IS NULL
      AND t.goal_id IS NULL AND t.obligation_id IS NULL
      AND NOT EXISTS (SELECT 1 FROM transaction_obligation_allocations al WHERE al.transaction_id = t.id)
      AND NOT COALESCE(c.name = ANY(%(non_reportable)s), FALSE)
      AND t.date >= %(start)s AND t.date < %(end)s
"""


class HistoryProposal(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    ref: str = Field(min_length=1, max_length=8)
    merchant: str | None = Field(max_length=120)
    description: str = Field(min_length=1, max_length=160)
    category_id: UUID | None
    kakeibo: Pillar | None
    offset_category_id: UUID | None
    confidence: float = Field(ge=0, le=1)
    reason: str = Field(min_length=1, max_length=160)


class HistorySuggestionBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    suggestions: list[HistoryProposal]


def history_prompt() -> str:
    return HISTORY_PROMPT_PATH.read_text(encoding="utf-8")


class SuggestionError(Exception):
    def __init__(self, status: int, code: str, message: str, **extra):
        self.status = status
        self.detail = {"code": code, "message": message, **extra}
        super().__init__(code)


@dataclass
class Proposal:
    transaction_id: str
    source: str
    merchant: str | None
    notes: str | None
    category_id: str
    kakeibo: str | None
    offset_category_id: str | None
    confidence: float | None
    reason: str


@dataclass
class RunCounts:
    processed: int = 0
    memory: int = 0
    rule: int = 0
    ai: int = 0
    unchanged: int = 0
    rejected_before: int = 0
    ai_calls: int = 0


@dataclass
class Plan:
    rows: list[dict]
    eligible_count: int
    proposals: list[Proposal] = field(default_factory=list)
    ai_rows: list[dict] = field(default_factory=list)


# --- Windows and availability --------------------------------------------------------------------


def local_today() -> date:
    return datetime.now(ZoneInfo(settings.tz)).date()


def window_bounds(from_date: date, to_date: date) -> tuple[datetime, datetime]:
    tz = ZoneInfo(settings.tz)
    return datetime.combine(from_date, time.min, tz), datetime.combine(to_date + timedelta(days=1), time.min, tz)


def validate_window(from_date: date, to_date: date) -> None:
    if to_date < from_date:
        raise ValueError("to_date must not be before from_date")
    if (to_date - from_date).days + 1 > MAX_WINDOW_DAYS:
        raise ValueError(f"window must be at most {MAX_WINDOW_DAYS} days")
    if to_date > local_today():
        raise ValueError("to_date must not be in the future")


def ai_unavailable_reason(provider) -> str | None:
    from app.services.openai_notification_provider import configuration_error

    if provider is None:
        return configuration_error(settings) or "ai_disabled"
    return configuration_error(settings)


# --- Values and comparison -----------------------------------------------------------------------


def folded(value: str | None) -> str:
    return normalize_key_text(value or "")


def current_values(row: dict) -> tuple:
    return (
        folded(row["merchant"]), folded(row["notes"]),
        str(row["category_id"]) if row["category_id"] else None,
        effective_pillar(row["type"], row["kakeibo_type"], row["category_kakeibo_type"]),
        str(row["offset_category_id"]) if row["offset_category_id"] else None,
    )


def proposal_values(proposal: Proposal) -> tuple:
    return (folded(proposal.merchant), folded(proposal.notes), proposal.category_id, proposal.kakeibo,
            proposal.offset_category_id)


def stored_values(row: dict) -> tuple:
    return (
        folded(row["proposed_merchant"]), folded(row["proposed_notes"]), str(row["proposed_category_id"]),
        row["proposed_kakeibo_type"],
        str(row["proposed_offset_category_id"]) if row["proposed_offset_category_id"] else None,
    )


# --- Owner reference data ------------------------------------------------------------------------


def load_categories(cur, user_id: str) -> dict[str, dict]:
    cur.execute(
        """SELECT id, name, kind, kakeibo_type, offsets_spending, is_archived,
                  COALESCE(is_excluded_from_budget, FALSE) AS is_excluded_from_budget
           FROM categories WHERE user_id = %s""",
        (user_id,),
    )
    return {str(row["id"]): row for row in cur.fetchall()}


def active_categories(categories: dict[str, dict], kind: str) -> list[dict]:
    return [row for row in categories.values()
            if row["kind"] == kind and not row["is_archived"] and row["name"] not in NON_REPORTABLE_CATEGORY_NAMES]


def offset_categories(categories: dict[str, dict]) -> list[dict]:
    """Expense categories a repayment may offset (the reimbursement rule)."""
    return [row for row in active_categories(categories, "expense") if not row["is_excluded_from_budget"]]


def load_owner_memory(cur, user_id: str) -> dict[str, dict]:
    cur.execute(
        """SELECT m.match_key, m.display_merchant, m.notes, m.category_id, m.kakeibo_type, m.offset_category_id
           FROM merchant_memory m
           JOIN categories c ON c.id = m.category_id AND c.user_id = m.user_id AND c.is_archived = FALSE
           WHERE m.user_id = %s AND m.source = 'owner'""",
        (user_id,),
    )
    return {row["match_key"]: row for row in cur.fetchall()}


def preferred_repayment_offset(cur, user_id: str, eligible: set[str]) -> str | None:
    """The spending category the owner most often marked as repaid by a friend."""
    cur.execute(
        """SELECT after_offset_category_id AS id, COUNT(*) AS uses FROM classification_feedback
           WHERE user_id = %s AND after_type = 'income' AND after_offset_category_id IS NOT NULL
           GROUP BY after_offset_category_id ORDER BY uses DESC, MAX(created_at) DESC""",
        (user_id,),
    )
    for row in cur.fetchall():
        if str(row["id"]) in eligible:
            return str(row["id"])
    return None


# --- Deterministic proposals ---------------------------------------------------------------------


def row_keys(row: dict) -> list[str]:
    keys = [merchant_match_key(row["merchant"]), row["match_key"]]
    return [key for key in dict.fromkeys(keys) if key]


def memory_proposal(row: dict, memory: dict[str, dict], categories: dict[str, dict]) -> Proposal | None:
    """An owner memory entry for the row's merchant or bank signature."""
    for key in row_keys(row):
        entry = memory.get(key)
        if not entry:
            continue
        category = categories.get(str(entry["category_id"]))
        if not category or category["kind"] != row["type"] or category["is_archived"]:
            continue
        offset = None
        if row["type"] == "income" and category["offsets_spending"] and entry["offset_category_id"]:
            offset_id = str(entry["offset_category_id"])
            if offset_id in {str(item["id"]) for item in offset_categories(categories)}:
                offset = offset_id
        merchant = entry["display_merchant"] or row["merchant"]
        reason = (f"Sama dengan pilihan Anda untuk {merchant}" if key.startswith("merchant:") and merchant
                  else "Sama dengan pilihan Anda untuk notifikasi bank serupa")
        return Proposal(
            transaction_id=str(row["id"]), source="memory", merchant=merchant,
            notes=entry["notes"] or row["notes"], category_id=str(category["id"]),
            kakeibo=effective_pillar(row["type"], entry["kakeibo_type"], category["kakeibo_type"]),
            offset_category_id=offset, confidence=None, reason=reason[:REASON_MAX_LENGTH],
        )
    return None


def row_text(row: dict) -> str:
    """Merchant, notes and the recording notification's text; read for keywords, never stored."""
    return " ".join(filter(None, [row["merchant"], row["notes"], row.get("event_text")]))


def rule_proposal(row: dict, categories: dict[str, dict], repayment_offset: str | None) -> Proposal | None:
    """Repayments of shared spending and café or snack spending, recognised by their words."""
    text = row_text(row)
    current = categories.get(str(row["category_id"])) if row["category_id"] else None
    if row["type"] == "income":
        if current and current["offsets_spending"]:
            return None
        if not REPAYMENT_PATTERN.search(text):
            return None
        target = next((item for item in sorted(active_categories(categories, "income"), key=lambda c: c["name"])
                       if item["offsets_spending"]), None)
        if not target:
            return None
        eligible = sorted(offset_categories(categories), key=lambda c: c["name"])
        offset = repayment_offset
        if offset is None and MEAL_PATTERN.search(text):
            offset = next((str(item["id"]) for item in eligible if "makan" in folded(item["name"])), None)
        if offset is None and CAFE_PATTERN.search(text):
            offset = next((str(item["id"]) for item in eligible if folded(item["name"]) == JAJAN_CATEGORY_NAME), None)
        return Proposal(
            transaction_id=str(row["id"]), source="rule", merchant=row["merchant"], notes="Ganti uang patungan",
            category_id=str(target["id"]), kakeibo=None, offset_category_id=offset, confidence=None,
            reason="Transfer masuk menyebut patungan atau ganti uang: teman membayar kembali, bukan pemasukan",
        )
    if not CAFE_PATTERN.search(text):
        return None
    target = next((item for item in active_categories(categories, "expense")
                   if folded(item["name"]) == JAJAN_CATEGORY_NAME), None)
    if not target or (current and str(current["id"]) == str(target["id"])):
        return None
    return Proposal(
        transaction_id=str(row["id"]), source="rule", merchant=row["merchant"], notes=row["notes"],
        category_id=str(target["id"]), kakeibo=effective_pillar("expense", None, target["kakeibo_type"]),
        offset_category_id=None, confidence=None,
        reason="Kafe, kopi atau jajanan masuk ke Jajan & Nongkrong",
    )


def plan_window(cur, user_id: str, from_date: date, to_date: date, max_rows: int) -> Plan:
    """Eligible rows of the window, newest first, with memory and rule proposals already made."""
    start, end = window_bounds(from_date, to_date)
    params = {"user_id": user_id, "non_reportable": list(NON_REPORTABLE_CATEGORY_NAMES), "start": start, "end": end}
    cur.execute(f"SELECT COUNT(*) AS count FROM ({ELIGIBLE_SQL}) eligible", params)
    eligible_count = int(cur.fetchone()["count"])
    cur.execute(f"{ELIGIBLE_SQL} ORDER BY t.date DESC, t.id LIMIT %(limit)s", {**params, "limit": max_rows})
    rows = cur.fetchall()
    categories = load_categories(cur, user_id)
    memory = load_owner_memory(cur, user_id)
    eligible_offsets = {str(item["id"]) for item in offset_categories(categories)}
    repayment_offset = preferred_repayment_offset(cur, user_id, eligible_offsets)
    plan = Plan(rows=rows, eligible_count=eligible_count)
    for row in rows:
        proposal = memory_proposal(row, memory, categories) or rule_proposal(row, categories, repayment_offset)
        if proposal:
            plan.proposals.append(proposal)
        else:
            plan.ai_rows.append(row)
    return plan


def estimate(cur, user_id: str, from_date: date, to_date: date, provider) -> dict:
    plan = plan_window(cur, user_id, from_date, to_date, settings.history_suggestion_max_rows)
    reason = ai_unavailable_reason(provider)
    batch_size = settings.history_suggestion_batch_size
    return {
        "from_date": from_date.isoformat(), "to_date": to_date.isoformat(),
        "eligible_count": plan.eligible_count, "selected_count": len(plan.rows),
        "truncated": plan.eligible_count > len(plan.rows), "max_rows": settings.history_suggestion_max_rows,
        "memory_count": sum(1 for item in plan.proposals if item.source == "memory"),
        "rule_count": sum(1 for item in plan.proposals if item.source == "rule"),
        "ai_rows": len(plan.ai_rows) if reason is None else 0,
        "ai_calls": math.ceil(len(plan.ai_rows) / batch_size) if reason is None else 0,
        "batch_size": batch_size, "ai_available": reason is None, "ai_unavailable_reason": reason,
    }


# --- Storing proposals ---------------------------------------------------------------------------


def save_proposals(cur, user_id: str, run_id: str, rows: dict[str, dict], proposals: list[Proposal]) -> RunCounts:
    """Insert new pending suggestions; skip ones equal to the row, to its pending one, or rejected before."""
    counts = RunCounts()
    if not proposals:
        return counts
    ids = [proposal.transaction_id for proposal in proposals]
    cur.execute(
        """SELECT * FROM transaction_suggestions
           WHERE user_id = %s AND transaction_id = ANY(%s::uuid[]) AND status IN ('pending', 'rejected')""",
        (user_id, ids),
    )
    pending: dict[str, dict] = {}
    rejected: dict[str, set[tuple]] = {}
    for existing in cur.fetchall():
        transaction_id = str(existing["transaction_id"])
        if existing["status"] == "pending":
            pending[transaction_id] = existing
        else:
            rejected.setdefault(transaction_id, set()).add(stored_values(existing))
    for proposal in proposals:
        row = rows[proposal.transaction_id]
        values = proposal_values(proposal)
        if values == current_values(row):
            counts.unchanged += 1
            continue
        if values in rejected.get(proposal.transaction_id, set()):
            counts.rejected_before += 1
            continue
        previous = pending.get(proposal.transaction_id)
        if previous is not None:
            if stored_values(previous) == values:
                counts.unchanged += 1
                continue
            cur.execute(
                """UPDATE transaction_suggestions SET status = 'superseded', decided_at = NOW(), updated_at = NOW()
                   WHERE id = %s AND status = 'pending'""",
                (previous["id"],),
            )
        cur.execute(
            """INSERT INTO transaction_suggestions (
                   user_id, transaction_id, run_id, proposal_source, proposed_merchant, proposed_notes,
                   proposed_category_id, proposed_kakeibo_type, proposed_offset_category_id, confidence, reason,
                   before_type, before_merchant, before_notes, before_category_id, before_kakeibo_type,
                   before_offset_category_id)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                user_id, proposal.transaction_id, run_id, proposal.source, proposal.merchant,
                (proposal.notes or None) and proposal.notes[:NOTES_MAX_LENGTH], proposal.category_id,
                proposal.kakeibo, proposal.offset_category_id, proposal.confidence, proposal.reason[:REASON_MAX_LENGTH],
                row["type"], row["merchant"], row["notes"],
                row["category_id"], effective_pillar(row["type"], row["kakeibo_type"], row["category_kakeibo_type"]),
                row["offset_category_id"],
            ),
        )
        setattr(counts, proposal.source, getattr(counts, proposal.source) + 1)
    return counts


def supersede_stale(cur, user_id: str) -> int:
    """Pending suggestions whose row changed since, or whose proposal can no longer apply."""
    cur.execute(
        """
        UPDATE transaction_suggestions s
        SET status = 'superseded', decided_at = NOW(), updated_at = NOW()
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
        WHERE s.user_id = %(user_id)s AND s.status = 'pending' AND t.id = s.transaction_id AND (
            t.owner_edited_at IS NOT NULL OR t.classified_by = 'owner'
            OR t.movement_id IS NOT NULL OR t.fee_for_movement_id IS NOT NULL
            OR t.type IS DISTINCT FROM s.before_type
            OR t.merchant IS DISTINCT FROM s.before_merchant
            OR NULLIF(btrim(COALESCE(t.notes, '')), '') IS DISTINCT FROM NULLIF(btrim(COALESCE(s.before_notes, '')), '')
            OR t.category_id IS DISTINCT FROM s.before_category_id
            OR (CASE WHEN t.type = 'expense' THEN COALESCE(t.kakeibo_type, c.kakeibo_type, 'need') END)
               IS DISTINCT FROM s.before_kakeibo_type
            OR t.offset_category_id IS DISTINCT FROM s.before_offset_category_id
            OR NOT EXISTS (
                SELECT 1 FROM categories pc
                WHERE pc.id = s.proposed_category_id AND pc.user_id = s.user_id
                  AND pc.is_archived = FALSE AND pc.kind = t.type
            )
        )
        """,
        {"user_id": user_id},
    )
    return cur.rowcount


# --- Model batches -------------------------------------------------------------------------------


def bank_category(row: dict) -> str | None:
    parts = (row.get("text_signature") or "").split("|")
    return parts[2] if len(parts) > 2 and parts[2] not in {"unknown", "payment"} else None


def batch_context(cur, user_id: str, rows: list[dict]) -> dict:
    """Redacted model input for one batch: rows, categories, owner memory and owner-classified history."""
    from app.services.notification_context import CONTEXT_BYTE_LIMIT, context_size, sanitize_text

    identity = owner_identity(cur, user_id)
    tz = ZoneInfo(settings.tz)
    categories = load_categories(cur, user_id)
    kinds = {row["type"] for row in rows}
    usable = [item for kind in sorted(kinds) for item in active_categories(categories, kind)]
    cur.execute(
        """SELECT m.display_merchant, m.notes, m.category_id, m.kakeibo_type
           FROM merchant_memory m
           JOIN categories c ON c.id = m.category_id AND c.user_id = m.user_id AND c.is_archived = FALSE
           WHERE m.user_id = %s AND m.source = 'owner' AND c.kind = ANY(%s)
           ORDER BY (m.match_key = ANY(%s)) DESC, m.hit_count DESC, m.updated_at DESC, m.id LIMIT %s""",
        (user_id, sorted(kinds), [key for row in rows for key in row_keys(row)], MEMORY_CONTEXT_LIMIT),
    )
    memory = cur.fetchall()
    merchants = sorted({folded(row["merchant"]) for row in rows if row["merchant"]})
    cur.execute(
        """SELECT t.type, t.merchant, LEFT(t.notes, 160) AS notes, t.category_id, t.amount,
                  CASE WHEN t.type = 'expense' THEN COALESCE(t.kakeibo_type, c.kakeibo_type, 'need') END AS kakeibo
           FROM transactions t LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
           WHERE t.user_id = %s AND t.type = ANY(%s) AND t.classified_by = 'owner' AND t.movement_id IS NULL
             AND t.category_id IS NOT NULL
           ORDER BY (lower(btrim(COALESCE(t.merchant, ''))) = ANY(%s)) DESC, t.date DESC, t.id LIMIT %s""",
        (user_id, sorted(kinds), merchants, OWNER_HISTORY_LIMIT),
    )
    history = cur.fetchall()
    context = {
        "rows": [{
            "ref": f"r{index + 1}", "direction": row["type"], "amount": int(row["amount"]),
            "date": row["date"].astimezone(tz).date().isoformat(),
            "account": sanitize_text(row["account_name"], identity)[:150],
            "merchant": sanitize_text(row["merchant"], identity)[:120] or None,
            "description": sanitize_text(row["notes"], identity)[:160] or None,
            "category_id": str(row["category_id"]) if row["category_id"] else None,
            "kakeibo": effective_pillar(row["type"], row["kakeibo_type"], row["category_kakeibo_type"]),
            "bank_category": bank_category(row), "merchant_less": not row["merchant"],
        } for index, row in enumerate(rows)],
        "categories": [{
            "id": str(item["id"]), "name": sanitize_text(item["name"], identity)[:100], "kind": item["kind"],
            "default_kakeibo": item["kakeibo_type"], "offsets_spending": bool(item["offsets_spending"]),
        } for item in sorted(usable, key=lambda item: (item["kind"], item["name"]))],
        "offset_categories": [{"id": str(item["id"]), "name": sanitize_text(item["name"], identity)[:100]}
                              for item in sorted(offset_categories(categories), key=lambda item: item["name"])]
        if "income" in kinds else [],
        "memory": [{
            "merchant": sanitize_text(item["display_merchant"], identity)[:120] or None,
            "description": sanitize_text(item["notes"], identity)[:160] or None,
            "category_id": str(item["category_id"]), "kakeibo": item["kakeibo_type"],
        } for item in memory],
        "owner_history": [{
            "direction": item["type"], "merchant": sanitize_text(item["merchant"], identity)[:120] or None,
            "description": sanitize_text(item["notes"], identity)[:160] or None,
            "category_id": str(item["category_id"]), "kakeibo": item["kakeibo"], "amount": int(item["amount"]),
        } for item in history],
    }
    for name in ("owner_history", "memory"):
        while context[name] and context_size(context) > CONTEXT_BYTE_LIMIT:
            context[name].pop()
    return context


def validated_proposals(batch: HistorySuggestionBatch, rows: list[dict], context: dict) -> tuple[list[Proposal], int]:
    """Model items that reference only the batch's rows and the owner's categories; returns (valid, invalid)."""
    by_ref = {item["ref"]: row for item, row in zip(context["rows"], rows)}
    categories = {item["id"]: item for item in context["categories"]}
    offsets = {item["id"] for item in context["offset_categories"]}
    seen: set[str] = set()
    valid: list[Proposal] = []
    invalid = 0
    for item in batch.suggestions:
        row = by_ref.get(item.ref)
        category = categories.get(str(item.category_id)) if item.category_id else None
        offset = str(item.offset_category_id) if item.offset_category_id else None
        if (row is None or item.ref in seen or category is None or category["kind"] != row["type"]
                or (row["type"] == "income" and item.kakeibo is not None)
                or (offset and (row["type"] != "income" or not category["offsets_spending"] or offset not in offsets))):
            invalid += 1
            continue
        seen.add(item.ref)
        if item.confidence < MIN_AI_CONFIDENCE:
            continue
        description = " ".join(item.description.split())
        try:
            merchant = normalize_merchant(item.merchant) or row["merchant"]
        except ValueError:
            invalid += 1
            continue
        valid.append(Proposal(
            transaction_id=str(row["id"]), source="ai", merchant=merchant[:MERCHANT_MAX_LENGTH] if merchant else None,
            notes=description or row["notes"], category_id=category["id"],
            kakeibo=effective_pillar(row["type"], item.kakeibo, category["default_kakeibo"]),
            offset_category_id=offset, confidence=round(item.confidence, 3),
            reason=" ".join(item.reason.split())[:REASON_MAX_LENGTH],
        ))
    return valid, invalid


# --- Runs ----------------------------------------------------------------------------------------


def expire_interrupted_runs(cur, user_id: str) -> None:
    """A running run whose lease ran out was interrupted (for example by a restart)."""
    cur.execute(
        """UPDATE suggestion_runs
           SET status = 'failed', error_code = 'run_interrupted', lease_token = NULL, lease_expires_at = NULL,
               finished_at = NOW(), updated_at = NOW()
           WHERE user_id = %s AND status = 'running' AND lease_expires_at < NOW()""",
        (user_id,),
    )


def create_run(cur, user_id: str, from_date: date, to_date: date, provider) -> dict:
    expire_interrupted_runs(cur, user_id)
    ai_reason = ai_unavailable_reason(provider)
    token = str(uuid4())
    try:
        cur.execute("SAVEPOINT create_suggestion_run")
        cur.execute(
            """INSERT INTO suggestion_runs (user_id, from_date, to_date, status, max_rows, model, prompt_version,
                   lease_token, lease_expires_at)
               VALUES (%s, %s, %s, 'running', %s, %s, %s, %s, NOW() + %s * INTERVAL '1 second')
               RETURNING *""",
            (user_id, from_date, to_date, settings.history_suggestion_max_rows,
             settings.notification_ai_model if ai_reason is None else None,
             HISTORY_PROMPT_VERSION if ai_reason is None else None, token, RUN_LEASE_SECONDS),
        )
    except psycopg.errors.UniqueViolation:
        cur.execute("ROLLBACK TO SAVEPOINT create_suggestion_run")
        raise SuggestionError(409, "suggestion_run_active", "Pembuatan saran sedang berjalan. Tunggu hingga selesai.")
    return cur.fetchone()


def run_view(run: dict | None) -> dict | None:
    if not run:
        return None
    return {
        "id": str(run["id"]), "status": run["status"],
        "from_date": run["from_date"].isoformat(), "to_date": run["to_date"].isoformat(),
        "eligible_count": run["eligible_count"], "selected_count": run["selected_count"],
        "processed_count": run["processed_count"], "memory_count": run["memory_count"],
        "rule_count": run["rule_count"], "ai_count": run["ai_count"], "unchanged_count": run["unchanged_count"],
        "rejected_before_count": run["rejected_before_count"], "ai_calls": run["ai_calls"],
        "error_code": run["error_code"], "model": run["model"], "prompt_version": run["prompt_version"],
        "created_at": run["created_at"].isoformat(),
        "finished_at": run["finished_at"].isoformat() if run["finished_at"] else None,
    }


def latest_run(cur, user_id: str) -> dict | None:
    expire_interrupted_runs(cur, user_id)
    cur.execute(
        "SELECT * FROM suggestion_runs WHERE user_id = %s ORDER BY created_at DESC, id LIMIT 1",
        (user_id,),
    )
    return cur.fetchone()


def leased_run(cur, run_id: str, token: str) -> dict | None:
    """The run under this worker's lease, renewed; None once the lease was lost."""
    cur.execute(
        """UPDATE suggestion_runs SET lease_expires_at = NOW() + %s * INTERVAL '1 second', updated_at = NOW()
           WHERE id = %s AND lease_token = %s AND status = 'running' AND lease_expires_at > NOW()
           RETURNING *""",
        (RUN_LEASE_SECONDS, run_id, token),
    )
    return cur.fetchone()


def record_progress(cur, run_id: str, counts: RunCounts) -> None:
    cur.execute(
        """UPDATE suggestion_runs
           SET processed_count = processed_count + %s, memory_count = memory_count + %s,
               rule_count = rule_count + %s, ai_count = ai_count + %s, unchanged_count = unchanged_count + %s,
               rejected_before_count = rejected_before_count + %s, ai_calls = ai_calls + %s, updated_at = NOW()
           WHERE id = %s""",
        (counts.processed, counts.memory, counts.rule, counts.ai, counts.unchanged, counts.rejected_before,
         counts.ai_calls, run_id),
    )


def deterministic_stage(run_id: str, token: str, user_id: str) -> list[dict] | None:
    """Store memory and rule suggestions; returns the rows left for the model, or None without a lease."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            run = leased_run(cur, run_id, token)
            if not run:
                conn.rollback()
                return None
            supersede_stale(cur, user_id)
            plan = plan_window(cur, user_id, run["from_date"], run["to_date"], run["max_rows"])
            rows = {str(row["id"]): row for row in plan.rows}
            counts = save_proposals(cur, user_id, run_id, rows, plan.proposals)
            counts.processed = len(plan.proposals)
            record_progress(cur, run_id, counts)
            cur.execute(
                "UPDATE suggestion_runs SET eligible_count = %s, selected_count = %s WHERE id = %s",
                (plan.eligible_count, len(plan.rows), run_id),
            )
        conn.commit()
    return plan.ai_rows


def prepare_batch(run_id: str, token: str, user_id: str, rows: list[dict]) -> dict | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            if not leased_run(cur, run_id, token):
                conn.rollback()
                return None
            context = batch_context(cur, user_id, rows)
        conn.commit()
    return context


def store_batch(run_id: str, token: str, user_id: str, rows: list[dict], proposals: list[Proposal]) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            if not leased_run(cur, run_id, token):
                conn.rollback()
                return False
            counts = save_proposals(cur, user_id, run_id, {str(row["id"]): row for row in rows}, proposals)
            counts.processed = len(rows)
            counts.ai_calls = 1
            record_progress(cur, run_id, counts)
        conn.commit()
    return True


def finish_run(run_id: str, token: str, status: str, error_code: str | None) -> None:
    with db_conn() as conn:
        conn.execute(
            """UPDATE suggestion_runs
               SET status = %s, error_code = %s,
                   lease_token = NULL, lease_expires_at = NULL, finished_at = NOW(), updated_at = NOW()
               WHERE id = %s AND lease_token = %s AND status = 'running'""",
            (status, error_code, run_id, token),
        )
        conn.commit()


async def execute_run(run_id: str, token: str, user_id: str, provider) -> None:
    """Memory and rule suggestions first, then model batches; a provider failure keeps what was made."""
    from app.services.notification_processing import database_operation
    from app.services.openai_notification_provider import ProviderError

    error_code = None
    try:
        remaining = await database_operation(deterministic_stage, run_id, token, user_id)
        if remaining is None:
            return
        ai_reason = ai_unavailable_reason(provider)
        if remaining and ai_reason:
            error_code = ai_reason
        batch_size = settings.history_suggestion_batch_size
        invalid = 0
        while remaining and error_code is None:
            batch, remaining = remaining[:batch_size], remaining[batch_size:]
            context = await database_operation(prepare_batch, run_id, token, user_id, batch)
            if context is None:
                return
            try:
                result = await provider.suggest_history(context)
            except ProviderError as exc:
                error_code = exc.code
                remaining = batch + remaining
                break
            proposals, rejected = validated_proposals(result, batch, context)
            invalid += rejected
            if not await database_operation(store_batch, run_id, token, user_id, batch, proposals):
                return
        await database_operation(finish_run, run_id, token, "completed", error_code)
        # Counts and codes only: rows, names and model output are personal data.
        logger.info("History suggestion run completed error_code=%s invalid_items=%s unprocessed=%s",
                    error_code or "none", invalid, len(remaining))
    except Exception as exc:
        logger.warning("History suggestion run failed: %s", type(exc).__name__)
        try:
            await database_operation(finish_run, run_id, token, "failed", "run_failed")
        except Exception as finish_exc:
            # The lease expires and the next request marks the run interrupted.
            logger.warning("History suggestion run could not be closed: %s", type(finish_exc).__name__)


# --- Listing and decisions -----------------------------------------------------------------------


PENDING_LIMIT = 1000

PENDING_SQL = """
    SELECT s.*, t.type, t.amount, t.date, a.name AS account_name,
           bc.name AS before_category_name, pc.name AS proposed_category_name,
           oc.name AS proposed_offset_category_name
    FROM transaction_suggestions s
    JOIN transactions t ON t.id = s.transaction_id AND t.user_id = s.user_id
    JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
    LEFT JOIN categories bc ON bc.id = s.before_category_id AND bc.user_id = s.user_id
    JOIN categories pc ON pc.id = s.proposed_category_id AND pc.user_id = s.user_id
    LEFT JOIN categories oc ON oc.id = s.proposed_offset_category_id AND oc.user_id = s.user_id
    WHERE s.user_id = %s AND s.status = 'pending'
    ORDER BY t.date DESC, s.id
    LIMIT %s
"""


def suggestion_item(row: dict) -> dict:
    return {
        "id": str(row["id"]), "transaction_id": str(row["transaction_id"]), "type": row["type"],
        "amount": int(row["amount"]), "date": row["date"].isoformat(), "account_name": row["account_name"],
        "source": row["proposal_source"],
        "confidence": float(row["confidence"]) if row["confidence"] is not None else None,
        "reason": row["reason"],
        "before": {
            "merchant": row["before_merchant"], "notes": row["before_notes"],
            "category_id": str(row["before_category_id"]) if row["before_category_id"] else None,
            "category_name": row["before_category_name"], "kakeibo": row["before_kakeibo_type"],
        },
        "after": {
            "merchant": row["proposed_merchant"], "notes": row["proposed_notes"],
            "category_id": str(row["proposed_category_id"]), "category_name": row["proposed_category_name"],
            "kakeibo": row["proposed_kakeibo_type"],
            "offset_category_id": str(row["proposed_offset_category_id"]) if row["proposed_offset_category_id"] else None,
            "offset_category_name": row["proposed_offset_category_name"],
        },
    }


def group_suggestions(rows: list[dict]) -> list[dict]:
    """Pending suggestions grouped by proposed merchant (or notes), category and offset."""
    groups: dict[str, dict] = {}
    for row in rows:
        item = suggestion_item(row)
        label = row["proposed_merchant"] or row["proposed_notes"] or row["proposed_category_name"]
        name_key = merchant_match_key(row["proposed_merchant"]) or f"notes:{folded(row['proposed_notes'])}"
        # The offset is part of the key so a group's preview is true for every suggestion in it.
        key = f"{row['type']}|{name_key}|{row['proposed_category_id']}|{row['proposed_offset_category_id'] or ''}"
        group = groups.get(key)
        if group is None:
            group = groups[key] = {
                "key": key, "type": row["type"], "label": label, "merchant": row["proposed_merchant"],
                "category": {"id": str(row["proposed_category_id"]), "name": row["proposed_category_name"]},
                "kakeibo": set(), "offset_category_names": set(), "sources": set(), "before_labels": {},
                "count": 0, "total": 0, "first_date": None, "last_date": None, "suggestions": [],
            }
        group["suggestions"].append(item)
        group["count"] += 1
        group["total"] += item["amount"]
        group["first_date"] = min(filter(None, [group["first_date"], item["date"]]))
        group["last_date"] = max(filter(None, [group["last_date"], item["date"]]))
        group["kakeibo"].add(row["proposed_kakeibo_type"])
        if row["proposed_offset_category_name"]:
            group["offset_category_names"].add(row["proposed_offset_category_name"])
        group["sources"].add(row["proposal_source"])
        before = row["before_notes"] or row["before_merchant"] or "Tanpa keterangan"
        before_category = row["before_category_name"] or "Tanpa kategori"
        before_key = (before, before_category)
        group["before_labels"][before_key] = group["before_labels"].get(before_key, 0) + 1
    result = []
    for group in groups.values():
        pillars = group.pop("kakeibo")
        group["kakeibo"] = next(iter(pillars)) if len(pillars) == 1 else None
        group["offset_category_names"] = sorted(group["offset_category_names"])
        group["sources"] = sorted(group["sources"])
        group["before"] = [
            {"label": label, "category_name": category, "count": count}
            for (label, category), count in sorted(group.pop("before_labels").items(), key=lambda entry: -entry[1])
        ]
        result.append(group)
    result.sort(key=lambda item: (-item["count"], -item["total"], item["label"] or ""))
    return result


def pending_suggestions(user_id: str) -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            supersede_stale(cur, user_id)
            cur.execute(PENDING_SQL, (user_id, PENDING_LIMIT + 1))
            rows = cur.fetchall()
        conn.commit()
    truncated = len(rows) > PENDING_LIMIT
    groups = group_suggestions(rows[:PENDING_LIMIT])
    return {"truncated": truncated, "pending": min(len(rows), PENDING_LIMIT), "groups": groups}


def locked_suggestions(cur, user_id: str, ids: list[str]) -> dict[str, dict]:
    cur.execute(
        """SELECT * FROM transaction_suggestions WHERE user_id = %s AND id = ANY(%s::uuid[])
           ORDER BY id FOR UPDATE""",
        (user_id, ids),
    )
    rows = {str(row["id"]): row for row in cur.fetchall()}
    missing = [value for value in ids if value not in rows]
    if missing:
        raise SuggestionError(404, "suggestions_not_found", "Sebagian saran tidak ditemukan.", suggestion_ids=missing)
    return rows


def accept_suggestions(user_id: str, ids: list[str]) -> dict:
    """Apply pending suggestions through the owner-edit hooks, all in one database transaction."""
    from app.routers.transactions import classification_change
    from app.services.merchant_memory import record_owner_corrections, snapshot_rows
    from app.services.provenance import OWNER_EDIT_ASSIGNMENTS
    from app.services.reimbursements import validate_offset_category

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Serialises with notification processing and bulk review, which write the same rows and memory.
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
            supersede_stale(cur, user_id)
            suggestions = locked_suggestions(cur, user_id, ids)
            pending = {sid: row for sid, row in suggestions.items() if row["status"] == "pending"}
            skipped = sorted(sid for sid in ids if sid not in pending)
            transaction_ids = sorted(str(row["transaction_id"]) for row in pending.values())
            cur.execute(
                """SELECT t.id, t.type, t.category_id, t.kakeibo_type, t.notes, t.merchant, t.offset_category_id,
                          c.kakeibo_type AS category_kakeibo_type
                   FROM transactions t LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
                   WHERE t.user_id = %s AND t.id = ANY(%s::uuid[]) ORDER BY t.id FOR UPDATE OF t""",
                (user_id, transaction_ids),
            )
            rows = {str(row["id"]): row for row in cur.fetchall()}
            categories = load_categories(cur, user_id)
            planned: dict[str, tuple[dict, dict]] = {}
            for sid, suggestion in pending.items():
                row = rows[str(suggestion["transaction_id"])]
                category = categories[str(suggestion["proposed_category_id"])]
                offset = str(suggestion["proposed_offset_category_id"]) if suggestion["proposed_offset_category_id"] else None
                if offset:
                    validate_offset_category(cur, user_id, offset)
                values = {
                    "category_id": str(category["id"]), "kakeibo_type": suggestion["proposed_kakeibo_type"],
                    "notes": suggestion["proposed_notes"], "merchant": suggestion["proposed_merchant"],
                    "type": row["type"], "offset_category_id": offset,
                }
                planned[sid] = (row, values)
            changed = {sid: values for sid, (row, values) in planned.items() if classification_change(row, values)}
            transaction_of = {sid: str(pending[sid]["transaction_id"]) for sid in changed}
            before = snapshot_rows(cur, user_id, list(transaction_of.values()))
            for sid, values in changed.items():
                category = categories[values["category_id"]]
                # A pillar equal to the category's is stored as NULL so the row keeps following its category.
                pillar = values["kakeibo_type"] if values["kakeibo_type"] != category["kakeibo_type"] else None
                cur.execute(
                    f"""UPDATE transactions
                        SET merchant = %s, notes = %s, category_id = %s, kakeibo_type = %s, offset_category_id = %s,
                            review_status = 'reviewed', updated_at = NOW(), {OWNER_EDIT_ASSIGNMENTS}
                        WHERE user_id = %s AND id = %s""",
                    (values["merchant"], values["notes"], values["category_id"],
                     pillar if values["type"] == "expense" else None, values["offset_category_id"],
                     user_id, transaction_of[sid]),
                )
            unchanged = [str(pending[sid]["transaction_id"]) for sid in pending if sid not in changed]
            if unchanged:
                cur.execute(
                    """UPDATE transactions SET review_status = 'reviewed', updated_at = NOW()
                       WHERE user_id = %s AND id = ANY(%s::uuid[]) AND review_status <> 'reviewed'""",
                    (user_id, unchanged),
                )
            if pending:
                cur.execute(
                    """UPDATE transaction_suggestions SET status = 'accepted', decided_at = NOW(), updated_at = NOW()
                       WHERE user_id = %s AND id = ANY(%s::uuid[]) AND status = 'pending'""",
                    (user_id, list(pending)),
                )
            corrections = record_owner_corrections(
                cur, user_id, before, kind="accepted_suggestion",
                history_suggestions={transaction_id: sid for sid, transaction_id in transaction_of.items()},
            )
        conn.commit()
    logger.info("History suggestions accepted count=%s skipped=%s corrections=%s", len(pending), len(skipped), corrections)
    return {"accepted": len(pending), "skipped": skipped, "corrections": corrections}


def reject_suggestions(user_id: str, ids: list[str]) -> dict:
    """Mark pending suggestions rejected; a later run never proposes the same values for the row again."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            supersede_stale(cur, user_id)
            suggestions = locked_suggestions(cur, user_id, ids)
            pending = [sid for sid, row in suggestions.items() if row["status"] == "pending"]
            if pending:
                cur.execute(
                    """UPDATE transaction_suggestions SET status = 'rejected', decided_at = NOW(), updated_at = NOW()
                       WHERE user_id = %s AND id = ANY(%s::uuid[]) AND status = 'pending'""",
                    (user_id, pending),
                )
        conn.commit()
    return {"rejected": len(pending), "skipped": sorted(sid for sid in ids if sid not in pending)}


def parse_batch(output: str) -> HistorySuggestionBatch:
    from app.services.openai_notification_provider import ProviderError

    try:
        return HistorySuggestionBatch.model_validate_json(output)
    except (ValueError, TypeError, ValidationError):
        raise ProviderError("provider_invalid_output") from None
