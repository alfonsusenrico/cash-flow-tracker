"""Transactions the owner types as one short sentence: submit, answer, record, learn and undo (design D1–D8).

Deterministic code owns the amount, date, kind and accounts; owner memory, then the model, propose the
classification; anything still uncertain becomes exactly one question at a time (`capture_resolution`).
"""

from __future__ import annotations

import json
import logging
import unicodedata
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from app.db.pool import db_conn
from app.services.capture_normalization import (
    CAPTURE_PROMPT_VERSION, InvalidCaptureNormalization, validate_capture_normalization,
)
from app.services.capture_parsing import WORD_PATTERN, fold, parse_amount_answer
from app.services.capture_resolution import (
    KIND_ROLES, KINDS, capture_facts, capture_key, default_pillar, evaluate, liquid_leaves, memory_state,
    model_context, model_unavailable, needs_model, owner_accounts, reportable_categories, resolved_amount,
    resolved_kind,
)
from app.services.kakeibo import PILLARS
from app.services.ledger_mutations import (
    create_bilateral_movement, delete_movement_rows, delete_transaction_rows, ensure_generic_movement_accounts,
    lock_owned_accounts,
)
from app.services.merchant_memory import count_memory_hit, insert_feedback, snapshot_rows, upsert_memory
from app.services.movement_linkage import ensure_internal_movement_categories, movement_note
from app.services.provenance import capture_provenance, normalize_merchant

logger = logging.getLogger(__name__)

TEXT_MAX_LENGTH = 200
ANSWER_TEXT_MAX_LENGTH = 50
DEVICE_CLOCK_SKEW = timedelta(minutes=10)
UNDO_WINDOW = timedelta(seconds=10)
UNDO_GRACE = timedelta(seconds=5)
CAPTURE_RATE_LIMIT = 30
CAPTURE_RATE_WINDOW = 60
MOVEMENT_DESCRIPTIONS = {"withdraw": "Tarik tunai", "deposit": "Setor tunai"}
# Answers to these questions are the owner's teaching; an amount answer is not a classification.
TEACHING_ANSWERS = frozenset({"kind", "account", "category"})
SNAPSHOT_COLUMNS = (
    "id, type, amount, account_id, date, category_id, kakeibo_type, notes, merchant, offset_category_id, "
    "movement_id, review_status, classified_by, owner_edited_at, updated_at"
)


def now() -> datetime:
    return datetime.now(timezone.utc)


def jsonable(value):
    return json.loads(json.dumps(value, default=str))


# --- Request boundary ---------------------------------------------------------------------------


def clean_text(value: str, limit: int, code: str) -> str:
    text = unicodedata.normalize("NFKC", value).strip()
    if not 1 <= len(text) <= limit or any(unicodedata.category(char).startswith("C") for char in text):
        raise HTTPException(status_code=422, detail={"code": code})
    return text


def validate_text_request(raw: dict) -> dict:
    device_time = raw["device_time"]
    if device_time.tzinfo is None or device_time > now() + DEVICE_CLOCK_SKEW:
        raise HTTPException(status_code=422, detail={"code": "invalid_device_time"})
    return {
        "client_capture_id": str(raw["client_capture_id"]),
        "text": clean_text(raw["text"], TEXT_MAX_LENGTH, "invalid_capture_text"),
        "device_time": device_time,
    }


# --- Recording, learning and undo -----------------------------------------------------------------


def row_states(cur, user_id: str, transaction_ids: list[str], *, lock: bool = False) -> list[dict]:
    cur.execute(
        f"""SELECT {SNAPSHOT_COLUMNS} FROM transactions
            WHERE user_id = %s AND id = ANY(%s::uuid[]) ORDER BY id {'FOR UPDATE' if lock else ''}""",
        (user_id, transaction_ids),
    )
    return jsonable(cur.fetchall())


def classified_by(draft: dict, classification: dict | None) -> str:
    owner = draft["owner"]
    if owner.get("kind") or (classification and classification["by"] == "owner"):
        return "owner"
    if classification is None or classification["by"] == "memory":
        return "rule"
    return "ai"


def record_plan(cur, user_id: str, capture_id: str, draft: dict, plan: dict) -> dict:
    """Write the ledger rows of a complete capture (D5); returns the row ids and the movement id."""
    facts = draft["facts"]
    tx_date = datetime.fromisoformat(facts["date"])
    key = f"capture:{capture_id}"
    by = classified_by(draft, plan["classification"])
    if plan["kind"] == "movement":
        source_id, target_id = plan["accounts"]["source"], plan["accounts"]["target"]
        locked = lock_owned_accounts(cur, user_id, [source_id, target_id])
        ensure_generic_movement_accounts(locked[source_id], locked[target_id])
        expense_category_id, income_category_id = ensure_internal_movement_categories(cur, user_id)
        notes = MOVEMENT_DESCRIPTIONS.get(facts["movement_form"]) or movement_note(cur, user_id, target_id)
        # A typed movement describes money already moved, so it is recorded even past the ledger balance.
        result = create_bilateral_movement(
            cur, user_id=user_id, source_id=source_id, target_id=target_id, amount=plan["amount"], notes=notes,
            tx_date=tx_date, expense_category_id=expense_category_id, income_category_id=income_category_id,
            source_account=locked[source_id], target_account=locked[target_id], idempotency_key=key,
            allow_negative=True, provenance=capture_provenance(by),
        )
        return {"transaction_ids": [result["expense_transaction_id"], result["income_transaction_id"]],
                "movement_id": result["movement_id"]}
    role = KIND_ROLES[plan["kind"]][0]
    account_id = plan["accounts"][role]
    lock_owned_accounts(cur, user_id, [account_id])
    classification = plan["classification"]
    categories = reportable_categories(cur, user_id, plan["kind"])
    category = categories[classification["category_id"]]
    kakeibo = None
    if plan["kind"] == "expense":
        kakeibo = classification.get("kakeibo") or default_pillar("expense", category)
    offset = classification.get("offset_category_id") if category["offsets_spending"] else None
    merchant = normalize_merchant((classification.get("merchant") or "")[:120])
    notes = (classification.get("description") or facts["description"] or category["name"])[:160]
    provenance = capture_provenance(by, classification.get("confidence"))
    cur.execute(
        """INSERT INTO transactions (
               user_id, account_id, category_id, type, amount, notes, date, kakeibo_type, idempotency_key,
               merchant, offset_category_id, source, review_status, classified_by, classification_confidence)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING id""",
        (user_id, account_id, classification["category_id"], plan["kind"], plan["amount"], notes, tx_date, kakeibo,
         key, merchant, offset, *provenance.columns()),
    )
    return {"transaction_ids": [str(cur.fetchone()["id"])], "movement_id": None}


def merchant_named(merchant: str | None, text: str) -> bool:
    words = [word.casefold() for word in WORD_PATTERN.findall(merchant or "")]
    folded = set(WORD_PATTERN.findall(fold(text)))
    return bool(words) and all(word in folded for word in words)


def memory_row(cur, user_id: str, match_key: str) -> dict | None:
    cur.execute(
        """SELECT match_key, key_kind, display_merchant, notes, category_id, kakeibo_type, offset_category_id,
                  account_id, source, hit_count, last_used_at, updated_at
           FROM merchant_memory WHERE user_id = %s AND match_key = %s FOR UPDATE""",
        (user_id, match_key),
    )
    row = cur.fetchone()
    return jsonable(row) if row else None


def learn(cur, user_id: str, capture: dict, draft: dict, plan: dict, recorded: dict) -> dict | None:
    """One `capture` feedback row, and memory for expense and income (D5); returns what undo must restore."""
    transaction_id = recorded["transaction_ids"][0]
    after = snapshot_rows(cur, user_id, [transaction_id])[transaction_id]
    match_key = capture_key(draft["facts"])
    insert_feedback(cur, user_id, kind="capture", before=None, after=after, key={"match_key": match_key},
                    answer={"text": capture["text"]})
    classification = plan["classification"]
    if plan["kind"] == "movement" or not match_key:
        return None
    answered = {item["type"] for item in capture["answers"]}
    taught = (bool(answered & TEACHING_ANSWERS) or classification["by"] in {"owner", "memory"}
              or merchant_named(after.get("merchant"), capture["text"]))
    before = memory_row(cur, user_id, match_key)
    if classification["by"] == "memory":
        count_memory_hit(cur, user_id, draft["memory"]["hit"]["memory_id"])
    upsert_memory(cur, user_id, match_key, {
        "merchant": after.get("merchant"), "notes": after.get("notes"), "category_id": str(after["category_id"]),
        "kakeibo_type": after.get("effective_kakeibo"),
        "offset_category_id": str(after["offset_category_id"]) if after.get("offset_category_id") else None,
        "account_id": str(after["account_id"]),
    }, "owner" if taught else "ai")
    written = memory_row(cur, user_id, match_key)
    return {"match_key": match_key, "before": before, "after_updated_at": written["updated_at"] if written else None}


def restore_memory(cur, user_id: str, learned: dict | None) -> None:
    """Undo the capture's memory write unless something changed the entry since."""
    if not learned:
        return
    current = memory_row(cur, user_id, learned["match_key"])
    if current is None or current["updated_at"] != learned["after_updated_at"]:
        return
    before = learned["before"]
    if before is None:
        cur.execute("DELETE FROM merchant_memory WHERE user_id = %s AND match_key = %s",
                    (user_id, learned["match_key"]))
        return
    cur.execute(
        """UPDATE merchant_memory SET display_merchant = %s, notes = %s, category_id = %s, kakeibo_type = %s,
               offset_category_id = %s, account_id = %s, source = %s, hit_count = %s, last_used_at = %s,
               updated_at = %s
           WHERE user_id = %s AND match_key = %s""",
        (before["display_merchant"], before["notes"], before["category_id"], before["kakeibo_type"],
         before["offset_category_id"], before["account_id"], before["source"], before["hit_count"],
         before["last_used_at"], before["updated_at"], user_id, learned["match_key"]),
    )


def advance(cur, user_id: str, capture: dict) -> dict:
    """Ask the next question or record the capture; returns the columns to store."""
    draft = capture["draft"]
    next_question, plan = evaluate(cur, user_id, draft)
    if next_question:
        return {"status": "needs_detail", "question": next_question}
    recorded = record_plan(cur, user_id, capture["id"], draft, plan)
    draft["learned"] = learn(cur, user_id, capture, draft, plan, recorded)
    draft["recorded"] = {"kind": plan["kind"], "accounts": plan["accounts"],
                         "classified_by": classified_by(draft, plan["classification"])}
    recorded_at = now()
    return {
        "status": "recorded", "question": None, "transaction_id": recorded["transaction_ids"][0],
        "movement_id": recorded["movement_id"],
        "recorded_rows": row_states(cur, user_id, recorded["transaction_ids"]),
        "recorded_at": recorded_at, "undo_until": recorded_at + UNDO_WINDOW,
    }


# --- Storage and views -------------------------------------------------------------------------


def lock_client_capture(cur, user_id: str, client_capture_id: str) -> dict | None:
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"capture:{user_id}:{client_capture_id}",))
    cur.execute("SELECT * FROM text_captures WHERE user_id = %s AND client_capture_id = %s",
                (user_id, client_capture_id))
    return cur.fetchone()


def locked_capture(cur, user_id: str, capture_id: str) -> dict:
    cur.execute("SELECT * FROM text_captures WHERE user_id = %s AND id = %s FOR UPDATE", (user_id, capture_id))
    capture = cur.fetchone()
    if not capture:
        raise HTTPException(status_code=404, detail={"code": "capture_not_found"})
    return capture


def replayed(existing: dict, request: dict) -> dict:
    if existing["text"] != request["text"]:
        raise HTTPException(status_code=409, detail={"code": "capture_id_conflict"})
    return existing


def transaction_view(cur, user_id: str, capture: dict) -> tuple[dict | None, dict | None]:
    ids = [row["id"] for row in capture["recorded_rows"] or []]
    if capture["status"] != "recorded" or not ids:
        return None, None
    cur.execute(
        """SELECT t.id, t.type, t.amount, t.date, t.account_id, a.name AS account_name, p.name AS parent_name,
                  t.merchant, t.notes, t.category_id, c.name AS category_name,
                  CASE WHEN t.type = 'expense' THEN COALESCE(t.kakeibo_type, c.kakeibo_type, 'need') END AS kakeibo,
                  t.offset_category_id, t.source, t.review_status, t.classified_by, t.movement_id, t.movement_role
           FROM transactions t
           JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
           LEFT JOIN accounts p ON p.id = a.parent_id AND p.user_id = a.user_id
           LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
           WHERE t.user_id = %s AND t.id = ANY(%s::uuid[])""",
        (user_id, ids),
    )
    rows = {str(row["id"]): row for row in cur.fetchall()}

    def view(row: dict) -> dict:
        label = f"{row['parent_name']} · {row['account_name']}" if row["parent_name"] else row["account_name"]
        return {
            "id": str(row["id"]), "type": row["type"], "amount": int(row["amount"]), "date": row["date"].isoformat(),
            "account_id": str(row["account_id"]), "account_name": label, "merchant": row["merchant"],
            "notes": row["notes"], "category_id": str(row["category_id"]) if row["category_id"] else None,
            "category_name": row["category_name"], "kakeibo": row["kakeibo"],
            "offset_category_id": str(row["offset_category_id"]) if row["offset_category_id"] else None,
            "source": row["source"], "review_status": row["review_status"], "classified_by": row["classified_by"],
        }

    first = rows.get(ids[0])
    if first is None:
        return None, None
    movement = None
    if capture["movement_id"]:
        legs = {row["movement_role"]: row for row in rows.values()}
        outbound, inbound = legs.get("outbound"), legs.get("inbound")
        if outbound and inbound:
            movement = {"id": str(capture["movement_id"]), "source": view(outbound), "target": view(inbound)}
    return view(first), movement


def capture_view(cur, user_id: str, capture: dict) -> dict:
    draft = capture["draft"]
    facts = draft["facts"]
    transaction, movement = transaction_view(cur, user_id, capture)

    def stamp(value):
        return value.isoformat() if value else None

    return {
        "id": str(capture["id"]), "client_capture_id": str(capture["client_capture_id"]),
        "status": capture["status"], "text": capture["text"], "question": capture["question"],
        "summary": {"type": draft.get("recorded", {}).get("kind") or resolved_kind(draft),
                    "amount": resolved_amount(draft), "date": facts["date"],
                    "description": facts["description"] or None},
        "transaction": transaction, "movement": movement,
        "created_at": stamp(capture["created_at"]), "recorded_at": stamp(capture["recorded_at"]),
        "undo_until": stamp(capture["undo_until"]), "undone_at": stamp(capture["undone_at"]),
    }


def log_capture(action: str, capture: dict) -> None:
    # Ids, status and reason codes only: the text and amounts are the owner's personal data.
    question_type = (capture.get("question") or {}).get("type")
    logger.info("Text capture %s id=%s status=%s classifier=%s question=%s", action, capture["id"],
                capture["status"], capture.get("classifier") or "none", question_type or "none")


# --- Operations --------------------------------------------------------------------------------


def prepare_capture(user_id: str, request: dict) -> dict:
    """Validate and read before any model call; a resend returns its capture at once."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            existing = lock_client_capture(cur, user_id, request["client_capture_id"])
            if existing:
                view = capture_view(cur, user_id, replayed(existing, request))
                conn.commit()
                return {"existing": view}
            facts, accounts, aliases = capture_facts(cur, user_id, request)
            memory = memory_state(cur, user_id, facts)
            needed = needs_model(facts, memory)
            context = model_context(cur, user_id, request, facts, memory, accounts, aliases) if needed else None
        conn.commit()
    return {"context": context, "needs_model": needed}


def store_capture(user_id: str, request: dict, model: dict | None, classifier: str | None,
                  model_name: str | None) -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            existing = lock_client_capture(cur, user_id, request["client_capture_id"])
            if existing:
                view = capture_view(cur, user_id, replayed(existing, request))
                conn.commit()
                return view
            facts, _, _ = capture_facts(cur, user_id, request)
            memory = memory_state(cur, user_id, facts)
            if memory["hit"]:
                model, classifier = None, "memory"
            capture = {
                "id": str(uuid4()), "client_capture_id": request["client_capture_id"], "text": request["text"],
                "answers": [], "classifier": classifier,
                "draft": {"facts": facts, "memory": memory, "model": model, "owner": {}},
            }
            columns = advance(cur, user_id, capture)
            cur.execute(
                """INSERT INTO text_captures (
                       id, user_id, client_capture_id, text, device_time, status, match_key, draft, question,
                       transaction_id, movement_id, recorded_rows, recorded_at, undo_until, classifier, model,
                       prompt_version, model_calls)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING *""",
                (capture["id"], user_id, request["client_capture_id"], request["text"], request["device_time"],
                 columns["status"], capture_key(facts), Jsonb(jsonable(capture["draft"])),
                 Jsonb(columns["question"]) if columns["question"] else None, columns.get("transaction_id"),
                 columns.get("movement_id"), Jsonb(columns["recorded_rows"]) if columns.get("recorded_rows") else None,
                 columns.get("recorded_at"), columns.get("undo_until"), classifier, model_name,
                 CAPTURE_PROMPT_VERSION if model_name else None, 1 if model_name else 0),
            )
            stored = cur.fetchone()
            view = capture_view(cur, user_id, stored)
        conn.commit()
    log_capture("submitted", stored)
    return view


async def submit_capture(user_id: str, raw: dict, provider) -> dict:
    from app.services.notification_processing import database_operation
    from app.services.openai_notification_provider import ProviderError

    request = validate_text_request(raw)
    prepared = await database_operation(prepare_capture, user_id, request)
    if "existing" in prepared:
        return prepared["existing"]
    model, classifier, model_name = None, None, None
    if prepared["needs_model"]:
        classifier = model_unavailable(provider)
        if classifier is None and prepared["context"] is None:
            classifier = "incomplete_context"
        if classifier is None:
            model_name = getattr(provider, "model", None) or "unknown"
            try:
                result = await provider.normalize_capture(prepared["context"])
                model = validate_capture_normalization(result, prepared["context"])
                classifier = "ok"
            except ProviderError as exc:
                classifier = exc.code
            except InvalidCaptureNormalization:
                classifier = "invalid_normalization"
    return await database_operation(store_capture, user_id, request, model, classifier, model_name)


def get_capture(user_id: str, capture_id: str) -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM text_captures WHERE user_id = %s AND id = %s", (user_id, capture_id))
            capture = cur.fetchone()
            if not capture:
                raise HTTPException(status_code=404, detail={"code": "capture_not_found"})
            return capture_view(cur, user_id, capture)


def invalid_answer() -> HTTPException:
    return HTTPException(status_code=422, detail={"code": "invalid_capture_answer"})


def answer_value(cur, user_id: str, capture: dict, current: dict, answer: dict) -> dict:
    """The owner's checked answer to the open question, as the value stored in the draft."""
    kind = current["type"]
    given = {name for name in ("option_id", "text", "account_id", "category_id", "kakeibo") if answer[name] is not None}
    option = next((item for item in current["options"] if item["id"] == answer["option_id"]), None)
    if kind == "amount":
        if given != {"text"}:
            raise invalid_answer()
        amount = parse_amount_answer(clean_text(answer["text"], ANSWER_TEXT_MAX_LENGTH, "invalid_capture_answer"))
        if amount is None:
            raise invalid_answer()
        return {"amount": amount}
    if kind == "kind":
        if given != {"option_id"} or answer["option_id"] not in KINDS:
            raise invalid_answer()
        return {"kind": answer["option_id"]}
    if kind == "account":
        if given == {"option_id"} and option:
            return {"account_id": option["id"]}
        leaves = liquid_leaves(owner_accounts(cur, user_id))
        recorded = capture["draft"]["owner"].get("accounts", {})
        other = next((value for role, value in recorded.items() if role != current["role"]), None)
        if given != {"account_id"} or answer["account_id"] not in leaves or answer["account_id"] == other:
            raise invalid_answer()
        return {"account_id": answer["account_id"]}
    draft_kind = resolved_kind(capture["draft"])
    if given == {"option_id"} and option:
        return {"category_id": option["category_id"], "kakeibo": option["kakeibo"]}
    categories = reportable_categories(cur, user_id, draft_kind)
    if not {"category_id"} <= given <= {"category_id", "kakeibo"} or answer["category_id"] not in categories:
        raise invalid_answer()
    if answer["kakeibo"] is not None and (draft_kind != "expense" or answer["kakeibo"] not in PILLARS):
        raise invalid_answer()
    category = categories[answer["category_id"]]
    return {"category_id": answer["category_id"],
            "kakeibo": answer["kakeibo"] or default_pillar(draft_kind, category)}


def apply_answer_value(draft: dict, current: dict, value: dict) -> None:
    owner = draft["owner"]
    if current["type"] == "amount":
        owner["amount"] = value["amount"]
    elif current["type"] == "kind":
        owner["kind"] = value["kind"]
        owner.pop("accounts", None)
        owner.pop("category", None)
    elif current["type"] == "account":
        owner.setdefault("accounts", {})[current["role"]] = value["account_id"]
    else:
        owner["category"] = {"category_id": value["category_id"], "kakeibo": value["kakeibo"],
                             "offset_category_id": None, "merchant": None, "description": None}
        model = draft.get("model")
        if model:
            owner["category"].update(merchant=model["merchant"], description=model["description"])


def answer_capture(user_id: str, capture_id: str, raw: dict) -> dict:
    answer = {
        "question_id": str(raw["question_id"]), "reply_id": str(raw["reply_id"]),
        "option_id": raw.get("option_id"), "text": raw.get("text"),
        "account_id": str(raw["account_id"]) if raw.get("account_id") else None,
        "category_id": str(raw["category_id"]) if raw.get("category_id") else None,
        "kakeibo": raw.get("kakeibo"),
    }
    with db_conn() as conn:
        with conn.cursor() as cur:
            capture = locked_capture(cur, user_id, capture_id)
            previous = next((item for item in capture["answers"] if item["reply_id"] == answer["reply_id"]), None)
            if previous is not None:
                if {key: previous["request"].get(key) for key in answer} != answer:
                    raise HTTPException(status_code=409, detail={"code": "capture_answer_conflict"})
                view = capture_view(cur, user_id, capture)
                conn.commit()
                return view
            current = capture["question"]
            if capture["status"] != "needs_detail" or not current or current["question_id"] != answer["question_id"]:
                raise HTTPException(status_code=409, detail={"code": "stale_capture_question"})
            value = answer_value(cur, user_id, capture, current, answer)
            draft = capture["draft"]
            apply_answer_value(draft, current, value)
            capture["answers"] = [*capture["answers"], {
                "reply_id": answer["reply_id"], "question_id": answer["question_id"], "type": current["type"],
                "role": current["role"], "request": answer, "value": value, "answered_at": now().isoformat(),
            }]
            columns = advance(cur, user_id, {**capture, "id": str(capture["id"]), "draft": draft})
            cur.execute(
                """UPDATE text_captures SET status = %s, draft = %s, question = %s, answers = %s,
                       transaction_id = %s, movement_id = %s, recorded_rows = %s, recorded_at = %s, undo_until = %s,
                       updated_at = NOW()
                   WHERE user_id = %s AND id = %s RETURNING *""",
                (columns["status"], Jsonb(jsonable(draft)),
                 Jsonb(columns["question"]) if columns["question"] else None, Jsonb(capture["answers"]),
                 columns.get("transaction_id"), columns.get("movement_id"),
                 Jsonb(columns["recorded_rows"]) if columns.get("recorded_rows") else None,
                 columns.get("recorded_at"), columns.get("undo_until"), user_id, capture_id),
            )
            stored = cur.fetchone()
            view = capture_view(cur, user_id, stored)
        conn.commit()
    log_capture("answered", stored)
    return view


def undo_capture(user_id: str, capture_id: str) -> dict:
    """Remove the recorded rows within the undo window, if they are unchanged (D6)."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            capture = locked_capture(cur, user_id, capture_id)
            if capture["status"] == "undone":
                view = capture_view(cur, user_id, capture)
                conn.commit()
                return view
            if capture["status"] != "recorded":
                raise HTTPException(status_code=409, detail={"code": "capture_not_recorded"})
            if now() > capture["undo_until"] + UNDO_GRACE:
                raise HTTPException(status_code=409, detail={"code": "undo_expired"})
            recorded = capture["recorded_rows"] or []
            if row_states(cur, user_id, [row["id"] for row in recorded], lock=True) != recorded:
                raise HTTPException(status_code=409, detail={"code": "capture_transaction_changed"})
            if capture["movement_id"]:
                delete_movement_rows(cur, user_id, str(capture["movement_id"]))
            else:
                delete_transaction_rows(cur, user_id, recorded[0]["id"])
            restore_memory(cur, user_id, capture["draft"].get("learned"))
            cur.execute(
                """UPDATE text_captures SET status = 'undone', undone_at = %s, transaction_id = NULL, updated_at = NOW()
                   WHERE user_id = %s AND id = %s RETURNING *""",
                (now(), user_id, capture_id),
            )
            stored = cur.fetchone()
            view = capture_view(cur, user_id, stored)
        conn.commit()
    log_capture("undone", stored)
    return view
