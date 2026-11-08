"""The "Apa ini?" question for notifications that name no merchant (design D9–D14)."""

from __future__ import annotations

import logging
import math
import unicodedata
from datetime import datetime
from uuid import uuid4

from psycopg.types.json import Jsonb

from app.core.config import settings
from app.db.pool import db_conn
from app.errors import ErrorCode, api_error
from app.i18n import account_language, format_money, t
from app.services.habits import HabitDecision, guess_label
from app.services.merchant_memory import (
    insert_feedback, live_feedback_sql, is_masked, is_movement_category, learn_from_owner, local_time_features,
    owner_identity, snapshot_rows, transaction_event_key,
)
from app.services.movement_pairing import await_counterpart, pair_answered_transfer
from app.services.notification_mapping import version_tuple
from app.services.prompt_language import PURPOSE_NORMALIZATION, select_prompt
from app.services.purpose_normalization import InvalidNormalization, validate_normalization
from app.services.reimbursements import validate_offset_category
from app.services.system_categories import is_non_reportable
from app.services.review_undo import ActionRecorder, null_token


logger = logging.getLogger(__name__)

PURPOSE_MIN_VERSION = (1, 8, 0)
SUGGESTION_LIMIT = 5
FEEDBACK_EXAMPLE_LIMIT = 300
TEXT_MAX_LENGTH = 200
TRANSFER_HINTS = frozenset({"transfer masuk", "transfer rekening", "account transfer"})


def supports_purpose_question(event: dict) -> bool:
    version = version_tuple(event.get("source_version"))
    return version is not None and version >= PURPOSE_MIN_VERSION


def amount_band(amount: int | None) -> int | None:
    return int(math.floor(math.log2(amount))) if amount and amount > 0 else None


def base_signature(signature: str | None) -> str | None:
    return signature.split("|from:", 1)[0] if signature else None


def hour_distance(left: int, right: int) -> int:
    difference = abs(left - right) % 24
    return min(difference, 24 - difference)


def example_score(example: dict, *, signature: str | None, account_id: str, amount: int,
                  weekday: int | None, hour: int | None) -> int:
    score = 0
    if signature and example.get("text_signature") == signature:
        score += 4
    elif signature and base_signature(example.get("text_signature")) == base_signature(signature):
        score += 2
    if str(example.get("account_id")) == account_id:
        score += 2
    band, example_band = amount_band(amount), amount_band(example.get("amount"))
    if band is not None and example_band is not None:
        if band == example_band:
            score += 2
        elif abs(band - example_band) == 1:
            score += 1
    if hour is not None and example.get("local_hour") is not None and hour_distance(hour, example["local_hour"]) <= 2:
        score += 1
    if weekday is not None and example.get("local_weekday") == weekday:
        score += 1
    return score


def classification_group(values: dict) -> tuple:
    def folded(value):
        return unicodedata.normalize("NFKC", value or "").casefold().strip()

    return (folded(values.get("merchant")), folded(values.get("notes")), str(values.get("category_id")),
            values.get("kakeibo"), str(values.get("offset_category_id")))


def owner_categories(cur, user_id: str) -> dict[str, dict]:
    cur.execute(
        """SELECT id, name, kind, kakeibo_type, offsets_spending, is_archived, system_key,
                  COALESCE(is_excluded_from_budget, FALSE) AS is_excluded_from_budget
           FROM categories WHERE user_id = %s""",
        (user_id,),
    )
    return {str(row["id"]): row for row in cur.fetchall()}


def suggestion(kind: str, values: dict, categories: dict[str, dict], language: str) -> dict:
    category = categories.get(str(values.get("category_id"))) if values.get("category_id") else None
    offset = categories.get(str(values.get("offset_category_id"))) if values.get("offset_category_id") else None
    notes = values.get("notes") or None
    merchant = values.get("merchant") or None
    label = notes or merchant or (category["name"] if category else t(language, "labels.fallback.other"))
    return {
        "kind": kind, "label": label[:160],
        "merchant": merchant, "notes": notes,
        "category_id": str(category["id"]) if category else None,
        "category_name": category["name"] if category else None,
        "kakeibo": values.get("kakeibo"),
        "offset_category_id": str(offset["id"]) if offset else None,
        "offset_category_name": offset["name"] if offset else None,
    }


def habit_suggestion(habit: HabitDecision, categories: dict[str, dict], language: str) -> dict | None:
    """The habit's best guess, offered first as "Sepertinya <kategori>?" (learn-habits-to-ask-less D8)."""
    category = categories.get(habit.category_id)
    if not category or category["is_archived"] or category["kind"] != "expense":
        return None
    values = {"merchant": habit.merchant, "notes": habit.notes or category["name"],
              "category_id": habit.category_id, "kakeibo": habit.kakeibo, "offset_category_id": None}
    guess = guess_label(language, category["name"])
    return {**suggestion("habit", values, categories, language), "label": guess["text"][:160],
            "label_code": guess["code"], "label_params": guess["params"],
            "basis": {"agreeing": habit.agreeing, "support": habit.support}}


def rank_suggestions(
    cur, user_id: str, *, direction: str, signature: str | None, account_id: str, amount: int,
    moment: datetime, current: dict | None, repayment_note: str | None, language: str,
    habit: HabitDecision | None = None,
) -> list[dict]:
    """The habit's guess, then owner history, the current classification and a repayment option (design D10)."""
    categories = owner_categories(cur, user_id)
    weekday, hour = local_time_features(moment)
    cur.execute(
        f"""SELECT f.after_merchant, f.after_notes, f.after_category_id, f.after_kakeibo_type,
                   f.after_offset_category_id, f.text_signature, f.account_id, f.amount, f.local_weekday,
                   f.local_hour, f.created_at
            FROM classification_feedback f
            WHERE f.user_id = %s AND f.after_type = %s AND f.after_category_id IS NOT NULL
              AND {live_feedback_sql("f")}
            ORDER BY f.created_at DESC, f.id LIMIT %s""",
        (user_id, direction, FEEDBACK_EXAMPLE_LIMIT),
    )
    groups: dict[tuple, dict] = {}
    for example in cur.fetchall():
        category = categories.get(str(example["after_category_id"]))
        if not category or category["is_archived"] or category["kind"] != direction:
            continue
        score = example_score(example, signature=signature, account_id=account_id, amount=amount,
                              weekday=weekday, hour=hour)
        if score == 0:
            continue
        values = {
            "merchant": example["after_merchant"], "notes": example["after_notes"],
            "category_id": example["after_category_id"],
            "kakeibo": example["after_kakeibo_type"] if direction == "expense" else None,
            "offset_category_id": example["after_offset_category_id"],
        }
        group = groups.setdefault(classification_group(values), {"values": values, "score": 0,
                                                                 "latest": example["created_at"]})
        group["score"] += score
    ranked = sorted(groups.values(), key=lambda item: (item["score"], item["latest"]), reverse=True)
    guess = habit_suggestion(habit, categories, language) if habit is not None and direction == "expense" else None
    if guess is not None:
        guessed = classification_group(guess)
        ranked = [item for item in ranked if classification_group(item["values"]) != guessed]
    history_slots = SUGGESTION_LIMIT - (1 if guess is not None else 0)
    suggestions = ([guess] if guess is not None else []) + [
        suggestion("history", item["values"], categories, language) for item in ranked[:history_slots]
    ]
    seen = {classification_group(item) for item in suggestions}
    if current and current.get("category_id") and classification_group(current) not in seen:
        if len(suggestions) < SUGGESTION_LIMIT:
            suggestions.append(suggestion("current", current, categories, language))
    if repayment_note is not None:
        repayment = next((row for row in sorted(categories.values(), key=lambda row: row["name"])
                          if row["kind"] == "income" and row["offsets_spending"] and not row["is_archived"]), None)
        if repayment and not any(item["category_id"] == str(repayment["id"]) for item in suggestions):
            entry = suggestion("repayment", {"notes": repayment_note, "category_id": repayment["id"]}, categories, language)
            if len(suggestions) >= SUGGESTION_LIMIT:
                suggestions[SUGGESTION_LIMIT - 1] = entry
            else:
                suggestions.append(entry)
    return [{"id": f"s{index}", **item} for index, item in enumerate(suggestions, start=1)]


def sender_display(event: dict, facts) -> str | None:
    resolution = event.get("sender_resolution") or {}
    if resolution.get("action") == "name" and resolution.get("name"):
        return resolution["name"]
    counterparty = (facts.parsed.counterparty or "").strip()
    return counterparty if counterparty and is_masked(counterparty) else None


def purpose_prompt(
    language: str, region: str, *, direction: str, amount: int, account: str, hint: str | None, sender: str | None,
) -> dict:
    """The question's text in the account's language, with the code and parameters it was rendered from (D5)."""
    if direction == "expense":
        code, params = ("questions.purpose.expense_hint", {"hint": hint}) if hint else ("questions.purpose.expense", {})
    else:
        code, params = ("questions.purpose.income_sender", {"sender": sender}) if sender else ("questions.purpose.income", {})
    params = {"amount": amount, "account": account, **params}
    text = t(language, code, **{**params, "amount": format_money(amount, region, "inline")})
    return {"text": text, "code": code, "params": params}


def is_transfer_income(facts) -> bool:
    hint = (facts.parsed.category_hint or "").casefold().strip()
    return facts.direction == "income" and (hint in TRANSFER_HINTS or is_masked(facts.parsed.counterparty or ""))


def attach_purpose_question(
    cur, event: dict, facts, context: dict, *, observed: dict, transaction_id: str, key: dict, current: dict,
) -> dict:
    """Open a purpose question on a recorded event; a new question id every time."""
    from app.services.notification_application import account_label

    user_id = str(event["user_id"])
    # Stored with the event in the account's language at creation, and never rewritten (D5, D6).
    language, region = account_language(cur, user_id)
    sender = sender_display(event, facts) if facts.direction == "income" else None
    repayment_note = None
    if is_transfer_income(facts):
        name = (event.get("sender_resolution") or {}).get("name")
        repayment_note = t(language, "notes.repayment_named", name=name) if name else t(language, "notes.repayment_shared")
    hint = (facts.parsed.category_hint or "").strip() or None
    learned = context.get("_habit")
    habit = learned["decision"] if learned and learned["account_id"] == str(observed["id"]) else None
    prompt = purpose_prompt(language, region, direction=facts.direction, amount=facts.amount,
                            account=account_label(observed, context["accounts"]),
                            hint=hint if facts.direction == "expense" else None, sender=sender)
    question = {
        "type": "purpose", "question_id": str(uuid4()), "transaction_id": transaction_id,
        "direction": facts.direction, "amount": facts.amount, "currency": facts.currency,
        "language": language,
        "prompt": prompt["text"], "prompt_code": prompt["code"], "prompt_params": prompt["params"],
        "allows_text": True, "text_max_length": TEXT_MAX_LENGTH,
        "suggestions": rank_suggestions(
            cur, user_id, direction=facts.direction, signature=key.get("text_signature"),
            account_id=str(observed["id"]), amount=facts.amount, moment=facts.timestamp,
            current=current, repayment_note=repayment_note, language=language, habit=habit,
        ),
    }
    cur.execute(
        "UPDATE notification_events SET active_question = %s WHERE user_id = %s AND id = %s",
        (Jsonb(question), user_id, str(event["id"])),
    )
    event["active_question"] = question
    # The message key, language and count only: the rendered prompt holds an amount and account names (D8).
    logger.info("Purpose question asked code=%s language=%s suggestions=%d",
                prompt["code"], language, len(question["suggestions"]))
    return question


def open_purpose_question(event: dict) -> dict | None:
    question = event.get("active_question") or {}
    return question if question.get("type") == "purpose" else None


def validate_answer_body(answer: dict) -> dict:
    suggestion_id, text = answer.get("suggestion_id"), answer.get("text")
    if (suggestion_id is None) == (text is None):
        raise api_error(422, ErrorCode.INVALID_PURPOSE_ANSWER)
    if text is not None:
        text = unicodedata.normalize("NFKC", text).strip()
        if (not 1 <= len(text) <= TEXT_MAX_LENGTH
                or any(unicodedata.category(char).startswith("C") for char in text)):
            raise api_error(422, ErrorCode.INVALID_PURPOSE_ANSWER)
    elif not 1 <= len(suggestion_id) <= 16:
        raise api_error(422, ErrorCode.INVALID_PURPOSE_ANSWER)
    return {"question_id": str(answer["question_id"]), "reply_id": str(answer["reply_id"]),
            "suggestion_id": suggestion_id, "text": text}


def locked_event(cur, user_id: str, event_id: str) -> dict:
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
    cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND id = %s FOR UPDATE", (user_id, event_id))
    event = cur.fetchone()
    if not event:
        raise api_error(404, ErrorCode.EVENT_NOT_FOUND)
    return event


def receipt(cur, user_id: str, reply_id: str) -> dict | None:
    cur.execute(
        """SELECT f.event_id, f.question_id, f.suggestion_id, f.answer_text,
                  EXISTS (SELECT 1 FROM classification_feedback reverting
                          WHERE reverting.user_id = f.user_id AND reverting.reverts_feedback_id = f.id) AS undone
           FROM classification_feedback f
           WHERE f.user_id = %s AND f.reply_id = %s""",
        (user_id, reply_id),
    )
    return cur.fetchone()


def replay_matches(row: dict, event_id: str, answer: dict) -> bool:
    if (str(row["event_id"]) != event_id or str(row["question_id"]) != answer["question_id"]
            or row["suggestion_id"] != answer["suggestion_id"] or row["answer_text"] != answer["text"]):
        raise api_error(409, ErrorCode.PURPOSE_ANSWER_CONFLICT)
    if row["undone"]:
        # The owner took this answer back in Tinjau; a late retry must not apply it again (D13).
        raise api_error(409, ErrorCode.PURPOSE_ANSWER_UNDONE)
    return True


def answerable_transaction(cur, user_id: str, question: dict) -> dict:
    cur.execute(
        """SELECT id, type, movement_id FROM transactions WHERE user_id = %s AND id = %s FOR UPDATE""",
        (user_id, question["transaction_id"]),
    )
    row = cur.fetchone()
    if not row or row["movement_id"] or row["type"] not in {"expense", "income"}:
        raise api_error(409, ErrorCode.PURPOSE_NOT_APPLICABLE)
    return row


def checked_question(cur, event: dict, answer: dict) -> tuple[dict, dict, dict | None]:
    question = open_purpose_question(event)
    if not question or question["question_id"] != answer["question_id"]:
        raise api_error(409, ErrorCode.STALE_QUESTION)
    transaction = answerable_transaction(cur, str(event["user_id"]), question)
    chosen = None
    if answer["suggestion_id"] is not None:
        chosen = next((item for item in question.get("suggestions", []) if item["id"] == answer["suggestion_id"]), None)
        if chosen is None:
            raise api_error(422, ErrorCode.UNKNOWN_SUGGESTION)
    return question, transaction, chosen


def normalizer_context(cur, user_id: str, event: dict, question: dict, text: str) -> dict:
    from app.services.notification_context import sanitize_text

    identity = owner_identity(cur, user_id)
    direction = question["direction"]
    owned = owner_categories(cur, user_id).values()
    categories = [row for row in owned if row["kind"] == direction and not row["is_archived"]]
    offsets = []
    if direction == "income":
        offsets = [row for row in owned
                   if row["kind"] == "expense" and not row["is_archived"] and not row["is_excluded_from_budget"]
                   and not is_non_reportable(row["system_key"])]
    signature_parts = ((event.get("interpretation") or {}).get("text_signature") or "").split("|")
    hint = signature_parts[2] if len(signature_parts) > 2 else None
    return {
        "answer": sanitize_text(text, identity),
        "direction": direction, "amount": question["amount"], "currency": question["currency"],
        "transaction_prompt": sanitize_text(question["prompt"], identity), "bank_category": hint,
        "categories": [{"id": str(row["id"]), "name": sanitize_text(row["name"], identity)[:100],
                        "default_kakeibo": row["kakeibo_type"], "offsets_spending": row["offsets_spending"]}
                       for row in sorted(categories, key=lambda row: row["name"])],
        "offset_categories": [{"id": str(row["id"]), "name": sanitize_text(row["name"], identity)[:100]}
                              for row in sorted(offsets, key=lambda row: row["name"])],
        "suggestions": [{"label": sanitize_text(item["label"], identity), "category_id": item["category_id"],
                         "kakeibo": item["kakeibo"]} for item in question.get("suggestions", [])],
    }


def prepare_answer(event_id: str, user_id: str, answer: dict) -> dict:
    """Validate before any model call; a replay returns its stored outcome at once."""
    from app.services.notification_processing import compact_result

    with db_conn() as conn:
        with conn.cursor() as cur:
            event = locked_event(cur, user_id, event_id)
            if (row := receipt(cur, user_id, answer["reply_id"])) and replay_matches(row, event_id, answer):
                result = {"replay": {**answer_response(cur, event, compact_result(cur, event)), **null_token()}}
                conn.commit()
                return result
            question, _, _ = checked_question(cur, event, answer)
            context = normalizer_context(cur, user_id, event, question, answer["text"]) if answer["text"] else None
            prompt = select_prompt(PURPOSE_NORMALIZATION, account_language(cur, user_id)[0]) if answer["text"] else None
        conn.commit()
    return {"normalizer_context": context, "prompt": prompt}


def answer_values(cur, user_id: str, transaction: dict, chosen: dict | None, normalized: dict | None,
                  text: str | None) -> tuple[dict, str]:
    """Final values and review status of an answer (design D11, D12)."""
    direction = transaction["type"]
    if chosen is not None:
        values = {"merchant": chosen["merchant"], "notes": chosen["notes"] or chosen["label"],
                  "category_id": chosen["category_id"],
                  "kakeibo_type": chosen["kakeibo"] if direction == "expense" else None,
                  "offset_category_id": chosen["offset_category_id"]}
        if values["category_id"] is None:
            raise api_error(422, ErrorCode.INVALID_SUGGESTION)
    elif normalized is not None and normalized["category_id"]:
        values = normalized
    elif normalized is not None:
        kept = {"notes": normalized["notes"]}
        if normalized["merchant"]:
            kept["merchant"] = normalized["merchant"]
        return kept, "needs_review"
    else:
        return {"notes": text}, "needs_review"
    cur.execute(
        """SELECT kind, offsets_spending FROM categories
           WHERE user_id = %s AND id = %s AND is_archived = FALSE""",
        (user_id, values["category_id"]),
    )
    category = cur.fetchone()
    if not category or category["kind"] != direction:
        raise api_error(422, ErrorCode.INVALID_SUGGESTION)
    if values.get("offset_category_id"):
        if direction != "income" or not category["offsets_spending"]:
            values["offset_category_id"] = None
        else:
            validate_offset_category(cur, user_id, values["offset_category_id"])
    return values, "reviewed"


def apply_answer(event_id: str, user_id: str, answer: dict, normalized: dict | None, normalizer: str | None,
                 prompt_version: str | None) -> dict:
    from app.services.notification_processing import compact_result

    with db_conn() as conn:
        with conn.cursor() as cur:
            event = locked_event(cur, user_id, event_id)
            if (row := receipt(cur, user_id, answer["reply_id"])) and replay_matches(row, event_id, answer):
                response = {**answer_response(cur, event, compact_result(cur, event)), **null_token()}
                conn.commit()
                return response
            question, transaction, chosen = checked_question(cur, event, answer)
            transaction_id = str(transaction["id"])
            values, review_status = answer_values(cur, user_id, transaction, chosen, normalized, answer["text"])
            recorder = ActionRecorder(cur, user_id, "purpose_answer")
            recorder.capture([transaction_id], event_ids=[event_id], learns=review_status == "reviewed",
                             merchants=[values.get("merchant")])
            before = snapshot_rows(cur, user_id, [transaction_id])[transaction_id]
            assignments = ", ".join(f"{field} = %s" for field in values)
            cur.execute(
                f"""UPDATE transactions SET {assignments}, review_status = %s, classified_by = 'owner',
                       classification_confidence = NULL, owner_edited_at = NOW(), updated_at = NOW()
                   WHERE user_id = %s AND id = %s""",
                (*values.values(), review_status, user_id, transaction_id),
            )
            after = snapshot_rows(cur, user_id, [transaction_id])[transaction_id]
            key = transaction_event_key(cur, user_id, transaction_id)
            insert_feedback(cur, user_id, kind="purpose_answer", before=before, after=after,
                            key={**key, "event_id": event_id}, answer=answer)
            if review_status == "reviewed" and is_movement_category(after["category_system_key"]):
                # A transfer answer names one side of a movement: link the other now, or keep asking (D4).
                if not pair_answered_transfer(cur, user_id, transaction_id, recorder):
                    await_counterpart(cur, user_id, transaction_id)
                    review_status = "needs_review"
            elif review_status == "reviewed":
                learn_from_owner(cur, user_id, after, key)
            outcome = {
                "question_id": question["question_id"], "reply_id": answer["reply_id"],
                "suggestion_id": answer["suggestion_id"], "review_status": review_status,
                "normalizer": normalizer, "normalizer_prompt_version": prompt_version if answer["text"] else None,
            }
            cur.execute(
                """UPDATE notification_events SET active_question = NULL,
                       interpretation = COALESCE(interpretation, '{}'::jsonb) || %s,
                       result_snapshot = jsonb_set(COALESCE(result_snapshot, '{}'::jsonb), '{description}', to_jsonb(%s::text)),
                       updated_at = NOW()
                   WHERE user_id = %s AND id = %s RETURNING *""",
                (Jsonb({"purpose_answer": outcome}), (after["notes"] or "")[:160], user_id, event_id),
            )
            event = cur.fetchone()
            response = {**answer_response(cur, event, compact_result(cur, event)), **recorder.finish()}
        conn.commit()
    # Reason codes only: answers and transaction content are personal data.
    logger.info("Purpose answer applied review_status=%s normalizer=%s", review_status, normalizer or "none")
    return response


def answer_response(cur, event: dict, result: dict) -> dict:
    stored = (event.get("interpretation") or {}).get("purpose_answer") or {}
    transaction_id = event.get("transaction_id")
    summary = None
    if transaction_id:
        rows = snapshot_rows(cur, str(event["user_id"]), [str(transaction_id)])
        row = rows.get(str(transaction_id))
        if row:
            summary = {
                "status": "applied" if stored.get("review_status") == "reviewed" else "needs_review",
                "transaction_id": str(row["id"]), "merchant": row["merchant"], "notes": row["notes"],
                "category_id": str(row["category_id"]) if row["category_id"] else None,
                "category_name": row["category_name"], "kakeibo": row["kakeibo_type"],
                "offset_category_id": str(row["offset_category_id"]) if row["offset_category_id"] else None,
                "review_status": row.get("review_status"),
            }
    return {"result": result, "answer": summary}


async def answer_purpose(event_id: str, user_id: str, raw_answer: dict, provider) -> dict:
    from app.services.notification_processing import database_operation, settings as processing_settings
    from app.services.openai_notification_provider import ProviderError, configuration_error

    answer = validate_answer_body(raw_answer)
    prepared = await database_operation(prepare_answer, event_id, user_id, answer)
    if "replay" in prepared:
        return prepared["replay"]
    normalized, normalizer = None, None
    if answer["text"] is not None:
        context = prepared["normalizer_context"]
        if provider is None or configuration_error(processing_settings):
            normalizer = "ai_disabled"
        else:
            try:
                result = await provider.normalize_purpose(context, prompt=prepared["prompt"])
                normalized = validate_normalization(
                    result, context, context["direction"], output_language=prepared["prompt"].language,
                )
                normalizer = "ok" if normalized["category_id"] else "no_category"
            except ProviderError as exc:
                normalizer = exc.code
            except InvalidNormalization:
                normalizer = "invalid_normalization"
    prompt_version = prepared["prompt"].version if prepared["prompt"] else None
    return await database_operation(apply_answer, event_id, user_id, answer, normalized, normalizer, prompt_version)


def list_open_questions(user_id: str, limit: int) -> list[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT n.id AS event_id, n.post_time, n.active_question, t.id, t.type, t.amount, t.date,
                          t.account_id, a.name AS account_name, t.notes, t.merchant, t.category_id,
                          c.name AS category_name, t.kakeibo_type, t.review_status
                   FROM notification_events n
                   JOIN transactions t ON t.user_id = n.user_id
                        AND t.id = (n.active_question ->> 'transaction_id')::uuid
                   JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
                   LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
                   WHERE n.user_id = %s AND n.active_question ->> 'type' = 'purpose'
                     AND n.processing_state = 'recorded' AND t.movement_id IS NULL
                   ORDER BY n.post_time DESC, n.id LIMIT %s""",
                (user_id, limit),
            )
            rows = cur.fetchall()
    return [{
        "event_id": str(row["event_id"]), "post_time": row["post_time"].isoformat(),
        "question": row["active_question"],
        "transaction": {
            "id": str(row["id"]), "type": row["type"], "amount": row["amount"],
            "date": row["date"].isoformat() if row["date"] else None,
            "account_id": str(row["account_id"]), "account_name": row["account_name"],
            "notes": row["notes"], "merchant": row["merchant"],
            "category_id": str(row["category_id"]) if row["category_id"] else None,
            "category_name": row["category_name"], "kakeibo_type": row["kakeibo_type"],
            "review_status": row["review_status"],
        },
    } for row in rows]
