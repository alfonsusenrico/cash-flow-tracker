"""Capture suggestions from the owner's own typed and manual history (add-capture-suggestions D1–D3).

Ranking is plain counting over the last 90 days; no model is called. Bank-notified rows never take part.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from app.core.config import settings
from app.db.pool import db_conn
from app.services.capture_parsing import (
    DAY_DATE_PATTERN, MONEY_PATTERN, RELATIVE_DATE_PATTERN, SLASH_DATE_PATTERN, fold, is_money_expression,
    parse_kind,
)
from app.services.merchant_memory import merchant_match_key
from app.services.reimbursements import NON_REPORTABLE_CATEGORY_NAMES
from app.services.text_capture import DEVICE_CLOCK_SKEW, TEXT_MAX_LENGTH

SUGGESTION_LIMIT = 6
HISTORY_DAYS = 90
HOUR_WINDOW_MINUTES = 120
MINUTES_PER_DAY = 24 * 60
RECENT_DAYS = 7
RECENT_WEIGHT = 1.0
MONTH_DAYS = 30
MONTH_WEIGHT = 0.5
HOUR_WEIGHT = 3
WEEKDAY_TYPE_WEIGHT = 2
FREQUENT_COUNT = 3
REASON_TIME = "biasanya jam segini"
REASON_FREQUENT = "sering"
REASON_RECENT = "terakhir"
DATE_PATTERNS = (RELATIVE_DATE_PATTERN, DAY_DATE_PATTERN, SLASH_DATE_PATTERN)
# A prefill text must parse back to the suggested kind, so a bare label gets the verb the word table knows.
KIND_VERBS = {"expense": "bayar", "income": "terima"}
WEEKEND_DAYS = frozenset({6, 7})


@dataclass(frozen=True)
class Entry:
    key: str
    moment: datetime
    undone: bool
    kind: str | None = None
    amount: int | None = None
    label: str | None = None
    category_name: str | None = None
    account_name: str | None = None
    capture_text: str | None = None
    capture_amount: int | None = None


def parse_device_time(value: str) -> datetime:
    """The device's current time; it must carry an offset and not run ahead of the server (D3)."""
    try:
        device_time = datetime.fromisoformat(value.strip())
    except ValueError:
        raise HTTPException(status_code=422, detail={"code": "invalid_device_time"}) from None
    if device_time.tzinfo is None or device_time > datetime.now(timezone.utc) + DEVICE_CLOCK_SKEW:
        raise HTTPException(status_code=422, detail={"code": "invalid_device_time"})
    return device_time


# --- Candidates (D1) ---------------------------------------------------------------------------


def entry_key(capture_key: str | None, merchant: str | None, notes: str | None) -> str | None:
    return capture_key or merchant_match_key(merchant) or merchant_match_key(notes)


def ledger_entries(cur, user_id: str, since: datetime) -> list[Entry]:
    """Owner-reviewed manual and capture rows; a recorded capture brings its text to its row."""
    cur.execute(
        """SELECT t.type, t.amount, t.date, t.merchant, t.notes, c.name AS category_name,
                  a.name AS account_name, p.name AS parent_name, tc.text AS capture_text,
                  tc.match_key AS capture_key, tc.draft -> 'facts' ->> 'amount' AS capture_amount
           FROM transactions t
           JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
           LEFT JOIN accounts p ON p.id = a.parent_id AND p.user_id = a.user_id
           LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
           LEFT JOIN text_captures tc ON tc.user_id = t.user_id AND tc.transaction_id = t.id
                AND tc.status = 'recorded'
           WHERE t.user_id = %s AND t.source IN ('manual', 'capture') AND t.review_status = 'reviewed'
             AND t.type IN ('expense', 'income') AND t.movement_id IS NULL AND t.fee_for_movement_id IS NULL
             AND (c.id IS NULL OR c.name <> ALL(%s)) AND t.date > %s""",
        (user_id, list(NON_REPORTABLE_CATEGORY_NAMES), since),
    )
    entries = []
    for row in cur.fetchall():
        key = entry_key(row["capture_key"], row["merchant"], row["notes"])
        if not key:
            continue
        account = f"{row['parent_name']} · {row['account_name']}" if row["parent_name"] else row["account_name"]
        entries.append(Entry(
            key=key, moment=row["date"], undone=False, kind=row["type"], amount=int(row["amount"]),
            label=(row["merchant"] or row["notes"] or row["category_name"] or "").strip() or None,
            category_name=row["category_name"], account_name=account, capture_text=row["capture_text"],
            capture_amount=int(row["capture_amount"]) if row["capture_amount"] else None,
        ))
    return entries


def undone_entries(cur, user_id: str, since: datetime) -> list[Entry]:
    """Undone expense and income captures; they only stop their group from being suggested (D2)."""
    cur.execute(
        """SELECT match_key, draft -> 'facts' ->> 'date' AS moment FROM text_captures
           WHERE user_id = %s AND status = 'undone' AND movement_id IS NULL AND match_key IS NOT NULL
             AND recorded_at > %s""",
        (user_id, since),
    )
    return [Entry(key=row["match_key"], moment=datetime.fromisoformat(row["moment"]), undone=True)
            for row in cur.fetchall()]


# --- Grouping and score (D1, D2) -----------------------------------------------------------------


def most_common(entries: list[Entry], field: str):
    """The most frequent non-empty value of a field; the most recently used one wins a tie."""
    counts: Counter = Counter()
    latest: dict = {}
    for entry in entries:
        value = getattr(entry, field)
        if value is None:
            continue
        counts[value] += 1
        latest[value] = max(latest.get(value, entry.moment), entry.moment)
    if not counts:
        return None
    return max(counts, key=lambda value: (counts[value], latest[value]))


def local_clock(moment: datetime) -> datetime:
    return moment.astimezone(ZoneInfo(settings.tz))


def within_hour_window(moment: datetime, reference: datetime) -> bool:
    minutes = [clock.hour * 60 + clock.minute for clock in (local_clock(moment), local_clock(reference))]
    distance = abs(minutes[0] - minutes[1])
    return min(distance, MINUTES_PER_DAY - distance) <= HOUR_WINDOW_MINUTES


def is_weekend(moment: datetime) -> bool:
    return local_clock(moment).isoweekday() in WEEKEND_DAYS


def recency(last_used: datetime, reference: datetime) -> float:
    age = max(reference - last_used, timedelta(0))
    if age <= timedelta(days=RECENT_DAYS):
        return RECENT_WEIGHT
    if age <= timedelta(days=MONTH_DAYS):
        return MONTH_WEIGHT
    return 0.0


def amount_shorthand(amount: int) -> str:
    """The amount as the owner would type it ("10rb", "1,5jt"); it parses back to the same value."""
    for unit, size, places in (("jt", 1_000_000, 2), ("rb", 1_000, 2)):
        if amount >= size and amount % (size // 10 ** places) == 0:
            whole, rest = divmod(amount, size)
            decimals = f"{rest * 10 ** places // size:0{places}d}".rstrip("0")
            return f"{whole},{decimals}{unit}" if decimals else f"{whole}{unit}"
    if amount >= 1_000:
        return f"{amount:,}".replace(",", ".")
    return str(amount) if amount >= 100 else f"rp{amount}"


def without_money_and_dates(text: str) -> str:
    spans = [match.span() for pattern in DATE_PATTERNS for match in pattern.finditer(text)]
    spans += [match.span() for match in MONEY_PATTERN.finditer(text) if is_money_expression(match)]
    kept = "".join(char for index, char in enumerate(text) if not any(start <= index < end for start, end in spans))
    return " ".join(kept.split())


def has_date_words(text: str) -> bool:
    return any(pattern.search(fold(text)) for pattern in DATE_PATTERNS)


def built_text(label: str, kind: str, amount: int) -> str:
    """"<label> <amount shorthand>" from a label that may hold its own numbers or dates (D1)."""
    words = without_money_and_dates(fold(label))
    if parse_kind(words)[0] != kind:
        words = f"{KIND_VERBS[kind]} {words}".strip()
    shorthand = amount_shorthand(amount)
    words = words[:TEXT_MAX_LENGTH - len(shorthand) - 1].rstrip()
    return f"{words} {shorthand}" if words else shorthand


def prefill_text(used: list[Entry], kind: str, amount: int, label: str) -> str:
    """The latest typed text holding exactly this amount and no date words, else a built text."""
    for entry in sorted(used, key=lambda item: item.moment, reverse=True):
        if (entry.capture_text and entry.amount == amount and entry.capture_amount == amount
                and not has_date_words(entry.capture_text)):
            return entry.capture_text
    return built_text(label, kind, amount)


def suggestion(entries: list[Entry], reference: datetime) -> tuple[tuple, dict] | None:
    """(sort key, suggestion) for one merchant key, or None when its last entry was undone (D2)."""
    if max(entries, key=lambda entry: (entry.moment, entry.undone)).undone:
        return None
    used = [entry for entry in entries if not entry.undone]
    count = len(used)
    hour_share = sum(within_hour_window(entry.moment, reference) for entry in used) / count
    weekday_share = sum(is_weekend(entry.moment) == is_weekend(reference) for entry in used) / count
    last_used = max(entry.moment for entry in used)
    frequency = math.log2(1 + count)
    recent = recency(last_used, reference)
    score = HOUR_WEIGHT * hour_share + WEEKDAY_TYPE_WEIGHT * weekday_share + frequency + recent
    factors = [(recent, REASON_RECENT)]
    if count >= FREQUENT_COUNT:
        factors.insert(0, (frequency, REASON_FREQUENT))
    if hour_share > 0:
        factors.insert(0, (HOUR_WEIGHT * hour_share, REASON_TIME))
    reason = max(factors, key=lambda factor: factor[0])[1]
    latest = max(used, key=lambda entry: entry.moment)
    amount = most_common(used, "amount")
    label = latest.label or ""
    return (-score, -last_used.timestamp(), entries[0].key), {
        "text": prefill_text(used, latest.kind, amount, label), "label": label, "amount": amount,
        "category_name": most_common(used, "category_name"), "account_name": most_common(used, "account_name"),
        "reason": reason,
    }


def capture_suggestions(user_id: str, device_time: datetime) -> list[dict]:
    """At most six suggestions, best first, from the 90 days before the device time."""
    since = device_time - timedelta(days=HISTORY_DAYS)
    with db_conn() as conn:
        with conn.cursor() as cur:
            entries = ledger_entries(cur, user_id, since) + undone_entries(cur, user_id, since)
    groups: dict[str, list[Entry]] = {}
    for entry in entries:
        if entry.moment > since:
            groups.setdefault(entry.key, []).append(entry)
    ranked = sorted((item for item in (suggestion(group, device_time) for group in groups.values()) if item),
                    key=lambda item: item[0])
    return [item for _, item in ranked[:SUGGESTION_LIMIT]]
