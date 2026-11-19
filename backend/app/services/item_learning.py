"""Which account pays for an item, learned across the owner's capture phrases (learn-accounts-by-item D1, D2).

Every capture phrase key ("merchant:parkir ptc") belongs to an item group ("parkir"), so "parkir ptc", "parkir k24"
and "parkiran mall" teach one habit. A key's group is decided once and stored when a capture with that key is first
recorded; captures recorded before groups were stored fall back to their first word. The paying account is then
learned from the latest captures of the group with the rule of succession of `pocket_learning`; the model only ever
names a group, never the account.

Evidence is read from what the ledger already holds:
- the final account of each recorded expense or income capture of the group (weight 1);
- a capture whose row the owner later moved to another account counts for the new account twice. The capture's
  `recorded_rows` keep the account it was recorded on, so a different current account is the owner's correction.
"""

from __future__ import annotations

import re
from collections import Counter

from app.services.capture_parsing import KEY_FILLERS
from app.services.pocket_learning import CORRECTION_WEIGHT, EVIDENCE_WINDOW, PocketDecision, decide

# The bound of `capture_item_groups.item_group`.
ITEM_GROUP_MAX_LENGTH = 60
ITEM_GROUP_MAX_WORDS = 3
# Most used first, so the cap keeps the groups the owner is likely to mean.
CONTEXT_GROUP_LIMIT = 100
RECORDED_WEIGHT = 1
# Numbers and one-letter words never name an item: "20 45 parkir ptc" is "parkir", "07 55 e money" is "money".
ITEM_WORD = re.compile(r"[^\W\d_]{2,}")
NO_DECISION = PocketDecision(None, False, 0, 0)


def first_item_word(phrase: str | None) -> str | None:
    """The first run of at least two letters outside the key fillers, lower-cased."""
    for word in (phrase or "").casefold().split():
        if word in KEY_FILLERS:
            continue
        match = ITEM_WORD.search(word)
        if match:
            return match.group(0)[:ITEM_GROUP_MAX_LENGTH]
    return None


def key_phrase(match_key: str) -> str:
    return match_key.removeprefix("merchant:")


def recorded_key_groups(cur, user_id: str) -> dict[str, tuple[str | None, int]]:
    """(group, recorded captures) per phrase key of the owner's recorded expense and income captures."""
    cur.execute(
        """SELECT c.match_key, g.item_group, COUNT(*) AS captures
           FROM text_captures c
           LEFT JOIN capture_item_groups g ON g.user_id = c.user_id AND g.match_key = c.match_key
           WHERE c.user_id = %s AND c.status = 'recorded' AND c.match_key IS NOT NULL AND c.movement_id IS NULL
           GROUP BY c.match_key, g.item_group""",
        (user_id,),
    )
    return {row["match_key"]: (row["item_group"] or first_item_word(key_phrase(row["match_key"])), row["captures"])
            for row in cur.fetchall()}


def group_counts(cur, user_id: str) -> Counter:
    counts: Counter = Counter()
    for group, captures in recorded_key_groups(cur, user_id).values():
        if group:
            counts[group] += captures
    return counts


def stored_item_group(cur, user_id: str, match_key: str) -> str | None:
    cur.execute("SELECT item_group FROM capture_item_groups WHERE user_id = %s AND match_key = %s",
                (user_id, match_key))
    row = cur.fetchone()
    return row["item_group"] if row else None


def known_item_group(cur, user_id: str, match_key: str | None) -> dict | None:
    """The key's group without the model (D1 steps 1–2): its stored group, else its first word when the owner has
    recorded captures in that group. `source` is `stored` or `word`."""
    if not match_key:
        return None
    stored = stored_item_group(cur, user_id, match_key)
    if stored:
        return {"group": stored, "source": "stored"}
    word = first_item_word(key_phrase(match_key))
    if word and group_counts(cur, user_id)[word]:
        return {"group": word, "source": "word"}
    return None


def decide_item_group(known: dict | None, match_key: str | None, model_group: str | None) -> dict | None:
    """The capture's group (D1): known without the model, else the model's checked group, else the first word."""
    if known:
        return known
    if model_group:
        return {"group": model_group, "source": "ai"}
    word = first_item_word(key_phrase(match_key)) if match_key else None
    return {"group": word, "source": "word"} if word else None


def owner_item_groups(cur, user_id: str, identity: list[str]) -> list[dict]:
    """The owner's groups with their recorded captures, most used first, for the model to choose from."""
    from app.services.notification_context import sanitize_text

    ranked = sorted(group_counts(cur, user_id).items(), key=lambda item: (-item[1], item[0]))
    # A group that names the owner is left out rather than sent masked: a masked name is not a group to reuse.
    return [{"group": group, "count": count} for group, count in ranked
            if sanitize_text(group, identity) == group][:CONTEXT_GROUP_LIMIT]


def item_account(cur, user_id: str, group: str, kind: str, eligible: set[str]) -> PocketDecision:
    """The learned account of an item group among the eligible accounts (D2)."""
    keys = [key for key, (key_group, _) in recorded_key_groups(cur, user_id).items() if key_group == group]
    if not keys:
        return NO_DECISION
    # A corrected capture is as recent as its correction, so a changed habit wins the tie (pocket learning D5).
    cur.execute(
        """SELECT t.account_id, c.recorded_rows -> 0 ->> 'account_id' AS recorded_account_id
           FROM text_captures c
           JOIN transactions t ON t.user_id = c.user_id AND t.id = c.transaction_id
           WHERE c.user_id = %s AND c.status = 'recorded' AND c.match_key = ANY(%s) AND c.movement_id IS NULL
             AND t.type = %s AND t.movement_id IS NULL
           ORDER BY GREATEST(c.recorded_at, CASE WHEN t.account_id::text IS DISTINCT FROM
                                                      c.recorded_rows -> 0 ->> 'account_id'
                                                 THEN t.updated_at END) DESC, c.id
           LIMIT %s""",
        (user_id, keys, kind, EVIDENCE_WINDOW),
    )
    choices = []
    for row in cur.fetchall():
        current = str(row["account_id"])
        if current not in eligible:
            continue
        corrected = row["recorded_account_id"] is not None and row["recorded_account_id"] != current
        choices.append((current, CORRECTION_WEIGHT if corrected else RECORDED_WEIGHT))
    return decide(choices, auto_enabled=True)


def remember_item_group(cur, user_id: str, match_key: str | None, item: dict | None) -> bool:
    """Store a recorded capture's group once per phrase key; True when this capture stored it."""
    if not match_key or not item or item["source"] == "stored":
        return False
    cur.execute(
        """INSERT INTO capture_item_groups (user_id, match_key, item_group, source) VALUES (%s, %s, %s, %s)
           ON CONFLICT (user_id, match_key) DO NOTHING RETURNING match_key""",
        (user_id, match_key, item["group"], item["source"]),
    )
    return cur.fetchone() is not None


def forget_item_group(cur, user_id: str, match_key: str, capture_id: str) -> None:
    """Undo of the capture that stored a group: the group goes unless another recorded capture has the key."""
    cur.execute(
        """DELETE FROM capture_item_groups g WHERE g.user_id = %s AND g.match_key = %s
           AND NOT EXISTS (SELECT 1 FROM text_captures c WHERE c.user_id = g.user_id AND c.match_key = g.match_key
                           AND c.status = 'recorded' AND c.id <> %s)""",
        (user_id, match_key, capture_id),
    )
