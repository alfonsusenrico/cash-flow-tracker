"""The owner's saved order of sections and cards on Rekening & Kantong (organize-accounts-by-use D5).

One layout per grouping. Under "use" the sections are the five roles; under "rekening" they are the
top-level accounts, a standalone top-level account being its own rekening. Cards are the accounts shown
as cards: every non-archived account without children, plus a parent whose own balance is not zero. A
card belongs to exactly one section: its role under "use", its top-level account under "rekening".

Every read and write is normalized against the owner's current accounts, so a layout never holds another
owner's ids, an archived account, an unknown section or a card in the wrong section, and new accounts
appear at the end of their section in the default order.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field
from psycopg.types.json import Jsonb

from app.services.account_roles import ROLES

GROUPING_PATTERN = "^(use|rekening)$"
MAX_LAYOUT_IDS = 500
MAX_KEY_LENGTH = 64


class AccountLayoutBody(BaseModel):
    grouping: str | None = Field(default=None, pattern=GROUPING_PATTERN)
    sections: list[str] = Field(max_length=MAX_LAYOUT_IDS)
    cards: dict[str, list[str]] = Field(max_length=MAX_LAYOUT_IDS)


def _tree_order(accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Top-level accounts in display order, each followed by its pockets in display order.

    `display_order` orders top-level accounts among themselves and pockets within their parent, so the
    tree order is the default that respects both.
    """
    def key(account: dict[str, Any]) -> tuple:
        return (int(account.get("display_order") or 0), account.get("created_at") or "", account.get("name") or "")

    by_parent: dict[str | None, list[dict[str, Any]]] = {}
    ids = {account["id"] for account in accounts}
    for account in accounts:
        parent = account.get("parent_id") if account.get("parent_id") in ids else None
        by_parent.setdefault(parent, []).append(account)
    ordered: list[dict[str, Any]] = []
    for top in sorted(by_parent.get(None, []), key=key):
        ordered.append(top)
        ordered.extend(sorted(by_parent.get(top["id"], []), key=key))
    return ordered


def layout_cards(accounts: list[dict[str, Any]], grouping: str) -> tuple[list[str], dict[str, str]]:
    """The default section order and each card's section, from non-archived accounts with balances."""
    active = [account for account in accounts if not account.get("is_archived")]
    ordered = _tree_order(active)
    ids = {account["id"] for account in ordered}
    has_children = {account["parent_id"] for account in ordered if account.get("parent_id") in ids}
    if grouping == "use":
        sections = list(ROLES)
    else:
        sections = [account["id"] for account in ordered if account.get("parent_id") not in ids]
    card_section: dict[str, str] = {}
    for account in ordered:
        is_parent = account["id"] in has_children
        if is_parent and int(account.get("own_balance", 0)) == 0:
            continue
        if grouping == "use":
            card_section[account["id"]] = account.get("role")
        else:
            parent = account.get("parent_id")
            card_section[account["id"]] = parent if parent in ids else account["id"]
    return sections, card_section


def normalize_layout(
    accounts: list[dict[str, Any]], grouping: str,
    sections: list[str] | None = None, cards: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Keeps the known part of a requested or stored order and appends what is missing in default order."""
    default_sections, card_section = layout_cards(accounts, grouping)
    valid_sections = set(default_sections)
    ordered_sections: list[str] = []
    for section in sections or []:
        if section in valid_sections and section not in ordered_sections:
            ordered_sections.append(section)
    ordered_sections += [section for section in default_sections if section not in ordered_sections]

    requested = cards or {}
    result: dict[str, list[str]] = {}
    for section in ordered_sections:
        placed: list[str] = []
        for account_id in requested.get(section, []):
            if card_section.get(account_id) == section and account_id not in placed:
                placed.append(account_id)
        placed += [
            account_id for account_id, home in card_section.items()
            if home == section and account_id not in placed
        ]
        result[section] = placed
    return {"grouping": grouping, "sections": ordered_sections, "cards": result}


def load_layout(cur, user_id: str, grouping: str) -> tuple[list[str] | None, dict[str, list[str]] | None]:
    cur.execute(
        "SELECT sections, cards FROM account_layouts WHERE user_id = %s AND grouping = %s",
        (user_id, grouping),
    )
    row = cur.fetchone()
    if not row:
        return None, None
    return row["sections"], row["cards"]


def save_layout(cur, user_id: str, layout: dict[str, Any]) -> None:
    cur.execute(
        """INSERT INTO account_layouts (user_id, grouping, sections, cards, updated_at)
           VALUES (%s, %s, %s, %s, NOW())
           ON CONFLICT (user_id, grouping) DO UPDATE
           SET sections = EXCLUDED.sections, cards = EXCLUDED.cards, updated_at = NOW()""",
        (user_id, layout["grouping"], Jsonb(layout["sections"]), Jsonb(layout["cards"])),
    )


def validate_layout_body(body: AccountLayoutBody) -> str | None:
    """The refusal code for a body that is too large or has oversized keys, else None."""
    if sum(len(ids) for ids in body.cards.values()) > MAX_LAYOUT_IDS:
        return "layout_too_large"
    keys = [*body.sections, *body.cards.keys(), *(item for ids in body.cards.values() for item in ids)]
    if any(len(key) > MAX_KEY_LENGTH for key in keys):
        return "layout_key_too_long"
    return None
