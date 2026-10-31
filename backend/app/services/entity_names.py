"""Exact name lookup for API clients that address accounts and categories by name.

A name resolves only when it equals exactly one of the owner's active entities after case
and whitespace normalisation. Anything else is answered with candidates for the client to
choose from; nothing is guessed and nothing is created.
"""
from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any

from fastapi import HTTPException

CANDIDATE_LIMIT = 5
# Below this similarity a name is not offered as a candidate for an unknown name.
CANDIDATE_MIN_SIMILARITY = 0.6
EXCHANGE_SUFFIX = re.compile(r"\.(jk|idx)$")


def normalize_entity_name(value: str | None) -> str:
    return " ".join((value or "").split()).casefold()


def _symbol_keys(account: dict[str, Any]) -> set[str]:
    symbol = normalize_entity_name(account.get("instrument_symbol"))
    if not symbol:
        return set()
    return {symbol, EXCHANGE_SUFFIX.sub("", symbol)}


def _similarity(wanted: str, name: str) -> float:
    if wanted in name or name in wanted:
        return 1.0
    return SequenceMatcher(None, wanted, name).ratio()


def _account_candidate(account: dict[str, Any], accounts: list[dict[str, Any]]) -> dict[str, Any]:
    parent_id = account.get("parent_id")
    parent = next((row for row in accounts if parent_id and str(row["id"]) == str(parent_id)), None)
    return {"id": str(account["id"]), "name": account.get("name"), "parent_name": parent.get("name") if parent else None}


def _category_candidate(category: dict[str, Any]) -> dict[str, Any]:
    return {"id": str(category["id"]), "name": category.get("name"), "kind": category.get("kind")}


def _closest(wanted: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored = []
    for row in rows:
        name = normalize_entity_name(row.get("name"))
        score = _similarity(wanted, name) if wanted and name else 0.0
        if score >= CANDIDATE_MIN_SIMILARITY:
            scored.append((-score, name, row))
    scored.sort(key=lambda item: (item[0], item[1]))
    return [row for _, _, row in scored[:CANDIDATE_LIMIT]]


def _unresolved(code: str, field: str, reason: str, message: str, candidates: list[dict[str, Any]]) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={"code": code, "field": field, "reason": reason, "message": message, "candidates": candidates[:CANDIDATE_LIMIT]},
    )


def resolve_account_name(raw_name: str, accounts: list[dict[str, Any]], *, field: str) -> dict[str, Any]:
    """Return the one active account named exactly `raw_name`, or raise a 422 with candidates.

    `accounts` must already be limited to the owner's non-archived accounts. An exact
    instrument symbol (with or without its exchange suffix) identifies an investment
    position. When a parent and a pocket share the name, the single exact pocket wins.
    """
    wanted = normalize_entity_name(raw_name)
    matches = [
        account for account in accounts
        if wanted and (normalize_entity_name(account.get("name")) == wanted or wanted in _symbol_keys(account))
    ]
    if len(matches) == 1:
        return matches[0]
    pockets = [account for account in matches if account.get("parent_id")]
    if len(pockets) == 1:
        return pockets[0]
    if matches:
        ordered = pockets + [account for account in matches if not account.get("parent_id")]
        raise _unresolved(
            "account_name_unresolved", field, "ambiguous",
            "Nama rekening cocok dengan lebih dari satu rekening. Kirim ID rekening.",
            [_account_candidate(account, accounts) for account in ordered],
        )
    raise _unresolved(
        "account_name_unresolved", field, "not_found",
        "Rekening dengan nama ini tidak ditemukan. Gunakan nama persis atau ID rekening.",
        [_account_candidate(account, accounts) for account in _closest(wanted, accounts)],
    )


def resolve_category_name(
    raw_name: str,
    categories: list[dict[str, Any]],
    *,
    transaction_type: str,
    field: str = "category_name",
) -> dict[str, Any]:
    """Return the one active category named exactly `raw_name`, or raise a 422 with candidates.

    A name shared by an expense and an income category resolves to the one whose kind
    matches the transaction type.
    """
    wanted = normalize_entity_name(raw_name)
    matches = [category for category in categories if wanted and normalize_entity_name(category.get("name")) == wanted]
    if len(matches) > 1:
        same_kind = [category for category in matches if category.get("kind") == transaction_type]
        if same_kind:
            matches = same_kind
    if len(matches) == 1:
        return matches[0]
    if matches:
        raise _unresolved(
            "category_name_unresolved", field, "ambiguous",
            "Nama kategori cocok dengan lebih dari satu kategori. Kirim ID kategori.",
            [_category_candidate(category) for category in matches],
        )
    raise _unresolved(
        "category_name_unresolved", field, "not_found",
        "Kategori dengan nama ini tidak ditemukan. Gunakan nama persis atau ID kategori.",
        [_category_candidate(category) for category in _closest(wanted, categories)],
    )
