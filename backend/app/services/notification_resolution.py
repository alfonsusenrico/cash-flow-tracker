from __future__ import annotations

import hashlib
import re

from app.services.notification_evidence import EvidenceError, NotificationFacts
from app.services.notification_interpretation import Interpretation


def normalize_name(value: str) -> str:
    value = re.sub(r"\b(?:pocket|kantong|your|my)\b", "", value.lower())
    value = re.sub(r"\s+", " ", value).strip()
    return {"main": "utama", "emergency fund": "dana darurat", "savings": "tabungan"}.get(value, value)


def institution_parent(accounts: list[dict], institution: str) -> dict:
    aliases = {"shopeepay": ("shopeepay",), "stockbit": ("stockbit",)}.get(institution, (institution,))
    candidates = []
    for account in accounts:
        name = re.sub(r"[^a-z0-9]", "", account["name"].lower())
        if account.get("parent_id") or (institution == "bca" and "rdn" in name):
            continue
        if any(alias in name for alias in aliases):
            candidates.append(account)
    if len(candidates) != 1:
        raise EvidenceError("uncertain_institution_mapping")
    return candidates[0]


def main_endpoint(parent: dict, accounts: list[dict]) -> dict:
    children = [account for account in accounts if str(account.get("parent_id")) == str(parent["id"])]
    if parent.get("default_pocket_id"):
        matches = [account for account in children if str(account["id"]) == str(parent["default_pocket_id"])]
        if len(matches) != 1:
            raise EvidenceError("invalid_default_pocket")
        return matches[0]
    matches = [account for account in children if normalize_name(account["name"]) == "utama"]
    if len(matches) > 1:
        raise EvidenceError("ambiguous_main_pocket")
    if children and not matches:
        raise EvidenceError("main_pocket_unconfigured")
    return matches[0] if matches else parent


def pocket_endpoint(name: str | None, parent: dict, accounts: list[dict]) -> dict:
    if not name or normalize_name(name) == "utama":
        return main_endpoint(parent, accounts)
    matches = [account for account in accounts
               if str(account.get("parent_id")) == str(parent["id"])
               and normalize_name(account["name"]) == normalize_name(name)]
    if len(matches) != 1:
        raise EvidenceError("uncertain_pocket_mapping")
    return matches[0]


def observed_endpoint(facts: NotificationFacts, accounts: list[dict], proposal: Interpretation | None = None) -> dict:
    parent = institution_parent(accounts, facts.institution)
    named = facts.parsed.source_pocket if facts.direction == "expense" else facts.parsed.target_pocket
    if named:
        return pocket_endpoint(named, parent, accounts)
    if facts.institution == "jago":
        role_words = r"using|from|menggunakan|dari" if facts.direction == "expense" else r"in|into|ke|di"
        pocket = re.search(r"\b(?:" + role_words + r")(?:\s+your)?\s+(.+?)\s+(?:Pocket|Kantong)\b", facts.text, re.I)
        if pocket:
            return pocket_endpoint(pocket.group(1), parent, accounts)
    return main_endpoint(parent, accounts)


def movement_categories(context: dict) -> tuple[str, str]:
    ids = []
    for kind in ("expense", "income"):
        rows = [row for row in context["categories"] if row["name"] == "Internal Movement" and row["kind"] == kind]
        if len(rows) != 1:
            raise EvidenceError("movement_category_missing")
        ids.append(str(rows[0]["id"]))
    return ids[0], ids[1]


def endpoint_context(facts: NotificationFacts, accounts: list[dict]) -> dict:
    if facts.status != "candidate" or facts.institution == "stockbit":
        return {}
    try:
        if facts.direction == "internal_movement":
            if facts.institution != "jago":
                raise EvidenceError("incomplete_movement")
            parent = institution_parent(accounts, "jago")
            source = pocket_endpoint(facts.parsed.source_pocket, parent, accounts)
            target = pocket_endpoint(facts.parsed.target_pocket, parent, accounts)
            if str(source["id"]) == str(target["id"]):
                raise EvidenceError("same_movement_endpoint")
            return {"source_account_id": str(source["id"]), "target_account_id": str(target["id"])}
        return {"effective_account_id": str(observed_endpoint(facts, accounts)["id"])}
    except EvidenceError as error:
        return {"mapping_error": error.code}


def transfer_signature(facts: NotificationFacts, context: dict, account_id: str) -> dict:
    text = context.get("notification", facts.text)
    transfer = bool(re.search(r"transfer|kir[i]?m|has sent|menerima|top.?up|terdebit|pindah|moved", text, re.I))
    self_identity = bool(context.get("self_identity_detected"))
    reference = re.search(r"(?:referensi|reference|ref)\s*[:#]\s*([a-z0-9-]{6,40})", facts.text, re.I)
    remote_id = None
    for institution in ("bca", "jago", "gopay", "shopeepay"):
        if institution == facts.institution or not re.search(r"\b" + institution + r"\b", text, re.I):
            continue
        try:
            parent = institution_parent(context["accounts"], institution)
            endpoint = main_endpoint(parent, context["accounts"])
        except EvidenceError:
            continue
        if remote_id is not None:
            remote_id = None
            break
        remote_id = str(endpoint["id"])
    if facts.institution == "jago" and transfer:
        parent = institution_parent(context["accounts"], "jago")
        normalized_text = normalize_name(text)
        pockets = [row for row in context["accounts"]
                   if str(row.get("parent_id")) == str(parent["id"]) and str(row["id"]) != account_id
                   and re.search(r"(?<!\w)" + re.escape(normalize_name(row["name"])) + r"(?!\w)", normalized_text)]
        if len(pockets) == 1:
            remote_id = str(pockets[0]["id"])
            self_identity = True
    return {
        "account_id": account_id, "direction": facts.direction, "currency": facts.currency,
        "transfer": transfer, "self_identity": self_identity, "remote_account_id": remote_id,
        "reference_hash": hashlib.sha256(reference.group(1).lower().encode()).hexdigest() if reference else None,
    }


def signatures_compatible(left: dict, right: dict) -> bool:
    if not left.get("transfer") or not right.get("transfer"):
        return False
    if left.get("account_id") == right.get("account_id") or left.get("direction") == right.get("direction"):
        return False
    if left.get("currency") != right.get("currency"):
        return False
    if left.get("reference_hash") and left["reference_hash"] == right.get("reference_hash"):
        return True
    endpoint_support = (
        left.get("remote_account_id") == right.get("account_id")
        or right.get("remote_account_id") == left.get("account_id")
    )
    return endpoint_support and (left.get("self_identity") or right.get("self_identity"))
