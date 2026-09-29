from __future__ import annotations

import hashlib
import re

from app.services.notification_evidence import EvidenceError, NotificationFacts
from app.services.notification_interpretation import Interpretation


NAME_TOKEN = re.compile(r"[A-Za-z*]+")
INSTITUTION_WORDS = frozenset({
    "bca", "jago", "gopay", "gojek", "shopeepay", "shopee", "stockbit", "bank", "tabungan", "by",
    "mybca", "rdn",
})
GENERIC_COUNTERPARTY_WORDS = frozenset({
    "payment", "top", "up", "debit", "qris", "merchant", "transfer", "masuk", "keluar",
    "pocket", "kantong", "utama", "main", "card",
})
EXTERNAL_SOURCE_HINTS = frozenset({"salary", "gaji", "interest", "bunga", "refund", "cashback", "bonus"})
INSTITUTION_LABELS = {"bca": "BCA", "jago": "Bank Jago", "gopay": "GoPay", "shopeepay": "ShopeePay", "stockbit": "Stockbit"}
MAPPABLE_ACCOUNT_TYPES = frozenset({"bank", "cash", "wallet", "ewallet"})
MAPPING_STOP_WORDS = frozenset({"pocket", "kantong", "my", "your", "bank", "by", "the", "dan", "and"})


class UnresolvedName(EvidenceError):
    """A bank-notification name that no learned alias or registered account name resolves."""

    def __init__(self, code: str, *, institution: str, name: str, name_normalized: str):
        super().__init__(code)
        self.institution = institution
        self.name = name
        self.name_normalized = name_normalized


def alias_key(institution: str, name_normalized: str) -> str:
    return f"{institution}|{name_normalized}"


def aliased_account(accounts: list[dict], institution: str, name_normalized: str) -> dict | None:
    key = alias_key(institution, name_normalized)
    matches = [account for account in accounts
               if any(learned.get("key") == key for learned in account.get("learned_notification_names") or ())]
    return matches[0] if len(matches) == 1 else None


def owner_aliases(profile: dict) -> list[str]:
    """Profile name plus configured aliases, whitespace-normalized and deduplicated."""
    aliases: list[str] = []
    seen: set[str] = set()
    for raw in [profile.get("name") or "", *(profile.get("name_aliases") or [])]:
        alias = " ".join(str(raw).split())
        if len(alias) >= 3 and alias.casefold() not in seen:
            seen.add(alias.casefold())
            aliases.append(alias)
    return aliases


def _word_matches(candidate: str, alias_word: str, *, allow_prefix: bool) -> bool:
    if len(candidate) > len(alias_word) or (not allow_prefix and len(candidate) != len(alias_word)):
        return False
    return all(c == "*" or c == a for c, a in zip(candidate.casefold(), alias_word.casefold()))


def owner_name_spans(text: str, aliases: list[str]) -> list[tuple[int, int]]:
    """Character spans naming the owner, literally, truncated, or bank-masked.

    Words align by position with alias words; an asterisk stands for one letter and only
    the final matched word may be truncated. A span needs two aligned words (one for a
    single-word alias) and at least two literal letters, so masked third parties such as
    "****LIN VALE**IA P" do not match "Alfonsus Enrico Soebijanto".
    """
    tokens = list(NAME_TOKEN.finditer(text or ""))
    spans: list[tuple[int, int]] = []
    for alias in aliases:
        alias_words = [word for word in NAME_TOKEN.findall(alias) if "*" not in word]
        if not alias_words:
            continue
        needed = min(2, len(alias_words))
        for start in range(len(tokens)):
            matched = literal = 0
            for offset, alias_word in enumerate(alias_words):
                if start + offset >= len(tokens):
                    break
                word = tokens[start + offset].group(0)
                if _word_matches(word, alias_word, allow_prefix=False):
                    matched += 1
                    literal += sum(ch != "*" for ch in word)
                    continue
                if len(word) >= 2 and _word_matches(word, alias_word, allow_prefix=True):
                    matched += 1
                    literal += sum(ch != "*" for ch in word)
                break
            if matched >= needed and literal >= 2:
                spans.append((tokens[start].start(), tokens[start + matched - 1].end()))
    spans.sort()
    merged: list[tuple[int, int]] = []
    for span in spans:
        if merged and span[0] <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], span[1]))
        else:
            merged.append(span)
    return merged


def names_owner(text: str | None, aliases: list[str]) -> bool:
    return bool(owner_name_spans(text or "", aliases))


def redact_owner_names(text: str, aliases: list[str]) -> str:
    for start, end in reversed(owner_name_spans(text, aliases)):
        text = text[:start] + "[SELF]" + text[end:]
    return text


def is_external_counterparty(counterparty: str | None, category_hint: str | None, aliases: list[str]) -> bool:
    """True when the notification names a third party (person, merchant, or employer)."""
    if category_hint and category_hint.strip().casefold() in EXTERNAL_SOURCE_HINTS:
        return True
    if not counterparty or names_owner(counterparty, aliases):
        return False
    remaining = [word for word in NAME_TOKEN.findall(counterparty)
                 if word.casefold() not in INSTITUTION_WORDS and word.casefold() not in GENERIC_COUNTERPARTY_WORDS]
    return bool(remaining)


def root_account_id(accounts: list[dict], account_id: str) -> str | None:
    account = next((row for row in accounts if str(row["id"]) == str(account_id)), None)
    if not account:
        return None
    return str(account.get("parent_id") or account["id"])


def normalize_name(value: str) -> str:
    value = re.sub(r"\b(?:pocket|kantong|your|my)\b", "", value.lower())
    value = re.sub(r"\s+", " ", value).strip()
    return {"main": "utama", "emergency fund": "dana darurat", "savings": "tabungan"}.get(value, value)


def institution_parent(accounts: list[dict], institution: str) -> dict:
    """The account an institution's notifications belong to.

    A top-level account named after the institution wins. When none exists, a uniquely
    named pocket is used instead: some balances shown in one app live in another bank,
    for example GoPay Tabungan, which is a Bank Jago pocket.
    """
    learned = aliased_account(accounts, institution, "")
    if learned:
        return learned
    aliases = {"shopeepay": ("shopeepay",), "stockbit": ("stockbit",)}.get(institution, (institution,))

    def matches(account: dict) -> bool:
        name = re.sub(r"[^a-z0-9]", "", account["name"].lower())
        if institution == "bca" and "rdn" in name:
            return False
        return any(alias in name for alias in aliases)

    candidates = [account for account in accounts if not account.get("parent_id") and matches(account)]
    if not candidates:
        candidates = [account for account in accounts if account.get("parent_id") and matches(account)]
    if len(candidates) != 1:
        raise UnresolvedName(
            "uncertain_institution_mapping", institution=institution,
            name=INSTITUTION_LABELS.get(institution, institution), name_normalized="",
        )
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


def pocket_endpoint(name: str | None, parent: dict, accounts: list[dict], *, institution: str) -> dict:
    if not name or normalize_name(name) == "utama":
        return main_endpoint(parent, accounts)
    target_name = normalize_name(name)
    learned = aliased_account(accounts, institution, target_name)
    if learned:
        return learned
    matches = [account for account in accounts
               if str(account.get("parent_id")) == str(parent["id"])
               and normalize_name(account["name"]) == target_name]
    if not matches:
        target_stem = target_name.rstrip("s")
        matches = [account for account in accounts
                   if str(account.get("parent_id")) == str(parent["id"])
                   and normalize_name(account["name"]).rstrip("s") == target_stem]
    if len(matches) != 1:
        raise UnresolvedName(
            "uncertain_pocket_mapping", institution=institution, name=name.strip(), name_normalized=target_name,
        )
    return matches[0]


def observed_endpoint(facts: NotificationFacts, accounts: list[dict], proposal: Interpretation | None = None) -> dict:
    parent = institution_parent(accounts, facts.institution)
    named = facts.parsed.source_pocket if facts.direction == "expense" else facts.parsed.target_pocket
    if named:
        return pocket_endpoint(named, parent, accounts, institution=facts.institution)
    if facts.institution == "jago":
        role_words = r"using|from|menggunakan|dari" if facts.direction == "expense" else r"in|into|ke|di"
        pocket = re.search(r"\b(?:" + role_words + r")(?:\s+your)?\s+(.+?)\s+(?:Pocket|Kantong)\b", facts.text, re.I)
        if pocket:
            return pocket_endpoint(pocket.group(1), parent, accounts, institution=facts.institution)
    return main_endpoint(parent, accounts)


def movement_categories(context: dict) -> tuple[str, str]:
    ids = []
    for kind in ("expense", "income"):
        rows = [row for row in context["categories"] if row["name"] == "Internal Movement" and row["kind"] == kind]
        if len(rows) != 1:
            raise EvidenceError("movement_category_missing")
        ids.append(str(rows[0]["id"]))
    return ids[0], ids[1]


def mapping_words(value: str) -> set[str]:
    return {word for word in re.findall(r"[a-z0-9]+", value.lower()) if len(word) >= 3 and word not in MAPPING_STOP_WORDS}


def eligible_account_ids(role: str, error: UnresolvedName, accounts: list[dict], excluded: set[str]) -> list[str]:
    """Accounts the model may propose for an unresolved name (design D2)."""
    liquid = [account for account in accounts
              if account.get("type") in MAPPABLE_ACCOUNT_TYPES and not account.get("instrument_type")
              and str(account["id"]) not in excluded]
    if role in {"source", "target"}:
        return [str(account["id"]) for account in liquid]
    root_id = None
    if error.name_normalized:
        try:
            root_id = str(institution_parent(accounts, error.institution)["id"])
        except EvidenceError:
            root_id = None
    words = mapping_words(error.name) | {error.institution}
    eligible = []
    for account in liquid:
        compact = re.sub(r"[^a-z0-9]", "", account["name"].lower())
        in_hierarchy = root_id is not None and root_id in {str(account["id"]), str(account.get("parent_id"))}
        if in_hierarchy or mapping_words(account["name"]) & words or error.institution in compact:
            eligible.append(str(account["id"]))
    return eligible


def unresolved_context(failures: list[tuple[str, EvidenceError]], accounts: list[dict], resolved: dict) -> dict:
    first = failures[0][1]
    if not all(isinstance(error, UnresolvedName) for _, error in failures):
        blocking = next(error for _, error in failures if not isinstance(error, UnresolvedName))
        return {"mapping_error": blocking.code}
    names = []
    for role, error in failures:
        eligible = eligible_account_ids(role, error, accounts, set(resolved.values()))
        if not eligible:
            return {"mapping_error": error.code}
        names.append({"role": role, "name": error.name, "institution": error.institution,
                      "name_normalized": error.name_normalized, "eligible_account_ids": eligible})
    return {**resolved, "mapping_error": first.code, "unresolved_names": names}


def endpoint_context(facts: NotificationFacts, accounts: list[dict]) -> dict:
    if facts.status != "candidate" or facts.institution == "stockbit":
        return {}
    if facts.direction == "internal_movement":
        if facts.institution != "jago":
            return {"mapping_error": "incomplete_movement"}
        try:
            parent = institution_parent(accounts, "jago")
        except EvidenceError as error:
            return {"mapping_error": error.code}
        resolved, failures = {}, []
        for role, name in (("source", facts.parsed.source_pocket), ("target", facts.parsed.target_pocket)):
            try:
                resolved[f"{role}_account_id"] = str(pocket_endpoint(name, parent, accounts, institution="jago")["id"])
            except EvidenceError as error:
                failures.append((role, error))
        if failures:
            return unresolved_context(failures, accounts, resolved)
        if resolved["source_account_id"] == resolved["target_account_id"]:
            return {"mapping_error": "same_movement_endpoint"}
        return resolved
    try:
        return {"effective_account_id": str(observed_endpoint(facts, accounts)["id"])}
    except EvidenceError as error:
        return unresolved_context([("observed", error)], accounts, {})


def transfer_signature(facts: NotificationFacts, context: dict, account_id: str) -> dict:
    text = context.get("notification", facts.text)
    transfer = bool(re.search(r"transfer|kir[i]?m|irim|has sent|you've sent|you have sent|sent to|menerima|top.?up|pengisian saldo|terdebit|pindah|moved", text, re.I))
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
        "root_account_id": root_account_id(context["accounts"], account_id),
        "external_counterparty": bool((context.get("facts") or {}).get("external_counterparty")),
    }


def signatures_compatible(left: dict, right: dict) -> bool:
    """Whether two recorded opposite legs are evidence of one owner transfer.

    Callers guarantee equal amount, the pairing time window, and ledger eligibility.
    Explicit transfer evidence (shared reference, or a named remote account plus owner
    identity) links any two accounts. Otherwise legs on different institutions link
    when neither names a third party, which covers wordings such as a card-style BCA
    debit paired with a ShopeePay top-up.
    """
    if left.get("account_id") == right.get("account_id"):
        return False
    if {left.get("direction"), right.get("direction")} != {"expense", "income"}:
        return False
    if left.get("currency") != right.get("currency"):
        return False
    if left.get("transfer") and right.get("transfer"):
        if left.get("reference_hash") and left["reference_hash"] == right.get("reference_hash"):
            return True
        endpoint_support = (
            left.get("remote_account_id") == right.get("account_id")
            or right.get("remote_account_id") == left.get("account_id")
        )
        if endpoint_support and (left.get("self_identity") or right.get("self_identity")):
            return True
    left_root, right_root = left.get("root_account_id"), right.get("root_account_id")
    return bool(
        left_root and right_root and left_root != right_root
        and not left.get("external_counterparty") and not right.get("external_counterparty")
    )
