"""Deterministic facts of one typed capture: amount, kind, time and account mentions (design D1).

The model never sets an amount; everything here is plain parsing of the owner's text. The time is read by
`capture_time`, and only a phrase it cannot place is left to the validated model fallback.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation

from app.services.capture_time import TimeReading, parse_time


AMOUNT_MAX = 1_000_000_000_000
BARE_AMOUNT_MIN_DIGITS = 3
UNIT_MULTIPLIERS = {"rb": 1000, "ribu": 1000, "k": 1000, "jt": 1_000_000, "juta": 1_000_000}

MONEY_PATTERN = re.compile(
    r"(?<![\w.,/])(?P<rp>rp\.?\s*)?(?P<number>\d[\d.,]*\d|\d)\s*(?P<unit>ribu|rb|juta|jt|k)?(?![\w/])",
    re.IGNORECASE,
)
WORD_PATTERN = re.compile(r"[^\W_]+(?:['&][^\W_]+)*")

# Unit-less forms: "10.000", "10.000,00" and plain digits with at least three digits.
BARE_GROUPED = re.compile(r"\d{1,3}(?:\.\d{3})+(?:,0{1,2})?")
BARE_PLAIN = re.compile(r"\d+")
# Forms before a unit: "10", "1,5", "1.5", "2,25" and "1.500".
UNIT_DECIMAL = re.compile(r"\d+(?:[.,]\d{1,2})?")
UNIT_GROUPED = re.compile(r"\d{1,3}(?:\.\d{3})+")

INCOME_PATTERNS = (
    re.compile(r"\btransfer masuk\b"), re.compile(r"\bterima\b(?! kasih)"), re.compile(r"\bdapat\b"),
    re.compile(r"\bdibayar\b"), re.compile(r"\bgaji\b"), re.compile(r"\brefund\b"),
    re.compile(r"\bdikembalikan\b"),
)
EXPENSE_PATTERNS = (re.compile(r"\bbayar\b"), re.compile(r"\bbyr\b"), re.compile(r"\bbeli\b"))
MOVEMENT_FORMS = (
    ("withdraw", re.compile(r"\btarik tunai\b")),
    ("deposit", re.compile(r"\bsetor tunai\b")),
    ("top_up", re.compile(r"\bisi saldo\b")),
    ("top_up", re.compile(r"\btop ?up\b(?=.*\bke\b)")),
    ("transfer", re.compile(r"\bpindah(?:kan)?\b(?=.*\bke\b)")),
)
# Words of a movement form, removed from the merchant phrase.
MOVEMENT_WORDS = frozenset({"tarik", "setor", "isi", "saldo", "top", "up", "topup", "pindah", "pindahkan"})
CASH_WORDS = frozenset({"tunai", "cash", "dompet"})
INSTITUTION_WORDS = {"bca": "bca", "jago": "jago", "gopay": "gopay", "shopeepay": "shopeepay"}
ROLE_MARKERS = {"dari": "source", "ke": "target"}
# Connectors that only say how or where money went; never part of a description or a key.
CONNECTORS = frozenset({"pakai", "pake", "via", "dari", "ke", "dengan", "dgn", "rp"})
# Verbs and fillers that do not identify what was bought, so "bayar parkir ptc" and "parkir ptc" share a key.
KEY_FILLERS = frozenset({
    "bayar", "byr", "beli", "buat", "untuk", "utk", "di", "terima", "dapat", "dibayar", "tadi", "sih", "aja",
})
CASH_ACCOUNT_TYPE = "cash"


@dataclass(frozen=True)
class AccountMention:
    role: str | None
    candidates: tuple[str, ...]
    word: str


@dataclass(frozen=True)
class CaptureFacts:
    amount: int | None
    amount_issue: str | None
    kind: str | None
    kind_guess: str
    movement_form: str | None
    date: datetime
    phrase: str
    description: str
    mentions: tuple[AccountMention, ...] = field(default_factory=tuple)
    time: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "amount": self.amount, "amount_issue": self.amount_issue, "kind": self.kind,
            "kind_guess": self.kind_guess, "movement_form": self.movement_form, "date": self.date.isoformat(),
            "phrase": self.phrase, "description": self.description,
            "mentions": [{"role": item.role, "candidates": list(item.candidates), "word": item.word}
                         for item in self.mentions],
            "time": self.time,
        }


def normalize_text(value: str) -> str:
    return unicodedata.normalize("NFKC", value)


def fold(value: str) -> str:
    return " ".join(normalize_text(value).casefold().split())


def money_value(match: re.Match) -> int | None:
    """The IDR value of one money expression, or None when its form is ambiguous or out of range."""
    number, unit = match.group("number"), (match.group("unit") or "").lower()
    if unit:
        if UNIT_GROUPED.fullmatch(number):
            value = Decimal(number.replace(".", ""))
        elif UNIT_DECIMAL.fullmatch(number):
            value = Decimal(number.replace(",", "."))
        else:
            return None
        value *= UNIT_MULTIPLIERS[unit]
    elif BARE_GROUPED.fullmatch(number):
        value = Decimal(number.split(",")[0].replace(".", ""))
    elif BARE_PLAIN.fullmatch(number):
        value = Decimal(number)
    else:
        return None
    try:
        if value != value.to_integral_value() or not 0 < value <= AMOUNT_MAX:
            return None
    except InvalidOperation:
        return None
    return int(value)


def is_money_expression(match: re.Match) -> bool:
    """Bare numbers below 100 and bare decimals ("1.5") are quantities or noise, not amounts."""
    if match.group("rp") or match.group("unit"):
        return True
    number = match.group("number")
    if BARE_GROUPED.fullmatch(number):
        return True
    if BARE_PLAIN.fullmatch(number):
        return len(number) >= BARE_AMOUNT_MIN_DIGITS
    return "," in number and not re.fullmatch(r"\d+,\d{1,2}", number)


def parse_amount(text: str, ignored: list[tuple[int, int]] | None = None) -> tuple[int | None, str | None, list]:
    """(amount, issue, spans): exactly one money expression gives an amount (D1)."""
    ignored = ignored or []
    expressions = []
    for match in MONEY_PATTERN.finditer(text):
        if any(start <= match.start() < end for start, end in ignored) or not is_money_expression(match):
            continue
        expressions.append(match)
    spans = [(match.start(), match.end()) for match in expressions]
    if not expressions:
        return None, "missing", spans
    if len(expressions) > 1:
        return None, "several", spans
    value = money_value(expressions[0])
    return value, None if value else "invalid", spans


def span_text(source: str, spans) -> str | None:
    """The owner's own words of the spans, in text order, joined by single spaces."""
    words = [" ".join(source[start:end].split()) for start, end in spans]
    return " ".join(word for word in words if word) or None


def time_facts(reading: TimeReading, source: str) -> dict:
    """The stored form of a time reading: status, basis, evidence and what an open time question needs."""
    basis = {"none": "device_time", "resolved": "parsed"}.get(reading.status)
    return {
        "status": reading.status, "basis": basis,
        "evidence": span_text(source, reading.spans) if reading.status != "none" else None,
        "reason": reading.reason, "options": [moment.isoformat() for moment in reading.options],
        "hints": [span_text(source, [span]) for span in reading.hints],
        "day": reading.day.isoformat() if reading.day else None,
        "explicit_date": reading.explicit_date.isoformat() if reading.explicit_date else None,
        "day_question": reading.day_question,
    }


def parse_kind(folded: str) -> tuple[str | None, str, str | None]:
    """(kind or None when the words disagree, best guess, movement form)."""
    movement_form = next((form for form, pattern in MOVEMENT_FORMS if pattern.search(folded)), None)
    income = any(pattern.search(folded) for pattern in INCOME_PATTERNS)
    expense = any(pattern.search(folded) for pattern in EXPENSE_PATTERNS)
    if movement_form:
        return (None if income or expense else "movement"), "movement", movement_form
    if income:
        return (None if expense else "income"), "income", None
    return "expense", "expense", None


def leaf_candidates(account: dict, accounts: dict[str, dict]) -> tuple[str, ...]:
    """Accounts that can hold a row: a parent with pockets resolves to its default pocket."""
    children = [row for row in accounts.values() if str(row.get("parent_id") or "") == str(account["id"])]
    if not children:
        return (str(account["id"]),)
    default = str(account.get("default_pocket_id") or "")
    if default and any(str(row["id"]) == default for row in children):
        return (default,)
    return tuple(sorted(str(row["id"]) for row in children))


def cash_accounts(accounts: dict[str, dict]) -> tuple[str, ...]:
    """Cash accounts that can hold a row ("tunai", "cash", "dompet")."""
    parents = {str(row.get("parent_id")) for row in accounts.values() if row.get("parent_id")}
    return tuple(sorted(str(row["id"]) for row in accounts.values()
                        if row.get("type") == CASH_ACCOUNT_TYPE and str(row["id"]) not in parents))


def account_phrases(accounts: dict[str, dict], aliases: list[dict]) -> list[tuple[tuple[str, ...], str]]:
    """(folded words, account id) for every account name, pocket label and learned alias; longest first."""
    phrases = []
    for account in accounts.values():
        names = [account["name"]]
        parent = accounts.get(str(account.get("parent_id") or ""))
        if parent:
            names.append(f"{parent['name']} {account['name']}")
        for name in names:
            words = tuple(word.casefold() for word in WORD_PATTERN.findall(normalize_text(name)))
            if words:
                phrases.append((words, str(account["id"])))
    for alias in aliases:
        words = tuple(word.casefold() for word in WORD_PATTERN.findall(normalize_text(alias["name"])))
        if words and str(alias["account_id"]) in accounts:
            phrases.append((words, str(alias["account_id"])))
    return sorted(phrases, key=lambda item: -len(item[0]))


def find_mentions(
    words: list[tuple[str, int, int]], used: set[int], accounts: dict[str, dict], aliases: list[dict],
) -> list[tuple[int, int, tuple[str, ...], str]]:
    """(first word, last word, candidate accounts, kind) of each account mention, in text order."""
    folded = [word.casefold() for word, _, _ in words]
    used = set(used)
    mentions = []
    for phrase, account_id in account_phrases(accounts, aliases):
        size = len(phrase)
        for index in range(len(folded) - size + 1):
            span = set(range(index, index + size))
            if tuple(folded[index:index + size]) == phrase and not span & used:
                used |= span
                mentions.append((index, index + size - 1, leaf_candidates(accounts[account_id], accounts), "name"))
    cash = cash_accounts(accounts)
    for index, word in enumerate(folded):
        if index in used:
            continue
        if word in CASH_WORDS:
            used.add(index)
            mentions.append((index, index, cash, "cash"))
        elif word in INSTITUTION_WORDS:
            matched = [row for row in accounts.values() if not row.get("parent_id") and word in {
                item.casefold() for item in WORD_PATTERN.findall(normalize_text(row["name"]))}]
            candidates = tuple(sorted({leaf for row in matched for leaf in leaf_candidates(row, accounts)}))
            used.add(index)
            mentions.append((index, index, candidates, "institution"))
    return sorted(mentions)


def assign_roles(
    mentions: list[tuple[int, int, tuple[str, ...], str]], folded: list[str], kind: str | None, form: str | None,
    accounts: dict[str, dict],
) -> tuple[AccountMention, ...]:
    """Roles of the mentions: "dari"/"ke" mark source and target; a movement form fills the rest."""
    marked = []
    for first, _, candidates, word in mentions:
        marker = folded[first - 1] if first > 0 else None
        marked.append([ROLE_MARKERS.get(marker), candidates, word])
    if kind != "movement":
        return tuple(AccountMention(role or None, candidates, word) for role, candidates, word in marked)
    cash = cash_accounts(accounts)
    fixed = {"withdraw": ("target", cash), "deposit": ("source", cash)}.get(form)
    result = []
    if fixed:
        result.append(AccountMention(fixed[0], fixed[1], "cash_form"))
    taken = {role for role, _, _ in marked if role} | ({fixed[0]} if fixed else set())
    open_roles = [role for role in (("target", "source") if form == "top_up" else ("source", "target"))
                  if role not in taken]
    for role, candidates, word in marked:
        if role is None and open_roles:
            role = open_roles.pop(0)
        if role is not None and role not in {item.role for item in result}:
            result.append(AccountMention(role, candidates, word))
    return tuple(result)


def parse_capture(
    text: str, device_time: datetime, *, accounts: list[dict], aliases: list[dict], tz: str,
) -> CaptureFacts:
    """All deterministic facts of a capture; raises CaptureDateError for an unusable explicit date."""
    source = normalize_text(text)
    folded_text = source.casefold()
    if len(folded_text) != len(source):
        # Positions must line up between both forms; the rare characters that fold longer lose their case.
        source = folded_text
    reading = parse_time(folded_text, device_time, tz)
    date_spans = list(reading.spans)
    amount, amount_issue, money_spans = parse_amount(folded_text, date_spans)
    kind, kind_guess, form = parse_kind(" ".join(folded_text.split()))
    words = [(match.group(0), match.start(), match.end()) for match in WORD_PATTERN.finditer(source)]
    # Time words, read or only flagged, say when it happened; they never describe it or key memory.
    covered = date_spans + list(reading.hints) + money_spans
    used = {index for index, (_, start, end) in enumerate(words)
            if any(start < span_end and span_start < end for span_start, span_end in covered)}
    folded = [word.casefold() for word, _, _ in words]
    if form in {"withdraw", "deposit"}:
        for index in range(len(folded) - 1):
            if folded[index] in {"tarik", "setor"} and folded[index + 1] == "tunai":
                used |= {index, index + 1}
    by_id = {str(row["id"]): row for row in accounts}
    mentions = find_mentions(words, used, by_id, aliases)
    mention_words = {index for first, last, _, _ in mentions for index in range(first, last + 1)}
    roles = assign_roles(mentions, folded, kind or kind_guess, form, by_id)
    kept = [index for index in range(len(words)) if index not in used and index not in mention_words
            and folded[index] not in CONNECTORS]
    description = " ".join(words[index][0] for index in kept if not (form and folded[index] in MOVEMENT_WORDS))
    phrase = " ".join(folded[index] for index in kept
                      if folded[index] not in KEY_FILLERS and folded[index] not in MOVEMENT_WORDS)
    return CaptureFacts(
        amount=amount, amount_issue=amount_issue, kind=kind, kind_guess=kind_guess, movement_form=form,
        date=reading.moment, phrase=phrase[:200], description=(description[:1].upper() + description[1:])[:160],
        mentions=roles, time=time_facts(reading, source),
    )


def parse_amount_answer(text: str) -> int | None:
    """The amount in an answer to the amount question: exactly one valid money expression."""
    amount, issue, _ = parse_amount(normalize_text(text).casefold())
    return amount if issue is None else None
