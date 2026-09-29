from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from app.services.notification_parser import ParsedNotification, parse_notification


SUPPORTED_PACKAGES = {
    "com.bca.mybca.omni.android": "bca",
    "id.co.bca.mybca.omni.android": "bca",
    "com.bca": "bca",
    "com.jago.digitalbanking": "jago",
    "com.gojek.gopay": "gopay",
    "com.gojek.app": "gopay",
    "com.gopay.wallet": "gopay",
    "com.shopeepay.id": "shopeepay",
    "com.shopee.id": "shopeepay",
    "com.stockbit.android": "stockbit",
}
TEXT_FIELDS = ("title", "body_text", "big_text", "sub_text", "summary_text")
MONEY_PATTERN = re.compile(r"\b(?:Rp\.?|IDR)\s*(-?\d[\d.,]*\d|-?\d)\b", re.I)
BALANCE_PATTERN = re.compile(
    r"\b(?:saldo saat ini(?: sebesar)?|current balance(?: is)?|sisa saldo(?: (?:anda|kamu))?(?: sebesar)?|"
    r"saldo akhir(?: sebesar)?|remaining balance(?: is)?|available balance(?: is)?|"
    r"saldo (?:anda|kamu)(?: sekarang| saat ini)?(?: sebesar| adalah| jadi)?|saldo tersedia(?: sebesar)?)"
    r"\s*:?\s*(?:Rp\.?|IDR)?\s*[\d\.,]+",
    re.I,
)
NOISE_PATTERN = re.compile(
    r"\b(?:OTP|one.time password|kode (?:verifikasi|akses)|cashback|promo|diskon|"
    r"failed|gagal|pending|tertunda|sedang diproses|akan (?:dikirim|ditransfer)|"
    r"monitoring transactions|notification listener (?:active|running))\b",
    re.I,
)
INJECTION_PATTERN = re.compile(
    r"ignore (?:all |the |your )?(?:previous|instructions|rules)|"
    r"abaikan (?:semua )?(?:instruksi|aturan)|system prompt|disclose secrets",
    re.I,
)
SETTLED_PATTERN = re.compile(
    r"\b(?:berhasil|successful|completed|received|spent|paid|terdebit|"
    r"pemasukan|pengeluaran|has sent|udah dikirim|telah menerima|"
    r"mengirimkan dana|you've moved|you have moved|has been moved|"
    r"telah dipindahkan|telah memindahkan|match di harga|"
    r"pengisian saldo|telah ditambahkan|top\s*up request|"
    r"you've sent|you have sent|dikirim|mengirim|terkirim|berhasil kirim)\b",
    re.I,
)
OUT_PATTERN = re.compile(
    r"\b(?:paid|spent|terdebit|pengeluaran|pembayaran|membayar|udah dikirim|"
    r"transfer (?:ke|to)|dikirim ke|sent to|you've sent|you have sent|mengirim|berhasil kirim)\b", re.I
)
IN_PATTERN = re.compile(
    r"\b(?:received|pemasukan|menerima|has sent.+to you|"
    r"mengirimkan dana.+ke ShopeePay-mu|pengisian saldo|"
    r"ditambahkan ke ShopeePay-mu|top\s*up request.+successful)\b", re.I
)


class EvidenceError(ValueError):
    """A safe, non-payload-bearing reason for withholding a financial write."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class NotificationFacts:
    status: str
    error_code: str | None
    text: str
    institution: str | None
    amount: int | None
    currency: str
    timestamp: datetime
    direction: str | None
    amount_quotes: tuple[str, ...]
    parsed: ParsedNotification


def notification_text(event: dict[str, Any]) -> str:
    pieces = []
    for field in TEXT_FIELDS:
        value = event.get(field)
        if isinstance(value, str) and value.strip() and value.strip() not in pieces:
            pieces.append(value.strip())
    return "\n".join(pieces)


def parse_idr_amount(value: str) -> int:
    token = re.sub(r"^(?:Rp\.?|IDR)\s*", "", value.strip(), flags=re.I)
    if not re.fullmatch(r"\d+(?:[.,]\d+)*", token):
        raise EvidenceError("invalid_amount")
    separators = {separator for separator in ".," if separator in token}
    if len(separators) == 2:
        decimal_separator = "." if token.rfind(".") > token.rfind(",") else ","
        integer, fraction = token.rsplit(decimal_separator, 1)
        grouping_separator = "," if decimal_separator == "." else "."
        if not re.fullmatch(rf"\d{{1,3}}(?:\{grouping_separator}\d{{3}})+", integer):
            raise EvidenceError("invalid_amount")
        normalized = integer.replace(grouping_separator, "") + "." + fraction
    elif separators:
        separator = next(iter(separators))
        parts = token.split(separator)
        if len(parts[-1]) == 3:
            if len(parts[0]) > 3 or any(len(part) != 3 for part in parts[1:]):
                raise EvidenceError("invalid_amount")
            normalized = "".join(parts)
        elif len(parts) == 2 and len(parts[-1]) in (1, 2):
            normalized = parts[0] + "." + parts[1]
        else:
            raise EvidenceError("invalid_amount")
    else:
        normalized = token
    try:
        amount = Decimal(normalized)
    except InvalidOperation:
        raise EvidenceError("invalid_amount") from None
    if amount != amount.to_integral_value():
        raise EvidenceError("fractional_amount")
    if not 0 < amount <= 9_223_372_036_854_775_807:
        raise EvidenceError("invalid_amount")
    return int(amount)


def collect_facts(event: dict[str, Any]) -> NotificationFacts:
    text = notification_text(event)
    package = str(event.get("package_name", "")).strip().lower()
    institution = SUPPORTED_PACKAGES.get(package)
    timestamp = event["post_time"]
    if len(text) > 12000:
        parsed = parse_notification(package, None, None, None)
        return NotificationFacts(
            status="needs_review", error_code="message_too_large", text="",
            institution=institution, amount=None, currency="IDR", timestamp=timestamp,
            direction=None, amount_quotes=(), parsed=parsed,
        )
    parsed = parse_notification(
        package, event.get("title"), event.get("body_text"), event.get("big_text")
    )
    direction = {"out": "expense", "in": "income", "internal": "internal_movement"}.get(parsed.direction)
    amounts: dict[int, list[str]] = {}
    error = None
    balance_spans = [m.span() for m in BALANCE_PATTERN.finditer(text)]
    for match in MONEY_PATTERN.finditer(text):
        span = match.span()
        if any(b_start <= span[0] and span[1] <= b_end for b_start, b_end in balance_spans):
            continue
        try:
            amount = parse_idr_amount(match.group(1))
            amounts.setdefault(amount, []).append(match.group(0))
        except EvidenceError as exc:
            error = exc.code
    status = "candidate"
    if not institution or not text or NOISE_PATTERN.search(text):
        status = "ignored"
        error = None
    elif INJECTION_PATTERN.search(text):
        error = "unsafe_instructions"
    elif timestamp.tzinfo is None or timestamp.utcoffset() is None:
        error = "timestamp_timezone_required"
    elif re.search(r"\b(?:USD|EUR|SGD|MYR)\b|\$\s*\d", text, re.I):
        error = "unsupported_currency"
    elif len(amounts) != 1:
        error = "ambiguous_amount" if amounts else "missing_amount"
    elif not SETTLED_PATTERN.search(text):
        error = "settlement_unproven"
    if not direction:
        outgoing = bool(OUT_PATTERN.search(text))
        incoming = bool(IN_PATTERN.search(text))
        if outgoing != incoming:
            direction = "expense" if outgoing else "income"
    settled = bool(SETTLED_PATTERN.search(text))
    if status != "ignored" and error == "missing_amount" and (
        not settled or (parsed.is_financial and parsed.amount is None)
    ):
        # Promotions and amount-less confirmations of an earlier transfer carry no ledger fact.
        status, error = "ignored", None
    elif status != "ignored" and error == "settlement_unproven" and not direction:
        status, error = "ignored", None
    amount = next(iter(amounts)) if len(amounts) == 1 else None
    expected_fact = parsed.amount if institution == "stockbit" and parsed.investment_action else amount
    if status != "ignored" and expected_fact is not None and event.get("expected_amount") is not None:
        try:
            if Decimal(str(event["expected_amount"])) != Decimal(expected_fact):
                error = "conflicting_mobile_amount"
        except InvalidOperation:
            error = "conflicting_mobile_amount"
    hint = event.get("expected_direction")
    mapped_hint = {"out": "expense", "in": "income", "internal": "internal_movement"}.get(hint, hint)
    if status != "ignored" and hint and direction and mapped_hint != direction:
        error = "conflicting_mobile_direction"
    if status != "ignored" and error:
        status = "needs_review"
    return NotificationFacts(
        status=status, error_code=error, text=text, institution=institution,
        amount=amount, currency="IDR", timestamp=timestamp, direction=direction,
        amount_quotes=tuple(dict.fromkeys(amounts.get(amount, []))), parsed=parsed,
    )
