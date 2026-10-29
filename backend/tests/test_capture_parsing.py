"""Deterministic facts of typed captures (text-capture D1). All texts, names and amounts are synthetic."""
from datetime import datetime, timedelta, timezone

import pytest

from app.services.capture_parsing import CaptureDateError, parse_amount_answer, parse_capture

JAKARTA = timezone(timedelta(hours=7))
SENT = datetime(2026, 10, 29, 9, 30, tzinfo=JAKARTA)
ACCOUNTS = [
    {"id": "cash", "name": "Tunai", "type": "cash"},
    {"id": "bca", "name": "BCA Harian", "type": "bank"},
    {"id": "rdn", "name": "RDN BCA", "type": "bank"},
    {"id": "jago", "name": "Bank Jago", "type": "bank", "default_pocket_id": "main"},
    {"id": "main", "name": "Kantong Utama", "type": "bank", "parent_id": "jago"},
    {"id": "fund", "name": "Dana Darurat", "type": "bank", "parent_id": "jago"},
    {"id": "gopay", "name": "GoPay", "type": "ewallet"},
]


def parse(text, sent=SENT, aliases=()):
    return parse_capture(text, sent, accounts=ACCOUNTS, aliases=list(aliases), tz="Asia/Jakarta")


@pytest.mark.parametrize("text, amount", [
    ("bayar parkir ptc 10rb", 10_000),
    ("kopi 25k", 25_000),
    ("servis motor 1,5jt", 1_500_000),
    ("beli bensin Rp20.000", 20_000),
    ("beli bensin Rp 20.000,00", 20_000),
    ("parkir 10.000", 10_000),
    ("parkir 10000", 10_000),
    ("makan 10 ribu", 10_000),
    ("laptop 1.5 juta", 1_500_000),
    ("sewa 2jt", 2_000_000),
    ("donasi 2,25jt", 2_250_000),
    ("tiket 1.500rb", 1_500_000),
    ("parkir 500", 500),
    ("tgl 3 bayar listrik 300.000", 300_000),
    ("kopi 2 gelas 25k", 25_000),
])
def test_amount_shorthand(text, amount):
    facts = parse(text)
    assert (facts.amount, facts.amount_issue) == (amount, None)


@pytest.mark.parametrize("text, issue", [
    ("bayar parkir", "missing"),
    ("servis 1.5", "missing"),
    ("servis 1,5", "missing"),
    ("kopi 2 gelas", "missing"),
    ("parkir 10rb dan kopi 25rb", "several"),
    ("parkir 10rb kopi Rp25.000", "several"),
    ("parkir 10,000", "invalid"),
    ("sewa 1,125jt", "invalid"),
    ("donasi 0rb", "invalid"),
    ("tagihan 2000000jt", "invalid"),
])
def test_ambiguous_missing_or_several_amounts_are_not_guessed(text, issue):
    facts = parse(text)
    assert (facts.amount, facts.amount_issue) == (None, issue)


def test_amount_answer_needs_exactly_one_expression():
    assert parse_amount_answer("10rb") == 10_000
    assert parse_amount_answer("Rp 25.000") == 25_000
    assert parse_amount_answer("1.5") is None
    assert parse_amount_answer("10rb 20rb") is None
    assert parse_amount_answer("sepuluh") is None


@pytest.mark.parametrize("text, kind, form", [
    ("bayar parkir 10rb", "expense", None),
    ("es teh 5rb", "expense", None),
    ("terima gaji 5jt", "income", None),
    ("refund tiket 200rb", "income", None),
    ("transfer masuk dari teman 50rb", "income", None),
    ("tarik tunai 200rb", "movement", "withdraw"),
    ("setor tunai 1jt", "movement", "deposit"),
    ("pindah 50rb ke GoPay", "movement", "transfer"),
    ("top up 50rb ke gopay", "movement", "top_up"),
    ("isi saldo gopay 100rb", "movement", "top_up"),
    ("terima kasih traktir 30rb", "expense", None),
    ("dapat refund tapi bayar ongkir 20rb", None, None),
    ("pindah 50rb ke gopay terima", None, "transfer"),
])
def test_kind_word_table(text, kind, form):
    facts = parse(text)
    assert (facts.kind, facts.movement_form) == (kind, form)


def test_relative_and_explicit_dates():
    assert parse("kemarin makan bakso 20rb").date.date().isoformat() == "2026-10-28"
    evening = parse("tadi malam nasi goreng 20rb").date
    assert (evening.date().isoformat(), evening.hour) == ("2026-10-28", 20)
    morning = parse("tadi pagi bubur 10rb").date
    assert (morning.date().isoformat(), morning.hour) == ("2026-10-29", 8)
    assert parse("tadi pagi bubur 10rb", SENT.replace(hour=7)).date == SENT.replace(hour=7)
    assert parse("tgl 3 listrik 300rb").date.date().isoformat() == "2026-10-03"
    assert parse("tgl 30 sewa 1jt").date.date().isoformat() == "2026-09-30"
    assert parse("3/10 makan 20rb").date.date().isoformat() == "2026-10-03"
    assert parse("3/10/2025 makan 20rb").date.date().isoformat() == "2025-10-03"
    assert parse("makan 20rb").date == SENT


@pytest.mark.parametrize("text", ["3/12 makan 20rb", "31/2 makan 20rb", "tgl 32 makan 20rb", "kemarin tgl 3 makan 20rb"])
def test_future_impossible_or_repeated_dates_are_rejected(text):
    with pytest.raises(CaptureDateError):
        parse(text)


def test_account_mentions_and_phrases():
    cash = parse("es teh manis 5rb tunai")
    assert [(item.role, item.candidates) for item in cash.mentions] == [(None, ("cash",))]
    assert (cash.phrase, cash.description) == ("es teh manis", "Es teh manis")
    pocket = parse("beli pulsa 50rb pakai jago")
    assert [item.candidates for item in pocket.mentions] == [("main",)]
    assert pocket.phrase == "pulsa"
    ambiguous = parse("parkir 5rb pakai bca")
    assert [item.candidates for item in ambiguous.mentions] == [("bca", "rdn")]
    named = parse("parkir 5rb pakai bca harian")
    assert [item.candidates for item in named.mentions] == [("bca",)]
    alias = parse("kopi 20rb pakai dompet kantor", aliases=[{"name": "Dompet Kantor", "account_id": "gopay"}])
    assert [item.candidates for item in alias.mentions] == [("gopay",)]
    assert parse("bayar parkir ptc 10rb").phrase == parse("parkir ptc 10rb").phrase == "parkir ptc"


def test_movement_roles():
    withdraw = parse("tarik tunai 200rb")
    assert [(item.role, item.candidates) for item in withdraw.mentions] == [("target", ("cash",))]
    assert withdraw.phrase == ""
    withdraw_from = parse("tarik tunai 200rb dari bca harian")
    assert {item.role: item.candidates for item in withdraw_from.mentions} == {"target": ("cash",), "source": ("bca",)}
    transfer = parse("pindah 50rb ke GoPay")
    assert [(item.role, item.candidates) for item in transfer.mentions] == [("target", ("gopay",))]
    top_up = parse("isi saldo gopay 100rb dari bca harian")
    assert {item.role: item.candidates for item in top_up.mentions} == {"target": ("gopay",), "source": ("bca",)}
    deposit = parse("setor tunai 1jt ke bca harian")
    assert {item.role: item.candidates for item in deposit.mentions} == {"source": ("cash",), "target": ("bca",)}
