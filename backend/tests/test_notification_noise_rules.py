from datetime import datetime, timezone

import pytest

from app.services.notification_evidence import collect_facts


def facts(text, package="com.jago.digitalbanking", title=None):
    return collect_facts({
        "package_name": package, "title": title, "body_text": text,
        "post_time": datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc),
    })


def test_settled_payment_mentioning_cashback_is_kept():
    result = facts(
        "Kamu baru aja bayar Rp45.000 ke Kedai Contoh pake GoPay. Dapat cashback!",
        package="com.gojek.gopay", title="Pembayaran berhasil",
    )
    assert result.status != "ignored"
    assert result.amount == 45_000
    assert result.direction == "expense"


@pytest.mark.parametrize(("text", "reason"), [
    ("Pay with Jago and get up to 30K BONUS before it's too late", "promotional_or_market"),
    ("Get up to $6.000 CREDIT BONUS with Digital Pro Card!", "promotional_or_market"),
    ("IHSG naik +1,12%, Emiten Contoh dapat komitmen investasi US$555 Juta", "promotional_or_market"),
    ("OJK mengenakan denda Rp1,5 T dan Rp500 M kepada emiten contoh", "headline_figures"),
    ("Transfer Rp350.000 ke BCA Virtual Account sebelum 11 Oct 2026, 22:55", "hard_noise"),
    ("Kami lagi memproses pengiriman Rp350.000 ke Tabungan Contoh", "hard_noise"),
    ("MERCHANT_CONTOH is requesting USD 0. Please confirm or reject", "hard_noise"),
    ("You spent on 3 categories in September. Learn more", "no_amount"),
    ("Temukan pengeluaran paling boros & hematmu. Cek catatan keuanganmu sekarang", "no_amount"),
])
def test_noise_is_ignored_instead_of_reviewed(text, reason):
    result = facts(text)
    assert result.status == "ignored"
    assert result.error_code is None
    assert result.ignore_reason == reason


def test_settled_foreign_currency_payment_is_still_reviewed():
    result = facts("You've paid USD 10.00 using your debit card")
    assert result.status == "needs_review"
    assert result.error_code == "unsupported_currency"


def test_promotional_word_alone_does_not_hide_recognised_transfer():
    result = facts("You've moved Rp100.000 from Subscriptions Pocket to your Main Pocket. Promo bulan ini!")
    assert result.status != "ignored"
    assert result.amount == 100_000


def test_one_time_code_is_still_hard_noise():
    result = facts("Kode OTP 123456 untuk transaksi Rp50.000", package="com.bca.mybca.omni.android")
    assert result.status == "ignored"
    assert result.ignore_reason == "hard_noise"


def test_full_amount_next_to_headline_figure_is_not_headline_only():
    result = facts("You've paid Rp125.000 to Kedai Contoh. Limit harian Rp5 jt")
    assert result.ignore_reason != "headline_figures"
