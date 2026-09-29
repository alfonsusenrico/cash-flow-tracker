"""Owner identity, counterparty, and movement pairing rules on real notification wording."""

from copy import deepcopy
from datetime import datetime

import pytest

from evaluation.notification_cases import BASE_CONTEXT, fixture_id
from app.services.notification_context import sanitize_text
from app.services.notification_evidence import collect_facts
from app.services.notification_resolution import (
    endpoint_context, is_external_counterparty, names_owner, owner_aliases, redact_owner_names,
    signatures_compatible, transfer_signature,
)

OWNER = "Alfonsus Enrico Soebijanto"


@pytest.mark.parametrize("text", [
    "ALFONSUS ENRICO SOEBIJANTO mengirimkan dana sebesar Rp7.491.557 ke ShopeePay-mu",
    "Alfonsus Enrico Soebijanto has sent Rp7.491.557 to you.",
    "You received IDR 1,500,000.00 from ALFO**US ***ICO *O at Account Transfer category.",
    "Rp500.000 udah dikirim ke BCA ALFONSUS ENRICO SOEBIJAN.",
])
def test_owner_is_recognised_literally_masked_and_truncated(text):
    assert names_owner(text, [OWNER])


@pytest.mark.parametrize("text", [
    "You received IDR 43,000.00 from ****LIN VALE**IA P at Account Transfer category.",
    "Pemasukan sebesar IDR 500,000.00 dari ***PET **AK ***GSA di kategori Transfer Rekening.",
    "Al sent Rp5.000",
    "Rp500.000 udah dikirim ke BCA Mira Langit.",
])
def test_third_parties_and_short_tokens_are_not_the_owner(text):
    assert not names_owner(text, [OWNER])


def test_single_word_alias_requires_whole_word():
    assert names_owner("Transfer dari ENRICO berhasil", ["Enrico"])
    assert not names_owner("Transfer dari ENRICHMENT berhasil", ["Enrico"])


def test_redaction_replaces_masked_owner_span_only():
    text = "You received IDR 1,500,000.00 from ALFO**US ***ICO *O at Account Transfer category."
    assert redact_owner_names(text, [OWNER]) == "You received IDR 1,500,000.00 from [SELF] at Account Transfer category."


def test_sanitize_text_redacts_every_alias_and_keeps_money():
    text = "ALFONSUS ENRICO SOEBIJANTO mengirimkan dana sebesar Rp7.491.557 ke ShopeePay-mu"
    assert sanitize_text(text, ["Enrico Personal", OWNER]) == "[SELF] mengirimkan dana sebesar Rp7.491.557 ke ShopeePay-mu"


def test_owner_aliases_include_profile_name_and_deduplicate():
    profile = {"name": OWNER, "name_aliases": ["  alfonsus   enrico soebijanto ", "ENRICO S", "ab"]}
    assert owner_aliases(profile) == [OWNER, "ENRICO S"]


@pytest.mark.parametrize("counterparty,hint,expected", [
    ("BCA Payment", "Shopping", False),
    ("ShopeePay Top Up", "Transfer Masuk", False),
    ("Bank Jago Payment", None, False),
    ("Tabungan by Jago", None, False),
    ("BCA ALFONSUS ENRICO SOEBIJAN", "Transfer Keluar", False),
    ("BCA Mira Langit", "Transfer Keluar", True),
    ("****LIN VALE**IA P", "Transfer Masuk", True),
    ("Kedai Awan", "Tagihan & Utilitas", True),
    ("Netflix", "Tagihan & Utilitas", True),
    ("BCA", "Salary", True),
    (None, None, False),
])
def test_external_counterparty_classification(counterparty, hint, expected):
    assert is_external_counterparty(counterparty, hint, [OWNER]) is expected


def signature_for(package, text, timestamp, identity=OWNER):
    facts = collect_facts({"package_name": package, "body_text": text, "post_time": datetime.fromisoformat(timestamp)})
    assert facts.status == "candidate", (text, facts.error_code)
    context = deepcopy(BASE_CONTEXT)
    context["notification"] = sanitize_text(text, identity)
    context["self_identity_detected"] = names_owner(text, [identity])
    context["facts"] = {"external_counterparty": is_external_counterparty(
        facts.parsed.counterparty, facts.parsed.category_hint, [identity])}
    account_id = endpoint_context(facts, context["accounts"])["effective_account_id"]
    return transfer_signature(facts, context, account_id)


def test_bca_debit_pairs_with_shopeepay_top_up_without_transfer_wording():
    bca = signature_for("com.bca.mybca.omni.android", "You spent IDR 7,490,557.00 at Shopping.", "2026-09-29T13:08:54+07:00")
    shopee = signature_for(
        "com.shopeepay.id",
        "Pengisian saldo sebesar Rp7.490.557 telah ditambahkan ke ShopeePay-mu. Saldo saat ini sebesar Rp7.491.557.",
        "2026-09-29T13:08:51+07:00",
    )
    assert bca["root_account_id"] == fixture_id(1) and shopee["root_account_id"] == fixture_id(6)
    assert signatures_compatible(bca, shopee) and signatures_compatible(shopee, bca)


def test_indonesian_send_verb_counts_as_transfer_evidence():
    shopee = signature_for(
        "com.shopeepay.id",
        "ALFONSUS ENRICO SOEBIJANTO mengirimkan dana sebesar Rp7.491.557 ke ShopeePay-mu melalui BI-Fast.",
        "2026-09-29T14:38:18+07:00",
    )
    assert shopee["transfer"] and shopee["self_identity"] and not shopee["external_counterparty"]


def test_two_incoming_legs_never_pair():
    jago = signature_for("com.jago.digitalbanking", "Alfonsus Enrico Soebijanto has sent Rp7.491.557 to you.", "2026-09-29T14:34:21+07:00")
    shopee = signature_for(
        "com.shopeepay.id",
        "ALFONSUS ENRICO SOEBIJANTO mengirimkan dana sebesar Rp7.491.557 ke ShopeePay-mu melalui BI-Fast.",
        "2026-09-29T14:38:18+07:00",
    )
    assert not signatures_compatible(jago, shopee)


def test_third_party_payment_does_not_pair_with_equal_income():
    gopay = signature_for("com.gojek.gopay", "Rp500.000 udah dikirim ke BCA Mira Langit.", "2026-09-29T10:00:00+07:00")
    shopee = signature_for(
        "com.shopeepay.id", "Pengisian saldo sebesar Rp500.000 telah ditambahkan ke ShopeePay-mu.", "2026-09-29T10:01:00+07:00",
    )
    assert gopay["external_counterparty"]
    assert not signatures_compatible(gopay, shopee)


def test_same_institution_legs_do_not_pair_by_amount_alone():
    left = {"account_id": "a", "root_account_id": "bank", "direction": "expense", "currency": "IDR"}
    right = {"account_id": "b", "root_account_id": "bank", "direction": "income", "currency": "IDR"}
    assert not signatures_compatible(left, right)


def test_legacy_signatures_without_roots_keep_strict_rule():
    left = {"account_id": "a", "direction": "expense", "currency": "IDR", "transfer": True}
    right = {"account_id": "b", "direction": "income", "currency": "IDR", "transfer": True}
    assert not signatures_compatible(left, right)


def test_failed_retry_schedule_escalates_and_caps():
    from app.services.notification_processing import failed_retry_delay

    assert [failed_retry_delay(attempt) for attempt in range(1, 9)] == [30, 120, 600, 1800, 3600, 10800, 10800, 10800]


@pytest.mark.parametrize("value,window,error", [
    ("900", 900, None), ("30", 30, None), ("86400", 86400, None),
    ("29", 900, "processor_configuration_invalid"), ("abc", 900, "processor_configuration_invalid"),
])
def test_pairing_window_configuration_bounds(monkeypatch, value, window, error):
    from app.core.config import load_settings

    monkeypatch.setenv("NOTIFICATION_PAIRING_WINDOW_SECONDS", value)
    loaded = load_settings()
    assert loaded.notification_pairing_window_seconds == window
    assert loaded.notification_ai_configuration_error == error


def wallet_hosted_in_bank_context():
    context = deepcopy(BASE_CONTEXT)
    context["accounts"] = [row for row in context["accounts"] if row["name"] != "GoPay"]
    context["accounts"].append({"id": fixture_id(10), "name": "GoPay Tabungan", "type": "ewallet", "parent_id": fixture_id(2)})
    return context


def test_wallet_app_notifications_map_to_a_pocket_when_no_top_level_account_exists():
    facts = collect_facts({"package_name": "com.gopay.wallet", "body_text": "QRIS Rp32.000 ke Kedai Awan berhasil",
                           "post_time": datetime.fromisoformat("2026-09-29T17:00:00+07:00")})
    assert endpoint_context(facts, wallet_hosted_in_bank_context()["accounts"]) == {"effective_account_id": fixture_id(10)}


def test_bank_pocket_move_reaches_the_hosted_wallet_pocket():
    facts = collect_facts({"package_name": "com.jago.digitalBanking",
                           "body_text": "Rp1.250.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket.",
                           "post_time": datetime.fromisoformat("2026-09-29T17:00:00+07:00")})
    context = wallet_hosted_in_bank_context()
    assert endpoint_context(facts, context["accounts"]) == {
        "source_account_id": fixture_id(3), "target_account_id": fixture_id(10),
    }


def test_top_level_institution_account_still_wins_over_pockets():
    from app.services.notification_resolution import institution_parent

    context = deepcopy(BASE_CONTEXT)
    context["accounts"].append({"id": fixture_id(10), "name": "GoPay Tabungan", "type": "ewallet", "parent_id": fixture_id(2)})
    assert institution_parent(context["accounts"], "gopay")["id"] == fixture_id(5)
