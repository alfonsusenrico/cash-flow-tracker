"""Regression tests for fix-notification-mapping-and-stockbit-noise. Synthetic data only."""
from datetime import datetime, timezone
from uuid import uuid4

from app.services.notification_evidence import collect_facts
from app.services.notification_processing import reclassify_review_noise
from app.services.notification_resolution import (
    alias_key,
    institution_parent,
    normalize_name,
    pocket_endpoint,
)


def _event(package_name: str, *, title: str = "", body_text: str = "", big_text: str = "") -> dict:
    return {
        "package_name": package_name,
        "title": title,
        "body_text": body_text,
        "big_text": big_text,
        "post_time": datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc),
    }


def test_normalize_name_strips_locked_and_terkunci_words():
    assert normalize_name("Dana Tagihan Locked Pocket") == "dana tagihan"
    assert normalize_name("Dana Tagihan Locked") == "dana tagihan"
    assert normalize_name("Kantong Dana Tagihan Terkunci") == "dana tagihan"
    assert normalize_name("Locked Savings") == "tabungan"


def test_pocket_endpoint_precedence_owner_then_active_name_then_ai_alias():
    parent = {"id": "jago-root", "name": "Bank Jago", "type": "bank", "parent_id": None}
    wrong_pocket = {
        "id": "pocket-wrong",
        "name": "Dana Darurat",
        "type": "bank",
        "parent_id": "jago-root",
        "learned_notification_names": [
            {"key": alias_key("jago", "bayar kredit"), "confirmed_by": "ai"},
        ],
    }
    right_pocket = {
        "id": "pocket-right",
        "name": "Bayar Kredit",
        "type": "bank",
        "parent_id": "jago-root",
        "learned_notification_names": [],
    }
    accounts = [parent, wrong_pocket, right_pocket]

    # Active pocket name "Bayar Kredit" beats stale AI-learned alias pointing to another pocket.
    assert pocket_endpoint("Bayar Kredit", parent, accounts, institution="jago")["id"] == "pocket-right"
    # Locked suffix normalizes to "bayar kredit" and also resolves directly to the active pocket.
    assert pocket_endpoint("Bayar Kredit Locked Pocket", parent, accounts, institution="jago")["id"] == "pocket-right"

    # Explicit owner-confirmed alias still overrides the active account name.
    owner_override = {
        **wrong_pocket,
        "learned_notification_names": [
            {"key": alias_key("jago", "bayar kredit"), "confirmed_by": "owner"},
        ],
    }
    assert pocket_endpoint("Bayar Kredit", parent, [parent, owner_override, right_pocket], institution="jago")["id"] == "pocket-wrong"

    # When no active pocket matches, an AI-learned alias still acts as fallback.
    ai_only = {
        **wrong_pocket,
        "learned_notification_names": [
            {"key": alias_key("jago", "lama"), "confirmed_by": "ai"},
        ],
    }
    assert pocket_endpoint("Kantong Lama", parent, [parent, ai_only, right_pocket], institution="jago")["id"] == "pocket-wrong"


def test_institution_parent_precedence_owner_then_active_name_then_ai_alias():
    real_jago = {"id": "jago-root", "name": "Bank Jago", "type": "bank", "parent_id": None, "learned_notification_names": []}
    other_bank = {
        "id": "other-root",
        "name": "Bank Lain",
        "type": "bank",
        "parent_id": None,
        "learned_notification_names": [{"key": alias_key("jago", ""), "confirmed_by": "ai"}],
    }
    assert institution_parent([real_jago, other_bank], "jago")["id"] == "jago-root"

    owner_aliased = {
        **other_bank,
        "learned_notification_names": [{"key": alias_key("jago", ""), "confirmed_by": "owner"}],
    }
    assert institution_parent([real_jago, owner_aliased], "jago")["id"] == "other-root"


def test_stockbit_market_news_with_usd_is_ignored_while_trades_deposits_and_bank_usd_stay_intact():
    # 1. Stockbit dividend / yield / commodity news containing US$ -> ignored (promotional_or_market).
    dividend = collect_facts(_event(
        "com.stockbit.android",
        title="Contoh: Emiten A Bagi Dividen Interim US$0,08/Saham",
        body_text="Yield diperkirakan 4,2% dengan asumsi kurs Rp 15.800 untuk tahun buku 2026.",
    ))
    assert dividend.status == "ignored"
    assert dividend.ignore_reason == "promotional_or_market"
    assert dividend.error_code is None

    commodity = collect_facts(_event(
        "com.stockbit.android",
        title="Harga Komoditas Hari Ini",
        body_text="Harga nikel di pasar global turun di level US$16.200/ton menyusul sell-off.",
    ))
    assert commodity.status == "ignored"
    assert commodity.ignore_reason == "promotional_or_market"

    commentary = collect_facts(_event(
        "com.stockbit.android",
        title="Ringkasan Pasar Pagi",
        body_text="Indeks global bergerak di kisaran USD 5.100 menjelang rilis data inflasi.",
    ))
    assert commentary.status == "ignored"
    assert commentary.ignore_reason == "promotional_or_market"

    # 2. Real Stockbit trade match -> candidate (not ignored).
    trade = collect_facts(_event(
        "com.stockbit.android",
        title="Order Match",
        body_text="Order Beli ABCD sebanyak 1 lot telah match di harga Rp 9.500",
    ))
    assert trade.status == "candidate"
    assert trade.error_code is None

    # 3. Real Stockbit RDN deposit -> candidate (not ignored).
    deposit = collect_facts(_event(
        "com.stockbit.android",
        title="Deposit Berhasil",
        body_text="Deposit Rp 1.500.000 sudah dapat digunakan untuk transaksi.",
    ))
    assert deposit.status == "candidate"
    assert deposit.error_code is None

    # 4. Bank Jago settled USD payment -> still needs_review with unsupported_currency.
    jago_usd = collect_facts(_event(
        "com.jago.digitalbanking",
        title="Transaction successful",
        body_text="You've paid USD 10.00 at Example Store using your Main Pocket.",
    ))
    assert jago_usd.status == "needs_review"
    assert jago_usd.error_code == "unsupported_currency"


def test_reclassify_review_noise_clears_unlinked_stockbit_news_only(client):
    from app.db.pool import db_conn

    user_id = str(uuid4())
    account_id = str(uuid4())
    tx_id = str(uuid4())
    news_id = str(uuid4())
    linked_news_id = str(uuid4())
    jago_usd_id = str(uuid4())
    now = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)

    with db_conn() as conn:
        conn.execute(
            "INSERT INTO users (id, username, name, password_hash, invite_code, language) VALUES (%s, %s, 'Sari Contoh', 'synthetic-hash', 'TEST', 'id')",
            (user_id, f"stockbit_{uuid4().hex}"),
        )
        conn.execute(
            "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'Bank Jago', 'bank', 1000000)",
            (account_id, user_id),
        )
        conn.execute(
            """INSERT INTO transactions (id, user_id, account_id, type, amount, date, source, review_status)
               VALUES (%s, %s, %s, 'expense', 50000, %s, 'notification', 'reviewed')""",
            (tx_id, user_id, account_id, now),
        )
        # Unlinked Stockbit news in needs_review -> MUST be moved to ignored.
        conn.execute(
            """INSERT INTO notification_events (id, user_id, device_id, package_name, title, body_text,
                   post_time, payload_hash, processing_state, error_code)
               VALUES (%s, %s, 'synthetic', 'com.stockbit.android', 'Emiten A Bagi Dividen Interim',
                   %s, %s, %s, 'needs_review', 'unsupported_currency')""",
            (news_id, user_id, "Dividen US$0,05/saham dengan yield 4%", now, uuid4().hex),
        )
        # Linked row -> MUST NOT be touched even if text looks like noise.
        conn.execute(
            """INSERT INTO notification_events (id, user_id, device_id, package_name, title, body_text,
                   post_time, payload_hash, processing_state, error_code, transaction_id)
               VALUES (%s, %s, 'synthetic', 'com.stockbit.android', 'Emiten A Bagi Dividen Interim',
                   'Dividen US$0,05/saham', %s, %s, 'needs_review', 'unsupported_currency', %s)""",
            (linked_news_id, user_id, now, uuid4().hex, tx_id),
        )
        # Real Bank Jago USD payment -> MUST remain needs_review.
        conn.execute(
            """INSERT INTO notification_events (id, user_id, device_id, package_name, title, body_text,
                   post_time, payload_hash, processing_state, error_code)
               VALUES (%s, %s, 'synthetic', 'com.jago.digitalbanking', 'Payment successful',
                   'You''ve paid USD 12.00 at Example Shop', %s, %s, 'needs_review', 'unsupported_currency')""",
            (jago_usd_id, user_id, now, uuid4().hex),
        )
        conn.commit()

    try:
        moved = reclassify_review_noise()
        assert moved >= 1
        with db_conn() as conn:
            rows = {
                str(r["id"]): (r["processing_state"], r["error_code"])
                for r in conn.execute(
                    "SELECT id, processing_state, error_code FROM notification_events WHERE id = ANY(%s)",
                    ([news_id, linked_news_id, jago_usd_id],),
                ).fetchall()
            }
        assert rows[news_id] == ("ignored", None)
        assert rows[linked_news_id] == ("needs_review", "unsupported_currency")
        assert rows[jago_usd_id] == ("needs_review", "unsupported_currency")
    finally:
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
            conn.commit()
