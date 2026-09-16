import pytest
from app.services.notification_parser import parse_notification


def test_bank_jago_pocket_movement():
    text = "Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."
    parsed = parse_notification(
        package_name="com.jago.digitalBanking",
        title="Jago",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "transfer"
    assert parsed.amount == 500000
    assert parsed.direction == "internal"
    assert parsed.source_pocket == "Main Pocket"
    assert parsed.target_pocket == "GoPay Tabungan"
    assert parsed.category_hint == "Internal Movement"
    assert parsed.confidence >= 0.95


def test_bank_jago_single_pocket_out():
    text = "You've moved Rp500.000 out of your My Emergency Fund Pocket. Need help? Contact Tanya Jago at 1500 746."
    parsed = parse_notification(
        package_name="com.jago.digitalBanking",
        title="Jago",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "transfer"
    assert parsed.amount == 500000
    assert parsed.direction == "internal"
    assert parsed.source_pocket == "My Emergency Fund"
    assert parsed.target_pocket == "Kantong Utama"
    assert parsed.counterparty == "My Emergency Fund → Kantong Utama"
    assert parsed.category_hint == "Internal Movement"


def test_bank_jago_single_pocket_in():
    text = "You've moved Rp250.000 into your Tabungan Pocket."
    parsed = parse_notification(
        package_name="com.jago.digitalBanking",
        title="Jago",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "transfer"
    assert parsed.amount == 250000
    assert parsed.direction == "internal"
    assert parsed.source_pocket == "Kantong Utama"
    assert parsed.target_pocket == "Tabungan"
    assert parsed.counterparty == "Kantong Utama → Tabungan"
    assert parsed.category_hint == "Internal Movement"


def test_bank_jago_single_pocket_out_id():
    text = "Kamu telah memindahkan Rp500.000 keluar dari Dana Darurat Kantong."
    parsed = parse_notification(
        package_name="com.jago.digitalBanking",
        title="Jago",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "transfer"
    assert parsed.amount == 500000
    assert parsed.direction == "internal"
    assert parsed.source_pocket == "Dana Darurat"
    assert parsed.target_pocket == "Kantong Utama"


def test_bank_jago_inbound_transfer():
    text = "Alfonsus Enrico Soebijanto has sent Rp500.050 to you. Need help? Contact Tanya Jago at 1500 746."
    parsed = parse_notification(
        package_name="com.jago.digitalBanking",
        title="Jago",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "income"
    assert parsed.amount == 500050
    assert parsed.direction == "in"
    assert "Alfonsus Enrico Soebijanto" in (parsed.counterparty or "")


def test_gopay_outbound_transfer():
    text = "Rp500.000 udah dikirim ke BCA ALFONSUS ENRICO SOEBIJAN."
    parsed = parse_notification(
        package_name="com.gojek.gopay",
        title="Transfer berhasil",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "expense"
    assert parsed.amount == 500000
    assert parsed.direction == "out"
    assert parsed.counterparty == "BCA ALFONSUS ENRICO SOEBIJAN"


def test_gopay_inbound_tabungan():
    text = "Anda telah menerima Tabungan by Jago sebanyak Rp500.000."
    parsed = parse_notification(
        package_name="com.gojek.gopay",
        title="GoPay",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "income"
    assert parsed.amount == 500000
    assert parsed.direction == "in"
    assert "Tabungan by Jago" in (parsed.counterparty or "")


def test_bca_mobile_inbound():
    text = "Financial Diary: Pemasukan sebesar IDR 500,000.00 dari ***PET **AK ***GSA di kategori Transfer Rekening."
    parsed = parse_notification(
        package_name="com.bca",
        title="BCA mobile",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "income"
    assert parsed.amount == 500000
    assert parsed.direction == "in"
    assert "***PET **AK ***GSA" in (parsed.counterparty or "")
    assert parsed.category_hint == "Transfer Rekening"


def test_mybca_inbound():
    text = "You received IDR 500,000.00 from ***PET **AK ***GSA at Account Transfer category."
    parsed = parse_notification(
        package_name="com.bca.mybca.omni.android",
        title="Financial Diary",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "income"
    assert parsed.amount == 500000
    assert parsed.direction == "in"
    assert "***PET **AK ***GSA" in (parsed.counterparty or "")


def test_bca_mobile_outbound():
    text = "Financial Diary: Pengeluaran sebesar IDR 500,000.00 di kategori Pembayaran."
    parsed = parse_notification(
        package_name="com.bca",
        title="BCA mobile",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "expense"
    assert parsed.amount == 500000
    assert parsed.direction == "out"
    assert parsed.category_hint == "Pembayaran"


def test_mybca_outbound():
    text = "You spent IDR 500,000.00 at Payment."
    parsed = parse_notification(
        package_name="com.bca.mybca.omni.android",
        title="Financial Diary",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "expense"
    assert parsed.amount == 500000
    assert parsed.direction == "out"


def test_shopeepay_inbound():
    text = "ALFONSUS ENRICO SOEBIJANTO mengirimkan dana sebesar Rp500.000 ke ShopeePay-mu melalui BI-Fast."
    parsed = parse_notification(
        package_name="com.shopeepay.id",
        title="Saldo ShopeePay diterima!",
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "income"
    assert parsed.amount == 500000
    assert parsed.direction == "in"
    assert parsed.counterparty == "ALFONSUS ENRICO SOEBIJANTO"


def test_noise_filtering():
    promo_text = "Transferan ke sesama GoPay buat dapetin puzzle & klaim hadiah miliaran. Yuk transferan💸"
    parsed_promo = parse_notification(
        package_name="com.gojek.gopay",
        title="Hadiah miliaran, cek!",
        body_text=promo_text,
    )
    assert parsed_promo.is_financial is False
    assert parsed_promo.event_class == "noise"

    va_text = "Transfer Rp500.000 ke BCA Virtual Account sebelum 16 Sep 2026, 08:25 untuk kami teruskan ke ALFONSUS ENRICO SOEBIJANTO TABUNGAN BY JAGO."
    parsed_va = parse_notification(
        package_name="com.gojek.gopay",
        title="Transfer ke nomor VA sekarang, ya 🔔",
        body_text=va_text,
    )
    assert parsed_va.is_financial is False
    assert parsed_va.event_class == "noise"

    inflight_text = "Kami lagi memproses pengiriman Rp500.000 ke TABUNGAN BY JAGO."
    parsed_inflight = parse_notification(
        package_name="com.gojek.gopay",
        title="Uangmu lagi dikirim 🚀",
        body_text=inflight_text,
    )
    assert parsed_inflight.is_financial is False
    assert parsed_inflight.event_class == "noise"


def test_stockbit_buy_match_4_lots():
    title = "Pembelian BBRI Fully Match"
    text = "Pembelian 4 lot BBRI match di harga Rp3.340"
    parsed = parse_notification(
        package_name="com.stockbit.android",
        title=title,
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "expense"
    assert parsed.amount == 1336000  # 4 * 100 * 3340
    assert parsed.direction == "out"
    assert "BBRI" in (parsed.counterparty or "")
    assert "4 lot @ Rp3.340" in (parsed.counterparty or "")
    assert parsed.category_hint == "Investasi"
    assert parsed.confidence == 1.0
    assert parsed.symbol == "BBRI"
    assert parsed.instrument_symbol == "BBRI.JK"
    assert parsed.lots == 4
    assert parsed.units == 400.0
    assert parsed.price_per_unit == 3340
    assert parsed.investment_action == "buy"


def test_stockbit_buy_match_10_lots():
    title = "Pembelian BBRI Fully Match"
    text = "Pembelian 10 lot BBRI match di harga Rp3.340"
    parsed = parse_notification(
        package_name="com.stockbit.android",
        title=title,
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "expense"
    assert parsed.amount == 3340000  # 10 * 100 * 3340
    assert parsed.direction == "out"
    assert "BBRI" in (parsed.counterparty or "")
    assert "10 lot @ Rp3.340" in (parsed.counterparty or "")
    assert parsed.category_hint == "Investasi"
    assert parsed.symbol == "BBRI"
    assert parsed.instrument_symbol == "BBRI.JK"
    assert parsed.lots == 10
    assert parsed.units == 1000.0
    assert parsed.price_per_unit == 3340
    assert parsed.investment_action == "buy"


def test_stockbit_sell_match():
    title = "Penjualan BBCA Fully Match"
    text = "Penjualan 5 lot BBCA match di harga Rp10.150"
    parsed = parse_notification(
        package_name="com.stockbit.android",
        title=title,
        body_text=text,
        big_text=text,
    )
    assert parsed.is_financial is True
    assert parsed.event_class == "income"
    assert parsed.amount == 5075000  # 5 * 100 * 10150
    assert parsed.direction == "in"
    assert "BBCA" in (parsed.counterparty or "")
    assert parsed.category_hint == "Investasi"
    assert parsed.symbol == "BBCA"
    assert parsed.instrument_symbol == "BBCA.JK"
    assert parsed.lots == 5
    assert parsed.units == 500.0
    assert parsed.price_per_unit == 10150
    assert parsed.investment_action == "sell"

