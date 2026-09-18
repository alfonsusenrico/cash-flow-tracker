from uuid import uuid4
from app.services.category_rules import resolve_category_for_notification
from app.services.notification_parser import parse_notification


SAMPLE_CATEGORIES = [
    {"id": uuid4(), "name": "Makanan & Minuman", "kind": "expense", "kakeibo_type": "need", "is_excluded_from_budget": False},
    {"id": uuid4(), "name": "Transportasi", "kind": "expense", "kakeibo_type": "need", "is_excluded_from_budget": False},
    {"id": uuid4(), "name": "Tagihan & Utilitas", "kind": "expense", "kakeibo_type": "need", "is_excluded_from_budget": False},
    {"id": uuid4(), "name": "Belanja", "kind": "expense", "kakeibo_type": "want", "is_excluded_from_budget": False},
    {"id": uuid4(), "name": "Kesehatan", "kind": "expense", "kakeibo_type": "need", "is_excluded_from_budget": False},
    {"id": uuid4(), "name": "Internal Movement", "kind": "expense", "kakeibo_type": None, "is_excluded_from_budget": True},
    {"id": uuid4(), "name": "Investasi", "kind": "expense", "kakeibo_type": "saving", "is_excluded_from_budget": True},
    {"id": uuid4(), "name": "Gaji", "kind": "income", "kakeibo_type": "need", "is_excluded_from_budget": False},
]


def test_jago_pocket_internal_movement():
    text = "Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."
    parsed = parse_notification("com.jago.digitalBanking", "Jago", text, text)
    result = resolve_category_for_notification(parsed, SAMPLE_CATEGORIES)
    assert result["category_name"] == "Internal Movement"
    assert result["is_excluded_from_budget"] is True


def test_self_transfer_categorized_as_internal_movement():
    text = "Rp500.000 udah dikirim ke BCA ALFONSUS ENRICO SOEBIJAN."
    parsed = parse_notification("com.gojek.gopay", "Transfer berhasil", text, text)
    result = resolve_category_for_notification(parsed, SAMPLE_CATEGORIES)
    assert result["category_name"] == "Internal Movement"
    assert result["is_excluded_from_budget"] is True


def test_merchant_food_keyword():
    text = "Pembayaran sebesar IDR 45.000 di Kopi Kenangan berhasil."
    parsed = parse_notification("com.bca.mybca.omni.android", "Financial Diary", text, text)
    result = resolve_category_for_notification(parsed, SAMPLE_CATEGORIES)
    assert result["category_name"] == "Makanan & Minuman"
    assert result["is_excluded_from_budget"] is False
    assert result["kakeibo_type"] == "need"


def test_transport_keyword():
    text = "Pembayaran sebesar IDR 25.000 untuk GoRide berhasil."
    parsed = parse_notification("com.gojek.gopay", "GoPay", text, text)
    result = resolve_category_for_notification(parsed, SAMPLE_CATEGORIES)
    assert result["category_name"] == "Transportasi"
    assert result["is_excluded_from_budget"] is False


def test_investment_keyword():
    text = "Transfer sebesar IDR 5.000.000 ke Stockbit berhasil."
    parsed = parse_notification("com.bca.mybca.omni.android", "Financial Diary", text, text)
    result = resolve_category_for_notification(parsed, SAMPLE_CATEGORIES)
    assert result["category_name"] == "Investasi"
    assert result["is_excluded_from_budget"] is True
    assert result["kakeibo_type"] == "saving"


def test_user_learned_merchant_rule():
    custom_cat_id = uuid4()
    categories_with_custom = SAMPLE_CATEGORIES + [
        {"id": custom_cat_id, "name": "Kucing & Peliharaan", "kind": "expense", "kakeibo_type": "need", "is_excluded_from_budget": False}
    ]
    user_rules = [
        {"merchant_pattern": "petshop barokah", "category_id": custom_cat_id}
    ]
    text = "Pengeluaran sebesar IDR 120.000 di Petshop Barokah."
    parsed = parse_notification("com.bca", "BCA mobile", text, text)
    result = resolve_category_for_notification(parsed, categories_with_custom, user_rules)
    assert result["category_name"] == "Kucing & Peliharaan"
    assert result["category_id"] == custom_cat_id
