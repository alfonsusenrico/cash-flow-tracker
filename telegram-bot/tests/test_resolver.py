from bot.resolver import resolve

ACCOUNTS = [
    {"account_id": "a-bca", "account_name": "ATM BCA", "profile_type": "dynamic_spending", "balance": 100000},
    {"account_id": "a-cash", "account_name": "Cash", "profile_type": "dynamic_spending", "balance": 50000},
    {"account_id": "a-bibit", "account_name": "Dana Darurat (Bibit)", "profile_type": "tabungan", "balance": 0},
]
CATEGORIES = [
    {"category_id": "c-food", "name": "Food & Drink", "kind": "expense"},
    {"category_id": "c-salary", "name": "Salary", "kind": "income"},
]
TH = 0.75


def test_complete_confident_create_executes():
    p = {"intent": "create_transaction", "transaction_type": "credit", "amount": 15000,
         "transaction_name": "Beli minum", "account_name": "BCA", "category_name": "Food & Drink",
         "confidence": 0.9}
    r = resolve(p, ACCOUNTS, CATEGORIES, TH)
    assert r["decision"] == "execute"
    assert r["fields"]["account_id"] == "a-bca"
    assert r["fields"]["category_id"] == "c-food"


def test_missing_amount_asks():
    p = {"intent": "create_transaction", "transaction_type": "credit", "transaction_name": "Beli minum",
         "account_name": "ATM BCA", "confidence": 0.55, "missing_fields": ["amount"]}
    r = resolve(p, ACCOUNTS, CATEGORIES, TH)
    assert r["decision"] == "ask"
    assert any(q["field"] == "amount" for q in r["questions"])


def test_low_confidence_complete_confirms():
    p = {"intent": "create_transaction", "transaction_type": "credit", "amount": 150000,
         "transaction_name": "Transfer ke orang", "account_name": "ATM BCA", "category_name": "Food & Drink",
         "confidence": 0.6}
    r = resolve(p, ACCOUNTS, CATEGORIES, TH)
    assert r["decision"] == "confirm"


def test_delete_always_confirms_even_high_confidence():
    p = {"intent": "delete_transaction", "query": "minum tadi", "confidence": 0.99}
    r = resolve(p, ACCOUNTS, CATEGORIES, TH)
    assert r["decision"] == "confirm"


def test_movement_resolves_both_accounts():
    p = {"intent": "create_movement", "amount": 500000, "account_name": "BCA",
         "target_account_name": "Cash", "confidence": 0.88}
    r = resolve(p, ACCOUNTS, CATEGORIES, TH)
    assert r["decision"] == "execute"
    assert r["fields"]["account_id"] == "a-bca"
    assert r["fields"]["target_account_id"] == "a-cash"


def test_unknown_account_asks():
    p = {"intent": "create_transaction", "transaction_type": "credit", "amount": 150000,
         "transaction_name": "Transfer", "account_name": None, "confidence": 0.3,
         "missing_fields": ["account_name"]}
    r = resolve(p, ACCOUNTS, CATEGORIES, TH)
    assert r["decision"] == "ask"
    assert any(q["field"] == "account" for q in r["questions"])


def test_query_intent_passthrough():
    p = {"intent": "query", "query": "pengeluaran makan minggu ini", "confidence": 0.8}
    r = resolve(p, ACCOUNTS, CATEGORIES, TH)
    assert r["decision"] == "query"
