from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.services.auth import get_current_user


def test_post_movement_creates_two_transactions():
    user_id = str(uuid4())
    mock_user = {
        "id": user_id,
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    src_id = str(uuid4())
    dst_id = str(uuid4())
    exp_cat_id = str(uuid4())
    inc_cat_id = str(uuid4())
    exp_tx_id = str(uuid4())
    inc_tx_id = str(uuid4())

    with patch("app.routers.movements.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        # 1. Accounts check fetchall
        # 2. Categories check fetchall
        cur.fetchall.side_effect = [
            [{"id": src_id, "name": "BCA"}, {"id": dst_id, "name": "Jago"}],
            [{"id": exp_cat_id, "kind": "expense"}, {"id": inc_cat_id, "kind": "income"}],
        ]
        # 1. insert expense RETURNING id
        # 2. insert income RETURNING id
        cur.fetchone.side_effect = [
            {"id": exp_tx_id},
            {"id": inc_tx_id},
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post(
            "/api/movements",
            json={
                "source_account_id": src_id,
                "target_account_id": dst_id,
                "amount": 250000,
                "notes": "Pindah ke Jago",
            },
        )
        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["expense_transaction_id"] == exp_tx_id
        assert data["income_transaction_id"] == inc_tx_id

        # Verify 2 INSERT INTO transactions were executed (expense first, income second)
        inserts = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(inserts) == 2
        assert "'expense'" in inserts[0][0][0]
        exp_args = inserts[0][0][1]
        assert exp_args[1] == src_id
        assert exp_args[2] == exp_cat_id
        assert exp_args[3] == 250000

        assert "'income'" in inserts[1][0][0]
        inc_args = inserts[1][0][1]
        assert inc_args[1] == dst_id
        assert inc_args[2] == inc_cat_id
        assert inc_args[3] == 250000

    app.dependency_overrides.clear()


def test_post_movement_same_account_rejected():
    user_id = str(uuid4())
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id,
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }

    same_id = str(uuid4())
    client = TestClient(app, raise_server_exceptions=True)
    res = client.post(
        "/api/movements",
        json={
            "source_account_id": same_id,
            "target_account_id": same_id,
            "amount": 100000,
        },
    )
    assert res.status_code == 400
    assert "must be different" in res.json()["detail"].lower()
    app.dependency_overrides.clear()


def test_default_pocket_routing():
    user_id = str(uuid4())
    mock_user = {
        "id": user_id,
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    parent_id = str(uuid4())
    default_pocket_id = str(uuid4())
    cat_id = str(uuid4())

    payload = {
        "account_name": "Bank Jago",
        "type": "expense",
        "amount": 50000,
        "category_name": "Makanan",
        "idempotency_key": "test_pocket_route_key_999",
    }

    with patch("app.routers.transactions.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchone.side_effect = [
            None,  # idempotency check
            {"kakeibo_type": "need", "is_primary": True},  # category kakeibo
            {"id": str(uuid4()), "created_at": datetime.now(timezone.utc)},  # tx insert RETURNING
        ]
        cur.fetchall.side_effect = [
            [
                {
                    "id": parent_id,
                    "name": "Bank Jago",
                    "type": "bank",
                    "parent_id": None,
                    "default_funding_account_id": None,
                    "default_pocket_id": default_pocket_id,
                },
                {
                    "id": default_pocket_id,
                    "name": "Kantong Utama",
                    "type": "bank",
                    "parent_id": parent_id,
                    "default_funding_account_id": None,
                    "default_pocket_id": None,
                },
            ],
            [
                {"id": cat_id, "name": "Makanan", "kind": "expense", "kakeibo_type": "need", "is_primary": True},
            ],
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/transactions", json=payload)
        assert res.status_code == 200
        assert res.json()["ok"] is True

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(insert_calls) == 1
        args = insert_calls[0][0][1]
        # Verify account_id was routed to default_pocket_id, NOT parent_id!
        assert args[1] == default_pocket_id

    app.dependency_overrides.clear()


def test_post_movement_excluded_from_pulse():
    """Verify that transactions under category 'Internal Movement' are excluded from pulse metrics."""
    user_id = str(uuid4())
    mock_user = {
        "id": user_id,
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
        "monthly_spending_budget": 5000000,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    with patch("app.routers.pulse.db_conn") as mock_conn, \
         patch("app.routers.pulse.get_accounts_with_balances", return_value=[]):
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchone.side_effect = [
            {"today_spent": 150000},       # today spent query
            {"cycle_spent": 800000, "cycle_income": 5000000},  # cycle query
            {"total_budget": 5000000},    # category budgets
            {"last_cycle_spent": 750000}, # previous cycle spent
        ]
        cur.fetchall.side_effect = [
            [],  # category breakdown
            [],  # daily spent histogram
            [],  # recent feed
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.get("/api/pulse")
        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["today_spent"] == 150000

        # Verify SQL queries executed contain the exclusion filter
        executed_sqls = [c[0][0] for c in cur.execute.call_args_list]
        today_query = next(q for q in executed_sqls if "today_spent" in q)
        assert "NOT IN ('Internal Movement', 'Investasi')" in today_query

        cycle_query = next(q for q in executed_sqls if "cycle_spent" in q)
        assert "NOT IN ('Internal Movement', 'Investasi')" in cycle_query

    app.dependency_overrides.clear()


def test_bearer_api_key_accounts_and_categories():
    """Verify that GET /api/accounts and GET /api/categories work with Bearer API key."""
    from app.services.auth import hash_token
    user_id = str(uuid4())
    plain_token = "cfk_test_secret_token_1234567890abcdef"
    token_hash = hash_token(plain_token)

    with patch("app.services.auth.db_conn") as mock_auth_conn, \
         patch("app.routers.accounts.db_conn") as mock_acc_conn, \
         patch("app.routers.categories.db_conn") as mock_cat_conn:

        auth_cur = MagicMock()
        mock_auth_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = auth_cur
        auth_cur.fetchone.return_value = {
            "id": user_id,
            "username": "tester",
            "name": "Tester",
            "payday_day": 25,
            "currency": "IDR",
            "emergency_fund_multiplier": 6,
            "monthly_spending_budget": None,
            "key_id": str(uuid4()),
        }

        acc_cur = MagicMock()
        mock_acc_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = acc_cur
        acc_cur.fetchall.return_value = [
            {
                "id": str(uuid4()),
                "parent_id": None,
                "name": "Bank BCA",
                "type": "bank",
                "initial_balance": 1000000,
                "instrument_type": None,
                "instrument_symbol": None,
                "units": None,
                "avg_buy_price": None,
                "last_price": None,
                "last_price_at": None,
                "color": "#2563eb",
                "display_order": 0,
                "default_pocket_id": None,
                "is_archived": False,
                "created_at": None,
                "ledger_balance": 1000000,
            }
        ]

        cat_cur = MagicMock()
        mock_cat_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cat_cur
        cat_cur.fetchall.return_value = [
            {
                "id": str(uuid4()),
                "name": "Makanan",
                "icon": "utensils",
                "color": "#ef4444",
                "kind": "expense",
                "monthly_budget": 1000000,
                "is_primary": True,
                "kakeibo_type": "need",
                "is_archived": False,
                "created_at": None,
            }
        ]

        client = TestClient(app, raise_server_exceptions=True)
        headers = {"Authorization": f"Bearer {plain_token}"}

        res_acc = client.get("/api/accounts", headers=headers)
        assert res_acc.status_code == 200
        assert res_acc.json()["ok"] is True
        assert len(res_acc.json()["accounts"]) == 1

        res_cat = client.get("/api/categories", headers=headers)
        assert res_cat.status_code == 200
        cat_data = res_cat.json()
        assert len(cat_data["categories"]) == 1
        assert cat_data["categories"][0]["kind"] == "expense"


def test_post_movement_auto_resolves_parent_to_default_pocket():
    user_id = str(uuid4())
    mock_user = {
        "id": user_id,
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    bca_id = str(uuid4())
    atm_id = str(uuid4())
    gold_id = str(uuid4())
    gopay_id = str(uuid4())
    exp_cat_id = str(uuid4())
    inc_cat_id = str(uuid4())
    exp_tx_id = str(uuid4())
    inc_tx_id = str(uuid4())

    with patch("app.routers.movements.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchall.side_effect = [
            [
                {"id": bca_id, "name": "BCA", "type": "bank", "parent_id": None, "default_pocket_id": atm_id},
                {"id": atm_id, "name": "ATM", "type": "bank", "parent_id": bca_id, "default_pocket_id": None},
                {"id": gold_id, "name": "Gold", "type": "bank", "parent_id": bca_id, "default_pocket_id": None},
                {"id": gopay_id, "name": "GoPay", "type": "wallet", "parent_id": None, "default_pocket_id": None},
            ],
            [{"id": exp_cat_id, "kind": "expense"}, {"id": inc_cat_id, "kind": "income"}],
        ]
        cur.fetchone.side_effect = [
            {"id": exp_tx_id},
            {"id": inc_tx_id},
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post(
            "/api/movements",
            json={
                "source_account_name": "BCA",
                "target_account_name": "GoPay",
                "amount": 100000,
                "notes": "BCA ke GoPay",
            },
        )
        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["expense_transaction_id"] == exp_tx_id
        assert data["income_transaction_id"] == inc_tx_id

        # Verify expense transaction has account_id = ATM (not BCA!)
        inserts = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(inserts) == 2
        exp_args = inserts[0][0][1]
        assert exp_args[1] == atm_id
        inc_args = inserts[1][0][1]
        assert inc_args[1] == gopay_id
