from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.services.auth import get_current_user


def test_create_transaction_by_name():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    gopay_id = str(uuid4())
    food_cat_id = str(uuid4())

    payload = {
        "account_name": "GoPay",
        "type": "expense",
        "amount": 25000,
        "category_name": "Makanan & Minuman",
        "notes": "Kopi Kenangan",
        "idempotency_key": "test_gopay_key_123",
    }

    with patch("app.routers.transactions.db_conn") as mock_conn, \
         patch("app.routers.transactions.lock_owned_accounts", return_value={
             gopay_id: {"id": gopay_id, "type": "wallet"},
         }), \
         patch("app.routers.transactions.ensure_sufficient_funds", return_value=100000):
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        # 1. Idempotency check returns None
        # 2. Accounts fetch
        # 3. Categories fetch
        # 4. Category kakeibo fetch
        # 5. Insert transaction RETURNING
        cur.fetchone.side_effect = [
            None,  # idempotency check
            {"kakeibo_type": "need", "is_primary": True},  # category kakeibo check
            {"id": str(uuid4()), "created_at": "2026-09-16T12:00:00Z"},  # insert returning
        ]
        cur.fetchall.side_effect = [
            [{"id": gopay_id, "name": "GoPay", "type": "wallet", "parent_id": None, "default_funding_account_id": None}],
            [{"id": food_cat_id, "name": "Makanan & Minuman", "kind": "expense", "kakeibo_type": "need", "is_primary": True}],
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/transactions", json=payload)

        assert res.status_code == 200
        assert res.json()["ok"] is True

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(insert_calls) == 1
        args = insert_calls[0][0][1]
        assert args[1] == gopay_id  # resolved source account
        assert args[2] == food_cat_id  # resolved category
        assert args[5] == "expense"
        assert args[6] == 25000
        assert args[7] == "Kopi Kenangan"
        assert args[11] == "test_gopay_key_123"


def test_create_jago_pocket_movement():
    from app.main import app
    from app.services.auth import get_current_user

    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    jago_parent_id = str(uuid4())
    emergency_pocket_id = str(uuid4())
    exp_cat_id = str(uuid4())
    inc_cat_id = str(uuid4())

    payload = {
        "source_account_id": emergency_pocket_id,
        "target_account_id": jago_parent_id,
        "amount": 500000,
        "notes": "Bank Jago: My Emergency Fund → Kantong Utama",
        "idempotency_key": "jago_pocket_transfer_key_456",
    }

    locked_accounts = {
        emergency_pocket_id: {"id": emergency_pocket_id, "type": "bank", "is_savings": True},
        jago_parent_id: {"id": jago_parent_id, "type": "bank", "is_savings": False},
    }
    with patch("app.routers.movements.db_conn") as mock_conn, \
         patch("app.routers.movements.lock_owned_accounts", return_value=locked_accounts), \
         patch("app.services.ledger_mutations.ensure_sufficient_funds", return_value=1000000):
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchall.side_effect = [
            [
                {"id": emergency_pocket_id, "name": "Dana Darurat"},
                {"id": jago_parent_id, "name": "Bank Jago"},
            ],
            [
                {"id": exp_cat_id, "kind": "expense"},
                {"id": inc_cat_id, "kind": "income"},
            ],
        ]
        cur.fetchone.side_effect = [
            None,  # idempotency check
            {"id": str(uuid4()), "created_at": "2026-09-16T12:00:00Z"},  # expense insert returning
            {"id": str(uuid4()), "created_at": "2026-09-16T12:00:00Z"},  # income insert returning
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/movements", json=payload)

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["expense_transaction_id"] is not None
        assert data["income_transaction_id"] is not None

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(insert_calls) == 2
        # First call is expense (outbound)
        assert insert_calls[0][0][1][1] == emergency_pocket_id
        assert insert_calls[0][0][1][2] == exp_cat_id
        assert insert_calls[0][0][1][4] == 500000
        # Second call is income (inbound)
        assert insert_calls[1][0][1][1] == jago_parent_id
        assert insert_calls[1][0][1][2] == inc_cat_id
        assert insert_calls[1][0][1][4] == 500000


def test_create_transaction_idempotent_replay():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    existing_id = str(uuid4())
    payload = {
        "account_name": "Bank Jago",
        "type": "income",
        "amount": 100000,
        "idempotency_key": "replay_key_789",
    }

    with patch("app.routers.transactions.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        # Idempotency check returns existing transaction
        cur.fetchone.return_value = {
            "id": existing_id,
            "created_at": MagicMock(isoformat=lambda: "2026-09-16T10:00:00Z"),
            "amount": 100000,
            "type": "income",
        }

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/transactions", json=payload)

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["transaction_id"] == existing_id
        assert data["idempotent"] is True

        # Ensure NO insert was called
        insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(insert_calls) == 0


def test_create_transaction_validation_errors():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user
    client = TestClient(app, raise_server_exceptions=True)

    # 1. Invalid amount (<= 0)
    res = client.post("/api/transactions", json={"account_name": "GoPay", "type": "expense", "amount": 0})
    assert res.status_code == 422

    # 2. Invalid type
    res = client.post("/api/transactions", json={"account_name": "GoPay", "type": "invalid_type", "amount": 1000})
    assert res.status_code == 422

    # 3. Missing account (neither id nor name) and 4. Transfer missing target
    with patch("app.routers.transactions.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur
        cur.fetchone.return_value = None
        cur.fetchall.side_effect = [
            [],  # accounts fetch for test 3
            [],  # categories fetch for test 3
            [{"id": str(uuid4()), "name": "Bank Jago", "type": "bank", "parent_id": None, "default_funding_account_id": None}],  # accounts for test 4
            [],  # categories for test 4
        ]

        res = client.post("/api/transactions", json={"type": "expense", "amount": 1000})
        assert res.status_code == 422
        assert "Either account_id or account_name must be provided" in res.json()["detail"]

        # Reject 'transfer' as invalid transaction type
        res = client.post("/api/transactions", json={"account_name": "Bank Jago", "type": "transfer", "amount": 1000})
        assert res.status_code == 422


def test_create_transaction_routes_to_default_pocket():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    bca_id = str(uuid4())
    atm_id = str(uuid4())
    gold_id = str(uuid4())
    food_cat_id = str(uuid4())

    with patch("app.routers.transactions.db_conn") as mock_conn, \
         patch("app.routers.transactions.lock_owned_accounts", return_value={
             atm_id: {"id": atm_id, "type": "bank"},
         }), \
         patch("app.routers.transactions.ensure_sufficient_funds", return_value=1000000):
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchone.side_effect = [
            None,  # idempotency check
            {"kakeibo_type": "need", "is_primary": True},  # category kakeibo check
            {"id": str(uuid4()), "created_at": "2026-09-17T12:00:00Z"},  # insert returning
        ]
        cur.fetchall.side_effect = [
            [
                {"id": bca_id, "name": "BCA", "type": "bank", "parent_id": None, "default_pocket_id": atm_id},
                {"id": atm_id, "name": "ATM", "type": "bank", "parent_id": bca_id, "default_pocket_id": None},
                {"id": gold_id, "name": "Gold", "type": "bank", "parent_id": bca_id, "default_pocket_id": None},
            ],
            [{"id": food_cat_id, "name": "Makanan & Minuman", "kind": "expense", "kakeibo_type": "need", "is_primary": True}],
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post(
            "/api/transactions",
            json={
                "account_name": "BCA",
                "type": "expense",
                "amount": 50000,
                "category_name": "Makanan & Minuman",
                "notes": "Makan siang",
                "idempotency_key": "test_bca_default_pocket_123",
            },
        )
        assert res.status_code == 200
        assert res.json()["ok"] is True

        # Verify that the inserted transaction account_id is ATM (not BCA!)
        inserts = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(inserts) == 1
        inserted_args = inserts[0][0][1]
        assert inserted_args[1] == atm_id


def test_list_transactions_summary_cycle_and_cumulative():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    with patch("app.routers.transactions.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        # 1. Summary aggregate fetch
        cur.fetchone.return_value = {
            "total": 15,
            "cumulative_inflow": 12000000,
            "cumulative_outflow": 5500000,
            "cycle_inflow": 7436000,
            "cycle_outflow": 1250000,
        }
        # 2. Select page fetch & allocations
        cur.fetchall.side_effect = [
            [],  # select page rows
            [],  # load allocation breakdowns
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.get("/api/transactions?limit=25&offset=0")

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["total"] == 15
        assert "summary" in data

        summary = data["summary"]
        assert summary["cumulative"]["inflow"] == 12000000
        assert summary["cumulative"]["outflow"] == 5500000
        assert summary["cumulative"]["net"] == 6500000

        assert summary["cycle"]["inflow"] == 7436000
        assert summary["cycle"]["outflow"] == 1250000
        assert summary["cycle"]["net"] == 6186000
        assert "start" in summary["cycle"]
        assert "end" in summary["cycle"]

        # Verify SQL query calculates aggregate summaries while excluding Internal Movement
        executed_queries = [str(c[0][0]) for c in cur.execute.call_args_list]
        summary_query = next(q for q in executed_queries if "cumulative_inflow" in q)
        assert "cumulative_outflow" in summary_query
        assert "cycle_inflow" in summary_query
        assert "cycle_outflow" in summary_query
        assert "Internal Movement" in summary_query
        assert "is_excluded_from_budget" in summary_query


def test_list_transactions_summary_with_filters_and_custom_payday():
    mock_user = {
        "id": "22222222-2222-2222-2222-222222222222",
        "username": "payday_user",
        "currency": "IDR",
        "payday_day": 1,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    filter_account_id = str(uuid4())
    filter_category_id = str(uuid4())

    with patch("app.routers.transactions.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchone.return_value = {
            "total": 3,
            "cumulative_inflow": 2000000,
            "cumulative_outflow": 500000,
            "cycle_inflow": 1000000,
            "cycle_outflow": 250000,
        }
        cur.fetchall.side_effect = [
            [{"id": filter_account_id}],  # accounts query for matching account IDs
            [],  # select page rows
            [],  # load allocation breakdowns
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.get(f"/api/transactions?account_id={filter_account_id}&category_id={filter_category_id}&q=warung")

        assert res.status_code == 200
        data = res.json()
        assert data["total"] == 3
        summary = data["summary"]
        assert summary["cumulative"]["inflow"] == 2000000
        assert summary["cumulative"]["outflow"] == 500000
        assert summary["cumulative"]["net"] == 1500000
        assert summary["cycle"]["inflow"] == 1000000
        assert summary["cycle"]["outflow"] == 250000
        assert summary["cycle"]["net"] == 750000

        # Verify SQL query preserved account, category, and q filters
        executed_queries = [str(c[0][0]) for c in cur.execute.call_args_list]
        summary_query = next(q for q in executed_queries if "cumulative_inflow" in q)
        assert "t.account_id IN" in summary_query
        assert "t.category_id = %s" in summary_query
        assert "t.notes ILIKE" in summary_query
