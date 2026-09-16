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

    with patch("app.routers.transactions.db_conn") as mock_conn:
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
        assert args[7] == 25000
        assert args[12] == "test_gopay_key_123"


def test_create_transfer_by_name_jago_pocket():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    jago_parent_id = str(uuid4())
    emergency_pocket_id = str(uuid4())
    internal_cat_id = str(uuid4())

    payload = {
        "account_name": "My Emergency Fund",
        "target_account_name": "Bank Jago",
        "type": "transfer",
        "amount": 500000,
        "category_name": "Internal Movement",
        "notes": "Bank Jago: My Emergency Fund → Kantong Utama",
        "idempotency_key": "jago_pocket_transfer_key_456",
    }

    with patch("app.routers.transactions.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchone.side_effect = [
            None,  # idempotency check
            {"id": str(uuid4()), "created_at": "2026-09-16T12:00:00Z"},  # insert returning
        ]
        cur.fetchall.side_effect = [
            [
                {"id": jago_parent_id, "name": "Bank Jago", "type": "bank", "parent_id": None, "default_funding_account_id": None},
                {"id": emergency_pocket_id, "name": "Dana Darurat", "type": "bank", "parent_id": jago_parent_id, "default_funding_account_id": None},
            ],
            [{"id": internal_cat_id, "name": "Internal Movement", "kind": "expense", "kakeibo_type": "saving", "is_primary": True}],
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/transactions", json=payload)

        assert res.status_code == 200
        assert res.json()["ok"] is True

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(insert_calls) == 1
        args = insert_calls[0][0][1]
        assert args[1] == emergency_pocket_id  # resolved source via synonym "emergency fund" -> "Dana Darurat"
        assert args[2] == internal_cat_id  # resolved category "Internal Movement"
        assert args[5] == "transfer"  # strictly type == transfer
        assert args[6] == jago_parent_id  # target is parent Bank Jago
        assert args[7] == 500000


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

        res = client.post("/api/transactions", json={"account_name": "Bank Jago", "type": "transfer", "amount": 1000})
        assert res.status_code == 400
        assert "Transfer requires a target account" in res.json()["detail"]
