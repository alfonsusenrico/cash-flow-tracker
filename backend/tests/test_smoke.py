"""Smoke tests: one happy-path request per resource endpoint."""
import pytest
from fastapi.testclient import TestClient


def test_health(client: TestClient):
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["ok"] is True


def test_me(auth_client: TestClient):
    res = auth_client.get("/me")
    assert res.status_code == 200
    assert res.json()["username"] == "testuser"


class TestAccounts:
    def test_list(self, auth_client: TestClient):
        res = auth_client.get("/accounts")
        assert res.status_code == 200
        assert "accounts" in res.json()

    def test_create_and_delete(self, auth_client: TestClient):
        res = auth_client.post("/accounts", json={"account_name": "Smoke Test Account"})
        assert res.status_code == 200
        account_id = res.json()["account_id"]

        res = auth_client.delete(f"/accounts/{account_id}")
        assert res.status_code == 200


class TestTransactions:
    def test_create_and_delete(self, auth_client: TestClient):
        # Create account first
        acc = auth_client.post("/accounts", json={"account_name": "TX Test Account"}).json()
        account_id = acc["account_id"]

        # Create transaction
        res = auth_client.post(
            "/transactions",
            json={
                "account_id": account_id,
                "transaction_type": "debit",
                "transaction_name": "Test income",
                "amount": 100000,
            },
        )
        assert res.status_code == 200
        tx_id = res.json()["transaction_id"]

        # Delete transaction
        res = auth_client.delete(f"/transactions/{tx_id}")
        assert res.status_code == 200

        # Cleanup
        auth_client.delete(f"/accounts/{account_id}")


class TestSummary:
    def test_summary(self, auth_client: TestClient):
        res = auth_client.get("/summary")
        assert res.status_code == 200
        body = res.json()
        assert "total_asset" in body
        assert "accounts" in body
        assert "range" in body


class TestLedger:
    def test_ledger(self, auth_client: TestClient):
        res = auth_client.get("/ledger")
        assert res.status_code == 200
        body = res.json()
        assert "rows" in body


class TestCategories:
    def test_list(self, auth_client: TestClient):
        res = auth_client.get("/categories")
        assert res.status_code == 200
        assert "categories" in res.json()

    def test_create_update_delete(self, auth_client: TestClient):
        res = auth_client.post("/categories", json={"name": "Smoke Category", "kind": "expense"})
        assert res.status_code == 200
        cat_id = res.json()["category_id"]

        res = auth_client.put(f"/categories/{cat_id}", json={"name": "Smoke Category Updated", "kind": "expense"})
        assert res.status_code == 200

        res = auth_client.delete(f"/categories/{cat_id}")
        assert res.status_code == 200


class TestPeriods:
    def test_list(self, auth_client: TestClient):
        res = auth_client.get("/periods")
        assert res.status_code == 200
        assert "periods" in res.json()


class TestPublicApi:
    def test_accounts_list(self, client: TestClient, api_key: str):
        res = client.post(
            "/v1/accounts/list",
            json={},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert res.status_code == 200
        assert "accounts" in res.json()

    def test_summary(self, client: TestClient, api_key: str):
        res = client.post(
            "/v1/summary",
            json={},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert res.status_code == 200
        assert "total_asset" in res.json()

    def test_categories(self, client: TestClient, api_key: str):
        res = client.get(
            "/v1/categories",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert res.status_code == 200
        assert "categories" in res.json()

    def test_periods(self, client: TestClient, api_key: str):
        res = client.get(
            "/v1/periods",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        assert res.status_code == 200
        assert "periods" in res.json()
