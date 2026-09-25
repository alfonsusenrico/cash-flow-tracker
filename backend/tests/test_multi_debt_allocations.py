from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest


def create_account(client: TestClient, balance: int = 4_000_000) -> str:
    response = client.post(
        "/api/accounts",
        json={
            "name": f"Debt funding {uuid4().hex[:10]}",
            "type": "bank",
            "initial_balance": balance,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["account"]["id"]


def create_debt(client: TestClient, remaining: int = 1_000_000) -> str:
    response = client.post(
        "/api/obligations",
        json={
            "name": f"Debt {uuid4().hex[:10]}",
            "total_amount": remaining,
            "remaining_amount": remaining,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["obligation"]["id"]


def get_debt(client: TestClient, debt_id: str) -> dict:
    response = client.get("/api/obligations?include_archived=true")
    assert response.status_code == 200, response.text
    return next(debt for debt in response.json()["obligations"] if debt["id"] == debt_id)


def get_account_balance(client: TestClient, account_id: str) -> int:
    response = client.get("/api/accounts")
    assert response.status_code == 200, response.text
    return next(account["balance"] for account in response.json()["accounts"] if account["id"] == account_id)


def test_allocated_payment_edit_delete_and_idempotent_retry(auth_client):
    account_id = create_account(auth_client)
    first_id = create_debt(auth_client)
    second_id = create_debt(auth_client)
    payload = {
        "account_id": account_id,
        "type": "expense",
        "amount": 1_500_000,
        "idempotency_key": f"debt-split-{uuid4().hex}",
        "obligation_allocations": [
            {"obligation_id": first_id, "amount": 1_000_000},
            {"obligation_id": second_id, "amount": 500_000},
        ],
    }

    created = auth_client.post("/api/transactions", json=payload)
    assert created.status_code == 200, created.text
    transaction_id = created.json()["transaction_id"]
    assert get_account_balance(auth_client, account_id) == 2_500_000
    assert get_debt(auth_client, first_id)["remaining_amount"] == 0
    assert get_debt(auth_client, first_id)["is_archived"] is True
    assert get_debt(auth_client, second_id)["remaining_amount"] == 500_000

    repeated = auth_client.post("/api/transactions", json=payload)
    assert repeated.status_code == 200, repeated.text
    assert repeated.json()["transaction_id"] == transaction_id
    assert repeated.json()["idempotent"] is True
    assert get_account_balance(auth_client, account_id) == 2_500_000

    detail = auth_client.get(f"/api/transactions/{transaction_id}")
    assert detail.status_code == 200, detail.text
    assert {item["obligation_id"]: item["amount"] for item in detail.json()["transaction"]["obligation_allocations"]} == {
        first_id: 1_000_000,
        second_id: 500_000,
    }
    filtered = auth_client.get(f"/api/transactions?obligation_id={first_id}")
    assert filtered.status_code == 200, filtered.text
    assert sum(item["id"] == transaction_id for item in filtered.json()["transactions"]) == 1

    edited = auth_client.patch(
        f"/api/transactions/{transaction_id}",
        json={
            "amount": 1_200_000,
            "obligation_allocations": [
                {"obligation_id": first_id, "amount": 700_000},
                {"obligation_id": second_id, "amount": 500_000},
            ],
        },
    )
    assert edited.status_code == 200, edited.text
    assert get_debt(auth_client, first_id)["remaining_amount"] == 300_000
    assert get_debt(auth_client, first_id)["is_archived"] is False
    assert get_debt(auth_client, second_id)["remaining_amount"] == 500_000
    assert get_account_balance(auth_client, account_id) == 2_800_000

    deleted = auth_client.delete(f"/api/transactions/{transaction_id}")
    assert deleted.status_code == 200, deleted.text
    assert get_debt(auth_client, first_id)["remaining_amount"] == 1_000_000
    assert get_debt(auth_client, second_id)["remaining_amount"] == 1_000_000
    assert get_account_balance(auth_client, account_id) == 4_000_000


def test_invalid_split_preserves_cash_and_debts(auth_client):
    account_id = create_account(auth_client)
    first_id = create_debt(auth_client)
    second_id = create_debt(auth_client)
    base = {"account_id": account_id, "type": "expense", "amount": 1_000_000}
    invalid_allocations = [
        [{"obligation_id": first_id, "amount": 400_000}],
        [{"obligation_id": first_id, "amount": 500_000}, {"obligation_id": first_id, "amount": 500_000}],
        [{"obligation_id": first_id, "amount": 1_000_001}],
        [{"obligation_id": str(uuid4()), "amount": 1_000_000}],
    ]
    for allocations in invalid_allocations:
        response = auth_client.post("/api/transactions", json={**base, "obligation_allocations": allocations})
        assert response.status_code in {404, 409, 422}, response.text

    mixed = auth_client.post(
        "/api/transactions",
        json={**base, "obligation_id": second_id, "obligation_allocations": [{"obligation_id": first_id, "amount": 1_000_000}]},
    )
    assert mixed.status_code == 422, mixed.text
    wrong_type = auth_client.post(
        "/api/transactions",
        json={**base, "type": "income", "obligation_allocations": [{"obligation_id": first_id, "amount": 1_000_000}]},
    )
    assert wrong_type.status_code == 422, wrong_type.text
    archive = auth_client.delete(f"/api/obligations/{second_id}")
    assert archive.status_code == 200, archive.text
    archived = auth_client.post(
        "/api/transactions",
        json={**base, "obligation_allocations": [{"obligation_id": second_id, "amount": 1_000_000}]},
    )
    assert archived.status_code == 409, archived.text
    assert get_account_balance(auth_client, account_id) == 4_000_000
    assert get_debt(auth_client, first_id)["remaining_amount"] == 1_000_000
    assert get_debt(auth_client, second_id)["remaining_amount"] == 1_000_000


def test_allocated_debt_cannot_be_hard_deleted(auth_client):
    account_id = create_account(auth_client)
    debt_id = create_debt(auth_client)
    created = auth_client.post(
        "/api/transactions",
        json={
            "account_id": account_id,
            "type": "expense",
            "amount": 200_000,
            "obligation_allocations": [{"obligation_id": debt_id, "amount": 200_000}],
        },
    )
    assert created.status_code == 200, created.text
    blocked = auth_client.delete(f"/api/obligations/{debt_id}?hard_delete=true")
    assert blocked.status_code == 409, blocked.text
    archived = auth_client.delete(f"/api/obligations/{debt_id}")
    assert archived.status_code == 200, archived.text


def test_concurrent_allocations_cannot_overpay_one_debt(auth_client):
    from app.main import app

    debt_id = create_debt(auth_client)
    accounts = [create_account(auth_client) for _ in range(2)]
    clients = [TestClient(app, base_url="https://testserver") for _ in range(2)]
    for client in clients:
        client.headers["Origin"] = "https://testserver"
        client.cookies.update(auth_client.cookies)

    def pay(pair):
        client, account_id = pair
        return client.post(
            "/api/transactions",
            json={
                "account_id": account_id,
                "type": "expense",
                "amount": 700_000,
                "obligation_allocations": [{"obligation_id": debt_id, "amount": 700_000}],
            },
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(pay, zip(clients, accounts)))

    assert sorted(response.status_code for response in responses) == [200, 409]
    assert get_debt(auth_client, debt_id)["remaining_amount"] == 300_000
    assert sorted(get_account_balance(auth_client, account_id) for account_id in accounts) == [3_300_000, 4_000_000]


def test_failed_edit_keeps_original_split_and_legacy_transition(auth_client):
    account_id = create_account(auth_client)
    first_id = create_debt(auth_client)
    second_id = create_debt(auth_client)
    created = auth_client.post(
        "/api/transactions",
        json={
            "account_id": account_id,
            "type": "expense",
            "amount": 1_000_000,
            "obligation_allocations": [
                {"obligation_id": first_id, "amount": 500_000},
                {"obligation_id": second_id, "amount": 500_000},
            ],
        },
    )
    assert created.status_code == 200, created.text
    transaction_id = created.json()["transaction_id"]
    failed = auth_client.patch(
        f"/api/transactions/{transaction_id}",
        json={
            "amount": 1_500_000,
            "obligation_allocations": [
                {"obligation_id": first_id, "amount": 1_000_001},
                {"obligation_id": second_id, "amount": 499_999},
            ],
        },
    )
    assert failed.status_code == 409, failed.text
    assert get_debt(auth_client, first_id)["remaining_amount"] == 500_000
    assert get_debt(auth_client, second_id)["remaining_amount"] == 500_000
    assert get_account_balance(auth_client, account_id) == 3_000_000

    legacy = auth_client.patch(
        f"/api/transactions/{transaction_id}",
        json={"amount": 500_000, "obligation_id": first_id},
    )
    assert legacy.status_code == 200, legacy.text
    detail = auth_client.get(f"/api/transactions/{transaction_id}").json()["transaction"]
    assert detail["obligation_id"] == first_id
    assert detail["obligation_allocations"] == [
        {"obligation_id": first_id, "obligation_name": get_debt(auth_client, first_id)["name"], "amount": 500_000}
    ]
    assert get_debt(auth_client, first_id)["remaining_amount"] == 500_000
    assert get_debt(auth_client, second_id)["remaining_amount"] == 1_000_000


def test_legacy_payment_can_be_replaced_by_allocations(auth_client):
    account_id = create_account(auth_client)
    first_id = create_debt(auth_client)
    second_id = create_debt(auth_client)
    created = auth_client.post(
        "/api/transactions",
        json={"account_id": account_id, "type": "expense", "amount": 1_000_000, "obligation_id": first_id},
    )
    assert created.status_code == 200, created.text
    assert get_debt(auth_client, first_id)["is_archived"] is True
    transaction_id = created.json()["transaction_id"]
    edited = auth_client.patch(
        f"/api/transactions/{transaction_id}",
        json={
            "obligation_id": None,
            "obligation_allocations": [
                {"obligation_id": first_id, "amount": 500_000},
                {"obligation_id": second_id, "amount": 500_000},
            ],
        },
    )
    assert edited.status_code == 200, edited.text
    assert get_debt(auth_client, first_id)["remaining_amount"] == 500_000
    assert get_debt(auth_client, first_id)["is_archived"] is False
    assert get_debt(auth_client, second_id)["remaining_amount"] == 500_000


def test_late_failure_rolls_back_cash_and_every_debt(auth_client):
    import app.routers.transactions as transaction_router

    account_id = create_account(auth_client)
    first_id = create_debt(auth_client)
    second_id = create_debt(auth_client)
    original_apply = transaction_router.apply_allocations

    def fail_after_first_debt(cur, user_id, allocations):
        original_apply(cur, user_id, allocations[:1])
        raise RuntimeError("simulated late settlement failure")

    with patch.object(transaction_router, "apply_allocations", side_effect=fail_after_first_debt):
        with pytest.raises(RuntimeError, match="simulated late settlement failure"):
            auth_client.post(
                "/api/transactions",
                json={
                    "account_id": account_id,
                    "type": "expense",
                    "amount": 1_000_000,
                    "obligation_allocations": [
                        {"obligation_id": first_id, "amount": 500_000},
                        {"obligation_id": second_id, "amount": 500_000},
                    ],
                },
            )

    assert get_account_balance(auth_client, account_id) == 4_000_000
    assert get_debt(auth_client, first_id)["remaining_amount"] == 1_000_000
    assert get_debt(auth_client, second_id)["remaining_amount"] == 1_000_000


def test_allocated_payment_can_change_funding_account(auth_client):
    source_id = create_account(auth_client)
    target_id = create_account(auth_client)
    first_id = create_debt(auth_client)
    second_id = create_debt(auth_client)
    created = auth_client.post(
        "/api/transactions",
        json={
            "account_id": source_id,
            "type": "expense",
            "amount": 800_000,
            "obligation_allocations": [
                {"obligation_id": first_id, "amount": 300_000},
                {"obligation_id": second_id, "amount": 500_000},
            ],
        },
    )
    assert created.status_code == 200, created.text
    edited = auth_client.patch(
        f"/api/transactions/{created.json()['transaction_id']}",
        json={"account_id": target_id},
    )
    assert edited.status_code == 200, edited.text
    assert get_account_balance(auth_client, source_id) == 4_000_000
    assert get_account_balance(auth_client, target_id) == 3_200_000
    assert get_debt(auth_client, first_id)["remaining_amount"] == 700_000
    assert get_debt(auth_client, second_id)["remaining_amount"] == 500_000
