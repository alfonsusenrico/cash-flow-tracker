"""Edits on a liquid account that a settled notification already took below zero.

Notifications and confirmed AI captures may record a real debit past the tracked balance.
An edit that takes no more money from that account must still save; one that takes more
is refused like a new expense would be.
"""

from uuid import uuid4

from fastapi.testclient import TestClient


def _create_account(client: TestClient, name: str, balance: int) -> str:
    response = client.post("/api/accounts", json={"name": name, "type": "bank", "initial_balance": balance})
    assert response.status_code == 200, response.text
    return response.json()["account"]["id"]


def _settled_expense(account_id: str, amount: int) -> str:
    """An expense recorded past the balance, as a settled notification does."""
    from app.db.pool import db_conn

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO transactions (user_id, account_id, type, amount, notes, date, source)
                   SELECT user_id, id, 'expense', %s, 'Top up saldo', NOW(), 'notification'
                   FROM accounts WHERE id = %s RETURNING id""",
                (amount, account_id),
            )
            transaction_id = str(cursor.fetchone()["id"])
        connection.commit()
    return transaction_id


def _below_zero_account(client: TestClient) -> tuple[str, str]:
    account_id = _create_account(client, f"Main pocket {uuid4().hex[:8]}", 0)
    return account_id, _settled_expense(account_id, 189_667)


def _balance(account_id: str) -> int:
    from app.services.ledger_mutations import get_locked_ledger_balance
    from app.db.pool import db_conn

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT user_id FROM accounts WHERE id = %s", (account_id,))
            user_id = str(cursor.fetchone()["user_id"])
            return get_locked_ledger_balance(cursor, user_id, account_id)


def test_editing_pillar_and_notes_saves_on_a_below_zero_account(auth_client):
    account_id, transaction_id = _below_zero_account(auth_client)

    response = auth_client.patch(
        f"/api/transactions/{transaction_id}",
        json={"amount": 189_667, "account_id": account_id, "kakeibo_type": "want", "notes": "Top up saldo untuk development"},
    )

    assert response.status_code == 200, response.text
    assert _balance(account_id) == -189_667


def test_lowering_the_amount_saves_on_a_below_zero_account(auth_client):
    account_id, transaction_id = _below_zero_account(auth_client)

    response = auth_client.patch(f"/api/transactions/{transaction_id}", json={"amount": 150_000})

    assert response.status_code == 200, response.text
    assert _balance(account_id) == -150_000


def test_raising_the_amount_is_refused_on_a_below_zero_account(auth_client):
    account_id, transaction_id = _below_zero_account(auth_client)

    response = auth_client.patch(f"/api/transactions/{transaction_id}", json={"amount": 189_668})

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "insufficient_funds"
    assert detail["required_amount"] == 189_668
    assert detail["available_amount"] == 0
    assert _balance(account_id) == -189_667


def test_moving_the_expense_to_an_unfunded_account_is_refused(auth_client):
    _, transaction_id = _below_zero_account(auth_client)
    other_id = _create_account(auth_client, f"Other pocket {uuid4().hex[:8]}", 1_000)

    response = auth_client.patch(f"/api/transactions/{transaction_id}", json={"account_id": other_id})

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "insufficient_funds"
    assert _balance(other_id) == 1_000


def test_renaming_a_movement_saves_when_its_source_is_below_zero(auth_client):
    source_id = _create_account(auth_client, f"Movement source {uuid4().hex[:8]}", 10_000)
    target_id = _create_account(auth_client, f"Movement target {uuid4().hex[:8]}", 0)
    moved = auth_client.post(
        "/api/movements", json={"source_account_id": source_id, "target_account_id": target_id, "amount": 10_000}
    )
    assert moved.status_code == 200, moved.text
    movement_id = moved.json()["movement_id"]
    _settled_expense(source_id, 5_000)

    renamed = auth_client.patch(
        f"/api/movements/{movement_id}",
        json={"source_account_id": source_id, "target_account_id": target_id, "amount": 10_000, "notes": "Isi dompet"},
    )
    assert renamed.status_code == 200, renamed.text

    lowered = auth_client.patch(
        f"/api/movements/{movement_id}",
        json={"source_account_id": source_id, "target_account_id": target_id, "amount": 8_000},
    )
    assert lowered.status_code == 200, lowered.text
    assert _balance(source_id) == -3_000

    raised = auth_client.patch(
        f"/api/movements/{movement_id}",
        json={"source_account_id": source_id, "target_account_id": target_id, "amount": 8_001},
    )
    assert raised.status_code == 409
    assert raised.json()["detail"]["code"] == "insufficient_funds"
    assert _balance(source_id) == -3_000
