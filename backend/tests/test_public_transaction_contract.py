"""The public `POST /api/transactions` answers that clients rely on. All names and amounts are synthetic."""
from uuid import uuid4

from test_reimbursements import owner  # noqa: F401 - fixture


def rows_for_key(owner, key):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(
            "SELECT id FROM transactions WHERE user_id = %s AND idempotency_key = %s", (owner["user_id"], key),
        ).fetchall()


def transaction_count(owner):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute("SELECT COUNT(*) AS n FROM transactions WHERE user_id = %s", (owner["user_id"],)).fetchone()["n"]


def test_new_and_replayed_transactions_answer_200_once(client, owner):
    key = f"synthetic-{uuid4().hex}"
    body = {"type": "expense", "account_id": owner["cash"], "category_id": owner["groceries"], "amount": 45_000,
            "idempotency_key": key}
    created = client.post("/api/transactions", json=body)
    assert created.status_code == 200, created.text
    assert created.json()["ok"] is True
    replayed = client.post("/api/transactions", json=body)
    assert replayed.status_code == 200, replayed.text
    answer = replayed.json()
    assert (answer["transaction_id"], answer["amount"], answer["type"], answer["idempotent"]) == (
        created.json()["transaction_id"], 45_000, "expense", True)
    assert len(rows_for_key(owner, key)) == 1


def test_transfers_are_not_transactions(client, owner):
    from app.db.pool import db_conn

    with db_conn() as conn:
        bank = str(conn.execute(
            "INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Bank Contoh', 'bank', 0) RETURNING id",
            (owner["user_id"],),
        ).fetchone()["id"])
        conn.commit()
    transfer_type = client.post("/api/transactions", json={"type": "transfer", "account_id": owner["cash"], "amount": 10_000})
    assert transfer_type.status_code == 422
    targeted = client.post("/api/transactions", json={
        "type": "expense", "account_id": owner["cash"], "target_account_id": bank, "amount": 10_000,
    })
    assert targeted.status_code == 400, targeted.text
    zero = client.post("/api/transactions", json={"type": "expense", "account_id": owner["cash"], "amount": 0})
    assert zero.status_code == 422
    assert transaction_count(owner) == 0
