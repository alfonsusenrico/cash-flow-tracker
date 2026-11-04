"""Ledger rows name the bank of each side of a movement (show-movement-routes-and-centered-dialogs, D1).

All names and amounts are synthetic. `notification_owner` has the pockets "Kantong Utama" and "Dana Darurat" under
"Bank Jago", and "BCA Harian" and "GoPay" without a parent.
"""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from test_notification_processing_database import notification_owner  # noqa: F401 - fixture

JAKARTA = timezone(timedelta(hours=7))


def rows_by_account(client, owner, query=""):
    response = client.get(f"/api/transactions?limit=100{query}")
    assert response.status_code == 200, response.text
    return {row["account_id"]: row for row in response.json()["transactions"]}


def detail(client, tx_id):
    response = client.get(f"/api/transactions/{tx_id}")
    assert response.status_code == 200, response.text
    return response.json()["transaction"]


def test_linked_legs_name_their_banks(client, notification_owner):  # noqa: F811
    owner = notification_owner
    response = client.post("/api/movements", json={
        "source_account_id": owner["emergency"], "target_account_id": owner["bca"], "amount": 75_000,
    })
    assert response.status_code == 200, response.text

    rows = rows_by_account(client, owner)
    outbound, inbound = rows[owner["emergency"]], rows[owner["bca"]]
    # The pocket's own row names its bank; its partner (an account without a parent) has none.
    assert outbound["movement_role"] == "outbound"
    assert (outbound["account_name"], outbound["account_parent_name"]) == ("Dana Darurat", "Bank Jago")
    assert (outbound["transfer_target_account_name"], outbound["transfer_target_account_parent_name"]) == ("BCA Harian", None)
    # The other leg mirrors it: its partner is the pocket, so the partner's bank is named.
    assert inbound["movement_role"] == "inbound"
    assert (inbound["account_name"], inbound["account_parent_name"]) == ("BCA Harian", None)
    assert (inbound["transfer_target_account_name"], inbound["transfer_target_account_parent_name"]) == ("Dana Darurat", "Bank Jago")

    for row in (outbound, inbound):
        one = detail(client, row["id"])
        for field in ("account_parent_name", "transfer_target_account_name", "transfer_target_account_parent_name"):
            assert one[field] == row[field], field


def test_filtered_view_keeps_the_partner_bank(client, notification_owner):  # noqa: F811
    owner = notification_owner
    response = client.post("/api/movements", json={
        "source_account_id": owner["gopay"], "target_account_id": owner["main"], "amount": 40_000,
    })
    assert response.status_code == 200, response.text

    # Filtered to the pocket, only the inbound leg is listed; it still names where the money came from.
    rows = rows_by_account(client, owner, f"&account_id={owner['main']}")
    assert list(rows) == [owner["main"]]
    leg = rows[owner["main"]]
    assert leg["movement_role"] == "inbound"
    assert (leg["account_parent_name"], leg["transfer_target_account_name"], leg["transfer_target_account_parent_name"]) == (
        "Bank Jago", "GoPay", None,
    )


def test_unlinked_leg_names_only_its_own_bank(client, notification_owner):  # noqa: F811
    from app.db.pool import db_conn

    owner = notification_owner
    tx_id = str(uuid4())
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO transactions (id, user_id, account_id, category_id, type, amount, notes, date, source,
                                         review_status, classified_by)
               VALUES (%s, %s, %s, %s, 'income', 120000, 'Pindah uang dari bank lain', %s, 'notification', 'reviewed', 'owner')""",
            (tx_id, owner["user_id"], owner["main"], owner["internal_in"], datetime(2026, 11, 4, 1, 50, tzinfo=JAKARTA)),
        )
        conn.commit()

    row = rows_by_account(client, owner)[owner["main"]]
    assert row["movement_id"] is None
    assert (row["account_name"], row["account_parent_name"]) == ("Kantong Utama", "Bank Jago")
    assert row["transfer_target_account_name"] is None
    assert row["transfer_target_account_parent_name"] is None
    one = detail(client, tx_id)
    assert (one["account_parent_name"], one["transfer_target_account_parent_name"]) == ("Bank Jago", None)
