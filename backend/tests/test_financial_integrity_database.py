from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from hashlib import sha256
from io import BytesIO
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi.testclient import TestClient
from fastapi import HTTPException
from PIL import Image
import pytest


def _create_account(client: TestClient, *, name: str, balance: int, account_type: str = "bank") -> str:
    response = client.post(
        "/api/accounts",
        json={"name": name, "type": account_type, "initial_balance": balance},
    )
    assert response.status_code == 200, response.text
    return response.json()["account"]["id"]


def test_account_locks_are_requested_in_stable_order():
    from app.services.ledger_mutations import lock_owned_accounts

    first_id, second_id = sorted((str(uuid4()), str(uuid4())))
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        {"id": first_id},
        {"id": second_id},
    ]

    locked = lock_owned_accounts(cursor, str(uuid4()), [second_id, first_id, first_id])

    query, params = cursor.execute.call_args.args
    assert params[1] == [first_id, second_id]
    assert "ORDER BY a.id" in query
    assert "FOR UPDATE" in query
    assert set(locked) == {first_id, second_id}


def test_receipt_image_pixel_limit_rejects_bomb_before_storage(monkeypatch):
    from app.services.receipts import prepare_receipt_payload

    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 100)
    image = Image.new("RGB", (20, 20), "white")
    raw = BytesIO()
    image.save(raw, format="PNG")

    with pytest.raises(HTTPException) as error:
        prepare_receipt_payload(
            raw=raw.getvalue(),
            filename="small-file.png",
            content_type="image/png",
            category=None,
        )
    assert error.value.status_code == 413


def test_concurrent_movements_cannot_overdraw_the_same_source(auth_client):
    from app.main import app
    from app.db.pool import db_conn

    source_id = _create_account(
        auth_client,
        name=f"Concurrent source {uuid4().hex[:8]}",
        balance=1_000,
    )
    target_ids = [
        _create_account(auth_client, name=f"Concurrent target {uuid4().hex[:8]}", balance=0)
        for _ in range(2)
    ]
    clients = [TestClient(app, base_url="https://testserver") for _ in range(2)]
    for client in clients:
        client.headers["Origin"] = "https://testserver"
        client.cookies.update(auth_client.cookies)

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(
            executor.map(
                lambda pair: pair[0].post(
                    "/api/movements",
                    json={
                        "source_account_id": source_id,
                        "target_account_id": pair[1],
                        "amount": 700,
                    },
                ),
                zip(clients, target_ids),
            )
        )

    assert sorted(response.status_code for response in responses) == [200, 409]
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT a.id, a.initial_balance
                     + COALESCE(SUM(CASE WHEN t.type = 'income' THEN t.amount ELSE -t.amount END), 0) AS balance
                FROM accounts a
                LEFT JOIN transactions t ON t.account_id = a.id
                WHERE a.id = ANY(%s)
                GROUP BY a.id, a.initial_balance
                """,
                ([source_id, *target_ids],),
            )
            balances = {str(row["id"]): row["balance"] for row in cursor.fetchall()}

    assert balances[source_id] == 300
    assert sorted(balances[target_id] for target_id in target_ids) == [0, 700]


def test_concurrent_notification_delivery_creates_one_ledger_effect(auth_client, api_key):
    from app.db.pool import db_conn
    from app.main import app

    account_id = _create_account(
        auth_client,
        name=f"BCA test {uuid4().hex[:8]}",
        balance=200_000,
    )
    user_id = auth_client.get("/api/auth/me").json()["user"]["id"]
    payload_hash = sha256(uuid4().bytes).hexdigest()
    payload = {
        "events": [
            {
                "device_id": "integration-test-device",
                "package_name": "id.co.bca.mybca.omni.android",
                "app_label": "myBCA",
                "title": "Transaksi Berhasil",
                "body_text": "Transfer QRIS Rp 50.000 ke Kopi Kenangan berhasil",
                "post_time": "2026-09-23T10:00:00Z",
                "payload_hash": payload_hash,
            }
        ]
    }

    clients = [TestClient(app), TestClient(app)]
    for client in clients:
        client.headers["Authorization"] = f"Bearer {api_key}"

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(
            executor.map(
                lambda client: client.post("/api/ingest/notifications", json=payload),
                clients,
            )
        )

    assert [response.status_code for response in responses] == [200, 200]
    results = [response.json() for response in responses]
    assert sum(result["inserted"] for result in results) == 1
    assert sum(result["updated"] for result in results) == 1
    assert sum(result["created_transactions"] for result in results) == 1

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT e.transaction_id, t.account_id, t.amount, t.type
                FROM notification_events e
                JOIN transactions t ON t.id = e.transaction_id
                WHERE e.user_id = %s AND e.payload_hash = %s
                """,
                (user_id, payload_hash),
            )
            result = cursor.fetchone()

    assert result is not None
    assert str(result["account_id"]) == account_id
    assert result["amount"] == 50_000
    assert result["type"] == "expense"


def test_user_expense_rejects_insufficient_funds_without_ledger_effect(auth_client):
    from app.db.pool import db_conn

    account_id = _create_account(
        auth_client,
        name=f"Expense balance {uuid4().hex[:8]}",
        balance=1_000,
    )
    note = f"insufficient-expense-{uuid4().hex}"

    response = auth_client.post(
        "/api/transactions",
        json={"type": "expense", "account_id": account_id, "amount": 1_001, "notes": note},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "insufficient_funds"
    assert response.json()["detail"]["required_amount"] == 1_001
    assert response.json()["detail"]["available_amount"] == 1_000
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS count FROM transactions WHERE account_id = %s AND notes = %s",
                (account_id, note),
            )
            transaction_count = cursor.fetchone()["count"]
            cursor.execute(
                """
                SELECT a.initial_balance
                     + COALESCE(SUM(CASE WHEN t.type = 'income' THEN t.amount ELSE -t.amount END), 0) AS balance
                FROM accounts a
                LEFT JOIN transactions t ON t.account_id = a.id
                WHERE a.id = %s
                GROUP BY a.id, a.initial_balance
                """,
                (account_id,),
            )
            balance = cursor.fetchone()["balance"]

    assert transaction_count == 0
    assert balance == 1_000


def test_manual_movement_rejects_insufficient_funds_without_either_half(auth_client):
    from app.db.pool import db_conn

    source_id = _create_account(
        auth_client,
        name=f"Movement source {uuid4().hex[:8]}",
        balance=1_000,
    )
    target_id = _create_account(
        auth_client,
        name=f"Movement target {uuid4().hex[:8]}",
        balance=0,
    )

    response = auth_client.post(
        "/api/movements",
        json={"source_account_id": source_id, "target_account_id": target_id, "amount": 1_001},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "insufficient_funds"
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS count FROM transactions WHERE account_id = ANY(%s)",
                ([source_id, target_id],),
            )
            transaction_count = cursor.fetchone()["count"]

    assert transaction_count == 0


def test_generic_movement_rejects_investment_positions_on_either_side(auth_client):
    from app.db.pool import db_conn

    liquid_id = _create_account(
        auth_client,
        name=f"Trade-only liquid {uuid4().hex[:8]}",
        balance=5_000,
    )
    position_response = auth_client.post(
        "/api/accounts",
        json={
            "name": f"Trade-only position {uuid4().hex[:8]}",
            "type": "investment",
            "instrument_type": "gold",
            "units": 5,
            "avg_buy_price": 1_000,
            "initial_balance": 0,
        },
    )
    assert position_response.status_code == 200, position_response.text
    position_id = position_response.json()["account"]["id"]

    for source_id, target_id in ((position_id, liquid_id), (liquid_id, position_id)):
        rejected = auth_client.post(
            "/api/movements",
            json={
                "source_account_id": source_id,
                "target_account_id": target_id,
                "amount": 1_000,
            },
        )
        assert rejected.status_code == 422
        assert rejected.json()["detail"]["code"] == "investment_trade_required"

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS count FROM transactions WHERE account_id = ANY(%s)",
                ([liquid_id, position_id],),
            )
            transaction_count = cursor.fetchone()["count"]

    assert transaction_count == 0


def test_transaction_rejects_archived_source_and_incompatible_category(auth_client):
    archived_account_id = _create_account(
        auth_client,
        name=f"Archived source {uuid4().hex[:8]}",
        balance=2_000,
    )
    archived = auth_client.patch(
        f"/api/accounts/{archived_account_id}",
        json={"is_archived": True},
    )
    assert archived.status_code == 200, archived.text

    archived_source = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": archived_account_id, "amount": 100},
    )
    assert archived_source.status_code == 404

    income_categories = auth_client.get("/api/categories?kind=income").json()["categories"]
    category_id = income_categories[0]["id"]
    liquid_id = _create_account(
        auth_client,
        name=f"Category source {uuid4().hex[:8]}",
        balance=1_000,
    )
    incompatible_category = auth_client.post(
        "/api/transactions",
        json={
            "type": "expense",
            "account_id": liquid_id,
            "category_id": category_id,
            "amount": 100,
        },
    )
    assert incompatible_category.status_code == 400
    assert "incompatible" in incompatible_category.json()["detail"].lower()


def test_obligation_combined_update_and_currency_validation(auth_client):
    created = auth_client.post(
        "/api/obligations",
        json={"name": f"Combined state {uuid4().hex[:8]}", "total_amount": 12_000, "remaining_amount": 10_000},
    )
    assert created.status_code == 200, created.text
    obligation_id = created.json()["obligation"]["id"]

    invalid = auth_client.patch(
        f"/api/obligations/{obligation_id}",
        json={"total_amount": 8_000},
    )
    assert invalid.status_code == 422
    preserved = auth_client.get("/api/obligations?include_archived=true").json()["obligations"]
    assert next(item for item in preserved if item["id"] == obligation_id)["total_amount"] == 12_000

    valid = auth_client.patch(
        f"/api/obligations/{obligation_id}",
        json={"total_amount": 11_000, "remaining_amount": 9_000},
    )
    assert valid.status_code == 200, valid.text
    assert valid.json()["obligation"]["total_amount"] == 11_000
    assert valid.json()["obligation"]["remaining_amount"] == 9_000

    unsupported_currency = auth_client.patch("/api/auth/settings", json={"currency": "EUR"})
    assert unsupported_currency.status_code == 422


def test_income_category_has_no_expense_only_metadata(auth_client):
    created = auth_client.post(
        "/api/categories",
        json={"name": f"Income category {uuid4().hex[:8]}", "kind": "income"},
    )
    assert created.status_code == 200, created.text
    category = created.json()["category"]
    assert category["kakeibo_type"] is None
    assert category["monthly_budget"] is None

    changed = auth_client.patch(
        f"/api/categories/{category['id']}",
        json={"kind": "expense", "monthly_budget": 1_000, "kakeibo_type": "want"},
    )
    assert changed.status_code == 200, changed.text
    assert changed.json()["category"]["kind"] == "expense"
    assert changed.json()["category"]["kakeibo_type"] == "want"


def test_category_delete_archives_when_recurring_rule_references_it(auth_client):
    category_response = auth_client.post(
        "/api/categories",
        json={"name": f"Recurring category {uuid4().hex[:8]}", "kind": "expense"},
    )
    assert category_response.status_code == 200, category_response.text
    category_id = category_response.json()["category"]["id"]
    source_id = _create_account(
        auth_client,
        name=f"Recurring source {uuid4().hex[:8]}",
        balance=100_000,
    )
    rule_response = auth_client.post(
        "/api/recurring",
        json={
            "name": "Recurring category archive regression",
            "type": "expense",
            "amount": 10_000,
            "source_account_id": source_id,
            "category_id": category_id,
            "schedule_type": "payday",
        },
    )
    assert rule_response.status_code == 200, rule_response.text

    deleted = auth_client.delete(f"/api/categories/{category_id}")

    assert deleted.status_code == 200, deleted.text
    archived_categories = auth_client.get("/api/categories?include_archived=true").json()["categories"]
    category = next(item for item in archived_categories if item["id"] == category_id)
    assert category["is_archived"] is True
    rules = auth_client.get("/api/recurring").json()["rules"]
    rule = next(item for item in rules if item["id"] == rule_response.json()["id"])
    assert rule["category_id"] == category_id


def test_receipt_upload_is_bounded_and_replacement_is_recoverable(auth_client, tmp_path, monkeypatch):
    from dataclasses import replace
    from pathlib import Path

    import app.routers.transactions as transactions_router
    import app.services.receipts as receipt_service
    from app.core.config import settings

    test_settings = replace(settings, receipts_dir=str(tmp_path), receipt_max_mb=1)
    monkeypatch.setattr(transactions_router, "settings", test_settings)
    monkeypatch.setattr(receipt_service, "settings", test_settings)
    account_id = _create_account(
        auth_client,
        name=f"Receipt account {uuid4().hex[:8]}",
        balance=0,
    )
    transaction = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": account_id, "amount": 100},
    )
    assert transaction.status_code == 200, transaction.text
    transaction_id = transaction.json()["transaction_id"]

    first_upload = auth_client.post(
        f"/api/transactions/{transaction_id}/receipt",
        files={"file": ("untrusted-name.pdf", b"%PDF-1.4\nreceipt one", "application/octet-stream")},
    )
    assert first_upload.status_code == 200, first_upload.text
    first_path = Path(tmp_path, first_upload.json()["receipt_path"])
    assert first_path.is_file()

    invalid_upload = auth_client.post(
        f"/api/transactions/{transaction_id}/receipt",
        files={"file": ("receipt.png", b"not an image", "image/png")},
    )
    assert invalid_upload.status_code == 400
    assert first_path.is_file()

    oversized_upload = auth_client.post(
        f"/api/transactions/{transaction_id}/receipt",
        files={"file": ("large.pdf", b"%PDF-1.4" + b"x" * (1024 * 1024), "application/pdf")},
    )
    assert oversized_upload.status_code == 413
    assert first_path.is_file()

    second_upload = auth_client.post(
        f"/api/transactions/{transaction_id}/receipt",
        files={"file": ("replacement.pdf", b"%PDF-1.4\nreceipt two", "application/pdf")},
    )
    assert second_upload.status_code == 200, second_upload.text
    second_path = Path(tmp_path, second_upload.json()["receipt_path"])
    assert second_path.is_file()
    assert not first_path.exists()

    deleted = auth_client.delete(f"/api/transactions/{transaction_id}")
    assert deleted.status_code == 200, deleted.text
    assert not second_path.exists()


def test_transaction_create_rejects_client_supplied_receipt_path(auth_client):
    account_id = _create_account(
        auth_client,
        name=f"Receipt path guard {uuid4().hex[:8]}",
        balance=0,
    )
    response = auth_client.post(
        "/api/transactions",
        json={
            "type": "income",
            "account_id": account_id,
            "amount": 100,
            "receipt_path": "another-user/receipt.pdf",
        },
    )
    assert response.status_code == 422
    assert "receipt endpoint" in response.json()["detail"]


def test_concurrent_receipt_replacements_leave_only_the_committed_file(auth_client, tmp_path, monkeypatch):
    from dataclasses import replace
    from pathlib import Path

    import app.routers.transactions as transactions_router
    import app.services.receipts as receipt_service
    from app.core.config import settings
    from app.main import app

    test_settings = replace(settings, receipts_dir=str(tmp_path))
    monkeypatch.setattr(transactions_router, "settings", test_settings)
    monkeypatch.setattr(receipt_service, "settings", test_settings)
    account_id = _create_account(
        auth_client,
        name=f"Concurrent receipt {uuid4().hex[:8]}",
        balance=0,
    )
    created = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": account_id, "amount": 100},
    )
    assert created.status_code == 200, created.text
    transaction_id = created.json()["transaction_id"]

    clients = [TestClient(app, base_url="https://testserver") for _ in range(2)]
    for client in clients:
        client.headers["Origin"] = "https://testserver"
        client.cookies.update(auth_client.cookies)
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(
            lambda pair: pair[0].post(
                f"/api/transactions/{transaction_id}/receipt",
                files={"file": ("receipt.pdf", f"%PDF-1.4\nreceipt {pair[1]}".encode(), "application/pdf")},
            ),
            zip(clients, range(2)),
        ))
    assert all(response.status_code == 200 for response in responses)
    assert len(list(Path(tmp_path).rglob("*.pdf.gz"))) == 1

    detail = auth_client.get(f"/api/transactions/{transaction_id}")
    assert detail.status_code == 200, detail.text
    active_path = detail.json()["transaction"]["receipt_path"]
    assert Path(tmp_path, active_path).is_file()


def test_receipt_upload_checks_owner_before_storing_bytes(auth_client, tmp_path, monkeypatch):
    from dataclasses import replace

    import app.routers.transactions as transactions_router
    import app.services.receipts as receipt_service
    from app.core.config import settings
    from app.main import app

    test_settings = replace(settings, receipts_dir=str(tmp_path))
    monkeypatch.setattr(transactions_router, "settings", test_settings)
    monkeypatch.setattr(receipt_service, "settings", test_settings)
    account_id = _create_account(
        auth_client,
        name=f"Owned receipt {uuid4().hex[:8]}",
        balance=0,
    )
    created = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": account_id, "amount": 100},
    )
    assert created.status_code == 200, created.text

    another_client = TestClient(app, base_url="https://testserver")
    another_client.headers["Origin"] = "https://testserver"
    registered = another_client.post(
        "/api/auth/register",
        json={
            "username": f"receipt_other_{uuid4().hex[:8]}",
            "password": "testpassword1",
            "invite_code": "TESTCODE",
        },
    )
    assert registered.status_code == 200, registered.text
    rejected = another_client.post(
        f"/api/transactions/{created.json()['transaction_id']}/receipt",
        files={"file": ("receipt.pdf", b"%PDF-1.4\nforeign", "application/pdf")},
    )
    assert rejected.status_code == 404
    assert not list(tmp_path.rglob("*"))


def test_receipt_storage_failure_preserves_previous_attachment(auth_client, tmp_path, monkeypatch):
    from dataclasses import replace
    from pathlib import Path

    import app.routers.transactions as transactions_router
    import app.services.receipts as receipt_service
    from app.core.config import settings

    test_settings = replace(settings, receipts_dir=str(tmp_path))
    monkeypatch.setattr(transactions_router, "settings", test_settings)
    monkeypatch.setattr(receipt_service, "settings", test_settings)
    account_id = _create_account(auth_client, name=f"Receipt failure {uuid4().hex[:8]}", balance=0)
    created = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": account_id, "amount": 100},
    )
    assert created.status_code == 200, created.text
    transaction_id = created.json()["transaction_id"]
    first = auth_client.post(
        f"/api/transactions/{transaction_id}/receipt",
        files={"file": ("first.pdf", b"%PDF-1.4\nfirst", "application/pdf")},
    )
    assert first.status_code == 200, first.text
    first_path = first.json()["receipt_path"]

    original_store = transactions_router.store_receipt

    def fail_after_partial_store(relative_path, content):
        original_store(relative_path, content)
        raise OSError("Injected storage failure")

    monkeypatch.setattr(transactions_router, "store_receipt", fail_after_partial_store)
    with pytest.raises(OSError, match="Injected storage failure"):
        auth_client.post(
            f"/api/transactions/{transaction_id}/receipt",
            files={"file": ("second.pdf", b"%PDF-1.4\nsecond", "application/pdf")},
        )

    detail = auth_client.get(f"/api/transactions/{transaction_id}").json()["transaction"]
    assert detail["receipt_path"] == first_path
    assert [path for path in Path(tmp_path).rglob("*.pdf.gz")] == [Path(tmp_path, first_path)]


def test_receipt_database_failure_removes_new_file_and_keeps_old(auth_client, tmp_path, monkeypatch):
    from contextlib import contextmanager
    from dataclasses import replace
    from pathlib import Path

    import app.routers.transactions as transactions_router
    import app.services.receipts as receipt_service
    from app.core.config import settings

    test_settings = replace(settings, receipts_dir=str(tmp_path))
    monkeypatch.setattr(transactions_router, "settings", test_settings)
    monkeypatch.setattr(receipt_service, "settings", test_settings)
    account_id = _create_account(auth_client, name=f"Receipt DB failure {uuid4().hex[:8]}", balance=0)
    created = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": account_id, "amount": 100},
    )
    assert created.status_code == 200, created.text
    transaction_id = created.json()["transaction_id"]
    first = auth_client.post(
        f"/api/transactions/{transaction_id}/receipt",
        files={"file": ("first.pdf", b"%PDF-1.4\nfirst", "application/pdf")},
    )
    assert first.status_code == 200, first.text
    first_path = first.json()["receipt_path"]

    class FailingCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def __enter__(self):
            self.cursor.__enter__()
            return self

        def __exit__(self, *args):
            return self.cursor.__exit__(*args)

        def execute(self, query, params=None):
            if query.strip().startswith("UPDATE transactions SET receipt_path"):
                raise RuntimeError("Injected receipt database failure")
            return self.cursor.execute(query, params)

        def fetchone(self):
            return self.cursor.fetchone()

    class FailingConnection:
        def __init__(self, connection):
            self.connection = connection

        def cursor(self):
            return FailingCursor(self.connection.cursor())

        def commit(self):
            self.connection.commit()

    original_db_conn = transactions_router.db_conn
    calls = 0

    @contextmanager
    def fail_receipt_update():
        nonlocal calls
        calls += 1
        with original_db_conn() as connection:
            yield FailingConnection(connection) if calls == 2 else connection

    monkeypatch.setattr(transactions_router, "db_conn", fail_receipt_update)
    with pytest.raises(RuntimeError, match="Injected receipt database failure"):
        auth_client.post(
            f"/api/transactions/{transaction_id}/receipt",
            files={"file": ("second.pdf", b"%PDF-1.4\nsecond", "application/pdf")},
        )
    monkeypatch.setattr(transactions_router, "db_conn", original_db_conn)

    detail = auth_client.get(f"/api/transactions/{transaction_id}").json()["transaction"]
    assert detail["receipt_path"] == first_path
    assert [path for path in Path(tmp_path).rglob("*.pdf.gz")] == [Path(tmp_path, first_path)]


def test_payroll_rejects_duplicate_rule_without_financial_effect(auth_client):
    source_id = _create_account(
        auth_client,
        name=f"Payroll duplicate source {uuid4().hex[:8]}",
        balance=1_000,
    )
    target_id = _create_account(
        auth_client,
        name=f"Payroll duplicate target {uuid4().hex[:8]}",
        balance=0,
    )
    created = auth_client.post(
        "/api/recurring",
        json={
            "name": f"Payroll duplicate {uuid4().hex[:8]}",
            "type": "transfer",
            "amount": 100,
            "source_account_id": source_id,
            "target_account_id": target_id,
            "schedule_type": "payday",
            "is_payroll_allocation": True,
        },
    )
    assert created.status_code == 200, created.text
    rule_id = created.json()["id"]
    item = {
        "rule_id": rule_id,
        "source_account_id": source_id,
        "target_account_id": target_id,
        "amount": 100,
    }
    result = auth_client.post("/api/recurring/payroll/execute", json={"items": [item, item]})
    assert result.status_code == 422
    assert "only be selected once" in result.json()["detail"]
    source = auth_client.get("/api/accounts").json()["accounts"]
    account = next(account for account in source if account["id"] == source_id)
    assert account["balance"] == 1_000


def test_pulse_and_dashboard_separate_child_investment_from_parent_cash(auth_client):
    before_pulse = auth_client.get("/api/pulse").json()
    before_dashboard = auth_client.get("/api/dashboard/overview").json()["kpis"]
    parent_id = _create_account(
        auth_client,
        name=f"Mixed parent {uuid4().hex[:8]}",
        balance=1_000,
    )
    child = auth_client.post(
        "/api/accounts",
        json={
            "name": f"Mixed position {uuid4().hex[:8]}",
            "type": "investment",
            "parent_id": parent_id,
            "initial_balance": 2_000,
            "instrument_type": "deposit",
        },
    )
    assert child.status_code == 200, child.text

    pulse = auth_client.get("/api/pulse")
    assert pulse.status_code == 200, pulse.text
    assert pulse.json()["total_liquid_balance"] - before_pulse["total_liquid_balance"] == 1_000
    assert pulse.json()["investment_balance"] - before_pulse["investment_balance"] == 2_000
    assert pulse.json()["total_net_worth"] - before_pulse["total_net_worth"] == 3_000

    dashboard = auth_client.get("/api/dashboard/overview")
    assert dashboard.status_code == 200, dashboard.text
    kpis = dashboard.json()["kpis"]
    assert kpis["liquid_balance"] - before_dashboard["liquid_balance"] == 1_000
    assert kpis["investment_balance"] - before_dashboard["investment_balance"] == 2_000
    assert kpis["total_balance"] - before_dashboard["total_balance"] == 3_000


def test_registration_name_and_settings_validation_persist_without_partial_update(client):
    from app.main import app

    registration_client = TestClient(app, base_url="https://testserver")
    registration_client.headers["Origin"] = "https://testserver"
    username = f"profile_{uuid4().hex[:10]}"
    registration = registration_client.post(
        "/api/auth/register",
        json={
            "username": username,
            "password": "testpassword1",
            "name": "Profile Owner",
            "invite_code": "TESTCODE",
        },
    )
    assert registration.status_code == 200, registration.text
    assert registration_client.get("/api/auth/me").json()["user"]["name"] == "Profile Owner"

    invalid = registration_client.patch(
        "/api/auth/settings",
        json={"name": "Changed Name", "currency": "EUR"},
    )
    assert invalid.status_code == 422
    assert registration_client.get("/api/auth/me").json()["user"]["name"] == "Profile Owner"

    blank = registration_client.patch("/api/auth/settings", json={"name": "   "})
    assert blank.status_code == 422

    updated = registration_client.patch(
        "/api/auth/settings",
        json={"name": "Changed Name", "currency": "USD", "payday_day": 31},
    )
    assert updated.status_code == 200, updated.text
    profile = registration_client.get("/api/auth/me").json()["user"]
    assert profile["name"] == "Changed Name"
    assert profile["currency"] == "USD"
    assert profile["payday_day"] == 31


def test_default_pocket_accepts_owned_child_and_can_be_cleared(auth_client):
    parent_id = _create_account(auth_client, name=f"Pocket parent {uuid4().hex[:8]}", balance=0)
    other_parent_id = _create_account(auth_client, name=f"Other parent {uuid4().hex[:8]}", balance=0)
    child = auth_client.post(
        "/api/accounts",
        json={"name": f"Pocket child {uuid4().hex[:8]}", "type": "bank", "parent_id": parent_id},
    )
    assert child.status_code == 200, child.text
    child_id = child.json()["account"]["id"]

    wrong_parent = auth_client.patch(
        f"/api/accounts/{other_parent_id}",
        json={"default_pocket_id": child_id},
    )
    assert wrong_parent.status_code == 400
    assigned = auth_client.patch(
        f"/api/accounts/{parent_id}",
        json={"default_pocket_id": child_id},
    )
    assert assigned.status_code == 200, assigned.text
    assert assigned.json()["account"]["default_pocket_id"] == child_id

    cleared = auth_client.patch(f"/api/accounts/{parent_id}", json={"default_pocket_id": None})
    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["account"]["default_pocket_id"] is None

    archived = auth_client.patch(f"/api/accounts/{child_id}", json={"is_archived": True})
    assert archived.status_code == 200, archived.text
    rejected_archived = auth_client.patch(
        f"/api/accounts/{parent_id}",
        json={"default_pocket_id": child_id},
    )
    assert rejected_archived.status_code == 400


def test_recurring_rule_clears_optional_fields_and_type_incompatible_fields(auth_client):
    source_id = _create_account(
        auth_client,
        name=f"Rule clearing source {uuid4().hex[:8]}",
        balance=1_000,
    )
    target_id = _create_account(
        auth_client,
        name=f"Rule clearing target {uuid4().hex[:8]}",
        balance=0,
    )
    category = auth_client.post(
        "/api/categories",
        json={"name": f"Rule clearing category {uuid4().hex[:8]}", "kind": "expense"},
    )
    assert category.status_code == 200, category.text
    obligation = auth_client.post(
        "/api/obligations",
        json={"name": f"Rule clearing debt {uuid4().hex[:8]}", "total_amount": 1_000, "remaining_amount": 1_000},
    )
    assert obligation.status_code == 200, obligation.text

    created = auth_client.post(
        "/api/recurring",
        json={
            "name": f"Rule clearing {uuid4().hex[:8]}",
            "type": "expense",
            "amount": 100,
            "source_account_id": source_id,
            "category_id": category.json()["category"]["id"],
            "obligation_id": obligation.json()["obligation"]["id"],
            "schedule_type": "weekly",
            "schedule_day": 3,
            "notes": "Old note",
        },
    )
    assert created.status_code == 200, created.text
    rule_id = created.json()["id"]

    cleared = auth_client.patch(
        f"/api/recurring/{rule_id}",
        json={"obligation_id": None, "notes": None},
    )
    assert cleared.status_code == 200, cleared.text
    switched = auth_client.patch(
        f"/api/recurring/{rule_id}",
        json={"type": "transfer", "target_account_id": target_id, "schedule_type": "payday"},
    )
    assert switched.status_code == 200, switched.text
    rules = auth_client.get("/api/recurring").json()["rules"]
    rule = next(rule for rule in rules if rule["id"] == rule_id)
    assert rule["category_id"] is None
    assert rule["obligation_id"] is None
    assert rule["notes"] is None
    assert rule["schedule_day"] is None
    assert rule["target_account_id"] == target_id

    invalid = auth_client.patch(f"/api/recurring/{rule_id}", json={"schedule_type": "weekly", "schedule_day": 8})
    assert invalid.status_code == 422


def test_recurring_rule_rejects_foreign_account_on_create_and_update(auth_client):
    from app.main import app

    source_id = _create_account(auth_client, name=f"Owned recurring source {uuid4().hex[:8]}", balance=1_000)
    target_id = _create_account(auth_client, name=f"Owned recurring target {uuid4().hex[:8]}", balance=0)
    another_client = TestClient(app, base_url="https://testserver")
    another_client.headers["Origin"] = "https://testserver"
    registered = another_client.post(
        "/api/auth/register",
        json={
            "username": f"rule_other_{uuid4().hex[:8]}",
            "password": "testpassword1",
            "invite_code": "TESTCODE",
        },
    )
    assert registered.status_code == 200, registered.text
    foreign_id = _create_account(another_client, name=f"Foreign target {uuid4().hex[:8]}", balance=0)
    valid_rule = {
        "name": f"Owned recurring rule {uuid4().hex[:8]}",
        "type": "transfer",
        "amount": 100,
        "source_account_id": source_id,
        "target_account_id": target_id,
        "schedule_type": "weekly",
        "schedule_day": 3,
    }
    rejected_create = auth_client.post("/api/recurring", json={**valid_rule, "target_account_id": foreign_id})
    assert rejected_create.status_code == 404
    created = auth_client.post("/api/recurring", json=valid_rule)
    assert created.status_code == 200, created.text
    rule_id = created.json()["id"]

    rejected_update = auth_client.patch(f"/api/recurring/{rule_id}", json={"target_account_id": foreign_id})
    assert rejected_update.status_code == 404
    rules = auth_client.get("/api/recurring").json()["rules"]
    assert next(rule for rule in rules if rule["id"] == rule_id)["target_account_id"] == target_id


def test_payroll_batch_checks_aggregate_source_before_writing(auth_client):
    from app.db.pool import db_conn

    source_id = _create_account(
        auth_client,
        name=f"Aggregate payroll source {uuid4().hex[:8]}",
        balance=100,
    )
    target_ids = [
        _create_account(auth_client, name=f"Aggregate payroll target {uuid4().hex[:8]}", balance=0)
        for _ in range(2)
    ]
    response = auth_client.post(
        "/api/recurring/payroll/execute",
        json={
            "items": [
                {"source_account_id": source_id, "target_account_id": target_id, "amount": 70}
                for target_id in target_ids
            ]
        },
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "insufficient_funds"

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS count FROM transactions WHERE account_id = ANY(%s)",
                ([source_id, *target_ids],),
            )
            assert cursor.fetchone()["count"] == 0


def test_manual_recurring_and_payroll_movements_share_savings_classification(auth_client):
    from app.db.pool import db_conn

    source_id = _create_account(auth_client, name=f"Savings source {uuid4().hex[:8]}", balance=1_000)
    savings_id = _create_account(auth_client, name=f"Savings target {uuid4().hex[:8]}", balance=0)
    operations_id = _create_account(auth_client, name=f"Operations target {uuid4().hex[:8]}", balance=0)
    linked_goal = auth_client.post(
        "/api/goals",
        json={"name": f"Savings link {uuid4().hex[:8]}", "target_amount": 1_000, "account_ids": [savings_id]},
    )
    assert linked_goal.status_code == 200, linked_goal.text

    manual = auth_client.post(
        "/api/movements",
        json={"source_account_id": source_id, "target_account_id": savings_id, "amount": 300},
    )
    assert manual.status_code == 200, manual.text
    payroll = auth_client.post(
        "/api/recurring/payroll/execute",
        json={"items": [{"source_account_id": source_id, "target_account_id": operations_id, "amount": 100}]},
    )
    assert payroll.status_code == 200, payroll.text
    recurring = auth_client.post(
        "/api/recurring",
        json={
            "name": f"Savings transfer {uuid4().hex[:8]}",
            "type": "transfer",
            "amount": 50,
            "source_account_id": operations_id,
            "target_account_id": savings_id,
            "schedule_type": "payday",
        },
    )
    assert recurring.status_code == 200, recurring.text
    executed = auth_client.post("/api/recurring/execute", json={"rule_ids": [recurring.json()["id"]]})
    assert executed.status_code == 200, executed.text
    withdrawal = auth_client.post(
        "/api/movements",
        json={"source_account_id": savings_id, "target_account_id": source_id, "amount": 50},
    )
    assert withdrawal.status_code == 200, withdrawal.text

    user_id = auth_client.get("/api/auth/me").json()["user"]["id"]
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT movement_id, account_id, amount, kakeibo_type
                FROM transactions
                WHERE user_id = %s AND movement_role = 'outbound'
                  AND account_id = ANY(%s)
                  AND movement_id = ANY(%s)
                """,
                (
                    user_id,
                    [source_id, operations_id, savings_id],
                    [
                        manual.json()["movement_id"],
                        payroll.json()["results"][0]["movement_id"],
                        executed.json()["results"][0]["movement_id"],
                        withdrawal.json()["movement_id"],
                    ],
                ),
            )
            rows = {str(row["movement_id"]): row for row in cursor.fetchall()}

    assert rows[manual.json()["movement_id"]]["kakeibo_type"] == "saving"
    assert rows[payroll.json()["results"][0]["movement_id"]]["kakeibo_type"] is None
    assert rows[executed.json()["results"][0]["movement_id"]]["kakeibo_type"] == "saving"
    assert rows[withdrawal.json()["movement_id"]]["kakeibo_type"] == "saving"


def test_payroll_mid_batch_failure_rolls_back_movements_and_rule_date(auth_client, monkeypatch):
    from app.db.pool import db_conn
    import app.routers.recurring as recurring_router

    source_id = _create_account(auth_client, name=f"Rollback payroll source {uuid4().hex[:8]}", balance=1_000)
    target_ids = [
        _create_account(auth_client, name=f"Rollback payroll target {uuid4().hex[:8]}", balance=0)
        for _ in range(2)
    ]
    rule = auth_client.post(
        "/api/recurring",
        json={
            "name": f"Rollback payroll rule {uuid4().hex[:8]}",
            "type": "transfer",
            "amount": 100,
            "source_account_id": source_id,
            "target_account_id": target_ids[0],
            "schedule_type": "payday",
            "is_payroll_allocation": True,
        },
    )
    assert rule.status_code == 200, rule.text
    rule_id = rule.json()["id"]
    due_before = rule.json()["next_due_date"]

    original_create = recurring_router.create_bilateral_movement
    calls = 0

    def fail_second_movement(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("Injected second movement failure")
        return original_create(*args, **kwargs)

    monkeypatch.setattr(recurring_router, "create_bilateral_movement", fail_second_movement)
    with pytest.raises(RuntimeError, match="Injected second movement failure"):
        auth_client.post(
            "/api/recurring/payroll/execute",
            json={
                "items": [
                    {"rule_id": rule_id, "source_account_id": source_id, "target_account_id": target_ids[0], "amount": 100},
                    {"source_account_id": source_id, "target_account_id": target_ids[1], "amount": 100},
                ]
            },
        )

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS count FROM transactions WHERE account_id = ANY(%s)",
                ([source_id, *target_ids],),
            )
            assert cursor.fetchone()["count"] == 0
            cursor.execute("SELECT next_due_date FROM recurring_rules WHERE id = %s", (rule_id,))
            assert cursor.fetchone()["next_due_date"].isoformat() == due_before


def test_movement_can_use_the_exact_available_balance(auth_client):
    from app.db.pool import db_conn

    source_id = _create_account(
        auth_client,
        name=f"Exact movement source {uuid4().hex[:8]}",
        balance=1_000,
    )
    target_id = _create_account(
        auth_client,
        name=f"Exact movement target {uuid4().hex[:8]}",
        balance=0,
    )

    response = auth_client.post(
        "/api/movements",
        json={"source_account_id": source_id, "target_account_id": target_id, "amount": 1_000},
    )

    assert response.status_code == 200, response.text
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT a.id, a.initial_balance
                     + COALESCE(SUM(CASE WHEN t.type = 'income' THEN t.amount ELSE -t.amount END), 0) AS balance
                FROM accounts a
                LEFT JOIN transactions t ON t.account_id = a.id
                WHERE a.id = ANY(%s)
                GROUP BY a.id, a.initial_balance
                """,
                ([source_id, target_id],),
            )
            balances = {str(row["id"]): row["balance"] for row in cursor.fetchall()}

    assert balances == {source_id: 0, target_id: 1_000}


def test_bilateral_movement_replay_retrieval_and_delete(auth_client):
    from app.db.pool import db_conn

    source_id = _create_account(
        auth_client,
        name=f"Movement lifecycle source {uuid4().hex[:8]}",
        balance=1_000,
    )
    target_id = _create_account(
        auth_client,
        name=f"Movement lifecycle target {uuid4().hex[:8]}",
        balance=0,
    )
    payload = {
        "source_account_id": source_id,
        "target_account_id": target_id,
        "amount": 300,
        "idempotency_key": f"movement-{uuid4().hex}",
    }

    created = auth_client.post("/api/movements", json=payload)
    replayed = auth_client.post("/api/movements", json=payload)

    assert created.status_code == 200, created.text
    assert replayed.status_code == 200, replayed.text
    assert replayed.json()["movement_id"] == created.json()["movement_id"]
    assert replayed.json()["idempotent"] is True

    ledger = auth_client.get("/api/transactions", params={"account_id": source_id})
    assert ledger.status_code == 200
    movement = next(
        row
        for row in ledger.json()["transactions"]
        if row.get("movement_id") == created.json()["movement_id"]
    )
    assert movement["transfer_target_account_id"] == target_id
    assert movement["amount"] == 300

    deleted = auth_client.delete(f"/api/movements/{created.json()['movement_id']}")
    assert deleted.status_code == 200, deleted.text
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS count FROM transactions WHERE movement_id = %s",
                (created.json()["movement_id"],),
            )
            remaining_legs = cursor.fetchone()["count"]

    assert remaining_legs == 0


def test_movement_update_replaces_both_legs_and_target(auth_client):
    from app.db.pool import db_conn

    source_id = _create_account(
        auth_client,
        name=f"Movement edit source {uuid4().hex[:8]}",
        balance=2_000,
    )
    first_target_id = _create_account(
        auth_client,
        name=f"Movement edit first target {uuid4().hex[:8]}",
        balance=0,
    )
    next_target_id = _create_account(
        auth_client,
        name=f"Movement edit next target {uuid4().hex[:8]}",
        balance=0,
    )
    created = auth_client.post(
        "/api/movements",
        json={
            "source_account_id": source_id,
            "target_account_id": first_target_id,
            "amount": 200,
        },
    )
    assert created.status_code == 200, created.text

    updated = auth_client.patch(
        f"/api/movements/{created.json()['movement_id']}",
        json={
            "source_account_id": source_id,
            "target_account_id": next_target_id,
            "amount": 300,
        },
    )
    assert updated.status_code == 200, updated.text

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT account_id, movement_role, amount FROM transactions WHERE movement_id = %s ORDER BY movement_role",
                (created.json()["movement_id"],),
            )
            legs = cursor.fetchall()

    assert [(str(leg["account_id"]), leg["movement_role"], leg["amount"]) for leg in legs] == [
        (next_target_id, "inbound", 300),
        (source_id, "outbound", 300),
    ]


def test_legacy_unlinked_transaction_rows_are_not_paired_by_heuristics(auth_client):
    source_id = _create_account(
        auth_client,
        name=f"Legacy source {uuid4().hex[:8]}",
        balance=1_000,
    )
    target_id = _create_account(
        auth_client,
        name=f"Legacy target {uuid4().hex[:8]}",
        balance=0,
    )
    shared_notes = f"Unlinked legacy movement {uuid4().hex}"

    outgoing = auth_client.post(
        "/api/transactions",
        json={"type": "expense", "account_id": source_id, "amount": 100, "notes": shared_notes},
    )
    incoming = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": target_id, "amount": 100, "notes": shared_notes},
    )
    assert outgoing.status_code == 200, outgoing.text
    assert incoming.status_code == 200, incoming.text

    source_ledger = auth_client.get("/api/transactions", params={"account_id": source_id})
    target_ledger = auth_client.get("/api/transactions", params={"account_id": target_id})
    assert source_ledger.status_code == target_ledger.status_code == 200
    source_row = next(row for row in source_ledger.json()["transactions"] if row["notes"] == shared_notes)
    target_row = next(row for row in target_ledger.json()["transactions"] if row["notes"] == shared_notes)

    assert source_row["movement_id"] is None
    assert source_row["transfer_target_account_id"] is None
    assert target_row["movement_id"] is None
    assert target_row["transfer_target_account_id"] is None


def test_confirmed_merge_links_existing_transactions_without_changing_balances(auth_client):
    from app.db.pool import db_conn

    source_id = _create_account(auth_client, name=f"Merge source {uuid4().hex[:8]}", balance=1_000)
    target_id = _create_account(auth_client, name=f"Merge target {uuid4().hex[:8]}", balance=0)
    expense_date = "2026-09-20T10:00:27Z"
    income_date = "2026-09-20T10:02:45Z"
    expense_response = auth_client.post(
        "/api/transactions",
        json={"type": "expense", "account_id": source_id, "amount": 300, "notes": "Paid by bank", "date": expense_date},
    )
    income_response = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": target_id, "amount": 300, "notes": "Received by wallet", "date": income_date},
    )
    assert expense_response.status_code == income_response.status_code == 200
    expense_id = expense_response.json()["transaction_id"]
    income_id = income_response.json()["transaction_id"]
    logical_before = auth_client.get("/api/transactions?logical_movements=true&limit=1").json()["total"]

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE transactions SET receipt_path = %s WHERE id = %s",
                ("receipts/merge-test.jpg", expense_id),
            )
            connection.commit()
            cursor.execute(
                """
                SELECT id, account_id, type, amount, notes, date, receipt_path
                FROM transactions WHERE id = ANY(%s) ORDER BY id
                """,
                ([expense_id, income_id],),
            )
            before = cursor.fetchall()

    merged = auth_client.post(
        "/api/movements/merge",
        json={"expense_transaction_id": expense_id, "income_transaction_id": income_id},
    )
    assert merged.status_code == 200, merged.text
    movement_id = merged.json()["movement_id"]

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, account_id, type, amount, notes, date, receipt_path,
                       movement_id, movement_role, category_id
                FROM transactions WHERE id = ANY(%s) ORDER BY id
                """,
                ([expense_id, income_id],),
            )
            after = cursor.fetchall()
            cursor.execute(
                "SELECT name, kind, is_excluded_from_budget FROM categories WHERE id = ANY(%s)",
                ([str(row["category_id"]) for row in after],),
            )
            categories = cursor.fetchall()

    assert [{key: row[key] for key in before[0]} for row in after] == before
    assert {str(row["movement_id"]) for row in after} == {movement_id}
    assert {row["movement_role"] for row in after} == {"outbound", "inbound"}
    assert {(row["name"], row["kind"], row["is_excluded_from_budget"]) for row in categories} == {
        ("Internal Movement", "expense", True),
        ("Internal Movement", "income", True),
    }
    logical_after = auth_client.get("/api/transactions?logical_movements=true&limit=1").json()["total"]
    assert logical_after == logical_before - 1
    inbound_search = auth_client.get(
        "/api/transactions",
        params={"logical_movements": "true", "q": "Received by wallet"},
    )
    assert inbound_search.status_code == 200
    assert income_id in {row["id"] for row in inbound_search.json()["transactions"]}


def test_confirmed_merge_rejects_invalid_or_already_linked_rows(auth_client):
    source_id = _create_account(auth_client, name=f"Merge reject source {uuid4().hex[:8]}", balance=1_000)
    target_id = _create_account(auth_client, name=f"Merge reject target {uuid4().hex[:8]}", balance=0)
    outgoing = auth_client.post(
        "/api/transactions",
        json={"type": "expense", "account_id": source_id, "amount": 200},
    )
    incoming = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": target_id, "amount": 100},
    )
    assert outgoing.status_code == incoming.status_code == 200
    out_id = outgoing.json()["transaction_id"]
    in_id = incoming.json()["transaction_id"]
    payload = {"expense_transaction_id": out_id, "income_transaction_id": in_id}

    unequal = auth_client.post("/api/movements/merge", json=payload)
    assert unequal.status_code == 422
    assert auth_client.get(f"/api/transactions/{out_id}").json()["transaction"]["movement_id"] is None

    corrected = auth_client.patch(f"/api/transactions/{in_id}", json={"amount": 200})
    assert corrected.status_code == 200, corrected.text
    merged = auth_client.post("/api/movements/merge", json=payload)
    assert merged.status_code == 200, merged.text
    replay = auth_client.post("/api/movements/merge", json=payload)
    assert replay.status_code == 409


def test_confirmed_merge_rejects_goal_link_without_partial_classification(auth_client):
    source_id = _create_account(auth_client, name=f"Merge goal source {uuid4().hex[:8]}", balance=1_000)
    target_id = _create_account(auth_client, name=f"Merge goal target {uuid4().hex[:8]}", balance=0)
    goal = auth_client.post(
        "/api/goals",
        json={"name": f"Merge guard {uuid4().hex[:8]}", "target_amount": 1_000},
    )
    assert goal.status_code == 200, goal.text
    goal_id = goal.json()["goal"]["id"]
    outgoing = auth_client.post(
        "/api/transactions",
        json={"type": "expense", "account_id": source_id, "amount": 100, "goal_id": goal_id},
    )
    incoming = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": target_id, "amount": 100},
    )
    assert outgoing.status_code == incoming.status_code == 200
    out_id = outgoing.json()["transaction_id"]
    in_id = incoming.json()["transaction_id"]

    rejected = auth_client.post(
        "/api/movements/merge",
        json={"expense_transaction_id": out_id, "income_transaction_id": in_id},
    )

    assert rejected.status_code == 409
    for transaction_id in (out_id, in_id):
        transaction = auth_client.get(f"/api/transactions/{transaction_id}").json()["transaction"]
        assert transaction["movement_id"] is None
        assert transaction["category_name"] != "Internal Movement"


def test_concurrent_confirmed_merge_claims_one_pair_only(auth_client):
    from app.main import app

    source_id = _create_account(auth_client, name=f"Merge race source {uuid4().hex[:8]}", balance=1_000)
    target_id = _create_account(auth_client, name=f"Merge race target {uuid4().hex[:8]}", balance=0)
    outgoing = auth_client.post(
        "/api/transactions",
        json={"type": "expense", "account_id": source_id, "amount": 100},
    )
    incoming = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": target_id, "amount": 100},
    )
    assert outgoing.status_code == incoming.status_code == 200
    payload = {
        "expense_transaction_id": outgoing.json()["transaction_id"],
        "income_transaction_id": incoming.json()["transaction_id"],
    }
    clients = [TestClient(app, base_url="https://testserver") for _ in range(2)]
    for client in clients:
        client.headers["Origin"] = "https://testserver"
        client.cookies.update(auth_client.cookies)

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda client: client.post("/api/movements/merge", json=payload), clients))

    assert sorted(response.status_code for response in responses) == [200, 409]
    outgoing_row = auth_client.get(f"/api/transactions/{payload['expense_transaction_id']}").json()["transaction"]
    incoming_row = auth_client.get(f"/api/transactions/{payload['income_transaction_id']}").json()["transaction"]
    assert outgoing_row["movement_id"] == incoming_row["movement_id"]
    assert outgoing_row["movement_id"] is not None


def test_rejected_movement_update_preserves_both_existing_legs(auth_client):
    from app.db.pool import db_conn

    source_id = _create_account(
        auth_client,
        name=f"Movement update source {uuid4().hex[:8]}",
        balance=1_000,
    )
    target_id = _create_account(
        auth_client,
        name=f"Movement update target {uuid4().hex[:8]}",
        balance=0,
    )
    created = auth_client.post(
        "/api/movements",
        json={"source_account_id": source_id, "target_account_id": target_id, "amount": 100},
    )
    assert created.status_code == 200, created.text
    movement_id = created.json()["movement_id"]

    expense = auth_client.post(
        "/api/transactions",
        json={"type": "expense", "account_id": source_id, "amount": 900},
    )
    assert expense.status_code == 200, expense.text

    rejected = auth_client.patch(
        f"/api/movements/{movement_id}",
        json={"source_account_id": source_id, "target_account_id": target_id, "amount": 101},
    )

    assert rejected.status_code == 409
    assert rejected.json()["detail"]["code"] == "insufficient_funds"
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT movement_role, amount FROM transactions WHERE movement_id = %s ORDER BY movement_role",
                (movement_id,),
            )
            legs = cursor.fetchall()

    assert [(leg["movement_role"], leg["amount"]) for leg in legs] == [
        ("inbound", 100),
        ("outbound", 100),
    ]


def test_investment_trade_funding_units_and_oversell_are_atomic(auth_client):
    from app.db.pool import db_conn

    funding_id = _create_account(
        auth_client,
        name=f"Trade funding {uuid4().hex[:8]}",
        balance=100_000,
    )
    position_response = auth_client.post(
        "/api/accounts",
        json={
            "name": f"Trade position {uuid4().hex[:8]}",
            "type": "investment",
            "instrument_type": "stock",
            "instrument_symbol": "TEST.JK",
            "units": 10,
            "avg_buy_price": 1_000,
            "initial_balance": 0,
        },
    )
    assert position_response.status_code == 200, position_response.text
    position_id = position_response.json()["account"]["id"]

    bought = auth_client.post(
        "/api/transactions",
        json={
            "type": "expense",
            "account_id": funding_id,
            "target_account_id": position_id,
            "amount": 10_000,
            "investment_action": "buy",
            "units": 5,
            "price_per_unit": 2_000,
        },
    )
    assert bought.status_code == 200, bought.text

    sold = auth_client.post(
        "/api/transactions",
        json={
            "type": "income",
            "account_id": position_id,
            "target_account_id": funding_id,
            "amount": 30_000,
            "investment_action": "sell",
            "units": 15,
            "price_per_unit": 2_000,
        },
    )
    assert sold.status_code == 200, sold.text

    oversold = auth_client.post(
        "/api/transactions",
        json={
            "type": "income",
            "account_id": position_id,
            "target_account_id": funding_id,
            "amount": 2_000,
            "investment_action": "sell",
            "units": 1,
            "price_per_unit": 2_000,
        },
    )
    assert oversold.status_code == 409
    assert oversold.json()["detail"]["code"] == "insufficient_units"

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT units, avg_buy_price FROM accounts WHERE id = %s",
                (position_id,),
            )
            position = cursor.fetchone()
            cursor.execute(
                "SELECT COUNT(*) AS count FROM transactions WHERE movement_id IN (%s, %s)",
                (bought.json()["movement_id"], sold.json()["movement_id"]),
            )
            leg_count = cursor.fetchone()["count"]

    assert position["units"] == 0
    assert position["avg_buy_price"] == 1_333
    assert leg_count == 4


def test_investment_buy_rejects_insufficient_funding_without_movement(auth_client):
    from app.db.pool import db_conn

    funding_id = _create_account(
        auth_client,
        name=f"Trade low funding {uuid4().hex[:8]}",
        balance=1_000,
    )
    position_response = auth_client.post(
        "/api/accounts",
        json={
            "name": f"Trade empty position {uuid4().hex[:8]}",
            "type": "investment",
            "instrument_type": "stock",
            "instrument_symbol": "TEST.JK",
            "units": 0,
            "initial_balance": 0,
        },
    )
    assert position_response.status_code == 200, position_response.text
    position_id = position_response.json()["account"]["id"]

    rejected = auth_client.post(
        "/api/transactions",
        json={
            "type": "expense",
            "account_id": funding_id,
            "target_account_id": position_id,
            "amount": 1_001,
            "investment_action": "buy",
            "units": 1,
            "price_per_unit": 1_001,
        },
    )

    assert rejected.status_code == 409
    assert rejected.json()["detail"]["code"] == "insufficient_funds"
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS count FROM transactions WHERE account_id = ANY(%s)",
                ([funding_id, position_id],),
            )
            transaction_count = cursor.fetchone()["count"]
            cursor.execute("SELECT units FROM accounts WHERE id = %s", (position_id,))
            units = cursor.fetchone()["units"]

    assert transaction_count == 0
    assert units == 0


def test_concurrent_recurring_scheduler_commits_one_occurrence(auth_client):
    from app.db.pool import db_conn
    from app.routers.recurring import process_due_recurring_rules

    source_id = _create_account(
        auth_client,
        name=f"Recurring source {uuid4().hex[:8]}",
        balance=1_000,
    )
    target_id = _create_account(
        auth_client,
        name=f"Recurring target {uuid4().hex[:8]}",
        balance=0,
    )
    created = auth_client.post(
        "/api/recurring",
        json={
            "name": f"Due transfer {uuid4().hex[:8]}",
            "type": "transfer",
            "amount": 700,
            "source_account_id": source_id,
            "target_account_id": target_id,
            "schedule_type": "monthly_day",
            "schedule_day": date.today().day,
            "auto_post": True,
        },
    )
    assert created.status_code == 200, created.text
    rule_id = created.json()["id"]
    due_date = date.today() - timedelta(days=1)
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE recurring_rules SET next_due_date = %s WHERE id = %s",
                (due_date, rule_id),
            )

    user_id = auth_client.get("/api/auth/me").json()["user"]["id"]
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda _: process_due_recurring_rules(user_id=user_id, limit=20),
                range(2),
            )
        )

    assert sum(result["processed_count"] for result in results) == 1
    assert sum(len(result["executed_rule_ids"]) for result in results) == 1
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT status, movement_id FROM recurring_executions WHERE recurring_rule_id = %s AND scheduled_for = %s",
                (rule_id, due_date),
            )
            occurrence = cursor.fetchone()
            cursor.execute(
                "SELECT COUNT(*) AS count FROM transactions WHERE movement_id = %s",
                (occurrence["movement_id"],),
            )
            movement_legs = cursor.fetchone()["count"]
            cursor.execute(
                "SELECT next_due_date FROM recurring_rules WHERE id = %s",
                (rule_id,),
            )
            next_due_date = cursor.fetchone()["next_due_date"]

    assert occurrence["status"] == "succeeded"
    assert movement_legs == 2
    assert next_due_date > due_date


def test_failed_recurring_occurrence_preserves_schedule_and_ledger(auth_client):
    from app.db.pool import db_conn
    from app.routers.recurring import process_due_recurring_rules

    source_id = _create_account(
        auth_client,
        name=f"Recurring low source {uuid4().hex[:8]}",
        balance=100,
    )
    category_response = auth_client.get("/api/categories")
    assert category_response.status_code == 200
    category = next(
        item
        for item in category_response.json()["categories"]
        if item["kind"] == "expense" and not item.get("is_archived", False)
    )
    created = auth_client.post(
        "/api/recurring",
        json={
            "name": f"Unaffordable expense {uuid4().hex[:8]}",
            "type": "expense",
            "amount": 101,
            "source_account_id": source_id,
            "category_id": category["id"],
            "schedule_type": "monthly_day",
            "schedule_day": date.today().day,
            "auto_post": True,
        },
    )
    assert created.status_code == 200, created.text
    rule_id = created.json()["id"]
    due_date = date.today() - timedelta(days=1)
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE recurring_rules SET next_due_date = %s WHERE id = %s",
                (due_date, rule_id),
            )

    user_id = auth_client.get("/api/auth/me").json()["user"]["id"]
    result = process_due_recurring_rules(user_id=user_id, limit=20)

    assert result["processed_count"] == 1
    assert result["results"][0]["status"] == "failed"
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT status, error_code FROM recurring_executions WHERE recurring_rule_id = %s AND scheduled_for = %s",
                (rule_id, due_date),
            )
            occurrence = cursor.fetchone()
            cursor.execute(
                "SELECT next_due_date FROM recurring_rules WHERE id = %s",
                (rule_id,),
            )
            next_due_date = cursor.fetchone()["next_due_date"]
            cursor.execute(
                "SELECT COUNT(*) AS count FROM transactions WHERE account_id = %s AND amount = 101",
                (source_id,),
            )
            transaction_count = cursor.fetchone()["count"]

    assert occurrence["status"] == "failed"
    assert occurrence["error_code"] == "insufficient_funds"
    assert next_due_date == due_date
    assert transaction_count == 0


def test_standalone_goal_adjustments_do_not_create_transactions(auth_client):
    from app.db.pool import db_conn

    user_id = auth_client.get("/api/auth/me").json()["user"]["id"]
    goal_response = auth_client.post(
        "/api/goals",
        json={"name": f"Standalone {uuid4().hex[:8]}", "target_amount": 100_000},
    )
    assert goal_response.status_code == 200, goal_response.text
    goal_id = goal_response.json()["goal"]["id"]

    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS count FROM transactions WHERE user_id = %s", (user_id,))
            transaction_count_before = cursor.fetchone()["count"]

    adjustment = auth_client.post(f"/api/goals/{goal_id}/adjust", json={"amount": 25_000})

    assert adjustment.status_code == 200, adjustment.text
    assert adjustment.json()["current_amount"] == 25_000
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS count FROM transactions WHERE user_id = %s", (user_id,))
            transaction_count = cursor.fetchone()["count"]

    assert transaction_count == transaction_count_before


def test_account_backed_goal_follows_transactions_without_adjustment_record(auth_client):
    account_id = _create_account(
        auth_client,
        name=f"Goal funding account {uuid4().hex[:8]}",
        balance=0,
    )
    goal_response = auth_client.post(
        "/api/goals",
        json={
            "name": f"Account-backed {uuid4().hex[:8]}",
            "target_amount": 100_000,
            "current_amount": 90_000,
            "account_ids": [account_id],
        },
    )
    assert goal_response.status_code == 200, goal_response.text
    goal_id = goal_response.json()["goal"]["id"]
    assert goal_response.json()["goal"]["current_amount"] == 0

    income = auth_client.post(
        "/api/transactions",
        json={"type": "income", "account_id": account_id, "amount": 50_000},
    )
    assert income.status_code == 200, income.text

    listed_goals = auth_client.get("/api/goals")
    assert listed_goals.status_code == 200
    goal = next(item for item in listed_goals.json()["goals"] if item["id"] == goal_id)
    assert goal["current_amount"] == 50_000

    rejected_adjustment = auth_client.post(f"/api/goals/{goal_id}/adjust", json={"amount": 1_000})
    assert rejected_adjustment.status_code == 409
    assert "cannot be adjusted directly" in rejected_adjustment.json()["detail"]


def test_settled_external_movement_marks_negative_source_for_reconciliation(
    client,
    monkeypatch,
):
    from app.db.pool import db_conn
    from app.main import app
    from app.services.notification_parser import ParsedNotification

    client = TestClient(app, base_url="https://testserver")
    client.headers["Origin"] = "https://testserver"
    registration = client.post(
        "/auth/register",
        json={
            "username": f"reconcile_{uuid4().hex[:10]}",
            "password": "testpassword1",
            "name": "Reconciliation Test",
            "invite_code": "TESTCODE",
        },
    )
    assert registration.status_code == 200, registration.text
    user_id = registration.json()["user"]["id"]
    key_response = client.post("/api-key/reset")
    assert key_response.status_code == 200, key_response.text
    api_key = key_response.json()["api_key"]

    source_id = _create_account(
        client,
        name=f"BCA reconciliation {uuid4().hex[:8]}",
        balance=0,
    )
    target_id = _create_account(
        client,
        name=f"Bank Jago reconciliation {uuid4().hex[:8]}",
        balance=0,
    )
    payload_hash = sha256(uuid4().bytes).hexdigest()
    parsed = ParsedNotification(
        is_financial=True,
        event_class="transfer",
        amount=50_000,
        currency="IDR",
        direction="internal",
        source_pocket=None,
        target_pocket=None,
        counterparty="Bank Jago",
        category_hint=None,
        confidence=1.0,
        raw_title="Transfer berhasil",
        raw_text="Transfer Rp 50.000 ke Bank Jago berhasil",
        package_name="id.co.bca.mybca.omni.android",
    )
    monkeypatch.setattr("app.routers.ingest.parse_notification", lambda **_: parsed)
    payload = {
        "events": [
            {
                "device_id": "integration-test-device",
                "package_name": parsed.package_name,
                "app_label": "myBCA",
                "title": parsed.raw_title,
                "body_text": parsed.raw_text,
                "post_time": "2026-09-23T10:00:00Z",
                "payload_hash": payload_hash,
            }
        ]
    }
    client.headers["Authorization"] = f"Bearer {api_key}"

    response = client.post("/api/ingest/notifications", json=payload)

    assert response.status_code == 200, response.text
    assert response.json()["created_transactions"] == 2
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT reconciliation_required, reconciliation_reason
                FROM accounts
                WHERE user_id = %s AND id = %s
                """,
                (user_id, source_id),
            )
            source = cursor.fetchone()

    assert source["reconciliation_required"] is True
    assert source["reconciliation_reason"] == "settled_notification_negative_balance"
    balances = {
        account["id"]: account["balance"]
        for account in client.get("/api/accounts").json()["accounts"]
    }
    assert balances[source_id] == -50_000
    assert balances[target_id] == 50_000

    pulse = client.get("/api/pulse")
    assert pulse.status_code == 200, pulse.text
    assert pulse.json()["total_liquid_balance"] == 50_000

    reconciled = client.post(
        f"/api/accounts/{source_id}/reconcile",
        json={"actual_balance": 0},
    )
    assert reconciled.status_code == 200, reconciled.text
    assert reconciled.json()["account"]["reconciliation_required"] is False
    assert reconciled.json()["account"]["balance"] == 0
    with db_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT reconciliation_required, reconciliation_reason, reconciliation_event_id FROM accounts WHERE id = %s",
                (source_id,),
            )
            cleared = cursor.fetchone()
    assert cleared == {
        "reconciliation_required": False,
        "reconciliation_reason": None,
        "reconciliation_event_id": None,
    }
