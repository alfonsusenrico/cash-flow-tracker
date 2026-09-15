from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app
from app.services.auth import get_current_user


def test_ingest_notifications_success():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }

    app.dependency_overrides[get_current_user] = lambda: mock_user

    payload = {
        "events": [
            {
                "device_id": "test_pixel_7",
                "package_name": "id.co.bca.mybca.omni.android",
                "app_label": "myBCA",
                "notification_key": "0|id.co.bca.mybca.omni.android|101|null|10001",
                "notification_id": 101,
                "channel_id": "trans_alerts",
                "category": "msg",
                "title": "Transaksi Berhasil",
                "body_text": "Transfer QRIS Rp 50.000 ke Kopi Kenangan berhasil",
                "big_text": "Transfer QRIS Rp 50.000 ke Kopi Kenangan berhasil",
                "sub_text": None,
                "summary_text": None,
                "post_time": "2026-09-14T10:00:00Z",
                "payload_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "raw_extras": {"android.title": "Transaksi Berhasil"},
                "source_version": "1.0.0",
                "is_financial": True,
                "event_class": "expense",
                "expected_amount": 50000.0,
                "expected_direction": "out",
                "expected_counterparty": "Kopi Kenangan",
                "label_notes": "Ground truth coffee purchase",
            }
        ]
    }

    with patch("app.routers.ingest.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur
        # Upsert returns was_inserted = True
        cur.fetchone.return_value = {"was_inserted": True}

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/ingest/notifications", json=payload)

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["inserted"] == 1
        assert data["updated"] == 0
        assert data["received"] == 1


def test_ingest_notifications_deduplicate_update():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }

    app.dependency_overrides[get_current_user] = lambda: mock_user

    payload = {
        "events": [
            {
                "device_id": "test_pixel_7",
                "package_name": "com.jago.digitalbanking",
                "title": "Kantong Utama berkurang",
                "body_text": "Rp 25.000 terdebit untuk GoPay Top Up",
                "post_time": "2026-09-14T11:00:00Z",
                "payload_hash": "a1b2c3d4e5f67890123456789abcdef0123456789abcdef0123456789abcdef0",
            }
        ]
    }

    with patch("app.routers.ingest.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur
        # Upsert returns was_inserted = False (updated existing)
        cur.fetchone.return_value = {"was_inserted": False}

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/ingest/notifications", json=payload)

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["inserted"] == 0
        assert data["updated"] == 1


def test_get_notifications_list():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }

    app.dependency_overrides[get_current_user] = lambda: mock_user

    with patch("app.routers.ingest.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur
        cur.fetchone.return_value = {"total": 1}
        cur.fetchall.return_value = [
            {
                "id": str(uuid4()),
                "device_id": "test_pixel_7",
                "package_name": "com.gojek.app",
                "app_label": "GoPay",
                "notification_key": "0|com.gojek.app|1|null|1000",
                "notification_id": 1,
                "channel_id": "orders",
                "category": None,
                "title": "Pembayaran Berhasil",
                "body_text": "Kamu telah membayar Rp 32.000 ke Bakmi GM",
                "big_text": None,
                "sub_text": None,
                "summary_text": None,
                "post_time": datetime.now(timezone.utc),
                "captured_at": datetime.now(timezone.utc),
                "payload_hash": "abcdef1234567890",
                "raw_extras": None,
                "source_version": "1.0.0",
                "is_financial": True,
                "event_class": "expense",
                "expected_amount": Decimal("32000"),
                "expected_direction": "out",
                "expected_counterparty": "Bakmi GM",
                "label_notes": None,
                "labelled_at": None,
                "created_at": datetime.now(timezone.utc),
            }
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.get("/api/ingest/notifications?package_name=com.gojek.app&limit=10")

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert len(data["events"]) == 1
        assert data["events"][0]["package_name"] == "com.gojek.app"
        assert float(data["events"][0]["expected_amount"]) == 32000.0


def test_update_notification_label():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }

    app.dependency_overrides[get_current_user] = lambda: mock_user
    event_id = str(uuid4())

    with patch("app.routers.ingest.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur
        cur.fetchone.return_value = {"id": event_id}

        client = TestClient(app, raise_server_exceptions=True)
        res = client.patch(
            f"/api/ingest/notifications/{event_id}/label",
            json={
                "is_financial": True,
                "event_class": "transfer",
                "expected_amount": 100000,
                "expected_direction": "out",
                "expected_counterparty": "Bank Jago",
                "label_notes": "Transfer antar rekening",
            },
        )

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert data["event_id"] == event_id


def test_ingest_stockbit_routes_to_default_funding_account():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    stockbit_id = str(uuid4())
    rdn_bca_id = str(uuid4())
    bbri_pocket_id = str(uuid4())

    payload = {
        "events": [
            {
                "device_id": "test_phone",
                "package_name": "com.stockbit.android",
                "title": "Pembelian BBRI Fully Match",
                "body_text": "Pembelian 10 lot BBRI match di harga Rp3.340",
                "post_time": "2026-09-15T10:00:00Z",
                "payload_hash": "stockbit_test_hash_12345",
            }
        ]
    }

    with patch("app.routers.ingest.db_conn") as mock_conn, \
         patch("app.routers.ingest.get_instrument_quote", return_value={"price": 3320}):
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchall.side_effect = [
            [{"id": str(uuid4()), "name": "Investasi", "kind": "expense", "kakeibo_type": "saving", "is_excluded_from_budget": True}],
            [],
            [
                {"id": rdn_bca_id, "name": "RDN BCA", "type": "bank", "default_funding_account_id": None},
                {"id": stockbit_id, "name": "Stockbit", "type": "investment", "default_funding_account_id": rdn_bca_id},
            ],
        ]
        cur.fetchone.side_effect = [
            None,  # existing_ev
            None,  # existing pocket check -> None (create new)
            {"id": bbri_pocket_id},  # created pocket
            {"id": str(uuid4())},  # created tx
            {"was_inserted": True},  # upsert event
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/ingest/notifications", json=payload)

        assert res.status_code == 200
        assert res.json()["created_transactions"] == 1

        pocket_insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO accounts" in str(c)]
        assert len(pocket_insert_calls) == 1
        p_args = pocket_insert_calls[0][0][1]
        assert p_args[1] == stockbit_id  # parent_id
        assert p_args[2] == "BBRI"  # name
        assert p_args[3] == "BBRI.JK"  # instrument_symbol
        assert p_args[4] == 1000.0  # units
        assert p_args[5] == 3340  # avg_buy_price
        assert p_args[6] == 3320  # last_price from live quote

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(insert_calls) == 1
        args = insert_calls[0][0][1]
        assert args[1] == rdn_bca_id
        assert args[2] == bbri_pocket_id  # targeted to BBRI pocket!
        assert args[4] == "transfer"
        assert args[5] == 3340000
        assert args[8] is None  # Kakeibo decoupled for investment trades!


def test_ingest_stockbit_accumulates_existing_stock_pocket():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    stockbit_id = str(uuid4())
    rdn_bca_id = str(uuid4())
    bbri_pocket_id = str(uuid4())

    payload = {
        "events": [
            {
                "device_id": "test_phone",
                "package_name": "com.stockbit.android",
                "title": "Pembelian BBRI Fully Match",
                "body_text": "Pembelian 4 lot BBRI match di harga Rp3.400",
                "post_time": "2026-09-15T11:00:00Z",
                "payload_hash": "stockbit_test_hash_67890",
            }
        ]
    }

    with patch("app.routers.ingest.db_conn") as mock_conn, \
         patch("app.routers.ingest.get_instrument_quote", return_value={"price": 3410}):
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchall.side_effect = [
            [{"id": str(uuid4()), "name": "Investasi", "kind": "expense", "kakeibo_type": "saving", "is_excluded_from_budget": True}],
            [],
            [
                {"id": rdn_bca_id, "name": "RDN BCA", "type": "bank", "default_funding_account_id": None, "parent_id": None},
                {"id": stockbit_id, "name": "Stockbit", "type": "investment", "default_funding_account_id": rdn_bca_id, "parent_id": None},
            ],
        ]
        # Existing pocket has 1,000 shares @ Rp 3.300
        cur.fetchone.side_effect = [
            None,  # existing_ev
            {"id": bbri_pocket_id, "units": 1000.0, "avg_buy_price": 3300, "last_price": 3300},  # existing pocket
            {"id": str(uuid4())},  # created tx
            {"was_inserted": True},  # upsert event
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/ingest/notifications", json=payload)

        assert res.status_code == 200
        assert res.json()["created_transactions"] == 1

        update_calls = [c for c in cur.execute.call_args_list if "UPDATE accounts" in str(c)]
        assert len(update_calls) == 1
        u_args = update_calls[0][0][1]
        assert u_args[0] == 1400.0  # new_units
        assert u_args[1] == 3329  # new weighted avg_buy_price
        assert u_args[2] == 3410  # last_price updated
        assert u_args[3] == "BBRI.JK"
        assert u_args[4] == bbri_pocket_id

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(insert_calls) == 1
        assert insert_calls[0][0][1][8] is None  # Kakeibo decoupled!


def test_ingest_jago_pocket_transfer_resolves_child_pockets():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    jago_parent_id = str(uuid4())
    main_pocket_id = str(uuid4())
    gopay_tabungan_id = str(uuid4())

    payload = {
        "events": [
            {
                "device_id": "test_pixel_7",
                "package_name": "com.jago.digitalbanking",
                "title": "Kantong Berpindah",
                "body_text": "Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket.",
                "post_time": "2026-09-15T12:00:00Z",
                "payload_hash": "jago_pocket_test_hash_111",
            }
        ]
    }

    with patch("app.routers.ingest.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchall.side_effect = [
            [{"id": str(uuid4()), "name": "Internal Movement", "kind": "expense", "kakeibo_type": "need", "is_excluded_from_budget": True}],
            [],
            [
                {"id": jago_parent_id, "name": "Bank Jago", "type": "bank", "default_funding_account_id": None, "parent_id": None},
                {"id": main_pocket_id, "name": "Kantong Utama", "type": "bank", "default_funding_account_id": None, "parent_id": jago_parent_id},
                {"id": gopay_tabungan_id, "name": "GoPay Tabungan", "type": "bank", "default_funding_account_id": None, "parent_id": jago_parent_id},
            ],
        ]
        cur.fetchone.side_effect = [
            None,  # existing_ev check
            {"id": str(uuid4())},  # created tx
            {"was_inserted": True},  # upsert event
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/ingest/notifications", json=payload)

        assert res.status_code == 200
        assert res.json()["created_transactions"] == 1

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(insert_calls) == 1
        args = insert_calls[0][0][1]
        assert args[1] == main_pocket_id  # resolved source child pocket
        assert args[2] == gopay_tabungan_id  # resolved target child pocket
        assert args[4] == "transfer"
        assert args[5] == 500000
        assert args[8] is None  # Kakeibo decoupled for internal transfers!


def test_ingest_jago_custom_pockets_transfer():
    mock_user = {
        "id": "11111111-1111-1111-1111-111111111111",
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }
    app.dependency_overrides[get_current_user] = lambda: mock_user

    jago_parent_id = str(uuid4())
    jajan_pocket_id = str(uuid4())
    tabungan_pocket_id = str(uuid4())

    payload = {
        "events": [
            {
                "device_id": "test_pixel_7",
                "package_name": "com.jago.digitalBanking",
                "title": "Kantong Berpindah",
                "body_text": "Rp 50.000 has been moved from your Jajan Pocket to your Tabungan Pocket",
                "post_time": "2026-09-15T12:30:00Z",
                "payload_hash": "jago_pocket_test_hash_222",
            }
        ]
    }

    with patch("app.routers.ingest.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchall.side_effect = [
            [],
            [],
            [
                {"id": jago_parent_id, "name": "Bank Jago", "type": "bank", "default_funding_account_id": None, "parent_id": None},
                {"id": jajan_pocket_id, "name": "Jajan", "type": "bank", "default_funding_account_id": None, "parent_id": jago_parent_id},
                {"id": tabungan_pocket_id, "name": "Tabungan", "type": "bank", "default_funding_account_id": None, "parent_id": jago_parent_id},
            ],
        ]
        cur.fetchone.side_effect = [
            None,  # existing_ev check
            {"id": str(uuid4())},  # created tx
            {"was_inserted": True},  # upsert event
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post("/api/ingest/notifications", json=payload)

        assert res.status_code == 200
        assert res.json()["created_transactions"] == 1

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO transactions" in str(c)]
        assert len(insert_calls) == 1
        args = insert_calls[0][0][1]
        assert args[1] == jajan_pocket_id  # resolved source pocket
        assert args[2] == tabungan_pocket_id  # resolved target pocket
        assert args[4] == "transfer"
        assert args[5] == 50000
        assert args[8] is None  # Kakeibo decoupled!


def test_ingest_supported_apps_scope_verification():
    """Verify that all 6 active registered apps produce transactions, and unconfigured apps gracefully fallback."""
    from app.services.notification_parser import parse_notification

    # 1. myBCA
    p1 = parse_notification("id.co.bca.mybca.omni.android", "Transaksi Berhasil", "Transfer QRIS Rp 50.000 berhasil")
    assert p1.is_financial is True

    # 2. BCA mobile
    p2 = parse_notification("com.bca", "m-Transfer", "Financial Diary: Pengeluaran sebesar IDR 75,000.00 di kategori Makanan.")
    assert p2.is_financial is True
    assert p2.amount == 75000

    # 3. Bank Jago
    p3 = parse_notification("com.jago.digitalbanking", "Pindah Kantong", "Rp100.000 has been moved from your Main Pocket to your Jajan Pocket")
    assert p3.is_financial is True
    assert p3.event_class == "transfer"

    # 4. GoPay
    p4 = parse_notification("com.gojek.gopay", "Bayar Berhasil", "Rp 25.000 udah dikirim ke BCA ALFONSUS ENRICO SOEBIJAN.")
    assert p4.is_financial is True
    assert p4.amount == 25000

    # 5. ShopeePay
    p5 = parse_notification("com.shopeepay.id", "Transfer Diterima", "ALFONSUS ENRICO SOEBIJANTO mengirimkan dana sebesar Rp500.000 ke ShopeePay-mu melalui BI-Fast.")
    assert p5.is_financial is True
    assert p5.amount == 500000

    # 6. Stockbit
    p6 = parse_notification("com.stockbit.android", "Order Matched", "Pembelian 5 lot BBRI match di harga Rp3.350")
    assert p6.is_financial is True
    assert p6.amount == 5 * 100 * 3350

