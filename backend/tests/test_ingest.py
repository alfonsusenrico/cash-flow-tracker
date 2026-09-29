from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.auth import get_current_user
from app.services.notification_evidence import collect_facts, EvidenceError
from app.services.notification_resolution import observed_endpoint, pocket_endpoint
from evaluation.notification_cases import synthetic_cases


def route_payload(hash_value="synthetic-payload"):
    return {
        "device_id": "synthetic-device", "package_name": "com.jago.digitalbanking",
        "body_text": "You've paid Rp125.000 to Kedai Awan",
        "post_time": "2026-09-28T10:00:27Z", "payload_hash": hash_value,
    }


def test_ingest_notifications_success():
    user_id = str(uuid4())
    app.dependency_overrides[get_current_user] = lambda: {"id": user_id}
    result = {"payload_hash": "synthetic-payload", "event_id": str(uuid4()), "status": "recorded"}
    with patch("app.services.notification_processing.accept_event", return_value=(result, True, 1)) as accept:
        response = TestClient(app).post("/api/ingest/notifications", json={"events": [route_payload()]})
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] and data["received"] == data["inserted"] == data["created_transactions"] == 1
    assert data["updated"] == 0 and data["results"] == [result]
    assert accept.call_args.args[1] == user_id
    assert accept.call_args.args[0]["post_time"].second == 27


def test_ingest_notifications_deduplicate_update():
    app.dependency_overrides[get_current_user] = lambda: {"id": str(uuid4())}
    result = {"payload_hash": "synthetic-payload", "event_id": str(uuid4()), "status": "queued"}
    with patch("app.services.notification_processing.accept_event", return_value=(result, False, 0)):
        response = TestClient(app).post("/api/ingest/notifications", json={"events": [route_payload()]})
    data = response.json()
    assert data["ok"] and data["inserted"] == data["created_transactions"] == 0
    assert data["updated"] == 1 and data["results"] == [result]
    assert "record_key" not in data["results"][0]


def test_batch_failure_preserves_other_events_and_input_order():
    app.dependency_overrides[get_current_user] = lambda: {"id": str(uuid4())}
    recorded = {"payload_hash": "first", "event_id": str(uuid4()), "status": "recorded"}
    queued = {"payload_hash": "last", "event_id": str(uuid4()), "status": "queued"}
    with patch("app.services.notification_processing.accept_event",
               side_effect=[(recorded, True, 1), psycopg.OperationalError("synthetic failure"), (queued, True, 0)]):
        response = TestClient(app).post("/api/ingest/notifications", json={
            "events": [route_payload("first"), route_payload("failed"), route_payload("last")]
        })
    data = response.json()
    assert response.status_code == 200 and data["ok"] is False
    assert data["inserted"] == 2 and data["created_transactions"] == 1
    assert [item["payload_hash"] for item in data["results"]] == ["first", "failed", "last"]
    assert data["results"][1] == {"payload_hash": "failed", "status": "failed", "error_code": "acceptance_failed"}


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


@pytest.mark.parametrize("case_name", [
    "jago-custom", "jago-into", "jago-out",
])
def test_jago_resolves_only_registered_pockets(case_name):
    cases = {case.name: case for case in synthetic_cases()}
    case = cases[case_name]
    facts = collect_facts(case.event)
    parent = next(row for row in case.context["accounts"] if row["name"] == "Bank Jago")
    assert str(pocket_endpoint(facts.parsed.source_pocket, parent, case.context["accounts"], institution="jago")["id"]) == case.expected["source_account_id"]
    assert str(pocket_endpoint(facts.parsed.target_pocket, parent, case.context["accounts"], institution="jago")["id"]) == case.expected["target_account_id"]


def test_unknown_pocket_is_not_a_substring_fallback():
    accounts = [
        {"id": "parent", "name": "Bank Jago", "parent_id": None},
        {"id": "food", "name": "Dana Makan", "parent_id": "parent"},
    ]
    with pytest.raises(EvidenceError, match="uncertain_pocket_mapping"):
        pocket_endpoint("Dana", accounts[0], accounts, institution="jago")


def test_ingest_supported_apps_scope_verification():
    """Verify legacy parser coverage for supported packages; trusted application validates owned configuration separately."""
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
