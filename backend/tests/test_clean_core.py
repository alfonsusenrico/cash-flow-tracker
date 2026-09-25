from datetime import date
from unittest.mock import patch
from zoneinfo import ZoneInfo
from fastapi.testclient import TestClient

from app.routers.pulse import get_cycle_window
from app.services.auth import generate_api_key, hash_token

TZ = ZoneInfo("Asia/Jakarta")


def test_cycle_window_calculation_mid_month():
    ref = date(2026, 9, 7)
    start, end, elapsed, total = get_cycle_window(25, ref)
    assert start.astimezone(TZ).date() == date(2026, 8, 25)
    assert end.astimezone(TZ).date() == date(2026, 9, 24)
    assert total == 31
    assert elapsed == 14


def test_cycle_window_calculation_post_payday():
    ref = date(2026, 9, 26)
    start, end, elapsed, total = get_cycle_window(25, ref)
    assert start.astimezone(TZ).date() == date(2026, 9, 25)
    assert end.astimezone(TZ).date() == date(2026, 10, 24)
    assert total == 30
    assert elapsed == 2


def test_cycle_window_calculation_exact_payday():
    ref = date(2026, 9, 25)
    start, end, elapsed, total = get_cycle_window(25, ref)
    assert start.astimezone(TZ).date() == date(2026, 9, 25)
    assert elapsed == 1


def test_cycle_window_first_of_month_payday():
    ref = date(2026, 9, 15)
    start, end, elapsed, total = get_cycle_window(1, ref)
    assert start.astimezone(TZ).date() == date(2026, 9, 1)
    assert end.astimezone(TZ).date() == date(2026, 9, 30)
    assert total == 30
    assert elapsed == 15


def test_api_key_generation_and_hashing():
    plain, key_hash, prefix = generate_api_key()
    assert plain.startswith("cfk_")
    assert prefix == plain[:8]
    assert hash_token(plain) == key_hash


def test_healthcheck_endpoint():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        with TestClient(app) as client:
            res = client.get("/api/health")
            assert res.status_code == 200
            assert res.json()["ok"] is True
            assert res.json()["status"] == "healthy"

            # Check health directly and via v1
            res_direct = client.get("/health")
            assert res_direct.status_code == 200


def test_api_key_info_and_v1_routing():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "11111111-1111-1111-1111-111111111111",
            "username": "tester",
            "currency": "IDR",
            "payday_day": 25,
        }

        app.dependency_overrides[get_current_user] = lambda: mock_user

        with TestClient(app) as client:
            # test /api-key/info via /api and /v1
            res_api = client.get("/api/auth/api-key/info")
            assert res_api.status_code == 200
            assert res_api.json()["username"] == "tester"

            res_v1 = client.post("/v1/auth/api-key/info")
            assert res_v1.status_code == 200
            assert res_v1.json()["currency"] == "IDR"

        app.dependency_overrides.clear()


def test_account_color_display_order_and_reordering():
    from unittest.mock import MagicMock

    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "11111111-1111-1111-1111-111111111111",
            "username": "tester",
            "currency": "IDR",
            "payday_day": 25,
        }

        app.dependency_overrides[get_current_user] = lambda: mock_user

        acc1_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        acc2_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

        with patch("app.routers.accounts.db_conn") as mock_conn:
            cur = MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

            # 1. Test create account with color and display_order
            cur.fetchone.side_effect = [
                None,  # name exists check
                {
                    "id": acc1_id,
                    "parent_id": None,
                    "name": "BCA Utama",
                    "type": "bank",
                    "initial_balance": 1000000,
                    "instrument_type": None,
                    "instrument_symbol": None,
                    "units": None,
                    "avg_buy_price": None,
                    "last_price": None,
                    "last_price_at": None,
                    "color": "#2563EB",
                    "display_order": 0,
                    "is_archived": False,
                    "created_at": None,
                },
            ]

            with TestClient(app) as client:
                res_create = client.post(
                    "/api/accounts",
                    json={
                        "name": "BCA Utama",
                        "type": "bank",
                        "initial_balance": 1000000,
                        "color": "#2563EB",
                        "display_order": 0,
                    },
                )
                assert res_create.status_code == 200
                data = res_create.json()
                assert data["ok"] is True
                assert data["account"]["color"] == "#2563EB"
                assert data["account"]["display_order"] == 0

            # 2. Test get accounts listing with color and display_order
            cur.fetchall.return_value = [
                {
                    "id": acc1_id,
                    "parent_id": None,
                    "name": "BCA Utama",
                    "type": "bank",
                    "initial_balance": 1000000,
                    "instrument_type": None,
                    "instrument_symbol": None,
                    "units": None,
                    "avg_buy_price": None,
                    "last_price": None,
                    "last_price_at": None,
                    "color": "#2563EB",
                    "display_order": 0,
                    "is_archived": False,
                    "created_at": None,
                    "ledger_balance": 1000000,
                },
                {
                    "id": acc2_id,
                    "parent_id": None,
                    "name": "Jago Poket",
                    "type": "bank",
                    "initial_balance": 500000,
                    "instrument_type": None,
                    "instrument_symbol": None,
                    "units": None,
                    "avg_buy_price": None,
                    "last_price": None,
                    "last_price_at": None,
                    "color": "#10B981",
                    "display_order": 1,
                    "is_archived": False,
                    "created_at": None,
                    "ledger_balance": 500000,
                },
            ]

            with TestClient(app) as client:
                res_list = client.get("/api/accounts")
                assert res_list.status_code == 200
                accounts = res_list.json()["accounts"]
                assert len(accounts) == 2
                assert accounts[0]["name"] == "BCA Utama"
                assert accounts[0]["color"] == "#2563EB"
                assert accounts[0]["display_order"] == 0
                assert accounts[1]["name"] == "Jago Poket"
                assert accounts[1]["color"] == "#10B981"
                assert accounts[1]["display_order"] == 1

            # 3. Test reorder endpoint
            with TestClient(app) as client:
                res_reorder = client.post(
                    "/api/accounts/reorder",
                    json={"account_ids": [acc2_id, acc1_id]},
                )
                assert res_reorder.status_code == 200
                assert res_reorder.json()["ok"] is True
                assert cur.execute.call_count >= 2

            # 4. Test update account color and display_order
            cur.fetchone.side_effect = None
            cur.fetchone.return_value = {
                "id": acc1_id,
                "parent_id": None,
                "type": "bank",
                "instrument_type": None,
                "default_pocket_id": None,
                "default_funding_account_id": None,
            }
            cur.fetchall.return_value = [
                {
                    "id": acc1_id,
                    "parent_id": None,
                    "name": "BCA Utama",
                    "type": "bank",
                    "initial_balance": 1000000,
                    "instrument_type": None,
                    "instrument_symbol": None,
                    "units": None,
                    "avg_buy_price": None,
                    "last_price": None,
                    "last_price_at": None,
                    "color": "#7C3AED",
                    "display_order": 3,
                    "is_archived": False,
                    "created_at": None,
                    "ledger_balance": 1000000,
                }
            ]
            with TestClient(app) as client:
                res_update = client.patch(
                    f"/api/accounts/{acc1_id}",
                    json={"color": "#7C3AED", "display_order": 3},
                )
                assert res_update.status_code == 200
                assert res_update.json()["ok"] is True
                assert res_update.json()["account"]["color"] == "#7C3AED"
                assert res_update.json()["account"]["display_order"] == 3

        app.dependency_overrides.clear()


def test_investment_valuation_updates_without_transactions():
    """Verify that updating an investment account valuation does NOT insert transactions."""
    import uuid
    from fastapi.testclient import TestClient
    from app.main import app
    from app.routers.auth import get_current_user
    from unittest.mock import patch, MagicMock

    user_id = str(uuid.uuid4())
    acc_id = str(uuid.uuid4())
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id,
        "username": "tester",
        "currency": "IDR",
        "payday_day": 25,
    }

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with patch("app.routers.accounts.db_conn", return_value=mock_conn):
        mock_cur.fetchone.return_value = {
            "id": acc_id,
            "type": "investment",
            "instrument_type": "mutual_fund",
            "instrument_symbol": "RD-TEST",
            "units": 100.0,
            "avg_buy_price": 1000,
            "initial_balance": 100000,
        }
        mock_cur.fetchall.return_value = [
            {
                "id": acc_id,
                "parent_id": None,
                "name": "Bibit Reksadana",
                "type": "investment",
                "initial_balance": 100000,
                "instrument_type": "mutual_fund",
                "instrument_symbol": "RD-TEST",
                "units": 100.0,
                "avg_buy_price": 1000,
                "last_price": 1200,
                "last_price_at": None,
                "color": "#3B82F6",
                "display_order": 0,
                "is_archived": False,
                "created_at": None,
                "ledger_balance": 100000,
            }
        ]

        with TestClient(app) as client:
            res = client.post(
                f"/api/accounts/{acc_id}/valuation",
                json={"current_balance": 120000, "cost_basis": 100000},
            )
            assert res.status_code == 200
            assert res.json()["ok"] is True

            # Verify no INSERT INTO transactions was executed!
            for call in mock_cur.execute.call_args_list:
                sql = call[0][0]
                assert "INSERT INTO transactions" not in sql, f"Valuation must not insert transactions: {sql}"

    app.dependency_overrides.clear()


def test_user_settings_and_currency_rates():
    """Verify that updating user settings persists emergency_fund_multiplier, monthly_spending_budget, and currency rates are served."""
    import uuid
    from fastapi.testclient import TestClient
    from app.main import app
    from app.routers.auth import get_current_user
    from unittest.mock import patch, MagicMock

    user_id = str(uuid.uuid4())
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id,
        "username": "tester",
        "name": "Tester User",
        "currency": "IDR",
        "payday_day": 25,
        "emergency_fund_multiplier": 6,
        "monthly_spending_budget": 5000000,
    }

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with patch("app.main.init_db_schema"), patch("app.routers.auth.db_conn", return_value=mock_conn):
        mock_cur.fetchone.return_value = {
            "id": user_id,
            "username": "tester",
            "name": "Enrico Updated",
            "payday_day": 28,
            "currency": "USD",
            "emergency_fund_multiplier": 8,
            "monthly_spending_budget": 12000000,
        }

        with TestClient(app) as client:
            res_patch = client.patch(
                "/api/auth/settings",
                json={
                    "name": "Enrico Updated",
                    "payday_day": 28,
                    "currency": "USD",
                    "emergency_fund_multiplier": 8,
                    "monthly_spending_budget": 12000000,
                },
            )
            assert res_patch.status_code == 200
            data = res_patch.json()
            assert data["ok"] is True
            assert data["user"]["emergency_fund_multiplier"] == 8
            assert data["user"]["monthly_spending_budget"] == 12000000
            assert data["user"]["currency"] == "USD"

            res_rates = client.get("/api/auth/currency/rates")
            assert res_rates.status_code == 200
            rates_json = res_rates.json()
            assert rates_json["ok"] is True
            assert "rates" in rates_json
            assert "USD" in rates_json["rates"]

    app.dependency_overrides.clear()


