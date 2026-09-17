import datetime
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from fastapi.testclient import TestClient

from app.routers.accounts import get_accounts_with_balances
from app.services.market_data import (
    get_instrument_quote,
    search_instruments,
    _map_yahoo_quote_type,
)


def test_map_yahoo_quote_type():
    assert _map_yahoo_quote_type("EQUITY", "BBCA.JK") == "stock"
    assert _map_yahoo_quote_type("COMMODITY", "GC=F") == "gold"
    assert _map_yahoo_quote_type("CRYPTOCURRENCY", "BTC-USD") == "crypto"
    assert _map_yahoo_quote_type("ETF", "SPY") == "mutual_fund"
    assert _map_yahoo_quote_type("UNKNOWN", "XYZ") == "other"


@pytest.mark.asyncio
async def test_search_instruments_sorting():
    mock_yahoo_response = {
        "quotes": [
            {"symbol": "BBCA", "shortname": "BetaBuilders", "quoteType": "ETF", "exchange": "BTS"},
            {"symbol": "BBCA.JK", "shortname": "Bank Central Asia", "quoteType": "EQUITY", "exchange": "JKT"},
            {"symbol": "IBKD.SI", "shortname": "i BBCA ID", "quoteType": "EQUITY", "exchange": "SES"},
        ]
    }
    with patch("app.services.market_data._http_get_json", return_value=mock_yahoo_response):
        results = await search_instruments("BBC", limit=5)
        assert len(results) == 3
        # BBCA.JK should be prioritized before other non-exact matches
        assert results[0]["symbol"] == "BBCA.JK"
        assert results[0]["type"] == "stock"
        assert results[0]["exchange"] == "JKT"


@pytest.mark.asyncio
async def test_get_instrument_quote_idr():
    mock_chart_response = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "currency": "IDR",
                        "regularMarketPrice": 6325.0,
                        "regularMarketTime": 1789118000,
                    }
                }
            ]
        }
    }
    with patch("app.services.market_data._http_get_json", return_value=mock_chart_response):
        quote = await get_instrument_quote("BBCA.JK")
        assert quote is not None
        assert quote["symbol"] == "BBCA.JK"
        assert quote["price"] == 6325
        assert quote["currency"] == "IDR"


@pytest.mark.asyncio
async def test_get_instrument_quote_usd_conversion():
    mock_chart_response = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "currency": "USD",
                        "regularMarketPrice": 100.0,
                        "regularMarketTime": 1789118000,
                    }
                }
            ]
        }
    }
    with patch("app.services.market_data._http_get_json", return_value=mock_chart_response), \
         patch("app.services.market_data.get_usdidr_rate", return_value=16000.0):
        quote = await get_instrument_quote("AAPL")
        assert quote is not None
        assert quote["symbol"] == "AAPL"
        # 100 USD * 16000 IDR/USD = 1,600,000 IDR
        assert quote["price"] == 1600000
        assert quote["currency"] == "IDR"
        assert quote["original_currency"] == "USD"


def test_account_capital_gain_calculation_mode_a_positive_gain():
    mock_rows = [
        {
            "id": "11111111-1111-1111-1111-111111111111",
            "parent_id": None,
            "name": "BBCA Stock",
            "type": "investment",
            "initial_balance": 6000000,
            "instrument_type": "stock",
            "instrument_symbol": "BBCA.JK",
            "units": 1000.0,
            "avg_buy_price": 6000,
            "last_price": 6325,
            "last_price_at": datetime.datetime.now(datetime.timezone.utc),
            "is_archived": False,
            "created_at": datetime.datetime.now(datetime.timezone.utc),
            "ledger_balance": 6000000,
        }
    ]

    with patch("app.routers.accounts.db_conn") as mock_conn:
        cur = MagicMock()
        cur.fetchall.return_value = mock_rows
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        accounts = get_accounts_with_balances("user-123")
        assert len(accounts) == 1
        acc = accounts[0]
        # Current valuation: 1000 * 6325 = 6,325,000
        assert acc["balance"] == 6325000
        # Cost basis: 1000 * 6000 = 6,000,000
        # Capital gain: 6,325,000 - 6,000,000 = +325,000 (+5.42%)
        assert acc["capital_gain"] == 325000
        assert acc["capital_gain_pct"] == 5.42


def test_account_capital_gain_calculation_mode_a_negative_gain():
    mock_rows = [
        {
            "id": "22222222-2222-2222-2222-222222222222",
            "parent_id": None,
            "name": "Falling Asset",
            "type": "investment",
            "initial_balance": 1000000,
            "instrument_type": "stock",
            "instrument_symbol": "FALL.JK",
            "units": 10.0,
            "avg_buy_price": 100000,
            "last_price": 85000,
            "last_price_at": datetime.datetime.now(datetime.timezone.utc),
            "is_archived": False,
            "created_at": datetime.datetime.now(datetime.timezone.utc),
            "ledger_balance": 1000000,
        }
    ]

    with patch("app.routers.accounts.db_conn") as mock_conn:
        cur = MagicMock()
        cur.fetchall.return_value = mock_rows
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        accounts = get_accounts_with_balances("user-123")
        assert len(accounts) == 1
        acc = accounts[0]
        assert acc["balance"] == 850000
        # Capital loss: 850,000 - 1,000,000 = -150,000 (-15.0%)
        assert acc["capital_gain"] == -150000
        assert acc["capital_gain_pct"] == -15.0


def test_parent_account_capital_gain_aggregation():
    parent_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    mock_rows = [
        {
            "id": parent_id,
            "parent_id": None,
            "name": "Bibit Master",
            "type": "investment",
            "initial_balance": 0,
            "instrument_type": None,
            "instrument_symbol": None,
            "units": None,
            "avg_buy_price": None,
            "last_price": None,
            "last_price_at": None,
            "is_archived": False,
            "created_at": datetime.datetime.now(datetime.timezone.utc),
            "ledger_balance": 0,
        },
        {
            "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "parent_id": parent_id,
            "name": "Bibit RDPU",
            "type": "investment",
            "initial_balance": 10000000,
            "instrument_type": "mutual_fund",
            "instrument_symbol": None,
            "units": None,
            "avg_buy_price": 10000000,
            "last_price": None,
            "last_price_at": None,
            "is_archived": False,
            "created_at": datetime.datetime.now(datetime.timezone.utc),
            "ledger_balance": 10450000,  # +450,000 gain
        },
        {
            "id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "parent_id": parent_id,
            "name": "BBCA Pocket",
            "type": "investment",
            "initial_balance": 6000000,
            "instrument_type": "stock",
            "instrument_symbol": "BBCA.JK",
            "units": 1000.0,
            "avg_buy_price": 6000,
            "last_price": 6325,
            "last_price_at": datetime.datetime.now(datetime.timezone.utc),
            "is_archived": False,
            "created_at": datetime.datetime.now(datetime.timezone.utc),
            "ledger_balance": 6000000,  # 6,325,000 market val -> +325,000 gain
        },
    ]

    with patch("app.routers.accounts.db_conn") as mock_conn:
        cur = MagicMock()
        cur.fetchall.return_value = mock_rows
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        accounts = get_accounts_with_balances("user-123")
        parent = next(a for a in accounts if a["id"] == parent_id)
        assert parent["is_parent"] is True
        assert len(parent["children"]) == 2
        # Total balance = 10,450,000 + 6,325,000 = 16,775,000
        assert parent["balance"] == 16775000
        # Total capital gain = 450,000 + 325,000 = 775,000
        assert parent["capital_gain"] == 775000
        # Total cost = 10,000,000 + 6,000,000 = 16,000,000
        # 775,000 / 16,000,000 = 4.84%
        assert parent["capital_gain_pct"] == 4.84


def test_account_endpoints_instrument_search_and_sync():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {"id": "test-user-id", "username": "testuser", "currency": "IDR", "payday_day": 25}
        app.dependency_overrides[get_current_user] = lambda: mock_user

        with patch("app.routers.accounts.search_instruments", new_callable=AsyncMock) as mock_search, \
             patch("app.routers.accounts.sync_all_tracked_prices", new_callable=AsyncMock) as mock_sync, \
             patch("app.routers.accounts.db_conn") as mock_conn:
            mock_search.return_value = [{"symbol": "BBCA.JK", "name": "PT Bank Central Asia", "type": "stock"}]
            mock_sync.return_value = {"symbols_checked": 1, "accounts_updated": 2}
            mock_conn.return_value.__enter__.return_value = MagicMock()

            with TestClient(app) as client:
                res = client.get("/api/accounts/instruments/search?q=BBC")
                assert res.status_code == 200
                assert res.json()["results"][0]["symbol"] == "BBCA.JK"

                res_sync = client.post("/api/accounts/sync-prices")
                assert res_sync.status_code == 200
                assert res_sync.json()["accounts_updated"] == 2

        app.dependency_overrides.clear()


def test_get_accounts_with_default_funding_account():
    stockbit_id = "stockbit-id-123"
    rdn_id = "rdn-bca-id-456"
    mock_rows = [
        {
            "id": rdn_id,
            "parent_id": None,
            "default_funding_account_id": None,
            "default_funding_account_name": None,
            "name": "RDN BCA",
            "type": "bank",
            "initial_balance": 5000000,
            "instrument_type": None,
            "instrument_symbol": None,
            "units": None,
            "avg_buy_price": None,
            "last_price": None,
            "last_price_at": None,
            "is_archived": False,
            "created_at": datetime.datetime.now(datetime.timezone.utc),
            "ledger_balance": 5000000,
        },
        {
            "id": stockbit_id,
            "parent_id": None,
            "default_funding_account_id": rdn_id,
            "default_funding_account_name": "RDN BCA",
            "name": "Stockbit",
            "type": "investment",
            "initial_balance": 0,
            "instrument_type": None,
            "instrument_symbol": None,
            "units": None,
            "avg_buy_price": None,
            "last_price": None,
            "last_price_at": None,
            "is_archived": False,
            "created_at": datetime.datetime.now(datetime.timezone.utc),
            "ledger_balance": 0,
        },
    ]

    with patch("app.routers.accounts.db_conn") as mock_conn:
        cur = MagicMock()
        cur.fetchall.return_value = mock_rows
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        accounts = get_accounts_with_balances("user-123")
        stockbit = next(a for a in accounts if a["id"] == stockbit_id)
        assert stockbit["default_funding_account_id"] == rdn_id
        assert stockbit["default_funding_account_name"] == "RDN BCA"


def test_record_investment_buy_trade_accumulates_units_and_weighted_average_price():
    from uuid import uuid4
    from app.main import app
    from app.services.auth import get_current_user

    mock_user = {"id": "user-trade-123", "username": "trader", "currency": "IDR", "payday_day": 25}
    app.dependency_overrides[get_current_user] = lambda: mock_user

    funding_id = str(uuid4())
    bbri_id = str(uuid4())

    with patch("app.routers.transactions.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        # Validate funding account, validate target account, insert tx, fetch invest account
        cur.fetchone.side_effect = [
            {"id": funding_id},  # source account check
            {"id": bbri_id},     # target account check
            {"id": str(uuid4())}, # category Investasi check (optional)
            {"id": str(uuid4()), "created_at": datetime.datetime.now(datetime.timezone.utc)},  # tx insert
            {"id": bbri_id, "units": 1400.0, "avg_buy_price": 3340, "last_price": 3340},  # invest_acc query
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post(
            "/api/transactions",
            json={
                "type": "expense",
                "account_id": funding_id,
                "target_account_name": "BBRI",
                "amount": 1920000,
                "investment_action": "buy",
                "units": 600.0,
                "price_per_unit": 3200.0,
            },
        )
        assert res.status_code == 200

        update_calls = [c for c in cur.execute.call_args_list if "UPDATE accounts" in str(c)]
        assert len(update_calls) == 1
        args = update_calls[0][0][1]
        assert args[0] == 2000.0  # new_units = 1400 + 600
        assert args[1] == 3298    # (1400*3340 + 600*3200) / 2000 = 3298
        assert args[2] == bbri_id

    app.dependency_overrides.clear()


def test_record_investment_sell_trade_decrements_units():
    from uuid import uuid4
    from app.main import app
    from app.services.auth import get_current_user

    mock_user = {"id": "user-trade-123", "username": "trader", "currency": "IDR", "payday_day": 25}
    app.dependency_overrides[get_current_user] = lambda: mock_user

    funding_id = str(uuid4())
    bbri_id = str(uuid4())

    with patch("app.routers.transactions.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchone.side_effect = [
            {"id": bbri_id},      # source account check
            {"id": str(uuid4())},  # Investasi category lookup
            {"id": str(uuid4()), "created_at": datetime.datetime.now(datetime.timezone.utc)},  # tx insert
            {"id": bbri_id, "units": 1400.0, "avg_buy_price": 3340, "last_price": 3500},  # invest_acc query
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post(
            "/api/transactions",
            json={
                "type": "income",
                "account_id": bbri_id,
                "amount": 1750000,
                "investment_action": "sell",
                "units": 500.0,
                "price_per_unit": 3500.0,
            },
        )
        assert res.status_code == 200

        update_calls = [c for c in cur.execute.call_args_list if "UPDATE accounts" in str(c)]
        assert len(update_calls) == 1
        args = update_calls[0][0][1]
        assert args[0] == 900.0  # new_units = 1400 - 500
        assert args[1] == bbri_id

    app.dependency_overrides.clear()


def test_record_investment_buy_trade_by_account_names():
    from uuid import uuid4
    from app.main import app
    from app.services.auth import get_current_user

    mock_user = {"id": "user-trade-123", "username": "trader", "currency": "IDR", "payday_day": 25}
    app.dependency_overrides[get_current_user] = lambda: mock_user

    rdn_id = str(uuid4())
    bbri_id = str(uuid4())
    invest_cat_id = str(uuid4())

    with patch("app.routers.transactions.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        cur.fetchone.side_effect = [
            {"id": str(uuid4()), "created_at": datetime.datetime.now(datetime.timezone.utc)},  # tx insert
            {"id": bbri_id, "units": 1000.0, "avg_buy_price": 3500, "last_price": 3500},  # invest_acc query
        ]
        cur.fetchall.side_effect = [
            [
                {"id": rdn_id, "name": "RDN BCA", "type": "bank", "parent_id": None, "instrument_type": None, "instrument_symbol": None, "default_funding_account_id": None},
                {"id": bbri_id, "name": "BBRI Stock", "type": "investment", "parent_id": None, "instrument_type": "stock", "instrument_symbol": "BBRI.JK", "default_funding_account_id": rdn_id},
            ],
            [
                {"id": invest_cat_id, "name": "Investasi", "kind": "expense", "kakeibo_type": None, "is_primary": False},
            ],
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post(
            "/api/transactions",
            json={
                "type": "expense",
                "account_name": "RDN BCA",
                "target_account_name": "BBRI",
                "amount": 3850000,
                "category_name": "Investasi",
                "investment_action": "buy",
                "units": 1000.0,
                "price_per_unit": 3850.0,
                "notes": "Stockbit: Stockbit BBRI (10 lot @ Rp3.850)",
            },
        )
        assert res.status_code == 200
        assert res.json()["ok"] is True

        # Check account update: old 1000 @ 3500, new +1000 @ 3850 -> 2000 @ 3675
        update_calls = [c for c in cur.execute.call_args_list if "UPDATE accounts" in str(c)]
        assert len(update_calls) == 1
        args = update_calls[0][0][1]
        assert args[0] == 2000.0  # units
        assert args[1] == 3675    # (1000*3500 + 1000*3850) / 2000 = 3675
    app.dependency_overrides.clear()


def test_record_investment_buy_auto_provisions_new_stock_ticker():
    from uuid import uuid4
    from app.main import app
    from app.services.auth import get_current_user

    mock_user = {"id": "user-trade-123", "username": "trader", "currency": "IDR", "payday_day": 25}
    app.dependency_overrides[get_current_user] = lambda: mock_user

    rdn_id = str(uuid4())
    new_stock_id = str(uuid4())
    invest_cat_id = str(uuid4())

    with patch("app.routers.transactions.db_conn") as mock_conn:
        cur = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

        # 1. auto-provision INSERT RETURNING
        # 2. tx insert RETURNING
        # 3. invest_acc query
        cur.fetchone.side_effect = [
            {"id": new_stock_id, "parent_id": None, "name": "TLKM", "type": "investment", "instrument_type": "stock", "instrument_symbol": "TLKM.JK", "default_funding_account_id": rdn_id},
            {"id": str(uuid4()), "created_at": datetime.datetime.now(datetime.timezone.utc)},
            {"id": new_stock_id, "units": 0.0, "avg_buy_price": 0, "last_price": None},
        ]
        cur.fetchall.side_effect = [
            [
                {"id": rdn_id, "name": "RDN BCA", "type": "bank", "parent_id": None, "instrument_type": None, "instrument_symbol": None, "default_funding_account_id": None},
            ],
            [
                {"id": invest_cat_id, "name": "Investasi", "kind": "expense", "kakeibo_type": None, "is_primary": False},
            ],
        ]

        client = TestClient(app, raise_server_exceptions=True)
        res = client.post(
            "/api/transactions",
            json={
                "type": "expense",
                "account_name": "RDN BCA",
                "target_account_name": "TLKM",
                "amount": 3000000,
                "category_name": "Investasi",
                "investment_action": "buy",
                "units": 1000.0,
                "price_per_unit": 3000.0,
            },
        )
        assert res.status_code == 200
        assert res.json()["ok"] is True

        # Check account was provisioned
        provision_calls = [c for c in cur.execute.call_args_list if "INSERT INTO accounts" in str(c)]
        assert len(provision_calls) == 1
        p_args = provision_calls[0][0][1]
        assert p_args[1] == "TLKM"
        assert p_args[2] == "TLKM.JK"

        # Check units updated
        update_calls = [c for c in cur.execute.call_args_list if "UPDATE accounts" in str(c)]
        assert len(update_calls) == 1
        u_args = update_calls[0][0][1]
        assert u_args[0] == 1000.0
        assert u_args[1] == 3000.0
        assert u_args[2] == new_stock_id

    app.dependency_overrides.clear()



