import pytest
from unittest.mock import AsyncMock, MagicMock
from bot.finance_client import FinanceClient


@pytest.mark.asyncio
async def test_finance_client_upsert_transaction_mapping():
    mock_http = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'{"ok": true, "transaction_id": "tx-123"}'
    mock_response.json.return_value = {"ok": True, "transaction_id": "tx-123"}
    mock_http.request = AsyncMock(return_value=mock_response)

    client = FinanceClient("http://test-server:8000", mock_http)

    # Legacy payload format (credit/debit + transaction_name)
    payload = {
        "transaction_type": "credit",
        "amount": 25000,
        "transaction_name": "Kopi Susu",
        "account_id": "acc-1",
        "category_id": "cat-1",
        "date": "2026-09-07T10:00:00Z",
    }

    res = await client.upsert_transaction("test-key", payload)
    assert res["ok"] is True
    assert res["transaction_id"] == "tx-123"

    mock_http.request.assert_called_once()
    args, kwargs = mock_http.request.call_args
    assert args[0] == "POST"
    assert "/api/transactions" in args[1]
    assert kwargs["headers"]["Authorization"] == "Bearer test-key"
    assert kwargs["json"]["type"] == "expense"
    assert kwargs["json"]["amount"] == 25000
    assert kwargs["json"]["notes"] == "Kopi Susu"


@pytest.mark.asyncio
async def test_finance_client_create_movement_mapping():
    mock_http = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'{"ok": true, "transaction_id": "tx-transfer-1"}'
    mock_response.json.return_value = {"ok": True, "transaction_id": "tx-transfer-1"}
    mock_http.request = AsyncMock(return_value=mock_response)

    client = FinanceClient("http://test-server:8000", mock_http)

    payload = {
        "amount": 100000,
        "source_account_id": "acc-from",
        "target_account_id": "acc-to",
        "notes": "ATM withdrawal",
    }

    res = await client.create_movement("test-key", payload)
    assert res["ok"] is True
    assert res["transfer_id"] == "tx-transfer-1"

    mock_http.request.assert_called_once()
    args, kwargs = mock_http.request.call_args
    assert args[0] == "POST"
    assert "/api/transactions" in args[1]
    assert kwargs["json"]["type"] == "transfer"
    assert kwargs["json"]["amount"] == 100000
    assert kwargs["json"]["account_id"] == "acc-from"
    assert kwargs["json"]["transfer_target_account_id"] == "acc-to"


@pytest.mark.asyncio
async def test_finance_client_list_accounts_aliases():
    mock_http = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'{"ok": true, "accounts": [{"id": "acc-1", "name": "BCA", "balance": 500000}]}'
    mock_response.json.return_value = {
        "ok": True,
        "accounts": [{"id": "acc-1", "name": "BCA", "balance": 500000}],
    }
    mock_http.request = AsyncMock(return_value=mock_response)

    client = FinanceClient("http://test-server:8000", mock_http)
    accounts = await client.list_accounts("test-key")

    assert len(accounts) == 1
    acc = accounts[0]
    # Check both clean and legacy alias fields
    assert acc["id"] == "acc-1"
    assert acc["account_id"] == "acc-1"
    assert acc["name"] == "BCA"
    assert acc["account_name"] == "BCA"
    assert acc["balance"] == 500000
    assert acc["current_balance"] == 500000
