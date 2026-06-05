import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from bot.app import BotApp
from bot.config import Settings

@pytest.fixture
def mock_app():
    settings = Settings(
        telegram_bot_token="fake-token",
        bot_secret="iA6aiaoaiaoaiaoaiaoaiaoaiaoaiaoaiaoaiaoaiao=",
        finance_api_base_url="http://fake.api",
        deepseek_api_key="fake-key",
        deepseek_base_url="http://fake.llm",
        deepseek_model="fake-model",
        confidence_threshold=0.7,
        llm_timeout=30,
        bot_database_url="postgresql://fake",
        webhook_listen="0.0.0.0",
        webhook_port=8080,
        telegram_webhook_url="https://fake.webhook",
        telegram_webhook_secret="fake-secret",
        vision_model="fake-vision-model",
        vision_base_url="http://fake.vision",
        use_two_step_vision=True
    )
    with patch("bot.app.Store") as MockStore, \
         patch("bot.app.FinanceClient") as MockFinance, \
         patch("bot.app.LLMPlanner") as MockLLM:
        app = BotApp(settings)
        app.store = MockStore.return_value
        app.finance = MockFinance.return_value
        app.llm = MockLLM.return_value
        yield app

@pytest.mark.asyncio
async def test_goals_and_obligations_executors(mock_app):
    # Setup mock update and context
    mock_update = MagicMock()
    mock_update.effective_user = MagicMock(id=12345)
    mock_update.effective_chat = MagicMock(id=67890)
    mock_update.message = MagicMock()
    mock_update.message.text = "Hello"
    mock_update.message.photo = None
    mock_update.message.reply_text = AsyncMock()

    mock_context = MagicMock()

    # Mock store and client methods
    mock_app.store.get_api_key = AsyncMock(return_value="fake-api-key")
    mock_app.store.get_chat_history = AsyncMock(return_value=[])
    mock_app.store.get_full_chat_history = AsyncMock(return_value=[])
    mock_app.store.add_chat_history = AsyncMock()
    mock_app.finance.list_accounts = AsyncMock(return_value=[
        {"account_id": "acc-123", "account_name": "ATM BCA"}
    ])
    mock_app.finance.list_categories = AsyncMock(return_value=[])
    mock_app.finance.list_goals = AsyncMock(return_value=[
        {"goal_id": "goal-123", "name": "Tabungan Laptop", "target_amount": 15000000, "status": "active", "notes": "my laptop", "target_date": "2026-12-31"}
    ])
    mock_app.finance.list_obligations = AsyncMock(return_value=[
        {"obligation_id": "ob-123", "title": "Utang Budi", "principal_amount": 5000000, "kind": "payable", "status": "open", "default_account_id": "acc-123"}
    ])
    mock_app.llm.propose = AsyncMock(return_value="Hello user!")

    # Capture the tool executor dictionary passed to propose
    tool_executors_captured = None
    async def capture_propose(*args, **kwargs):
        nonlocal tool_executors_captured
        tool_executors_captured = kwargs.get("tool_executors")
        return "Hello!"
    
    mock_app.llm.propose.side_effect = capture_propose

    # Run handle_message to populate and capture tool executors
    with patch("os.path.exists", return_value=False):
        await mock_app.handle_message(mock_update, mock_context)

    assert tool_executors_captured is not None

    # Verify Goals Executors
    # 1. list_goals
    mock_app.finance.list_goals.reset_mock()
    res = await tool_executors_captured["list_goals"]()
    assert json.loads(res)["success"] is True
    mock_app.finance.list_goals.assert_called_once()

    # 2. create_goal
    mock_app.finance.create_goal = AsyncMock(return_value={"goal_id": "new-goal"})
    res = await tool_executors_captured["create_goal"](name="Mobil Baru", target_amount=200000000, target_date="2027-01-01", notes="buy a car")
    assert json.loads(res)["success"] is True
    mock_app.finance.create_goal.assert_called_once_with("fake-api-key", {
        "name": "Mobil Baru",
        "target_amount": 200000000,
        "target_date": "2027-01-01",
        "notes": "buy a car"
    })

    # 3. update_goal
    mock_app.finance.update_goal = AsyncMock(return_value={"ok": True})
    res = await tool_executors_captured["update_goal"](name="Tabungan Laptop", new_name="Laptop Baru", target_amount=18000000)
    assert json.loads(res)["success"] is True
    mock_app.finance.update_goal.assert_called_once_with("fake-api-key", "goal-123", {
        "name": "Laptop Baru",
        "target_amount": 18000000,
        "target_date": "2026-12-31",
        "notes": "my laptop",
        "status": "active"
    })

    # 4. delete_goal
    mock_app.finance.delete_goal = AsyncMock(return_value={"ok": True})
    res = await tool_executors_captured["delete_goal"](name="Tabungan Laptop")
    assert json.loads(res)["success"] is True
    mock_app.finance.delete_goal.assert_called_once_with("fake-api-key", "goal-123")

    # 5. contribute_goal
    mock_app.finance.contribute_goal = AsyncMock(return_value={"ok": True})
    res = await tool_executors_captured["contribute_goal"](name="Tabungan Laptop", amount=500000, source_account_name="ATM BCA", notes="saving")
    assert json.loads(res)["success"] is True
    mock_app.finance.contribute_goal.assert_called_once_with("fake-api-key", "goal-123", {
        "amount": 500000,
        "source": "manual",
        "notes": "saving (from ATM BCA)"
    })

    # Verify Obligations Executors
    # 6. list_obligations
    mock_app.finance.list_obligations.reset_mock()
    res = await tool_executors_captured["list_obligations"](kind="payable", status="open")
    assert json.loads(res)["success"] is True
    mock_app.finance.list_obligations.assert_called_once_with("fake-api-key", {"kind": "payable", "status": "open"})

    # 7. create_obligation
    mock_app.finance.create_obligation = AsyncMock(return_value={"obligation_id": "new-ob"})
    res = await tool_executors_captured["create_obligation"](
        kind="payable", title="Utang Mobil", principal_amount=150000000, counterparty_name="Dealer", due_date="2028-12-31", default_account_name="ATM BCA", notes="car loan"
    )
    assert json.loads(res)["success"] is True
    mock_app.finance.create_obligation.assert_called_once_with("fake-api-key", {
        "kind": "payable",
        "title": "Utang Mobil",
        "principal_amount": 150000000,
        "counterparty_name": "Dealer",
        "due_date": "2028-12-31",
        "default_account_id": "acc-123",
        "notes": "car loan"
    })

    # 8. update_obligation
    mock_app.finance.update_obligation = AsyncMock(return_value={"ok": True})
    res = await tool_executors_captured["update_obligation"](
        title="Utang Budi", new_title="Utang Budi Edit", principal_amount=6000000
    )
    assert json.loads(res)["success"] is True
    mock_app.finance.update_obligation.assert_called_once_with("fake-api-key", "ob-123", {
        "title": "Utang Budi Edit",
        "principal_amount": 6000000
    })

    # 9. settle_obligation
    mock_app.finance.settle_obligation = AsyncMock(return_value={"ok": True})
    res = await tool_executors_captured["settle_obligation"](
        title="Utang Budi", amount=2000000, source_account_name="ATM BCA", notes="part payment"
    )
    assert json.loads(res)["success"] is True
    mock_app.finance.settle_obligation.assert_called_once_with("fake-api-key", "ob-123", {
        "amount": 2000000,
        "account_id": "acc-123",
        "notes": "part payment"
    })
