import pytest
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch, mock_open
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
        telegram_webhook_secret="fake-secret"
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
async def test_clear_command(mock_app):
    # Setup mock update and context
    mock_update = MagicMock()
    mock_update.effective_user = MagicMock(id=12345)
    mock_update.message = MagicMock()
    mock_update.message.reply_text = AsyncMock()

    mock_context = MagicMock()

    # Setup store mock
    mock_app.store.clear_chat_history = AsyncMock()

    # Run command
    await mock_app.clear_command(mock_update, mock_context)

    # Assertions
    mock_app.store.clear_chat_history.assert_called_once_with(12345)
    mock_update.message.reply_text.assert_called_once_with(
        "🧹 Riwayat percakapan Anda telah dibersihkan. Silakan mulai percakapan baru!"
    )


@pytest.mark.asyncio
async def test_handle_message_user_preferences(mock_app):
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
    mock_app.store.add_chat_history = AsyncMock()
    mock_app.finance.list_accounts = AsyncMock(return_value=[])
    mock_app.finance.list_categories = AsyncMock(return_value=[])
    mock_app.llm.propose = AsyncMock(return_value="Hello user!")

    # We will patch os.path.exists to return True for our specific user pref path,
    # and mock open to return custom preference text.
    def mock_exists(path):
        if "user_12345.md" in path:
            return True
        return False

    mock_open_read = mock_open(read_data="- Avoid using credit cards")

    with patch("os.path.exists", side_effect=mock_exists), \
         patch("builtins.open", mock_open_read):
        await mock_app.handle_message(mock_update, mock_context)

    # Verify that llm.propose was called with the preferences we provided
    mock_app.llm.propose.assert_called_once()
    called_kwargs = mock_app.llm.propose.call_args.kwargs
    assert called_kwargs["user_preferences"] == "- Avoid using credit cards"


@pytest.mark.asyncio
async def test_handle_message_update_preferences_tool(mock_app):
    # Setup mock update and context
    mock_update = MagicMock()
    mock_update.effective_user = MagicMock(id=12345)
    mock_update.effective_chat = MagicMock(id=67890)
    mock_update.message = MagicMock()
    mock_update.message.text = "Set preference"
    mock_update.message.photo = None
    mock_update.message.reply_text = AsyncMock()

    mock_context = MagicMock()

    # Mock store and client methods
    mock_app.store.get_api_key = AsyncMock(return_value="fake-api-key")
    mock_app.store.get_chat_history = AsyncMock(return_value=[])
    mock_app.store.add_chat_history = AsyncMock()
    mock_app.finance.list_accounts = AsyncMock(return_value=[])
    mock_app.finance.list_categories = AsyncMock(return_value=[])
    mock_app.llm.propose = AsyncMock(return_value="Preferences saved!")

    # Capture the tool executor dictionary passed to propose
    tool_executors_captured = None
    async def capture_propose(*args, **kwargs):
        nonlocal tool_executors_captured
        tool_executors_captured = kwargs.get("tool_executors")
        return "Preferences saved!"
    
    mock_app.llm.propose.side_effect = capture_propose

    # Run handle_message
    with patch("os.path.exists", return_value=False):
        await mock_app.handle_message(mock_update, mock_context)

    # Assert that propose was called and we captured tool_executors
    assert tool_executors_captured is not None
    assert "update_user_preferences" in tool_executors_captured

    # Execute the update_user_preferences executor and verify it writes the file
    mock_open_write = mock_open()
    with patch("builtins.open", mock_open_write), \
         patch("os.makedirs"):
        executor = tool_executors_captured["update_user_preferences"]
        res = await executor(preferences_content="- Always format as list")
        
        # Verify success
        res_json = json.loads(res)
        assert res_json.get("success") is True

        # Verify it opened the correct file for writing
        mock_open_write.assert_called_once_with(
            "/app/storage/user_preferences/user_12345.md", "w", encoding="utf-8"
        )
        mock_open_write().write.assert_called_once_with("- Always format as list")
