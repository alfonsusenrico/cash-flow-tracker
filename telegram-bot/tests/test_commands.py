import pytest
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
