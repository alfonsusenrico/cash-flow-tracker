import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from bot.llm import LLMPlanner

@pytest.mark.asyncio
async def test_llm_planner_agentic_loop():
    # Mock OpenRouter/OpenAI client
    mock_client = MagicMock()
    mock_completions = AsyncMock()
    mock_client.chat = MagicMock()
    mock_client.chat.completions = MagicMock()
    mock_client.chat.completions.create = mock_completions

    # Setup mock responses
    # Turn 1: LLM decides to call get_account_balance
    mock_tc = MagicMock()
    mock_tc.id = "call_abc123"
    mock_tc.function = MagicMock()
    mock_tc.function.name = "get_account_balance"
    mock_tc.function.arguments = json.dumps({"account_name": "ATM BCA"})

    mock_msg1 = MagicMock()
    mock_msg1.content = None
    mock_msg1.tool_calls = [mock_tc]

    mock_choice1 = MagicMock()
    mock_choice1.message = mock_msg1

    mock_resp1 = MagicMock()
    mock_resp1.choices = [mock_choice1]
    mock_resp1.usage = MagicMock(prompt_tokens=100, completion_tokens=20)
    mock_resp1.usage.prompt_tokens_details = None

    # Turn 2: LLM finishes and returns natural language text
    mock_msg2 = MagicMock()
    mock_msg2.content = "Balance of ATM BCA is Rp5,000,000."
    mock_msg2.tool_calls = None

    mock_choice2 = MagicMock()
    mock_choice2.message = mock_msg2

    mock_resp2 = MagicMock()
    mock_resp2.choices = [mock_choice2]
    mock_resp2.usage = MagicMock(prompt_tokens=150, completion_tokens=15)
    mock_resp2.usage.prompt_tokens_details = None

    # Make the mock completions call return first response, then second response
    mock_completions.side_effect = [mock_resp1, mock_resp2]

    # Initialize LLMPlanner with mock client patch
    with patch("bot.llm.AsyncOpenAI", return_value=mock_client):
        planner = LLMPlanner(api_key="fake-key", base_url="https://fake.url", model="fake-model")

    # Define a tool executor
    tool_executed = False
    async def mock_executor(account_name):
        nonlocal tool_executed
        tool_executed = True
        return json.dumps({"account_name": account_name, "balance": 5000000})

    tool_executors = {
        "get_account_balance": mock_executor
    }

    # Run proposal
    result = await planner.propose(
        message_text="Berapa saldo BCA?",
        now_iso="2026-06-01T10:00:00+07:00",
        timezone="Asia/Jakarta",
        accounts=[{"account_name": "ATM BCA", "profile_type": "checking", "balance": 5000000}],
        categories=[],
        tool_executors=tool_executors
    )

    # Assertions
    assert result == "Balance of ATM BCA is Rp5,000,000."
    assert tool_executed is True
    assert mock_completions.call_count == 2
