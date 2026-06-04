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


@pytest.mark.asyncio
async def test_llm_planner_user_preferences():
    # Mock OpenRouter/OpenAI client
    mock_client = MagicMock()
    mock_completions = AsyncMock()
    mock_client.chat = MagicMock()
    mock_client.chat.completions = MagicMock()
    mock_client.chat.completions.create = mock_completions

    # Setup mock responses
    # Turn 1: LLM decides to call update_user_preferences
    mock_tc = MagicMock()
    mock_tc.id = "call_pref123"
    mock_tc.function = MagicMock()
    mock_tc.function.name = "update_user_preferences"
    mock_tc.function.arguments = json.dumps({"preferences_content": "- Treat Kopi Latte as transfer from BCA to Cash"})

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
    mock_msg2.content = "Preferences updated."
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

    # Define a tool executor for update_user_preferences
    pref_content_received = None
    async def mock_pref_executor(preferences_content):
        nonlocal pref_content_received
        pref_content_received = preferences_content
        return json.dumps({"success": True})

    tool_executors = {
        "update_user_preferences": mock_pref_executor
    }

    # Run proposal with user_preferences
    test_prefs = "- Treat all coffee as Food category"
    result = await planner.propose(
        message_text="Mulai sekarang kopi catat sebagai Makan & Minum",
        now_iso="2026-06-01T10:00:00+07:00",
        timezone="Asia/Jakarta",
        accounts=[],
        categories=[],
        tool_executors=tool_executors,
        user_preferences=test_prefs
    )

    # Assertions
    assert result == "Preferences updated."
    assert pref_content_received == "- Treat Kopi Latte as transfer from BCA to Cash"
    assert mock_completions.call_count == 2
    
    # Assert that system prompt passed to completions had user preferences
    call_args = mock_completions.call_args_list[0]
    called_kwargs = call_args.kwargs
    system_message = called_kwargs["messages"][0]
    assert system_message["role"] == "system"
    assert "User Preferences" in system_message["content"]
    assert test_prefs in system_message["content"]


@pytest.mark.asyncio
async def test_llm_planner_two_step_vision_enabled():
    mock_client = MagicMock()
    mock_completions = AsyncMock()
    mock_client.chat = MagicMock()
    mock_client.chat.completions = MagicMock()
    mock_client.chat.completions.create = mock_completions

    # First call: Vision extraction (no tools)
    mock_vision_msg = MagicMock()
    mock_vision_msg.content = "Bakso Rp30,000"
    mock_vision_msg.tool_calls = None
    mock_vision_resp = MagicMock()
    mock_vision_resp.choices = [MagicMock(message=mock_vision_msg)]
    mock_vision_resp.usage = None

    # Second call: Text model agentic step (final response)
    mock_text_msg = MagicMock()
    mock_text_msg.content = "Mencatat bakso 30rb."
    mock_text_msg.tool_calls = None
    mock_text_resp = MagicMock()
    mock_text_resp.choices = [MagicMock(message=mock_text_msg)]
    mock_text_resp.usage = MagicMock(prompt_tokens=100, completion_tokens=20)
    mock_text_resp.usage.prompt_tokens_details = None

    mock_completions.side_effect = [mock_vision_resp, mock_text_resp]

    with patch("bot.llm.AsyncOpenAI", return_value=mock_client):
        planner = LLMPlanner(
            api_key="fake-key",
            base_url="https://fake.url",
            model="text-model",
            vision_model="vision-model",
            use_two_step_vision=True
        )

    result, tool_summary = await planner.propose(
        message_text="Simpan struk ini",
        now_iso="2026-06-01T10:00:00+07:00",
        timezone="Asia/Jakarta",
        accounts=[],
        categories=[],
        image_bytes=b"fake-image-bytes",
    )

    assert result == "Mencatat bakso 30rb."
    assert mock_completions.call_count == 2

    # Assert model parameter for the two calls
    # Call 1 (vision): model="vision-model", tools=None, tool_choice=None
    call1_args = mock_completions.call_args_list[0]
    assert call1_args.kwargs["model"] == "vision-model"
    assert "tools" not in call1_args.kwargs or call1_args.kwargs["tools"] is None

    # Call 2 (text agentic loop): model="text-model"
    call2_args = mock_completions.call_args_list[1]
    assert call2_args.kwargs["model"] == "text-model"
    
    # Verify that the extracted text was injected into messages of the second call
    user_message = call2_args.kwargs["messages"][-1]
    assert user_message["role"] == "user"
    assert "Bakso Rp30,000" in user_message["content"][0]["text"]


@pytest.mark.asyncio
async def test_llm_planner_two_step_vision_disabled():
    mock_client = MagicMock()
    mock_completions = AsyncMock()
    mock_client.chat = MagicMock()
    mock_client.chat.completions = MagicMock()
    mock_client.chat.completions.create = mock_completions

    # Vision model handles the loop directly (legacy behavior)
    mock_msg = MagicMock()
    mock_msg.content = "Mencatat bakso 30rb via vision model."
    mock_msg.tool_calls = None
    mock_resp = MagicMock()
    mock_resp.choices = [MagicMock(message=mock_msg)]
    mock_resp.usage = MagicMock(prompt_tokens=100, completion_tokens=20)
    mock_resp.usage.prompt_tokens_details = None

    mock_completions.side_effect = [mock_resp]

    with patch("bot.llm.AsyncOpenAI", return_value=mock_client):
        planner = LLMPlanner(
            api_key="fake-key",
            base_url="https://fake.url",
            model="text-model",
            vision_model="vision-model",
            use_two_step_vision=False
        )

    result, tool_summary = await planner.propose(
        message_text="Simpan struk ini",
        now_iso="2026-06-01T10:00:00+07:00",
        timezone="Asia/Jakarta",
        accounts=[],
        categories=[],
        image_bytes=b"fake-image-bytes",
    )

    assert result == "Mencatat bakso 30rb via vision model."
    assert mock_completions.call_count == 1

    # Assert vision model was used for the main loop call
    call_args = mock_completions.call_args_list[0]
    assert call_args.kwargs["model"] == "vision-model"

