"""LLM planner: turns a user message (+optional image) into a structured proposal.

Implements an agentic loop using OpenAI-compatible function calling:
1. LLM receives user context and available tool definitions.
2. If the LLM needs data (e.g. account balance), it calls a tool.
3. Tool results are fed back; the loop continues until the LLM produces a
   final JSON response (or the max iteration limit is reached).

Iteration awareness: the LLM receives its current step/max each round-trip so
it can self-regulate and avoid hitting the hard cutoff fallback.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable

from openai import AsyncOpenAI

from .tools import TOOL_DEFINITIONS

_PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "system_prompt.md"

logger = logging.getLogger(__name__)

# Type alias for async tool functions injected from the app layer.
ToolFunc = Callable[..., Awaitable[str]]


class LLMPlanner:
    MAX_ITERATIONS = 30

    def __init__(self, api_key: str, base_url: str, model: str) -> None:
        self._client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self._model = model
        self._system_prompt = _PROMPT_PATH.read_text(encoding="utf-8")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def propose(
        self,
        *,
        message_text: str,
        now_iso: str,
        timezone: str,
        accounts: list[dict[str, Any]],
        categories: list[dict[str, Any]],
        image_bytes: bytes | None = None,
        image_mime: str = "image/jpeg",
        history: list[dict[str, str]] | None = None,
        timeout: int = 120,
        tool_executors: dict[str, ToolFunc] | None = None,
        user_preferences: str | None = None,
    ) -> str:
        """Run the agentic propose loop.

        The LLM may call tools (get_account_balance, search_transactions, etc.)
        before producing its final response. Each iteration injects an
        awareness hint so the model can self-regulate.
        """
        capped_msg = (message_text[:200] + "...") if message_text and len(message_text) > 200 else message_text
        logger.info(f"LLM propose started. User message (capped): {capped_msg!r}")

        context: dict[str, Any] = {
            "now": now_iso,
            "timezone": timezone,
            "accounts": [
                {
                    "account_name": a.get("account_name"),
                    "profile_type": a.get("profile_type"),
                    "balance": a.get("balance"),
                }
                for a in accounts
            ],
            "categories": [
                {"name": c.get("name"), "kind": c.get("kind")} for c in categories
            ],
            "message": message_text or "",
            "has_media_attached": bool(image_bytes),
        }

        # Build the user content (text + optional image)
        text_part: dict[str, Any] = {
            "type": "text",
            "text": json.dumps(context, ensure_ascii=False),
        }
        if image_bytes:
            b64 = base64.b64encode(image_bytes).decode()
            user_content: Any = [
                text_part,
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:{image_mime};base64,{b64}"},
                },
            ]
        else:
            user_content = [text_part]

        # Seed the message list
        system_prompt = self._system_prompt
        if user_preferences:
            system_prompt += f"\n\n## User Preferences\nFollow these custom rules and preferences specified by the user:\n{user_preferences}"

        messages: list[dict[str, Any]] = [
            {"role": "system", "content": system_prompt}
        ]
        if history:
            logger.info(f"LLM history: {json.dumps(history, ensure_ascii=False)}")
            for turn in history:
                messages.append({"role": turn["role"], "content": turn["content"]})
        messages.append({"role": "user", "content": user_content})

        # Token / cost accumulators across all iterations
        total_prompt_tokens = 0
        total_completion_tokens = 0
        total_cached_tokens = 0
        iterations_used = 0

        # ----------------------------------------------------------------
        # Agentic loop
        # ----------------------------------------------------------------
        for iteration in range(self.MAX_ITERATIONS):
            iterations_used = iteration + 1

            # Inject iteration-awareness hint from iteration 2 onward
            if iteration > 0:
                if iteration >= 10:
                    hint = (
                        f"You have been processing for {iteration + 1} steps. "
                        "Please reflect on whether this extended looping is intended to gather more complete information, "
                        "or if you are stuck in a faulty loop (e.g., repeatedly failing tools). "
                        "If it is faulty, you must stop the loop, summarize what happened, and ask the user for clarification."
                    )
                else:
                    hint = (
                        f"Step {iteration + 1}/{self.MAX_ITERATIONS}. "
                    )
                messages.append({"role": "system", "content": hint})

            # On the last iteration, force a text response (no more tools)
            is_last = iteration >= self.MAX_ITERATIONS - 1
            tool_choice: Any = "none" if is_last else "auto"

            try:
                resp = await asyncio.wait_for(
                    self._client.chat.completions.create(
                        model=self._model,
                        temperature=0,
                        messages=messages,
                        tools=TOOL_DEFINITIONS if tool_executors else None,
                        tool_choice=tool_choice if tool_executors else None,
                    ),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                logger.error(
                    f"LLM timeout at iteration {iteration + 1} | "
                    f"Message preview: {message_text[:100]}"
                )
                return self._timeout_response()

            # Accumulate token usage
            usage = getattr(resp, "usage", None)
            if usage:
                total_prompt_tokens += getattr(usage, "prompt_tokens", 0)
                total_completion_tokens += getattr(usage, "completion_tokens", 0)
                prompt_details = getattr(usage, "prompt_tokens_details", None)
                if prompt_details:
                    total_cached_tokens += getattr(
                        prompt_details, "cached_tokens", 0
                    )
                elif isinstance(prompt_details, dict):
                    total_cached_tokens += prompt_details.get("cached_tokens", 0)

            msg = resp.choices[0].message

            # ---- No tool calls: model is done, return final answer ----
            if not msg.tool_calls:
                raw = msg.content or ""
                self._log_result(
                    raw,
                    total_prompt_tokens,
                    total_completion_tokens,
                    total_cached_tokens,
                    iterations_used,
                )
                return raw

            # ---- Tool calls: execute each and feed results back -------
            tool_call_details = [
                f"{tc.function.name}({tc.function.arguments})" for tc in msg.tool_calls
            ]
            logger.info(
                f"LLM iteration {iteration + 1}/{self.MAX_ITERATIONS} — "
                f"tool calls: {tool_call_details}"
            )

            # Serialize the assistant message (tool_calls must be preserved)
            messages.append(
                {
                    "role": "assistant",
                    "content": msg.content,
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments,
                            },
                        }
                        for tc in msg.tool_calls
                    ],
                }
            )

            for tc in msg.tool_calls:
                func_name = tc.function.name
                try:
                    func_args = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    func_args = {}

                if tool_executors and func_name in tool_executors:
                    try:
                        result_str = await tool_executors[func_name](**func_args)
                    except Exception as exc:
                        result_str = json.dumps(
                            {"error": f"Tool '{func_name}' failed: {exc}"}
                        )
                else:
                    result_str = json.dumps(
                        {"error": f"Unknown tool: {func_name}"}
                    )

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result_str,
                    }
                )
            # Continue loop so the model can reason about tool results

        # ----------------------------------------------------------------
        # Should be unreachable (last iteration forces tool_choice="none"),
        # but kept as a safety net.
        # ----------------------------------------------------------------
        logger.warning(
            f"Agent loop exhausted {self.MAX_ITERATIONS} iterations without "
            "producing a final response."
        )
        return self._fallback_response()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _log_result(
        self,
        raw_response: str,
        prompt_tokens: int,
        completion_tokens: int,
        cached_tokens: int,
        iterations: int,
    ) -> None:
        """Log a single clean line summarising the agentic call."""
        total_tokens = prompt_tokens + completion_tokens

        # Estimate cost
        model_lower = (self._model or "").lower()
        if "deepseek-chat" in model_lower or "deepseek-v3" in model_lower:
            input_rate, output_rate, cached_rate = 0.14, 0.28, 0.07
        elif "flash" in model_lower or "v4-flash" in model_lower:
            input_rate, output_rate, cached_rate = 0.08, 0.16, 0.04
        else:
            input_rate, output_rate, cached_rate = 0.15, 0.30, 0.075

        non_cached = max(0, prompt_tokens - cached_tokens)
        cost = (
            cached_tokens * (cached_rate / 1_000_000)
            + non_cached * (input_rate / 1_000_000)
            + completion_tokens * (output_rate / 1_000_000)
        )

        logger.info(
            f"LLM Done | Model: {self._model} | Iterations: {iterations}/{self.MAX_ITERATIONS} | "
            f"Tokens: In={prompt_tokens} (cached={cached_tokens}), Out={completion_tokens}, Total={total_tokens} | "
            f"Cost: ${cost:.6f} USD | Response Length: {len(raw_response)}\n"
            f"Response (capped): {raw_response[:500]!r}"
        )

    @staticmethod
    def _timeout_response() -> str:
        return "Maaf, permintaan ke AI kehabisan waktu (timeout). Silakan coba lagi."

    @staticmethod
    def _fallback_response() -> str:
        return "Maaf, terlalu banyak langkah pemrosesan. Silakan coba lagi."
