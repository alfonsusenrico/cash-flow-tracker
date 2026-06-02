"""LLM planner: turns a user message (+optional image) into a structured proposal.

Network I/O is async (AsyncOpenAI). Prompt file read is a one-time sync load at
startup. JSON parsing is pure/sync.
"""
from __future__ import annotations

import base64
import json
import logging
from pathlib import Path
from typing import Any

from openai import AsyncOpenAI

_PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "system_prompt.md"

logger = logging.getLogger(__name__)


class LLMPlanner:
    def __init__(self, api_key: str, base_url: str, model: str) -> None:
        self._client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self._model = model
        self._system_prompt = _PROMPT_PATH.read_text(encoding="utf-8")

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
    ) -> dict[str, Any]:
        context = {
            "now": now_iso,
            "timezone": timezone,
            "accounts": [
                {"account_name": a.get("account_name"), "profile_type": a.get("profile_type"), "balance": a.get("balance")}
                for a in accounts
            ],
            "categories": [{"name": c.get("name"), "kind": c.get("kind")} for c in categories],
            "message": message_text or "",
        }
        text_part = {"type": "text", "text": json.dumps(context, ensure_ascii=False)}
        content: Any
        if image_bytes:
            b64 = base64.b64encode(image_bytes).decode()
            content = [
                text_part,
                {"type": "image_url", "image_url": {"url": f"data:{image_mime};base64,{b64}"}},
            ]
        else:
            content = [text_part]

        resp = await self._client.chat.completions.create(
            model=self._model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": self._system_prompt},
                {"role": "user", "content": content},
            ],
        )

        # Extract token usage and cache info
        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0
        cached_tokens = 0

        usage = getattr(resp, "usage", None)
        if usage:
            prompt_tokens = getattr(usage, "prompt_tokens", 0)
            completion_tokens = getattr(usage, "completion_tokens", 0)
            total_tokens = getattr(usage, "total_tokens", 0)
            
            prompt_details = getattr(usage, "prompt_tokens_details", None)
            if prompt_details:
                cached_tokens = getattr(prompt_details, "cached_tokens", 0)
            else:
                if isinstance(prompt_details, dict):
                    cached_tokens = prompt_details.get("cached_tokens", 0)

        # Estimate costs (per 1M tokens) based on model name
        model_lower = (self._model or "").lower()
        if "deepseek-chat" in model_lower or "deepseek-v3" in model_lower:
            input_rate = 0.14
            output_rate = 0.28
            cached_rate = 0.07
        elif "flash" in model_lower or "v4-flash" in model_lower:
            input_rate = 0.08
            output_rate = 0.16
            cached_rate = 0.04
        else:
            # General fallback rate
            input_rate = 0.15
            output_rate = 0.30
            cached_rate = 0.075

        # Calculate total cost in USD
        non_cached_input = max(0, prompt_tokens - cached_tokens)
        cost = (
            (cached_tokens * (cached_rate / 1_000_000))
            + (non_cached_input * (input_rate / 1_000_000))
            + (completion_tokens * (output_rate / 1_000_000))
        )

        raw = resp.choices[0].message.content or "{}"
        intent = "unknown"
        confidence = 0.0
        missing_fields = []
        ambiguities = []

        try:
            parsed = json.loads(raw)
            intent = parsed.get("intent", "none")
            confidence = parsed.get("confidence", 0.0)
            missing_fields = parsed.get("missing_fields", [])
            ambiguities = parsed.get("ambiguities", [])
        except json.JSONDecodeError:
            parsed = {
                "intent": "none",
                "confidence": 0.0,
                "missing_fields": [],
                "ambiguities": [],
                "assistant_message": "Maaf, saya tidak bisa memproses pesan itu. Coba tulis ulang.",
            }

        # Log LLM execution details per request
        logger.info(
            f"\n"
            f"=================== LLM PROCESS LOG ===================\n"
            f"API Endpoint / Router : {self._client.base_url}\n"
            f"Model Used            : {self._model}\n"
            f"Request Stats:\n"
            f"  - Input Tokens      : {prompt_tokens} (cached: {cached_tokens}, non-cached: {non_cached_input})\n"
            f"  - Output Tokens     : {completion_tokens}\n"
            f"  - Total Tokens      : {total_tokens}\n"
            f"  - Estimated Cost    : ${cost:.6f} USD\n"
            f"Tool Call Info (Proposed Action):\n"
            f"  - Intent            : {intent}\n"
            f"  - Confidence        : {confidence:.2f}\n"
            f"  - Missing Fields    : {missing_fields}\n"
            f"  - Ambiguities       : {len(ambiguities)} found\n"
            f"======================================================="
        )

        return parsed
