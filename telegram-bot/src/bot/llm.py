"""LLM planner: turns a user message (+optional image) into a structured proposal.

Network I/O is async (AsyncOpenAI). Prompt file read is a one-time sync load at
startup. JSON parsing is pure/sync.
"""
from __future__ import annotations

import asyncio
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
        history: list[dict[str, str]] | None = None,
        timeout: int = 120,
        pending_action: dict[str, Any] | None = None,
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
        if pending_action:
            context["pending_action"] = pending_action
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

        messages: list[dict[str, Any]] = [
            {"role": "system", "content": self._system_prompt}
        ]
        if history:
            for turn in history:
                messages.append({"role": turn["role"], "content": turn["content"]})
        messages.append({"role": "user", "content": content})

        try:
            resp = await asyncio.wait_for(
                self._client.chat.completions.create(
                    model=self._model,
                    temperature=0,
                    response_format={"type": "json_object"},
                    messages=messages,
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.error(f"LLM Request Timed Out | Timeout={timeout}s | Message preview: {message_text[:100]}...")
            return {
                "actions": [
                    {
                        "intent": "none",
                        "confidence": 0.0,
                        "missing_fields": [],
                        "ambiguities": ["AI request timed out. Silakan coba lagi."],
                    }
                ],
                "assistant_message": "Maaf, permintaan ke AI kehabisan waktu (timeout). Silakan coba lagi.",
            }

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
            actions_list = parsed.get("actions", [])
            if actions_list:
                # For logging, print the first action or a summary
                first_action = actions_list[0]
                intent = first_action.get("intent", "none")
                confidence = first_action.get("confidence", 0.0)
                missing_fields = first_action.get("missing_fields", [])
                ambiguities = first_action.get("ambiguities", [])
                
                if len(actions_list) > 1:
                    intent = f"batch({len(actions_list)} actions: {', '.join(a.get('intent', 'none') for a in actions_list)})"
            else:
                intent = "none"
        except Exception:
            parsed = {
                "actions": [
                    {
                        "intent": "none",
                        "confidence": 0.0,
                        "missing_fields": [],
                        "ambiguities": [],
                    }
                ],
                "assistant_message": "Maaf, saya tidak bisa memproses pesan itu. Coba tulis ulang.",
            }

        # Log LLM execution details per request as a single clean line
        logger.info(
            f"LLM Request | Model: {self._model} | Router: {self._client.base_url} | "
            f"Intent: {intent} (conf: {confidence:.2f}) | "
            f"Tokens: In={prompt_tokens} (cached={cached_tokens}), Out={completion_tokens}, Total={total_tokens} | "
            f"Cost: ${cost:.6f} USD | "
            f"Missing: {missing_fields} | Ambiguities: {len(ambiguities)}"
        )

        return parsed
