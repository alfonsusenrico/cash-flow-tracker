"""LLM planner: turns a user message (+optional image) into a structured proposal.

Network I/O is async (AsyncOpenAI). Prompt file read is a one-time sync load at
startup. JSON parsing is pure/sync.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

from openai import AsyncOpenAI

_PROMPT_PATH = Path(__file__).resolve().parents[2] / "prompts" / "system_prompt.md"


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
        raw = resp.choices[0].message.content or "{}"
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {
                "intent": "none",
                "confidence": 0.0,
                "missing_fields": [],
                "ambiguities": [],
                "assistant_message": "Maaf, saya tidak bisa memproses pesan itu. Coba tulis ulang.",
            }
