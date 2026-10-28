from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx2 as httpx
from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AsyncOpenAI,
    AuthenticationError,
    BadRequestError,
    InternalServerError,
    PermissionDeniedError,
    RateLimitError,
)
from pydantic import ValidationError

from app.services.notification_context import CONTEXT_BYTE_LIMIT
from app.services.notification_interpretation import Interpretation, system_prompt
from app.services.purpose_normalization import PurposeNormalization, purpose_prompt


# Models approved for live transaction parsing. Expand here to add candidates during evaluation.
# Reasoning models (o-series / Luna): support reasoning= param.
# Chat models (gpt-4o family): use temperature= instead; reasoning= is omitted.
OPERATIONAL_MODELS = frozenset({
    "gpt-5.6-luna",
    "gpt-6-luna",
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-4.1",
    "gpt-4.1-mini",
})
REASONING_MODELS = frozenset({"gpt-5.6-luna", "gpt-6-luna"})
PERMITTED_REASONING_EFFORTS = frozenset({"none", "low"})


class ProviderError(Exception):
    def __init__(self, code: str, *, transient: bool = False, retry_after: float = 0):
        self.code = code
        self.transient = transient
        self.retry_after = retry_after
        super().__init__(code)


def configuration_error(settings) -> str | None:
    if not settings.notification_ai_enabled:
        return "ai_disabled"
    if settings.notification_ai_configuration_error:
        return settings.notification_ai_configuration_error
    if settings.notification_ai_model not in OPERATIONAL_MODELS:
        return "provider_model_not_permitted"
    if settings.notification_ai_model in REASONING_MODELS and settings.notification_ai_reasoning_effort not in PERMITTED_REASONING_EFFORTS:
        return "provider_reasoning_not_permitted"
    if not settings.openai_api_key:
        return "provider_credential_missing"
    return None


def dry_run_configuration_error(settings) -> str | None:
    if settings.app_env != "development" or not settings.notification_ai_dry_run_enabled:
        return "dry_run_unavailable"
    if settings.notification_ai_configuration_error:
        return settings.notification_ai_configuration_error
    if settings.notification_ai_model not in OPERATIONAL_MODELS:
        return "provider_model_not_permitted"
    if settings.notification_ai_model in REASONING_MODELS and settings.notification_ai_reasoning_effort not in PERMITTED_REASONING_EFFORTS:
        return "provider_reasoning_not_permitted"
    if not settings.openai_api_key:
        return "provider_credential_missing"
    return None


def retry_after_seconds(value: str | None) -> float:
    if not value:
        return 0
    try:
        delay = float(value)
    except ValueError:
        try:
            instant = parsedate_to_datetime(value)
            delay = (instant - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return 0
    return max(0, min(3600, delay))


def strict_json_schema(schema: dict) -> dict:
    """OpenAI strict mode needs every property required and no defaults, at every level."""
    def visit(node):
        if isinstance(node, dict):
            node.pop("default", None)
            if node.get("type") == "object" and "properties" in node:
                node["required"] = list(node["properties"])
                node["additionalProperties"] = False
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)
    visit(schema)
    return schema


def response_schema(model=Interpretation, name: str = "notification_interpretation") -> dict:
    return {
        "type": "json_schema",
        "name": name,
        "strict": True,
        "schema": strict_json_schema(model.model_json_schema()),
    }


def model_input(context: dict) -> str:
    """Serialised model input; underscore-prefixed keys are backend-internal and never sent."""
    public = {key: value for key, value in context.items() if not key.startswith("_")}
    context_json = json.dumps(public, default=str, ensure_ascii=False)
    if len(context_json.encode("utf-8")) > CONTEXT_BYTE_LIMIT:
        raise ProviderError("incomplete_context")
    return context_json


class OpenAINotificationProvider:
    def __init__(
        self,
        http_client: httpx.AsyncClient,
        *,
        api_key: str,
        model: str,
        reasoning_effort: str,
        timeout: float = 30,
    ):
        self.api_key = api_key
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout = max(1, min(60, timeout))
        self.last_usage: dict[str, int] | None = None
        self.client = AsyncOpenAI(
            api_key=api_key,
            timeout=self.timeout,
            max_retries=0,
            http_client=http_client,
        )

    async def interpret(self, context: dict) -> Interpretation:
        output = await self.respond(system_prompt(), context, response_schema())
        try:
            proposal = Interpretation.model_validate_json(output)
        except (ValueError, TypeError, ValidationError):
            raise ProviderError("provider_invalid_output") from None
        if proposal.direction == "income" and proposal.kakeibo is not None:
            proposal = proposal.model_copy(update={"kakeibo": None})
        # Movement categories and Kakeibo are assigned by the backend, never by the model.
        if proposal.direction == "internal_movement" and (proposal.category_id or proposal.kakeibo):
            proposal = proposal.model_copy(update={"category_id": None, "kakeibo": None})
        return proposal

    async def normalize_purpose(self, context: dict) -> PurposeNormalization:
        output = await self.respond(
            purpose_prompt(), context, response_schema(PurposeNormalization, "purpose_normalization"),
        )
        try:
            return PurposeNormalization.model_validate_json(output)
        except (ValueError, TypeError, ValidationError):
            raise ProviderError("provider_invalid_output") from None

    async def suggest_history(self, context: dict):
        # Imported here: the suggestion service needs the database, and this module must load without it.
        from app.services.history_suggestions import HistorySuggestionBatch, history_prompt, parse_batch

        output = await self.respond(
            history_prompt(), context, response_schema(HistorySuggestionBatch, "history_suggestions"),
        )
        return parse_batch(output)

    async def respond(self, instructions: str, context: dict, schema: dict) -> str:
        if self.model not in OPERATIONAL_MODELS:
            raise ProviderError("provider_model_not_permitted")
        if self.reasoning_effort not in PERMITTED_REASONING_EFFORTS and self.model in REASONING_MODELS:
            raise ProviderError("provider_reasoning_not_permitted")
        if not self.api_key:
            raise ProviderError("provider_credential_missing")

        context_json = model_input(context)
        is_reasoning_model = self.model in REASONING_MODELS
        try:
            self.last_usage = None
            call_kwargs: dict = {
                "model": self.model,
                "instructions": instructions,
                "input": context_json,
                "text": {"format": schema},
                "max_output_tokens": 2048,
                "store": False,
            }
            if is_reasoning_model:
                call_kwargs["reasoning"] = {"effort": self.reasoning_effort}
            else:
                call_kwargs["temperature"] = 0.2
            response = await self.client.responses.create(**call_kwargs)
            if response.usage:
                self.last_usage = {
                    name: int(value)
                    for name in ("input_tokens", "output_tokens", "total_tokens")
                    if (value := getattr(response.usage, name, None)) is not None
                }
            if response.status != "completed" or not response.output_text:
                raise ProviderError("provider_invalid_output")
            return response.output_text
        except ProviderError:
            raise
        except (APITimeoutError, APIConnectionError, asyncio.TimeoutError):
            raise ProviderError("provider_timeout", transient=True) from None
        except (AuthenticationError, PermissionDeniedError):
            raise ProviderError("provider_access_denied") from None
        except RateLimitError as exc:
            raise ProviderError(
                "provider_unavailable",
                transient=True,
                retry_after=retry_after_seconds(exc.response.headers.get("Retry-After") if exc.response else None),
            ) from None
        except InternalServerError as exc:
            raise ProviderError(
                "provider_unavailable",
                transient=True,
                retry_after=retry_after_seconds(exc.response.headers.get("Retry-After") if exc.response else None),
            ) from None
        except (BadRequestError, APIStatusError):
            raise ProviderError("provider_request_rejected") from None
        except (ValueError, TypeError, ValidationError):
            raise ProviderError("provider_invalid_output") from None
