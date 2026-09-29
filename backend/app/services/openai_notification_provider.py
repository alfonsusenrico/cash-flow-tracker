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

from app.services.notification_interpretation import Interpretation, system_prompt


OPERATIONAL_MODELS = frozenset({"gpt-5.6-luna"})
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
    if settings.notification_ai_reasoning_effort not in PERMITTED_REASONING_EFFORTS:
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
    if settings.notification_ai_reasoning_effort not in PERMITTED_REASONING_EFFORTS:
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


def response_schema() -> dict:
    return {
        "type": "json_schema",
        "name": "notification_interpretation",
        "strict": True,
        "schema": Interpretation.model_json_schema(),
    }


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
        if self.model not in OPERATIONAL_MODELS:
            raise ProviderError("provider_model_not_permitted")
        if self.reasoning_effort not in PERMITTED_REASONING_EFFORTS:
            raise ProviderError("provider_reasoning_not_permitted")
        if not self.api_key:
            raise ProviderError("provider_credential_missing")

        context_json = json.dumps(context, default=str, ensure_ascii=False)
        if len(context_json.encode("utf-8")) > 60000:
            raise ProviderError("incomplete_context")

        try:
            self.last_usage = None
            response = await self.client.responses.create(
                model=self.model,
                instructions=system_prompt(),
                input=context_json,
                reasoning={"effort": self.reasoning_effort},
                text={"format": response_schema()},
                max_output_tokens=2048,
                store=False,
            )
            if response.usage:
                self.last_usage = {
                    name: int(value)
                    for name in ("input_tokens", "output_tokens", "total_tokens")
                    if (value := getattr(response.usage, name, None)) is not None
                }
            if response.status != "completed" or not response.output_text:
                raise ProviderError("provider_invalid_output")
            return Interpretation.model_validate_json(response.output_text)
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
