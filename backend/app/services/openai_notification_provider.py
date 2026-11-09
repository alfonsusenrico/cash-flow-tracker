from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
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
from pydantic import BaseModel, ValidationError

from app.services.capture_normalization import CaptureNormalization
from app.services.notification_context import CONTEXT_BYTE_LIMIT
from app.services.notification_interpretation import Interpretation
from app.services.prompt_language import (
    CAPTURE_NORMALIZATION, HISTORY_SUGGESTIONS, NOTIFICATION_INTERPRETATION, PURPOSE_NORMALIZATION, Prompt,
)
from app.services.purpose_normalization import PurposeNormalization


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


# Synthetic benchmark replies kept for diagnosis are cut to this many characters (D4).
CAPTURED_OUTPUT_LIMIT = 4000


@dataclass(frozen=True)
class OutputDiagnosis:
    """Why a reply was unusable, without any of its content (align-notification-output-contract D3).

    `fields` holds `path:error_type` pairs whose paths use schema property names and list positions only.
    """

    failure: str
    incomplete_reason: str | None = None
    fields: tuple[str, ...] = ()
    output_tokens: int | None = None

    def as_dict(self) -> dict:
        return {
            "failure": self.failure, "incomplete_reason": self.incomplete_reason,
            "fields": list(self.fields), "output_tokens": self.output_tokens,
        }


class ProviderError(Exception):
    def __init__(
        self, code: str, *, transient: bool = False, retry_after: float = 0, diagnosis: OutputDiagnosis | None = None,
    ):
        self.code = code
        self.transient = transient
        self.retry_after = retry_after
        self.diagnosis = diagnosis
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


def schema_property_names(model: type[BaseModel]) -> frozenset[str]:
    names = set()

    def visit(node):
        if isinstance(node, dict):
            names.update((node.get("properties") or {}).keys())
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)
    visit(model.model_json_schema())
    return frozenset(names)


def error_field(location: tuple, property_names: frozenset[str], error_type: str) -> str:
    """A pydantic error as `path:type`; a key the schema does not define (an extra key) is never echoed."""
    parts = []
    for part in location:
        if isinstance(part, int):
            parts.append(str(part))
        elif part in property_names:
            parts.append(part)
        else:
            parts.append("?")
    return f"{'.'.join(parts)}:{error_type}"


def output_diagnosis(error: ValidationError, model: type[BaseModel], output_tokens: int | None) -> OutputDiagnosis:
    errors = error.errors(include_url=False, include_context=False, include_input=False)
    if any(item["type"] == "json_invalid" for item in errors):
        return OutputDiagnosis("json_decode", output_tokens=output_tokens)
    names = schema_property_names(model)
    fields = tuple(dict.fromkeys(error_field(tuple(item["loc"]), names, item["type"]) for item in errors))
    return OutputDiagnosis("schema_mismatch", fields=fields, output_tokens=output_tokens)


def parse_output(model: type[BaseModel], output: str, *, output_tokens: int | None = None):
    """Validate a reply against the schema that was sent; a violation is `provider_invalid_output` with its diagnosis."""
    try:
        return model.model_validate_json(output)
    except ValidationError as exc:
        raise ProviderError("provider_invalid_output", diagnosis=output_diagnosis(exc, model, output_tokens)) from None


def refusal_text(response) -> str | None:
    for item in response.output or []:
        if getattr(item, "type", None) != "message":
            continue
        for content in getattr(item, "content", None) or []:
            if getattr(content, "type", None) == "refusal":
                return getattr(content, "refusal", None) or ""
    return None


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


def instructions(prompt: Prompt, name: str) -> str:
    """The prompt text for one call; the caller chose its output language (localize-server-text D7)."""
    if prompt.name != name:
        raise ValueError("prompt does not belong to this call")
    return prompt.text


class OpenAINotificationProvider:
    def __init__(
        self,
        http_client: httpx.AsyncClient,
        *,
        api_key: str,
        model: str,
        reasoning_effort: str,
        timeout: float = 30,
        capture_invalid_output: bool = False,
    ):
        self.api_key = api_key
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout = max(1, min(60, timeout))
        self.last_usage: dict[str, int] | None = None
        # Only the synthetic benchmark sets this; production never keeps reply text (D4).
        self.capture_invalid_output = capture_invalid_output
        self.last_invalid_output: str | None = None
        self.client = AsyncOpenAI(
            api_key=api_key,
            timeout=self.timeout,
            max_retries=0,
            http_client=http_client,
        )

    async def interpret(self, context: dict, *, prompt: Prompt) -> Interpretation:
        output = await self.respond(instructions(prompt, NOTIFICATION_INTERPRETATION), context, response_schema())
        proposal = self.parse(Interpretation, output)
        if proposal.direction == "income" and proposal.kakeibo is not None:
            proposal = proposal.model_copy(update={"kakeibo": None})
        # Movement categories and Kakeibo are assigned by the backend, never by the model.
        if proposal.direction == "internal_movement" and (proposal.category_id or proposal.kakeibo):
            proposal = proposal.model_copy(update={"category_id": None, "kakeibo": None})
        return proposal

    async def normalize_purpose(self, context: dict, *, prompt: Prompt) -> PurposeNormalization:
        output = await self.respond(
            instructions(prompt, PURPOSE_NORMALIZATION), context, response_schema(PurposeNormalization, "purpose_normalization"),
        )
        return self.parse(PurposeNormalization, output)

    async def normalize_capture(self, context: dict, *, prompt: Prompt) -> CaptureNormalization:
        output = await self.respond(
            instructions(prompt, CAPTURE_NORMALIZATION), context, response_schema(CaptureNormalization, "capture_normalization"),
        )
        return self.parse(CaptureNormalization, output)

    async def suggest_history(self, context: dict, *, prompt: Prompt):
        # Imported here: the suggestion service needs the database, and this module must load without it.
        from app.services.history_suggestions import HistorySuggestionBatch

        output = await self.respond(
            instructions(prompt, HISTORY_SUGGESTIONS), context, response_schema(HistorySuggestionBatch, "history_suggestions"),
        )
        return self.parse(HistorySuggestionBatch, output)

    def parse(self, model: type[BaseModel], output: str):
        try:
            return parse_output(model, output, output_tokens=self.output_tokens())
        except ProviderError:
            self.keep_invalid_output(output)
            raise

    def output_tokens(self) -> int | None:
        return (self.last_usage or {}).get("output_tokens")

    def keep_invalid_output(self, text: str | None) -> None:
        if self.capture_invalid_output and text is not None:
            self.last_invalid_output = text[:CAPTURED_OUTPUT_LIMIT]

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
            self.last_invalid_output = None
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
            refusal = refusal_text(response)
            if not response.status:
                raise ProviderError("provider_invalid_output", diagnosis=OutputDiagnosis(
                    "malformed_response", output_tokens=self.output_tokens(),
                ))
            if response.status != "completed":
                details = getattr(response, "incomplete_details", None)
                reason = getattr(details, "reason", None) or response.status
                self.keep_invalid_output(refusal)
                raise ProviderError("provider_invalid_output", diagnosis=OutputDiagnosis(
                    "incomplete", incomplete_reason=reason, output_tokens=self.output_tokens(),
                ))
            if refusal is not None and not response.output_text:
                self.keep_invalid_output(refusal)
                raise ProviderError("provider_invalid_output", diagnosis=OutputDiagnosis(
                    "refusal", output_tokens=self.output_tokens(),
                ))
            if not response.output_text:
                raise ProviderError("provider_invalid_output", diagnosis=OutputDiagnosis(
                    "empty_output", output_tokens=self.output_tokens(),
                ))
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
            # A response body the SDK cannot read as a Responses object.
            raise ProviderError("provider_invalid_output", diagnosis=OutputDiagnosis(
                "malformed_response", output_tokens=self.output_tokens(),
            )) from None
