"""Server message catalogues `app/messages/{en,id}.json` and their renderer (localize-server-text D2).

Values are `str.format` templates with named placeholders. A plural message is an object with `one` and
`other` (English) or `other` only (Indonesian), chosen by the `count` parameter.
"""

from __future__ import annotations

import json
import logging
import pathlib
from functools import cache
from typing import Any

from app.services.language import LANGUAGES

logger = logging.getLogger(__name__)

MESSAGES_DIR = pathlib.Path(__file__).resolve().parents[1] / "messages"
FALLBACK_LANGUAGE = "id"
_reported: set[tuple[str, str]] = set()


class MessageError(LookupError):
    """A missing key or parameter. Raised in tests and development; production falls back to Indonesian."""


@cache
def catalogue(language: str) -> dict[str, Any]:
    if language not in LANGUAGES:
        raise MessageError(f"unsupported language {language!r}")
    return json.loads((MESSAGES_DIR / f"{language}.json").read_text(encoding="utf-8"))


def _lookup(language: str, key: str) -> Any:
    node: Any = catalogue(language)
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            raise MessageError(f"missing message {key!r} in {language}")
        node = node[part]
    return node


def has_message(language: str, key: str) -> bool:
    try:
        return isinstance(_lookup(language, key), (str, dict))
    except MessageError:
        return False


def _template(language: str, key: str, params: dict[str, Any]) -> str:
    value = _lookup(language, key)
    if isinstance(value, dict):
        if "count" not in params:
            raise MessageError(f"plural message {key!r} needs count")
        form = "one" if language == "en" and params["count"] == 1 else "other"
        value = value.get(form)
    if not isinstance(value, str):
        raise MessageError(f"message {key!r} in {language} is not text")
    return value


def _render(language: str, key: str, params: dict[str, Any]) -> str:
    try:
        return _template(language, key, params).format_map(params)
    except KeyError as missing:
        raise MessageError(f"message {key!r} in {language} needs {missing}") from None


def _strict() -> bool:
    from app.core.config import settings

    return settings.app_env in {"test", "development", "dev", "local"}


def t(language: str, key: str, **params: Any) -> str:
    """The message `key` in `language`, rendered with `params`.

    A missing key or parameter raises in tests and development. In production it logs the key path once,
    never the text, and returns the Indonesian message, so a user never sees a key path.
    """
    try:
        return _render(language, key, params)
    except MessageError:
        if _strict() or language == FALLBACK_LANGUAGE:
            raise
        if (language, key) not in _reported:
            _reported.add((language, key))
            logger.warning("Server message missing key=%s language=%s", key, language)
        return _render(FALLBACK_LANGUAGE, key, params)
