"""Server text in the account's language (localize-server-text D1–D3)."""

from app.i18n.catalogue import MessageError, has_message, t
from app.i18n.money import format_money
from app.i18n.resolve import (
    account_language,
    bind_account,
    current_language,
    current_region,
    language_for_request,
    language_for_user,
    language_scope,
    region_for_user,
)



def notice(key: str, **params) -> str:
    """The success `message` of a mutation, in the request's server text language."""
    return t(current_language(), f"notices.{key}", **params)


__all__ = [
    "MessageError",
    "account_language",
    "bind_account",
    "current_language",
    "current_region",
    "format_money",
    "has_message",
    "language_for_request",
    "language_for_user",
    "language_scope",
    "notice",
    "region_for_user",
    "t",
]
