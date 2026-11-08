"""The language of the text the server writes (localize-server-text D1).

The account decides: every authenticated request (session or Bearer), the companion's ingestion, the
background jobs and the reports use `users.language`. `Accept-Language` is read only while a request has no
account. Errors render from a per-request holder that the middleware opens from the header and the auth
dependencies bind to the account; composed text takes the language explicitly.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator

from app.services.language import DEFAULT_LANGUAGE, DEFAULT_REGION, LANGUAGES, REGIONS, language_from_accept_language


@dataclass
class TextLanguage:
    language: str
    region: str
    account_bound: bool = False


# The holder object is shared by the copies of the context that FastAPI's thread pool makes, so a binding made
# in a sync dependency is visible to the endpoint, the exception handler and the middleware.
_holder: ContextVar[TextLanguage | None] = ContextVar("server_text_language", default=None)


def language_for_request(headers: Any) -> str:
    """The request's preferred supported language, or English (RFC 9110 §12.5.4)."""
    return language_from_accept_language(headers.get("accept-language"))


def open_request(headers: Any):
    """Start a request's holder from its headers; returns the token that `close_request` resets."""
    return _holder.set(TextLanguage(language_for_request(headers), DEFAULT_REGION))


def close_request(token) -> None:
    _holder.reset(token)


def _checked(language: str, region: str) -> tuple[str, str]:
    if language not in LANGUAGES or region not in REGIONS:
        raise ValueError("unsupported language or region")
    return language, region


def account_language(cur, user_id: str) -> tuple[str, str]:
    """The owner's `(language, region)` from their row; jobs and services that hold only an id use this."""
    cur.execute("SELECT language, region FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    if row is None:
        raise LookupError("unknown account")
    return _checked(row["language"], row["region"])


def _from_user(user: dict) -> tuple[str, str]:
    if user.get("language") and user.get("region"):
        return _checked(user["language"], user["region"])
    from app.db.pool import db_conn

    with db_conn() as conn, conn.cursor() as cur:
        return account_language(cur, str(user["id"]))


def language_for_user(user: dict) -> str:
    """The account's language. A user dict without it (a partial view) is completed from the owner's row."""
    return _from_user(user)[0]


def region_for_user(user: dict) -> str:
    return _from_user(user)[1]


def bind_account(user: dict) -> None:
    """Make the authenticated account's language the language of the rest of this request."""
    holder = _holder.get()
    if holder is not None and user.get("language") and user.get("region"):
        holder.language, holder.region = _checked(user["language"], user["region"])
        holder.account_bound = True


def current_language() -> str:
    """The language of the current request or scope; English where there is neither (D1, unauthenticated)."""
    holder = _holder.get()
    return holder.language if holder else DEFAULT_LANGUAGE


def current_region() -> str:
    holder = _holder.get()
    return holder.region if holder else DEFAULT_REGION


@contextmanager
def language_scope(language: str, region: str = DEFAULT_REGION) -> Iterator[None]:
    """Run job work for one owner in that owner's language, e.g. the notification worker per event."""
    token = _holder.set(TextLanguage(*_checked(language, region), account_bound=True))
    try:
        yield
    finally:
        _holder.reset(token)
