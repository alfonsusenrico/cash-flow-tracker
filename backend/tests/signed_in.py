"""Test helpers for the signed-in account's language (localize-server-text D1, D8).

`get_current_user` and `get_session_user` bind the signed-in account's language and region, so errors and notices
raised later in the request follow the account. A plain lambda override skips that; `signed_in` does not.
"""

from __future__ import annotations

RENDERED_FIELDS = {"text", "prompt", "label", "reason", "explanation", "message", "question"}


def signed_in(user: dict, *, language: str = "id", region: str = "ID"):
    account = {**user, "language": language, "region": region}

    def dependency() -> dict:
        from app.i18n import bind_account

        bind_account(account)
        return account

    return dependency


def switch_account_language(user_id: str, language: str, region: str = "ID", **user) -> None:
    """Store the account's language and sign it in with it, as a settings change followed by a request does."""
    from app.db.pool import db_conn
    from app.main import app
    from app.services.auth import get_current_user

    with db_conn() as conn:
        conn.execute("UPDATE users SET language = %s, region = %s WHERE id = %s", (language, region, user_id))
        conn.commit()
    app.dependency_overrides[get_current_user] = signed_in({"id": user_id, "currency": "IDR", **user},
                                                           language=language, region=region)


def rendered_texts(value) -> set[str]:
    """Every server-rendered string in a response body: questions, prompts, labels, reasons and messages."""
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key in RENDERED_FIELDS and isinstance(child, str) and len(child) > 3:
                found.add(child)
            found |= rendered_texts(child)
    elif isinstance(value, list):
        for child in value:
            found |= rendered_texts(child)
    return found


def leaked(log_lines: list[str], secrets: set[str]) -> list[str]:
    """Log lines that contain a rendered text or a piece of user data."""
    return [line for line in log_lines if any(secret.casefold() in line.casefold() for secret in secrets)]
