"""An auth override that behaves like the real dependencies: it binds the account's language to the request.

`get_current_user` and `get_session_user` bind the signed-in account's language and region, so errors and notices
raised later in the request follow the account (localize-server-text D1). A plain lambda override skips that.
"""

from __future__ import annotations


def signed_in(user: dict, *, language: str = "id", region: str = "ID"):
    account = {**user, "language": language, "region": region}

    def dependency() -> dict:
        from app.i18n import bind_account

        bind_account(account)
        return account

    return dependency
