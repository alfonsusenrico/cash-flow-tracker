"""Tag colours given to new accounts when the request names none.

Stored colours are user data: these defaults apply only when an account is created and never rewrite an
existing account's colour.
"""

DEFAULT_ACCOUNT_COLOR = "#3b82f6"
# The web's lavender investment fill (`--color-investment` in frontend/tokens.css, light theme).
INVESTMENT_ACCOUNT_COLOR = "#8B5CF6"


def default_account_color(account_type: str | None) -> str:
    return INVESTMENT_ACCOUNT_COLOR if account_type == "investment" else DEFAULT_ACCOUNT_COLOR
