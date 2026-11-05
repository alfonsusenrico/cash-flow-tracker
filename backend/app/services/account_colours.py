"""Tag colours given to new accounts when the request names none.

Stored colours are user data: these defaults apply only when an account is created and never rewrite an
existing account's colour.
"""

DEFAULT_ACCOUNT_COLOR = "#3b82f6"
# The web's lavender investment fill (`--color-investment` in frontend/tokens.css, light theme).
INVESTMENT_ACCOUNT_COLOR = "#8B5CF6"
# Slate, the web's default for a credit account (frontend/src/lib/accountColours.ts).
CREDIT_ACCOUNT_COLOR = "#475569"


def default_account_color(account_type: str | None) -> str:
    if account_type == "investment":
        return INVESTMENT_ACCOUNT_COLOR
    if account_type == "credit":
        return CREDIT_ACCOUNT_COLOR
    return DEFAULT_ACCOUNT_COLOR
