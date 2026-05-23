"""
Auth middleware helpers: inject req.state.username from either
session cookie (web) or Bearer API key (public).
Used by the unified resource routers.
"""
from fastapi import HTTPException, Request

from app.services.auth import (
    get_api_user_by_token,
    parse_bearer_token,
    require_session_user,
    enforce_public_rate_limit,
)


def inject_session_user(req: Request) -> None:
    """Middleware-style: sets req.state.username from session cookie."""
    req.state.username = require_session_user(req)


def inject_api_user(req: Request) -> None:
    """Middleware-style: sets req.state.username from Bearer token."""
    token = parse_bearer_token(req)
    enforce_public_rate_limit(req, token)
    req.state.username = get_api_user_by_token(token)
