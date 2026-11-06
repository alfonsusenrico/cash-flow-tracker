"""Structured API errors: `detail: {code, params, message}` (add-language-system D8).

`code` is stable snake_case that clients render in their own language, `params` holds the values a message
needs (`{}` when none) and `message` is the English fallback for clients that do not know the code.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException


def coded_error(
    status_code: int,
    code: str,
    message: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail={"code": code, "params": params or {}, "message": message},
        headers=headers,
    )
