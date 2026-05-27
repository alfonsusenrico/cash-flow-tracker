from contextlib import asynccontextmanager
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import settings
from app.db.pool import close_db_pool, open_db_pool
from app.routers.public import router as public_router
from app.routers.web import router as web_router
from app.routers.resources.categories import router as categories_router
from app.routers.resources.periods import router as periods_router
from app.routers.resources.buckets import router as buckets_router
from app.routers.resources.allocation import router as allocation_router, start_allocation_scheduler
from app.routers.resources.strategy import router as strategy_router
from app.routers.resources.goals import router as goals_router
from app.routers.resources.assets import router as assets_router
from app.routers.resources.dashboard import router as dashboard_router
from app.routers.resources.obligations import router as obligations_router
from app.services.auth import (
    get_api_user_by_token,
    parse_bearer_token,
    require_session_user,
    enforce_public_rate_limit,
)

# Prefixes for all resource routers (mounted at both cookie and Bearer paths)
_RESOURCE_PREFIXES = [
    "/categories", "/periods", "/buckets", "/allocation-plans", "/strategy-rules",
    "/goals", "/assets", "/dashboard", "/obligations",
]
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}
_CSRF_EXEMPT_PATHS = {"/auth/login", "/auth/register"}


def _origin_from_value(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlsplit(value.strip())
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}".rstrip("/")


def _request_origin(request: Request) -> str | None:
    origin = _origin_from_value(request.headers.get("origin"))
    if origin:
        return origin
    return _origin_from_value(request.headers.get("referer"))


def _allowed_origins(request: Request) -> set[str]:
    configured = {origin for raw in settings.app_origins if (origin := _origin_from_value(raw))}
    if configured:
        return configured

    host = request.headers.get("host", "").lower()
    if not host:
        return set()
    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
    return {f"{scheme.lower()}://{host}".rstrip("/")}


def _requires_csrf_origin_check(request: Request) -> bool:
    path = request.url.path
    if request.method.upper() in _SAFE_METHODS:
        return False
    if path.startswith("/v1/") or path in _CSRF_EXEMPT_PATHS:
        return False
    return bool(request.cookies.get("ledger_session"))


@asynccontextmanager
async def lifespan(_: FastAPI):
    open_db_pool()
    start_allocation_scheduler()
    try:
        yield
    finally:
        close_db_pool()


app = FastAPI(lifespan=lifespan)

app.include_router(web_router)
app.include_router(public_router)

for _router, _prefix in [
    (categories_router, "/categories"),
    (periods_router, "/periods"),
    (buckets_router, "/buckets"),
    (allocation_router, "/allocation-plans"),
    (strategy_router, "/strategy-rules"),
    (goals_router, "/goals"),
    (assets_router, "/assets"),
    (dashboard_router, "/dashboard"),
    (obligations_router, "/obligations"),
]:
    app.include_router(_router, prefix=_prefix)
    app.include_router(_router, prefix=f"/v1{_prefix}")


@app.middleware("http")
async def inject_username_middleware(request: Request, call_next):
    if _requires_csrf_origin_check(request):
        origin = _request_origin(request)
        if not origin or origin not in _allowed_origins(request):
            return JSONResponse(status_code=403, content={"ok": False, "detail": "Invalid request origin"})

    path = request.url.path
    matched = next((p for p in _RESOURCE_PREFIXES if path.startswith(p) or path.startswith(f"/v1{p}")), None)
    if matched:
        is_api = path.startswith("/v1/")
        try:
            if is_api:
                token = parse_bearer_token(request)
                enforce_public_rate_limit(request, token)
                request.state.username = get_api_user_by_token(token)
            else:
                request.state.username = require_session_user(request)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})

    return await call_next(request)


app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    session_cookie="ledger_session",
    same_site="strict",
    https_only=settings.cookie_secure,
)


@app.exception_handler(HTTPException)
def http_exc_handler(_, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})
