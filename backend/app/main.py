from contextlib import asynccontextmanager
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import settings
from app.db.init_db import init_db_schema
from app.db.pool import close_db_pool, db_conn, open_db_pool
from app.routers.accounts import router as accounts_router
from app.routers.auth import router as auth_router
from app.routers.categories import router as categories_router
from app.routers.dashboard import router as dashboard_router
from app.routers.goals import router as goals_router
from app.routers.ingest import router as ingest_router
from app.routers.movements import router as movements_router
from app.routers.obligations import router as obligations_router
from app.routers.pulse import router as pulse_router
from app.routers.recurring import router as recurring_router
from app.routers.recurring import process_due_recurring_rules
from app.routers.transactions import router as transactions_router

_SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}
_CSRF_EXEMPT_PATHS = {"/api/auth/login", "/api/auth/register", "/api/health"}


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
    if request.method.upper() in _SAFE_METHODS or path in _CSRF_EXEMPT_PATHS:
        return False
    # If request has Bearer token, it's an API request, not cookie-based CSRF vulnerable
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return False
    return bool(request.cookies.get("ledger_session"))


import asyncio
import logging
from app.services.market_data import sync_all_tracked_prices

logger = logging.getLogger("api")


async def _daily_price_sync_loop():
    await asyncio.sleep(10)
    while True:
        try:
            with db_conn() as conn:
                res = await sync_all_tracked_prices(conn)
                logger.info(f"Daily price sync completed: {res}")
        except asyncio.CancelledError:
            break


async def _recurring_scheduler_loop():
    await asyncio.sleep(15)
    while True:
        try:
            result = await asyncio.to_thread(process_due_recurring_rules, limit=20)
            if result["processed_count"]:
                logger.info("Recurring scheduler processed %s occurrence(s)", result["processed_count"])
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("Recurring scheduler error: %s", exc)
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Daily price sync error: {e}")
        try:
            await asyncio.sleep(86400)
        except asyncio.CancelledError:
            break


@asynccontextmanager
async def lifespan(_: FastAPI):
    open_db_pool()
    # Initialize baseline schema if not present
    try:
        init_db_schema()
    except Exception as e:
        print(f"Schema init warning (handled by migrations): {e}")
    sync_task = asyncio.create_task(_daily_price_sync_loop())
    recurring_task = asyncio.create_task(_recurring_scheduler_loop())
    try:
        yield
    finally:
        sync_task.cancel()
        recurring_task.cancel()
        close_db_pool()


app = FastAPI(title="Cash Flow Tracker API", version="2.0.0", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.app_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# CSRF protection for cookie writes
@app.middleware("http")
async def csrf_middleware(request: Request, call_next):
    if _requires_csrf_origin_check(request):
        origin = _request_origin(request)
        if not origin or origin not in _allowed_origins(request):
            return JSONResponse(status_code=403, content={"ok": False, "detail": "Invalid request origin"})
    return await call_next(request)


# Session cookie middleware
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    session_cookie="ledger_session",
    same_site="lax",
    https_only=settings.cookie_secure,
)


@app.get("/api/health")
@app.get("/health")
def healthcheck():
    return {"ok": True, "status": "healthy", "service": "cash-flow-tracker-api"}


# Mount domain routers (supporting /api/path, /v1/path, and direct /path)
for prefix in ("", "/api", "/v1"):
    app.include_router(auth_router, prefix=f"{prefix}/auth")
    app.include_router(auth_router, prefix=prefix)
    app.include_router(accounts_router, prefix=f"{prefix}/accounts")
    app.include_router(categories_router, prefix=f"{prefix}/categories")
    app.include_router(transactions_router, prefix=f"{prefix}/transactions")
    app.include_router(movements_router, prefix=prefix)
    app.include_router(goals_router, prefix=f"{prefix}/goals")
    app.include_router(obligations_router, prefix=f"{prefix}/obligations")
    app.include_router(recurring_router, prefix=f"{prefix}/recurring")
    app.include_router(dashboard_router, prefix=f"{prefix}/dashboard")
    app.include_router(ingest_router, prefix=f"{prefix}/ingest")
    app.include_router(pulse_router, prefix=prefix)


@app.exception_handler(HTTPException)
def http_exc_handler(_, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"ok": False, "detail": exc.detail},
        headers=exc.headers,
    )
