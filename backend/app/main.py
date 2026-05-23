from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import settings
from app.db.pool import close_db_pool, open_db_pool
from app.routers.public import router as public_router
from app.routers.web import router as web_router
from app.routers.resources.categories import router as categories_router
from app.routers.resources.periods import router as periods_router
from app.services.auth import (
    get_api_user_by_token,
    parse_bearer_token,
    require_session_user,
    enforce_public_rate_limit,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    open_db_pool()
    try:
        yield
    finally:
        close_db_pool()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    session_cookie="ledger_session",
    same_site="strict",
    https_only=settings.cookie_secure,
)

app.include_router(web_router)
app.include_router(public_router)

# Resource routers mounted at both cookie (/prefix) and Bearer (/v1/prefix) paths.
# Auth is resolved per-request in the middleware below before the handler runs.
app.include_router(categories_router, prefix="/categories")
app.include_router(categories_router, prefix="/v1/categories")
app.include_router(periods_router, prefix="/periods")
app.include_router(periods_router, prefix="/v1/periods")


@app.middleware("http")
async def inject_username_middleware(request: Request, call_next):
    """
    Inject req.state.username for resource routers.
    Raises 401 immediately if auth fails — never swallows errors silently.
    """
    path = request.url.path

    if path.startswith("/v1/categories") or path.startswith("/v1/periods"):
        try:
            token = parse_bearer_token(request)
            enforce_public_rate_limit(request, token)
            request.state.username = get_api_user_by_token(token)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})

    elif path.startswith("/categories") or path.startswith("/periods"):
        try:
            request.state.username = require_session_user(request)
        except HTTPException as exc:
            return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})

    return await call_next(request)


@app.exception_handler(HTTPException)
def http_exc_handler(_, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})
