from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.sessions import SessionMiddleware

from app.core.config import settings
from app.db.pool import close_db_pool, open_db_pool
from app.routers.public import router as public_router
from app.routers.web import router as web_router


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


@app.exception_handler(HTTPException)
def http_exc_handler(_, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})
