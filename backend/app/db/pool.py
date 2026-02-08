from contextlib import contextmanager

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from app.core.config import settings

DB_POOL = ConnectionPool(
    settings.database_url,
    min_size=settings.db_pool_min,
    max_size=settings.db_pool_max,
    timeout=settings.db_pool_timeout,
    max_waiting=settings.db_pool_max_waiting,
    open=False,
    kwargs={"row_factory": dict_row},
)


def open_db_pool() -> None:
    DB_POOL.open()


def close_db_pool() -> None:
    DB_POOL.close()


@contextmanager
def db_conn():
    with DB_POOL.connection() as conn:
        yield conn
