"""
Shared pytest fixtures.

Requires a real Postgres instance. Set TEST_DATABASE_URL in the environment,
or it defaults to a local ledger_test database.

The fixture:
1. Creates a fresh schema by running all Flyway migrations via psql.
2. Seeds one test user.
3. Yields a FastAPI TestClient with the session cookie pre-set.
4. Drops the schema after the test session.

Usage:
    TEST_DATABASE_URL=postgresql://ledger:ledgerpass@localhost:5432/ledger_test pytest
"""
import os
import subprocess
import pathlib
from uuid import uuid4
import pytest
from fastapi.testclient import TestClient

TEST_DB_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://ledger:ledgerpass@localhost:5432/ledger_test",
)
MIGRATIONS_DIR = pathlib.Path(__file__).resolve().parents[2] / "db" / "migrations"


def _run_migrations(db_url: str) -> None:
    """Apply all Flyway migrations using the flyway CLI or psql fallback."""
    # Try flyway first (available in CI via docker)
    flyway_cmd = [
        "flyway",
        f"-url=jdbc:{db_url.replace('postgresql://', 'postgresql://')}",
        f"-locations=filesystem:{MIGRATIONS_DIR}",
        "-baselineOnMigrate=true",
        "migrate",
    ]
    result = subprocess.run(flyway_cmd, capture_output=True)
    if result.returncode == 0:
        return

    # Fallback: apply SQL files in order via psql
    sql_files = sorted(MIGRATIONS_DIR.glob("V*.sql"))
    for sql_file in sql_files:
        subprocess.run(
            ["psql", db_url, "-f", str(sql_file)],
            check=True,
            capture_output=True,
        )


@pytest.fixture(scope="session")
def db_url() -> str:
    return TEST_DB_URL


@pytest.fixture(scope="session", autouse=True)
def apply_migrations(db_url: str):
    """
    Apply migrations only when a real Postgres is reachable.
    Skips silently for pure unit tests that don't need a DB.
    """
    if os.getenv("SKIP_TEST_MIGRATIONS") == "1":
        yield
        return

    import socket
    # Quick TCP check — if Postgres isn't up, skip migration entirely.
    try:
        host, port_str = db_url.split("@")[-1].split("/")[0].rsplit(":", 1)
        port = int(port_str)
        with socket.create_connection((host, port), timeout=2):
            pass
    except Exception:
        yield  # No DB available — unit tests still run
        return

    _run_migrations(db_url)
    yield


@pytest.fixture(scope="session")
def client(db_url: str, apply_migrations):
    os.environ["DATABASE_URL"] = db_url
    os.environ["SESSION_SECRET"] = "test-secret-for-pytest"
    os.environ.setdefault("INVITE_CODE", "TESTCODE")
    os.environ["COOKIE_SECURE"] = "false"
    os.environ["REDIS_URL"] = ""  # disable Redis in tests

    # Import app after env is set so config loads correctly
    from app.main import app  # noqa: PLC0415

    with TestClient(app, base_url="https://testserver", raise_server_exceptions=True) as c:
        yield c


@pytest.fixture(scope="session")
def auth_client(client: TestClient):
    """TestClient with a valid session cookie for a unique smoke user."""
    username = f"testuser_{uuid4().hex[:10]}"
    password = "testpassword1"
    # Register
    res = client.post(
        "/auth/register",
        json={
            "username": username,
            "password": password,
            "full_name": "Test User",
            "invite_code": os.getenv("INVITE_CODE", "TESTCODE"),
        },
    )
    assert res.status_code == 200, res.text
    # Login
    res = client.post(
        "/auth/login",
        json={"username": username, "password": password},
    )
    assert res.status_code == 200, res.text
    client._smoke_username = username
    return client


@pytest.fixture(scope="session")
def api_key(auth_client: TestClient) -> str:
    """Return a valid Bearer API key for testuser."""
    res = auth_client.get("/api-key")
    assert res.status_code == 200
    # Reset to get the plain key
    res = auth_client.post("/api-key/reset")
    assert res.status_code == 200
    return res.json()["api_key"]
