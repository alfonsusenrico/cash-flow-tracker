"""Secure defaults and explicit local HTTP configuration."""

import os
from pathlib import Path
import subprocess
import sys

import pytest

from app.core.config import load_settings


@pytest.mark.parametrize("flag", [None, "", " ", "true", "TRUE", "0", "no", "unexpected"])
def test_cookie_security_defaults_to_secure(monkeypatch, flag):
    monkeypatch.setenv("APP_ENV", "test")
    if flag is None:
        monkeypatch.delenv("COOKIE_SECURE", raising=False)
    else:
        monkeypatch.setenv("COOKIE_SECURE", flag)

    assert load_settings().cookie_secure is True


@pytest.mark.parametrize("environment", [None, "production", " Production ", "prod"])
@pytest.mark.parametrize("flag", ["false", " FALSE "])
def test_production_refuses_insecure_cookies(monkeypatch, environment, flag):
    if environment is None:
        monkeypatch.delenv("APP_ENV", raising=False)
    else:
        monkeypatch.setenv("APP_ENV", environment)
    monkeypatch.setenv("COOKIE_SECURE", flag)

    with pytest.raises(RuntimeError, match="COOKIE_SECURE"):
        load_settings()


@pytest.mark.parametrize("environment", ["development", "test"])
@pytest.mark.parametrize("flag", ["false", " FALSE "])
def test_explicit_local_opt_in_allows_http(monkeypatch, environment, flag):
    monkeypatch.setenv("APP_ENV", environment)
    monkeypatch.setenv("COOKIE_SECURE", flag)

    assert load_settings().cookie_secure is False


def isolated_app_environment():
    return {
        "PATH": os.defpath,
        "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
        "DATABASE_URL": "postgresql://localhost/fictional_cookie_test",
        "SESSION_SECRET": "fictional-cookie-test-session",
        "REDIS_URL": "",
    }


def test_production_app_import_fails_before_serving():
    environment = isolated_app_environment()
    environment.update(APP_ENV="production", COOKIE_SECURE="false")
    result = subprocess.run(
        [sys.executable, "-c", "import app.main"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0, "Production app imported with insecure cookies"
    assert "COOKIE_SECURE" in result.stderr
    assert environment["SESSION_SECRET"] not in result.stderr


@pytest.mark.parametrize("local_http", [False, True])
def test_actual_app_session_cookie_header(local_http):
    environment = isolated_app_environment()
    if local_http:
        environment.update(APP_ENV="development", COOKIE_SECURE="false")
    script = """
from fastapi import Request
from fastapi.testclient import TestClient
from app.main import app

@app.get('/cookie-security-test')
def create_session(request: Request):
    request.session['cookie_test'] = True
    return {'ok': True}

# No lifespan: this synthetic route requires neither DB nor background workers.
client = TestClient(app, base_url='http://testserver')
response = client.get('/cookie-security-test')
assert response.status_code == 200
attributes = {item.strip().lower() for item in response.headers['set-cookie'].split(';')[1:]}
assert 'httponly' in attributes
assert ('secure' in attributes) == EXPECT_SECURE, 'Unexpected Secure cookie attribute'
""".replace("EXPECT_SECURE", str(not local_http))
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
