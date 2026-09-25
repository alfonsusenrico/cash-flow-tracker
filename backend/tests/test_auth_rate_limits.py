from dataclasses import replace

from fastapi import HTTPException
from fastapi.testclient import TestClient


class RecordingLimiter:
    def __init__(self, exceeded_after: int):
        self.exceeded_after = exceeded_after
        self.counts: dict[str, int] = {}
        self.reset_keys: list[str] = []

    def exceeded(self, key: str, _limit: int, _window: int) -> bool:
        count = self.counts.get(key, 0)
        self.counts[key] = count + 1
        return count >= self.exceeded_after

    def reset(self, *keys: str) -> None:
        self.reset_keys.extend(keys)
        for key in keys:
            self.counts.pop(key, None)


def test_login_rate_limit_is_generic_and_includes_retry_after(monkeypatch):
    import app.routers.auth as auth_router
    from app.core.config import settings
    from app.main import app

    limiter = RecordingLimiter(exceeded_after=1)
    monkeypatch.setattr(auth_router, "rate_limiter", limiter)
    monkeypatch.setattr(auth_router, "settings", replace(settings, login_rate_limit=1, login_user_rate_limit=5))
    monkeypatch.setattr(
        auth_router,
        "authenticate_user",
        lambda *_: (_ for _ in ()).throw(HTTPException(status_code=401, detail="Invalid username or password")),
    )

    client = TestClient(app, base_url="https://testserver")
    first = client.post("/api/auth/login", json={"username": "UnknownName", "password": "wrong-password"})
    limited = client.post("/api/auth/login", json={"username": "UnknownName", "password": "wrong-password"})

    assert first.status_code == 401
    assert first.json()["detail"] == "Invalid username or password"
    assert limited.status_code == 429
    assert limited.headers.get("retry-after") == str(settings.login_rate_window), dict(limited.headers)
    assert "UnknownName" not in limited.text


def test_successful_login_resets_only_normalized_user_failures(monkeypatch):
    import app.routers.auth as auth_router
    from app.core.config import settings
    from app.main import app

    limiter = RecordingLimiter(exceeded_after=10)
    monkeypatch.setattr(auth_router, "rate_limiter", limiter)
    monkeypatch.setattr(auth_router, "settings", replace(settings, login_rate_limit=10, login_user_rate_limit=10))
    monkeypatch.setattr(
        auth_router,
        "authenticate_user",
        lambda *_: {"id": "user-id", "username": "owner", "currency": "IDR", "payday_day": 25},
    )

    response = TestClient(app, base_url="https://testserver").post(
        "/api/auth/login",
        json={"username": " Owner ", "password": "valid-password"},
    )

    assert response.status_code == 200
    assert limiter.reset_keys == ["login:user:owner"]


def test_registration_rate_limit_is_client_scoped_and_generic(monkeypatch):
    import app.routers.auth as auth_router
    from app.core.config import settings
    from app.main import app

    limiter = RecordingLimiter(exceeded_after=1)
    monkeypatch.setattr(auth_router, "rate_limiter", limiter)
    monkeypatch.setattr(
        auth_router,
        "settings",
        replace(settings, register_rate_limit=1, register_rate_window=600),
    )
    monkeypatch.setattr(
        auth_router,
        "register_user",
        lambda *_args: (_ for _ in ()).throw(HTTPException(status_code=400, detail="Invalid invite code")),
    )

    client = TestClient(app, base_url="https://testserver")
    payload = {
        "username": "new-user",
        "password": "valid-password",
        "invite_code": "invalid",
    }
    first = client.post("/api/auth/register", json=payload)
    limited = client.post("/api/auth/register", json=payload)

    assert first.status_code == 400
    assert limited.status_code == 429
    assert limited.headers.get("retry-after") == "600"
    assert "new-user" not in limited.text
