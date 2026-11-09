"""Companion health reports: auth, validation, rate limit, readiness, latest-only storage and scoping.

Users, device names, ids and keys are synthetic.
"""
import itertools
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = ROOT / "db/migrations"
V42 = MIGRATIONS / "V42__device_health.sql"
# The phone sends no Accept-Language and the key names no account, so the message is English (localize-server-text D1).
INVALID_KEY = {"ok": False, "detail": {
    "code": "device_key_invalid", "params": {}, "message": "The device key was rejected. Pair the phone again.",
}}
HEALTH_FIELDS = {
    "reported_at", "ready", "app_version", "listener_connected", "notification_access", "post_notifications",
    "battery_unrestricted", "self_test", "last_capture_at", "queue_pending",
}


class UnlimitedLimiter:
    def exceeded(self, *_args) -> bool:
        return False

    def reset(self, *_keys) -> None:
        pass


@pytest.fixture(autouse=True)
def unlimited_rate_limits(monkeypatch):
    import app.routers.auth as auth_router
    import app.routers.devices as devices_router

    monkeypatch.setattr(auth_router, "rate_limiter", UnlimitedLimiter())
    monkeypatch.setattr(devices_router, "rate_limiter", UnlimitedLimiter())


def browser() -> TestClient:
    from app.main import app

    session = TestClient(app, base_url="https://testserver")
    session.headers["Origin"] = "https://testserver"
    return session


def phone(key: str | None = None) -> TestClient:
    from app.main import app

    client = TestClient(app, base_url="https://testserver")
    if key is not None:
        client.headers["Authorization"] = f"Bearer {key}"
    return client


@pytest.fixture
def make_owner(client):
    from app.db.pool import db_conn
    from passlib.hash import bcrypt

    created: list[str] = []

    def make() -> dict:
        username = f"health_{uuid4().hex[:12]}"
        password = "synthetic-pass-1"
        with db_conn() as conn:
            user_id = conn.execute(
                "INSERT INTO users (username, password_hash, invite_code, language) VALUES (%s, %s, 'TEST', 'id') RETURNING id",
                (username, bcrypt.hash(password)),
            ).fetchone()["id"]
            conn.commit()
        created.append(str(user_id))
        session = browser()
        response = session.post("/api/auth/login", json={"username": username, "password": password})
        assert response.status_code == 200, response.text
        return {"id": str(user_id), "web": session}

    yield make
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = ANY(%s::uuid[])", (created,))
        conn.commit()


@pytest.fixture
def owner(make_owner):
    return make_owner()


def pair_phone(web: TestClient, *, device_id: str | None = None, device_name: str = "Synthetic Phone") -> str:
    code = web.post("/api/devices/pairing-codes").json()["code"]
    response = phone().post("/api/devices/pair", json={
        "code": code, "device_id": device_id or f"android-{uuid4().hex[:16]}", "device_name": device_name,
    })
    assert response.status_code == 200, response.text
    return response.json()["api_key"]


def report(**overrides) -> dict:
    body = {
        "app_version": "2.4.0",
        "listener_connected": True,
        "notification_access": True,
        "post_notifications": True,
        "battery_unrestricted": False,
        "self_test": {"status": "passed", "failed_step": None, "at": "2026-11-02T03:00:00+00:00"},
        "last_capture_at": None,
        "queue_pending": 0,
    }
    for key, value in overrides.items():
        if key == "self_test" and isinstance(value, dict):
            body["self_test"] = {**body["self_test"], **value}
        else:
            body[key] = value
    return body


def send(key: str, body: dict):
    return phone(key).post("/api/devices/health", json=body)


def listed(web: TestClient) -> list[dict]:
    response = web.get("/api/devices")
    assert response.status_code == 200, response.text
    return response.json()["devices"]


def health_rows(key_ids: list[str]) -> list[dict]:
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(
            "SELECT * FROM device_health WHERE api_key_id = ANY(%s::uuid[])", (key_ids,),
        ).fetchall()


def device_ids(web: TestClient) -> list[str]:
    return [device["id"] for device in listed(web)]


# --- Auth ------------------------------------------------------------------------------------


def test_a_paired_phone_reports_its_health(owner):
    key = pair_phone(owner["web"])

    response = send(key, report(last_capture_at="2026-11-02T02:59:00+07:00", queue_pending=3))

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ok"] is True
    health = body["health"]
    assert set(health) == HEALTH_FIELDS
    assert health["ready"] is True
    assert health["app_version"] == "2.4.0" and health["queue_pending"] == 3
    assert health["self_test"] == {"status": "passed", "failed_step": None, "at": "2026-11-02T03:00:00+00:00"}
    assert datetime.fromisoformat(health["last_capture_at"]) == datetime(2026, 11, 1, 19, 59, tzinfo=timezone.utc)
    assert datetime.now(timezone.utc) - datetime.fromisoformat(health["reported_at"]) < timedelta(minutes=1)


@pytest.mark.parametrize("header", [None, "", "Bearer ", "Basic abc", "Bearer cfk_not-a-real-key"])
def test_missing_or_unknown_keys_get_401(owner, header):
    pair_phone(owner["web"])
    client = phone()
    if header is not None:
        client.headers["Authorization"] = header

    response = client.post("/api/devices/health", json=report())

    assert response.status_code == 401 and response.json() == INVALID_KEY
    assert health_rows(device_ids(owner["web"])) == []


def test_a_browser_session_alone_cannot_report(owner):
    pair_phone(owner["web"])

    response = owner["web"].post("/api/devices/health", json=report())

    assert response.status_code == 401 and response.json() == INVALID_KEY


def test_a_revoked_key_gets_401_and_its_health_is_deleted(owner):
    key = pair_phone(owner["web"])
    assert send(key, report()).status_code == 200
    device_id = device_ids(owner["web"])[0]

    assert owner["web"].delete(f"/api/devices/{device_id}").status_code == 200
    response = send(key, report(app_version="2.4.1"))

    assert response.status_code == 401 and response.json() == INVALID_KEY
    assert health_rows([device_id]) == []


def test_repairing_a_phone_starts_without_health(owner):
    first = pair_phone(owner["web"], device_id="android-repair")
    assert send(first, report()).status_code == 200

    second = pair_phone(owner["web"], device_id="android-repair")

    assert send(first, report()).status_code == 401
    assert [device["health"] for device in listed(owner["web"])] == [None]
    assert send(second, report()).status_code == 200


# --- Validation ------------------------------------------------------------------------------


def without(field: str) -> dict:
    body = report()
    del body[field]
    return body


@pytest.mark.parametrize("body", [
    report(listener_connected="true"),
    report(notification_access=1),
    report(post_notifications=None),
    report(battery_unrestricted="false"),
    report(queue_pending=-1),
    report(queue_pending=1.5),
    report(queue_pending="0"),
    report(app_version=""),
    report(app_version="2.4.0 beta"),
    report(app_version="v" * 33),
    report(app_version=240),
    report(self_test={"status": "skipped"}),
    report(self_test={"status": "PASSED"}),
    report(self_test={"status": "failed", "failed_step": None}),
    report(self_test={"status": "failed", "failed_step": "network"}),
    report(self_test={"status": "passed", "failed_step": "capture"}),
    report(self_test={"status": "not_run", "failed_step": "post", "at": None}),
    report(self_test={"status": "passed", "at": None}),
    report(self_test={"status": "failed", "failed_step": "server", "at": None}),
    report(self_test={"status": "passed", "at": "2026-11-02T03:00:00"}),
    report(self_test={"status": "passed", "at": 1793588400}),
    report(self_test={"status": "passed", "at": "yesterday"}),
    report(last_capture_at="2026-11-02T10:00:00"),
    report(last_capture_at=1793588400),
    report(self_test="passed"),
    without("app_version"),
    without("self_test"),
    without("last_capture_at"),
    without("queue_pending"),
])
def test_invalid_reports_get_422_and_store_nothing(owner, body):
    key = pair_phone(owner["web"])

    response = send(key, body)

    assert response.status_code == 422, response.text
    assert health_rows(device_ids(owner["web"])) == []


def test_unknown_fields_are_ignored(owner):
    key = pair_phone(owner["web"])
    body = report(future_field={"nested": True})
    body["self_test"]["future_detail"] = "x"

    response = send(key, body)

    assert response.status_code == 200, response.text
    assert "future_field" not in response.json()["health"]
    assert "future_detail" not in response.json()["health"]["self_test"]


def test_not_run_self_test_may_omit_its_time(owner):
    key = pair_phone(owner["web"])

    response = send(key, report(self_test={"status": "not_run", "failed_step": None, "at": None}))

    assert response.status_code == 200, response.text
    assert response.json()["health"]["self_test"] == {"status": "not_run", "failed_step": None, "at": None}
    assert response.json()["health"]["ready"] is False


@pytest.mark.parametrize("step", ["post", "capture", "server"])
def test_failed_self_test_keeps_its_step(owner, step):
    key = pair_phone(owner["web"])

    response = send(key, report(self_test={"status": "failed", "failed_step": step}))

    assert response.status_code == 200, response.text
    assert response.json()["health"]["self_test"]["failed_step"] == step
    assert listed(owner["web"])[0]["health"]["self_test"]["failed_step"] == step


# --- Rate limit ------------------------------------------------------------------------------


def test_reports_are_rate_limited_per_key(owner, monkeypatch):
    import app.routers.devices as devices_router
    from app.core.rate_limit import RateLimiter

    limiter = RateLimiter(redis_url=None)
    monkeypatch.setattr(devices_router, "rate_limiter", limiter)
    key = pair_phone(owner["web"])
    other = pair_phone(owner["web"])

    statuses = [send(key, report(queue_pending=n)).status_code for n in range(30)]
    limited = send(key, report(queue_pending=99))
    other_status = send(other, report()).status_code

    assert statuses == [200] * 30
    assert limited.status_code == 429
    assert limited.json()["detail"]["code"] == "device_health_rate_limited"
    assert limited.headers["retry-after"] == "60"
    assert other_status == 200
    stored = {device["key_prefix"]: device["health"] for device in listed(owner["web"])}
    assert stored[key[:8]]["queue_pending"] == 29


def test_bad_keys_do_not_consume_a_device_bucket(owner, monkeypatch):
    import app.routers.devices as devices_router
    from app.core.rate_limit import RateLimiter

    limiter = RateLimiter(redis_url=None)
    monkeypatch.setattr(devices_router, "rate_limiter", limiter)
    for _ in range(40):
        assert send("cfk_not-a-real-key", report()).status_code == 401

    assert limiter._events == {}


# --- Readiness -------------------------------------------------------------------------------


COMBINATIONS = list(itertools.product([True, False], [True, False], ["passed", "failed", "not_run"], [True, False]))


@pytest.mark.parametrize(("access", "listener", "status", "battery"), COMBINATIONS)
def test_ready_rule(access, listener, status, battery):
    from app.services.device_health import is_ready

    assert is_ready(access, listener, status) is (access and listener and status == "passed")


def test_ready_over_the_api_for_every_combination(owner):
    key = pair_phone(owner["web"])
    seen = []
    for access, listener, status, battery in COMBINATIONS:
        self_test = {"status": status, "failed_step": "capture" if status == "failed" else None,
                     "at": None if status == "not_run" else "2026-11-02T03:00:00+00:00"}
        body = report(notification_access=access, listener_connected=listener, battery_unrestricted=battery,
                      post_notifications=not battery, self_test=self_test)
        response = send(key, body)
        assert response.status_code == 200, response.text
        expected = access and listener and status == "passed"
        assert response.json()["health"]["ready"] is expected
        assert listed(owner["web"])[0]["health"]["ready"] is expected
        seen.append(expected)

    assert len(seen) == 24 and seen.count(True) == 2


# --- Storage and listing ---------------------------------------------------------------------


def test_device_list_shows_null_before_the_first_report(owner):
    pair_phone(owner["web"])
    manual = owner["web"].post("/api/api-key/reset")
    assert manual.status_code == 200

    devices = listed(owner["web"])

    assert len(devices) == 2
    assert all(device["health"] is None for device in devices)


def test_only_the_latest_report_is_kept(owner):
    key = pair_phone(owner["web"])
    first = send(key, report(listener_connected=False, app_version="2.3.9")).json()["health"]
    second = send(key, report(app_version="2.4.0", queue_pending=2)).json()["health"]

    rows = health_rows(device_ids(owner["web"]))
    health = listed(owner["web"])[0]["health"]

    assert len(rows) == 1
    assert health == second
    assert health["app_version"] == "2.4.0" and health["listener_connected"] is True and health["ready"] is True
    assert datetime.fromisoformat(second["reported_at"]) >= datetime.fromisoformat(first["reported_at"])


def test_health_is_owner_scoped(make_owner):
    alice, bob = make_owner(), make_owner()
    alice_key = pair_phone(alice["web"], device_name="Alice Phone")
    bob_key = pair_phone(bob["web"], device_name="Bob Phone")

    assert send(alice_key, report()).status_code == 200
    assert send(bob_key, report(notification_access=False)).status_code == 200

    alice_devices, bob_devices = listed(alice["web"]), listed(bob["web"])
    assert [device["name"] for device in alice_devices] == ["Alice Phone"]
    assert [device["name"] for device in bob_devices] == ["Bob Phone"]
    assert alice_devices[0]["health"]["ready"] is True
    assert bob_devices[0]["health"]["ready"] is False
    # Bob's key reports only for Bob's phone; Alice's health is untouched.
    assert send(bob_key, report(app_version="9.9.9")).status_code == 200
    assert listed(alice["web"])[0]["health"]["app_version"] == "2.4.0"


def test_a_phone_reads_its_own_health_in_the_device_list(owner):
    key = pair_phone(owner["web"])
    send(key, report())

    response = phone(key).get("/api/devices")

    assert response.status_code == 200
    assert response.json()["devices"][0]["health"]["ready"] is True


def test_storing_rechecks_the_key_so_a_revoke_after_auth_leaves_nothing(owner):
    from fastapi import HTTPException
    from app.services import device_health

    key = pair_phone(owner["web"])
    device_id = device_ids(owner["web"])[0]
    owner["web"].delete(f"/api/devices/{device_id}")

    with pytest.raises(HTTPException) as raised:
        device_health.record_report(device_id, device_health.HealthReport(**report()))

    assert raised.value.status_code == 401
    assert health_rows([device_id]) == []
    assert send(key, report()).status_code == 401


# --- Logs ------------------------------------------------------------------------------------


def test_logs_hold_only_key_ids_and_codes(owner, caplog):
    caplog.set_level(logging.DEBUG)
    key = pair_phone(owner["web"], device_name="Synthetic Secret Phone")
    device_id = device_ids(owner["web"])[0]

    send(key, report(self_test={"status": "failed", "failed_step": "capture"}, app_version="7.7.7-secret"))
    send("cfk_not-a-real-key", report())

    assert f"Device health reported key_id={device_id} ready=False self_test=failed failed_step=capture" in caplog.text
    for private in ("Synthetic Secret Phone", key, "7.7.7-secret", "cfk_not-a-real-key"):
        assert private not in caplog.text


# --- Migration -------------------------------------------------------------------------------


def migrate_to_v41(conn):
    paths = sorted(MIGRATIONS.glob("V*.sql"), key=lambda p: int(p.name.split("__")[0][1:]))
    for path in paths:
        if path.name.startswith("V42__"):
            break
        conn.execute(path.read_text())


def test_v42_is_repeatable_and_enforces_consistency(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    with db_conn() as conn:
        # The scratch schema and everything in it are rolled back after verification.
        with conn.transaction(force_rollback=True):
            schema = f"fictional_v42_{uuid4().hex}"
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema)))
            migrate_to_v41(conn)
            conn.execute(V42.read_text())
            conn.execute(V42.read_text())
            user = conn.execute(
                "INSERT INTO users (username, password_hash, invite_code) VALUES ('fictional-v42', 'x', 'x') RETURNING id"
            ).fetchone()["id"]
            key = conn.execute(
                "INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES (%s, 'h', 'cfk_fict') RETURNING id", (user,),
            ).fetchone()["id"]
            base = {
                "api_key_id": key, "app_version": "2.4.0", "listener_connected": True, "notification_access": True,
                "post_notifications": True, "battery_unrestricted": True, "self_test_status": "passed",
                "self_test_failed_step": None, "self_test_at": datetime.now(timezone.utc), "queue_pending": 0,
            }

            def insert(**changes):
                row = {**base, **changes}
                columns = sql.SQL(", ").join(map(sql.Identifier, row))
                values = sql.SQL(", ").join(sql.Placeholder() * len(row))
                conn.execute(sql.SQL("INSERT INTO device_health ({}) VALUES ({})").format(columns, values), list(row.values()))

            for bad, error in [
                ({"self_test_status": "skipped"}, psycopg.errors.CheckViolation),
                ({"self_test_status": "failed"}, psycopg.errors.CheckViolation),
                ({"self_test_failed_step": "post"}, psycopg.errors.CheckViolation),
                ({"self_test_status": "failed", "self_test_failed_step": "network"}, psycopg.errors.CheckViolation),
                ({"self_test_at": None}, psycopg.errors.CheckViolation),
                ({"queue_pending": -1}, psycopg.errors.CheckViolation),
                ({"app_version": "2.4 beta"}, psycopg.errors.CheckViolation),
            ]:
                with pytest.raises(error):
                    with conn.transaction():
                        insert(**bad)

            insert(self_test_status="not_run", self_test_at=None)
            with pytest.raises(psycopg.errors.UniqueViolation):
                with conn.transaction():
                    insert()
            conn.execute("DELETE FROM api_keys WHERE id = %s", (key,))
            assert conn.execute("SELECT COUNT(*) AS n FROM device_health").fetchone()["n"] == 0
