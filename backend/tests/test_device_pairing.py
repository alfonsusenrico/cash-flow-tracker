"""QR device pairing: single-use codes, per-device keys, revoke, manual-key rotation and boundaries.

Users, device names and ids are synthetic.
"""
import hashlib
import logging
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = ROOT / "db/migrations"
V34 = MIGRATIONS / "V34__device_pairing.sql"
CROCKFORD = set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")
INVALID_BODY = {"ok": False, "detail": {"code": "pairing_code_invalid"}}


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


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def browser(cookies=None) -> TestClient:
    from app.main import app

    session = TestClient(app, base_url="https://testserver")
    session.headers["Origin"] = "https://testserver"
    if cookies is not None:
        session.cookies.update(cookies)
    return session


def phone() -> TestClient:
    from app.main import app

    return TestClient(app, base_url="https://testserver")


@pytest.fixture
def make_owner(client):
    from app.db.pool import db_conn
    from passlib.hash import bcrypt

    created: list[str] = []

    def make() -> dict:
        username = f"pair_{uuid4().hex[:12]}"
        password = "synthetic-pass-1"
        with db_conn() as conn:
            user_id = conn.execute(
                "INSERT INTO users (username, password_hash, invite_code) VALUES (%s, %s, 'TEST') RETURNING id",
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


def new_code(web: TestClient) -> dict:
    response = web.post("/api/devices/pairing-codes")
    assert response.status_code == 200, response.text
    return response.json()


def pair(code: str, *, device_id: str | None = None, device_name: str = "TECNO CN7c"):
    return phone().post("/api/devices/pair", json={
        "code": code, "device_id": device_id or f"android-{uuid4().hex[:16]}", "device_name": device_name,
    })


def paired(code: str, **kwargs) -> dict:
    response = pair(code, **kwargs)
    assert response.status_code == 200, response.text
    return response.json()


def bearer_status(key: str) -> int:
    return phone().get("/api/auth/me", headers={"Authorization": f"Bearer {key}"}).status_code


def key_count(user_id: str) -> int:
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute("SELECT COUNT(*) AS n FROM api_keys WHERE user_id = %s", (user_id,)).fetchone()["n"]


def code_count(user_id: str) -> int:
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute("SELECT COUNT(*) AS n FROM pairing_codes WHERE user_id = %s", (user_id,)).fetchone()["n"]


def devices(web: TestClient) -> list[dict]:
    response = web.get("/api/devices")
    assert response.status_code == 200, response.text
    return response.json()["devices"]


def wrong_code(real: str) -> str:
    from app.services.device_pairing import generate_code

    candidate = real
    while candidate == real:
        candidate = generate_code()
    return candidate


# --- Code generation and storage -------------------------------------------------------------


def test_code_is_ten_crockford_characters_from_the_os_csprng(monkeypatch):
    from app.services import device_pairing

    drawn: list[str] = []
    real_choice = device_pairing.secrets.choice

    def recording_choice(alphabet):
        drawn.append(alphabet)
        return real_choice(alphabet)

    monkeypatch.setattr(device_pairing.secrets, "choice", recording_choice)
    codes = {device_pairing.generate_code() for _ in range(200)}

    assert len(codes) == 200
    assert all(len(code) == 10 and set(code) <= CROCKFORD for code in codes)
    assert set(drawn) == {"0123456789ABCDEFGHJKMNPQRSTVWXYZ"} and len(drawn) == 2000
    assert not {"I", "L", "O", "U"} & set(device_pairing.CODE_ALPHABET)


@pytest.mark.parametrize(("raw", "expected"), [
    ("ABCDE12345", "ABCDE12345"),
    (" abcde-12345 ", "ABCDE12345"),
    ("ABCDE 12345", "ABCDE12345"),
    ("OIL0000000", "0110000000"),
    ("ABCDE1234", None),
    ("ABCDE123456", None),
    ("ABCDE1234U", None),
    ("ABCDE1234!", None),
])
def test_codes_are_read_with_crockford_folding(raw, expected):
    from app.services.device_pairing import normalize_code

    assert normalize_code(raw) == expected


def test_new_code_is_stored_only_as_a_hash_and_expires_in_five_minutes(owner):
    from app.db.pool import db_conn

    before = datetime.now(timezone.utc)
    body = new_code(owner["web"])

    code = body["code"]
    assert len(code) == 10 and set(code) <= CROCKFORD
    assert body["expires_in_seconds"] == 300
    expires_at = datetime.fromisoformat(body["expires_at"])
    assert before + timedelta(seconds=295) <= expires_at <= datetime.now(timezone.utc) + timedelta(seconds=301)
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM pairing_codes WHERE user_id = %s", (owner["id"],)).fetchall()
    assert len(rows) == 1
    assert rows[0]["code_hash"] == sha256(code)
    assert code not in repr(rows[0])


def test_qr_payload_names_the_configured_server():
    from app.core.config import settings
    from app.services.device_pairing import qr_payload

    payload = qr_payload("ABCDE12345")

    assert settings.pairing_server_url == "https://finance.alfonsusenrico.com"
    assert payload == "cashflow://pair?v=1&server=https%3A%2F%2Ffinance.alfonsusenrico.com&code=ABCDE12345"
    parts = urlsplit(payload)
    assert (parts.scheme, parts.netloc) == ("cashflow", "pair")
    assert parse_qs(parts.query) == {"v": ["1"], "server": ["https://finance.alfonsusenrico.com"], "code": ["ABCDE12345"]}


def test_created_code_carries_its_qr_payload(owner):
    body = new_code(owner["web"])

    assert body["qr_payload"] == (
        "cashflow://pair?v=1&server=https%3A%2F%2Ffinance.alfonsusenrico.com&code=" + body["code"]
    )


@pytest.mark.parametrize(("raw", "expected"), [
    (None, "https://finance.alfonsusenrico.com"),
    ("https://Finance.Example.test/", "https://finance.example.test"),
    ("https://finance.example.test:8443", "https://finance.example.test:8443"),
])
def test_pairing_server_setting(monkeypatch, raw, expected):
    from app.core.config import load_settings

    if raw is None:
        monkeypatch.delenv("PAIRING_SERVER_URL", raising=False)
    else:
        monkeypatch.setenv("PAIRING_SERVER_URL", raw)
    loaded = load_settings()

    assert loaded.pairing_server_url == expected
    assert loaded.pairing_configuration_error is None


@pytest.mark.parametrize("raw", [
    "http://finance.example.test", "https://finance.example.test/api", "https://finance.example.test?x=1",
    "https://user@finance.example.test", "finance.example.test", "", "https://",
])
def test_invalid_pairing_server_disables_new_codes(monkeypatch, raw, owner):
    import app.services.device_pairing as device_pairing
    from app.core.config import load_settings

    monkeypatch.setenv("PAIRING_SERVER_URL", raw)
    loaded = load_settings()
    assert loaded.pairing_configuration_error == "pairing_configuration_invalid"

    monkeypatch.setattr(device_pairing, "settings", loaded)
    response = owner["web"].post("/api/devices/pairing-codes")

    assert response.status_code == 503
    assert response.json()["detail"] == {"code": "pairing_configuration_invalid"}
    assert code_count(owner["id"]) == 0


def test_runtime_materializer_writes_the_pairing_server(tmp_path):
    output = tmp_path / "runtime.env"
    subprocess.run(
        [sys.executable, str(ROOT / "scripts/materialize_runtime_env.py"), "--allow-missing", "--output", str(output)],
        env={"PATH": os.defpath}, check=True, capture_output=True, timeout=10,
    )

    assert "PAIRING_SERVER_URL=https://finance.alfonsusenrico.com\n" in output.read_text()


# --- Who may create codes --------------------------------------------------------------------


def test_new_code_replaces_the_old_one(owner):
    first = new_code(owner["web"])
    second = new_code(owner["web"])

    assert pair(first["code"]).json() == INVALID_BODY
    assert pair(second["code"]).status_code == 200


def test_bearer_clients_cannot_create_codes(owner):
    key = paired(new_code(owner["web"])["code"])["api_key"]
    codes_before = code_count(owner["id"])

    bearer_only = phone().post("/api/devices/pairing-codes", headers={"Authorization": f"Bearer {key}"})
    bearer_with_session = owner["web"].post("/api/devices/pairing-codes", headers={"Authorization": f"Bearer {key}"})

    for response in (bearer_only, bearer_with_session):
        assert response.status_code == 403
        assert response.json()["detail"] == {"code": "session_required"}
    assert code_count(owner["id"]) == codes_before


def test_anonymous_clients_cannot_create_codes():
    assert phone().post("/api/devices/pairing-codes").status_code == 401


def test_creating_a_code_passes_the_csrf_origin_check(owner):
    foreign = owner["web"].post("/api/devices/pairing-codes", headers={"Origin": "https://attacker.example"})
    no_origin = browser(owner["web"].cookies)
    del no_origin.headers["Origin"]
    missing = no_origin.post("/api/devices/pairing-codes")

    assert foreign.status_code == 403 and missing.status_code == 403
    assert foreign.json()["detail"] == "Invalid request origin"
    assert code_count(owner["id"]) == 0


# --- Exchange --------------------------------------------------------------------------------


def test_successful_pairing_creates_a_device_key(owner):
    from app.db.pool import db_conn

    code = new_code(owner["web"])["code"]
    body = paired(code, device_id="android-0001", device_name="  TECNO   CN7c ")

    assert body["ok"] is True
    assert body["device_name"] == "TECNO CN7c"
    assert body["api_key"].startswith("cfk_") and body["key_prefix"] == body["api_key"][:8]
    assert bearer_status(body["api_key"]) == 200
    listed = devices(owner["web"])
    device = next(item for item in listed if item["name"] == "TECNO CN7c")
    assert device["created_via"] == "pairing" and device["key_prefix"] == body["key_prefix"]
    assert datetime.now(timezone.utc) - datetime.fromisoformat(device["created_at"]) < timedelta(minutes=1)
    assert device["last_used_at"] is not None
    with db_conn() as conn:
        key = conn.execute("SELECT * FROM api_keys WHERE id = %s", (device["id"],)).fetchone()
        used = conn.execute("SELECT used_at, api_key_id FROM pairing_codes WHERE code_hash = %s", (sha256(code),)).fetchone()
    assert key["key_hash"] == sha256(body["api_key"]) and body["api_key"] not in repr(key)
    assert (key["device_id"], key["device_name"], key["revoked_at"]) == ("android-0001", "TECNO CN7c", None)
    assert used["used_at"] is not None and used["api_key_id"] == key["id"]


def test_typed_code_with_separator_and_lowercase_pairs(owner):
    code = new_code(owner["web"])["code"]

    assert pair(f"{code[:5].lower()}-{code[5:].lower()}").status_code == 200


def test_reused_code_returns_410_without_a_key(owner):
    code = new_code(owner["web"])["code"]
    paired(code)
    keys_before = key_count(owner["id"])

    again = pair(code)

    assert again.status_code == 410 and again.json() == INVALID_BODY
    assert key_count(owner["id"]) == keys_before


def test_expired_code_returns_410_without_a_key(owner):
    from app.db.pool import db_conn

    code = new_code(owner["web"])["code"]
    with db_conn() as conn:
        conn.execute(
            "UPDATE pairing_codes SET expires_at = NOW() - INTERVAL '1 second' WHERE code_hash = %s", (sha256(code),),
        )
        conn.commit()
    keys_before = key_count(owner["id"])

    expired = pair(code)

    assert expired.status_code == 410 and expired.json() == INVALID_BODY
    assert key_count(owner["id"]) == keys_before


@pytest.mark.parametrize("code", ["ZZZZZZZZZZ", "TOO-SHORT", "!!!!!!!!!!", ""])
def test_unknown_and_malformed_codes_get_the_same_410(owner, code):
    keys_before = key_count(owner["id"])

    response = pair(code)

    assert response.status_code == 410 and response.json() == INVALID_BODY
    assert key_count(owner["id"]) == keys_before


def test_code_survives_four_wrong_attempts(owner):
    code = new_code(owner["web"])["code"]
    for _ in range(4):
        assert pair(wrong_code(code)).status_code == 410

    assert pair(code).status_code == 200


def test_code_is_burned_after_five_wrong_attempts(owner):
    from app.db.pool import db_conn

    code = new_code(owner["web"])["code"]
    for _ in range(5):
        assert pair(wrong_code(code)).json() == INVALID_BODY
    keys_before = key_count(owner["id"])

    burned = pair(code)

    assert burned.status_code == 410 and burned.json() == INVALID_BODY
    assert key_count(owner["id"]) == keys_before
    with db_conn() as conn:
        row = conn.execute("SELECT failed_attempts, used_at FROM pairing_codes WHERE code_hash = %s", (sha256(code),)).fetchone()
    assert row == {"failed_attempts": 5, "used_at": None}


def test_concurrent_exchanges_of_one_code_create_exactly_one_key(owner):
    from app.db.pool import db_conn

    code = new_code(owner["web"])["code"]
    keys_before = key_count(owner["id"])
    barrier = threading.Barrier(2)

    def exchange(index: int):
        client = phone()
        barrier.wait()
        return client.post("/api/devices/pair", json={
            "code": code, "device_id": f"race-device-{index}", "device_name": f"Synthetic Phone {index}",
        })

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(exchange, range(2)))

    assert sorted(response.status_code for response in responses) == [200, 410]
    assert key_count(owner["id"]) == keys_before + 1
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT device_id FROM api_keys WHERE user_id = %s AND created_via = 'pairing'", (owner["id"],),
        ).fetchall()
    assert len(rows) == 1


def test_repairing_the_same_device_replaces_its_key(owner):
    first = paired(new_code(owner["web"])["code"], device_id="android-repair")
    second = paired(new_code(owner["web"])["code"], device_id="android-repair", device_name="TECNO CN7c Baru")

    assert bearer_status(first["api_key"]) == 401
    assert bearer_status(second["api_key"]) == 200
    names = [device["name"] for device in devices(owner["web"]) if device["created_via"] == "pairing"]
    assert names == ["TECNO CN7c Baru"]


@pytest.mark.parametrize("body", [
    {"device_id": "bad id with spaces", "device_name": "TECNO CN7c"},
    {"device_id": "", "device_name": "TECNO CN7c"},
    {"device_id": "x" * 65, "device_name": "TECNO CN7c"},
    {"device_id": "android-1", "device_name": "   "},
    {"device_id": "android-1", "device_name": "N" * 81},
    {"device_id": "android-1", "device_name": "Phone\u0000Name"},
])
def test_invalid_device_fields_are_rejected_without_using_the_code(owner, body):
    code = new_code(owner["web"])["code"]

    rejected = phone().post("/api/devices/pair", json={"code": code, **body})

    assert rejected.status_code == 422
    assert pair(code).status_code == 200


def test_pair_is_rate_limited_per_client_ip(monkeypatch):
    import app.routers.devices as devices_router
    from app.core.rate_limit import RateLimiter

    limiter = RateLimiter(redis_url=None)
    monkeypatch.setattr(devices_router, "rate_limiter", limiter)
    client = phone()
    statuses = [pair("ZZZZZZZZZZ").status_code for _ in range(10)]
    limited = client.post("/api/devices/pair", json={
        "code": "ZZZZZZZZZZ", "device_id": "android-limit", "device_name": "Synthetic Phone",
    })

    assert statuses == [410] * 10
    assert limited.status_code == 429
    assert limited.json()["detail"] == {"code": "pairing_rate_limited"}
    assert limited.headers["retry-after"] == "60"
    assert set(limiter._events) == {"pair:client:testclient"}


# --- Device list and revoke ------------------------------------------------------------------


def test_revoked_phone_gets_401_while_other_devices_keep_working(owner):
    lost = paired(new_code(owner["web"])["code"], device_id="android-lost", device_name="TECNO CN7c")
    kept = paired(new_code(owner["web"])["code"], device_id="android-kept", device_name="Synthetic Tablet")
    lost_id = next(device["id"] for device in devices(owner["web"]) if device["name"] == "TECNO CN7c")

    revoked = owner["web"].delete(f"/api/devices/{lost_id}")

    assert revoked.status_code == 200 and revoked.json() == {"ok": True}
    assert bearer_status(lost["api_key"]) == 401
    assert bearer_status(kept["api_key"]) == 200
    assert [device["name"] for device in devices(owner["web"])] == ["Synthetic Tablet"]
    assert owner["web"].delete(f"/api/devices/{lost_id}").status_code == 404


def test_a_phone_may_revoke_its_own_key(owner):
    key = paired(new_code(owner["web"])["code"])["api_key"]
    headers = {"Authorization": f"Bearer {key}"}
    own_id = phone().get("/api/devices", headers=headers).json()["devices"][0]["id"]

    assert phone().delete(f"/api/devices/{own_id}", headers=headers).status_code == 200
    assert bearer_status(key) == 401


def test_device_list_and_revoke_are_owner_scoped(make_owner):
    alice, bob = make_owner(), make_owner()
    alice_key = paired(new_code(alice["web"])["code"], device_name="Alice Phone")["api_key"]
    alice_device = devices(alice["web"])[0]["id"]

    assert devices(bob["web"]) == []
    foreign = bob["web"].delete(f"/api/devices/{alice_device}")
    unknown = bob["web"].delete(f"/api/devices/{uuid4()}")

    assert foreign.status_code == 404 and foreign.json()["detail"] == {"code": "device_not_found"}
    assert unknown.status_code == 404 and unknown.json() == foreign.json()
    assert bearer_status(alice_key) == 200
    assert bob["web"].delete("/api/devices/not-a-uuid").status_code == 422


def test_device_list_names_manual_keys(owner):
    manual = owner["web"].post("/api/api-key/reset").json()
    paired(new_code(owner["web"])["code"], device_name="TECNO CN7c")

    listed = {device["name"]: device for device in devices(owner["web"])}

    assert set(listed) == {"API key manual", "TECNO CN7c"}
    assert listed["API key manual"]["created_via"] == "manual"
    assert listed["API key manual"]["key_prefix"] == manual["key_prefix"]
    assert set(listed["TECNO CN7c"]) == {"id", "name", "created_via", "key_prefix", "created_at", "last_used_at"}


# --- Manual key fallback ---------------------------------------------------------------------


def test_rotating_the_manual_key_keeps_paired_phones(owner):
    first_manual = owner["web"].post("/api/api-key/reset").json()
    phone_key = paired(new_code(owner["web"])["code"])["api_key"]

    rotated = owner["web"].post("/api/api-key/reset")

    assert rotated.status_code == 200
    second_manual = rotated.json()
    assert bearer_status(phone_key) == 200
    assert bearer_status(first_manual["api_key"]) == 401
    assert bearer_status(second_manual["api_key"]) == 200
    metadata = owner["web"].get("/api/api-key").json()["api_key"]
    assert metadata["key_prefix"] == second_manual["key_prefix"]
    assert sorted(device["created_via"] for device in devices(owner["web"])) == ["manual", "pairing"]


def test_manual_key_metadata_ignores_paired_and_revoked_keys(owner):
    manual = owner["web"].post("/api/api-key/reset").json()
    manual_id = next(device["id"] for device in devices(owner["web"]) if device["created_via"] == "manual")
    paired(new_code(owner["web"])["code"])

    assert owner["web"].get("/api/api-key").json()["api_key"]["key_prefix"] == manual["key_prefix"]
    owner["web"].delete(f"/api/devices/{manual_id}")
    assert owner["web"].get("/api/api-key").json()["api_key"] is None
    assert bearer_status(manual["api_key"]) == 401


# --- Logs ------------------------------------------------------------------------------------


def test_logs_hold_no_code_key_or_device_name(owner, caplog):
    caplog.set_level(logging.DEBUG)

    first = new_code(owner["web"])
    second = new_code(owner["web"])
    pair(first["code"])
    body = paired(second["code"], device_name="Synthetic Secret Phone")
    pair(second["code"])
    device_id = devices(owner["web"])[0]["id"]
    owner["web"].delete(f"/api/devices/{device_id}")

    assert "Pairing code created" in caplog.text and "Device paired" in caplog.text
    for secret in (first["code"], second["code"], body["api_key"], "Synthetic Secret Phone"):
        assert secret not in caplog.text


# --- Migration -------------------------------------------------------------------------------


def migrate_to_v33(conn):
    paths = sorted(MIGRATIONS.glob("V*.sql"), key=lambda p: int(p.name.split("__")[0][1:]))
    for path in paths:
        if path.name.startswith("V34__"):
            break
        conn.execute(path.read_text())


def test_v34_marks_existing_keys_manual_and_is_repeatable(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    with db_conn() as conn:
        # The scratch schema and everything in it are rolled back after verification.
        with conn.transaction(force_rollback=True):
            schema = f"fictional_v34_{uuid4().hex}"
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema)))
            migrate_to_v33(conn)
            user = conn.execute(
                "INSERT INTO users (username, password_hash, invite_code) VALUES ('fictional-v34', 'x', 'x') RETURNING id"
            ).fetchone()["id"]
            conn.execute(
                "INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES (%s, %s, 'cfk_fict')", (user, sha256("old")),
            )

            conn.execute(V34.read_text())
            conn.execute(V34.read_text())

            key = conn.execute("SELECT created_via, device_id, device_name, revoked_at FROM api_keys").fetchone()
            assert key == {"created_via": "manual", "device_id": None, "device_name": None, "revoked_at": None}
            with pytest.raises(psycopg.errors.CheckViolation):
                with conn.transaction():
                    conn.execute(
                        "INSERT INTO api_keys (user_id, key_hash, key_prefix, created_via) VALUES (%s, %s, 'cfk_bad', 'pairing')",
                        (user, sha256("no-device")),
                    )
            conn.execute(
                """INSERT INTO api_keys (user_id, key_hash, key_prefix, created_via, device_id, device_name)
                   VALUES (%s, %s, 'cfk_one', 'pairing', 'dev-1', 'Phone')""", (user, sha256("one")),
            )
            with pytest.raises(psycopg.errors.UniqueViolation):
                with conn.transaction():
                    conn.execute(
                        """INSERT INTO api_keys (user_id, key_hash, key_prefix, created_via, device_id, device_name)
                           VALUES (%s, %s, 'cfk_two', 'pairing', 'dev-1', 'Phone')""", (user, sha256("two")),
                    )
