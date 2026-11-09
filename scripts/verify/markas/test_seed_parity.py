"""Guard tests for seed_parity.py; no database is needed (apply-markas-shell D13).

Each refused run must exit non-zero and write nothing: the fake connection records every statement and
fails the test on any write or commit.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).with_name("seed_parity.py")
spec = importlib.util.spec_from_file_location("seed_parity", SCRIPT)
seed_parity = importlib.util.module_from_spec(spec)
# Dataclasses resolve their annotations through sys.modules, so the module is registered first.
sys.modules["seed_parity"] = seed_parity
spec.loader.exec_module(seed_parity)

GOOD_URL = "postgresql://ledger:not-a-secret@127.0.0.1:55450/cft_markas_parity"
ENV = {"MARKAS_PASSWORD": "contoh-sandi-123"}
READ_ONLY = re.compile(r"^\s*SELECT\b", re.IGNORECASE)


class FakeCursor:
    def __init__(self, connection: "FakeConnection"):
        self.connection = connection
        self.result: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql: str, params=None) -> None:
        self.connection.statements.append(sql)
        if not READ_ONLY.match(sql):
            raise AssertionError(f"a refused run wrote to the database: {sql.split()[0]}")
        if "to_regclass" in sql:
            self.result = [{"present": True}]
        elif "flyway_schema_history" in sql:
            self.result = [{"version": str(version)} for version in self.connection.versions]
        elif "FROM users" in sql:
            self.result = [{"count": self.connection.user_count}]
        else:
            self.result = []

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return list(self.result)


class FakeConnection:
    def __init__(self, *, versions=(1, 50, 51), user_count=0):
        self.versions = list(versions)
        self.user_count = user_count
        self.statements: list[str] = []
        self.committed = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def synthetic_data(tmp_path: Path, *, synthetic=True) -> Path:
    meta = {"today": "2026-11-05", "currency": "IDR"}
    if synthetic is not None:
        meta["synthetic"] = synthetic
    path = tmp_path / "data.js"
    path.write_text("window.CFT_DATA = " + json.dumps({"meta": meta}) + ";\n")
    return path


def run(tmp_path: Path, *, url=GOOD_URL, env=ENV, connection: FakeConnection | None = None,
        synthetic=True, extra=()) -> tuple[int, list[FakeConnection]]:
    opened: list[FakeConnection] = []

    def connect(_url: str) -> FakeConnection:
        conn = connection or FakeConnection()
        opened.append(conn)
        return conn

    argv = ["--data", str(synthetic_data(tmp_path, synthetic=synthetic)), "--database-url", url,
            "--today", "2026-11-05", "--check-out", str(tmp_path / "out"), *extra]
    code = seed_parity.main(argv, env=dict(env), connect=connect)
    return code, opened


def assert_nothing_written(tmp_path: Path, opened: list[FakeConnection]) -> None:
    for conn in opened:
        assert not conn.committed
        assert all(READ_ONLY.match(sql) for sql in conn.statements)
    assert not (tmp_path / "out").exists()


def test_refuses_a_host_that_is_not_loopback(tmp_path):
    code, opened = run(tmp_path, url="postgresql://ledger:x@db.example.com:5432/cft_markas_parity")
    assert code != 0
    assert opened == []
    assert_nothing_written(tmp_path, opened)


def test_refuses_a_database_name_without_the_markas_prefix(tmp_path):
    code, opened = run(tmp_path, url="postgresql://ledger:x@127.0.0.1:55450/ledger")
    assert code != 0
    assert opened == []
    assert_nothing_written(tmp_path, opened)


def test_refuses_existing_users_without_reset(tmp_path):
    connection = FakeConnection(user_count=1)
    code, opened = run(tmp_path, connection=connection)
    assert code != 0
    assert opened == [connection]
    assert any("FROM users" in sql for sql in connection.statements)
    assert_nothing_written(tmp_path, opened)


def test_refuses_data_without_meta_synthetic(tmp_path):
    code, opened = run(tmp_path, synthetic=None)
    assert code != 0
    assert opened == []
    assert_nothing_written(tmp_path, opened)


def test_refuses_a_schema_at_another_version(tmp_path):
    connection = FakeConnection(versions=(1, 50, 52))
    code, opened = run(tmp_path, connection=connection)
    assert code != 0
    assert opened == [connection]
    assert not any("FROM users" in sql for sql in connection.statements)
    assert_nothing_written(tmp_path, opened)


def test_refuses_a_missing_password(tmp_path):
    code, opened = run(tmp_path, env={})
    assert code != 0
    assert opened == []
    assert_nothing_written(tmp_path, opened)


def test_reset_is_refused_outside_a_markas_database():
    target = seed_parity.DatabaseTarget(host="127.0.0.1", port=55450, name="ledger")
    with pytest.raises(seed_parity.SeedRefusal):
        seed_parity.reset_application_tables(FakeConnection().cursor(), target)


def test_accepts_loopback_markas_targets():
    for url in (GOOD_URL, "postgresql://ledger@localhost/cft_markas_audit", "postgresql://ledger@[::1]:5432/cft_markas_x"):
        assert seed_parity.database_target(url).name.startswith("cft_markas_")


TINJAU_SCRIPT = Path(__file__).with_name("seed_tinjau.py")
tinjau_spec = importlib.util.spec_from_file_location("seed_tinjau", TINJAU_SCRIPT)
seed_tinjau = importlib.util.module_from_spec(tinjau_spec)
tinjau_spec.loader.exec_module(seed_tinjau)


@pytest.mark.parametrize("url", [
    "postgresql://ledger:not-a-secret@10.0.0.5:5432/cft_markas_parity",
    "postgresql://ledger:not-a-secret@127.0.0.1:5432/ledger",
])
def test_tinjau_seed_refuses_a_database_that_is_not_a_local_markas_one(url, capsys):
    assert seed_tinjau.main(["--database-url", url, "--username", "dimas_contoh", "--kinds"]) == 2
    assert "refused" in capsys.readouterr().err


def test_tinjau_seed_stress_merchant_is_sixty_characters():
    assert len(seed_tinjau.LONG_MERCHANT) == 60
    assert seed_tinjau.STRESS_WAITING_ROWS + seed_tinjau.STRESS_GROUP_ROWS > 1000


# --- seed_rekening.py guards (apply-markas-rekening D22); no backend or database is needed. -------------------

REKENING = Path(__file__).with_name("seed_rekening.py")
rekening_spec = importlib.util.spec_from_file_location("seed_rekening", REKENING)
seed_rekening = importlib.util.module_from_spec(rekening_spec)
rekening_spec.loader.exec_module(seed_rekening)


def test_rekening_seed_refuses_a_backend_off_loopback():
    with pytest.raises(SystemExit, match="not a loopback backend"):
        seed_rekening.Api("http://192.168.1.20:8000", "http://127.0.0.1:3750")


@pytest.mark.parametrize("url", [
    "postgresql://ledger:x@10.0.0.5:5432/cft_markas_rk",
    "postgresql://ledger:x@127.0.0.1:5432/ledger",
])
def test_rekening_flag_refuses_anything_but_a_loopback_markas_database(url):
    with pytest.raises(SystemExit, match="refused"):
        seed_rekening.set_flag(url, "dimas_contoh", "Tagihan Rumah")


def test_rekening_month_shift_clamps_to_the_month_end():
    from datetime import date

    assert seed_rekening.months_back(date(2026, 3, 31), 1) == date(2026, 2, 28)
    assert seed_rekening.months_back(date(2026, 1, 8), 1) == date(2025, 12, 8)
