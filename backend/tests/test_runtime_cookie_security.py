"""Production cookie materialization uses only synthetic, isolated inputs."""

import os
from pathlib import Path
import re
import stat
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]


def materialize_runtime(tmp_path, cookie_flag=None, seed_cookie=None):
    environment = {"PATH": os.defpath}
    if cookie_flag is not None:
        environment["COOKIE_SECURE"] = cookie_flag
    output = tmp_path / "runtime.env"
    command = [
        sys.executable,
        str(ROOT / "scripts/materialize_runtime_env.py"),
        "--allow-missing",
        "--output",
        str(output),
    ]
    if seed_cookie is not None:
        seed = tmp_path / "synthetic-seed.env"
        seed.write_text(f"COOKIE_SECURE={seed_cookie}\nTZ=UTC\n", encoding="utf-8")
        command.extend(["--env-file", str(seed)])
    subprocess.run(command, env=environment, check=True, capture_output=True, timeout=10)
    values = dict(
        line.split("=", 1)
        for line in output.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    return values


@pytest.mark.parametrize("cookie_flag", [None, "", " "])
@pytest.mark.parametrize("seed_cookie", [None, "false", "true"])
def test_production_materializer_defaults_to_secure(tmp_path, cookie_flag, seed_cookie):
    values = materialize_runtime(tmp_path, cookie_flag, seed_cookie)

    assert values["COOKIE_SECURE"] == "true"
    if seed_cookie is not None:
        assert values["TZ"] == "UTC", "Unrelated seed precedence must remain unchanged"


@pytest.mark.parametrize("cookie_flag", ["true", "false"])
def test_explicit_cookie_override_preserved_for_startup_validation(tmp_path, cookie_flag):
    values = materialize_runtime(tmp_path, cookie_flag, seed_cookie="true")

    assert values["COOKIE_SECURE"] == cookie_flag


def test_workflow_unset_cookie_secret_materializes_secure(tmp_path):
    workflow = (ROOT / ".github/workflows/deploy.yml").read_text(encoding="utf-8")
    match = re.search(r"^\s+COOKIE_SECURE:\s*\$\{\{\s*secrets.COOKIE_SECURE(.*?)\}\}", workflow, re.M)
    assert match is not None
    fallback = re.search(r"\|\|\s*'([^']*)'", match.group(1))
    # GitHub evaluates an unset secret to empty, or to the expression's fallback.
    cookie_flag = fallback.group(1) if fallback else ""

    assert materialize_runtime(tmp_path, cookie_flag)["COOKIE_SECURE"] == "true"


def test_base_compose_defaults_to_secure():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    cookie = re.search(r"COOKIE_SECURE:\s*\$\{COOKIE_SECURE:-([^}]+)\}", compose)
    assert cookie is not None
    assert cookie.group(1) == "true"


def test_local_compose_override_is_explicit_and_non_production():
    assert not (ROOT / "docker-compose.override.yml").exists()
    local = (ROOT / "docker-compose.local.yml").read_text(encoding="utf-8")
    assert re.search(r'COOKIE_SECURE:\s*[\"\']false[\"\']', local)
    assert re.search(r"APP_ENV:\s*development", local)
