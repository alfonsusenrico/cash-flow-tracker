"""Simulate scripts/deploy_remote_release.sh locally: ssh, scp, docker and curl are shims.

The "remote" host is a temporary directory, docker only records its arguments, and the
source archive holds a stub Flyway preflight, so no container, server or database is used.
"""
import io
import os
import stat
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from release_storage import ReleaseStorageError, resolve_backup_dir  # noqa: E402

RELEASE_SHA = "0123456789abcdef0123456789abcdef01234567"
PROJECT_LABEL = "com.docker.compose.project=cashflow-production"

SSH_SHIM = """#!/bin/sh
while [ $# -gt 0 ]; do
  case "$1" in
    -p|-o|-i) shift 2 ;;
    -*) shift ;;
    *) break ;;
  esac
done
shift
exec sh -c "$*"
"""

SCP_SHIM = """#!/bin/sh
for last; do :; done
source=""
for argument; do
  case "$argument" in
    -P|-o|-i) skip=1; continue ;;
  esac
  if [ "${skip:-0}" = 1 ]; then skip=0; continue; fi
  case "$argument" in -*) continue ;; esac
  if [ "$argument" != "$last" ]; then source=$argument; fi
done
cp "$source" "${last#*:}"
"""

DOCKER_SHIM = """#!/bin/sh
printf 'docker %s | profiles=%s backup_dir=%s uid=%s\\n' "$*" "${COMPOSE_PROFILES:-}" \
  "${BACKUP_HOST_DIR:-}" "${BACKUP_UID:-}" >> "$DEPLOY_SIM_LOG"
case " $* " in
  *" run --rm backup "*)
    if [ "${DEPLOY_SIM_FAIL_BACKUP:-0}" = 1 ]; then exit 1; fi ;;
esac
exit 0
"""

PREFLIGHT_STUB = """import os
with open(os.environ["DEPLOY_SIM_LOG"], "a", encoding="utf-8") as log:
    log.write("flyway-v14-preflight\\n")
"""


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _source_archive(path: Path) -> None:
    data = PREFLIGHT_STUB.encode()
    with tarfile.open(path, "w:gz") as archive:
        member = tarfile.TarInfo("scripts/flyway_v14_preflight.py")
        member.size = len(data)
        archive.addfile(member, io.BytesIO(data))


def _deploy(tmp_path: Path, *, fail_backup: bool = False, runtime_extra: str = ""):
    shims = tmp_path / "bin"
    shims.mkdir()
    _write_executable(shims / "ssh", SSH_SHIM)
    _write_executable(shims / "scp", SCP_SHIM)
    _write_executable(shims / "docker", DOCKER_SHIM)
    _write_executable(shims / "curl", "#!/bin/sh\nexit 0\n")

    archive = tmp_path / "source.tar.gz"
    _source_archive(archive)
    runtime_env = tmp_path / "runtime.env"
    runtime_env.write_text("POSTGRES_DB=ledger\n" + runtime_extra, encoding="utf-8")
    log = tmp_path / "deploy.log"
    log.touch()
    deploy_root = tmp_path / "srv"

    environment = {
        "PATH": f"{shims}{os.pathsep}{os.environ['PATH']}",
        "HOME": str(tmp_path),
        "DEPLOY_HOST": "deploy-host.invalid",
        "DEPLOY_USER": "deployer",
        "DEPLOY_ROOT": str(deploy_root),
        "CI_COMMIT_SHA": RELEASE_SHA,
        "RUNTIME_ENV_FILE": str(runtime_env),
        "RELEASE_SOURCE_ARCHIVE": str(archive),
        "DEPLOY_SIM_LOG": str(log),
        "DEPLOY_SIM_FAIL_BACKUP": "1" if fail_backup else "0",
    }
    completed = subprocess.run(
        ["sh", str(ROOT / "scripts/deploy_remote_release.sh")],
        cwd=ROOT, env=environment, capture_output=True, text=True, timeout=60,
    )
    return completed, log.read_text(encoding="utf-8").splitlines(), deploy_root / "production"


def _index(lines: list[str], fragment: str) -> int:
    matches = [number for number, line in enumerate(lines) if fragment in line]
    assert matches, f"{fragment!r} not in deploy log:\n" + "\n".join(lines)
    return matches[0]


def test_deploy_backs_up_before_preflight_and_migration_and_prunes_only_project_images(tmp_path):
    completed, lines, environment_root = _deploy(tmp_path)

    assert completed.returncode == 0, completed.stderr
    backup_dir = (environment_root / "backups").resolve()
    build = _index(lines, " build | ")
    backup = _index(lines, f"run --rm backup backup-now pre-migrate-{RELEASE_SHA[:12]}")
    preflight = _index(lines, "flyway-v14-preflight")
    migrate = _index(lines, "run --rm migrate")
    up = _index(lines, " up --detach")
    assert build < backup < preflight < migrate < up
    assert f"profiles=backup backup_dir={backup_dir} uid={os.getuid()}" in lines[up]
    assert stat.S_IMODE(backup_dir.stat().st_mode) == 0o700
    assert (environment_root / "current").resolve() == (environment_root / "releases" / RELEASE_SHA).resolve()

    prunes = [line.split(" | ")[0] for line in lines if " image prune" in line]
    assert prunes == [
        "docker image prune --force --filter label=cash-flow-tracker.managed=true",
        f"docker image prune --force --filter label={PROJECT_LABEL}",
    ]


def test_failing_pre_migration_backup_aborts_before_preflight_and_migration(tmp_path):
    completed, lines, environment_root = _deploy(tmp_path, fail_backup=True)

    assert completed.returncode != 0
    assert "Pre-migration backup failed; aborting before any migration." in completed.stderr
    _index(lines, "run --rm backup backup-now")
    assert not any("flyway-v14-preflight" in line for line in lines)
    assert not any("run --rm migrate" in line for line in lines)
    assert not any(" up " in line or " image prune" in line for line in lines)
    assert not (environment_root / "current").exists()


def test_backup_dir_inside_release_tree_aborts_before_build(tmp_path):
    inside = tmp_path / "srv/production/releases/backups"
    completed, lines, _ = _deploy(tmp_path, runtime_extra=f"BACKUP_HOST_DIR={inside}\n")

    assert completed.returncode != 0
    assert "must not be inside" in completed.stderr
    assert not any(" build" in line or " run " in line for line in lines)


def _env_file(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "runtime.env"
    path.write_text(content, encoding="utf-8")
    return path


def test_backup_dir_defaults_under_environment_root(tmp_path):
    root = tmp_path / "production"
    resolved = resolve_backup_dir(root, _env_file(tmp_path, "SESSION_SECRET=fictional\n"))

    assert resolved == (root / "backups").resolve()
    assert stat.S_IMODE(resolved.stat().st_mode) == 0o700


def test_backup_dir_uses_the_last_configured_value(tmp_path):
    first, chosen = tmp_path / "first", tmp_path / "chosen"
    env_file = _env_file(tmp_path, f"BACKUP_HOST_DIR={first}\nBACKUP_HOST_DIR={chosen}\n")

    assert resolve_backup_dir(tmp_path / "production", env_file) == chosen.resolve()
    assert not first.exists()


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("relative/backups", "absolute path"),
        ("/srv/back:ups", "without ':'"),
        ("/", "dedicated directory"),
        ("/var/lib/docker/volumes/dumps", "must not be inside"),
    ],
)
def test_backup_dir_rejects_unsafe_values(tmp_path, value, message):
    env_file = _env_file(tmp_path, f"BACKUP_HOST_DIR={value}\n")

    with pytest.raises(ReleaseStorageError, match=message):
        resolve_backup_dir(tmp_path / "production", env_file)


def test_backup_dir_rejects_the_environment_root_and_release_tree(tmp_path):
    root = tmp_path / "production"
    for value in (root, root / "releases" / RELEASE_SHA / "dumps"):
        with pytest.raises(ReleaseStorageError):
            resolve_backup_dir(root, _env_file(tmp_path, f"BACKUP_HOST_DIR={value}\n"))
