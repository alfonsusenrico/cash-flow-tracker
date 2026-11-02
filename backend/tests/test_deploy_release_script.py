"""Simulate scripts/deploy_remote_release.sh locally: ssh, scp, docker, df and curl are shims.

The "remote" host is a temporary directory, docker only records its arguments, df reports a chosen
amount of free space, and the source archive holds a stub Flyway preflight, so no container, server
or database is used.
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
from release_storage import ReleaseStorageError, resolve_backup_dir, resolve_min_free_gb  # noqa: E402

RELEASE_SHA = "0123456789abcdef0123456789abcdef01234567"
PREVIOUS_SHA = "fedcba9876543210fedcba9876543210fedcba98"
GIB_IN_KIB = 1024 * 1024
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
  " info "*)
    printf '%s\\n' "$DEPLOY_SIM_DOCKER_ROOT" ;;
esac
exit 0
"""

# Reports DEPLOY_SIM_FREE_KIB available, or DEPLOY_SIM_BACKUP_FREE_KIB for the backup directory when
# set, in the POSIX format of `df -Pk`.
DF_SHIM = """#!/bin/sh
for last; do :; done
printf 'df %s\\n' "$last" >> "$DEPLOY_SIM_LOG"
available=$DEPLOY_SIM_FREE_KIB
case "$last" in
  */backups) available=${DEPLOY_SIM_BACKUP_FREE_KIB:-$available} ;;
esac
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\\n'
printf '/dev/sim 999999999 1 %s 1%% /\\n' "$available"
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


def _deploy(
    tmp_path: Path,
    *,
    fail_backup: bool = False,
    runtime_extra: str = "",
    free_kib: int | str = 50 * GIB_IN_KIB,
    backup_free_kib: int | None = None,
):
    shims = tmp_path / "bin"
    shims.mkdir()
    _write_executable(shims / "ssh", SSH_SHIM)
    _write_executable(shims / "scp", SCP_SHIM)
    _write_executable(shims / "docker", DOCKER_SHIM)
    _write_executable(shims / "df", DF_SHIM)
    _write_executable(shims / "curl", "#!/bin/sh\nexit 0\n")

    archive = tmp_path / "source.tar.gz"
    _source_archive(archive)
    runtime_env = tmp_path / "runtime.env"
    runtime_env.write_text("POSTGRES_DB=ledger\n" + runtime_extra, encoding="utf-8")
    log = tmp_path / "deploy.log"
    log.touch()
    deploy_root = tmp_path / "srv"
    docker_root = tmp_path / "docker-data"
    docker_root.mkdir()

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
        "DEPLOY_SIM_DOCKER_ROOT": str(docker_root),
        "DEPLOY_SIM_FREE_KIB": str(free_kib),
    }
    if backup_free_kib is not None:
        environment["DEPLOY_SIM_BACKUP_FREE_KIB"] = str(backup_free_kib)
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
    disk_checks = [_index(lines, f"df {tmp_path / 'docker-data'}"), _index(lines, f"df {backup_dir}")]
    build = _index(lines, " build | ")
    assert max(disk_checks) < build
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


def _seed_running_release(environment_root: Path) -> None:
    """A release that is already live, which a refused deploy must leave in place."""
    (environment_root / "releases" / PREVIOUS_SHA).mkdir(parents=True)
    (environment_root / "current").symlink_to(f"releases/{PREVIOUS_SHA}")


def _assert_nothing_built_or_written(lines: list[str], environment_root: Path) -> None:
    assert not any(" build" in line or " run " in line or " up " in line for line in lines)
    assert not any("flyway-v14-preflight" in line or " image prune" in line for line in lines)
    assert not (environment_root / "releases" / RELEASE_SHA).exists()
    assert os.readlink(environment_root / "current") == f"releases/{PREVIOUS_SHA}"


def test_low_disk_aborts_before_build_backup_and_migration(tmp_path):
    environment_root = tmp_path / "srv/production"
    _seed_running_release(environment_root)

    completed, lines, _ = _deploy(tmp_path, free_kib=285 * 1024)

    assert completed.returncode != 0
    assert (
        f"Not enough free disk space for Docker data at {tmp_path / 'docker-data'}: 285 MiB free, "
        "below DEPLOY_MIN_FREE_GB=5 GiB." in completed.stderr
    )
    assert "the running release is untouched" in completed.stderr
    _assert_nothing_built_or_written(lines, environment_root)


def test_configured_threshold_aborts_when_backup_disk_is_below_it(tmp_path):
    environment_root = tmp_path / "srv/production"
    _seed_running_release(environment_root)

    completed, lines, _ = _deploy(
        tmp_path, runtime_extra="DEPLOY_MIN_FREE_GB=8\n", free_kib=50 * GIB_IN_KIB, backup_free_kib=6 * GIB_IN_KIB
    )

    assert completed.returncode != 0
    assert "💽 Docker data: 51200 MiB free" in completed.stdout
    backup_dir = (environment_root / "backups").resolve()
    assert (
        f"Not enough free disk space for database backups at {backup_dir}: 6144 MiB free, "
        "below DEPLOY_MIN_FREE_GB=8 GiB." in completed.stderr
    )
    _assert_nothing_built_or_written(lines, environment_root)


def test_free_space_at_the_threshold_proceeds_and_reports_it(tmp_path):
    environment_root = tmp_path / "srv/production"
    _seed_running_release(environment_root)

    completed, lines, _ = _deploy(tmp_path, free_kib=5 * GIB_IN_KIB)

    assert completed.returncode == 0, completed.stderr
    assert f"💽 Docker data: 5120 MiB free at {tmp_path / 'docker-data'} (minimum 5 GiB)." in completed.stdout
    assert "💽 database backups: 5120 MiB free at" in completed.stdout
    assert _index(lines, f"df {tmp_path / 'docker-data'}") < _index(lines, " build | ")
    _index(lines, "run --rm backup backup-now")
    _index(lines, "run --rm migrate")
    assert (environment_root / "current").resolve() == (environment_root / "releases" / RELEASE_SHA).resolve()


@pytest.mark.parametrize("value", ["0", "-1", "2.5", "5GB", "10000", "05"])
def test_invalid_threshold_aborts_before_build(tmp_path, value):
    environment_root = tmp_path / "srv/production"
    _seed_running_release(environment_root)

    completed, lines, _ = _deploy(tmp_path, runtime_extra=f"DEPLOY_MIN_FREE_GB={value}\n")

    assert completed.returncode != 0
    assert "DEPLOY_MIN_FREE_GB must be a whole number of GiB from 1 to 9999" in completed.stderr
    assert not any(line.startswith("df ") for line in lines)
    _assert_nothing_built_or_written(lines, environment_root)


def test_unreadable_free_space_aborts_before_build(tmp_path):
    environment_root = tmp_path / "srv/production"
    _seed_running_release(environment_root)

    completed, lines, _ = _deploy(tmp_path, free_kib="unknown")

    assert completed.returncode != 0
    assert "Could not read the free space for Docker data" in completed.stderr
    _assert_nothing_built_or_written(lines, environment_root)


@pytest.mark.parametrize(
    ("content", "expected"),
    [("", 5), ("DEPLOY_MIN_FREE_GB=\n", 5), ("DEPLOY_MIN_FREE_GB=3\nDEPLOY_MIN_FREE_GB=12\n", 12)],
)
def test_min_free_gb_defaults_to_five_and_uses_the_last_value(tmp_path, content, expected):
    assert resolve_min_free_gb(_env_file(tmp_path, content)) == expected


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
