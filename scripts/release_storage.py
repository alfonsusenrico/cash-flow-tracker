#!/usr/bin/env python3
"""Manage safe immutable release directories for Cash Flow Tracker.

Releases are stored in <environment_root>/releases/<commit_sha>/.
Atomic promotion updates the symlink <environment_root>/current -> releases/<commit_sha>.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path

SHA_PATTERN = re.compile(r"^[0-9a-f]{7,40}$")
RELEASE_DIRECTORY_MODE = 0o750
PRIVATE_FILE_MODE = 0o600
DEFAULT_RETAINED_RELEASES = 3


class ReleaseStorageError(ValueError):
    """Raised when a release path or state is invalid."""


def _validate_sha(release_sha: str) -> str:
    if not SHA_PATTERN.fullmatch(release_sha):
        raise ReleaseStorageError(f"Invalid release SHA format: {release_sha}")
    return release_sha


def _lstat(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def _require_directory(path: Path, *, create: bool, mode: int = RELEASE_DIRECTORY_MODE) -> None:
    status = _lstat(path)
    if status is not None and stat.S_ISLNK(status.st_mode):
        raise ReleaseStorageError(f"Release path must not be a symlink: {path}")
    if status is not None and not stat.S_ISDIR(status.st_mode):
        raise ReleaseStorageError(f"Release path must be a directory: {path}")
    if status is None:
        if not create:
            raise ReleaseStorageError(f"Required release directory is missing: {path}")
        path.mkdir(mode=mode, parents=True, exist_ok=True)
        status = _lstat(path)
        if status is None:
            raise ReleaseStorageError(f"Failed to create directory: {path}")

    try:
        os.chmod(path, mode)
    except PermissionError:
        pass


def _release_paths(environment_root: Path, release_sha: str) -> tuple[Path, Path]:
    release_sha = _validate_sha(release_sha)
    releases_root = environment_root / "releases"
    release_dir = releases_root / release_sha
    return releases_root, release_dir


def prepare_release(environment_root: Path, release_sha: str) -> Path:
    """Create and validate release tree before sensitive files are unpacked."""
    _require_directory(environment_root, create=True)
    releases_root, release_dir = _release_paths(environment_root, release_sha)
    _require_directory(releases_root, create=True)
    _require_directory(release_dir, create=True)
    return release_dir


def secure_release_file(release_dir: Path, filename: str) -> None:
    """Ensure a sensitive release file has strict root/user-private permissions."""
    path = release_dir / filename
    status = _lstat(path)
    if status is None or stat.S_ISLNK(status.st_mode) or not stat.S_ISREG(status.st_mode):
        raise ReleaseStorageError(f"Release file is missing or invalid: {path}")
    os.chmod(path, PRIVATE_FILE_MODE)


def promote_current(environment_root: Path, release_sha: str) -> None:
    """Atomically point current -> releases/<release_sha>."""
    releases_root, release_dir = _release_paths(environment_root, release_sha)
    _require_directory(environment_root, create=False)
    _require_directory(releases_root, create=False)
    _require_directory(release_dir, create=False)

    current = environment_root / "current"
    current_status = _lstat(current)
    if current_status is not None and not stat.S_ISLNK(current_status.st_mode):
        raise ReleaseStorageError(f"Current path exists and is not a symlink: {current}")

    temporary = environment_root / f".current-{release_sha}-{os.getpid()}"
    if _lstat(temporary) is not None:
        try:
            temporary.unlink()
        except OSError:
            pass

    os.symlink(f"releases/{release_sha}", temporary)
    os.replace(temporary, current)
    print(f"✔ Successfully promoted current -> releases/{release_sha}")


def _current_release_sha(environment_root: Path) -> str | None:
    current = environment_root / "current"
    status = _lstat(current)
    if status is None or not stat.S_ISLNK(status.st_mode):
        return None
    target = os.readlink(current)
    match = re.fullmatch(r"releases/([0-9a-fA-F0-9_.-]+)", target)
    return match.group(1) if match else None


def _running_release_shas(compose_project: str) -> set[str]:
    try:
        completed = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                f"label=com.docker.compose.project={compose_project}",
                "--format",
                "{{.Image}}",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            return set()
        return {
            image.rsplit(":", 1)[-1]
            for image in completed.stdout.splitlines()
            if SHA_PATTERN.fullmatch(image.rsplit(":", 1)[-1])
        }
    except Exception:
        return set()


def cleanup_releases(
    environment_root: Path,
    *,
    compose_project: str,
    retain_count: int = DEFAULT_RETAINED_RELEASES,
) -> list[str]:
    """Prune older releases, keeping current, active container images, and the last N releases."""
    releases_root = environment_root / "releases"
    if not releases_root.exists():
        return []

    current_sha = _current_release_sha(environment_root)
    running_shas = _running_release_shas(compose_project)

    directories = [
        p for p in releases_root.iterdir()
        if p.is_dir() and not p.is_symlink() and SHA_PATTERN.fullmatch(p.name)
    ]
    directories.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    protected: set[str] = set()
    if current_sha:
        protected.add(current_sha)
    protected.update(running_shas)

    retained: set[str] = {p.name for p in directories[:retain_count]}
    retained.update(protected)

    deleted: list[str] = []
    for directory in directories:
        if directory.name not in retained:
            print(f"🗑 Removing expired release: {directory.name}")
            shutil.rmtree(directory, ignore_errors=True)
            deleted.append(directory.name)

    return deleted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    p_prep = commands.add_parser("prepare")
    p_prep.add_argument("--environment-root", required=True, type=Path)
    p_prep.add_argument("--release-sha", required=True)

    p_sec = commands.add_parser("secure-file")
    p_sec.add_argument("--release-dir", required=True, type=Path)
    p_sec.add_argument("--filename", required=True)

    p_prom = commands.add_parser("promote")
    p_prom.add_argument("--environment-root", required=True, type=Path)
    p_prom.add_argument("--release-sha", required=True)

    p_clean = commands.add_parser("cleanup")
    p_clean.add_argument("--environment-root", required=True, type=Path)
    p_clean.add_argument("--compose-project", required=True)
    p_clean.add_argument("--retain", type=int, default=DEFAULT_RETAINED_RELEASES)

    args = parser.parse_args()

    if args.command == "prepare":
        dir_path = prepare_release(args.environment_root, args.release_sha)
        print(str(dir_path))
    elif args.command == "secure-file":
        secure_release_file(args.release_dir, args.filename)
    elif args.command == "promote":
        promote_current(args.environment_root, args.release_sha)
    elif args.command == "cleanup":
        deleted = cleanup_releases(
            args.environment_root,
            compose_project=args.compose_project,
            retain_count=args.retain,
        )
        if deleted:
            print(f"✔ Cleaned up {len(deleted)} old releases: {', '.join(deleted)}")
        else:
            print("✔ Release history within retention bounds, no deletions needed.")


if __name__ == "__main__":
    main()
