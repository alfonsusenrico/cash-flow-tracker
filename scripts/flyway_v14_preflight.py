#!/usr/bin/env python3
"""Guard the one-time Flyway V14 checksum correction during release."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Callable

FLYWAY_SERVICE = "migrate"
V14_DESCRIPTION = "remove transfer type and add default pocket"
VALID_HISTORY_STATES = {"Success", "Baseline", "Ignored (Baseline)", "Pending"}

Runner = Callable[..., subprocess.CompletedProcess[str]]


class PreflightError(Exception):
    """Raised when migration history is not safe for the targeted repair."""


def flyway_command(
    project_name: str,
    env_file: Path | None,
    action: str,
) -> list[str]:
    command = [
        "docker",
        "compose",
        "--project-name",
        project_name,
    ]
    if env_file is not None:
        command.extend(["--env-file", str(env_file)])
    command.extend([
        "run",
        "--rm",
        FLYWAY_SERVICE,
        "-outputType=json",
        action,
    ])
    return command


def run_flyway_json(
    project_name: str,
    env_file: Path | None,
    action: str,
    runner: Runner = subprocess.run,
) -> dict[str, object]:
    try:
        result = runner(
            flyway_command(project_name, env_file, action),
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as error:
        raise PreflightError("Could not run Docker Compose for Flyway; migration preflight stopped.") from error
    if result.returncode != 0:
        raise PreflightError(f"Flyway {action} command failed; migration preflight stopped.")

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise PreflightError(f"Flyway {action} did not return valid JSON; migration preflight stopped.") from error

    if not isinstance(payload, dict) or payload.get("operation") != action:
        raise PreflightError(f"Flyway {action} returned an unexpected result; migration preflight stopped.")
    return payload


def validate_history_states(info: dict[str, object]) -> bool:
    migrations = info.get("migrations")
    if not isinstance(migrations, list):
        raise PreflightError("Flyway history is unavailable; migration preflight stopped.")

    v14_entries = [
        migration
        for migration in migrations
        if isinstance(migration, dict)
        and migration.get("category") == "Versioned"
        and migration.get("version") == "14"
    ]
    if len(v14_entries) != 1:
        raise PreflightError("Expected exactly one versioned V14 history entry; migration preflight stopped.")

    v14 = v14_entries[0]
    if v14.get("description") != V14_DESCRIPTION:
        raise PreflightError("V14 history does not match the expected migration; migration preflight stopped.")

    for migration in migrations:
        if not isinstance(migration, dict):
            raise PreflightError("Flyway history contains an invalid entry; migration preflight stopped.")
        if migration.get("state") not in VALID_HISTORY_STATES:
            raise PreflightError("Flyway history contains an unsupported state; migration preflight stopped.")
        if migration.get("state") == "Pending" and migration.get("version"):
            try:
                pending_version = int(str(migration["version"]))
            except ValueError as error:
                raise PreflightError("Flyway history contains an unsupported pending version; migration preflight stopped.") from error
            if pending_version < 14 and v14.get("state") != "Pending":
                raise PreflightError("An earlier migration is pending before V14; migration preflight stopped.")

    if v14.get("state") == "Pending":
        return False

    if v14.get("state") != "Success" or not v14.get("installedOnUTC"):
        raise PreflightError("V14 has not been successfully applied; migration preflight stopped.")
    return True


def validation_issue_state(
    validation: dict[str, object],
    info: dict[str, object],
    v14_applied: bool,
) -> tuple[bool, bool]:
    """Return (safe, needs_repair); pending migrations must match Flyway info."""
    invalid_migrations = validation.get("invalidMigrations")
    if not isinstance(invalid_migrations, list):
        return False, False
    if validation.get("validationSuccessful") is True:
        return not invalid_migrations, False
    if validation.get("validationSuccessful") is not False or not invalid_migrations:
        return False, False

    pending = {
        (migration.get("version"), migration.get("description"))
        for migration in info["migrations"]
        if migration.get("category") == "Versioned" and migration.get("state") == "Pending"
    }
    needs_repair = False
    for invalid in invalid_migrations:
        if not isinstance(invalid, dict):
            return False, False
        error_details = invalid.get("errorDetails")
        error_code = error_details.get("errorCode") if isinstance(error_details, dict) else None
        identity = (invalid.get("version"), invalid.get("description"))
        if error_code == "RESOLVED_VERSIONED_MIGRATION_NOT_APPLIED" and identity in pending:
            continue
        if (
            v14_applied
            and identity == ("14", V14_DESCRIPTION)
            and error_code == "CHECKSUM_MISMATCH"
            and not needs_repair
        ):
            needs_repair = True
            continue
        return False, False
    return True, needs_repair


def require_v14_only_repair(repair: dict[str, object]) -> None:
    aligned = repair.get("migrationsAligned")
    if not isinstance(aligned, list) or len(aligned) != 1:
        raise PreflightError("Flyway repair did not align exactly one migration; inspect migration history before continuing.")

    aligned_v14 = aligned[0]
    if not isinstance(aligned_v14, dict) or aligned_v14.get("version") != "14":
        raise PreflightError("Flyway repair changed an unexpected migration; inspect migration history before continuing.")

    for result_key in ("migrationsRemoved", "migrationsDeleted"):
        entries = repair.get(result_key)
        if not isinstance(entries, list) or entries:
            raise PreflightError("Flyway repair changed migration history beyond V14; inspect before continuing.")


def repair_v14_checksum(
    project_name: str,
    env_file: Path | None,
    runner: Runner = subprocess.run,
) -> bool:
    info = run_flyway_json(project_name, env_file, "info", runner)
    v14_applied = validate_history_states(info)
    validation = run_flyway_json(project_name, env_file, "validate", runner)

    safe, needs_repair = validation_issue_state(validation, info, v14_applied)
    if not safe:
        raise PreflightError("Flyway validation found an issue other than the expected V14 checksum mismatch; repair refused.")
    if not needs_repair:
        return False

    repair = run_flyway_json(project_name, env_file, "repair", runner)
    require_v14_only_repair(repair)

    post_repair_validation = run_flyway_json(project_name, env_file, "validate", runner)
    safe, needs_repair = validation_issue_state(post_repair_validation, info, v14_applied)
    if not safe or needs_repair:
        raise PreflightError("Flyway validation still fails after V14 checksum repair; migration stopped.")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-name", required=True)
    parser.add_argument("--env-file", type=Path)
    arguments = parser.parse_args()

    try:
        repaired = repair_v14_checksum(arguments.project_name, arguments.env_file)
    except PreflightError as error:
        print(str(error), file=sys.stderr)
        return 1

    if repaired:
        print("Validated and repaired only the expected V14 checksum mismatch.")
    else:
        print("Flyway history validates; no checksum repair was needed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
