import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from flyway_v14_preflight import PreflightError, flyway_command, repair_v14_checksum


def _migration(version: str, state: str, description: str | None = None) -> dict[str, str]:
    return {
        "category": "Versioned",
        "version": version,
        "description": description or f"migration {version}",
        "state": state,
        "installedOnUTC": "2026-09-23T00:00:00Z" if state != "Pending" else "",
    }


def _info_result(*, v14_state: str = "Success", other_state: str = "Success") -> dict[str, object]:
    migrations = [_migration("1", "Ignored (Baseline)")]
    migrations.append({"category": "", "version": "1", "state": "Baseline"})
    migrations.extend(_migration(str(version), other_state) for version in range(2, 14))
    migrations.append(_migration("14", v14_state, "remove transfer type and add default pocket"))
    migrations.extend(_migration(str(version), other_state) for version in range(15, 19))
    return {"operation": "info", "migrations": migrations}


def _validation_result(*, valid: bool = True) -> dict[str, object]:
    if valid:
        return {"operation": "validate", "validationSuccessful": True, "invalidMigrations": []}

    return {
        "operation": "validate",
        "validationSuccessful": False,
        "invalidMigrations": [
            {
                "version": "14",
                "description": "remove transfer type and add default pocket",
                "errorDetails": {"errorCode": "CHECKSUM_MISMATCH"},
            }
        ],
    }


def _pending_history() -> dict[str, object]:
    history = _info_result()
    for version in range(16, 20):
        description = f"migration {version}"
        if version == 19:
            history["migrations"].append(_migration("19", "Pending", description))
        else:
            entry = next(item for item in history["migrations"] if item.get("version") == str(version))
            entry["state"] = "Pending"
            entry["installedOnUTC"] = ""
    return history


def _validation_with_pending(*, checksum_mismatch: bool) -> dict[str, object]:
    issues = _validation_result(valid=False)["invalidMigrations"] if checksum_mismatch else []
    issues.extend(
        {
            "version": str(version),
            "description": f"migration {version}",
            "errorDetails": {"errorCode": "RESOLVED_VERSIONED_MIGRATION_NOT_APPLIED"},
        }
        for version in range(16, 20)
    )
    return {"operation": "validate", "validationSuccessful": False, "invalidMigrations": issues}


def _repair_result(*versions: str) -> dict[str, object]:
    return {
        "operation": "repair",
        "migrationsAligned": [{"version": version} for version in versions],
        "migrationsRemoved": [],
        "migrationsDeleted": [],
    }


def _queued_runner(*payloads: dict[str, object]):
    responses = list(payloads)
    actions: list[str] = []

    def runner(command, **_):
        actions.append(command[-1])
        return subprocess.CompletedProcess(command, 0, json.dumps(responses.pop(0)), "")

    return runner, actions


def test_local_flyway_command_uses_compose_default_environment_file():
    assert flyway_command("cash-flow-tracker", None, "info") == [
        "docker",
        "compose",
        "--project-name",
        "cash-flow-tracker",
        "run",
        "--rm",
        "migrate",
        "-outputType=json",
        "info",
    ]


def test_valid_history_skips_repair():
    runner, actions = _queued_runner(_info_result(), _validation_result())

    assert repair_v14_checksum("test-project", Path("runtime.env"), runner) is False
    assert actions == ["info", "validate"]


def test_repairs_only_applied_v14_checksum_then_revalidates():
    runner, actions = _queued_runner(
        _info_result(),
        _validation_result(valid=False),
        _repair_result("14"),
        _validation_result(),
    )

    assert repair_v14_checksum("test-project", Path("runtime.env"), runner) is True
    assert actions == ["info", "validate", "repair", "validate"]


def test_repairs_v14_with_verified_pending_migrations_and_revalidates():
    runner, actions = _queued_runner(
        _pending_history(),
        _validation_with_pending(checksum_mismatch=True),
        _repair_result("14"),
        _validation_with_pending(checksum_mismatch=False),
    )

    assert repair_v14_checksum("test-project", None, runner) is True
    assert actions == ["info", "validate", "repair", "validate"]


def test_pending_migrations_without_mismatch_need_no_repair():
    runner, actions = _queued_runner(
        _pending_history(),
        _validation_with_pending(checksum_mismatch=False),
    )

    assert repair_v14_checksum("test-project", None, runner) is False
    assert actions == ["info", "validate"]


def test_unverified_pending_migration_refuses_repair():
    invalid = _validation_with_pending(checksum_mismatch=True)
    invalid["invalidMigrations"][-1]["version"] = "20"
    runner, actions = _queued_runner(_pending_history(), invalid)

    with pytest.raises(PreflightError, match="repair refused"):
        repair_v14_checksum("test-project", None, runner)

    assert actions == ["info", "validate"]


def test_refuses_other_validation_errors():
    other_mismatch = {
        "operation": "validate",
        "validationSuccessful": False,
        "invalidMigrations": [
            {
                "version": "13",
                "description": "migration 13",
                "errorDetails": {"errorCode": "CHECKSUM_MISMATCH"},
            }
        ],
    }
    runner, actions = _queued_runner(_info_result(), other_mismatch)

    with pytest.raises(PreflightError, match="repair refused"):
        repair_v14_checksum("test-project", Path("runtime.env"), runner)

    assert actions == ["info", "validate"]


def test_refuses_failed_or_out_of_order_history_before_validation():
    runner, actions = _queued_runner(_info_result(other_state="Out of Order"))

    with pytest.raises(PreflightError, match="unsupported state"):
        repair_v14_checksum("test-project", Path("runtime.env"), runner)

    assert actions == ["info"]


def test_refuses_pending_migration_before_applied_v14():
    history = _info_result()
    history["migrations"][13]["state"] = "Pending"
    history["migrations"][13]["installedOnUTC"] = ""
    runner, actions = _queued_runner(history)

    with pytest.raises(PreflightError, match="earlier migration is pending"):
        repair_v14_checksum("test-project", Path("runtime.env"), runner)

    assert actions == ["info"]


def test_repair_result_must_align_v14_only():
    runner, actions = _queued_runner(
        _info_result(),
        _validation_result(valid=False),
        _repair_result("13", "14"),
    )

    with pytest.raises(PreflightError, match="exactly one migration"):
        repair_v14_checksum("test-project", Path("runtime.env"), runner)

    assert actions == ["info", "validate", "repair"]


def test_repair_must_be_followed_by_successful_validation():
    runner, actions = _queued_runner(
        _info_result(),
        _validation_result(valid=False),
        _repair_result("14"),
        _validation_result(valid=False),
    )

    with pytest.raises(PreflightError, match="still fails"):
        repair_v14_checksum("test-project", Path("runtime.env"), runner)

    assert actions == ["info", "validate", "repair", "validate"]
