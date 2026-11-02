# Proposal

## Why

A read-only production audit on 2026-11-02 found three operational gaps:

- There are no database backups. The only copy of the ledger is the live volume `cashflow-production_db_data`.
- CI runs the backend suite without PostgreSQL, so 566 database tests are skipped on every deploy.
- The deploy cleanup runs an unfiltered `docker image prune` on a host other projects share.

The service is becoming public and multi-user, so losing the volume would lose every user's book.

## Approval

On 2026-11-02 the owner said "ok I approve all you recommend", approving the audit recommendations, and "Phase 0 starts now". This change implements the Phase 0 operations-hardening job. On 2026-11-02 the owner also decided "backup to the server for now, don't do backup here yet": dumps stay on the server, and no off-host copy, sync or upload is built. The off-host destination is recorded only as a future option. Recorded 2026-11-02.

## Expected Outcome

- Every night, and before every deploy migrates, a verified `pg_dump` of the production database lands in a host directory outside the Docker volumes. Dumps are kept 14 days.
- If the pre-migration dump fails, the deploy stops before migrating and the running release stays online.
- The owner can restore any dump into a new database with one documented command, compare row counts and the Flyway version, and swap the restored database in.
- Every branch push, pull request and production preflight runs the full backend suite on PostgreSQL 16, migrated V1→latest by Flyway as production does, with no database test skipped.
- A main push that changes only `mobile/**` does not redeploy the server.
- Deploy cleanup removes only this project's dangling images.

## What Changes

- New `backup` Compose service, profile `backup`. It is built from `db/backup/` (postgres:16 plus `age`, labelled as a project image) and dumps nightly at `BACKUP_TIME`. Each dump is in custom format with a timestamped name, written atomically, verified with `pg_restore --list` and kept `BACKUP_RETENTION_DAYS` days. The log holds only the outcome, file name, size and duration. Dumps are encrypted to an age public key only when `BACKUP_AGE_RECIPIENT` is set. The image also carries `restore-backup` (restore into a new database) and `db-row-counts`.
- `scripts/deploy_remote_release.sh` resolves and creates `BACKUP_HOST_DIR` (default `<DEPLOY_ROOT>/production/backups`, never inside the release tree or Docker storage). It enables the backup profile as the deploy user, and takes a pre-migration dump before the Flyway preflight and migrations, aborting on failure. It prunes images by label only. Commands that may read stdin get `/dev/null`, since the remote script streams over SSH stdin.
- New `.github/workflows/checks.yml`, run on branch pushes and pull requests and called by `deploy.yml` as its preflight. It has three jobs:
  - backend: PostgreSQL 16 service, Flyway migration, full suite with `REQUIRE_TEST_DATABASE=1`;
  - frontend: type check, lint, build;
  - operations: Compose config with and without the backup profile, and the backup and restore round trip.
- `deploy.yml` ignores `mobile/**` pushes and passes the backup settings to the runtime materializer.
- Tests:
  - `conftest.py` fails the run when `REQUIRE_TEST_DATABASE=1` and the database is unreachable.
  - `requirements-dev.txt` pins `pytest-asyncio`, and CI installs the pinned dev requirements. Unpinned pytest 9.2 breaks 22 database tests.
  - A calendar-dependent plan helper in `test_expectations.py` is fixed.
- Docs:
  - new runbook `docs/database-backups.md`;
  - updates to `docs/production-deployment.md`, `README.md` and `.env.example`.

## Non-goals

- Any off-host copy of the dumps (owner decision 2026-11-02). No placeholder code, variable or hook is shipped for it.
- Android workflows. Another Phase 0 job moves the app into `mobile/` and adds them.
- Point-in-time recovery (WAL archiving), replicas or backups of receipt files in `api_storage`.
- Production access. All verification is local, on disposable containers with synthetic data. Pushing the branch is authorised. Merging and the first production run of the new pipeline stay with the owner.

## Capabilities

### New Capabilities

- `database-backups`: scheduled and pre-migration dumps, verification, atomic writes, retention, optional encryption, host storage location, logging limits, and the restore procedure.
- `release-pipeline`: the checks that gate a production deploy (database tests on migrated PostgreSQL), mobile-only pushes skipping the deploy, the pre-migration backup gate, and scoped image cleanup.

### Modified Capabilities

None. `schema-migrations` already requires that a migration failure stop the release, and that stays as written.

Standards: the `database` and `deployed` capabilities apply (migration safety: backup before migrate, data-preservation evidence by row counts; deployment: scoped image cleanup, preflight before release). `personal-data` applies to the logging limits and the optional encryption. No project capability is added.

## Impact

- New: `db/backup/` (Dockerfile, `backup.sh`, `schedule.sh`, `restore.sh`, `row_counts.sh`, `healthcheck.sh`), `.github/workflows/checks.yml`, `scripts/verify/backup_restore_roundtrip.sh`, `backend/tests/test_deploy_release_script.py`, `docs/database-backups.md`.
- Changed:
  - `docker-compose.yml`: build labels and the backup service;
  - `scripts/deploy_remote_release.sh`, `scripts/release_storage.py` (`backup-dir`) and `scripts/materialize_runtime_env.py`;
  - `.github/workflows/deploy.yml`;
  - `backend/tests/conftest.py`, `backend/tests/test_expectations.py` and `backend/requirements-dev.txt`;
  - `docs/production-deployment.md`, `README.md` and `.env.example`.
- Runtime: one new long-running container `ledger_backup`, and a host directory with up to 14 days of dumps (about 127 KB each for the synthetic data set, so size depends on real data). No schema migration, no API change.
- CI: three parallel jobs instead of one, all on free GitHub-hosted runners.
