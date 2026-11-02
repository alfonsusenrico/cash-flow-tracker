# Tasks

## 1. Database backups (D1, D2, D3, D7, D8, D9)

- [x] 1.1 Add the `backup` image (`db/backup/`). It contains `backup-now`, `backup-schedule`, `backup-healthcheck`, `restore-backup` and `db-row-counts`, and nothing for an off-host copy (owner decision 2026-11-02). Evidence: the image builds, and `shellcheck` reports nothing on `db/backup/*.sh`.
- [x] 1.2 Add the Compose `backup` service: profile `backup`, run as `BACKUP_UID:BACKUP_GID`, bind mount `BACKUP_HOST_DIR`, healthcheck, project image label. Evidence: `docker compose config --quiet` passes with and without `--profile backup`, and the default service list is unchanged (db, migrate, redis, api, frontend, web).
- [x] 1.3 Materialize `BACKUP_TIME` and `BACKUP_RETENTION_DAYS` (with defaults), plus optional `BACKUP_HOST_DIR` and `BACKUP_AGE_RECIPIENT`, from GitHub secrets. Evidence: `deploy.yml` env, `materialize_runtime_env.py`, and the existing materializer tests passing.
- [x] 1.4 Write `scripts/verify/backup_restore_roundtrip.sh`. Evidence: 27 PASS checks on disposable `cft-ops-*` containers:
  - migration V1→V39 with synthetic rows (30 tables, 123 rows);
  - a plain and an encrypted dump, restored into new databases on a fresh postgres:16, with identical row counts per table, Flyway version 39 and `flyway validate`;
  - the restored database swapped in under the live name;
  - retention: the 15-day-old dump deleted, the 13-day-old one kept;
  - scheduler: the catch-up run, the next run at 02:30 +0700, and a stop on TERM within 5 seconds;
  - negative paths: an existing target refused; a failed dump leaving no file, no temporary file and no password in the log.
- [x] 1.5 Write the runbook `docs/database-backups.md`, and link it from `README.md`, `docs/production-deployment.md` and `.env.example`. Evidence: the restore commands in the runbook match the ones the round trip runs.

Expected Result: a verified dump lands nightly in a private host directory outside the volumes, is kept 14 days, and restores into a new database with identical counts and Flyway version.

## 2. Deploy script (D3, D4, D6)

- [x] 2.1 Add `release_storage.py backup-dir`: a validated, private, deploy-user-owned directory with default `<environment_root>/backups`. Evidence: `test_deploy_release_script.py` covers the default, the last assignment winning, relative, `:`, `/`, Docker storage, the environment root and the release tree.
- [x] 2.2 Take the pre-migration dump after the build and before the V14 preflight, and abort on failure. Give stdin-reading commands `/dev/null`. Evidence:
  - the deploy simulation (ssh, scp, docker and curl shims; synthetic archive) shows build < backup < preflight < migrate < up;
  - with a failing backup it exits non-zero with the abort message, and runs no preflight, migration, `up`, prune or promotion;
  - an unsafe backup directory stops the deploy before the build.
- [x] 2.3 Prune only by `label=cash-flow-tracker.managed=true` and `label=com.docker.compose.project=cashflow-production`, and warn on failure. Evidence: the deploy simulation asserts exactly these two prune commands. A local Docker proof with synthetic images removed only the two dangling project images, and kept the unlabelled, other-project, other-label and tagged ones. Pre-existing dangling images were unchanged.

Expected Result: no deploy migrates without a fresh verified dump, and cleanup never touches another project's images.

## 3. CI (D5)

- [x] 3.1 Add `.github/workflows/checks.yml` with three jobs: backend on a postgres:16 service after Flyway V1→latest; frontend; operations (Compose config and the round trip). `deploy.yml` calls it as preflight and ignores `mobile/**`. Evidence: `actionlint` 1.7.7 (Docker image) reports nothing on `checks.yml`. Its only `deploy.yml` findings are two SC2086 notices that also appear on the unchanged main file.
- [x] 3.2 Add `REQUIRE_TEST_DATABASE` to `conftest.py`. Evidence: with the variable set and the database unreachable, every test errors; without it, 673 passed and 566 skipped, as before.
- [x] 3.3 Install the pinned `requirements-dev.txt` (pytest 9.0.3, adding `pytest-asyncio==1.4.1.post1`) and fix the calendar-dependent `plan` helper. Evidence: unchanged main on a fresh migrated database gave 23 failed (22 from pytest 9.2.0 monkeypatch undo on Starlette `State`, 1 from the schedule day on 2026-11-02). With this change, the full suite gave 1239 passed, 0 failed, 0 skipped.
- [x] 3.4 Push the branch and record the Checks run and its duration. Evidence: `gh run list` / `gh run view` results in verification.md.

Expected Result: every branch push, pull request and production preflight runs all 1239 backend tests on a migrated PostgreSQL 16, and mobile-only pushes do not deploy.

## 4. Close-out

- [x] 4.1 Run `git diff --check` and `openspec validate --all --strict`. Evidence: both exit 0 (verification.md).
- [x] 4.2 Archive the change on the branch, push, and report the compare link. Merging and the first production deploy stay with the owner (PROJECT_STATE.md tracks the owner's post-deploy checks from `docs/database-backups.md` §7).

Expected Result: specs, code and evidence are on one pushed branch, ready for the owner's review.
