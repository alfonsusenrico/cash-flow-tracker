# Design

## Context

The audit of 2026-11-02 (read-only production checks) found:

- no database backup of any kind;
- CI running `python -m pytest tests/` without PostgreSQL, which skips 566 database tests;
- `docker image prune --force` with no filter at line 147 of `scripts/deploy_remote_release.sh`, on a host other projects share.

Releases run through `.github/workflows/deploy.yml`, which SSHes over Tailscale into `msi`. There `deploy_remote_release.sh` builds `releases/<sha>/`, runs the Flyway V14 preflight and `migrate`, starts Compose, health-checks and promotes `current`. The work is on `chore/operations-hardening` from origin/main 5004b11.

## Goals / Non-Goals

Goals: nightly and pre-migration dumps that are verified, atomic, retained for 14 days and restorable by a tested procedure; CI that runs every database test on a database migrated as production is; deploy cleanup that touches only this project's images; mobile-only pushes that do not redeploy.

Non-goals: off-host copies (owner decision, D7), WAL archiving or replicas, receipt-file backups, Android workflows, production access.

## Decisions

### 1. Backups run as a Compose service built from the repository

A `backup` service, built from `db/backup/Dockerfile` (`FROM postgres:16`, plus the Debian `age` package), runs `backup-schedule`. That is a small bash loop that sleeps until `BACKUP_TIME` in `TZ` and calls `backup-now nightly`. It ships through the normal build. The deploy also uses it for the pre-migration dump, so both share one code path, one lock and one log format.

- The client tools must match the server major version, so the image is pinned to the same `postgres:16` as `db`.
- The service uses the Compose profile `backup`. The deploy script exports `COMPOSE_PROFILES=backup`, and local stacks opt in with `--profile backup`. Without the profile, `docker compose up` on a developer machine would try to bind the production default path.
- The service healthcheck (`backup-healthcheck`) reports unhealthy when no dump has succeeded for 26 hours. The deploy's own pre-migration dump refreshes that marker, so `up --wait` sees the container healthy on its first check, within 30 seconds.
- At start, a last success older than 24 hours triggers an immediate catch-up dump. This covers a host that was off at 02:30.

Rejected:

- Host cron or systemd timer: that needs a manual server edit, which the brief and AGENTS.md forbid.
- Deploy-script-only dumps: not nightly.
- A third-party backup image: an unreviewed dependency for about 100 lines of shell.
- `docker exec ledger_db pg_dump` from the host: that needs host scheduling.
- A cron daemon inside the container: more moving parts than one sleep loop.

### 2. One dump run: lock, dump, verify, encrypt, flush, rename, prune

`backup-now <reason>` takes `flock` on `<dir>/.backup.lock`, waiting up to 900 seconds. It then:

1. runs `pg_dump --format=custom` into `.tmp-<name>.<pid>`;
2. verifies it with `pg_restore --list`;
3. encrypts it to an age recipient when one is configured, then deletes the plaintext temporary file;
4. `sync`s the file, `mv`s it to the final name in the same directory, and `sync`s the directory;
5. touches `.last-success`;
6. deletes dumps whose mtime is older than `BACKUP_RETENTION_DAYS × 24 h`, never the file just written, plus `.tmp-cashflow-*` leftovers older than a day.

The name is `cashflow-<db>-<UTC yyyymmddThhmmssZ>-<reason>.dump[.age]`. The reason is `nightly` or `pre-migrate-<12-char sha>`, validated as `^[a-z0-9][a-z0-9-]{0,39}$`.

- Retention runs only after a verified dump, so a long run of failures cannot delete the last good dump.
- Every step's failure is reported as `status=failed reason=… step=<step> exit=<code>`. Tool output goes to `/dev/null`, because pg_dump, pg_restore and age messages can quote connection details or data. The success line adds `file`, `size_bytes`, `duration_s` and `pruned`.
- `umask 077` gives files mode 0600.

Rejected:

- Plain SQL dumps: no `pg_restore --list` verification, and no selective restore.
- Streaming `pg_dump | age` without a plaintext temporary file: the dump could not be verified before encryption.

### 3. The host directory is resolved and created by the deploy, as the deploy user

`release_storage.py backup-dir` reads only `BACKUP_HOST_DIR` from `runtime.env`, using its last assignment. The default is `<environment_root>/backups`, which is `/opt/cash-flow-tracker/production/backups`. It refuses:

- a relative path, `:` (Compose short volume syntax), or control characters;
- `/` or the environment root;
- anything inside `releases/` (deleted by cleanup) or inside `/var/lib/docker` (lost with the volumes).

It creates the directory with mode 0700 and checks that the deploy user can write to it. The deploy exports `BACKUP_HOST_DIR`, and `BACKUP_UID`/`BACKUP_GID` from `id`. The container runs as that user, so the owner can list and read dumps without root. The shell environment overrides `--env-file` in Compose interpolation, so the validated value is the one used.

Rejected:

- `/var/backups/cash-flow-tracker`: the non-root deploy user cannot create it without a manual sudo step.
- Letting Docker create the bind source: it would be root-owned.
- A Docker named volume: it is lost together with the other volumes.

### 4. The pre-migration dump gates the deploy

It runs right after `docker compose build`, before the V14 preflight, because that preflight can run `flyway repair`. The command is `docker compose run --rm backup backup-now pre-migrate-<sha12> </dev/null`. A non-zero exit prints `❌ Pre-migration backup failed; aborting before any migration.` and exits 1. Nothing is migrated, started, promoted or pruned, and the previous release keeps serving.

The remote script is streamed over SSH stdin to `sh -s`. `docker compose run`, the Python preflight (which runs Compose) and the migrate run therefore get `</dev/null`, so none of them can read the rest of the script. This was a latent risk for the existing `migrate` run, and it grows with the script.

### 5. CI: one reusable checks workflow, the database suite only

`.github/workflows/checks.yml` triggers on `push` to every branch except main, on `pull_request` and on `workflow_call`, ignoring `mobile/**`. Its three parallel jobs run on `ubuntu-latest`:

- **backend**: a `postgres:16` service; `flyway/flyway:10 migrate` with the Compose `migrate` service's settings (baseline on migrate, baseline version 1, filesystem locations, `--network host`), plus a `Schema version` line from `info`; then `python -m pytest tests/ -ra` with `TEST_DATABASE_URL`, `SKIP_TEST_MIGRATIONS=1` and `REQUIRE_TEST_DATABASE=1`;
- **frontend**: `npm ci`, type check, lint and build, unchanged;
- **operations**: `docker compose config` with and without the backup profile, and `scripts/verify/backup_restore_roundtrip.sh`.

`deploy.yml` calls it as `preflight` (`uses: ./.github/workflows/checks.yml`). Main is then deployed only after exactly the checks that branches run.

- The no-database run is **replaced**, not kept. With a database, every test that the no-database run executes still runs (673 tests, a strict subset of the 1239). A second run would only show that the unit tests need no database, and `REQUIRE_TEST_DATABASE` covers the failure mode that matters.
- `conftest.py` turns an unreachable database into a failure when `REQUIRE_TEST_DATABASE=1`. Without it, a broken service container would silently bring back the 566 skips.
- CI installs `backend/requirements-dev.txt`, which pins pytest 9.0.3, and that file now pins `pytest-asyncio==1.4.1.post1`. The old command installed unpinned pytest. Pytest 9.2.0 changed `monkeypatch.setattr` undo for objects without a data descriptor (pytest #10644): it restores the instance `__dict__` entry. Starlette's `State` keeps attributes in `_state`, so the undo deletes `app.state.notification_provider`, and the next test that patches it fails. That broke 22 database tests on unchanged main (reproduced on a clean export). With the pinned pytest they pass. `requirements-test.txt` pins `pytest-asyncio==0.24.0`, which cannot install with pytest 9. It is used nowhere and left as a follow-up.
- `test_expectations.plan` used `schedule_day = today.day`, even when a test moved the due date into the previous month. On the 1st and 2nd of a month, the occurrence after a linked due date was then due today, and `test_one_certain_payment_is_linked_once_and_applies_the_debt_once` failed. It failed on 2026-11-02 on unchanged main. The helper now uses the due date's day. This is test-only, and the product's monthly scheduling is unchanged.
- Mobile-only pushes: `deploy.yml` has `paths-ignore: ["mobile/**"]` on its push trigger, and checks ignore `mobile/**`. The Android workflows belong to the other Phase 0 job.

Rejected:

- Running the Compose `db` and `migrate` services in CI: the brief asks for a service container, and the Flyway settings are copied verbatim (with a comment pointing at the Compose service).
- Duplicating the checks inside `deploy.yml`: two copies drift apart.
- Paid or larger runners: not approved (ADR 0012).

### 6. Image cleanup by label

The `api`, `frontend` and `backup` builds carry `cash-flow-tracker.managed: "true"` through a YAML anchor in `docker-compose.yml`. Cleanup runs `docker image prune --force --filter label=cash-flow-tracker.managed=true`, then the same with `label=com.docker.compose.project=cashflow-production`. Compose adds that second label to the images it builds (seen on Compose 5.6.1), so it also covers dangling images built before this change.

`prune` without `-a` removes only dangling images, so the running images (tagged `cashflow-production-<service>`) and third-party images are never touched. A failure is printed as a warning and does not fail an already promoted release. That replaces the old `>/dev/null 2>&1 || true`.

Rejected:

- Removing superseded tags explicitly: Compose reuses one tag per service, so the superseded images are exactly the dangling ones.
- `docker system prune` or `image prune -a`: other projects' images on the shared host.

### 7. Off-host copy: not built (owner decision 2026-11-02)

The owner said "backup to the server for now, don't do backup here yet". Dumps stay on the server. No off-host copy, sync or upload exists, and no variable, stub or hook is shipped for one. The runbook (`docs/database-backups.md` §6) records rsync over Tailscale to the owner's machine and a free cloud bucket as future options for a separately approved change. Either should copy only verified, preferably encrypted files, and never delete local dumps.

Consequence: these backups protect against volume loss, bad migrations and operator error, but not against losing the host or its disk.

### 8. Encryption is optional and asymmetric

When `BACKUP_AGE_RECIPIENT` (an `age1…` public key, checked against the bech32 alphabet and length) is set, dumps are stored as `.dump.age`. The server holds only the public key, so a copy of the backup directory is unreadable without the owner's private key. Restore takes the identity file through `BACKUP_AGE_IDENTITY_FILE`, mounted for that run only.

Rejected:

- `openssl enc` with a passphrase: the decryption secret would live on the same host as the dumps.
- GPG: a heavier toolchain and keyring state for the same result.

### 9. Restore is non-destructive and proven by a round-trip script

`restore-backup <file> <db>` validates the name, decrypts if needed and verifies with `pg_restore --list`. It refuses an existing database, runs `createdb`, then `pg_restore --exit-on-error --single-transaction --no-owner`, and drops the new database on failure. It reports the Flyway version. `db-row-counts <db>` prints exact counts per public table and no row contents.

Swapping the restored database in (stop the app, rename the databases, `flyway validate`, start) stays a documented owner-run step, because it replaces live data.

`scripts/verify/backup_restore_roundtrip.sh` proves the whole path on disposable containers and runs in CI. The checks are listed in tasks 1.4.

## Data, failure and rollback

- No schema migration and no API change. The money invariants are untouched: dumps are read-only snapshots, and the only test change is a fixture schedule day.
- New runtime state: the host backup directory and the `ledger_backup` container.
- **Rollback**: revert the change and redeploy. `--remove-orphans` stops `ledger_backup`, and the dumps stay on disk.
- **Failure modes**:
  - an unwritable or unsafe directory stops the deploy before the build;
  - a failed pre-migration dump stops it before migrating;
  - a failed nightly dump logs `status=failed` and, after 26 hours, turns the container unhealthy;
  - a failed prune logs a warning.

## Open decisions for owner

- **Off-host copy target** (D7): rsync over Tailscale to the owner's machine, or a free cloud bucket. Deferred by the owner on 2026-11-02, and nothing is built for it.
- **Encryption**: whether to set `BACKUP_AGE_RECIPIENT` now. Without it, dumps are unencrypted, mode 0600, in a 0700 directory. Encryption matters most once an off-host copy exists.
- **Receipt files** (`api_storage` volume): not backed up. Decide whether they need their own backup.
- **`requirements-test.txt`**: unused and unsatisfiable with pytest 9. Delete it, or align it with `requirements-dev.txt`.
- **Pre-label dangling images on the host**: the Compose-project filter removes them if the host's Compose version labelled its builds. Otherwise they stay until removed by hand, by ID.
