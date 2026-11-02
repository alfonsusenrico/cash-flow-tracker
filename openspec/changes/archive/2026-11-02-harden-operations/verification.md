# Verification — 2026-11-02

Branch `chore/operations-hardening` from origin/main 5004b11. Every check ran locally on disposable containers (`cft-ops-*`, test database on 127.0.0.1:55651) with synthetic data. No production host, phone or AI provider was used.

## Baseline (5004b11, clean `git archive` export, its own fresh database)

- **CI-style, no database**: not rerun on the export. On this branch, with the database unreachable, the result was 683 passed and 566 skipped. All 20 new tests need no database, so the 566 skips match the audit.
- **Fresh postgres:16, `flyway/flyway:10 migrate`** to v39, then `SKIP_TEST_MIGRATIONS=1` with the CI's old unpinned install (pytest 9.2.0): 1206 passed, **23 failed**.
  - 22 failures: `AttributeError: … State … has no attribute 'notification_provider'`. Each failing test passes alone. A bisection showed that the second test to patch `app.state.notification_provider` fails: pytest 9.2's `monkeypatch` undo (#10644) deletes Starlette `State` attributes. With pytest 9.0.3 (pinned in `requirements-dev.txt`), they pass.
  - 1 failure: `test_plan_auto_link.py::test_one_certain_payment_is_linked_once_and_applies_the_debt_once`, with 3 of 3 failing runs on 2026-11-02. The helper scheduled day 2 while due fell on 31 October, so the next occurrence was due today (design D5).

## Backend suite on a database migrated as production is (final working tree)

- Fresh `postgres:16` (`cft-ops-db`). `flyway/flyway:10 -connectRetries=60 migrate` with the Compose `migrate` settings reported "Successfully applied 39 migrations … now at version v39", and `info` reported "Schema version: 39".
- `TEST_DATABASE_URL=… SKIP_TEST_MIGRATIONS=1 REQUIRE_TEST_DATABASE=1 python -m pytest tests/ -ra` from `requirements-dev.txt` (pytest 9.0.3, pytest-asyncio 1.4.1.post1): **1249 passed, 0 failed, 0 skipped** in 60 s.
- No database, without the guard: 683 passed, 566 skipped. The 20 new tests (10 deploy simulation, 10 fixture dates) need no database.
- No database, with `REQUIRE_TEST_DATABASE=1`: every selected test errors ("TEST_DATABASE_URL is not reachable"), so a broken CI database cannot pass as skips.

## Calendar dependence of the plan fixture (frozen clock)

An evidence-only pytest plugin (scratch area, `ops-hardening/frozen/`) froze `date.today()` and `datetime.now()` at 12:00 WIB, never later than the real clock, because the database's `NOW()` cannot be frozen. It patched `app.routers.recurring`, `app.routers.transactions`, `app.routers.obligations`, `app.services.plan_occurrences`, `app.services.payment_links`, `test_expectations` and `test_plan_auto_link`. `test_plan_auto_link.py` and `test_expectations.py` (23 tests, every caller of `plan(due=…)`) then ran on separate fresh V39 databases:

| Frozen date | Unchanged main | Branch |
| --- | --- | --- |
| 2026-01-01, 03-01, 03-02, 10-01, 10-02, 11-01, 11-02 | 22 passed, 1 failed (`test_one_certain_payment_is_linked_once_and_applies_the_debt_once`) | 23 passed |
| 2026-01-31, 02-28, 04-30, 07-15, 09-30, 10-31 | 23 passed | 23 passed |

`calculate_next_due_date` shows the mechanism. With the old schedule day (`today.day`), the occurrence after a met `today-2` due date is today itself on each failing date (for example 2026-03-02 → 2026-03-02). With the due date's day, it is 2026-03-28.

## Backup and restore round trip

`VERIFY_PREFIX=cft-ops scripts/verify/backup_restore_roundtrip.sh` ran in 24 s and passed **27 checks, 0 failed**:

- **Source**: Flyway V1→V39 (highest file V39), synthetic rows in 30 tables (123 rows, 75 transactions).
- **Plain dump**: `status=ok reason=roundtrip … size_bytes=127000 duration_s=0 pruned=1`, written once, with no temporary file left and a passing healthcheck.
- **Retention** (14 days): the 15-day-old planted dump was deleted, and the 13-day-old one was kept.
- **Encrypted dump** (throwaway age key): only `.dump.age` was written, with no plaintext beside it.
- **Restore** into new databases on a second fresh postgres:16, for both the plain and the encrypted dump: `restore status=ok … flyway_version=39`. `db-row-counts` matched the source for all 30 tables (`diff` empty), the Flyway schema version was 39 like the source, and `flyway validate` passed.
- **Swap**: the restored database was renamed in under the live name (`ledger`). Row counts matched, `flyway validate` passed, and the version was 39.
- **Scheduler**: a catch-up nightly dump at start, `next_run=2026-11-03T02:30:00+0700`, and a stop on TERM within 5 s.
- **Negative paths**:
  - restoring into an existing database failed with `step=target-exists`, and that database's 75 transactions were intact;
  - a wrong password gave `status=failed reason=failing step=dump exit=1`, with no new dump and no temporary file;
  - no log contained either password.

Compose path (`docker compose -p cft-ops --profile backup`, `BACKUP_UID`/`BACKUP_GID` from `id`, host directory in the scratch area):

- `run --rm backup backup-now pre-migrate-0123456789ab` succeeded. The dump was mode `-rw-------`, owned by the invoking user, in a `drwx------` directory.
- `up -d --wait backup` reported healthy after 6 s, logged `next_run=2026-11-03T02:30:00+0700`, and did not run a catch-up dump because the marker was fresh.
- The built image carries `cash-flow-tracker.managed=true` and `com.docker.compose.project=cft-ops`. Everything was torn down with `down -v --rmi local`.

## Deploy script

`backend/tests/test_deploy_release_script.py` passed 10 tests. It runs the real `deploy_remote_release.sh` with ssh, scp, docker and curl shims, a temporary "remote" root and a stub Flyway preflight.

- **Success**: build < `run --rm backup backup-now pre-migrate-0123456789ab` < Flyway preflight < `run --rm migrate` < `up`. `up` runs with `COMPOSE_PROFILES=backup`, `BACKUP_HOST_DIR=<root>/production/backups` (mode 0700) and the invoking uid. `current` is promoted. The only prune commands are the two label-filtered ones.
- **Failing backup**: a non-zero exit with "Pre-migration backup failed; aborting before any migration." No preflight, migration, `up`, prune or promotion runs.
- **`BACKUP_HOST_DIR` inside `releases/`**: a non-zero exit with "must not be inside", and no build or run.
- **`backup-dir` unit cases**: the default, the last assignment winning, and rejection of a relative path, `:`, `/`, `/var/lib/docker/...`, the environment root and the release tree.

## Scoped image prune (local Docker 29.9.2, containerd image store)

The script and log are in the scratch area (`ops-hardening/prune/`). Dangling images were created with `docker import`, because this engine drops a replaced build tag instead of keeping it dangling. The two commands from step H then gave:

- **Removed**: the dangling image labelled `cash-flow-tracker.managed=true`, and the dangling image labelled `com.docker.compose.project=cashflow-production`.
- **Kept**: a dangling image with no label, a dangling image of another Compose project, a dangling image with `other-app.managed=true`, and a tagged image with `cash-flow-tracker.managed=true`.
- Pre-existing dangling images (0) were unchanged. Every synthetic image was removed afterwards.

## Static checks

- **actionlint 1.7.7** (`rhysd/actionlint` Docker image, no install): `checks.yml` is clean. `deploy.yml` reports only two SC2086 notices in the unchanged "Setup SSH Key" step (`>> $GITHUB_ENV`). The same two notices appear on the 5004b11 file.
- **shellcheck** (bundled in that image): clean on `db/backup/*.sh`, `scripts/verify/backup_restore_roundtrip.sh` and the remote heredoc of `deploy_remote_release.sh`. The local part of the deploy script reports only SC2086/SC2029 notices on its pre-existing, intentionally split `$ssh_opts`/`$scp_opts`.
- **`docker compose config --quiet`**: passes with and without `--profile backup`.
- **`git diff --cached --check`**: clean for every commit.
- **`openspec validate --all --strict`**: 64 passed, 0 failed before the archive, and 65 passed, 0 failed after it (the new `database-backups` and `release-pipeline` main specs).

## CI on GitHub (free `ubuntu-latest` runners, push to `chore/operations-hardening`)

Branch pushes trigger the new Checks workflow. The Deploy Production workflow did not run, because it is main-only.

- **Run 41385437216** (commit d54d65a): failure. The operations job (51 s) and the frontend job (1 min 45 s) passed. The backend job ran with the still-unpinned install, which gave pytest 9.2.0: `22 failed, 1217 passed` on the migrated database (0 skipped). These are exactly the 22 pytest-9.2 failures reproduced locally. The plan test passed, so the fixture fix held in CI.
- **Run 41385675625** (commit with `pip install -r backend/requirements-dev.txt`): **success**. Wall time from trigger to completion was 2 min 11 s (03:55:51Z → 03:58:02Z), and the three jobs ran in parallel:
  - operations: 51 s, with the round trip "Round trip verified: 30 tables, Flyway version 39";
  - backend: 1 min 57 s, with `pytest-9.0.3`, Flyway "Schema version: 39", and 1239 passed, 0 failed, 0 skipped;
  - frontend: 2 min 7 s.
- The previous single-job preflight had no database. Its duration on main was not measured for this change.
- **Run 41386669903** (fb1dd09, implementation complete): **success**. Wall time was 1 min 48 s (04:07:15Z → 04:09:03Z):
  - backend: 1 min 32 s, with `pytest-9.0.3`, and 1249 passed, 0 failed, 0 skipped in 48 s;
  - operations: 47 s, with "Round trip verified: 30 tables, Flyway version 39";
  - frontend: 1 min 45 s.
