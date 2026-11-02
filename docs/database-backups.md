# Database Backups and Restore Runbook

The production database is dumped every night, and before every migration, by the `backup`
service in `docker-compose.yml`. Everything here ships through the repository and the deploy
pipeline. Nothing is installed or edited on the server by hand.

Running any command below against production needs the owner's explicit instruction for that
action (`AGENTS.md`, Production Isolation Guardrail).

## 1. What runs, and when

| When | What | File name |
| --- | --- | --- |
| Nightly at `BACKUP_TIME` (default 02:30, `TZ` Asia/Jakarta) | `backup-now nightly` in the long-running `ledger_backup` container | `cashflow-ledger-<UTC time>-nightly.dump` |
| At container start, when the last success is older than 24 hours | the same nightly run, to catch up a missed night | as above |
| Every deploy, after the image build and before the Flyway preflight and migrations | `backup-now pre-migrate-<12-character commit>` in a one-off container | `cashflow-ledger-<UTC time>-pre-migrate-<sha>.dump` |

If the pre-migration backup fails, the deploy stops with
`❌ Pre-migration backup failed; aborting before any migration.` Nothing is migrated, and the
running release stays online.

Each run:

1. takes the `backup` lock, so a nightly run and a deploy never write at the same time;
2. runs `pg_dump --format=custom` into a temporary `.tmp-…` file in the same directory;
3. verifies it with `pg_restore --list`, which reads the whole archive table of contents;
4. encrypts it when `BACKUP_AGE_RECIPIENT` is set (section 4);
5. flushes it to disk and renames it to its final name, so a dump is either complete or absent;
6. deletes dumps older than `BACKUP_RETENTION_DAYS` (default 14). Retention runs only after a
   verified dump, so the newest good dump is never deleted, even after weeks of failures.

The log line holds only the outcome, file name, size, duration and number of pruned files:

```text
2026-11-02T19:30:00Z backup status=ok reason=nightly file=cashflow-ledger-20261102T193000Z-nightly.dump size_bytes=127000 duration_s=1 pruned=1
2026-11-02T19:30:00Z backup status=failed reason=nightly step=dump exit=1 duration_s=0
```

Tool output is never logged, because it can quote connection details or data. To investigate
a failed `step`, rerun that step by hand.

## 2. Where the dumps are

On the host, in `BACKUP_HOST_DIR`. The default is `<DEPLOY_ROOT>/production/backups`, which is
`/opt/cash-flow-tracker/production/backups` with the default `DEPLOY_ROOT`. The directory is
outside every Docker volume, so the dumps survive the loss of `cashflow-production_db_data`. It
is also outside `releases/`, which release cleanup deletes.

The deploy script resolves the directory with `release_storage.py backup-dir` and creates it with
mode `0700`. It refuses a relative path, a path containing `:`, `/`, the environment root, any
path inside `releases/`, and any path inside `/var/lib/docker`. Dumps are written as the deploy
user (`BACKUP_UID`/`BACKUP_GID` are exported from `id`) with mode `0600`.

Backups stay on this server only (owner decision, 2026-11-02). They do not protect against
losing the whole host or its disk. An off-host copy is a future option (section 6).

## 3. Configuration

All variables are optional. They are materialized into `runtime.env` from GitHub secrets of the
same name, or kept from the previous release's `runtime.env`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `BACKUP_HOST_DIR` | `<DEPLOY_ROOT>/production/backups` | Absolute host directory for dumps |
| `BACKUP_TIME` | `02:30` | Local time (`TZ`) of the nightly dump, `HH:MM` |
| `BACKUP_RETENTION_DAYS` | `14` | Days a dump is kept |
| `BACKUP_AGE_RECIPIENT` | empty (no encryption) | age public key (`age1…`) that dumps are encrypted to |

An invalid value fails the run with `step=config-…`. In a deploy, that stops the release before
any migration.

## 4. Optional encryption at rest

Without `BACKUP_AGE_RECIPIENT`, dumps are stored unencrypted, readable only by the deploy user
(directory `0700`, files `0600`). To encrypt them:

1. On the owner's own machine, not on the server: `age-keygen -o cash-flow-tracker-backup.key`.
   It prints the public key (`age1…`). Keep the key file off the server and back it up. Without
   it, encrypted dumps cannot be restored.
2. Add the public key as the GitHub secret `BACKUP_AGE_RECIPIENT`. It is not secret, but a
   secret is how the workflow passes deploy settings.
3. Deploy. New dumps end in `.dump.age`. Verification runs on the plaintext before encryption,
   and the plaintext temporary file is deleted. Older plain dumps keep their names until
   retention removes them.

The server only holds the public key, so it cannot decrypt the dumps.

## 5. Restore

Restoring always goes into a new database. `restore-backup` refuses a target that already
exists. It restores in one transaction (`--single-transaction --exit-on-error --no-owner`), and
drops the new database again if the restore fails. It prints the restored Flyway version.

Run these commands on the server, from the current release:

```sh
cd /opt/cash-flow-tracker/production/current/source
export COMPOSE_PROFILES=backup
export BACKUP_HOST_DIR=/opt/cash-flow-tracker/production/backups
export BACKUP_UID="$(id -u)" BACKUP_GID="$(id -g)"
compose() { docker compose --project-name cashflow-production --env-file ../runtime.env "$@"; }
```

### 5.1 Restore a dump next to the live database

```sh
ls -l "$BACKUP_HOST_DIR"
compose run --rm --no-deps backup restore-backup /backups/<dump file> ledger_restore
compose run --rm --no-deps backup db-row-counts ledger_restore > /tmp/restore-counts.txt
compose run --rm --no-deps backup db-row-counts ledger > /tmp/live-counts.txt
diff /tmp/live-counts.txt /tmp/restore-counts.txt
```

`db-row-counts` prints table names and exact row counts only. For an encrypted dump, copy the
identity file to the server for this run only, then delete it:

```sh
compose run --rm --no-deps \
  --volume /path/to/cash-flow-tracker-backup.key:/run/age-identity:ro \
  --env BACKUP_AGE_IDENTITY_FILE=/run/age-identity \
  backup restore-backup /backups/<dump file>.age ledger_restore
```

### 5.2 Put the restored database into service

This replaces live data. It needs the owner's explicit go for the swap itself.

```sh
compose stop web frontend api backup
compose exec db sh -c 'psql -U "$POSTGRES_USER" -d postgres \
  -c "ALTER DATABASE ledger RENAME TO ledger_before_restore_$(date +%Y%m%d)" \
  -c "ALTER DATABASE ledger_restore RENAME TO ledger"'
compose run --rm migrate -connectRetries=60 validate
compose up --detach --wait --wait-timeout 180
curl --fail --silent --show-error http://127.0.0.1:8090/healthz
```

Keep `ledger_before_restore_<date>` until the owner confirms the restored data, then drop it.
If the database volume itself was lost, `compose up --detach db` first creates an empty `ledger`.
Then follow 5.1 and 5.2 unchanged: the empty database is renamed out of the way.

### 5.3 Proof

`scripts/verify/backup_restore_roundtrip.sh` runs this procedure on disposable containers with
synthetic data. CI runs it on every check. It migrates a fresh postgres:16 with Flyway, takes a
plain dump and an encrypted dump, restores both into a second fresh postgres:16, and compares
the exact row counts of every table and the Flyway version. It also swaps a restored database in
under the live name, checks retention, and checks the scheduler's catch-up run and prompt stop.
Negative paths: an existing target is refused, and a failed dump leaves no file and no password
in the log.

## 6. Off-host copy (future option, not implemented)

The owner decided on 2026-11-02 to keep backups on the server for now. No off-host copy, sync or
upload exists, and no variable or hook is reserved for one. Candidate destinations for a later,
separately approved change:

- `rsync` over Tailscale to the owner's machine;
- a free cloud bucket.

Either one should copy only verified, preferably encrypted (`.dump.age`) files. It should keep
its credentials out of the log, and it must never delete the local dumps.

## 7. Checks after the first deploy with backups

1. The deploy log shows `💾 Backing up the database into …` followed by a
   `backup status=ok reason=pre-migrate-<sha>` line, before `Running Flyway database migrations`.
2. `ls -l /opt/cash-flow-tracker/production/backups` lists that dump, owned by the deploy user
   with mode `-rw-------`, in a `drwx------` directory.
3. `docker ps --filter name=ledger_backup` shows the container `Up … (healthy)`. It turns
   unhealthy when no dump has succeeded for 26 hours (`BACKUP_MAX_AGE_MINUTES`).
4. The next morning, `docker logs ledger_backup` shows a `reason=nightly` success near 02:30 WIB.
5. Optionally, restore that dump into `ledger_restore` (5.1), compare the row counts, then drop
   `ledger_restore`.
