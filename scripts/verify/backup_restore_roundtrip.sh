#!/usr/bin/env bash
# Prove the backup image end to end on disposable containers with synthetic data only:
#   1. migrate a fresh postgres:16 with Flyway as production does, and add synthetic rows;
#   2. take a plain and an age-encrypted dump with backup-now;
#   3. restore each into a NEW database on a second fresh postgres:16 with restore-backup;
#   4. compare exact row counts per table and the Flyway schema version;
#   5. check the negative paths: an existing target is refused, a failed dump leaves no file,
#      and retention removes only dumps older than BACKUP_RETENTION_DAYS;
#   6. check that every backup log line records the free space, that a run below
#      DEPLOY_MIN_FREE_GB logs a disk-space warning, and that an invalid threshold is refused.
#
# Usage: scripts/verify/backup_restore_roundtrip.sh
#   VERIFY_PREFIX  container, network and image name prefix (default cft-verify)
#   VERIFY_WORK_DIR  host directory for dumps; it must be empty or absent (default: mktemp -d)
# Exits non-zero on the first mismatch. Removes every container, network and image it created.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
prefix=${VERIFY_PREFIX:-cft-verify}
work_dir=${VERIFY_WORK_DIR:-$(mktemp -d)}
network="${prefix}-backup-net"
scheduler="${prefix}-backup-scheduler"
source_db="${prefix}-backup-source"
target_db="${prefix}-backup-target"
image="${prefix}-backup-image:verify"
# Disposable credentials for throwaway containers; never a real deployment's.
db_user=ledger
db_password=verify-only-password
db_name=ledger

mkdir -p "$work_dir/plain" "$work_dir/encrypted" "$work_dir/key" "$work_dir/scheduled"
if [[ -n "$(find "$work_dir" -mindepth 2 -print -quit)" ]]; then
    echo "VERIFY_WORK_DIR must be empty: $work_dir" >&2
    exit 2
fi

cleanup() {
    docker rm --force "$source_db" "$target_db" "$scheduler" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    docker image rm "$image" >/dev/null 2>&1 || true
}
trap cleanup EXIT

step() {
    printf '\n== %s\n' "$*"
}

check() {
    local description=$1 output
    shift
    if output=$("$@" 2>&1); then
        echo "PASS ${description}"
    else
        printf '%s\n' "$output" >&2
        echo "FAIL ${description}" >&2
        exit 1
    fi
}

start_postgres() {
    docker run --detach --name "$1" --network "$network" \
        --env POSTGRES_DB="$db_name" --env POSTGRES_USER="$db_user" \
        --env POSTGRES_PASSWORD="$db_password" postgres:16 >/dev/null
    for _ in $(seq 1 60); do
        if docker exec "$1" pg_isready --username="$db_user" --dbname="$db_name" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "postgres container $1 did not become ready" >&2
    exit 1
}

# Same image, flags and locations as the migrate service in docker-compose.yml.
flyway() {
    local host=$1 database=$2
    shift 2
    docker run --rm --network "$network" \
        --volume "$root/db/migrations:/flyway/migrations:ro" \
        --env FLYWAY_URL="jdbc:postgresql://${host}:5432/${database}" \
        --env FLYWAY_USER="$db_user" --env FLYWAY_PASSWORD="$db_password" \
        --env FLYWAY_BASELINE_ON_MIGRATE=true --env FLYWAY_BASELINE_VERSION=1 \
        --env FLYWAY_LOCATIONS=filesystem:/flyway/migrations \
        flyway/flyway:10 -connectRetries=60 "$@"
}

flyway_version() {
    flyway "$1" "$2" info | sed -n 's/^Schema version: *//p' | tr -d '[:space:]'
}

in_backup_image() {
    local host=$1 dumps=$2
    shift 2
    docker run --rm --network "$network" --user "$(id -u):$(id -g)" \
        --env PGHOST="$host" --env PGUSER="$db_user" --env PGPASSWORD="$db_password" \
        --env PGDATABASE="$db_name" --env BACKUP_DIR=/backups --env TZ=Asia/Jakarta \
        --volume "$dumps:/backups" --volume "$work_dir/key:/key" "$@"
}

psql_in() {
    docker exec --env PGPASSWORD="$db_password" "$1" \
        psql --username="$db_user" --dbname="$2" --no-psqlrc --tuples-only --no-align --command="$3"
}

step "Build the backup image and start two fresh postgres:16 servers"
docker build --quiet --tag "$image" "$root/db/backup" >/dev/null
docker network create "$network" >/dev/null
start_postgres "$source_db"
start_postgres "$target_db"

step "Migrate the source with Flyway and add synthetic rows"
flyway "$source_db" "$db_name" migrate >/dev/null
docker exec -i --env PGPASSWORD="$db_password" "$source_db" \
    psql --username="$db_user" --dbname="$db_name" --no-psqlrc --quiet --set=ON_ERROR_STOP=1 >/dev/null <<'SQL'
INSERT INTO users (username, password_hash, invite_code)
SELECT 'fictional-user-' || n, 'fictional-hash', 'FICTIONAL' FROM generate_series(1, 3) AS n;
INSERT INTO accounts (user_id, name, type)
SELECT id, 'Fictional Wallet', 'cash' FROM users;
INSERT INTO categories (user_id, name, kind)
SELECT id, 'Fictional Groceries', 'expense' FROM users;
INSERT INTO transactions (user_id, type, account_id, category_id, amount)
SELECT u.id, 'expense', a.id, c.id, 1000 * n
FROM users u
JOIN accounts a ON a.user_id = u.id
JOIN categories c ON c.user_id = u.id
CROSS JOIN generate_series(1, 25) AS n;
SQL
source_version=$(flyway_version "$source_db" "$db_name")
latest_file_version=$(find "$root/db/migrations" -name 'V*__*.sql' -exec basename {} \; \
    | sed 's/^V\([0-9][0-9]*\)__.*/\1/' | sort -n | tail -1)
echo "source Flyway version: ${source_version}; highest migration file: V${latest_file_version}"
check "source is migrated V1 to the latest file" test "$source_version" = "$latest_file_version"
in_backup_image "$source_db" "$work_dir/plain" "$image" db-row-counts "$db_name" >"$work_dir/source-counts.txt"
echo "source tables: $(wc -l <"$work_dir/source-counts.txt" | tr -d ' '); rows: $(awk '{n += $2} END {print n}' "$work_dir/source-counts.txt"); transactions: $(awk '$1=="transactions"{print $2}' "$work_dir/source-counts.txt")"

step "Retention: plant synthetic old dumps (15 and 13 days old)"
touch -t "$(date -v-15d +%Y%m%d%H%M 2>/dev/null || date -d '15 days ago' +%Y%m%d%H%M)" \
    "$work_dir/plain/cashflow-ledger-20000101T000000Z-expired.dump"
touch -t "$(date -v-13d +%Y%m%d%H%M 2>/dev/null || date -d '13 days ago' +%Y%m%d%H%M)" \
    "$work_dir/plain/cashflow-ledger-20000102T000000Z-recent.dump"

step "Take a plain dump"
in_backup_image "$source_db" "$work_dir/plain" --env BACKUP_RETENTION_DAYS=14 "$image" backup-now roundtrip \
    | tee "$work_dir/plain-backup.log"
plain_dump=$(find "$work_dir/plain" -maxdepth 1 -name 'cashflow-ledger-*-roundtrip.dump' -exec basename {} \;)
check "plain dump written once" test "$(printf '%s\n' "$plain_dump" | grep -c .)" -eq 1
check "log reports success with size and duration" \
    grep -Eq 'status=ok reason=roundtrip file=cashflow-ledger-[0-9TZ]+-roundtrip.dump size_bytes=[1-9][0-9]* duration_s=[0-9]+ pruned=1 free_mib=[0-9]+$' \
    "$work_dir/plain-backup.log"
check "no disk-space warning above the default 5 GiB threshold" \
    bash -c "! grep -q 'step=disk-space' '$work_dir/plain-backup.log'"
check "retention removed the 15-day-old dump" test ! -e "$work_dir/plain/cashflow-ledger-20000101T000000Z-expired.dump"
check "retention kept the 13-day-old dump" test -e "$work_dir/plain/cashflow-ledger-20000102T000000Z-recent.dump"
check "no temporary file left" test -z "$(find "$work_dir/plain" -name '.tmp-*' -print -quit)"
check "healthcheck passes after a success" in_backup_image "$source_db" "$work_dir/plain" "$image" backup-healthcheck

step "Take an age-encrypted dump with a throwaway key"
in_backup_image "$source_db" "$work_dir/encrypted" "$image" age-keygen --output /key/identity.txt 2>/dev/null
recipient=$(in_backup_image "$source_db" "$work_dir/encrypted" "$image" age-keygen -y /key/identity.txt)
in_backup_image "$source_db" "$work_dir/encrypted" --env BACKUP_AGE_RECIPIENT="$recipient" "$image" \
    backup-now roundtrip-encrypted | tee "$work_dir/encrypted-backup.log"
encrypted_dump=$(find "$work_dir/encrypted" -maxdepth 1 -name 'cashflow-ledger-*-roundtrip-encrypted.dump.age' -exec basename {} \;)
check "encrypted dump written" test -n "$encrypted_dump"
check "no plaintext dump beside the encrypted one" \
    test -z "$(find "$work_dir/encrypted" -name '*.dump' -print -quit)"

step "Restore both into new databases on the fresh target server"
in_backup_image "$target_db" "$work_dir/plain" "$image" restore-backup "/backups/${plain_dump}" ledger_restored \
    | tee "$work_dir/restore-plain.log"
in_backup_image "$target_db" "$work_dir/encrypted" --env BACKUP_AGE_IDENTITY_FILE=/key/identity.txt "$image" \
    restore-backup "/backups/${encrypted_dump}" ledger_restored_encrypted | tee "$work_dir/restore-encrypted.log"

for restored in ledger_restored ledger_restored_encrypted; do
    in_backup_image "$target_db" "$work_dir/plain" "$image" db-row-counts "$restored" >"$work_dir/${restored}-counts.txt"
    check "${restored}: row counts match the source for every table" \
        diff -u "$work_dir/source-counts.txt" "$work_dir/${restored}-counts.txt"
    restored_version=$(flyway_version "$target_db" "$restored")
    echo "${restored} Flyway version: ${restored_version}"
    check "${restored}: Flyway version matches the source" test "$restored_version" = "$source_version"
    check "${restored}: Flyway validate passes" flyway "$target_db" "$restored" validate
done

step "Swap the restored database in under the live name, as the runbook does"
psql_in "$target_db" postgres "ALTER DATABASE ${db_name} RENAME TO ${db_name}_before_restore" >/dev/null
psql_in "$target_db" postgres "ALTER DATABASE ledger_restored_encrypted RENAME TO ${db_name}" >/dev/null
in_backup_image "$target_db" "$work_dir/plain" "$image" db-row-counts "$db_name" >"$work_dir/swapped-counts.txt"
check "swapped database has the source row counts" diff -u "$work_dir/source-counts.txt" "$work_dir/swapped-counts.txt"
check "swapped database passes Flyway validate" flyway "$target_db" "$db_name" validate
check "swapped database reports the source Flyway version" \
    test "$(flyway_version "$target_db" "$db_name")" = "$source_version"

step "Scheduler: catch-up run at start, next run at BACKUP_TIME, prompt stop"
in_backup_image "$source_db" "$work_dir/scheduled" --detach --name "$scheduler" --env BACKUP_TIME=02:30 \
    "$image" >/dev/null
for _ in $(seq 1 30); do
    if docker logs "$scheduler" 2>&1 | grep -q 'next_run='; then
        break
    fi
    sleep 1
done
docker logs "$scheduler" 2>&1 | tee "$work_dir/scheduler.log"
check "scheduler caught up with a nightly dump" grep -q 'status=ok reason=nightly' "$work_dir/scheduler.log"
check "scheduler waits for the next 02:30 local time" grep -Eq 'next_run=[0-9-]+T02:30:00\+0700' "$work_dir/scheduler.log"
stop_started=$(date +%s)
docker stop --time 10 "$scheduler" >/dev/null
# docker stop sends TERM and waits 10 s before KILL, so a quick stop means the TERM trap worked.
check "scheduler stops on TERM within 5 seconds" test "$(($(date +%s) - stop_started))" -le 5

step "Negative paths"
if in_backup_image "$target_db" "$work_dir/plain" "$image" restore-backup "/backups/${plain_dump}" ledger_restored \
    >"$work_dir/restore-existing.log"; then
    echo "FAIL restore into an existing database was not refused" >&2
    exit 1
fi
check "restore refuses an existing target" grep -q 'status=failed step=target-exists' "$work_dir/restore-existing.log"
check "refused restore left the existing database intact" \
    test "$(psql_in "$target_db" ledger_restored 'SELECT count(*) FROM transactions')" = \
    "$(awk '$1=="transactions"{print $2}' "$work_dir/source-counts.txt")"

dumps_before=$(find "$work_dir/plain" -maxdepth 1 -name 'cashflow-*' | wc -l | tr -d ' ')
if in_backup_image "$source_db" "$work_dir/plain" --env PGPASSWORD=wrong-password "$image" backup-now failing \
    >"$work_dir/failing-backup.log"; then
    echo "FAIL a dump with a wrong password succeeded" >&2
    exit 1
fi
cat "$work_dir/failing-backup.log"
check "failed dump is logged with its step" grep -Eq 'status=failed reason=failing step=dump exit=[1-9]' \
    "$work_dir/failing-backup.log"
check "failed dump leaves no file" test "$(find "$work_dir/plain" -maxdepth 1 -name 'cashflow-*' | wc -l | tr -d ' ')" = "$dumps_before"
check "failed dump leaves no temporary file" test -z "$(find "$work_dir/plain" -name '.tmp-*' -print -quit)"
check "failed dump records the free space" grep -Eq 'step=dump exit=[1-9][0-9]* duration_s=[0-9]+ free_mib=[0-9]+$' \
    "$work_dir/failing-backup.log"

step "Free space: a warning below DEPLOY_MIN_FREE_GB, and an invalid threshold refused"
# 9999 GiB is more than any runner has free, so the run must still dump and then warn.
in_backup_image "$source_db" "$work_dir/plain" --env DEPLOY_MIN_FREE_GB=9999 "$image" backup-now low-space \
    | tee "$work_dir/low-space-backup.log"
check "low-space run still writes its dump" \
    test -n "$(find "$work_dir/plain" -maxdepth 1 -name 'cashflow-ledger-*-low-space.dump' -print -quit)"
check "low-space run logs a disk-space warning with its free space" \
    grep -Eq 'status=warning reason=low-space step=disk-space min_free_gb=9999 free_mib=[0-9]+$' "$work_dir/low-space-backup.log"
dumps_before=$(find "$work_dir/plain" -maxdepth 1 -name 'cashflow-*' | wc -l | tr -d ' ')
if in_backup_image "$source_db" "$work_dir/plain" --env DEPLOY_MIN_FREE_GB=0 "$image" backup-now bad-threshold \
    >"$work_dir/bad-threshold-backup.log"; then
    echo "FAIL a backup with DEPLOY_MIN_FREE_GB=0 was not refused" >&2
    exit 1
fi
check "invalid threshold is refused before the dump" grep -q 'status=failed reason=bad-threshold step=config-min-free' \
    "$work_dir/bad-threshold-backup.log"
check "refused run writes no dump" test "$(find "$work_dir/plain" -maxdepth 1 -name 'cashflow-*' | wc -l | tr -d ' ')" = "$dumps_before"
check "logs carry no password" bash -c "! grep -rq -e '$db_password' -e wrong-password '$work_dir'/*.log"

echo
echo "Round trip verified: $(wc -l <"$work_dir/source-counts.txt" | tr -d ' ') tables, Flyway version ${source_version}."
