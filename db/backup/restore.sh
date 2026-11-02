#!/usr/bin/env bash
# Restore one dump into a NEW database on the server named by the libpq variables.
#
# Usage: restore-backup <dump file> <target database>
#   An encrypted dump (*.dump.age) needs BACKUP_AGE_IDENTITY_FILE, the path of the age
#   identity (private key) file mounted read-only for this run only.
#
# The target must not exist: this never overwrites a database. It is created, restored in one
# transaction and dropped again if the restore fails. Tool output is suppressed because it can
# quote data; rerun pg_restore by hand to investigate a failure.
set -euo pipefail
umask 077

log() {
    printf '%s restore %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

if [[ $# -ne 2 ]]; then
    echo "usage: restore-backup <dump file> <target database>" >&2
    exit 2
fi
dump_file=$1
target_database=$2

if [[ ! -f "$dump_file" ]]; then
    log "status=failed step=dump-file-missing"
    exit 2
fi
if [[ ! "$target_database" =~ ^[a-z_][a-z0-9_]{0,62}$ ]]; then
    log "status=failed step=target-name"
    exit 2
fi

work_dir=$(mktemp -d)
created_database=""
cleanup() {
    rm -rf -- "$work_dir"
}
trap cleanup EXIT

fail() {
    local step=$1
    local exit_code=$2
    if [[ -n "$created_database" ]]; then
        dropdb --maintenance-db=postgres "$created_database" >/dev/null 2>&1 || true
    fi
    log "status=failed step=${step} exit=${exit_code}"
    exit 1
}

archive=$dump_file
if [[ "$dump_file" == *.age ]]; then
    identity=${BACKUP_AGE_IDENTITY_FILE:-}
    if [[ -z "$identity" || ! -r "$identity" ]]; then
        fail age-identity-missing 2
    fi
    archive="${work_dir}/restore.dump"
    age --decrypt --identity "$identity" --output "$archive" "$dump_file" >/dev/null 2>&1 \
        || fail decrypt $?
fi

pg_restore --list "$archive" >/dev/null 2>&1 || fail verify $?

exists=$(psql --dbname=postgres --no-psqlrc --tuples-only --no-align \
    --set=target="$target_database" <<'SQL'
SELECT count(*) FROM pg_database WHERE datname = :'target';
SQL
) || fail check-target $?
if [[ "$exists" != "0" ]]; then
    fail target-exists 2
fi

createdb --maintenance-db=postgres "$target_database" >/dev/null 2>&1 || fail create $?
created_database=$target_database

pg_restore --exit-on-error --single-transaction --no-owner \
    --dbname="$target_database" "$archive" >/dev/null 2>&1 || fail restore $?

query() {
    psql --dbname="$target_database" --no-psqlrc --tuples-only --no-align --command="$1"
}
flyway_version=none
has_history=$(query "SELECT to_regclass('public.flyway_schema_history') IS NOT NULL") \
    || fail read-flyway-version $?
if [[ "$has_history" == "t" ]]; then
    flyway_version=$(query "SELECT version FROM flyway_schema_history
        WHERE success AND version IS NOT NULL ORDER BY installed_rank DESC LIMIT 1") \
        || fail read-flyway-version $?
fi

log "status=ok database=${target_database} flyway_version=${flyway_version}"
