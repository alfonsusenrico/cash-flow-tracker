#!/usr/bin/env bash
# Take one verified PostgreSQL dump into BACKUP_DIR, then prune dumps past retention.
#
# Usage: backup-now [reason]
#   reason  lower-case label in the file name, for example nightly or pre-migrate-1a2b3c4.
#
# Connection settings come from the libpq variables PGHOST, PGPORT, PGUSER, PGPASSWORD and
# PGDATABASE. The log holds only the outcome, file name, size, duration and the free space of
# BACKUP_DIR: never connection settings, tool output or data. A run that ends with less free space
# than DEPLOY_MIN_FREE_GB (default 5 GiB, the deploy's own threshold) logs a disk-space warning.
set -euo pipefail
umask 077

backup_dir=${BACKUP_DIR:-/backups}
retention_days=${BACKUP_RETENTION_DAYS:-14}
lock_wait_seconds=${BACKUP_LOCK_WAIT_SECONDS:-900}
age_recipient=${BACKUP_AGE_RECIPIENT:-}
min_free_gb=${DEPLOY_MIN_FREE_GB:-5}
reason=${1:-nightly}
database=${PGDATABASE:-}

started_at=$(date +%s)
temporary_dump=""
temporary_encrypted=""

# Free space of the filesystem holding BACKUP_DIR in MiB, or "unknown" when df cannot read it.
free_space_mib() {
    local available_kib
    available_kib=$(df -Pk -- "$backup_dir" 2>/dev/null | awk 'NR == 2 { print $4 }') || available_kib=""
    if [[ "$available_kib" =~ ^[0-9]+$ ]]; then
        printf '%s' "$((available_kib / 1024))"
    else
        printf 'unknown'
    fi
}

log() {
    printf '%s backup %s free_mib=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" "$(free_space_mib)"
}

warn_if_low_on_space() {
    local free_mib
    # An invalid threshold has already failed the run as config-min-free; it cannot be compared.
    if [[ ! "$min_free_gb" =~ ^[1-9][0-9]{0,3}$ ]]; then
        return 0
    fi
    free_mib=$(free_space_mib)
    if [[ "$free_mib" =~ ^[0-9]+$ ]] && ((free_mib < min_free_gb * 1024)); then
        log "status=warning reason=${reason} step=disk-space min_free_gb=${min_free_gb}"
    fi
}

remove_temporary_files() {
    if [[ -n "$temporary_dump" ]]; then
        rm -f -- "$temporary_dump"
    fi
    if [[ -n "$temporary_encrypted" ]]; then
        rm -f -- "$temporary_encrypted"
    fi
}

fail() {
    local step=$1
    local exit_code=$2
    remove_temporary_files
    log "status=failed reason=${reason} step=${step} exit=${exit_code} duration_s=$(($(date +%s) - started_at))"
    warn_if_low_on_space
    exit 1
}

trap 'fail interrupted 130' INT TERM

if [[ ! "$reason" =~ ^[a-z0-9][a-z0-9-]{0,39}$ ]]; then
    fail config-reason 2
fi
if [[ ! "$database" =~ ^[A-Za-z0-9_]{1,63}$ ]]; then
    fail config-database 2
fi
if [[ ! "$retention_days" =~ ^[1-9][0-9]{0,3}$ ]]; then
    fail config-retention 2
fi
if [[ ! "$lock_wait_seconds" =~ ^[0-9]{1,5}$ ]]; then
    fail config-lock-wait 2
fi
if [[ ! "$min_free_gb" =~ ^[1-9][0-9]{0,3}$ ]]; then
    fail config-min-free 2
fi
# An age recipient is a public key, so the host holds nothing that can decrypt the dumps.
if [[ -n "$age_recipient" && ! "$age_recipient" =~ ^age1[02-9ac-hj-np-z]{58}$ ]]; then
    fail config-age-recipient 2
fi
if [[ ! -d "$backup_dir" || ! -w "$backup_dir" ]]; then
    fail config-backup-dir 2
fi

# The nightly run and a deploy's pre-migration run share one directory; take turns.
exec 9>"${backup_dir}/.backup.lock"
flock --wait "$lock_wait_seconds" 9 || fail lock $?

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
base_name="cashflow-${database}-${timestamp}-${reason}.dump"
temporary_dump="${backup_dir}/.tmp-${base_name}.$$"

pg_dump --format=custom --file="$temporary_dump" >/dev/null 2>&1 || fail dump $?

# pg_restore --list reads the whole table of contents, so a truncated or corrupt archive fails here.
pg_restore --list "$temporary_dump" >/dev/null 2>&1 || fail verify $?

final_name=$base_name
ready_file=$temporary_dump
if [[ -n "$age_recipient" ]]; then
    final_name="${base_name}.age"
    temporary_encrypted="${backup_dir}/.tmp-${final_name}.$$"
    age --encrypt --recipient "$age_recipient" --output "$temporary_encrypted" "$temporary_dump" \
        >/dev/null 2>&1 || fail encrypt $?
    rm -f -- "$temporary_dump"
    temporary_dump=""
    ready_file=$temporary_encrypted
fi

final_path="${backup_dir}/${final_name}"
sync "$ready_file" || fail sync $?
# A rename within one directory is atomic: readers see either no dump or a complete one.
mv -f -- "$ready_file" "$final_path" || fail rename $?
temporary_dump=""
temporary_encrypted=""
sync "$backup_dir" || fail sync-directory $?
touch "${backup_dir}/.last-success" || fail mark-success $?

size_bytes=$(stat --format=%s "$final_path") || fail stat $?

# Retention runs only after a verified dump, so the newest good dump is never pruned,
# however long backups had been failing before.
retention_minutes=$((retention_days * 24 * 60))
pruned=$(find "$backup_dir" -maxdepth 1 -type f \
    \( -name 'cashflow-*.dump' -o -name 'cashflow-*.dump.age' \) \
    ! -name "$final_name" -mmin "+${retention_minutes}" -print -delete | wc -l) || fail prune $?
# Leftovers of interrupted runs are never valid dumps.
find "$backup_dir" -maxdepth 1 -type f -name '.tmp-cashflow-*' -mmin +1440 -delete || fail prune-temporary $?

log "status=ok reason=${reason} file=${final_name} size_bytes=${size_bytes} duration_s=$(($(date +%s) - started_at)) pruned=${pruned}"
warn_if_low_on_space
