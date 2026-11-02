#!/usr/bin/env bash
# Run backup-now once a day at BACKUP_TIME (HH:MM, container local time from TZ).
# A missed run is caught up at start: when the last success is older than a day, back up now.
set -euo pipefail

backup_dir=${BACKUP_DIR:-/backups}
backup_time=${BACKUP_TIME:-02:30}
success_marker="${backup_dir}/.last-success"

log() {
    printf '%s backup-schedule %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

if [[ ! "$backup_time" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]]; then
    log "status=failed step=config-time"
    exit 2
fi

sleeper=""
stop() {
    if [[ -n "$sleeper" ]]; then
        kill "$sleeper" 2>/dev/null || true
    fi
    exit 0
}
trap stop INT TERM

run_backup() {
    # backup-now logs its own outcome; a failed night must not stop the schedule.
    if ! backup-now nightly; then
        log "status=failed-run-will-retry-next-schedule"
    fi
}

if [[ -z "$(find "$success_marker" -mmin -1440 2>/dev/null)" ]]; then
    run_backup
fi

while true; do
    now=$(date +%s)
    next=$(date -d "today ${backup_time}" +%s)
    if ((next <= now)); then
        next=$(date -d "tomorrow ${backup_time}" +%s)
    fi
    log "next_run=$(date -d "@${next}" +%Y-%m-%dT%H:%M:%S%z)"
    # Sleeping in the background keeps the TERM trap responsive during a long wait.
    sleep "$((next - now))" &
    sleeper=$!
    wait "$sleeper" || true
    sleeper=""
    run_backup
done
