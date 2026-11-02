#!/usr/bin/env bash
# Healthy while the last verified dump is younger than BACKUP_MAX_AGE_MINUTES (default 26 hours:
# one nightly interval plus slack for a slow run).
set -euo pipefail

backup_dir=${BACKUP_DIR:-/backups}
max_age_minutes=${BACKUP_MAX_AGE_MINUTES:-1560}

[[ -n "$(find "${backup_dir}/.last-success" -mmin "-${max_age_minutes}" 2>/dev/null)" ]]
