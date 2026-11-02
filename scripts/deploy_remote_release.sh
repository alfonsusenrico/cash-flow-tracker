#!/bin/sh
# Deploy an immutable Git commit archive and runtime.env to the production host.
set -eu

require_value() {
  eval "val=\${$1:-}"
  if [ -z "$val" ]; then
    echo "❌ Missing required deployment variable: $1" >&2
    exit 1
  fi
}

require_file() {
  eval "val=\${$1:-}"
  if [ -z "$val" ] || [ ! -f "$val" ]; then
    echo "❌ Missing required deployment file: $1" >&2
    exit 1
  fi
}

require_value DEPLOY_HOST
require_value DEPLOY_USER
require_value DEPLOY_ROOT
require_value CI_COMMIT_SHA
require_file RUNTIME_ENV_FILE
require_file RELEASE_SOURCE_ARCHIVE

deploy_port="${DEPLOY_SSH_PORT:-${DEPLOY_PORT:-22}}"
target="${DEPLOY_USER}@${DEPLOY_HOST}"
release_sha="${CI_COMMIT_SHA}"
environment_root="${DEPLOY_ROOT%/}/production"
compose_project="cashflow-production"

key_opt=""
key_candidate="${DEPLOY_SSH_KEY_FILE:-}"
case "$key_candidate" in
  ~/*) key_candidate="${HOME:-/home/runner}/${key_candidate#\~/}" ;;
esac

if [ -n "$key_candidate" ] && [ -f "$key_candidate" ]; then
  key_opt="-i $key_candidate"
elif [ -f "${HOME:-}/.ssh/deploy_key" ]; then
  key_opt="-i ${HOME}/.ssh/deploy_key"
elif [ -f "/home/runner/.ssh/deploy_key" ]; then
  key_opt="-i /home/runner/.ssh/deploy_key"
fi

ssh_opts="-p ${deploy_port} -o BatchMode=yes -o StrictHostKeyChecking=accept-new ${key_opt}"
scp_opts="-P ${deploy_port} -o BatchMode=yes -o StrictHostKeyChecking=accept-new ${key_opt}"

echo "🚀 Initiating remote deployment to ${target}:${deploy_port} [SHA: ${release_sha}]"

# 1. Create a secure temporary bootstrap directory on the remote host
bootstrap_dir=$(ssh $ssh_opts "$target" "umask 077; mktemp -d /tmp/cashflow-release.XXXXXXXX")
case "$bootstrap_dir" in
  /tmp/cashflow-release.*) ;;
  *)
    echo "❌ Deployment host returned unsafe temporary path: $bootstrap_dir" >&2
    exit 1
    ;;
esac

cleanup_bootstrap() {
  if [ -n "${bootstrap_dir:-}" ]; then
    ssh $ssh_opts "$target" "rm -rf '$bootstrap_dir'" >/dev/null 2>&1 || true
  fi
}
trap cleanup_bootstrap EXIT HUP INT TERM

echo "📦 Uploading release artifacts to ${bootstrap_dir}..."
scp $scp_opts scripts/release_storage.py "$target:$bootstrap_dir/release_storage.py"
scp $scp_opts "$RELEASE_SOURCE_ARCHIVE" "$target:$bootstrap_dir/source.tar.gz"
scp $scp_opts "$RUNTIME_ENV_FILE" "$target:$bootstrap_dir/runtime.env"

echo "⚙️ Executing remote release installation and promotion..."
ssh $ssh_opts "$target" /bin/sh -s -- "$bootstrap_dir" "$environment_root" "$compose_project" "$release_sha" <<'REMOTE'
set -eu

bootstrap_dir=$1
environment_root=$2
compose_project=$3
release_sha=$4

cleanup_bootstrap() {
  rm -rf "$bootstrap_dir"
}
trap cleanup_bootstrap EXIT HUP INT TERM

# 0. Check free disk space before anything is written. Image layers fill Docker's data directory and
# the pre-migration dump fills the backup directory; a full disk must stop the deploy here, while the
# running release is untouched, not halfway through a build or a backup.
min_free_gb=$(python3 "$bootstrap_dir/release_storage.py" min-free-gb --env-file "$bootstrap_dir/runtime.env")

# Dumps live in a host directory outside the release tree and the Docker volumes. The nightly
# backup service runs as the deploy user that owns it, so that user can read and manage the dumps.
backup_host_dir=$(python3 "$bootstrap_dir/release_storage.py" backup-dir \
  --environment-root "$environment_root" \
  --env-file "$bootstrap_dir/runtime.env")

# Commands that may read stdin get /dev/null: stdin is this script, streamed over SSH.
if ! docker_root_dir=$(docker info --format '{{.DockerRootDir}}' </dev/null) || [ -z "$docker_root_dir" ]; then
  echo "❌ Could not read Docker's data directory; aborting before any build, backup or migration." >&2
  exit 1
fi

require_free_space() {
  space_label=$1
  space_path=$2
  # df -P prints one line per filesystem; its fourth field is the space available to this user, in KiB.
  available_kib=$(df -Pk -- "$space_path" | awk 'NR == 2 { print $4 }')
  case "$available_kib" in
    "" | *[!0-9]*)
      echo "❌ Could not read the free space for ${space_label} at ${space_path}; aborting before any build, backup or migration." >&2
      exit 1
      ;;
  esac
  available_mib=$((available_kib / 1024))
  if [ "$available_kib" -lt $((min_free_gb * 1024 * 1024)) ]; then
    echo "❌ Not enough free disk space for ${space_label} at ${space_path}: ${available_mib} MiB free, below DEPLOY_MIN_FREE_GB=${min_free_gb} GiB. Aborting before any build, backup or migration; the running release is untouched. See docs/production-deployment.md, section 6." >&2
    exit 1
  fi
  echo "💽 ${space_label}: ${available_mib} MiB free at ${space_path} (minimum ${min_free_gb} GiB)."
}

echo "💽 Checking free disk space (DEPLOY_MIN_FREE_GB=${min_free_gb})..."
require_free_space "Docker data" "$docker_root_dir"
require_free_space "database backups" "$backup_host_dir"

# A. Prepare immutable release directory
release_dir=$(python3 "$bootstrap_dir/release_storage.py" prepare \
  --environment-root "$environment_root" \
  --release-sha "$release_sha")

install -m 0600 "$bootstrap_dir/source.tar.gz" "$release_dir/source.tar.gz"
install -m 0600 "$bootstrap_dir/runtime.env" "$release_dir/runtime.env"
python3 "$bootstrap_dir/release_storage.py" secure-file --release-dir "$release_dir" --filename source.tar.gz
python3 "$bootstrap_dir/release_storage.py" secure-file --release-dir "$release_dir" --filename runtime.env

source_dir="$release_dir/source"
rm -rf "$source_dir"
mkdir -p "$source_dir"
tar -xzf "$release_dir/source.tar.gz" -C "$source_dir"
cd "$source_dir"
cp "$release_dir/runtime.env" "$source_dir/.env"

runtime_env="$release_dir/runtime.env"

export BACKUP_HOST_DIR="$backup_host_dir"
BACKUP_UID=$(id -u)
BACKUP_GID=$(id -g)
export BACKUP_UID BACKUP_GID
export COMPOSE_PROFILES=backup

# B. Validate Compose configuration
echo "🔍 Validating docker compose configuration..."
docker compose --project-name "$compose_project" --env-file "$runtime_env" config --quiet

# C. Build containers from archived commit
echo "🔨 Building Docker images for ${compose_project}..."
docker compose --project-name "$compose_project" --env-file "$runtime_env" build

# D. Back up the database, then run migrations (both fail safely before replacing running containers)
# The backup precedes the V14 preflight too, because that preflight may repair Flyway history.
# Commands that may read stdin get /dev/null: stdin is this script, streamed over SSH.
echo "💾 Backing up the database into ${backup_host_dir} before migrations..."
release_short_sha=$(printf '%s' "$release_sha" | cut -c1-12)
if ! docker compose --project-name "$compose_project" --env-file "$runtime_env" run --rm backup \
  backup-now "pre-migrate-${release_short_sha}" </dev/null; then
  echo "❌ Pre-migration backup failed; aborting before any migration." >&2
  exit 1
fi

echo "🔍 Validating Flyway history and checking for the expected V14 checksum correction..."
python3 "$source_dir/scripts/flyway_v14_preflight.py" \
  --project-name "$compose_project" \
  --env-file "$runtime_env" </dev/null

echo "🗄️ Running Flyway database migrations..."
docker compose --project-name "$compose_project" --env-file "$runtime_env" run --rm migrate </dev/null

# E. Start / replace running services
echo "🚀 Starting Compose services..."
docker compose --project-name "$compose_project" --env-file "$runtime_env" up \
  --detach --wait --wait-timeout 180 --remove-orphans

# F. Verify local healthcheck
echo "🩺 Verifying local healthcheck on http://127.0.0.1:8090/healthz..."
sleep 2
curl --fail --silent --show-error --max-time 15 "http://127.0.0.1:8090/healthz" >/dev/null

# G. Promote current release symlink
echo "⭐ Promoting current release pointer..."
python3 "$bootstrap_dir/release_storage.py" promote \
  --environment-root "$environment_root" \
  --release-sha "$release_sha"

# H. Cleanup older releases and this project's dangling images
echo "🧹 Pruning older releases and stale images..."
python3 "$bootstrap_dir/release_storage.py" cleanup \
  --environment-root "$environment_root" \
  --compose-project "$compose_project" \
  --retain 3
# The host is shared: prune only dangling images this project built. The first label is set in
# docker-compose.yml; the second is added by Compose and also covers images built before it.
for image_label in "cash-flow-tracker.managed=true" "com.docker.compose.project=${compose_project}"; do
  if ! docker image prune --force --filter "label=${image_label}" >/dev/null; then
    echo "⚠️ Image prune for label ${image_label} failed; images were left in place." >&2
  fi
done

echo "✅ Deployment of commit ${release_sha} completed successfully!"
REMOTE

echo "🎉 Remote deployment finished successfully!"
