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

# B. Validate Compose configuration
echo "🔍 Validating docker compose configuration..."
docker compose --project-name "$compose_project" --env-file "$runtime_env" config --quiet

# C. Build containers from archived commit
echo "🔨 Building Docker images for ${compose_project}..."
docker compose --project-name "$compose_project" --env-file "$runtime_env" build

# D. Run database migrations (fails safely before replacing running containers)
echo "🗄️ Running Flyway database migrations..."
docker compose --project-name "$compose_project" --env-file "$runtime_env" run --rm migrate

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

# H. Cleanup older releases and dangling images
echo "🧹 Pruning older releases and stale images..."
python3 "$bootstrap_dir/release_storage.py" cleanup \
  --environment-root "$environment_root" \
  --compose-project "$compose_project" \
  --retain 3
docker image prune --force >/dev/null 2>&1 || true

echo "✅ Deployment of commit ${release_sha} completed successfully!"
REMOTE

echo "🎉 Remote deployment finished successfully!"
