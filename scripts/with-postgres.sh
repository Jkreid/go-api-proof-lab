#!/usr/bin/env bash
# Start only a new disposable cluster. Never use an existing connection string.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ $# -eq 0 ]]; then
  echo "usage: scripts/with-postgres.sh command [arguments...]" >&2
  exit 2
fi
if [[ "$(id -u)" -eq 0 ]]; then
  echo "Run this sample as a non-root user; initdb requires it." >&2
  exit 1
fi
for sample_binary in initdb pg_ctl createdb; do
  if ! command -v "$sample_binary" >/dev/null; then
    echo "Missing PostgreSQL 18 tool: $sample_binary (add its bin directory to PATH)." >&2
    exit 1
  fi
done
if [[ "$(initdb --version)" != *" 18."* ]]; then
  echo "PostgreSQL 18 tools are required." >&2
  exit 1
fi

# libpq, pgx, and goose must not discover credentials, service files, or options
# through the invoking shell. The only URL passed below is constructed here.
for sample_env_name in ${!PG@} ${!GOOSE@}; do
  unset "$sample_env_name"
done
unset DATABASE_URL SAMPLE_DATABASE_URL SAMPLE_CLUSTER_DIR
export LC_ALL=C TZ=UTC
umask 077
sample_cluster_dir=$(mktemp -d /tmp/go-api-reliability-lab.XXXXXXXX)
sample_cluster_dir=$(cd "$sample_cluster_dir" && pwd -P)
sample_marker='go-api-reliability-lab disposable PostgreSQL cluster v1'
printf '%s\n' "$sample_marker" > "$sample_cluster_dir/.sample-cluster"
mkdir "$sample_cluster_dir/socket"

cleanup() {
  local sample_status=$?
  trap - EXIT INT TERM
  if pg_ctl -D "$sample_cluster_dir/data" status >/dev/null 2>&1; then
    if ! pg_ctl -D "$sample_cluster_dir/data" -m immediate -w stop > /dev/null 2>&1; then
      echo "Could not stop the disposable cluster; its files were retained at $sample_cluster_dir." >&2
      exit 1
    fi
  fi
  if [[ -f "$sample_cluster_dir/.sample-cluster" ]] && [[ "$(cat "$sample_cluster_dir/.sample-cluster")" == "$sample_marker" ]]; then
    rm -rf -- "$sample_cluster_dir"
  fi
  exit "$sample_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if ! initdb -D "$sample_cluster_dir/data" --username=sample_user --encoding=UTF8 --locale=C --auth-local=trust --auth-host=reject > "$sample_cluster_dir/initdb.log" 2>&1; then
  cat "$sample_cluster_dir/initdb.log" >&2
  exit 1
fi
cat >> "$sample_cluster_dir/data/postgresql.conf" <<EOF
listen_addresses = ''
port = 5432
unix_socket_directories = '$sample_cluster_dir/socket'
unix_socket_permissions = 0700
timezone = 'UTC'
max_connections = 40
shared_buffers = '16MB'
EOF
if ! pg_ctl -D "$sample_cluster_dir/data" -l "$sample_cluster_dir/postgres.log" -w start > /dev/null 2>&1; then
  cat "$sample_cluster_dir/postgres.log" >&2
  exit 1
fi
: > "$sample_cluster_dir/empty-passfile"
PGPASSFILE="$sample_cluster_dir/empty-passfile" createdb --host="$sample_cluster_dir/socket" --port=5432 --username=sample_user --no-password sample_catalog

# mktemp's fixed prefix and alphanumeric suffix need no URL escaping.
export SAMPLE_CLUSTER_DIR="$sample_cluster_dir"
export SAMPLE_DATABASE_URL="postgresql://sample_user@/sample_catalog?host=$sample_cluster_dir/socket&port=5432&sslmode=disable"
"$@"
