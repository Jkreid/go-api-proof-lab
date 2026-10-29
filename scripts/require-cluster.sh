#!/usr/bin/env bash
# Source from internal commands before running migrations or maintenance.
sample_expected_marker='go-api-reliability-lab disposable PostgreSQL cluster v1'
if [[ -z "${SAMPLE_CLUSTER_DIR:-}" || -z "${SAMPLE_DATABASE_URL:-}" ]]; then
  echo "Use scripts/with-postgres.sh to create a disposable cluster." >&2
  exit 1
fi
case "$SAMPLE_CLUSTER_DIR" in
  /tmp/go-api-reliability-lab.*|/private/tmp/go-api-reliability-lab.*) ;;
  *) echo "Refusing a cluster outside the disposable sample directory." >&2; exit 1 ;;
esac
if [[ ! "$(basename "$SAMPLE_CLUSTER_DIR")" =~ ^go-api-reliability-lab\.[[:alnum:]]+$ ]] ||
   [[ ! -d "$SAMPLE_CLUSTER_DIR" || -L "$SAMPLE_CLUSTER_DIR" ]] ||
   [[ ! -f "$SAMPLE_CLUSTER_DIR/.sample-cluster" || -L "$SAMPLE_CLUSTER_DIR/.sample-cluster" ]] ||
   [[ "$(cat "$SAMPLE_CLUSTER_DIR/.sample-cluster")" != "$sample_expected_marker" ]] ||
   [[ "$(cd "$SAMPLE_CLUSTER_DIR" && pwd -P)" != "$SAMPLE_CLUSTER_DIR" ]] ||
   [[ "$SAMPLE_DATABASE_URL" != "postgresql://sample_user@/sample_catalog?host=$SAMPLE_CLUSTER_DIR/socket&port=5432&sslmode=disable" ]]; then
  echo "Refusing unverified database settings; start a new disposable sample cluster." >&2
  exit 1
fi
for sample_env_name in ${!PG@} ${!GOOSE@}; do
  unset "$sample_env_name"
done
unset DATABASE_URL
