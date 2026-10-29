#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/require-cluster.sh
case "${1:-}" in
  demo|api) sample_command=$1 ;;
  *) echo "Expected demo or api." >&2; exit 2 ;;
esac
goose -env none -dir db/migrations postgres "$SAMPLE_DATABASE_URL&passfile=/dev/null" up
go build -trimpath -o "$SAMPLE_CLUSTER_DIR/sample-command" "./cmd/$sample_command"
exec "$SAMPLE_CLUSTER_DIR/sample-command"
