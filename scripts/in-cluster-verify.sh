#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/require-cluster.sh

goose -env none -dir db/migrations postgres "$SAMPLE_DATABASE_URL&passfile=/dev/null" up
goose -env none -dir db/migrations postgres "$SAMPLE_DATABASE_URL&passfile=/dev/null" down-to 0
goose -env none -dir db/migrations postgres "$SAMPLE_DATABASE_URL&passfile=/dev/null" up
go test -race -tags=integration -count=1 -timeout=2m ./...
