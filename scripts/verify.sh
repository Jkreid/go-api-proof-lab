#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

sample_unformatted=$(find cmd internal -name '*.go' -type f -exec gofmt -l {} +)
if [[ -n "$sample_unformatted" ]]; then
  printf 'Run gofmt on:\n%s\n' "$sample_unformatted" >&2
  exit 1
fi

./scripts/check-generated.sh
go vet -tags=integration ./...
./scripts/with-postgres.sh ./scripts/in-cluster-verify.sh
