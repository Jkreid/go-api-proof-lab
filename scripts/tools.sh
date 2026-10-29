#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
sample_root=$(pwd -P)
mkdir -p "$sample_root/.tools/bin"

install_tool() {
  local sample_tool=$1 sample_version=$2 sample_package=$3 sample_existing
  sample_existing=$(command -v "$sample_tool" || true)
  if [[ "$sample_tool" == goose && -n "$sample_existing" ]] && [[ "$("$sample_existing" --version)" == "goose version: $sample_version" ]]; then
    if [[ "$sample_existing" != "$sample_root/.tools/bin/$sample_tool" ]]; then
      cp "$sample_existing" "$sample_root/.tools/bin/$sample_tool"
    fi
    return
  fi
  # sqlc spells its version subcommand differently from goose.
  if [[ "$sample_tool" == sqlc && -n "$sample_existing" ]] && [[ "$("$sample_existing" version)" == "$sample_version" ]]; then
    if [[ "$sample_existing" != "$sample_root/.tools/bin/$sample_tool" ]]; then
      cp "$sample_existing" "$sample_root/.tools/bin/$sample_tool"
    fi
    return
  fi
  GOBIN="$sample_root/.tools/bin" go install "$sample_package@$sample_version"
}

install_tool goose v3.27.1 github.com/pressly/goose/v3/cmd/goose
install_tool sqlc v1.31.1 github.com/sqlc-dev/sqlc/cmd/sqlc
