SHELL := /bin/bash
.DEFAULT_GOAL := verify

export GOCACHE ?= $(CURDIR)/.cache/go-build
export GOMODCACHE ?= $(CURDIR)/.cache/go-mod
export GOTOOLCHAIN := local
export PATH := $(CURDIR)/.tools/bin:$(PATH)

.PHONY: tools generate verify demo serve

tools:
	@./scripts/tools.sh

generate: tools
	@sqlc generate

verify: tools
	@./scripts/verify.sh

demo: tools
	@./scripts/with-postgres.sh ./scripts/run.sh demo

serve: tools
	@./scripts/with-postgres.sh ./scripts/run.sh api
