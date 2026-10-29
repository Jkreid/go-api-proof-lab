# Durable identity, disposable metadata, bounded retries

A small Go/PostgreSQL case study: **what should remain usable when every provider
display snapshot disappears?**

Saved document references and authored notes point to a durable external identity.
Titles and summaries live in one replaceable snapshot. A historical verification
timestamp permits future references without retaining a provider response forever.
The provider is an in-process simulator; all documents and notes are synthetic.
Optional idempotency keys add a fourth lifetime: a successful write receipt that
is valid for replay for 24 hours, independently of display snapshots. Physical
retention can last longer until cleanup or key reuse.

## Five-minute review

1. Read the storage choices and alternatives in [the case study](docs/case-study.md).
2. Run `make demo` to see ten asserted HTTP/database scenarios, including lost-response replay.
3. Read [the catalog schema](db/migrations/00001_catalog.sql),
   [the receipt schema](db/migrations/00002_reference_write_receipts.sql), and
   [save/read orchestration](internal/catalog/service.go), and
   [the keyed-write protocol](internal/catalog/idempotency.go).
4. Inspect [the catalog implementation and integration tests](internal/catalog/)
   for concurrent identity/key reuse, rollback, replay, expiry, and cleanup.

## Run locally

Prerequisites: Go **1.26.4**, PostgreSQL **18** (`initdb`, `pg_ctl`, `createdb`,
and `psql` on `PATH`), Bash, and Make. Run as a non-root user. PostgreSQL's normal
shared-memory and process facilities must be available. No running PostgreSQL
service, Docker, provider account, or credentials are needed.

```sh
make tools
make verify
make demo
```

`make tools` installs pinned sqlc and goose binaries into ignored `.tools/bin`
(or copies an already-installed exact version). Go module/build caches stay in
ignored `.cache`. Initial setup needs network access for dependencies and checksum
verification. Dependencies are pinned in `go.mod`/`go.sum`; this is not a claim
that those versions are the latest.

`make verify` checks formatting, regenerates SQL queries in a temporary directory
and compares them, runs vet, checks migration up/down/up, and runs race-enabled
unit and real database tests. Integration tests use separate migrated schemas and
fail when isolated database settings are absent. GitHub Actions runs `make verify`
and `make demo`; a local pass does not establish a hosted CI pass.

Each database command creates a new cluster in a private temporary directory,
uses a Unix socket with TCP disabled, and removes that cluster on exit. It ignores
ambient database URLs and PostgreSQL connection settings. The Go executables
also check the wrapper's marker and expected connection destination. These guards
prevent accidental use of unrelated databases; they are not a security boundary.

For manual HTTP exploration:

```sh
make serve
```

In another terminal:

```sh
curl -i http://127.0.0.1:8080/references \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: review-chapter-001' \
  -d '{"externalId":"doc-101","note":"Review the transaction chapter."}'

# Repeat that POST unchanged within 24 hours to replay its original response.
# Without a key, each successful request intentionally creates another reference:
curl -i http://127.0.0.1:8080/references \
  -H 'Content-Type: application/json' \
  -d '{"externalId":"doc-101","note":"A separate intentional save."}'

# Substitute a returned reference ID:
curl http://127.0.0.1:8080/references/REFERENCE_ID
```

The simulator initially knows `doc-101` and `doc-202`. Stop the server with Ctrl-C
to remove its disposable data. `make serve` uses real time; the walkthrough uses
an injected clock and simulator controls, with no sleeps or HTTP control endpoints.

## HTTP behavior

`POST /references` accepts exactly `externalId` and `note`. External IDs are
1–64 lowercase ASCII letters, digits, underscores, or hyphens, starting with a
letter or digit. Notes are trimmed and must contain 1–1,000 Unicode characters.
Bodies are limited to 16 KiB; duplicate/unknown fields and trailing JSON are rejected.

A first save requires a valid provider result for the exact external ID. Identity,
initial metadata, and the saved reference commit together. Later saves need no
provider call. Without `Idempotency-Key`, every successful POST returns `201` and
a new reference; repeated saves intentionally create separate notes sharing the
resource ID.

`GET /references/{id}` returns the note, stable resource ID/external ID,
`firstVerifiedAt`, `metadataState` (`fresh`, `stale`, or `missing`), and nullable
`metadata` containing title, summary, and `fetchedAt`. When metadata is missing,
the external ID is the display fallback. Missing display data does not invalidate
the saved reference.

| Situation | Behavior |
| --- | --- |
| Snapshot younger than 1 minute | Return fresh metadata without provider I/O |
| Stale or absent snapshot | Make one lookup, bounded to 500 ms |
| Valid result | Replace normalized display data and fetch time |
| Temporary failure before `fetchedAt + 11 minutes` | Return labeled stale data, without extending its lifetime |
| Temporary failure at/after that boundary | Return the reference with missing metadata |
| Definitive not-found | Delete the snapshot, preserve identity and notes |
| Malformed provider result | Preserve stored state; return missing display, not stale fallback |

Existing references return `200` for these provider failures. An unknown external
identity returns `404` for not-found, `503` for temporary failure, or `502` for a
malformed result, creating no rows. Invalid input uses `400`, oversized bodies
`413`, unsupported media `415`; storage unavailability uses `503` and invalid
stored state uses `500`. Request work has a two-second context deadline. Cancellation
does not return a fallback after the caller has stopped waiting.

## Retrying a write

An optional `Idempotency-Key` protects a retry within a **24-hour window from
successful completion**. Keys must match `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`;
empty, duplicate, or malformed key headers are rejected. Keys are case-sensitive.
The scope is actor + operation + key. The HTTP server uses one fixed synthetic
actor; tests exercise independent actor scopes. This is not authentication.

- The first successful keyed write commits the reference and its exact `201` body
  together. The receipt contains stable identity and authored content, without
  provider display metadata. Raw keys are hashed before storage.
- The same key and normalized request replay those bytes, `Content-Type`, and
  `Location`. JSON property order and surrounding note whitespace do not change
  the fingerprint. `Idempotency-Replayed` is `false` initially and `true` on replay.
- A different normalized request under that key returns `409 idempotency_conflict`
  while the receipt is live. Failed writes and provider errors are not cached.
- Concurrent retries coordinate through a PostgreSQL unique key and row lock.
  A busy keyed write returns `503` with `Retry-After: 1`; retry the same request.
  The keyed transaction has a 250 ms timeout per lock acquisition within the
  two-second operation deadline, not a guaranteed 250 ms total duration.
- At exactly `expiresAt`, the receipt stops protecting retries. Reusing that key
  may create a new reference, including with a different payload. No permanent
  used-key tombstones remain. Cleanup is not required for expiration to take effect.

If response delivery or commit acknowledgement is uncertain, retry the same
actor/key/payload within the window. The client must retain them; switching keys
can duplicate the business write. This is bounded deduplication, not exactly-once
execution or protection against retries after expiry. Replay does not consult the
provider or current display metadata. A later `GET` reflects the current metadata
policy rather than rewriting the historical create receipt.

## Eviction and limits

Snapshots become eligible for manual cleanup one hour after their last successful
fetch. Cleanup deletes at most a selected batch and skips locked rows. Eligibility
is not a guaranteed deletion deadline: there is no scheduler. The demo includes
explicit eviction; tests cover age-based cleanup and exact boundaries. Expired
write receipts have a separate bounded cleanup command. Deleting either kind of
expiring row leaves resources and saved references intact.

For maintenance exploration within a single disposable cluster:

```sh
make tools
# These commands stay in the same child shell/cluster:
PATH="$PWD/.tools/bin:$PATH" ./scripts/with-postgres.sh bash
source ./scripts/require-cluster.sh
goose -env none -dir db/migrations postgres "$SAMPLE_DATABASE_URL&passfile=/dev/null" up
go run ./cmd/maintenance purge -limit 100
go run ./cmd/maintenance purge-receipts -limit 100
go run ./cmd/maintenance evict -external-id doc-101
exit
```

The empty-cluster example removes zero rows. To inspect populated data, run the
API inside that same shell before using maintenance. There is no arbitrary database
URL argument.

This is a single-provider, single-user local sample without authentication. There
is no metadata history, negative cache, retry loop, background worker, matching
engine, or permanent idempotency ledger. Provider I/O holds no database transaction.
Live receipt payloads and deadlines remain unchanged. Concurrent contenders may
create extra row versions while acquiring the same key. Live receipts are validated
before replay; expired response bytes are discarded without decoding them as a
current response. There is no key quota or scheduled cleanup in this sample.
Overlapping refreshes are unordered: late results can replace newer display data,
and an in-flight lookup can repopulate an evicted snapshot. Application clock skew
affects freshness and receipt expiry. Historical verification does not prove current availability.
There are no production-readiness, exactly-once, or benchmark claims.

## Origin and authorship

The storage-boundary question arose in prior private backend work. This independent
sample uses a new generic domain and synthetic data. Stable identity and replaceable
snapshot patterns informed it; independent historical verification, eviction,
the simulator, and the runnable case study were developed for this sample.
Prior private work also informed scoped fingerprints, atomic receipt storage,
and replay. Reusable 24-hour expiry, the no-op UPSERT used to serialize contenders,
and SQL lock timeout handling are sample additions; this is not a verbatim extraction.

Justin Reid framed and challenged the permanent-storage requirements. The sample's
specific policies and mechanisms were developed collaboratively with AI assistance;
implementation and tests were also AI-assisted. This is not a claim that every
mechanism was his original invention or that the sample has production usage.
See [the case study](docs/case-study.md) for the reasoning and its limits.
