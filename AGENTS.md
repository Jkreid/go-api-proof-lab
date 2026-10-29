# Contributor guide

This is a small Go/PostgreSQL engineering case study about the separate lifetimes
of external identity, application-owned references, provider display data, and
bounded write receipts.

- Start with `README.md` and `docs/case-study.md`. Keep changes tied to their stated guarantees.
- Run `make tools`, then `make verify` and `make demo`. PostgreSQL 18 binaries and Go 1.26.4 are required.
- Use only the disposable database wrapper. Never substitute an existing database URL, credentials, or a shared database. Integration tests require the wrapper and fail when it is absent.
- Edit SQL in `db/queries` and `db/migrations`; run `make generate` to update `internal/db/queries`. Do not hand-edit generated code.
- Keep provider I/O outside transactions. Identity and historical verification survive metadata eviction. Provider failures are not database failures.
- Keyed saves atomically commit the reference and successful response receipt. Preserve actor/operation/key scoping, canonical fingerprints, exact replay, and 24-hour reusable expiry. Do not persist failures, raw keys, provider metadata in receipts, or permanent key tombstones.
- Keep the no-key API behavior unchanged. Bound keyed lock waits; test concurrency, rollback, failed delivery, and exact expiry against real PostgreSQL. Metadata cleanup and receipt cleanup must leave durable history intact.
- Keep fixtures synthetic. Do not add real provider accounts, raw response archives, private source identifiers, paths, or history.
- No background jobs, extra providers, frontend, or generic caching framework without a concrete requirement and scope review.
- Distinguish executed checks from assumptions. Do not claim production usage, exactly-once behavior, or security guarantees this sample does not establish.
- Preserve the contribution and AI-assistance distinctions in the case study. Publishing, choosing a license, or adding a remote requires the owner's explicit approval.
