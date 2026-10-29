# Which information earns durable storage?

The question behind this example is whether every piece of information used while
processing a request also needs a permanent place in the application model.
It often does not. That still leaves a harder question: what would stop working
if we discarded it?

Here, a user saves a technical document with an authored note. The provider knows
the document's current title and summary. Those fields can change, disappear,
or become temporarily impossible to retrieve. The application owns the saved
reference and note; it should not lose them because a display lookup failed.

## Alternatives evaluated for this sample

| Choice | What it buys | What it costs |
| --- | --- | --- |
| Copy provider metadata into every saved reference | Reads need no separate lookup; each reference keeps a historical display | Duplicate data and update fan-out; silently introduces a history requirement |
| Fetch all metadata on every read | No stored display snapshot or freshness policy | Every display depends on provider latency and availability; repeated lookups |
| Durable identity plus one replaceable snapshot | Shared identity, independent notes, bounded display reuse, eviction without relationship loss | A metadata table, lifecycle policy, and partial-display behavior to explain and test |

These are prospective design comparisons, not claims of historical prototypes or
measured performance results. The selected approach fits this sample's requirements;
it is not a universal argument that caching is better.

## Four lifetimes, four reasons

**Identity and historical verification are durable.** Every saved reference points to the same internal resource
ID for one external ID. A database unique constraint enforces this even when first
saves race. Relationships do not point to a cache row or copied provider title.

The sample permits another reference
to a previously verified resource after metadata eviction. That requirement earns
one timestamp on the identity. It records the successful lookup associated with
the winning initial identity transaction, not necessarily the earliest concurrent
provider observation. It makes no promise that the external document still exists.
If each new reference required current availability, this policy would be wrong.

**Application references are durable.** The authored note belongs to the
application, rather than to the provider or the retry mechanism. Every intentional
save creates its own reference to shared identity. Neither display eviction nor
receipt cleanup deletes that authored history.

**Display data is replaceable.** One title/summary snapshot avoids repetitive
lookups while fresh and permits a short, explicitly labeled stale fallback on
temporary failure. A new successful lookup replaces it. There is no snapshot
history because the sample has no audit or historical-display requirement.
Raw lookup envelopes and diagnostic fields are discarded after normalization.
They are not domain facts, and no debugging requirement here justifies archiving
them. The implementation makes no memory-erasure claim.

**Write receipts are temporary promises.** An uncertain client needs a way to
ask whether its save committed without creating a second reference. That requirement
earns a separate receipt containing the canonical request fingerprint and exact
successful create response. Its 24-hour window is independent of the one-minute
display freshness and one-hour snapshot cleanup policy. A receipt contains no
provider title or summary. It remains stable even when every display snapshot is
deleted. At expiry the key can be reused; the saved note still survives.

PostgreSQL holds durable rows, disposable snapshots, and expiring receipts. Storage technology
does not determine which lifetime a row deserves. A separate cache service would
add operations and failure modes without evidence that this tiny sample needs one.

## What failure means

The provider simulator is authoritative only within this synthetic example. It
can return a matching document, a timeout/outage, a definitive absence, or malformed
data. Those outcomes do not mean the same thing.

An outage does not disprove historical identity. The application returns its saved
record, with a snapshot only while the stale grace allows. A not-found result
removes display data but preserves the identity and authored note. Malformed output
cannot establish a new identity, freshen a snapshot, or justify stale fallback.
Database failure remains an error: returning an empty catalog would hide a different
kind of problem.

The one-minute freshness, ten-minute stale grace, and one-hour cleanup eligibility
are deliberately short demonstration values. Equality is stale at one minute,
outside the fallback window at eleven minutes, and eligible for cleanup at one
hour. Successful lookup completion supplies the fetch timestamp. Failed lookups
never reset the clock. Cleanup is explicit and bounded, so delayed maintenance
can retain undisplayed snapshots longer than one hour.

The walkthrough makes these distinctions visible through HTTP responses, provider
call counts, and row counts. Tests verify guarantees rather than speed: references
share identity, an initial failure rolls back all rows, refresh preserves notes,
and complete metadata eviction leaves historical identity usable.

## What the retry promise requires

The key is scoped to an actor and the create-reference operation. Its stored digest
identifies the retry; a separate fingerprint covers the validated, normalized
request. Canonicalization makes equivalent JSON and trimmed note text equivalent
without treating different authored content as a retry.

The first successful keyed write stores its reference and receipt in one database
transaction. A completed live receipt found by the read-only preflight replays
without a write. PostgreSQL uniqueness and a no-op UPSERT serialize contenders
that reach the transaction; a contender sees the committed receipt before deciding
whether to replay or conflict. A deferred database constraint prevents a pending
claim from committing without its response. Failed transactions roll back both,
and failures are not retained as receipts. A live receipt is not refreshed or
extended by retries. The UPSERT trades some row-version churn among contenders
for a compact concurrency protocol.

Provider verification for a new identity happens outside transactions. A later
transactional check still arbitrates races with other saves. Lock waits in the keyed finalization transaction
have a 250 ms per-lock timeout; the HTTP request has a two-second deadline.
Waiting callers can receive `503` with `Retry-After: 1`; a subsequent attempt must
reuse the same key and payload. The timeout is not a guarantee that every operation
finishes in 250 ms.

The walkthrough fails an original response write after the database commits, then
checks that retry returns the exact original body and Location. It also shows a
payload conflict, replay during an outage after metadata eviction, and a new save
at the exact 24-hour boundary. The client should not interpret that last save as
proof of exactly-once execution: the protection is intentionally finite.

The alternative is retaining used-key tombstones indefinitely, which would ban
reuse but add permanent state. This sample chooses reusable expiry because it
only promises a bounded client retry window. Both lazy replacement and a separate
bounded cleanup command remove expired receipts without deleting business history.

## Deliberate limits and reasons to reconsider

This example has one provider, one identity namespace, and no identity reuse or
revocation policy. No real catalog terms or availability guarantees are implied.
Reads perform at most one bounded lookup and do not retry internally. Concurrent
refresh completion order is not provider revision order; a late lookup can refill
an evicted row or overwrite a more recent result. Coordinating those operations
would require additional state or serialization, which this sample does not claim.

Requirements that would change the choice include:

- Historical display/audit requirements: justify retained snapshots and an explicit retention policy.
- Revocable verification or external ID reuse: require a richer identity/verification model.
- Provider retention restrictions: change whether snapshots can be stored and for how long.
- Measured provider cost, latency, or contention: may justify coalescing, backoff, or proactive refresh.
- Required ordering or an eviction barrier: justify synchronization beyond last-committed updates.
- Retries beyond 24 hours or permanent key uniqueness: require a longer window or retained tombstones.
- Untrusted callers or high retry volume: require authentication, quotas, and measured contention controls.

There is no matching engine or candidate database. Temporary comparison evidence
would need its own concrete review/debug requirement before gaining storage.
Proactive lookup is a separate possible decision, not hidden background work here.

## Contribution and origin

Justin Reid's contribution was framing and challenging permanent-storage needs:
which information is a durable fact, which is display data, and which can be
retrieved or recomputed. The concrete lifecycle choices and implementation in this
sample were developed collaboratively with AI assistance.

The sample adapts broad identity/snapshot separation patterns from prior private
backend work. It does not reproduce that product's model or claim that every
mechanism originated with Justin. Independent historical verification and explicit
eviction are new sample decisions; the earlier design used retained metadata as
evidence for future eligibility. The provider simulator, failure walkthrough,
isolated tooling, and sample-specific tests are also new. Prior private work
informed actor-scoped fingerprints, transactional response receipts, conflict
handling, and replay. This sample adds reusable 24-hour expiry without tombstones,
the no-op UPSERT protocol, and SQL lock timeout handling; these are explicit new
choices rather than claims about that earlier implementation. No production usage,
human-only authorship, historical benchmark, or matching implementation is claimed.
