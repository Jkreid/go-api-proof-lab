# Authorship and attribution

Maintainer and project owner: [Justin Reid (@Jkreid)](https://github.com/Jkreid).

## Public Git identity

```text
Justin Reid <7332149+Jkreid@users.noreply.github.com>
```

Commits use this GitHub noreply address for account attribution without publishing
a personal email address.

## Contributions and AI assistance

Justin framed and challenged the permanent-storage requirements: which information
is a durable fact, which is display data, and which can be retrieved or recomputed.
The sample's concrete policies and mechanisms were developed collaboratively with
AI assistance through OpenAI Codex. Implementation, tests, and documentation were
also AI-assisted. Git authorship identifies the contributor responsible for the
commits; it does not imply human-only implementation.

Prior private backend work informed the identity/snapshot separation and scoped
fingerprint, atomic receipt, and replay patterns. Independent historical
verification, explicit eviction, the simulator and walkthrough, reusable 24-hour
expiry, the no-op UPSERT protocol, and SQL lock timeout handling were developed
for this sample. The [case study](docs/case-study.md#contribution-and-origin)
explains these distinctions. No production usage or original invention of every
mechanism is claimed.

## Third-party components

Runtime and migration dependencies are recorded in [go.mod](go.mod) and
[go.sum](go.sum); pinned development tools are recorded in
[scripts/tools.sh](scripts/tools.sh). PostgreSQL is an external prerequisite.
Generated sqlc files retain their generator headers. Third-party components
retain their own licenses; the project's [MIT license](LICENSE) does not replace
them.
