-- name: FindResource :one
SELECT id, external_id, first_verified_at FROM resources WHERE external_id = $1;

-- name: InsertResource :one
INSERT INTO resources (id, external_id, first_verified_at)
VALUES ($1, $2, $3)
ON CONFLICT (external_id) DO NOTHING
RETURNING id, external_id, first_verified_at;

-- name: InsertReference :one
INSERT INTO saved_references (id, resource_id, note, created_at)
VALUES ($1, $2, $3, $4)
RETURNING id, resource_id, note, created_at;

-- name: GetReference :one
SELECT s.id, s.note, s.created_at, r.id AS resource_id, r.external_id, r.first_verified_at
FROM saved_references s JOIN resources r ON r.id = s.resource_id
WHERE s.id = $1;

-- name: GetMetadata :one
SELECT resource_id, title, summary, fetched_at
FROM resource_metadata WHERE resource_id = $1;

-- name: PutMetadata :exec
INSERT INTO resource_metadata (resource_id, title, summary, fetched_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (resource_id) DO UPDATE
SET title = EXCLUDED.title, summary = EXCLUDED.summary, fetched_at = EXCLUDED.fetched_at;

-- name: EvictMetadata :execrows
DELETE FROM resource_metadata WHERE resource_id = $1;

-- name: PurgeMetadata :execrows
WITH expired AS (
    SELECT resource_id FROM resource_metadata
    WHERE fetched_at <= sqlc.arg(cutoff)::timestamptz
    ORDER BY fetched_at, resource_id
    LIMIT sqlc.arg(batch_limit)::int
    FOR UPDATE SKIP LOCKED
)
DELETE FROM resource_metadata m USING expired e WHERE m.resource_id = e.resource_id;
