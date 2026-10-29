-- name: FindReferenceReceipt :one
SELECT * FROM reference_write_receipts
WHERE actor_id = $1 AND operation = $2 AND key_digest = $3;

-- A no-op UPDATE is deliberate: acquire and return a conflicting row in one
-- statement, even when expiry cleanup races. It does create a new row version.
-- name: AcquireReferenceReceipt :one
INSERT INTO reference_write_receipts (actor_id, operation, key_digest, request_fingerprint, state)
VALUES ($1, $2, $3, $4, 'claimed')
ON CONFLICT (actor_id, operation, key_digest)
DO UPDATE SET key_digest = EXCLUDED.key_digest
RETURNING *;

-- name: ReclaimReferenceReceipt :execrows
UPDATE reference_write_receipts
SET request_fingerprint = $4, state = 'claimed', reference_id = NULL,
    response_body = NULL, completed_at = NULL, expires_at = NULL
WHERE actor_id = $1 AND operation = $2 AND key_digest = $3 AND state = 'succeeded';

-- name: CompleteReferenceReceipt :one
UPDATE reference_write_receipts
SET state = 'succeeded', reference_id = $4, response_body = $5,
    completed_at = $6, expires_at = $7
WHERE actor_id = $1 AND operation = $2 AND key_digest = $3 AND state = 'claimed'
RETURNING *;

-- name: PurgeReferenceReceipts :execrows
WITH expired AS (
    SELECT actor_id, operation, key_digest FROM reference_write_receipts
    WHERE state = 'succeeded' AND expires_at <= sqlc.arg(cutoff)::timestamptz
    ORDER BY expires_at, actor_id, operation, key_digest
    LIMIT sqlc.arg(batch_limit)::int
    FOR UPDATE SKIP LOCKED
)
DELETE FROM reference_write_receipts r USING expired e
WHERE r.actor_id = e.actor_id AND r.operation = e.operation AND r.key_digest = e.key_digest;
