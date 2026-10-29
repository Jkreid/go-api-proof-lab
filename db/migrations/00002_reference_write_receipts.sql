-- +goose Up
CREATE TABLE reference_write_receipts (
    actor_id uuid NOT NULL CHECK (actor_id <> '00000000-0000-0000-0000-000000000000'),
    operation text NOT NULL CHECK (operation = 'reference.create.v1'),
    key_digest bytea NOT NULL CHECK (octet_length(key_digest) = 32),
    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    state text NOT NULL CHECK (state IN ('claimed', 'succeeded')),
    reference_id uuid REFERENCES saved_references(id) ON DELETE RESTRICT,
    response_body bytea CHECK (octet_length(response_body) BETWEEN 1 AND 8192),
    completed_at timestamptz CHECK (isfinite(completed_at)),
    expires_at timestamptz CHECK (isfinite(expires_at)),
    PRIMARY KEY (actor_id, operation, key_digest),
    CHECK (
        (state = 'claimed' AND reference_id IS NULL AND response_body IS NULL
            AND completed_at IS NULL AND expires_at IS NULL)
        OR
        (state = 'succeeded' AND reference_id IS NOT NULL AND response_body IS NOT NULL
            AND completed_at IS NOT NULL AND expires_at IS NOT NULL
            AND expires_at = completed_at + interval '24 hours')
    )
);
CREATE INDEX reference_write_receipts_expiry_idx
ON reference_write_receipts (expires_at, actor_id, operation, key_digest)
WHERE state = 'succeeded';

-- A claim exists only inside the mutation transaction. Check the current row,
-- not the earlier INSERT event, because finalization happens later in that tx.
-- +goose StatementBegin
CREATE FUNCTION require_completed_reference_receipt() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM reference_write_receipts
        WHERE actor_id = NEW.actor_id AND operation = NEW.operation
            AND key_digest = NEW.key_digest AND state = 'claimed'
    ) THEN
        RAISE EXCEPTION 'unfinished reference receipt' USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER reference_receipt_must_complete
AFTER INSERT OR UPDATE ON reference_write_receipts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (NEW.state = 'claimed')
EXECUTE FUNCTION require_completed_reference_receipt();

-- +goose Down
DROP TABLE reference_write_receipts;
DROP FUNCTION require_completed_reference_receipt();
