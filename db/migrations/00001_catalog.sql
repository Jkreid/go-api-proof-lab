-- +goose Up
CREATE TABLE resources (
    id uuid PRIMARY KEY,
    external_id text NOT NULL UNIQUE
        CHECK (external_id ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
    first_verified_at timestamptz NOT NULL CHECK (isfinite(first_verified_at))
);

CREATE TABLE saved_references (
    id uuid PRIMARY KEY,
    resource_id uuid NOT NULL REFERENCES resources(id) ON DELETE RESTRICT,
    note text NOT NULL CHECK (char_length(note) BETWEEN 1 AND 1000),
    created_at timestamptz NOT NULL CHECK (isfinite(created_at))
);
CREATE INDEX saved_references_resource_idx ON saved_references(resource_id);

CREATE TABLE resource_metadata (
    resource_id uuid PRIMARY KEY REFERENCES resources(id) ON DELETE CASCADE,
    title text NOT NULL CHECK (char_length(title) BETWEEN 1 AND 200),
    summary text CHECK (summary IS NULL OR char_length(summary) BETWEEN 1 AND 2000),
    fetched_at timestamptz NOT NULL CHECK (isfinite(fetched_at))
);
CREATE INDEX resource_metadata_fetched_idx ON resource_metadata(fetched_at, resource_id);

-- +goose Down
DROP TABLE resource_metadata;
DROP TABLE saved_references;
DROP TABLE resources;
