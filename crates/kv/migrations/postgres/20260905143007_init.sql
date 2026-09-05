CREATE SCHEMA IF NOT EXISTS alternate;

CREATE TABLE alternate.kv_entries (
    key TEXT NOT NULL PRIMARY KEY,
    value BYTEA NOT NULL,
    expires_at TIMESTAMPTZ
);

CREATE INDEX kv_entries_expires_at_idx
ON alternate.kv_entries (expires_at)
WHERE expires_at IS NOT NULL;
