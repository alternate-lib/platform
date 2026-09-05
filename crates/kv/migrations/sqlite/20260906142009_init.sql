CREATE TABLE kv_entries (
    key TEXT NOT NULL PRIMARY KEY,
    value BLOB NOT NULL,
    expires_at TEXT
);

CREATE INDEX kv_entries_expires_at_idx
ON kv_entries (expires_at)
WHERE expires_at IS NOT NULL;
