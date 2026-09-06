CREATE SCHEMA IF NOT EXISTS alternate;

CREATE TABLE alternate.queue_messages (
    id TEXT PRIMARY KEY,
    queue_key TEXT NOT NULL,
    message BYTEA NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    attempts INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    leased_by TEXT,
    leased_at TIMESTAMPTZ,
    lease_token TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX queue_messages_ready_idx
ON alternate.queue_messages (queue_key, created_at, id)
WHERE state = 'ready';

CREATE INDEX queue_messages_scheduled_idx
ON alternate.queue_messages (queue_key, run_at)
WHERE state = 'scheduled';

CREATE INDEX queue_messages_inflight_idx
ON alternate.queue_messages (queue_key, lease_expires_at)
WHERE state = 'inflight';

CREATE FUNCTION alternate.queue_messages_notify() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.state = 'ready'
        AND (TG_OP = 'INSERT' OR OLD.state IS DISTINCT FROM 'ready')
    THEN
        PERFORM pg_notify('alternate_queue', NEW.queue_key);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER queue_messages_notify_trg
AFTER INSERT OR UPDATE OF state ON alternate.queue_messages
FOR EACH ROW
EXECUTE FUNCTION alternate.queue_messages_notify();
