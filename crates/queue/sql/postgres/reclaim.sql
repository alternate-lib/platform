UPDATE alternate.queue_messages
SET state = 'ready',
  run_at = clock_timestamp(),
  leased_by = NULL,
  leased_at = NULL,
  lease_token = NULL,
  lease_expires_at = NULL
WHERE id IN (
  SELECT id
  FROM alternate.queue_messages
  WHERE queue_key = $1
    AND state = 'inflight'
    AND lease_expires_at <= clock_timestamp()
  ORDER BY lease_expires_at,
    created_at,
    id
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
RETURNING 1
