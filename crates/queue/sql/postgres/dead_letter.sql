UPDATE alternate.queue_messages
SET state = 'dead',
  leased_by = NULL,
  leased_at = NULL,
  lease_token = NULL,
  lease_expires_at = NULL
WHERE queue_key = $1
  AND id = $2
  AND state = 'inflight'
  AND lease_token = $3
RETURNING attempts
