WITH claimed AS (
  SELECT id
  FROM alternate.queue_messages
  WHERE queue_key = $1
    AND state = 'ready'
  ORDER BY created_at, id
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE alternate.queue_messages AS qm
SET state = 'inflight',
    leased_by = $2,
    leased_at = clock_timestamp(),
    lease_token = $3,
    lease_expires_at = clock_timestamp() + ($4::bigint * interval '1 microsecond')
FROM claimed
WHERE qm.id = claimed.id
RETURNING qm.id,
  qm.message,
  qm.attempts,
  qm.attributes
