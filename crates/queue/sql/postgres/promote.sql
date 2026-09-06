UPDATE alternate.queue_messages
SET state = 'ready',
  run_at = clock_timestamp()
WHERE id IN (
  SELECT id
  FROM alternate.queue_messages
  WHERE queue_key = $1
    AND state = 'scheduled'
    AND run_at <= clock_timestamp()
  ORDER BY run_at,
    created_at,
    id
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
RETURNING 1
