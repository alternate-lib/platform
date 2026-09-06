UPDATE alternate.queue_messages
SET lease_expires_at = clock_timestamp() + ($4::bigint * interval '1 microsecond')
WHERE queue_key = $1
  AND id = $2
  AND state = 'inflight'
  AND lease_token = $3
