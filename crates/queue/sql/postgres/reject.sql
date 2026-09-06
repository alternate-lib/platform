UPDATE alternate.queue_messages
SET attempts = attempts + 1,
  state = CASE
    WHEN
      $3::bigint IS NOT NULL
      AND attempts + 1 > $3::bigint
      THEN 'dead'
    WHEN $4::bigint IS NOT NULL
      THEN 'scheduled'
    ELSE 'ready'
  END,
  run_at = CASE
      WHEN $4::bigint IS NOT NULL
        THEN clock_timestamp() + ($4::bigint * interval '1 microsecond')
      ELSE clock_timestamp()
  END,
  leased_by = NULL,
  leased_at = NULL,
  lease_token = NULL,
  lease_expires_at = NULL
WHERE queue_key = $1
  AND id = $2
  AND state = 'inflight'
  AND lease_token = $5
RETURNING attempts, 
  state = 'dead'
