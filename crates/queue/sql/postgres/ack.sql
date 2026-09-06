DELETE FROM alternate.queue_messages
WHERE queue_key = $1
  AND id = $2
  AND state = 'inflight'
  AND lease_token = $3
