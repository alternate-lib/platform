SELECT state,
  attempts
FROM alternate.queue_messages
WHERE queue_key = $1
  AND id = $2
