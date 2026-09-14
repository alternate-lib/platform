INSERT INTO alternate.queue_messages (
  id,
  queue_key,
  payload,
  state
)
VALUES (
  $1,
  $2,
  $3,
  'ready'
)
