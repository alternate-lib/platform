INSERT INTO alternate.queue_messages (
  id,
  queue_key,
  message,
  attributes,
  state
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  'ready'
)
