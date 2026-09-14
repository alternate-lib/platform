use super::{PostgresConsumer, PostgresQueueError, attempts};
use crate::{MessageState, MessageStatus};

impl PostgresConsumer {
    pub async fn inspect(&self, job_id: &str) -> Result<Option<MessageStatus>, PostgresQueueError> {
        let client = self.pool.get().await?;

        let statement = client.prepare_cached(self.scripts.inspect).await?;
        let row = client
            .query_opt(&statement, &[&self.config.queue_key, &job_id])
            .await?;

        row.map(|row| {
            Ok(MessageStatus {
                id: job_id.to_owned(),
                state: match row.get::<_, String>(0).as_str() {
                    "ready" => MessageState::Ready,
                    "inflight" => MessageState::Inflight,
                    "scheduled" => MessageState::Scheduled,
                    _ => MessageState::Dead,
                },
                attempts: attempts(row.get(1))?,
            })
        })
        .transpose()
    }
}
