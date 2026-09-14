use deadpool_postgres::Pool;
use jiff::Timestamp;
use tokio_postgres::error::SqlState;
use uuid::Uuid;

use super::PostgresQueueError;
use crate::{QueueError, QueueMessage, QueueProducer, QueueScheduled};

#[derive(Debug, Clone)]
pub struct PostgresProducer {
    pool: Pool,
    config: PostgresProducerConfig,
    scripts: PostgresScripts,
}

impl PostgresProducer {
    pub fn new(pool: Pool, config: PostgresProducerConfig) -> Self {
        Self {
            pool,
            config,
            scripts: PostgresScripts::new(),
        }
    }
}

impl QueueProducer for PostgresProducer {
    type Error = PostgresQueueError;
    type MessageId = String;

    async fn send(&self, message: QueueMessage) -> Result<Self::MessageId, Self::Error> {
        let client = self.pool.get().await?;

        let id = message.id.unwrap_or_else(|| Uuid::now_v7().to_string());

        let stmt = client.prepare_cached(self.scripts.send).await?;
        client
            .execute(&stmt, &[&id, &self.config.queue_key, &message.payload])
            .await
            .map_err(map_insert_error)?;

        Ok(id)
    }
}

impl QueueScheduled for PostgresProducer {
    async fn schedule(
        &self,
        message: QueueMessage,
        run_at: Timestamp,
    ) -> Result<Self::MessageId, Self::Error> {
        let client = self.pool.get().await?;

        let id = message.id.unwrap_or_else(|| Uuid::now_v7().to_string());

        let stmt = client.prepare_cached(self.scripts.schedule).await?;
        client
            .execute(
                &stmt,
                &[&id, &self.config.queue_key, &message.payload, &run_at],
            )
            .await
            .map_err(map_insert_error)?;

        Ok(id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresProducerConfig {
    queue_key: String,
}

impl PostgresProducerConfig {
    pub fn builder() -> PostgresProducerConfigBuilder {
        PostgresProducerConfigBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostgresProducerConfigBuilder {
    queue_key: Option<String>,
}

impl PostgresProducerConfigBuilder {
    #[must_use]
    pub fn queue_key(mut self, queue_key: impl Into<String>) -> Self {
        self.queue_key = Some(queue_key.into());
        self
    }

    pub fn build(self) -> Result<PostgresProducerConfig, PostgresProducerConfigError> {
        Ok(PostgresProducerConfig {
            queue_key: self
                .queue_key
                .ok_or(PostgresProducerConfigError::MissingQueueKey)?,
        })
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PostgresProducerConfigError {
    #[error("queue key not configured")]
    MissingQueueKey,
}

#[derive(Clone, Debug)]
struct PostgresScripts {
    schedule: &'static str,
    send: &'static str,
}

impl PostgresScripts {
    fn new() -> Self {
        Self {
            schedule: include_str!("../../sql/postgres/schedule.sql"),
            send: include_str!("../../sql/postgres/send.sql"),
        }
    }
}

fn map_insert_error(error: tokio_postgres::Error) -> PostgresQueueError {
    if error
        .code()
        .is_some_and(|code| code == &SqlState::UNIQUE_VIOLATION)
    {
        PostgresQueueError::Queue(QueueError::DuplicateMessageId)
    } else {
        PostgresQueueError::Postgres(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_requires_queue_keys() {
        assert_eq!(
            PostgresProducerConfig::builder().build(),
            Err(PostgresProducerConfigError::MissingQueueKey)
        );
    }
}
