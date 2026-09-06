use std::{collections::BTreeMap, sync::Arc, time::Duration};

use deadpool_postgres::Pool;
use tokio::{sync::Notify, time};
use tokio_postgres::{AsyncMessage, NoTls};
use uuid::Uuid;

use super::PostgresQueueError;
use crate::{
    QueueConsumer, QueueDelivery, QueueError, QueueLeaseReclaim, QueueLeaseRenewal, QueueRejection,
    QueueScheduledPromotion, RejectAction, RejectOutcome,
};

#[derive(Debug)]
pub struct PostgresConsumer {
    pool: Pool,
    config: PostgresConsumerConfig,
    ready: Arc<Notify>,
    scripts: PostgresScripts,
}

impl PostgresConsumer {
    const MAX_IDLE_TIMEOUT: Duration = Duration::from_hours(1);

    const DEFAULT_BACKOFF: Duration = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    pub fn create(
        pool: Pool,
        conn_info: impl Into<String>,
        config: PostgresConsumerConfig,
    ) -> Self {
        let consumer = Self {
            pool,
            config,
            ready: Arc::new(Notify::new()),
            scripts: PostgresScripts::new(),
        };

        tokio::spawn(Self::listen_loop(
            conn_info.into(),
            consumer.config.queue_key.clone(),
            consumer.ready.clone(),
        ));

        consumer
    }

    async fn listen_loop(conn_info: String, queue_key: String, ready: Arc<Notify>) {
        let mut backoff = Self::DEFAULT_BACKOFF;

        loop {
            let Ok((listener, mut conn)) = tokio_postgres::connect(&conn_info, NoTls).await else {
                tracing::warn!("postgres listener connection failed; retrying");

                time::sleep(backoff).await;
                backoff = (backoff * 2).min(Self::MAX_BACKOFF);

                continue;
            };

            if let Err(error) = listener.batch_execute("LISTEN alternate_queue").await {
                tracing::warn!(error = %error, "postgres LISTEN failed; retrying");

                time::sleep(backoff).await;
                backoff = (backoff * 2).min(Self::MAX_BACKOFF);

                continue;
            }

            backoff = Duration::from_secs(1);

            loop {
                match std::future::poll_fn(|cx| conn.poll_message(cx)).await {
                    Some(Ok(AsyncMessage::Notification(notif))) if notif.payload() == queue_key => {
                        ready.notify_waiters();
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "postgres listener connection failed; reconnecting");

                        break;
                    }
                    None => {
                        tracing::warn!("postgres listener connection closed; reconnecting");

                        break;
                    }
                }
            }

            time::sleep(backoff).await;
            backoff = (backoff * 2).min(Self::MAX_BACKOFF);
        }
    }

    async fn try_claim(
        &self,
    ) -> Result<Option<QueueDelivery<PostgresReceipt>>, PostgresQueueError> {
        let client = self.pool.get().await?;

        let token = Uuid::now_v7().to_string();

        let stmt = client.prepare_cached(self.scripts.claim).await?;
        let rows = client
            .query(
                &stmt,
                &[
                    &self.config.queue_key,
                    &self.config.worker_id,
                    &token,
                    &self.config.visibility_micros,
                ],
            )
            .await?;

        rows.into_iter()
            .next()
            .map(|row| {
                let id = row.get::<_, String>(0);
                let attributes = serde_json::from_value::<BTreeMap<String, String>>(row.get(3))?;

                Ok(QueueDelivery {
                    id: id.clone(),
                    payload: row.get(1),
                    attempts: attempts(row.get(2))?,
                    attributes,
                    receipt: PostgresReceipt {
                        message_id: id,
                        lease_token: token,
                    },
                })
            })
            .transpose()
    }
}

impl QueueConsumer for PostgresConsumer {
    type Error = PostgresQueueError;
    type Receipt = PostgresReceipt;

    async fn receive(&self) -> Result<Option<QueueDelivery<Self::Receipt>>, Self::Error> {
        if let Some(delivery) = self.try_claim().await? {
            return Ok(Some(delivery));
        }

        tokio::select! {
            () = self.ready.notified() => {}
            () = time::sleep(self.config.idle_timeout) => {}
        }

        self.try_claim().await
    }

    async fn ack(&self, receipt: Self::Receipt) -> Result<(), Self::Error> {
        let client = self.pool.get().await?;

        let stmt = client.prepare_cached(self.scripts.ack).await?;
        let deleted = client
            .execute(
                &stmt,
                &[
                    &self.config.queue_key,
                    &receipt.message_id,
                    &receipt.lease_token,
                ],
            )
            .await?;

        if deleted == 0 {
            return Err(QueueError::StaleReceipt.into());
        }

        Ok(())
    }
}

impl QueueLeaseReclaim for PostgresConsumer {
    async fn reclaim(&self) -> Result<usize, Self::Error> {
        let client = self.pool.get().await?;

        let stmt = client.prepare_cached(self.scripts.reclaim).await?;
        let rows = client
            .query(&stmt, &[&self.config.queue_key, &self.config.reclaim_batch])
            .await?;

        Ok(rows.len())
    }
}

impl QueueLeaseRenewal for PostgresConsumer {
    async fn renew(
        &self,
        receipt: &Self::Receipt,
        visibility: Duration,
    ) -> Result<(), Self::Error> {
        let client = self.pool.get().await?;

        let stmt = client.prepare_cached(self.scripts.renew).await?;
        let updated = client
            .execute(
                &stmt,
                &[
                    &self.config.queue_key,
                    &receipt.message_id,
                    &receipt.lease_token,
                    &duration_micros(visibility)?,
                ],
            )
            .await?;

        if updated == 0 {
            return Err(QueueError::StaleReceipt.into());
        }

        Ok(())
    }
}

impl QueueRejection for PostgresConsumer {
    async fn reject(
        &self,
        receipt: Self::Receipt,
        action: RejectAction,
    ) -> Result<RejectOutcome, Self::Error> {
        let client = self.pool.get().await?;

        match action {
            RejectAction::Retry {
                after,
                max_attempts,
            } => {
                let after_micros = after.map(duration_micros).transpose()?;
                let max_attempts = max_attempts
                    .map(i64::try_from)
                    .transpose()
                    .map_err(|_| PostgresQueueError::InvalidAttemptLimit)?;

                let stmt = client.prepare_cached(self.scripts.reject).await?;
                let row = client
                    .query_opt(
                        &stmt,
                        &[
                            &self.config.queue_key,
                            &receipt.message_id,
                            &max_attempts,
                            &after_micros,
                            &receipt.lease_token,
                        ],
                    )
                    .await?
                    .ok_or(QueueError::StaleReceipt)?;

                Ok(RejectOutcome {
                    attempts: attempts(row.get(0))?,
                    exhausted: row.get(1),
                })
            }
            RejectAction::DeadLetter => {
                let stmt = client.prepare_cached(self.scripts.dead_letter).await?;
                let row = client
                    .query_opt(
                        &stmt,
                        &[
                            &self.config.queue_key,
                            &receipt.message_id,
                            &receipt.lease_token,
                        ],
                    )
                    .await?
                    .ok_or(QueueError::StaleReceipt)?;

                Ok(RejectOutcome {
                    attempts: attempts(row.get(0))?,
                    exhausted: false,
                })
            }
        }
    }
}

impl QueueScheduledPromotion for PostgresConsumer {
    async fn promote(&self) -> Result<usize, Self::Error> {
        let client = self.pool.get().await?;

        let stmt = client.prepare_cached(self.scripts.promote).await?;
        let rows = client
            .query(&stmt, &[&self.config.queue_key, &self.config.promote_batch])
            .await?;

        Ok(rows.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresReceipt {
    pub message_id: String,
    pub lease_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresConsumerConfig {
    queue_key: String,
    worker_id: String,
    visibility_micros: i64,
    reclaim_batch: i64,
    promote_batch: i64,
    idle_timeout: Duration,
}

impl PostgresConsumerConfig {
    pub fn builder() -> PostgresConsumerConfigBuilder {
        PostgresConsumerConfigBuilder::default()
    }
}

#[derive(Debug, Clone)]
pub struct PostgresConsumerConfigBuilder {
    queue_key: Option<String>,
    worker_id: Option<String>,
    visibility: Option<Duration>,
    reclaim_batch: i64,
    promote_batch: i64,
    idle_timeout: Duration,
}

impl Default for PostgresConsumerConfigBuilder {
    fn default() -> Self {
        Self {
            queue_key: None,
            worker_id: None,
            visibility: None,
            reclaim_batch: 100,
            promote_batch: 100,
            idle_timeout: Duration::from_secs(30),
        }
    }
}

impl PostgresConsumerConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn queue_key(mut self, queue_key: impl Into<String>) -> Self {
        self.queue_key = Some(queue_key.into());
        self
    }

    #[must_use]
    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    #[must_use]
    pub fn visibility(mut self, visibility: Duration) -> Self {
        self.visibility = Some(visibility);
        self
    }

    #[must_use]
    pub fn reclaim_batch(mut self, reclaim_batch: i64) -> Self {
        self.reclaim_batch = reclaim_batch;
        self
    }

    #[must_use]
    pub fn promote_batch(mut self, promote_batch: i64) -> Self {
        self.promote_batch = promote_batch;
        self
    }

    #[must_use]
    pub fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    pub fn build(self) -> Result<PostgresConsumerConfig, PostgresConsumerConfigError> {
        let queue_key = self
            .queue_key
            .ok_or(PostgresConsumerConfigError::MissingQueueKey)?;
        let visibility = self
            .visibility
            .ok_or(PostgresConsumerConfigError::MissingVisibility)?;

        let Ok(visibility_micros) = i64::try_from(visibility.as_micros()) else {
            return Err(PostgresConsumerConfigError::InvalidVisibilityTimeout);
        };
        if visibility_micros == 0 {
            return Err(PostgresConsumerConfigError::InvalidVisibilityTimeout);
        }
        if self.reclaim_batch <= 0 {
            return Err(PostgresConsumerConfigError::InvalidReclaimBatch);
        }
        if self.promote_batch <= 0 {
            return Err(PostgresConsumerConfigError::InvalidPromoteBatch);
        }
        if self.idle_timeout.is_zero() || self.idle_timeout > PostgresConsumer::MAX_IDLE_TIMEOUT {
            return Err(PostgresConsumerConfigError::InvalidIdleTimeout);
        }

        Ok(PostgresConsumerConfig {
            queue_key,
            worker_id: self.worker_id.unwrap_or_else(|| Uuid::now_v7().to_string()),
            visibility_micros,
            reclaim_batch: self.reclaim_batch,
            promote_batch: self.promote_batch,
            idle_timeout: self.idle_timeout,
        })
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PostgresConsumerConfigError {
    #[error("queue key not configured")]
    MissingQueueKey,

    #[error("visibility not configured")]
    MissingVisibility,

    #[error("visibility timeout must not overflow")]
    InvalidVisibilityTimeout,

    #[error("reclaim batch count must be greater than 0")]
    InvalidReclaimBatch,

    #[error("promote batch count must be greater than 0")]
    InvalidPromoteBatch,

    #[error("idle timeout must be >0 seconds and <60 minutes")]
    InvalidIdleTimeout,
}

#[derive(Clone, Debug)]
struct PostgresScripts {
    ack: &'static str,
    claim: &'static str,
    dead_letter: &'static str,
    promote: &'static str,
    reclaim: &'static str,
    reject: &'static str,
    renew: &'static str,
}

impl PostgresScripts {
    fn new() -> Self {
        Self {
            ack: include_str!("../../sql/postgres/ack.sql"),
            claim: include_str!("../../sql/postgres/claim.sql"),
            dead_letter: include_str!("../../sql/postgres/dead_letter.sql"),
            promote: include_str!("../../sql/postgres/promote.sql"),
            reclaim: include_str!("../../sql/postgres/reclaim.sql"),
            reject: include_str!("../../sql/postgres/reject.sql"),
            renew: include_str!("../../sql/postgres/renew.sql"),
        }
    }
}

fn attempts(value: i32) -> Result<usize, PostgresQueueError> {
    usize::try_from(value).map_err(|_| PostgresQueueError::InvalidAttemptLimit)
}

fn duration_micros(value: Duration) -> Result<i64, PostgresQueueError> {
    i64::try_from(value.as_micros()).map_err(|_| PostgresQueueError::InvalidDuration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_builder_validates_operational_settings() {
        assert!(matches!(
            PostgresConsumerConfig::builder()
                .queue_key("queue")
                .visibility(Duration::ZERO)
                .build(),
            Err(PostgresConsumerConfigError::InvalidVisibilityTimeout)
        ));
        assert!(matches!(
            PostgresConsumerConfig::builder()
                .queue_key("queue")
                .visibility(Duration::from_secs(1))
                .reclaim_batch(0)
                .build(),
            Err(PostgresConsumerConfigError::InvalidReclaimBatch)
        ));
        assert!(matches!(
            PostgresConsumerConfig::builder()
                .queue_key("queue")
                .visibility(Duration::from_secs(1))
                .promote_batch(0)
                .build(),
            Err(PostgresConsumerConfigError::InvalidPromoteBatch)
        ));
    }

    #[test]
    fn builders_require_queue_keys() {
        assert_eq!(
            PostgresConsumerConfig::builder().build(),
            Err(PostgresConsumerConfigError::MissingQueueKey)
        );
    }
}
