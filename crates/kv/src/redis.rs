use std::{fmt::Debug, time::Duration};

use deadpool_redis::{Pool, PoolError};
use redis::{AsyncCommands, RedisError};

use crate::{KvClient, KvExpiry};

#[derive(Debug, Clone)]
pub struct RedisClient {
    pool: Pool,
}

impl RedisClient {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl KvClient for RedisClient {
    type Error = RedisClientError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let mut conn = self.pool.get().await?;

        let raw = conn.get::<_, Option<Vec<u8>>>(key).await?;

        Ok(raw)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn exists(&self, key: &str) -> Result<bool, Self::Error> {
        let mut conn = self.pool.get().await?;

        let exists = conn.exists(key).await?;

        Ok(exists)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self, value), err(Debug))
    )]
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), RedisClientError> {
        let mut conn = self.pool.get().await?;

        let () = conn.set(key, value).await?;

        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn delete(&self, key: &str) -> Result<(), RedisClientError> {
        let mut conn = self.pool.get().await?;

        let () = conn.del(key).await?;

        Ok(())
    }
}

impl KvExpiry for RedisClient {
    async fn set_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> Result<(), Self::Error> {
        let mut conn = self.pool.get().await?;

        let () = conn.set_ex(key, value, ttl.as_secs()).await?;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RedisClientError {
    #[error(transparent)]
    Pool(#[from] PoolError),

    #[error(transparent)]
    Client(#[from] RedisError),
}
