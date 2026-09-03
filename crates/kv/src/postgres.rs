use std::{fmt::Debug, time::Duration};

use deadpool_postgres::{Pool, PoolError};

use crate::{KvClient, KvClientExpiry};

#[derive(Debug, Clone)]
pub struct PostgresClient {
    pool: Pool,
}

impl PostgresClient {
    const EXPIRY_FILTER: &'static str = "expires_at IS NULL OR expires_at > now()";

    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl KvClient for PostgresClient {
    type Error = PostgresClientError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let client = self.pool.get().await?;

        let stmt = client
            .prepare_cached(&format!(
                "SELECT value FROM alternate.kv_entries \
                 WHERE key = $1 AND ({})",
                Self::EXPIRY_FILTER,
            ))
            .await?;

        let row = client.query_opt(&stmt, &[&key]).await?;
        Ok(row.map(|r| r.get(0)))
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn exists(&self, key: &str) -> Result<bool, Self::Error> {
        let client = self.pool.get().await?;

        let stmt = client
            .prepare_cached(&format!(
                r"SELECT EXISTS (
                    SELECT 1 FROM alternate.kv_entries
                    WHERE key = $1 AND ({})
                )",
                Self::EXPIRY_FILTER,
            ))
            .await?;

        let exists: bool = client.query_one(&stmt, &[&key]).await?.get(0);
        Ok(exists)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self, value), err(Debug))
    )]
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), Self::Error> {
        let client = self.pool.get().await?;

        let stmt = client
            .prepare_cached(
                r"INSERT INTO alternate.kv_entries (key, value, expires_at)
                VALUES ($1, $2, NULL)
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, expires_at = NULL",
            )
            .await?;

        client.execute(&stmt, &[&key, &value]).await?;
        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn delete(&self, key: &str) -> Result<(), Self::Error> {
        let client = self.pool.get().await?;

        let stmt = client
            .prepare_cached("DELETE FROM alternate.kv_entries WHERE key = $1")
            .await?;

        client.execute(&stmt, &[&key]).await?;

        Ok(())
    }
}

impl KvClientExpiry for PostgresClient {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self, value), err(Debug))
    )]
    async fn set_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> Result<(), Self::Error> {
        let client = self.pool.get().await?;

        let ttl_micros = i64::try_from(ttl.as_micros()).unwrap().to_string();

        let stmt = client
            .prepare_cached(
                r"INSERT INTO alternate.kv_entries (key, value, expires_at)
                VALUES ($1, $2, now() + ($3 || ' microseconds')::interval)
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at",
            )
            .await?;

        client.execute(&stmt, &[&key, &value, &ttl_micros]).await?;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PostgresClientError {
    #[error(transparent)]
    Pool(#[from] PoolError),

    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
}
