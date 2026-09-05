use std::{fmt::Debug, time::Duration};

use alternate_migration::Migration;
use deadpool_postgres::{Pool, PoolError};
use tokio::{task::JoinHandle, time};

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

    pub async fn sweep_expired(&self) -> Result<u64, PostgresClientError> {
        let client = self.pool.get().await?;

        let stmt = client
            .prepare_cached("DELETE FROM alternate.kv_entries WHERE expires_at <= now()")
            .await?;

        let count = client.execute(&stmt, &[]).await?;

        Ok(count)
    }

    pub fn spawn_sweeper(self, interval: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

            loop {
                ticker.tick().await;

                let _ = self.sweep_expired().await;
            }
        })
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

#[derive(rust_embed::Embed)]
#[folder = "migrations/postgres"]
struct Migrations;

/// # Panics
///
/// Will panic if a migration file is not found
pub fn migrations() -> Result<Vec<Migration>, alternate_migration::MigrationError> {
    Migrations::iter()
        .map(|filename| {
            let migration = Migrations::get(&filename).expect("migration file not found");
            let sql = String::from_utf8_lossy(migration.data.as_ref()).to_string();

            Migration::try_new(&filename, sql)
        })
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub enum PostgresClientError {
    #[error(transparent)]
    Pool(#[from] PoolError),

    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
}
