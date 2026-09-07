use std::{fmt::Debug, time::Duration};

use alternate_migration::Migration;
use deadpool_sqlite::{InteractError, Pool, PoolError, rusqlite};
use tokio::{task::JoinHandle, time};

use crate::{KvClient, KvExpiry};

#[derive(Debug, Clone)]
pub struct SqliteClient {
    pool: Pool,
}

impl SqliteClient {
    const NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

    const EXPIRY_FILTER: &str =
        "expires_at IS NULL OR expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

    const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    pub async fn sweep_expired(&self) -> Result<u64, SqliteClientError> {
        let client = self.pool.get().await?;

        let deleted = client
            .interact(move |conn| {
                Self::configure_connection(conn)?;

                let sql = format!(
                    "DELETE FROM kv_entries
                     WHERE expires_at IS NOT NULL AND expires_at <= ({})",
                    Self::NOW,
                );

                let mut stmt = conn.prepare_cached(&sql)?;
                let deleted = stmt.execute([])?;

                u64::try_from(deleted).map_err(|_| SqliteClientError::RowCount)
            })
            .await??;

        Ok(deleted)
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

    fn configure_connection(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
        conn.busy_timeout(Self::BUSY_TIMEOUT)
    }
}

impl KvClient for SqliteClient {
    type Error = SqliteClientError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        use rusqlite::OptionalExtension as _;

        let client = self.pool.get().await?;
        let key = key.to_owned();

        let value = client
            .interact(move |conn| {
                Self::configure_connection(conn)?;

                let sql = format!(
                    "SELECT value FROM kv_entries WHERE key = ?1 AND ({})",
                    Self::EXPIRY_FILTER,
                );

                let mut stmt = conn.prepare_cached(&sql)?;
                stmt.query_row([&key], |row| row.get(0)).optional()
            })
            .await??;

        Ok(value)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn exists(&self, key: &str) -> Result<bool, Self::Error> {
        let client = self.pool.get().await?;
        let key = key.to_owned();

        let exists = client
            .interact(move |conn| {
                Self::configure_connection(conn)?;

                let sql = format!(
                    "SELECT EXISTS (
                        SELECT 1 FROM kv_entries
                        WHERE key = ?1 AND ({})
                    )",
                    Self::EXPIRY_FILTER,
                );

                let mut stmt = conn.prepare_cached(&sql)?;
                stmt.query_row([&key], |row| row.get(0))
            })
            .await??;

        Ok(exists)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self, value), err(Debug))
    )]
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), Self::Error> {
        let client = self.pool.get().await?;
        let key = key.to_owned();
        let value = value.to_owned();

        client
            .interact(move |conn| {
                Self::configure_connection(conn)?;

                let mut stmt = conn.prepare_cached(
                    "INSERT INTO kv_entries (key, value, expires_at)
                    VALUES (?1, ?2, NULL)
                    ON CONFLICT (key) DO UPDATE
                    SET value = excluded.value, expires_at = NULL",
                )?;

                stmt.execute(rusqlite::params![key, value])?;

                Ok::<(), rusqlite::Error>(())
            })
            .await??;

        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn delete(&self, key: &str) -> Result<(), Self::Error> {
        let client = self.pool.get().await?;
        let key = key.to_owned();

        client
            .interact(move |conn| {
                Self::configure_connection(conn)?;

                let mut stmt = conn.prepare_cached("DELETE FROM kv_entries WHERE key = ?1")?;

                stmt.execute([&key])?;

                Ok::<(), rusqlite::Error>(())
            })
            .await??;

        Ok(())
    }
}

impl KvExpiry for SqliteClient {
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
        let ttl_millis = u64::try_from(ttl.as_millis()).map_err(|_| SqliteClientError::Ttl)?;
        let ttl_millis = i64::try_from(ttl_millis).map_err(|_| SqliteClientError::Ttl)?;

        let client = self.pool.get().await?;
        let key = key.to_owned();
        let value = value.to_owned();

        client
            .interact(move |conn| {
                Self::configure_connection(conn)?;

                let mut stmt = conn.prepare_cached(
                    "INSERT INTO kv_entries (key, value, expires_at)
                    VALUES (?1, ?2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || (?3 / 1000.0) || ' seconds'))
                    ON CONFLICT (key) DO UPDATE
                    SET value = excluded.value, expires_at = excluded.expires_at",
                )?;

                stmt.execute(rusqlite::params![key, value, ttl_millis])?;

                Ok::<(), rusqlite::Error>(())
            })
            .await??;

        Ok(())
    }
}

#[derive(rust_embed::Embed)]
#[folder = "migrations/sqlite"]
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

impl From<InteractError> for SqliteClientError {
    fn from(error: InteractError) -> Self {
        Self::Interact(error.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SqliteClientError {
    #[error(transparent)]
    Pool(#[from] PoolError),

    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error("interact with pooled SQLite connection failed: {0}")]
    Interact(String),

    #[error("TTL is out of range")]
    Ttl,

    #[error("row count does not fit into u64")]
    RowCount,
}
