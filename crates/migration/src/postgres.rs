use anyhow::Context as _;
use tokio_postgres::{Client, Row};

use crate::{AppliedMigration, AsyncMigrationBackend, Migration, MigrationError};

const DEFAULT_TABLE_NAME: &str = "_alternate_migrations";

pub struct PostgresBackend {
    client: Client,
    table_name: String,
}

impl PostgresBackend {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            table_name: DEFAULT_TABLE_NAME.to_owned(),
        }
    }

    #[must_use]
    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();

        self
    }
}

impl AsyncMigrationBackend for PostgresBackend {
    async fn ensure_metadata_table(&self) -> Result<(), MigrationError> {
        let create_sql = format!(
            r"CREATE TABLE IF NOT EXISTS {table_name} (
                version BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            table_name = self.table_name,
        );

        self.client
            .batch_execute(&create_sql)
            .await
            .context("create migrations table")?;

        Ok(())
    }

    async fn load_applied(&mut self) -> Result<Vec<AppliedMigration>, MigrationError> {
        let select_sql = format!(
            r"SELECT version, name, checksum, applied_at
                FROM {table_name}
                ORDER BY version",
            table_name = self.table_name,
        );

        let rows = self
            .client
            .query(&select_sql, &[])
            .await
            .context("select applied migrations")?;
        let migrations = rows.into_iter().map(Into::into).collect();

        Ok(migrations)
    }

    async fn apply(&mut self, migration: &Migration) -> Result<(), MigrationError> {
        let tx = self
            .client
            .transaction()
            .await
            .map_err(anyhow::Error::from)?;

        tx.batch_execute(&migration.sql)
            .await
            .with_context(|| format!("apply migration: {}", migration.version()))?;

        let insert_sql = format!(
            r"INSERT INTO {table_name} (version, name, checksum)
                VALUES ($1, $2, $3)",
            table_name = self.table_name,
        );

        tx.execute(
            &insert_sql,
            &[
                &migration.version().cast_signed(),
                &migration.name(),
                &migration.checksum,
            ],
        )
        .await
        .with_context(|| format!("record migration: {}", migration.version()))?;

        tx.commit().await.map_err(anyhow::Error::from)?;

        Ok(())
    }

    async fn record(&mut self, migration: &Migration) -> Result<(), MigrationError> {
        let insert_sql = format!(
            r"INSERT INTO {table_name} (version, name, checksum)
                VALUES ($1, $2, $3)",
            table_name = self.table_name,
        );

        self.client
            .execute(
                &insert_sql,
                &[
                    &migration.version().cast_signed(),
                    &migration.name(),
                    &migration.checksum,
                ],
            )
            .await
            .with_context(|| format!("record migration: {}", migration.version()))?;

        Ok(())
    }
}

impl From<Row> for AppliedMigration {
    fn from(row: Row) -> Self {
        Self {
            version: row.get::<_, i64>("version").cast_unsigned(),
            name: row.get("name"),
            checksum: row.get("checksum"),
            applied_at: row.get("applied_at"),
        }
    }
}
