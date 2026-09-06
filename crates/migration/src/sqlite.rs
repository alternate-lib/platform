use rusqlite::{Connection, Row, params};

use crate::{AppliedMigration, Migration, SyncMigrationBackend};

const DEFAULT_TABLE_NAME: &str = "_alternate_migrations";

pub struct SqliteMigrationBackend {
    conn: Connection,
    table_name: String,
}

impl SqliteMigrationBackend {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            table_name: DEFAULT_TABLE_NAME.to_owned(),
        }
    }

    #[must_use]
    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }
}

impl SyncMigrationBackend for SqliteMigrationBackend {
    type Error = SqliteBackendError;

    fn ensure_metadata_table(&mut self) -> Result<(), Self::Error> {
        let conn = &self.conn;

        let create_sql = format!(
            r"CREATE TABLE IF NOT EXISTS {table_name} (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            )",
            table_name = self.table_name,
        );

        conn.execute_batch(&create_sql)?;

        Ok(())
    }

    fn load_applied(&mut self) -> Result<Vec<AppliedMigration>, Self::Error> {
        let conn = &self.conn;

        let select_sql = format!(
            r"SELECT version, name, checksum, applied_at
                FROM {table_name}
                ORDER BY version",
            table_name = self.table_name,
        );

        let mut stmt = conn.prepare(&select_sql)?;
        let migrations = stmt
            .query_map([], applied_migration_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(migrations)
    }

    fn apply(&mut self, migration: &Migration) -> Result<(), Self::Error> {
        let tx = self.conn.transaction()?;

        tx.execute_batch(&migration.sql)?;

        let insert_sql = format!(
            r"INSERT INTO {table_name} (version, name, checksum)
                VALUES (?1, ?2, ?3)",
            table_name = self.table_name,
        );

        tx.execute(
            &insert_sql,
            params![
                migration.version().cast_signed(),
                migration.name(),
                migration.checksum
            ],
        )?;

        tx.commit()?;

        Ok(())
    }

    fn record(&mut self, migration: &Migration) -> Result<(), Self::Error> {
        let conn = &self.conn;

        let insert_sql = format!(
            r"INSERT INTO {table_name} (version, name, checksum)
                VALUES (?1, ?2, ?3)",
            table_name = self.table_name,
        );

        conn.execute(
            &insert_sql,
            params![
                migration.version().cast_signed(),
                migration.name(),
                migration.checksum
            ],
        )?;

        Ok(())
    }
}

fn applied_migration_from_row(row: &Row<'_>) -> rusqlite::Result<AppliedMigration> {
    Ok(AppliedMigration {
        version: row.get::<_, i64>("version")?.cast_unsigned(),
        name: row.get("name")?,
        checksum: row.get("checksum")?,
        applied_at: row.get("applied_at")?,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum SqliteBackendError {
    #[error(transparent)]
    Client(#[from] rusqlite::Error),
}
