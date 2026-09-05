use anyhow::Context as _;
use rusqlite::{Connection, Row, params};

use crate::{AppliedMigration, Migration, MigrationError, SyncMigrationBackend};

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
    fn ensure_metadata_table(&mut self) -> Result<(), MigrationError> {
        let conn = &self.conn;

        let create_sql = format!(
            r"CREATE TABLE IF NOT EXISTS {table_name} (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )",
            table_name = self.table_name,
        );

        conn.execute_batch(&create_sql)
            .context("create migrations table")?;

        Ok(())
    }

    fn load_applied(&mut self) -> Result<Vec<AppliedMigration>, MigrationError> {
        let conn = &self.conn;

        let select_sql = format!(
            r"SELECT version, name, checksum, applied_at
                FROM {table_name}
                ORDER BY version",
            table_name = self.table_name,
        );

        let mut stmt = conn
            .prepare(&select_sql)
            .context("select applied migrations")?;
        let migrations = stmt
            .query_map([], |r| Ok(AppliedMigration::from(r)))
            .context("query applied migrations")?
            .filter_map(Result::ok)
            .collect();

        Ok(migrations)
    }

    fn apply(&mut self, migration: &Migration) -> Result<(), MigrationError> {
        let tx = self.conn.transaction().context("begin transaction")?;

        tx.execute_batch(&migration.sql)
            .with_context(|| format!("apply migration: {}", migration.version()))?;

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
        )
        .with_context(|| format!("record migration: {}", migration.version()))?;

        tx.commit().context("commit transaction")?;

        Ok(())
    }

    fn record(&mut self, migration: &Migration) -> Result<(), MigrationError> {
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
        )
        .with_context(|| format!("record migration: {}", migration.version()))?;

        Ok(())
    }
}

impl<'a> From<&'a Row<'a>> for AppliedMigration {
    fn from(row: &'a Row<'a>) -> Self {
        Self {
            version: row.get::<_, i64>("version").unwrap().cast_unsigned(),
            name: row.get("name").unwrap(),
            checksum: row.get("checksum").unwrap(),
            applied_at: row.get("applied_at").unwrap(),
        }
    }
}
