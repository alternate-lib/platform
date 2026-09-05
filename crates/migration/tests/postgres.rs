#![cfg(feature = "postgres")]

use std::time::Duration;

use alternate_migration::{
    AsyncMigrationBackend, Migration, MigrationError, MigrationRunner, postgres::PostgresBackend,
};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt as _, core::IntoContainerPort, runners::AsyncRunner,
};
use tokio::time;
use tokio_postgres::{Client, NoTls};

const POSTGRES_PORT: u16 = 5432;
const METADATA_TABLE: &str = "_alternate_migrations";

struct PostgresServer {
    _container: ContainerAsync<GenericImage>,
    host: String,
    port: u16,
}

impl PostgresServer {
    async fn init() -> Result<Self, Box<dyn std::error::Error>> {
        let container = GenericImage::new("postgres", "17-alpine")
            .with_exposed_port(POSTGRES_PORT.tcp())
            .with_env_var("POSTGRES_USER", "migration-test")
            .with_env_var("POSTGRES_PASSWORD", "migration-test")
            .with_env_var("POSTGRES_DB", "migration-test")
            .start()
            .await?;

        let host = container
            .get_host()
            .await?
            .to_string()
            .replace("localhost", "127.0.0.1");
        let port = container.get_host_port_ipv4(POSTGRES_PORT).await?;

        Ok(PostgresServer {
            _container: container,
            host,
            port,
        })
    }

    fn connection_string(&self) -> String {
        format!(
            "postgres://migration-test:migration-test@{}:{}/migration-test",
            self.host, self.port
        )
    }

    async fn connect(&self) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) =
            tokio_postgres::connect(&self.connection_string(), NoTls).await?;

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres connection error: {error}");
            }
        });

        Ok(client)
    }

    async fn connect_ready(&self) -> Client {
        let mut last_error = String::new();

        for _ in 0..30 {
            match self.connect().await {
                Ok(client) => return client,
                Err(error) => last_error = error.to_string(),
            }

            time::sleep(Duration::from_millis(500)).await;
        }

        panic!("Postgres did not become ready ({last_error})");
    }
}

fn migration(version: u16, name: &str, sql: &str) -> Migration {
    Migration::try_new(&format!("{version:04}_{name}.sql"), sql.to_owned()).unwrap()
}

fn checksum(sql: &str) -> String {
    blake3::hash(sql.as_bytes()).to_hex().to_string()
}

async fn table_exists(client: &Client, table_name: &str) -> Result<bool, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables WHERE table_name = $1
            )",
            &[&table_name],
        )
        .await?;

    Ok(row.get(0))
}

#[tokio::test]
async fn creates_metadata_table_idempotently() -> Result<(), Box<dyn std::error::Error>> {
    let server = PostgresServer::init().await?;
    let mut backend = PostgresBackend::new(server.connect_ready().await);
    let probe = server.connect().await?;

    backend.ensure_metadata_table().await?;
    backend.ensure_metadata_table().await?;

    assert!(table_exists(&probe, METADATA_TABLE).await?);

    let columns = probe
        .query(
            "SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position",
            &[&METADATA_TABLE],
        )
        .await?
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();

    assert_eq!(
        columns,
        [
            ("version".to_owned(), "bigint".to_owned()),
            ("name".to_owned(), "text".to_owned()),
            ("checksum".to_owned(), "text".to_owned()),
            (
                "applied_at".to_owned(),
                "timestamp with time zone".to_owned()
            ),
        ]
    );

    assert_eq!(
        backend.load_applied().await?,
        [] as [alternate_migration::AppliedMigration; 0]
    );

    Ok(())
}

#[tokio::test]
async fn records_and_loads_migrations_in_version_order() -> Result<(), Box<dyn std::error::Error>> {
    let server = PostgresServer::init().await?;
    let mut backend = PostgresBackend::new(server.connect_ready().await);

    backend.ensure_metadata_table().await?;

    let second = migration(2, "second", "SELECT 2;");
    let first = migration(1, "first", "SELECT 1;");

    backend.record(&second).await?;
    backend.record(&first).await?;

    let applied = backend.load_applied().await?;

    assert_eq!(
        applied.iter().map(|m| m.version).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        applied.iter().map(|m| m.name.as_str()).collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert_eq!(applied[0].checksum, checksum("SELECT 1;"));
    assert_eq!(applied[1].checksum, checksum("SELECT 2;"));

    Ok(())
}

#[tokio::test]
async fn applies_sql_and_records_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let server = PostgresServer::init().await?;
    let mut backend = PostgresBackend::new(server.connect_ready().await);
    let probe = server.connect().await?;

    backend.ensure_metadata_table().await?;

    let sql = "CREATE TABLE probe_data (id BIGINT); INSERT INTO probe_data (id) VALUES (42);";
    let migration = migration(1, "create_probe", sql);
    backend.apply(&migration).await?;

    let row = probe
        .query_one("SELECT count(*) FROM probe_data", &[])
        .await?;
    let count: i64 = row.get(0);
    assert_eq!(count, 1);

    let applied = backend.load_applied().await?;

    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].version, 1);
    assert_eq!(applied[0].name, "create_probe");
    assert_eq!(applied[0].checksum, checksum(sql));

    Ok(())
}

#[tokio::test]
async fn rolls_back_failed_migration() -> Result<(), Box<dyn std::error::Error>> {
    let server = PostgresServer::init().await?;
    let mut backend = PostgresBackend::new(server.connect_ready().await);
    let probe = server.connect().await?;

    backend.ensure_metadata_table().await?;

    let migration = migration(
        1,
        "broken",
        "CREATE TABLE rollback_probe (id BIGINT);
         SELECT * FROM definitely_missing_table;",
    );

    let result = backend.apply(&migration).await;

    assert!(matches!(result, Err(MigrationError::Backend(_))));
    assert!(!table_exists(&probe, "rollback_probe").await?);
    assert_eq!(
        backend.load_applied().await?,
        [] as [alternate_migration::AppliedMigration; 0]
    );

    Ok(())
}

#[tokio::test]
async fn uses_custom_metadata_table() -> Result<(), Box<dyn std::error::Error>> {
    let server = PostgresServer::init().await?;
    let mut backend = PostgresBackend::new(server.connect_ready().await).with_table_name("_custom");
    let probe = server.connect().await?;

    backend.ensure_metadata_table().await?;
    backend.record(&migration(1, "first", "SELECT 1;")).await?;

    let row = probe.query_one("SELECT count(*) FROM _custom", &[]).await?;
    let count: i64 = row.get(0);
    assert_eq!(count, 1);

    assert!(!table_exists(&probe, METADATA_TABLE).await?);

    Ok(())
}

#[tokio::test]
async fn runner_applies_pending_migrations_and_is_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let server = PostgresServer::init().await?;
    let mut runner = MigrationRunner::new(PostgresBackend::new(server.connect_ready().await));
    let probe = server.connect().await?;

    let migrations = vec![
        migration(2, "second", "CREATE TABLE second (id BIGINT);"),
        migration(1, "first", "CREATE TABLE first (id BIGINT);"),
    ];

    runner.migrate_to_latest(migrations.clone()).await?;
    runner.migrate_to_latest(migrations).await?;

    assert!(table_exists(&probe, "first").await?);
    assert!(table_exists(&probe, "second").await?);

    let rows = probe
        .query(
            "SELECT version FROM _alternate_migrations ORDER BY version",
            &[],
        )
        .await?;
    let versions = rows
        .iter()
        .map(|row| row.get::<_, i64>(0).cast_unsigned())
        .collect::<Vec<_>>();
    assert_eq!(versions, [1, 2]);

    Ok(())
}

#[tokio::test]
async fn runner_sync_records_without_applying_sql() -> Result<(), Box<dyn std::error::Error>> {
    let server = PostgresServer::init().await?;
    let mut runner = MigrationRunner::new(PostgresBackend::new(server.connect_ready().await));
    let probe = server.connect().await?;

    runner
        .record_to_latest(vec![migration(
            1,
            "create_untouched",
            "CREATE TABLE untouched (id BIGINT);",
        )])
        .await?;

    assert!(!table_exists(&probe, "untouched").await?);

    let rows = probe
        .query("SELECT version, name FROM _alternate_migrations", &[])
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "create_untouched");

    Ok(())
}
