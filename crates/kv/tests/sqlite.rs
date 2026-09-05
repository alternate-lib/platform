#![cfg(feature = "sqlite")]

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alternate_kv::{
    KvClient, KvClientExpiry,
    sqlite::{SqliteClient, SqliteClientError},
};
use alternate_migration::{SyncMigrationRunner, sqlite::SqliteMigrationBackend};
use deadpool_sqlite::{Config as SqlitePoolConfig, Runtime};
use rusqlite::Connection;
use tokio::time;

struct SqliteDatabase {
    path: String,
}

impl SqliteDatabase {
    fn init(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let path = std::env::temp_dir()
            .join(format!(
                "kv-sqlite-test-{name}-{}-{nanos}.db",
                std::process::id()
            ))
            .to_string_lossy()
            .into_owned();

        SqliteDatabase { path }
    }

    fn connect(&self) -> Connection {
        Connection::open(&self.path).expect("sqlite database opened")
    }
}

impl Drop for SqliteDatabase {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("failed to remove test database: {error}");
        }
    }
}

fn init_client(db: &SqliteDatabase) -> SqliteClient {
    apply_migrations(db);

    let config = SqlitePoolConfig::new(db.path.clone());

    let pool = config
        .create_pool(Runtime::Tokio1)
        .expect("sqlite pool created");

    SqliteClient::new(pool)
}

fn apply_migrations(db: &SqliteDatabase) {
    let mut runner = SyncMigrationRunner::new(SqliteMigrationBackend::new(db.connect()));

    runner
        .migrate_to_latest(
            alternate_kv::sqlite::migrations().expect("embedded migrations are valid"),
        )
        .expect("kv migrations applied");
}

fn unique_key(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    format!("kv-test-{name}-{}-{nanos}", std::process::id())
}

fn row_count(db: &SqliteDatabase, key: &str) -> i64 {
    let conn = db.connect();

    conn.query_row(
        "SELECT count(*) FROM kv_entries WHERE key = ?1",
        [&key],
        |row| row.get(0),
    )
    .expect("row count queried")
}

#[tokio::test]
async fn roundtrips_value() -> Result<(), Box<dyn std::error::Error>> {
    let db = SqliteDatabase::init("roundtrip");
    let client = init_client(&db);

    let key = unique_key("roundtrip");
    let payload: Vec<u8> = (0..=255u8).chain([0, 0]).collect();
    let updated = b"updated value".to_vec();

    assert!(!client.exists(&key).await?);
    assert_eq!(client.get(&key).await?, None);

    client.set(&key, &payload).await?;

    assert!(client.exists(&key).await?);
    assert_eq!(client.get(&key).await?, Some(payload));

    client.set(&key, &updated).await?;
    assert_eq!(client.get(&key).await?, Some(updated));

    client.delete(&key).await?;

    assert!(!client.exists(&key).await?);
    assert_eq!(client.get(&key).await?, None);

    client.delete(&key).await?;

    Ok(())
}

#[tokio::test]
async fn expires_value_after_ttl() -> Result<(), Box<dyn std::error::Error>> {
    let db = SqliteDatabase::init("ttl");
    let client = init_client(&db);

    let key = unique_key("ttl");
    let payload = b"ephemeral".to_vec();

    client
        .set_with_ttl(&key, &payload, Duration::from_millis(500))
        .await?;

    assert!(client.exists(&key).await?);
    assert_eq!(client.get(&key).await?, Some(payload));

    let deadline = Instant::now() + Duration::from_secs(10);

    while client.get(&key).await?.is_some() {
        assert!(Instant::now() < deadline, "key did not expire in time");

        time::sleep(Duration::from_millis(100)).await;
    }

    assert!(!client.exists(&key).await?);
    assert_eq!(client.get(&key).await?, None);

    Ok(())
}

#[tokio::test]
async fn overwriting_value_clears_ttl() -> Result<(), Box<dyn std::error::Error>> {
    let db = SqliteDatabase::init("overwrite-clears-ttl");
    let client = init_client(&db);

    let key = unique_key("overwrite-clears-ttl");
    let payload = b"ephemeral".to_vec();
    let updated = b"durable".to_vec();

    client
        .set_with_ttl(&key, &payload, Duration::from_millis(500))
        .await?;
    client.set(&key, &updated).await?;

    time::sleep(Duration::from_millis(1_000)).await;

    assert!(client.exists(&key).await?);
    assert_eq!(client.get(&key).await?, Some(updated));

    Ok(())
}

#[tokio::test]
async fn set_with_ttl_overwrites_existing_value_and_expiry()
-> Result<(), Box<dyn std::error::Error>> {
    let db = SqliteDatabase::init("overwrite-with-ttl");
    let client = init_client(&db);

    let key = unique_key("overwrite-with-ttl");
    let payload = b"durable".to_vec();
    let updated = b"ephemeral".to_vec();

    client.set(&key, &payload).await?;

    client
        .set_with_ttl(&key, &updated, Duration::from_millis(500))
        .await?;

    assert_eq!(client.get(&key).await?, Some(updated));

    let deadline = Instant::now() + Duration::from_secs(10);

    while client.get(&key).await?.is_some() {
        assert!(Instant::now() < deadline, "key did not expire in time");

        time::sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}

#[tokio::test]
async fn sweep_expired_removes_only_expired_rows() -> Result<(), Box<dyn std::error::Error>> {
    let db = SqliteDatabase::init("sweep");
    let client = init_client(&db);

    let expired_key = unique_key("sweep-expired");
    let durable_key = unique_key("sweep-durable");
    let long_ttl_key = unique_key("sweep-long-ttl");

    client
        .set_with_ttl(&expired_key, b"expired", Duration::from_millis(200))
        .await?;
    client.set(&durable_key, b"durable").await?;
    client
        .set_with_ttl(&long_ttl_key, b"long-lived", Duration::from_secs(3_600))
        .await?;

    time::sleep(Duration::from_millis(500)).await;

    let deleted = client.sweep_expired().await?;

    assert_eq!(deleted, 1);

    assert_eq!(row_count(&db, &expired_key), 0);
    assert_eq!(row_count(&db, &durable_key), 1);
    assert_eq!(row_count(&db, &long_ttl_key), 1);

    Ok(())
}

#[tokio::test]
async fn spawned_sweeper_removes_expired_rows() -> Result<(), Box<dyn std::error::Error>> {
    let db = SqliteDatabase::init("sweeper");
    let client = init_client(&db);

    let key = unique_key("sweeper");

    client
        .set_with_ttl(&key, b"ephemeral", Duration::from_millis(200))
        .await?;

    time::sleep(Duration::from_millis(500)).await;

    let sweeper = client.clone().spawn_sweeper(Duration::from_millis(100));

    let deadline = Instant::now() + Duration::from_secs(10);

    while row_count(&db, &key) > 0 {
        assert!(Instant::now() < deadline, "sweeper did not remove the row");

        time::sleep(Duration::from_millis(100)).await;
    }

    sweeper.abort();

    Ok(())
}

#[tokio::test]
async fn maps_missing_table_to_sqlite_error() -> Result<(), Box<dyn std::error::Error>> {
    let db = SqliteDatabase::init("missing-table");
    let client = init_client(&db);

    let probe = db.connect();
    probe.execute_batch("DROP TABLE kv_entries")?;

    let result = client.get("any-key").await;

    assert!(matches!(result, Err(SqliteClientError::Sqlite(_))));

    Ok(())
}

#[tokio::test]
async fn maps_unopenable_database_to_pool_error() -> Result<(), Box<dyn std::error::Error>> {
    let config = SqlitePoolConfig::new("/nonexistent-directory/kv-test.db");

    let pool = config.create_pool(Runtime::Tokio1)?;

    let client = SqliteClient::new(pool);

    let result = client.get("any-key").await;

    assert!(matches!(result, Err(SqliteClientError::Pool(_))));

    Ok(())
}
