#![cfg(feature = "postgres")]

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alternate_kv::{KvClient, KvClientExpiry, postgres::PostgresClient};
use alternate_migration::{AsyncMigrationRunner, postgres::PostgresBackend};
use deadpool_postgres::{Config as PostgresPoolConfig, Pool, Runtime};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt as _, core::IntoContainerPort, runners::AsyncRunner,
};
use tokio::time;
use tokio_postgres::{Client, NoTls};

const POSTGRES_PORT: u16 = 5432;

struct PostgresServer {
    _container: ContainerAsync<GenericImage>,
    host: String,
    port: u16,
}

impl PostgresServer {
    async fn init() -> Result<Self, Box<dyn std::error::Error>> {
        let container = GenericImage::new("postgres", "17-alpine")
            .with_exposed_port(POSTGRES_PORT.tcp())
            .with_env_var("POSTGRES_USER", "kv-test")
            .with_env_var("POSTGRES_PASSWORD", "kv-test")
            .with_env_var("POSTGRES_DB", "kv-test")
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
            "postgres://kv-test:kv-test@{}:{}/kv-test",
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

async fn init_client(server: &PostgresServer) -> PostgresClient {
    apply_migrations(server).await;

    let pool = init_pool(server);

    wait_ready(&pool).await;

    PostgresClient::new(pool)
}

async fn apply_migrations(server: &PostgresServer) {
    let mut runner = AsyncMigrationRunner::new(PostgresBackend::new(server.connect_ready().await));

    runner
        .migrate_to_latest(
            alternate_kv::postgres::migrations().expect("embedded migrations are valid"),
        )
        .await
        .expect("kv migrations applied");
}

fn init_pool(server: &PostgresServer) -> Pool {
    let mut config = PostgresPoolConfig::new();
    config.url = Some(server.connection_string());

    config.create_pool(Some(Runtime::Tokio1), NoTls).unwrap()
}

async fn wait_ready(pool: &Pool) {
    let mut ready = false;
    let mut last_error = String::new();

    for _ in 0..30 {
        match pool.get().await {
            Ok(client) => match client.simple_query("SELECT 1").await {
                Ok(_) => {
                    ready = true;
                    break;
                }
                Err(error) => last_error = error.to_string(),
            },
            Err(error) => last_error = error.to_string(),
        }

        time::sleep(Duration::from_millis(500)).await;
    }

    assert!(ready, "Postgres pool did not become ready ({last_error})");
}

fn unique_key(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    format!("kv-test-{name}-{}-{nanos}", std::process::id())
}

async fn row_count(probe: &Client, key: &str) -> Result<i64, tokio_postgres::Error> {
    let row = probe
        .query_one(
            "SELECT count(*) FROM alternate.kv_entries WHERE key = $1",
            &[&key],
        )
        .await?;

    Ok(row.get(0))
}

#[tokio::test]
async fn roundtrips_value() -> Result<(), Box<dyn std::error::Error>> {
    let server = PostgresServer::init().await?;
    let client = init_client(&server).await;

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
    let server = PostgresServer::init().await?;
    let client = init_client(&server).await;

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
    let server = PostgresServer::init().await?;
    let client = init_client(&server).await;

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
    let server = PostgresServer::init().await?;
    let client = init_client(&server).await;

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
    let server = PostgresServer::init().await?;
    let client = init_client(&server).await;
    let probe = server.connect().await?;

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

    assert_eq!(row_count(&probe, &expired_key).await?, 0);
    assert_eq!(row_count(&probe, &durable_key).await?, 1);
    assert_eq!(row_count(&probe, &long_ttl_key).await?, 1);

    Ok(())
}

#[tokio::test]
async fn spawned_sweeper_removes_expired_rows() -> Result<(), Box<dyn std::error::Error>> {
    let server = PostgresServer::init().await?;
    let client = init_client(&server).await;
    let probe = server.connect().await?;

    let key = unique_key("sweeper");

    client
        .set_with_ttl(&key, b"ephemeral", Duration::from_millis(200))
        .await?;

    time::sleep(Duration::from_millis(500)).await;

    let sweeper = client.clone().spawn_sweeper(Duration::from_millis(100));

    let deadline = Instant::now() + Duration::from_secs(10);

    while row_count(&probe, &key).await? > 0 {
        assert!(Instant::now() < deadline, "sweeper did not remove the row");

        time::sleep(Duration::from_millis(100)).await;
    }

    sweeper.abort();

    Ok(())
}

#[tokio::test]
async fn maps_missing_table_to_postgres_error() -> Result<(), Box<dyn std::error::Error>> {
    let server = PostgresServer::init().await?;
    let client = init_client(&server).await;

    let probe = server.connect().await?;
    probe
        .batch_execute("DROP TABLE alternate.kv_entries")
        .await?;

    let result = client.get("any-key").await;

    assert!(matches!(
        result,
        Err(alternate_kv::postgres::PostgresClientError::Postgres(_))
    ));

    Ok(())
}

#[tokio::test]
async fn maps_unreachable_server_to_pool_error() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = PostgresPoolConfig::new();
    config.url = Some("postgres://kv-test:kv-test@127.0.0.1:1/kv-test".to_owned());

    let pool = config.create_pool(Some(Runtime::Tokio1), NoTls)?;

    let client = PostgresClient::new(pool);

    let result = client.get("any-key").await;

    assert!(matches!(
        result,
        Err(alternate_kv::postgres::PostgresClientError::Pool(_))
    ));

    Ok(())
}
