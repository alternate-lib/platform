#![cfg(feature = "redis")]

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alternate_kv::{KvClient, KvClientExpiry, redis::RedisClient};
use deadpool_redis::{Config as RedisPoolConfig, Pool, Runtime};
use testcontainers::{ContainerAsync, GenericImage, core::IntoContainerPort, runners::AsyncRunner};
use tokio::time;

const REDIS_PORT: u16 = 6379;

struct RedisServer {
    _container: ContainerAsync<GenericImage>,
    host: String,
    port: u16,
}

impl RedisServer {
    async fn init() -> Result<Self, Box<dyn std::error::Error>> {
        let container = GenericImage::new("redis", "7.4-alpine")
            .with_exposed_port(REDIS_PORT.tcp())
            .start()
            .await?;

        let host = container
            .get_host()
            .await?
            .to_string()
            .replace("localhost", "127.0.0.1");
        let port = container.get_host_port_ipv4(REDIS_PORT).await?;

        Ok(RedisServer {
            _container: container,
            host,
            port,
        })
    }

    fn redis_url(&self) -> String {
        format!("redis://{}:{}", self.host, self.port)
    }
}

fn init_pool(server: &RedisServer) -> Pool {
    RedisPoolConfig::from_url(server.redis_url())
        .create_pool(Some(Runtime::Tokio1))
        .unwrap()
}

async fn init_client(server: &RedisServer) -> RedisClient {
    let pool = init_pool(server);

    wait_ready(&pool).await;

    RedisClient::new(pool)
}

async fn wait_ready(pool: &Pool) {
    let mut ready = false;
    let mut last_error = String::new();

    for _ in 0..30 {
        match pool.get().await {
            Ok(mut conn) => match redis::cmd("PING").query_async::<()>(&mut conn).await {
                Ok(()) => {
                    ready = true;
                    break;
                }
                Err(error) => last_error = error.to_string(),
            },
            Err(error) => last_error = error.to_string(),
        }

        time::sleep(Duration::from_millis(500)).await;
    }

    assert!(ready, "Redis did not become ready ({last_error})");
}

fn unique_key(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    format!("kv-test-{name}-{}-{nanos}", std::process::id())
}

#[tokio::test]
async fn roundtrips_value() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::init().await?;
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
    let server = RedisServer::init().await?;
    let client = init_client(&server).await;

    let key = unique_key("ttl");
    let payload = b"ephemeral".to_vec();

    client
        .set_with_ttl(&key, &payload, Duration::from_secs(1))
        .await?;

    assert!(client.exists(&key).await?);
    assert_eq!(client.get(&key).await?, Some(payload));

    let deadline = Instant::now() + Duration::from_secs(10);

    while client.get(&key).await?.is_some() {
        assert!(Instant::now() < deadline, "key did not expire in time");

        time::sleep(Duration::from_millis(200)).await;
    }

    assert!(!client.exists(&key).await?);

    Ok(())
}

#[tokio::test]
async fn rejects_sub_second_ttl() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::init().await?;
    let client = init_client(&server).await;

    let key = unique_key("sub-second-ttl");

    let result = client
        .set_with_ttl(&key, b"payload", Duration::from_millis(500))
        .await;

    assert!(matches!(
        result,
        Err(alternate_kv::redis::RedisClientError::Client(_))
    ));

    Ok(())
}

#[tokio::test]
async fn maps_wrongtype_to_client_error() -> Result<(), Box<dyn std::error::Error>> {
    let server = RedisServer::init().await?;
    let client = init_client(&server).await;

    let key = unique_key("wrongtype");

    let raw = redis::Client::open(server.redis_url())?;
    let mut conn = raw.get_multiplexed_async_connection().await?;
    redis::cmd("LPUSH")
        .arg(&key)
        .arg("list-value")
        .query_async::<()>(&mut conn)
        .await?;

    let result = client.get(&key).await;

    assert!(matches!(
        result,
        Err(alternate_kv::redis::RedisClientError::Client(_))
    ));

    Ok(())
}

#[tokio::test]
async fn maps_unreachable_server_to_pool_error() -> Result<(), Box<dyn std::error::Error>> {
    let pool =
        RedisPoolConfig::from_url("redis://127.0.0.1:1/").create_pool(Some(Runtime::Tokio1))?;

    let client = RedisClient::new(pool);

    let result = client.get("any-key").await;

    assert!(matches!(
        result,
        Err(alternate_kv::redis::RedisClientError::Pool(_))
    ));

    Ok(())
}
