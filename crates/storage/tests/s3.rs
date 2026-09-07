#![cfg(feature = "s3")]

use std::time::Duration;

use alternate_storage::{
    StorageClient, StoragePresign,
    s3::{S3Client, S3ClientConfig, S3ClientError},
};
use s3::{Bucket, BucketConfiguration, Region, creds::Credentials};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt as _, core::IntoContainerPort as _,
    runners::AsyncRunner as _,
};
use tokio::time;

const S3_PORT: u16 = 9000;
const ACCESS_KEY: &str = "test-access-key";
const SECRET_KEY: &str = "test-secret-key";
const REGION: &str = "us-east-1";
const BUCKET: &str = "integration-test-bucket";

struct RustfsServer {
    _container: ContainerAsync<GenericImage>,
    host: String,
    port: u16,
}

impl RustfsServer {
    async fn init() -> Result<Self, Box<dyn std::error::Error>> {
        let container = GenericImage::new("rustfs/rustfs", "1.0.0-rc.5")
            .with_exposed_port(S3_PORT.tcp())
            .with_env_var("RUSTFS_VOLUMES", "/data")
            .with_env_var("RUSTFS_ADDRESS", format!("0.0.0.0:{S3_PORT}"))
            .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
            .start()
            .await?;

        let host = container
            .get_host()
            .await?
            .to_string()
            .replace("localhost", "127.0.0.1");
        let port = container.get_host_port_ipv4(S3_PORT).await?;

        Ok(RustfsServer {
            _container: container,
            host,
            port,
        })
    }

    fn endpoint(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    async fn create_bucket(&self, bucket: &str) -> Result<(), Box<dyn std::error::Error>> {
        let region = Region::Custom {
            region: REGION.to_string(),
            endpoint: self.endpoint(),
        };
        let credentials = Credentials::new(Some(ACCESS_KEY), Some(SECRET_KEY), None, None, None)?;

        Bucket::create_with_path_style(bucket, region, credentials, BucketConfiguration::private())
            .await?;

        Ok(())
    }
}

struct HealthClient {
    client: reqwest::Client,
    base_url: String,
}

impl HealthClient {
    fn new(host: &str, port: u16) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: format!("http://{host}:{port}"),
        }
    }

    async fn wait(&self) {
        let mut ready = false;
        let mut last_error = String::new();

        let readiness_endpoint = format!("{}/health/ready", self.base_url);

        for _ in 0..30 {
            match self.client.get(&readiness_endpoint).send().await {
                Ok(response) if response.status().is_success() => {
                    ready = true;
                    break;
                }
                Ok(response) => last_error = response.status().to_string(),
                Err(error) => last_error = error.to_string(),
            }

            time::sleep(Duration::from_millis(1000)).await;
        }

        assert!(
            ready,
            "RustFS readiness endpoint did not become ready ({last_error}); URL: {readiness_endpoint}"
        );
    }
}

async fn init_server() -> Result<RustfsServer, Box<dyn std::error::Error>> {
    let server = RustfsServer::init().await?;

    HealthClient::new(&server.host, server.port).wait().await;

    Ok(server)
}

async fn init_client(endpoint: &str, bucket: &str) -> Result<S3Client, Box<dyn std::error::Error>> {
    let config = S3ClientConfig::builder()
        .access_key_id(ACCESS_KEY)
        .secret_access_key(SECRET_KEY)
        .region(REGION)
        .endpoint(endpoint)
        .bucket(bucket)
        .use_path_style(true)
        .build()?;

    Ok(S3Client::init(config).await?)
}

#[tokio::test]
async fn roundtrips_object() -> Result<(), Box<dyn std::error::Error>> {
    let server = init_server().await?;
    server.create_bucket(BUCKET).await?;

    let client = init_client(&server.endpoint(), BUCKET).await?;

    let key = "documents/example.bin";
    let payload: Vec<u8> = (0..32 * 1024u32)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    let updated: Vec<u8> = b"updated payload".to_vec();

    assert!(!client.exists(key).await?);
    assert_eq!(client.get(key).await?, None);

    client.put(key, &payload).await?;

    assert!(client.exists(key).await?);
    assert_eq!(client.get(key).await?, Some(payload));

    client.put(key, &updated).await?;

    assert_eq!(client.get(key).await?, Some(updated));

    client.delete(key).await?;

    assert!(!client.exists(key).await?);
    assert_eq!(client.get(key).await?, None);

    Ok(())
}

#[tokio::test]
async fn round_trip_object_with_presigned_urls() -> Result<(), Box<dyn std::error::Error>> {
    let server = init_server().await?;
    server.create_bucket(BUCKET).await?;

    let client = init_client(&server.endpoint(), BUCKET).await?;

    let key = "uploads/presigned.bin";
    let payload: Vec<u8> = b"presigned round trip".to_vec();
    let http = reqwest::Client::new();

    let put_url = client.put_presigned(key, Duration::from_secs(60)).await?;
    let put_response = http.put(&put_url).body(payload.clone()).send().await?;

    assert!(
        put_response.status().is_success(),
        "presigned PUT failed with {}",
        put_response.status()
    );

    assert_eq!(client.get(key).await?, Some(payload.clone()));

    let get_url = client.get_presigned(key, Duration::from_secs(60)).await?;
    let get_response = http.get(&get_url).send().await?;

    assert!(
        get_response.status().is_success(),
        "presigned GET failed with {}",
        get_response.status()
    );
    assert_eq!(get_response.bytes().await?, payload);

    Ok(())
}

#[tokio::test]
async fn init_fails_when_bucket_is_missing() -> Result<(), Box<dyn std::error::Error>> {
    let server = init_server().await?;

    let missing_bucket = "missing-bucket";
    let Err(error) = init_client(&server.endpoint(), missing_bucket).await else {
        panic!("init should fail for a missing bucket");
    };

    assert!(matches!(
        error.downcast_ref::<S3ClientError>(),
        Some(S3ClientError::BucketNotFound(name)) if name == missing_bucket
    ));

    Ok(())
}
