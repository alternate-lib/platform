use std::time::Duration;

use s3::{
    Bucket, Region,
    creds::{Credentials, error::CredentialsError},
    error::S3Error,
};

use crate::{StorageClient, StorageClientPresign};

#[derive(Clone)]
pub struct S3Client {
    bucket: Box<Bucket>,
}

impl S3Client {
    const MIN_EXPIRY: Duration = Duration::from_secs(1);
    const MAX_EXPIRY: Duration = Duration::from_hours(24 * 7);

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(config), fields(access_key_id = config.access_key_id, region = config.region, endpoint = config.endpoint, bucket = config.bucket), err(Debug))
    )]
    pub async fn init(config: S3Config) -> Result<Self, S3ClientError> {
        let region = Region::Custom {
            region: config.region,
            endpoint: config.endpoint,
        };
        let credentials = Credentials::new(
            Some(&config.access_key_id),
            Some(&config.secret_access_key),
            None,
            None,
            None,
        )?;

        let mut bucket = Bucket::new(&config.bucket, region, credentials)?;

        if config.use_path_style {
            bucket.set_path_style();
        }

        let exists = bucket.exists().await?;
        if !exists {
            return Err(S3ClientError::BucketNotFound(config.bucket));
        }

        Ok(Self { bucket })
    }

    fn get_expiry_secs(expiry: Duration) -> Result<u32, S3ClientError> {
        if expiry < Self::MIN_EXPIRY || expiry > Self::MAX_EXPIRY {
            return Err(S3ClientError::ExpiryOutOfRange(expiry));
        }
        let expiry_secs = u32::try_from(expiry.as_secs()).unwrap();

        Ok(expiry_secs)
    }
}

impl StorageClient for S3Client {
    type Error = S3ClientError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let data = self.bucket.get_object(path).await?;
        if data.status_code() == 404 {
            return Ok(None);
        }

        Ok(Some(data.to_vec()))
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn exists(&self, path: &str) -> Result<bool, Self::Error> {
        let (_, status) = self.bucket.head_object(path).await?;

        Ok(status != 404)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self, data), err(Debug))
    )]
    async fn put(&self, path: &str, data: &[u8]) -> Result<(), Self::Error> {
        self.bucket.put_object(path, data).await?;

        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn delete(&self, path: &str) -> Result<(), Self::Error> {
        self.bucket.delete_object(path).await?;

        Ok(())
    }
}

impl StorageClientPresign for S3Client {
    type Error = S3ClientError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn get_presigned(&self, key: &str, expires_in: Duration) -> Result<String, Self::Error> {
        let expiry_secs = Self::get_expiry_secs(expires_in)?;

        Ok(self.bucket.presign_get(key, expiry_secs, None).await?)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(level = "debug", skip(self), err(Debug))
    )]
    async fn put_presigned(&self, key: &str, expires_in: Duration) -> Result<String, Self::Error> {
        let expiry_secs = Self::get_expiry_secs(expires_in)?;

        Ok(self
            .bucket
            .presign_put(key, expiry_secs, None, None)
            .await?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    access_key_id: String,
    secret_access_key: String,
    region: String,
    endpoint: String,
    bucket: String,
    use_path_style: bool,
}

impl S3Config {
    pub fn builder() -> S3ConfigBuilder {
        S3ConfigBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct S3ConfigBuilder {
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    bucket: Option<String>,
    use_path_style: Option<bool>,
}

impl S3ConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn access_key_id(mut self, access_key_id: impl Into<String>) -> Self {
        self.access_key_id = Some(access_key_id.into());
        self
    }

    #[must_use]
    pub fn secret_access_key(mut self, secret_access_key: impl Into<String>) -> Self {
        self.secret_access_key = Some(secret_access_key.into());
        self
    }

    #[must_use]
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    #[must_use]
    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    #[must_use]
    pub fn use_path_style(mut self, use_path_style: bool) -> Self {
        self.use_path_style = Some(use_path_style);
        self
    }

    pub fn build(self) -> Result<S3Config, S3ConfigError> {
        Ok(S3Config {
            access_key_id: self
                .access_key_id
                .ok_or(S3ConfigError::MissingField("access_key_id"))?,
            secret_access_key: self
                .secret_access_key
                .ok_or(S3ConfigError::MissingField("secret_access_key"))?,
            region: self.region.ok_or(S3ConfigError::MissingField("region"))?,
            endpoint: self
                .endpoint
                .ok_or(S3ConfigError::MissingField("endpoint"))?,
            bucket: self.bucket.ok_or(S3ConfigError::MissingField("bucket"))?,
            use_path_style: self.use_path_style.unwrap_or(false),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum S3ClientError {
    #[error("bucket not found with name {0}")]
    BucketNotFound(String),

    #[error("presigned URL expiry {0:?} is not supported")]
    ExpiryOutOfRange(Duration),

    #[error(transparent)]
    Credentials(#[from] CredentialsError),

    #[error(transparent)]
    Client(#[from] S3Error),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum S3ConfigError {
    #[error("missing S3 config value: {0}")]
    MissingField(&'static str),
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn builder_accepts_overrides() {
        let config = S3Config::builder()
            .access_key_id("key")
            .secret_access_key("secret")
            .region("us-east-1")
            .endpoint("http://localhost:9000")
            .bucket("objects")
            .use_path_style(true)
            .build()
            .unwrap();

        assert_eq!(config.access_key_id, "key");
        assert_eq!(config.secret_access_key, "secret");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.endpoint, "http://localhost:9000");
        assert_eq!(config.bucket, "objects");
        assert!(config.use_path_style);
    }

    #[test]
    fn builder_fails_on_missing_fields() {
        let mut builder = S3Config::builder();

        assert_eq!(
            builder.clone().build(),
            Err(S3ConfigError::MissingField("access_key_id"))
        );

        builder = builder.access_key_id("key");

        assert_eq!(
            builder.clone().build(),
            Err(S3ConfigError::MissingField("secret_access_key"))
        );

        builder = builder.secret_access_key("secret");

        assert_eq!(
            builder.clone().build(),
            Err(S3ConfigError::MissingField("region"))
        );

        builder = builder.region("us-east-1");

        assert_eq!(
            builder.clone().build(),
            Err(S3ConfigError::MissingField("endpoint"))
        );

        builder = builder.endpoint("http://localhost:9000");

        assert_eq!(
            builder.clone().build(),
            Err(S3ConfigError::MissingField("bucket"))
        );

        let config = builder.bucket("objects").build().unwrap();

        assert_eq!(config.bucket, "objects");
        assert!(!config.use_path_style);
    }

    #[test]
    fn accepts_valid_expiry() {
        assert_eq!(S3Client::get_expiry_secs(S3Client::MIN_EXPIRY).unwrap(), 1);
        assert_eq!(
            S3Client::get_expiry_secs(S3Client::MAX_EXPIRY).unwrap(),
            7 * 24 * 60 * 60
        );
    }

    #[test]
    fn rejects_out_of_bounds_expiry() {
        let res = S3Client::get_expiry_secs(
            S3Client::MIN_EXPIRY
                .checked_sub(Duration::from_secs(1))
                .unwrap(),
        );

        assert!(matches!(
            res.unwrap_err(),
            S3ClientError::ExpiryOutOfRange(_)
        ));

        let res = S3Client::get_expiry_secs(
            S3Client::MAX_EXPIRY
                .checked_add(Duration::from_secs(1))
                .unwrap(),
        );

        assert!(matches!(
            res.unwrap_err(),
            S3ClientError::ExpiryOutOfRange(_)
        ));
    }
}
